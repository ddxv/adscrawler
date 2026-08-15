import datetime
import time

import pandas as pd

from adscrawler.config import get_logger
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import (
    insert_s3_key_to_hot,
    insert_version_code,
    query_apps_to_download,
    query_apps_to_sdk_scan,
    query_apps_to_sdk_scan_fix,
    upsert_df,
)
from adscrawler.metrics import (
    DOWNLOAD_RESULTS_COUNTER,
    SDK_SCAN_RESULTS_COUNTER,
)
from adscrawler.packages.apks.download_apk import (
    manage_apk_download,
    manual_process_download,
)
from adscrawler.packages.apks.manifest import process_manifest
from adscrawler.packages.ipas.download_ipa import manage_ipa_download
from adscrawler.packages.ipas.get_plist import process_plist
from adscrawler.packages.utils import (
    move_downloaded_app_to_main_dir,
    remove_tmp_files,
)
from adscrawler.process.storage import (
    set_iptables_rule_for_wt0,
    upload_apk_to_s3,
)
from adscrawler.process.version_details import write_version_details_to_s3

logger = get_logger(__name__)


def manual_download_app(
    pgdb: PostgresEngine,
    store_id: str,
    store: int,
) -> None:
    if store == 1:
        manual_process_download(
            pgdb=pgdb,
            store_id=store_id,
            store=store,
        )
    elif store == 2:
        raise NotImplementedError("Manual download of ipa is not implemented")
    else:
        raise ValueError(f"Invalid store: {store}")


def download_apps(
    store: int, pgdb: PostgresEngine, number_of_apps_to_pull: int = 20
) -> None:
    total_errors = 0
    apps = query_apps_to_download(pgdb=pgdb, store=store, limit=number_of_apps_to_pull)
    if apps.empty:
        total_apps = 0
    else:
        total_apps = apps["total_queue_depth"].values[0]
    logger.info(f"download_apps: {store=} {total_apps=:,} start")
    apps = apps.head(number_of_apps_to_pull)
    set_iptables_rule_for_wt0()
    for _id, row in apps.iterrows():
        store_id = row.store_id
        store_app = row.store_app
        last_downloaded_version_code = row.last_downloaded_version_code
        s3_key = None
        download_result = None
        try:
            if store == 1:
                download_result = manage_apk_download(
                    store_id=store_id,
                    last_downloaded_version_code=last_downloaded_version_code,
                )
            elif store == 2:
                download_result = manage_ipa_download(
                    store_id=store_id,
                )
            else:
                raise ValueError(f"Invalid store: {store}")
            if (
                download_result.downloaded_file_path
                and download_result.crawl_result in [1, 3]
                and download_result.md5_hash
            ):
                s3_key = upload_apk_to_s3(
                    store=store,
                    store_id=store_id,
                    extension=download_result.downloaded_file_path.suffix.replace(
                        ".", ""
                    ),
                    md5_hash=download_result.md5_hash,
                    version_str=download_result.version_str,
                    file_path=download_result.downloaded_file_path,
                )

                move_downloaded_app_to_main_dir(download_result.downloaded_file_path)
            version_code_db_id = insert_version_code(
                version_str=download_result.version_str,
                store_app=row.store_app,
                crawl_result=download_result.crawl_result,
                pgdb=pgdb,
                apk_hash=download_result.md5_hash,
            )
            if version_code_db_id and s3_key:
                insert_s3_key_to_hot(
                    myregion="loki",
                    s3_key=s3_key,
                    store_app=store_app,
                    version_str=download_result.version_str,
                    version_code_id=version_code_db_id,
                    last_modified=datetime.datetime.now(datetime.UTC),
                    pgdb=pgdb,
                )

        except Exception:
            logger.exception(f"Download for {store_id} failed")
        remove_tmp_files(store_id=store_id)
        DOWNLOAD_RESULTS_COUNTER.add(
            1,
            attributes={
                "store": str(store),
                "download_result": (
                    str(download_result.crawl_result) if download_result else "0"
                ),
            },
        )
        errors_msg = (
            f" with errors={download_result.error_count}"
            if download_result.error_count > 0
            else ""
        )
        logger.info(f"{store_id=} finished{errors_msg} {total_errors=}")
        # Handle sleep & errors
        if download_result.error_count == 0:
            if total_errors > 0:
                total_errors -= 1
            sleep_time = total_errors + 10
            logger.info(f"Sleeping for default time: {sleep_time}")
            time.sleep(sleep_time)
        elif download_result.error_count > 0:
            total_errors += download_result.error_count
            sleep_time = total_errors * total_errors * 5
            logger.info(f"Sleeping for {sleep_time} seconds due to {total_errors=}")
            time.sleep(sleep_time)
        if total_errors > 11:
            logger.error(f"Too many errors: {total_errors=} breaking loop")
            break
    logger.info("Finished downloading APKs")


def process_sdks(
    store: int,
    pgdb: PostgresEngine,
    number_of_apps_to_pull: int = 20,
    run_fixes: bool = False,
) -> None:
    """
    Decompile the app into its various files and directories.
    This shows which SDKs are used in the app.
    All results are saved to the database.
    """
    if run_fixes:
        apps = query_apps_to_sdk_scan_fix(pgdb, store)
    else:
        apps = query_apps_to_sdk_scan(
            pgdb=pgdb, store=store, limit=number_of_apps_to_pull
        )
    apps["store"] = store
    log_info = f"process_sdks: {store=}"
    logger.info(f"{log_info} {number_of_apps_to_pull} start")
    apps = apps.head(number_of_apps_to_pull)
    i = 0
    for _id, row in apps.iterrows():
        i += 1
        store_id = row.store_id
        row_info = f"{log_info} {store_id=}"
        store_app = row.store_app
        version_str = row["version_code_str"]
        version_code_dbid = row["version_code_db_id"]
        crawl_result = 3
        logger.info(f"{row_info} {i}/{number_of_apps_to_pull} start")
        if version_code_dbid is None:
            logger.error(f"{row_info} version code dbid is None, data not recorded!")
            raise
        try:
            if store == 1:
                details_df, crawl_result, raw_txt_str = process_manifest(
                    store_id=store_id, store=store, specific_version_str=version_str
                )
            elif store == 2:
                details_df, crawl_result, raw_txt_str = process_plist(
                    store_id=store_id, version_str=version_str
                )
            else:
                raise ValueError(f"Invalid store: {store}")
        except Exception:
            logger.exception(f"{row_info} failed")
            raise

        if details_df is None or details_df.empty:
            details_df = pd.DataFrame(
                [
                    {
                        "store_app": store_app,
                        "version_code_id": version_code_dbid,
                        "scan_result": crawl_result,
                    }
                ]
            )
        else:
            details_df["store_app"] = store_app
            details_df["version_code_id"] = version_code_dbid
            details_df["scan_result"] = crawl_result

        version_code_df = details_df[
            ["version_code_id", "scan_result"]
        ].drop_duplicates()

        version_code_df.to_sql(
            "version_code_sdk_scan_results",
            pgdb.engine,
            if_exists="append",
            index=False,
        )
        if crawl_result == 1:
            upsert_sdk_details_df(
                details_df=details_df,
                pgdb=pgdb,
                store_id=store_id,
                raw_txt_str=raw_txt_str,
            )
        else:
            logger.info(f"{row_info} {crawl_result=} skipping upsert")

        SDK_SCAN_RESULTS_COUNTER.add(
            1,
            attributes={
                "store": str(store),
                "scan_result": str(crawl_result),
            },
        )
        remove_tmp_files(store_id=store_id)
        logger.info(f"{row_info} {crawl_result=} end")


def upsert_sdk_details_df(
    details_df: pd.DataFrame,
    pgdb: PostgresEngine,
    store_id: str,
    raw_txt_str: str,
) -> None:
    details_df = details_df.rename(
        columns={
            "path": "xml_path",
            "android_name": "value_name",
            "version_code_id": "version_code",
        }
    )
    key_insert_columns = ["xml_path", "tag", "value_name"]
    logger.info(f"{store_id=} insert {details_df.shape[0]:,} version_strings to db")
    details_df.loc[details_df["tag"].isna(), "tag"] = ""
    strings_df = details_df[key_insert_columns + ["version_code"]].drop_duplicates()
    version_strings_df = upsert_df(
        df=strings_df,
        table_name="version_strings",
        pgdb=pgdb,
        key_columns=key_insert_columns,
        insert_columns=key_insert_columns,
        return_rows=True,
    )
    if version_strings_df is None:
        logger.error(f"{store_id=} insert version_strings to db returned None")
        logger.error(strings_df[strings_df["tag"].isna()])
        raise Exception(f"{store_id=} insert version_strings to db")
    version_strings_df = version_strings_df.rename(columns={"id": "string_id"})
    strings_map_df = pd.merge(
        strings_df,
        version_strings_df,
        how="left",
        on=["xml_path", "tag", "value_name"],
        validate="many_to_one",
    )
    if strings_map_df["string_id"].isna().any():
        logger.error(f"{store_id=} insert strings_map to db")
        logger.error(strings_map_df[strings_map_df["string_id"].isna()])
    write_version_details_to_s3(
        version_details_df=strings_map_df.rename(
            columns={"version_code": "version_code_id"}
        )[["version_code_id", "string_id"]],
        store_id=store_id,
    )
    strings_map_df["manifest_string"] = raw_txt_str
    manifest_df = strings_map_df[["version_code", "manifest_string"]].drop_duplicates()
    upsert_df(
        df=manifest_df,
        table_name="version_manifests",
        pgdb=pgdb,
        key_columns=["version_code"],
        insert_columns=["version_code", "manifest_string"],
    )
    logger.info(f"{store_id=} finished")
