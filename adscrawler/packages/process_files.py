import time

import pandas as pd

from adscrawler.config import get_logger
from adscrawler.dbcon.connection import PostgresCon
from adscrawler.dbcon.queries import (
    get_version_code_dbid,
    insert_version_code,
    query_apps_to_download,
    query_apps_to_sdk_scan,
    upsert_details_df,
)
from adscrawler.packages.apks.download_apk import (
    manage_apk_download,
    manual_process_download,
)
from adscrawler.packages.apks.manifest import process_manifest
from adscrawler.packages.ipas.download_ipa import manage_ipa_download
from adscrawler.packages.ipas.get_plist import process_plist
from adscrawler.packages.storage import (
    download_s3_app_by_key,
    get_store_id_apk_s3_keys,
    upload_apk_to_s3,
)
from adscrawler.packages.utils import (
    get_local_file_path,
    move_downloaded_app_to_main_dir,
    remove_tmp_files,
)

logger = get_logger(__name__)


def manual_download_app(
    database_connection: PostgresCon,
    store_id: str,
    store: int,
) -> None:
    if store == 1:
        manual_process_download(
            database_connection=database_connection,
            store_id=store_id,
            store=store,
        )
    elif store == 2:
        raise NotImplementedError("Manual download of ipa is not implemented")
    else:
        raise ValueError(f"Invalid store: {store}")


def download_apps(
    store: int, database_connection: PostgresCon, number_of_apps_to_pull: int = 20
) -> None:
    total_errors = 0
    apps = query_apps_to_download(
        database_connection=database_connection,
        store=store,
    )
    logger.info(
        f"download_apps: {store=} {apps.shape[0]} total apps, start top {number_of_apps_to_pull}"
    )
    apps = apps.head(number_of_apps_to_pull)
    for _id, row in apps.iterrows():
        store_id = row.store_id
        existing_file_path = None
        is_already_in_s3 = False
        try:
            s3df = get_store_id_apk_s3_keys(store=store, store_id=store_id)
            if not s3df.empty:
                s3df = s3df[
                    s3df["version_code"].notna() & ~(s3df["version_code"] == "failed")
                ]
            if s3df.empty:
                raise FileNotFoundError(f"No S3 key found for {store_id=}")
            s3_key = s3df.sort_values(by="version_code", ascending=False)["key"][0]
            is_already_in_s3 = True
        except FileNotFoundError:
            is_already_in_s3 = False
            s3_key = None
        file_path = get_local_file_path(store, store_id)
        external_download = not is_already_in_s3 and not file_path
        has_local_file_but_no_s3 = file_path is not None and not is_already_in_s3
        can_s3_download = file_path is None and is_already_in_s3
        if external_download:
            # proceed to regular download
            pass
        elif has_local_file_but_no_s3:
            # Found a local file, process into s3
            existing_file_path = file_path
        elif can_s3_download:
            # download from s3
            logger.warning(
                f"{store_id=} was already present in S3, this should rarely happen"
            )
            existing_file_path = download_s3_app_by_key(s3_key=s3_key)
        try:
            if store == 1:
                download_result = manage_apk_download(
                    store_id=store_id,
                    existing_local_file_path=existing_file_path,
                )
            elif store == 2:
                download_result = manage_ipa_download(
                    store_id=store_id,
                    existing_local_file_path=existing_file_path,
                )
            insert_version_code(
                version_str=download_result.version_str,
                store_app=row.store_app,
                crawl_result=download_result.crawl_result,
                database_connection=database_connection,
                return_rows=False,
                apk_hash=download_result.md5_hash,
            )
            if (
                download_result.downloaded_file_path
                and download_result.crawl_result in [1, 3]
                and not is_already_in_s3
                and download_result.md5_hash
            ):
                upload_apk_to_s3(
                    store,
                    store_id,
                    download_result.downloaded_file_path.suffix.replace(".", ""),
                    download_result.md5_hash,
                    download_result.version_str,
                    download_result.downloaded_file_path,
                )
                move_downloaded_app_to_main_dir(download_result.downloaded_file_path)
        except Exception:
            logger.exception(f"Download for {store_id} failed")
        remove_tmp_files(store_id=store_id)
        logger.info(
            f"{store_id=} finished with errors={download_result.error_count} {total_errors=}"
        )
        # Handle sleep & errors
        if download_result.error_count == 0:
            if existing_file_path:
                pass
            else:
                if total_errors > 0:
                    total_errors -= 1
                sleep_time = total_errors + 30
                logger.info(f"Sleeping for default time: {sleep_time}")
                time.sleep(sleep_time)
        elif download_result.error_count > 0:
            total_errors += download_result.error_count
            sleep_time = total_errors * total_errors * 10
            logger.info(f"Sleeping for {sleep_time} seconds due to {total_errors=}")
            time.sleep(sleep_time)
        if total_errors > 11:
            logger.error(f"Too many errors: {total_errors=} breaking loop")
            break
    logger.info("Finished downloading APKs")


def process_sdks(
    store: int, database_connection: PostgresCon, number_of_apps_to_pull: int = 20
) -> None:
    """
    Decompile the app into its various files and directories.
    This shows which SDKs are used in the app.
    All results are saved to the database.
    """
    apps = query_apps_to_sdk_scan(
        database_connection=database_connection,
        store=store,
    )
    apps["store"] = store
    logger.info(
        f"SDK processing: {store=} total apps:{apps.shape[0]} top {number_of_apps_to_pull} start"
    )
    apps = apps.head(number_of_apps_to_pull)
    for _id, row in apps.iterrows():
        store_id = row.store_id
        store_app = row.store_app
        version_str = None
        crawl_result = 3
        logger.info(f"SDK processing: {store_id=} start")
        try:
            if store == 1:
                details_df, crawl_result, version_str, raw_txt_str = process_manifest(
                    store_id=store_id, store=store
                )
            elif store == 2:
                details_df, crawl_result, version_str, raw_txt_str = process_plist(
                    store_id=store_id
                )
            else:
                raise ValueError(f"Invalid store: {store}")
            time.sleep(1)
        except Exception:
            logger.exception(f"Manifest for {store_id} failed")
        version_code_dbid = get_version_code_dbid(
            store_app=store_app,
            version_code=version_str,
            database_connection=database_connection,
        )
        if version_code_dbid is None:
            logger.error(
                f"Version code dbid is None for {store_id=}, data not recorded!"
            )
            continue
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
            database_connection.engine,
            if_exists="append",
            index=False,
        )
        if crawl_result == 1:
            upsert_details_df(
                details_df=details_df,
                database_connection=database_connection,
                store_id=store_id,
                raw_txt_str=raw_txt_str,
            )
        else:
            logger.info(f"{store_id=} crawl_result {crawl_result=} skipping upsert")

        remove_tmp_files(store_id=store_id)
