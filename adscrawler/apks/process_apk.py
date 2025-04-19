import os
import pathlib
import time

import pandas as pd

from adscrawler.apks import manifest, mitm_process_log
from adscrawler.config import (
    ANDROID_SDK,
    APK_PARTIALS_DIR,
    APKS_DIR,
    PACKAGE_DIR,
    XAPKS_DIR,
    get_logger,
)
from adscrawler.connection import PostgresCon
from adscrawler.queries import get_top_ranks_for_unpacking

logger = get_logger(__name__, "download_apk")


def process_apks(
    database_connection: PostgresCon, number_of_apps_to_pull: int = 20
) -> None:
    error_count = 0
    store = 1
    apps = get_top_ranks_for_unpacking(
        database_connection=database_connection,
        store=store,
        limit=number_of_apps_to_pull,
    )
    logger.info(f"Start APK processing: {apps.shape=}")
    for _id, row in apps.iterrows():
        if error_count > 5:
            continue
        if error_count > 0:
            time.sleep(error_count * error_count * 10)
        store_id = row.store_id

        try:
            this_error_count, extension = manifest.process_manifest(
                database_connection=database_connection, row=row
            )
            error_count += this_error_count
        except Exception:
            logger.exception(f"Manifest for {store_id} failed")

        try:
            run_waydroid_app(database_connection, extension, row)
        except Exception:
            logger.exception(f"Waydroid API call scraping for {store_id} failed")

        remove_partial_apks(store_id=store_id)


def run_waydroid_app(database_connection: PostgresCon, extension: str, row: pd.Series):
    store_id = row.store_id
    store_app = row.store_app
    if extension == ".xapk":
        xapk_path = pathlib.Path(XAPKS_DIR, f"{store_id}{extension}")
        os.system(f"java -jar APKEditor.jar m -i {xapk_path.as_posix()}")
        # APKEditor merged APKs must be signed to install
        apk_path = pathlib.Path(APKS_DIR, f"{store_id}.apk")
        merged_apk_path = pathlib.Path(XAPKS_DIR, f"{store_id}_merged.apk")
        os.system(
            f"{ANDROID_SDK}/apksigner sign --ks ~/.android/debug.keystore  --ks-key-alias androiddebugkey   --ks-pass pass:android   --key-pass pass:android   --out {apk_path}  {merged_apk_path}"
        )
        apk_path.exists()

    # store_id = 'com.speechtexter.speechtexter'
    # store_id = 'com.water.balls'

    start_script = pathlib.Path(PACKAGE_DIR, "/adscrawler/apks/install_apk_run_mitm.sh")
    os.system(f'.{start_script.as_posix()} -s "{store_id}"')

    mitm_script = pathlib.Path(PACKAGE_DIR, "/adscrawler/apks/mitm_start.sh")
    os.system(f".{mitm_script.as_posix()} -d")

    mdf = mitm_process_log.parse_mitm_log(store_id)
    mdf = mdf.rename(columns={"timestamp": "crawled_at"})
    mdf["url"] = mdf["url"].str[0:1000]
    mdf = mdf[["url", "host", "crawled_at", "status_code", "tld_url"]].drop_duplicates()
    mdf["store_app"] = store_app
    mdf.to_sql(
        name="store_app_api_calls",
        con=database_connection.engine,
        if_exists="append",
        index=None,
    )


def remove_partial_apks(store_id: str) -> None:
    apk_path = pathlib.Path(APK_PARTIALS_DIR, f"{store_id}.apk")
    try:
        apk_path.unlink(missing_ok=True)
        logger.info(f"{store_id=} deleted apk {apk_path.as_posix()}")
    except FileNotFoundError:
        pass
    xapk_path = pathlib.Path(APK_PARTIALS_DIR, f"{store_id}.xapk")
    try:
        xapk_path.unlink(missing_ok=True)
        logger.info(f"{store_id=} deleted xapk {xapk_path.as_posix()}")
    except FileNotFoundError:
        pass
