import pathlib
import time

import pandas as pd

from adscrawler.apks import manifest, waydroid
from adscrawler.config import (
    APK_PARTIALS_DIR,
    APKS_DIR,
    get_logger,
)
from adscrawler.connection import PostgresCon
from adscrawler.queries import (
    get_top_ranks_for_unpacking,
    query_store_id_api_called_map,
)

logger = get_logger(__name__)


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

        remove_partial_apks(store_id=store_id)


def get_downloaded_apks() -> list[str]:
    apks = []
    apk_path = pathlib.Path(APKS_DIR)
    for apk in apk_path.glob("*.apk"):
        apks.append(apk.stem)
    return apks


def process_apks_for_waydroid(database_connection: PostgresCon) -> None:
    # Note, we haven't started handling xapks yet
    apks = get_downloaded_apks()
    store_id_map = query_store_id_api_called_map(
        database_connection=database_connection, store_ids=apks
    )
    store_id_map["crawled_at"] = pd.to_datetime(store_id_map["crawled_at"])
    last_crawled_limit = (
        pd.Timestamp.now() - pd.Timedelta(days=31) > store_id_map["crawled_at"]
    )
    store_id_map = store_id_map[last_crawled_limit | store_id_map["crawled_at"].isna()]
    waydroid_process = waydroid.restart()
    if not waydroid_process:
        logger.error("Waydroid failed to start")
        return
    for _, row in store_id_map.iterrows():
        if waydroid.check_session():
            try:
                waydroid.run_app(database_connection, extension=".apk", row=row)
            except Exception:
                try:
                    waydroid.restart()
                except Exception:
                    logger.exception("Failed to restart waydroid, exiting")
                    return
        else:
            logger.error("Waydroid session not running")
            break


def remove_partial_apks(store_id: str) -> None:
    apk_path = pathlib.Path(APK_PARTIALS_DIR, f"{store_id}.apk")
    if apk_path.exists():
        apk_path.unlink(missing_ok=True)
        logger.info(f"{store_id=} deleted partial apk {apk_path.as_posix()}")
    xapk_path = pathlib.Path(APK_PARTIALS_DIR, f"{store_id}.xapk")
    if xapk_path.exists():
        xapk_path.unlink(missing_ok=True)
        logger.info(f"{store_id=} deleted partial xapk {xapk_path.as_posix()}")


if __name__ == "__main__":
    from adscrawler.connection import get_db_connection

    use_tunnel = False
    database_connection = get_db_connection(use_ssh_tunnel=use_tunnel)
    database_connection.set_engine()
    process_apks_for_waydroid(database_connection=database_connection)
