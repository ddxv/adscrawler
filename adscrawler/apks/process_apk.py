import os
import pathlib
import time

from adscrawler.apks import manifest, waydroid
from adscrawler.config import (
    APK_PARTIALS_DIR,
    APKS_DIR,
    XAPKS_DIR,
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


def get_downloaded_xapks() -> list[str]:
    xapks = []
    apk_path = pathlib.Path(XAPKS_DIR)
    for apk in apk_path.glob("*.xapk"):
        xapks.append(apk.stem)
    return xapks


def process_xapks_for_waydroid(database_connection: PostgresCon) -> None:
    store_ids = get_downloaded_xapks()
    store_id_map = query_store_id_api_called_map(
        database_connection=database_connection, store_ids=store_ids
    )
    store_id_map = store_id_map.sort_values(by="crawled_at", ascending=False)
    logger.info(
        f"Waydroid has {store_id_map.shape[0]} apps to process, starting top 20"
    )
    store_id_map = store_id_map.head(10)
    for _, row in store_id_map.iterrows():
        store_id = row.store_id
        xapk_path = pathlib.Path(XAPKS_DIR, f"{store_id}.xapk")
        tmp_apk_dir = pathlib.Path("/tmp/unzippedxapks/", f"{store_id}")
        tmp_apk_path = pathlib.Path(tmp_apk_dir, f"{store_id}.apk")
        if not tmp_apk_dir.exists():
            os.makedirs(tmp_apk_dir)
        unzip_command = f"unzip -o {xapk_path.as_posix()} {store_id}.apk -d {tmp_apk_dir.as_posix()}"
        _unzip_result = os.system(unzip_command)
        if _unzip_result != 0:
            logger.error(f"Failed to unzip {xapk_path.as_posix()}")
            continue
        waydroid.process_app_for_waydroid(
            database_connection=database_connection,
            apk_path=tmp_apk_path,
            store_id=store_id,
            store_app=row.store_app,
        )


def process_apks_for_waydroid(database_connection: PostgresCon) -> None:
    # Note, we haven't started handling xapks yet
    apks = get_downloaded_apks()
    store_id_map = query_store_id_api_called_map(
        database_connection=database_connection, store_ids=apks
    )
    store_id_map = store_id_map.sort_values(by="crawled_at", ascending=False)
    logger.info(f"Waydroid has {store_id_map.shape[0]} apps to process, starting 20")
    store_id_map = store_id_map.head(10)
    for _, row in store_id_map.iterrows():
        store_id = row.store_id
        apk_path = pathlib.Path(APKS_DIR, f"{store_id}.apk")
        waydroid.process_app_for_waydroid(
            database_connection=database_connection,
            apk_path=apk_path,
            store_id=store_id,
            store_app=row.store_app,
        )


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
