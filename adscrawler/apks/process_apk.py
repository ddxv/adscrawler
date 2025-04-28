import pathlib
import shutil
import subprocess
import time

from adscrawler.apks import manifest, waydroid
from adscrawler.config import (
    APK_PARTIALS_DIR,
    APKS_DIR,
    XAPKS_DIR,
    XAPKS_ISSUES_DIR,
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


def check_xapk_is_valid(xapk_path: pathlib.Path) -> bool:
    check_unzip_command = f"unzip -qt {xapk_path.as_posix()}"
    _check_unzip_result = subprocess.run(
        check_unzip_command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if _check_unzip_result.returncode != 0:
        logger.error(f"Failed to unzip {xapk_path}, moving to {XAPKS_ISSUES_DIR}")
        shutil.move(xapk_path, pathlib.Path(XAPKS_ISSUES_DIR, xapk_path.name))
        return False
    else:
        return True


def process_xapks_for_waydroid(
    database_connection: PostgresCon, num_apps: int = 10
) -> None:
    store_ids = get_downloaded_xapks()
    store_id_map = query_store_id_api_called_map(
        database_connection=database_connection,
        store_ids=store_ids,
    )
    store_id_map = store_id_map.sort_values(by="crawled_at", ascending=False)
    logger.info(
        f"Waydroid has {store_id_map.shape[0]} (xapk) apps to process, starting top {num_apps}"
    )
    store_id_map = store_id_map.head(num_apps)
    for _, row in store_id_map.iterrows():
        store_id = row.store_id
        xapk_path = pathlib.Path(XAPKS_DIR, f"{store_id}.xapk")
        if check_xapk_is_valid(xapk_path):
            pass
        else:
            continue
        waydroid.process_app_for_waydroid(
            database_connection=database_connection,
            extension="xapk",
            store_id=store_id,
            store_app=row.store_app,
        )


def process_apks_for_waydroid(
    database_connection: PostgresCon, num_apps: int = 10
) -> None:
    apks = get_downloaded_apks()
    store_id_map = query_store_id_api_called_map(
        database_connection=database_connection, store_ids=apks
    )
    store_id_map = store_id_map.sort_values(by="crawled_at", ascending=False)
    logger.info(
        f"Waydroid has {store_id_map.shape[0]} apps to process, starting {num_apps}"
    )
    store_id_map = store_id_map.head(num_apps)
    for _, row in store_id_map.iterrows():
        store_id = row.store_id
        waydroid.process_app_for_waydroid(
            database_connection=database_connection,
            extension="apk",
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
