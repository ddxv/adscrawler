import pathlib
import time

from adscrawler.apks import manifest
from adscrawler.config import APK_PARTIALS_DIR, get_logger
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
            error_count += manifest.process_manifest(
                database_connection=database_connection, row=row
            )
        except Exception:
            logger.exception(f"Manifest for {store_id} failed")

        # try:
        #     process_waydroid
        # except exception

        remove_partial_apks(store_id=store_id)


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
