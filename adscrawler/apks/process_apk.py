import os
import pathlib
import shutil
import subprocess
import time

from adscrawler.apks import manifest, waydroid
from adscrawler.config import (
    APK_TMP_PARTIALS_DIR,
    APK_TMP_UNZIPPED_DIR,
    APKS_DIR,
    APKS_INCOMING_DIR,
    XAPKS_DIR,
    XAPKS_INCOMING_DIR,
    XAPKS_ISSUES_DIR,
    get_logger,
)
from adscrawler.connection import PostgresCon
from adscrawler.queries import (
    get_next_to_sdk_scan,
    query_store_id_api_called_map,
)

logger = get_logger(__name__)


def process_sdks(
    database_connection: PostgresCon, number_of_apps_to_pull: int = 20
) -> None:
    """
    Decompile the app into its various files and directories.
    This shows which SDKs are used in the app.
    All results are saved to the database.
    """
    store = 1
    apps = get_next_to_sdk_scan(
        database_connection=database_connection,
        store=store,
        limit=number_of_apps_to_pull,
    )
    logger.info(f"Start APK processing: {apps.shape=}")
    for _id, row in apps.iterrows():
        store_id = row.store_id

        try:
            manifest.process_manifest(database_connection=database_connection, row=row)
            time.sleep(1)
        except Exception:
            logger.exception(f"Manifest for {store_id} failed")


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
        capture_output=True,
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
    store_id_map = store_id_map.sort_values(by="run_at", ascending=False)
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
            run_name="regular",
        )
    waydroid.remove_all_third_party_apps()


def process_apks_for_waydroid(
    database_connection: PostgresCon, num_apps: int = 10
) -> None:
    apks = get_downloaded_apks()
    store_id_map = query_store_id_api_called_map(
        database_connection=database_connection, store_ids=apks
    )
    store_id_map = store_id_map.sort_values(by="run_at", ascending=False)
    logger.info(
        f"Waydroid has {store_id_map.shape[0]} apps to process, starting {num_apps}"
    )
    store_id_map = store_id_map.head(num_apps)
    extension = "apk"
    for _, row in store_id_map.iterrows():
        store_id = row.store_id
        waydroid.process_app_for_waydroid(
            database_connection=database_connection,
            extension=extension,
            store_id=store_id,
            store_app=row.store_app,
            run_name="regular",
        )
    waydroid.remove_all_third_party_apps()


def unzip_apk(store_id: str, file_path: pathlib.Path) -> pathlib.Path:
    extension = file_path.suffix
    if extension == ".apk":
        apk_to_decode_path = file_path
    elif extension == ".xapk":
        os.makedirs(APK_TMP_PARTIALS_DIR / store_id, exist_ok=True)
        partial_apk_dir = pathlib.Path(APK_TMP_PARTIALS_DIR, f"{store_id}")
        partial_apk_path = pathlib.Path(partial_apk_dir, f"{store_id}.apk")
        unzip_command = f"unzip -o {file_path.as_posix()} {store_id}.apk -d {partial_apk_dir.as_posix()}"
        unzip_result = os.system(unzip_command)
        apk_to_decode_path = partial_apk_path
        logger.info(f"Output unzipped from xapk to apk: {unzip_result}")
    else:
        raise ValueError(f"Invalid extension: {extension}")

    tmp_decoded_output_path = pathlib.Path(APK_TMP_UNZIPPED_DIR, store_id)
    if tmp_decoded_output_path.exists():
        tmp_apk_path = pathlib.Path(tmp_decoded_output_path, f"{store_id}.apk")
        if tmp_apk_path.exists():
            tmp_apk_path.unlink()

    if not apk_to_decode_path.exists():
        logger.error(f"decode path: {apk_to_decode_path.as_posix()} but file not found")
        raise FileNotFoundError
    try:
        # https://apktool.org/docs/the-basics/decoding
        command = [
            "apktool",
            "decode",
            apk_to_decode_path.as_posix(),
            "-f",
            "-o",
            tmp_decoded_output_path.as_posix(),
        ]
        # Run the command and capture output
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,  # Don't raise exception on non-zero exit
        )

        if "java.lang.OutOfMemoryError" in result.stderr:
            # Possibly related: https://github.com/iBotPeaches/Apktool/issues/3736
            logger.error("Java heap space error occurred, try with -j 1")
            # Handle the error as needed
            result = subprocess.run(
                command + ["-j", "1"],
                capture_output=True,
                text=True,
                check=False,  # Don't raise exception on non-zero exit
            )

        if result.stderr:
            logger.error(f"Error: {result.stderr}")

        # Check return code
        if result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, command)

    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with return code {e.returncode}")
        raise
    return tmp_decoded_output_path


def get_existing_apk_path(store_id: str) -> pathlib.Path | None:
    """Check if an APK or XAPK file exists and return its extension.

    Args:
        store_id: The store ID of the app

    Returns:
        The file extension ('.apk' or '.xapk') if found, None otherwise
    """

    # Define all possible paths to check
    # In the future we would check version codes as well

    paths_to_check = [
        pathlib.Path(APKS_DIR, f"{store_id}.apk"),
        pathlib.Path(XAPKS_DIR, f"{store_id}.xapk"),
        pathlib.Path(APKS_INCOMING_DIR, f"{store_id}.apk"),
        pathlib.Path(XAPKS_INCOMING_DIR, f"{store_id}.xapk"),
    ]

    for path in paths_to_check:
        if path.exists():
            return path

    return None


def empty_folder(pth: pathlib.Path) -> None:
    for sub in pth.iterdir():
        if sub.is_dir() and not sub.is_symlink():
            empty_folder(sub)
            os.rmdir(sub)
        else:
            sub.unlink()


def remove_tmp_files(store_id: str) -> None:
    paths = [APK_TMP_PARTIALS_DIR, APK_TMP_UNZIPPED_DIR]
    for path in paths:
        app_path = pathlib.Path(path, store_id)
        if app_path.exists():
            empty_folder(app_path)
            os.rmdir(app_path)
        else:
            continue
        logger.info(f"{store_id=} deleted {path=}")
