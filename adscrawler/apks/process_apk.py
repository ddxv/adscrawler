import hashlib
import os
import pathlib
import shutil
import subprocess

import yaml

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

logger = get_logger(__name__)


def get_md5_hash(file_path: pathlib.Path) -> str:
    return hashlib.md5(file_path.read_bytes()).hexdigest()


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


def unzip_apk(store_id: str, file_path: pathlib.Path) -> pathlib.Path:
    extension = file_path.suffix
    if extension == ".apk":
        apk_to_decode_path = file_path
    elif extension == ".xapk":
        os.makedirs(APK_TMP_PARTIALS_DIR / store_id, exist_ok=True)
        if not check_xapk_is_valid(file_path):
            raise FileNotFoundError(
                f"{store_id=} xapk was invalid: moved to issues dir"
            )
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
        empty_folder(tmp_decoded_output_path)

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


def get_version(apktool_info_path: pathlib.Path) -> str:
    # Open and read the YAML file
    with apktool_info_path.open("r") as file:
        data = yaml.safe_load(file)
    version = str(data["versionInfo"]["versionCode"])
    return version


def get_local_apk_path(store_id: str) -> pathlib.Path | None:
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

    logger.info(f"Checking {store_id=} if exists locally")

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
            try:
                sub.unlink()
            except FileNotFoundError:
                pass


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
