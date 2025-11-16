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
    IPAS_DIR,
    IPAS_INCOMING_DIR,
    IPAS_TMP_UNZIPPED_DIR,
    XAPKS_DIR,
    XAPKS_INCOMING_DIR,
    XAPKS_ISSUES_DIR,
    XAPKS_TMP_UNZIP_DIR,
    get_logger,
)

logger = get_logger(__name__)


def get_md5_hash(file_path: pathlib.Path) -> str:
    md5_hash = hashlib.md5(file_path.read_bytes()).hexdigest()
    if not md5_hash:
        raise ValueError(f"Failed to get MD5 hash for {file_path.as_posix()}")
    return md5_hash


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


def move_downloaded_app_to_main_dir(downloaded_file_path: pathlib.Path) -> pathlib.Path:
    """Move the apk file to the main directory."""
    if not downloaded_file_path.exists():
        raise FileNotFoundError(f"{downloaded_file_path=} not found")
    suffix = downloaded_file_path.suffix
    if suffix == ".apk":
        destination_path = pathlib.Path(APKS_DIR, downloaded_file_path.name)
    elif suffix == ".xapk":
        destination_path = pathlib.Path(XAPKS_DIR, downloaded_file_path.name)
    elif suffix == ".ipa":
        destination_path = pathlib.Path(IPAS_DIR, downloaded_file_path.name)
    else:
        raise ValueError(f"Invalid extension: {downloaded_file_path.suffix}")
    shutil.move(downloaded_file_path, destination_path)
    return destination_path


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
        output = subprocess.run(
            unzip_command,
            shell=True,
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if "filename not matched" in output.stderr:
            unzip_command = f"unzip -o {file_path.as_posix()} base.apk -d {partial_apk_dir.as_posix()}"
            _output = subprocess.run(
                unzip_command, shell=True, check=True, capture_output=True, timeout=60
            )
            base_apk_path = pathlib.Path(partial_apk_dir, "base.apk")
            if base_apk_path.exists():
                partial_apk_path = base_apk_path
            else:
                raise FileNotFoundError(f"{store_id=} xapk unable to unzip base")
        apk_to_decode_path = partial_apk_path
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


def unzip_ipa(ipa_path: pathlib.Path, store_id: str) -> pathlib.Path:
    tmp_decoded_output_path = pathlib.Path(IPAS_TMP_UNZIPPED_DIR, store_id)
    if tmp_decoded_output_path.exists():
        empty_folder(tmp_decoded_output_path)
    if not ipa_path.exists():
        logger.error(f"path: {ipa_path.as_posix()} file not found")
        raise FileNotFoundError(f"IPA file not found: {ipa_path.as_posix()}")
    command = f"unzip {ipa_path.as_posix()} -d {tmp_decoded_output_path.as_posix()}"
    # Run the command
    result = os.system(command)
    # Print the standard output of the command
    logger.info(f"Unzip output: {result}")
    return tmp_decoded_output_path


def get_version(apktool_info_path: pathlib.Path) -> str:
    # Open and read the YAML file
    with apktool_info_path.open("r") as file:
        data = yaml.safe_load(file)
    version = str(data["versionInfo"]["versionCode"])
    return version


def get_local_file_path(store: int, store_id: str) -> pathlib.Path | None:
    """Check if an APK, XAPK, or IPA file exists and return its extension.

    Args:
        store: The store ID of the app
        store_id: The store ID of the app

    Returns:
        The file extension ('.apk' or '.xapk') if found, None otherwise
    """

    # Define all possible paths to check
    # In the future we would check version codes as well

    apk_paths = [
        pathlib.Path(APKS_DIR, f"{store_id}.apk"),
        pathlib.Path(XAPKS_DIR, f"{store_id}.xapk"),
        pathlib.Path(APKS_INCOMING_DIR, f"{store_id}.apk"),
        pathlib.Path(XAPKS_INCOMING_DIR, f"{store_id}.xapk"),
    ]

    ipa_paths = [
        pathlib.Path(IPAS_DIR, f"{store_id}.ipa"),
        pathlib.Path(IPAS_INCOMING_DIR, f"{store_id}.ipa"),
    ]

    paths_to_check = apk_paths if store == 1 else ipa_paths

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
    paths = [
        APK_TMP_PARTIALS_DIR,
        APK_TMP_UNZIPPED_DIR,
        XAPKS_TMP_UNZIP_DIR,
        IPAS_TMP_UNZIPPED_DIR,
    ]
    for path in paths:
        app_path = pathlib.Path(path, store_id)
        if app_path.exists():
            empty_folder(app_path)
            os.rmdir(app_path)
        else:
            continue
        logger.info(f"{store_id=} deleted {path=}")
