"""Download an APK and extract it's manifest."""

import os
import pathlib

from adscrawler.config import MODULE_DIR, get_logger

logger = get_logger(__name__)


APKS_DIR = pathlib.Path(MODULE_DIR, "apks/")
UNZIPPED_DIR = pathlib.Path(MODULE_DIR, "apksunzipped/")


def check_dirs() -> None:
    """Create if not exists for apks directory."""
    dirs = [APKS_DIR, UNZIPPED_DIR]
    for _dir in dirs:
        if not pathlib.Path.exists(_dir):
            logger.info(f"creating {_dir} directory")
            pathlib.Path.mkdir(_dir, exist_ok=True)


def extract_manifest():

    if UNZIPPED_DIR.exists():
        pathlib.Path.rmdir(UNZIPPED_DIR)

    apk = "apks/com.zhiliaoapp.musically.apk"

    check_dirs()

    # https://apktool.org/docs/the-basics/decoding
    command = f"apktool decode {apk} -o apksunzipped"

    # Execute the command
    try:

        # Run the command
        result = os.system(command)

        # Print the standard output of the command
        logger.info(f"Output: {result.stdout}")

        # Check if there was any error
        if result.stderr:
            print("Error:\n", result.stderr)
    except FileNotFoundError:
        # Handle case where apktool is not installed or not in PATH
        logger.exception(
            "apktool not found. Please ensure it is installed and in your PATH."
        )
