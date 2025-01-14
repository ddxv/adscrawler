#!/usr/bin/env python
"""Download APK files from with Python.

Author: James O'Claire

This script scrapes https://apkpure.com to get the apk download link
"""

import argparse
import pathlib

import requests

from adscrawler.config import APKS_DIR, get_logger

logger = get_logger(__name__, "download_apk")

URL = "https://d.apkpure.net/b/XAPK/{store_id}?version=latest"


def check_apk_dir_created() -> None:
    """Create if not exists for apks directory."""
    dirs = [APKS_DIR]
    for _dir in dirs:
        if not pathlib.Path.exists(_dir):
            logger.info("creating apks directory")
            pathlib.Path.mkdir(_dir, exist_ok=True)


def download(store_id: str, do_redownload: bool = False) -> str:
    """Download the apk file.

    store_id: str the id of the android apk

    """

    logger.info(f"{store_id=} download start")
    check_apk_dir_created()
    filepath = pathlib.Path(APKS_DIR, f"{store_id}.apk")
    exists = filepath.exists()
    if exists:
        if not do_redownload:
            logger.info(f"apk already exists {filepath=}, skipping")
            return filepath.suffix

    r = requests.get(
        URL.format(store_id=store_id),
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
        },
        stream=True,
        timeout=10,
    )
    if r.status_code == 200:
        # Try to get extension from Content-Type header
        # content_type = r.headers.get("Content-Type", "")
        # logger.info(f"Received file with Content-Type: {content_type}")

        # Try different methods to determine extension
        extension = ".apk"  # default fallback

        # Method 1: Check Content-Disposition for filename
        content_disposition = r.headers.get("Content-Disposition", "")
        if "filename=" in content_disposition:
            filename = content_disposition.split("filename=")[-1].strip("\"'")
            ext = pathlib.Path(filename).suffix
            if ext:
                extension = ext
                logger.info(f"Found extension in Content-Disposition: {extension}")

        filepath = pathlib.Path(APKS_DIR, f"{store_id}{extension}")
        logger.info(f"Saving to: {filepath}")

        with filepath.open("wb") as file:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)

    else:
        logger.error(f"{store_id=} Request failed with {r.status_code=} {r.text[:50]}")
        raise requests.exceptions.HTTPError
    logger.info(f"{store_id=} download finished")
    return extension


def main(args: argparse.Namespace) -> None:
    """Download APK to local directory and exit."""
    store_id = args.store_id
    download(store_id=store_id)


def parse_args() -> argparse.Namespace:
    """Check passed args.

    will check for command line --store-id in the form of com.example.app
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--store-id",
        help="Store id to download, ie -s 'org.moire.opensudoku'",
    )
    args, leftovers = parser.parse_known_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    main(args)
