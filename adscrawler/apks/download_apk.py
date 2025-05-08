#!/usr/bin/env python
"""Download APK files from with Python.

Author: James O'Claire

This script scrapes https://apkpure.com to get the apk download link
"""

import hashlib
import os
import pathlib
import shutil
import subprocess
import time

import pandas as pd
import requests

from adscrawler.apks import apkmirror, apkpure
from adscrawler.apks.manifest import get_version
from adscrawler.apks.process_apk import (
    get_downloaded_apks,
    get_downloaded_xapks,
    get_existing_apk_path,
    remove_partial_apks,
    unzip_apk,
)
from adscrawler.config import (
    APKS_DIR,
    APKS_INCOMING_DIR,
    XAPKS_DIR,
    XAPKS_INCOMING_DIR,
    get_logger,
)
from adscrawler.connection import PostgresCon
from adscrawler.queries import (
    get_top_apps_to_download,
    get_version_code_by_md5_hash,
    insert_version_code,
)

APK_SOURCES = ["apkmirror", "apkpure"]


logger = get_logger(__name__, "download_apk")

FAILED_VERSION_STR = "-1"


def download_apks(
    database_connection: PostgresCon, number_of_apps_to_pull: int = 20
) -> None:
    error_count = 0
    store = 1
    apps = get_top_apps_to_download(
        database_connection=database_connection,
        store=store,
        limit=number_of_apps_to_pull,
    )
    logger.info(f"Start APK downloads: {apps.shape=}")
    for _id, row in apps.iterrows():
        if error_count > 5:
            continue
        if error_count > 0:
            time.sleep(error_count * error_count * 30)
        store_id = row.store_id

        try:
            this_error_count = manage_download(
                database_connection=database_connection, row=row
            )
            error_count += this_error_count
        except Exception:
            logger.exception(f"Download for {store_id} failed")

        remove_partial_apks(store_id=store_id)
    check_local_apks(database_connection=database_connection)


def check_local_apks(database_connection: PostgresCon) -> None:
    downloaded_apks = get_downloaded_apks()
    downloaded_xapks = get_downloaded_xapks()
    files = [{"file_type": "apk", "package_name": apk} for apk in downloaded_apks] + [
        {"file_type": "xapk", "package_name": xapk} for xapk in downloaded_xapks
    ]
    df = pd.DataFrame(files)
    df.to_sql(
        "local_apks", con=database_connection.engine, if_exists="replace", index=False
    )


def empty_folder(pth: pathlib.Path) -> None:
    for sub in pth.iterdir():
        if sub.is_dir() and not sub.is_symlink():
            empty_folder(sub)
            os.rmdir(sub)
        else:
            sub.unlink()


def check_version_code_exists(
    database_connection: PostgresCon, store_id: str, file_path: pathlib.Path
) -> str | None:
    md5_hash = hashlib.md5(file_path.read_bytes()).hexdigest()
    version_code = get_version_code_by_md5_hash(database_connection, md5_hash, store_id)
    return version_code


def get_md5_hash(store_id: str, extension: str) -> str:
    if extension == ".apk":
        file_path = pathlib.Path(APKS_INCOMING_DIR, f"{store_id}.apk")
    elif extension == ".xapk":
        file_path = pathlib.Path(XAPKS_INCOMING_DIR, f"{store_id}.xapk")
    else:
        raise ValueError(f"Invalid extension: {extension}")
    return hashlib.md5(file_path.read_bytes()).hexdigest()


def manage_download(database_connection: PostgresCon, row: pd.Series) -> int:
    """Manage the download of an apk or xapk file"""
    crawl_result = 3
    store_id = row.store_id
    logger.info(f"{store_id=} start")
    version_str = FAILED_VERSION_STR
    md5_hash = None
    download_in_incoming = False
    try:
        file_path = get_existing_apk_path(store_id)
        if file_path:
            if "incoming" in file_path.as_posix():
                logger.info(
                    f"{store_id=} already exists in incoming dir, skipping download"
                )
                extension = file_path.suffix
                download_in_incoming = True
            else:
                logger.info(
                    f"{store_id=} already exists in main dir, skipping download"
                )
                if check_version_code_exists(database_connection, store_id, file_path):
                    logger.info(f"{store_id=} version code already in db")
                    return 0
        else:
            extension = download(store_id)
            download_in_incoming = True
        if extension == ".apk":
            file_path = pathlib.Path(APKS_INCOMING_DIR, f"{store_id}{extension}")
        elif extension == ".xapk":
            file_path = pathlib.Path(XAPKS_INCOMING_DIR, f"{store_id}{extension}")
        else:
            raise ValueError(f"Invalid extension: {extension}")
        apk_tmp_decoded_output_path = unzip_apk(store_id=store_id, file_path=file_path)
        apktool_info_path = pathlib.Path(apk_tmp_decoded_output_path, "apktool.yml")
        version_str = get_version(apktool_info_path)

        md5_hash = get_md5_hash(store_id, extension)
        crawl_result = 1
        logger.info(f"{store_id=} unzipped finished")
    except requests.exceptions.HTTPError:
        crawl_result = 2  # 404, 403s etc
    except requests.exceptions.ConnectionError:
        crawl_result = 2  # 404s etc
    except FileNotFoundError:
        logger.exception(f"{store_id=} unable to unpack apk")
        crawl_result = 1
    except subprocess.CalledProcessError as e:
        logger.exception(f"Unexpected error for {store_id=}: {str(e)}")
        crawl_result = 3  # Unzip Error
    except Exception as e:
        logger.exception(f"Unexpected error for {store_id=}: {str(e)}")
        crawl_result = 3  # Unexpected errors
    if crawl_result in [2]:
        error_count = 5
    elif crawl_result in [2, 3, 4]:
        error_count = 1
    elif crawl_result in [1]:
        error_count = 0
    # TODO: this doesn't work quite right when running locally
    # The files will only be available in the local and not on the main server
    # Will need to move the files to a centralized object storage service
    insert_version_code(
        version_str=version_str,
        store_app=row.store_app,
        crawl_result=crawl_result,
        database_connection=database_connection,
        return_rows=False,
        apk_hash=md5_hash,
    )
    if crawl_result in [1, 3] and download_in_incoming:
        # Move files from incoming to main dir
        move_files_to_main_dir(store_id, extension)
    return error_count


def move_files_to_main_dir(store_id: str, extension: str) -> None:
    """Move files from incoming to main dir"""
    if extension == ".apk":
        file_path = pathlib.Path(APKS_INCOMING_DIR, f"{store_id}.apk")
        main_dir = pathlib.Path(APKS_DIR)
    elif extension == ".xapk":
        file_path = pathlib.Path(XAPKS_INCOMING_DIR, f"{store_id}.xapk")
        main_dir = pathlib.Path(XAPKS_DIR)
    else:
        raise ValueError(f"Invalid extension: {extension}")
    if file_path.exists():
        shutil.move(file_path, main_dir / file_path.name)


def get_download_url(store_id: str, source: str) -> str:
    """Get the download url for the apk."""
    if source == "apkmirror":
        try:
            download_url = apkmirror.get_download_url(store_id)
            logger.info("download apk from apkmirror")
            return download_url
        except Exception as e:
            logger.error(f"Error getting APKMIRROR download url for {store_id}: {e}")
            raise e
    elif source == "apkpure":
        try:
            download_url = apkpure.get_download_url(store_id)
            logger.info("download apk from apkpure")
            return download_url
        except Exception as e:
            logger.error(f"Error getting APKPURE download url for {store_id}: {e}")
            raise e
    else:
        raise ValueError(f"Invalid source: {source}")


def download(store_id: str) -> str:
    """Download the apk file.

    Downloaded APK or XAPK files are stored in the incoming directories while processing in this script.
    After processing, they are moved to the main apk/xapk directory.

    store_id: str the id of the android apk
    do_redownload: bool if True, download the apk even if it already exists
    """

    func_info = f"{store_id=} download"

    logger.info(f"{func_info} start")

    download_url = None
    for source in APK_SOURCES:
        try:
            download_url = get_download_url(store_id, source)
            logger.info(f"{func_info} found download url for {source}")
            break
        except Exception as e:
            logger.error(
                f"{func_info} error getting {source} download url for {store_id}: {e}"
            )
            continue

    if not download_url:
        raise requests.exceptions.HTTPError(
            f"{func_info} no download url found for any source."
        )

    r = requests.get(
        download_url,
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

        if extension == ".xapk":
            apk_filepath = pathlib.Path(XAPKS_INCOMING_DIR, f"{store_id}{extension}")
        if extension == ".apk":
            apk_filepath = pathlib.Path(APKS_INCOMING_DIR, f"{store_id}{extension}")
        logger.info(f"{func_info} saving to: {apk_filepath}")

        with apk_filepath.open("wb") as file:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)

    else:
        logger.error(f"{func_info} request failed with {r.status_code=} {r.text[:50]}")
        raise requests.exceptions.HTTPError
    logger.info(f"{func_info} finished")
    return extension
