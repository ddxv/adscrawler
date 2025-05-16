#!/usr/bin/env python
"""Download APK files from with Python.

Author: James O'Claire

This script scrapes https://apkpure.com to get the apk download link
"""

import hashlib
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
    remove_tmp_files,
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
    total_errors = 0
    last_error_count = 0
    this_error_count = 0
    store = 1
    apps = get_top_apps_to_download(
        database_connection=database_connection,
        store=store,
        limit=number_of_apps_to_pull,
    )
    logger.info(f"Start APK downloads: {apps.shape=}")
    for _id, row in apps.iterrows():
        last_error_count = this_error_count
        this_error_count = 0
        if last_error_count > 0:
            sleep_time = total_errors * total_errors * 10
            logger.info(f"Sleeping for {sleep_time} seconds due to {total_errors=}")
            time.sleep(sleep_time)
        if total_errors > 11:
            logger.error(f"Too many errors: {total_errors=} breaking loop")
            break
        store_id = row.store_id
        existing_file_path = None

        try:
            file_path = get_existing_apk_path(store_id)
            if file_path:
                if (
                    "apk-files/apks/" in file_path.as_posix()
                    or "apk-files/xapks/" in file_path.as_posix()
                ) and check_version_code_exists(
                    database_connection, store_id, file_path
                ):
                    logger.info(f"{store_id=} version code already in db, skipping")
                    continue
                else:
                    existing_file_path = file_path
            this_error_count = manage_download(
                database_connection=database_connection,
                row=row,
                exsiting_file_path=existing_file_path,
            )
            if not existing_file_path:
                total_errors += this_error_count
                if this_error_count == 0:
                    sleep_time = total_errors + 30
                    logger.info(f"Sleeping for default time: {sleep_time}")
                    time.sleep(sleep_time)
        except Exception:
            logger.exception(f"Download for {store_id} failed")

        remove_tmp_files(store_id=store_id)
        logger.info(f"{store_id=} finished with {this_error_count=} {total_errors=}")
    check_local_apks(database_connection=database_connection)
    logger.info("Finished downloading APKs")


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


def check_version_code_exists(
    database_connection: PostgresCon, store_id: str, file_path: pathlib.Path
) -> int | None:
    md5_hash = hashlib.md5(file_path.read_bytes()).hexdigest()
    version_code = get_version_code_by_md5_hash(database_connection, md5_hash, store_id)
    return version_code


def get_md5_hash(file_path: pathlib.Path) -> str:
    return hashlib.md5(file_path.read_bytes()).hexdigest()


def manage_download(
    database_connection: PostgresCon,
    row: pd.Series,
    exsiting_file_path: pathlib.Path | None,
) -> int:
    """Manage the download of an apk or xapk file"""
    store_id = row.store_id
    func_info = f"{store_id=} manage_download"
    crawl_result = 3
    store_id = row.store_id
    logger.info(f"{func_info} start")
    version_str = FAILED_VERSION_STR
    md5_hash = None
    file_path = None

    download_url = None
    for source in APK_SOURCES:
        try:
            download_url = get_download_url(store_id, source)
            break
        except Exception as e:
            logger.error(f"{func_info} {source=}: {e}")
            continue

    if not download_url:
        raise requests.exceptions.HTTPError(
            f"{store_id=} no download URL found for any source."
        )

    try:
        if exsiting_file_path:
            file_path = exsiting_file_path
            logger.info(f"{func_info} using existing file: {file_path}")
        else:
            file_path = download(store_id, download_url)
        apk_tmp_decoded_output_path = unzip_apk(store_id=store_id, file_path=file_path)
        apktool_info_path = pathlib.Path(apk_tmp_decoded_output_path, "apktool.yml")
        version_str = get_version(apktool_info_path)

        md5_hash = get_md5_hash(file_path)
        crawl_result = 1

    except requests.exceptions.HTTPError:
        crawl_result = 2  # 404, 403s etc
    except requests.exceptions.ConnectionError:
        crawl_result = 2  # 404s etc
    except FileNotFoundError:
        logger.exception(f"{func_info} unable to unpack apk")
        crawl_result = 1
    except subprocess.CalledProcessError as e:
        logger.exception(f"Unexpected error for {store_id=}: {str(e)}")
        crawl_result = 3  # Unzip Error
    except Exception as e:
        logger.exception(f"Unexpected error for {store_id=}: {str(e)}")
        crawl_result = 3  # Unexpected errors

    if crawl_result in [2]:
        error_count = 3
    elif crawl_result in [2, 3, 4]:
        error_count = 1
    elif crawl_result in [1]:
        error_count = 0

    logger.info(f"{func_info} {crawl_result=} {md5_hash=} {version_str=}")
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
    if crawl_result in [1, 3] and "incoming" in file_path.as_posix():
        # Move files from incoming to main dir
        move_files_to_main_dir(store_id, file_path.suffix)
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
    """Get the download URL for the apk."""
    if source == "apkmirror":
        try:
            download_url = apkmirror.get_download_url(store_id)
            return download_url
        except Exception as e:
            raise e
    elif source == "apkpure":
        try:
            download_url = apkpure.get_download_url(store_id)
            return download_url
        except Exception as e:
            raise e
    else:
        raise ValueError(f"Invalid source: {source}")


def download(store_id: str, download_url: str) -> pathlib.Path:
    """Download the apk file.

    Downloaded APK or XAPK files are stored in the incoming directories while processing in this script.
    After processing, they are moved to the main apk/xapk directory.

    store_id: str the id of the android apk
    do_redownload: bool if True, download the apk even if it already exists
    """

    func_info = f"{store_id=} {download_url=} download"
    logger.info(f"{func_info} start")

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
        elif extension == ".apk":
            apk_filepath = pathlib.Path(APKS_INCOMING_DIR, f"{store_id}{extension}")
        else:
            raise ValueError(f"Invalid extension: {extension}")
        logger.info(f"{func_info} saving to: {apk_filepath}")

        with apk_filepath.open("wb") as file:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)

    else:
        logger.error(f"{func_info} request failed with {r.status_code=} {r.text[:50]}")
        raise requests.exceptions.HTTPError
    logger.info(f"{func_info} finished")
    return apk_filepath
