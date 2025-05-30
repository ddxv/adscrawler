#!/usr/bin/env python
"""Download APK files from with Python."""

import pathlib
import subprocess
import time

import pandas as pd
import requests

from adscrawler.apks import apkmirror, apkpure
from adscrawler.apks.process_apk import (
    get_local_apk_path,
    get_md5_hash,
    get_version,
    move_incoming_apk_to_main_dir,
    remove_tmp_files,
    unzip_apk,
)
from adscrawler.apks.storage import (
    download_s3_apk,
    get_store_id_s3_keys,
    upload_apk_to_s3,
)
from adscrawler.config import (
    APKS_INCOMING_DIR,
    XAPKS_INCOMING_DIR,
    get_logger,
)
from adscrawler.connection import PostgresCon
from adscrawler.queries import (
    insert_version_code,
    query_apps_to_download,
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
    apps = query_apps_to_download(
        database_connection=database_connection,
        store=store,
        limit=number_of_apps_to_pull,
    )
    logger.info(f"Start APK downloads: {apps.shape=}")
    for _id, row in apps.iterrows():
        store_id = row.store_id
        existing_file_path = None
        s3df = get_store_id_s3_keys(store_id=store_id)
        if not s3df.empty:
            s3df = s3df[s3df["version_code"].notna()]
        file_path = get_local_apk_path(store_id)
        needs_external_download = s3df.empty and not file_path
        has_local_file_but_no_s3 = file_path and s3df.empty
        needs_s3_download = not file_path and not s3df.empty

        if needs_external_download:
            # proceed to regular download
            pass
        elif has_local_file_but_no_s3:
            # Found a local file, process into s3
            existing_file_path = file_path
        elif needs_s3_download:
            # download from s3
            logger.warning(
                f"{store_id=} was already present in S3, this should rarely happen"
            )
            s3_key = s3df.sort_values(by="version_code", ascending=False).iloc[0].key
            existing_file_path = download_s3_apk(s3_key=s3_key)

        last_error_count = this_error_count
        this_error_count = 0
        if last_error_count > 0:
            sleep_time = total_errors * total_errors * 10
            logger.info(f"Sleeping for {sleep_time} seconds due to {total_errors=}")
            time.sleep(sleep_time)
        if total_errors > 11:
            logger.error(f"Too many errors: {total_errors=} breaking loop")
            break

        try:
            this_error_count = manage_download(
                database_connection=database_connection,
                row=row,
                existing_s3_path=existing_file_path,
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
    logger.info("Finished downloading APKs")


def manage_download(
    database_connection: PostgresCon,
    row: pd.Series,
    existing_s3_path: pathlib.Path | None,
) -> int:
    """Manage the download of an apk or xapk file"""
    store_id = row.store_id
    func_info = f"manage_download {store_id=}"
    crawl_result = 3
    store_id = row.store_id
    logger.info(f"{func_info} start")
    version_str = FAILED_VERSION_STR
    md5_hash = None
    downloaded_file_path = None

    try:
        if existing_s3_path:
            downloaded_file_path = download_s3_apk(s3_key=existing_s3_path.as_posix())
            logger.info(f"{func_info} found existing file: {downloaded_file_path}")
        else:
            downloaded_file_path = download(store_id)
        apk_tmp_decoded_output_path = unzip_apk(
            store_id=store_id, file_path=downloaded_file_path
        )
        apktool_info_path = pathlib.Path(apk_tmp_decoded_output_path, "apktool.yml")
        version_str = get_version(apktool_info_path)
        md5_hash = get_md5_hash(downloaded_file_path)
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
        error_count = 2
    elif crawl_result in [3, 4]:
        error_count = 1
    elif crawl_result in [1]:
        error_count = 0

    if existing_s3_path:
        error_count = 0

    logger.info(f"{func_info} {crawl_result=} {md5_hash=} {version_str=}")

    insert_version_code(
        version_str=version_str,
        store_app=row.store_app,
        crawl_result=crawl_result,
        database_connection=database_connection,
        return_rows=False,
        apk_hash=md5_hash,
    )
    if (
        downloaded_file_path
        and crawl_result in [1, 3]
        and not existing_s3_path
        and md5_hash
    ):
        upload_apk_to_s3(
            store_id,
            downloaded_file_path.suffix.replace(".", ""),
            md5_hash,
            version_str,
            downloaded_file_path,
        )
        move_incoming_apk_to_main_dir(downloaded_file_path)
    return error_count


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


def download(store_id: str) -> pathlib.Path:
    """Download the apk file.

    Downloaded APK or XAPK files are stored in the incoming directories while processing in this script.
    After processing, they are moved to the main apk/xapk directory.

    store_id: str the id of the android apk
    do_redownload: bool if True, download the apk even if it already exists
    source: str the source of the download
    """

    func_info = f"download {store_id=}"

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

    func_info = f"download {store_id=} {source=}"
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
            else:
                logger.info(f"{func_info} no extension found in Content-Disposition")

        if extension == ".xapk":
            apk_filepath = pathlib.Path(XAPKS_INCOMING_DIR, f"{store_id}{extension}")
        elif extension == ".apk":
            apk_filepath = pathlib.Path(APKS_INCOMING_DIR, f"{store_id}{extension}")
        else:
            raise ValueError(f"Invalid extension: {extension}")

        with apk_filepath.open("wb") as file:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)

    else:
        logger.error(f"{func_info} {r.status_code=} {r.text[:20]}")
        raise requests.exceptions.HTTPError
    logger.info(f"{func_info} finished")
    return apk_filepath
