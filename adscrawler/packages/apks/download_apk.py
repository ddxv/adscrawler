#!/usr/bin/env python
"""Download APK files from with Python."""

import pathlib
import shutil
import subprocess
import sys
import zipfile

import requests

from adscrawler.config import (
    APKS_INCOMING_DIR,
    SPLITS_INCOMING_DIR,
    XAPKS_INCOMING_DIR,
    get_logger,
)
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import (
    insert_version_code,
    query_store_app_by_store_id,
)
from adscrawler.packages.apks import apkmirror, apkpure
from adscrawler.packages.models import DownloadResult
from adscrawler.packages.storage import (
    upload_apk_to_s3,
)
from adscrawler.packages.utils import (
    get_local_file_path,
    get_md5_hash,
    get_version,
    move_downloaded_app_to_main_dir,
    unzip_apk,
)

APK_SOURCES = ["gplaydl", "apkpure", "apkmirror"]


logger = get_logger(__name__, "download_apk")

FAILED_VERSION_STR = "-1"


def manual_process_download(
    pgdb: PostgresEngine,
    store_id: str,
    store: int,
) -> None:
    """Manual download of an apk file."""
    store_app = query_store_app_by_store_id(pgdb=pgdb, store_id=store_id)
    existing_local_file_path = get_local_file_path(store, store_id)
    if store == 1:
        download_result = manage_apk_download(
            store_id=store_id,
            existing_local_file_path=existing_local_file_path,
        )
    version_str = download_result.version_str or FAILED_VERSION_STR
    insert_version_code(
        version_str=version_str,
        store_app=store_app,
        crawl_result=download_result.crawl_result,
        pgdb=pgdb,
        return_rows=False,
        apk_hash=download_result.md5_hash,
    )
    if (
        download_result.downloaded_file_path
        and download_result.crawl_result in [1, 3]
        and download_result.md5_hash
    ):
        upload_apk_to_s3(
            store,
            store_id,
            download_result.downloaded_file_path.suffix.replace(".", ""),
            download_result.md5_hash,
            version_str,
            download_result.downloaded_file_path,
        )
        move_downloaded_app_to_main_dir(download_result.downloaded_file_path)


def manage_apk_download(
    store_id: str,
    existing_local_file_path: pathlib.Path | None,
) -> DownloadResult:
    """Manage the download of an apk or xapk file"""
    func_info = f"manage_download {store_id=}"
    crawl_result = 3
    logger.info(f"{func_info} start")
    version_str = FAILED_VERSION_STR
    md5_hash = None
    downloaded_file_path = None

    try:
        if existing_local_file_path:
            downloaded_file_path = existing_local_file_path
        else:
            downloaded_file_path = external_download(store_id)
        if not downloaded_file_path:
            raise FileNotFoundError(f"Downloaded file path not found for {store_id=}")
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

    if existing_local_file_path:
        error_count = 0

    logger.info(f"{func_info} {crawl_result=} {md5_hash=} {version_str=}")

    return DownloadResult(
        crawl_result=crawl_result,
        version_str=version_str,
        md5_hash=md5_hash,
        downloaded_file_path=downloaded_file_path,
        error_count=error_count,
    )


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


def download_from_url(download_url: str, store_id: str) -> pathlib.Path:
    func_info = f"download_from_url {store_id=}"
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


def gplaydl_download(store_id: str) -> pathlib.Path:
    destination_dir = pathlib.Path(SPLITS_INCOMING_DIR, store_id)
    gplaydl_path = pathlib.PATH(sys.prefix, "bin", "gplaydl")
    subprocess.run([str(gplaydl_path), "auth"], check=True)
    subprocess.run(
        [str(gplaydl_path), "download", store_id, "-o", destination_dir], check=True
    )
    # get the downloaded file path
    downloaded_files = list(destination_dir.glob("*"))
    if not downloaded_files:
        raise FileNotFoundError(f"No file downloaded for {store_id=}")
    # Rename downloaded file to {store_id}.apk from {store_id}-version.apk
    found_base_apk = False
    for file in downloaded_files:
        if file.suffix in [".apk"]:
            parts = file.name.split("-")
            if len(parts) == 2:
                shutil.move(
                    file, pathlib.Path(destination_dir, f"{store_id}{file.suffix}")
                )
                found_base_apk = True
    if not found_base_apk:
        raise FileNotFoundError(f"No base apk found for {store_id=}")
    # Zip the directory to create an xapk
    xapk_path = pathlib.Path(XAPKS_INCOMING_DIR, f"{store_id}.xapk")
    with zipfile.ZipFile(xapk_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file in sorted(destination_dir.iterdir()):
            if file.is_file():
                archive.write(file, arcname=file.name)
    return xapk_path


def external_download(store_id: str) -> pathlib.Path:
    """Download the apk file.

    Downloaded APK or XAPK files are stored in the incoming directories while processing in this script.
    After processing, they are moved to the main apk/xapk directory.

    store_id: str the id of the android apk
    do_redownload: bool if True, download the apk even if it already exists
    source: str the source of the download
    """

    for source in APK_SOURCES:
        func_info = f"download {store_id=} {source=}"
        try:
            if source == "gplaydl":
                apk_filepath = gplaydl_download(store_id)
            else:
                download_url = get_download_url(store_id, source)
                apk_filepath = download_from_url(download_url, store_id)
            break
        except Exception as e:
            logger.error(f"{func_info}: {e}")
            continue
    if not download_url:
        raise requests.exceptions.HTTPError(
            f"{store_id=} no download URL found for any source."
        )
    return apk_filepath
