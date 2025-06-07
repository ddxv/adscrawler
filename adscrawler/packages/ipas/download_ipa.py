"""Download IPA files.


CONFIG will expect apple: email, password
"""

import os
import pathlib

import requests

from adscrawler.app_stores.apple import lookupby_id
from adscrawler.config import CONFIG, IPAS_INCOMING_DIR, get_logger
from adscrawler.packages.ipas.get_plist import get_parsed_plist
from adscrawler.packages.models import DownloadResult
from adscrawler.packages.utils import get_md5_hash, unzip_ipa

logger = get_logger(__name__, "download_ipa")


# Replace with your email and password
EMAIL = CONFIG["apple"]["email"]
PASSWORD = CONFIG["apple"]["password"]
KEYCHAIN_PASSPHRASE = CONFIG["apple"]["keychain_passphrase"]

FAILED_VERSION_STR = "-1"


def manage_ipa_download(
    store_id: str,
    existing_local_file_path: pathlib.Path | None,
) -> DownloadResult:
    func_info = f"manage_ipa_download {store_id=}"
    logger.info(f"{func_info} start")
    ipatool_auth()
    error_count = 0
    crawl_result = 4
    version_str = FAILED_VERSION_STR
    md5_hash = None
    downloaded_file_path = None

    try:
        if existing_local_file_path:
            downloaded_file_path = existing_local_file_path
        else:
            r = lookupby_id(app_id=store_id)
            bundle_id: str = r["bundleId"]
            downloaded_file_path = external_download(
                bundle_id=bundle_id, do_redownload=False
            )
        tmp_decoded_output_path = unzip_ipa(
            ipa_path=downloaded_file_path, store_id=store_id
        )
        version_str, _plist_str, _details_df = get_parsed_plist(
            tmp_decoded_output_path=tmp_decoded_output_path
        )
        md5_hash = get_md5_hash(downloaded_file_path)
        crawl_result = 1
        logger.info(f"{store_id=} plist finished")
    except requests.exceptions.HTTPError:
        crawl_result = 3  # 404s etc
    except requests.exceptions.ConnectionError:
        crawl_result = 3  # 404s etc
    except FileNotFoundError:
        logger.exception(f"{store_id=} unable to unpack IPA or unpack failed")
        crawl_result = 2
    except Exception as e:
        logger.exception(f"Unexpected error for {store_id=}: {str(e)}")
        crawl_result = 3  # Unexpected errors
    if crawl_result in [3, 4]:
        error_count += 3
    if crawl_result in [2]:
        error_count += 1

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


def ipatool_auth() -> None:
    command = f"ipatool auth login --email {EMAIL} --password '{PASSWORD}' --non-interactive --keychain-passphrase '{KEYCHAIN_PASSPHRASE}'"
    print(command)
    result = os.system(command)
    logger.info(f"ipatool auth result: {result}")


def external_download(bundle_id: str, do_redownload: bool = False) -> pathlib.Path:
    filepath = pathlib.Path(IPAS_INCOMING_DIR, f"{bundle_id}.ipa")
    exists = filepath.exists()
    if exists:
        if not do_redownload:
            logger.info(f"ipa already exists {filepath=}, skipping")
            return
    logger.info(f"Will download {bundle_id}")
    command = f"ipatool download -b '{bundle_id}' -o  {filepath.as_posix()} --keychain-passphrase '{KEYCHAIN_PASSPHRASE}' --non-interactive --purchase --verbose >> ~/.config/adscrawler/logs/ipatool.log 2>&1"
    result = os.system(command)
    logger.info(f"ipatool download result: {result}")
    if filepath.exists():
        return filepath
    else:
        raise FileNotFoundError(f"Failed to download {bundle_id}")
