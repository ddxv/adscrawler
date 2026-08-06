"""Download IPA files.


CONFIG will expect apple: email, password
"""

import pathlib
import re
import subprocess
import time

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
        r = lookupby_id(app_id=store_id)
        bundle_id: str = r["bundleId"]
        time.sleep(1)
        downloaded_file_path = external_download(
            store_id=store_id, bundle_id=bundle_id, do_redownload=True
        )
        tmp_decoded_output_path = unzip_ipa(
            ipa_path=downloaded_file_path, store_id=store_id
        )
        version_str, _plist_str, _details_df = get_parsed_plist(
            tmp_decoded_output_path=tmp_decoded_output_path
        )
        if version_str is None:
            version_str = FAILED_VERSION_STR
            logger.error(f"APP HAS NO VERSION STR: {store_id=} {version_str=}")
        md5_hash = get_md5_hash(downloaded_file_path)
        crawl_result = 1
        logger.info(f"{store_id=} plist finished")
    except requests.exceptions.HTTPError:
        crawl_result = 3  # 404s etc
    except requests.exceptions.ConnectionError:
        crawl_result = 3  # 404s etc
    except (FileNotFoundError, RuntimeError):
        logger.exception(f"{store_id=} unable to unpack IPA or download failed")
        crawl_result = 2
    except Exception as e:
        logger.exception(f"Unexpected error for {store_id=}: {str(e)}")
        crawl_result = 3  # Unexpected errors
    if crawl_result in [3, 4]:
        error_count += 3
    if crawl_result in [2]:
        error_count += 1

    logger.info(f"{func_info} {crawl_result=} {md5_hash=} {version_str=}")

    return DownloadResult(
        crawl_result=crawl_result,
        version_str=version_str,
        md5_hash=md5_hash,
        downloaded_file_path=downloaded_file_path,
        error_count=error_count,
    )


IPATOOL_KNOWN_ERRORS = {
    re.compile(
        r"HTTP 204.*empty or non-plist body"
    ): "Apple returned empty response (HTTP 204) — try again later",
    re.compile(
        r"HTTP 503.*Service Temporarily Unavailable"
    ): "Apple service temporarily unavailable (HTTP 503)",
    re.compile(r"HTTP 4\d{2}"): "Apple returned a client error (HTTP 4xx)",
    re.compile(r"request failed"): "Generic ipatool request failure",
    re.compile(r"invalid.*credentials", re.IGNORECASE): "Invalid Apple ID credentials",
}


def _parse_ipatool_error(stderr: str, stdout: str) -> str | None:
    """Return a human-readable error message if a known ipatool failure is detected."""
    combined = stderr + "\n" + stdout
    for pattern, message in IPATOOL_KNOWN_ERRORS.items():
        if pattern.search(combined):
            return message
    return None


def _run_ipatool(command: str) -> subprocess.CompletedProcess:
    """Run an ipatool command via subprocess and log the outcome.

    Raises:
        RuntimeError: if the command exits non-zero and a known error is matched.
    """
    logger.debug(f"Running: {command}")
    result = subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    logger.info(f"ipatool exit code: {result.returncode}")

    if result.returncode != 0:
        error_msg = _parse_ipatool_error(result.stderr, result.stdout)
        if error_msg:
            logger.error(
                f"ipatool failed: {error_msg} (stderr: {result.stderr.strip()})"
            )
            raise RuntimeError(f"ipatool failed: {error_msg}")
        logger.error(
            f"ipatool non-zero exit: {result.returncode} (stderr: {result.stderr.strip()})"
        )
        raise RuntimeError(
            f"ipatool exited with code {result.returncode}: {result.stderr.strip()}"
        )
    return result


def ipatool_auth() -> None:
    command = (
        f"ipatool auth login --email {EMAIL} --password '{PASSWORD}' "
        f"--non-interactive --keychain-passphrase '{KEYCHAIN_PASSPHRASE}'"
    )
    try:
        _run_ipatool(command)
        logger.info("ipatool auth succeeded")
    except RuntimeError:
        logger.exception("ipatool auth failed")
        raise


def external_download(
    store_id: str, bundle_id: str, do_redownload: bool = False
) -> pathlib.Path:
    filepath = pathlib.Path(IPAS_INCOMING_DIR, f"{store_id}.ipa")
    exists = filepath.exists()
    if exists:
        if not do_redownload:
            logger.info(f"ipa already exists {filepath=}, skipping")
            return
    logger.info(f"Will download {bundle_id}")
    command = (
        f"ipatool download -b '{bundle_id}' -o {filepath.as_posix()} "
        f"--keychain-passphrase '{KEYCHAIN_PASSPHRASE}' "
        f"--non-interactive --purchase --verbose"
    )
    try:
        _run_ipatool(command)
    except RuntimeError:
        logger.exception(f"ipatool download failed for {bundle_id}")
        raise FileNotFoundError(f"ipatool download failed for {bundle_id}")
    if filepath.exists():
        return filepath
    else:
        raise FileNotFoundError(f"Failed to download {bundle_id}")
