"""Download IPA files.


CONFIG will expect apple: email, password
"""

import os
import pathlib

from adscrawler.config import CONFIG, MODULE_DIR, get_logger

logger = get_logger(__name__)


# Replace with your email and password
EMAIL = CONFIG["apple"]["email"]
PASSWORD = CONFIG["apple"]["password"]
KEYCHAIN_PASSPHRASE = CONFIG["apple"]["keychain_passphrase"]


IPAS_DIR = pathlib.Path(MODULE_DIR, "ipas")


def check_ipa_dir_created() -> None:
    """Create if not exists for ipas directory."""
    dirs = [IPAS_DIR]
    for _dir in dirs:
        if not pathlib.Path.exists(_dir):
            logger.info("creating apks directory")
            pathlib.Path.mkdir(_dir, exist_ok=True)


def ipatool_auth() -> None:
    command = f"ipatool auth login --email {EMAIL} --password '{PASSWORD}' --non-interactive --keychain-passphrase '{KEYCHAIN_PASSPHRASE}'"
    result = os.system(command)
    logger.info(f"ipatool auth result: {result}")


def download(bundle_id: str, do_redownload: bool = False) -> None:
    check_ipa_dir_created()
    filepath = pathlib.Path(IPAS_DIR, f"{bundle_id}.ipa")
    exists = filepath.exists()
    if exists:
        if not do_redownload:
            logger.info(f"ipa already exists {filepath=}, skipping")
            return
    logger.info(f"Will download {bundle_id}")
    command = f"ipatool download -b '{bundle_id}' -o ipas/{bundle_id}.ipa --keychain-passphrase '{KEYCHAIN_PASSPHRASE}' --non-interactive --purchase"
    result = os.system(command)
    logger.info(f"ipatool download result: {result}")
