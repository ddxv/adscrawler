import logging
import os
import pathlib
import sys
import tomllib
import typing
from logging import Formatter
from logging.handlers import RotatingFileHandler

HOME = pathlib.Path.home()
PROJECT_NAME = "adscrawler"
TOP_CONFIGDIR = pathlib.Path(HOME, pathlib.Path(".config"))
CONFIG_DIR = pathlib.Path(TOP_CONFIGDIR, pathlib.Path(PROJECT_NAME))
LOG_DIR = pathlib.Path(CONFIG_DIR, pathlib.Path("logs"))
MODULE_DIR = pathlib.Path(__file__).resolve().parent
PACKAGE_DIR = pathlib.Path(__file__).resolve().parent.parent

GEO_DATA_DIR = pathlib.Path(CONFIG_DIR, pathlib.Path("geo-data"))

# APK File dirs
APKS_FILES_DIR = pathlib.Path(HOME, "apk-files")


# Incoming for downloading/downloaded but not yet processed
INCOMING_DIR = pathlib.Path(APKS_FILES_DIR, "incoming")
APKS_INCOMING_DIR = pathlib.Path(INCOMING_DIR, "apks")
XAPKS_INCOMING_DIR = pathlib.Path(INCOMING_DIR, "xapks")

# Processed and ready for use
APKS_DIR = pathlib.Path(APKS_FILES_DIR, "apks")
XAPKS_DIR = pathlib.Path(APKS_FILES_DIR, "xapks")

# Failed to unzip, often due to unzip tool. I think failed during download?
XAPKS_ISSUES_DIR = pathlib.Path(APKS_FILES_DIR, "xapks-issues")
APKS_ISSUES_DIR = pathlib.Path(APKS_FILES_DIR, "apks-issues")

TMP_DIR = pathlib.Path("/tmp/adscrawler")

# The TMP partials dir has the base APK from an unzipped xapk.
# Careful, as the files look like APKs but are not installable on their own.
APK_TMP_PARTIALS_DIR = pathlib.Path(TMP_DIR, "apk-partials")
APK_TMP_UNZIPPED_DIR = pathlib.Path(TMP_DIR, "apks-unzipped")
XAPKS_TMP_UNZIP_DIR = pathlib.Path(TMP_DIR, "xapks-unzipped")

# Location of the Android SDK
ANDROID_SDK = pathlib.Path(HOME, "Android/Sdk/build-tools/35.0.0")

# Putting files in WAYDROID_MEDIA_DIR will put them in the internal emulated directory
WAYDROID_MEDIA_DIR = pathlib.Path(HOME, ".local/share/waydroid/data/media")

# This is not a directory on your filesystem, but the internal emulated directory
WAYDROID_INTERNAL_EMULATED_DIR = pathlib.Path("/data/media/")


@typing.no_type_check
def handle_exception(exc_type, exc_value, exc_traceback) -> None:
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


import traceback


@typing.no_type_check
def exc_handler(exctype, value, tb):
    if issubclass(exctype, KeyboardInterrupt):
        sys.__excepthook__(exctype, value, tb)
        return
    logger.exception("".join(traceback.format_exception(exctype, value, tb)))
    # logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


def check_dirs() -> None:
    dirs = [
        TOP_CONFIGDIR,
        CONFIG_DIR,
        LOG_DIR,
        APKS_FILES_DIR,
        TMP_DIR,
        INCOMING_DIR,
        APK_TMP_PARTIALS_DIR,
        APK_TMP_UNZIPPED_DIR,
        XAPKS_TMP_UNZIP_DIR,
        XAPKS_DIR,
        XAPKS_ISSUES_DIR,
        APKS_ISSUES_DIR,
        APKS_INCOMING_DIR,
        XAPKS_INCOMING_DIR,
        GEO_DATA_DIR,
    ]
    for _dir in dirs:
        if not pathlib.Path.exists(_dir):
            pathlib.Path.mkdir(_dir, exist_ok=True)


FORMATTER = Formatter(
    "%(asctime)s.%(msecs)03d | "
    "%(process)d | "
    "%(levelname)-5s | "
    "%(filename)s:%(lineno)d | "
    "%(message)s"
)


FORMATTER.datefmt = "%Y-%m-%d %H:%M:%S"


def get_logger(mod_name: str, sep_file: str | None = "main") -> logging.Logger:
    check_dirs()

    # Get or create logger
    logger = logging.getLogger(mod_name)

    # Set level
    logger.setLevel(logging.INFO)

    # Add file handler for individual log file
    indiv_handler = RotatingFileHandler(
        filename=os.path.join(LOG_DIR, f"{sep_file}.log"),
        maxBytes=50 * 1024 * 1024,
        backupCount=10,
    )
    indiv_handler.setFormatter(FORMATTER)
    logger.addHandler(indiv_handler)

    if sep_file != "main":
        root_logger = logging.getLogger(PROJECT_NAME)
        if not root_logger.handlers:
            # Add main file handler if it doesn't exist
            main_handler = RotatingFileHandler(
                filename=os.path.join(LOG_DIR, "main.log"),
                maxBytes=50 * 1024 * 1024,
                backupCount=10,
            )
            main_handler.setFormatter(FORMATTER)
            root_logger.addHandler(main_handler)

            # Add console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(FORMATTER)
            root_logger.addHandler(console_handler)

        # Ensure the root logger level is set to capture all messages
        root_logger.setLevel(logging.INFO)

        # Set propagate to True for non-main loggers
        logger.propagate = True
    else:
        # For the main logger (root logger), add console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(FORMATTER)
        logger.addHandler(console_handler)
        logger.propagate = False  # Main logger shouldn't propagate

    return logger


# Set global handling of uncaught exceptions
# sys.excepthook = handle_exception
sys.excepthook = exc_handler

logger = get_logger(__name__)

CONFIG_FILENAME = "config.toml"

CONFIG_FILE_PATH = pathlib.Path(CONFIG_DIR, CONFIG_FILENAME)
if not pathlib.Path.exists(CONFIG_FILE_PATH):
    error = f"Couldn't find {CONFIG_FILENAME} please add to {CONFIG_DIR}"
    logger.error(error)
    raise FileNotFoundError(error)


with open(CONFIG_FILE_PATH, "rb") as f:
    CONFIG = tomllib.load(f)


DATE_FORMAT = "%Y-%m-%d"


logger.info("Logger and Config loaded")


DEVLEOPER_IGNORE_TLDS = [
    "00webhostapp.com",
    "bitballoon.com",
    "blogger.com",
    "linkedin.com",
    "blogspot.com",
    "blogspot.co.id",
    "blogspot.in",
    "bytehost6.com",
    "facebook.com",
    "flycricket.io",
    "github.io",
    "github.com",
    "instagram.com",
    "netlify.com",
    "page.link",
    "site123.me",
    "simplesite.com",
    "subsplash.com",
    "twitter.com",
    "tumblr.com",
    "weebly.com",
    "wix.com",
    "wixsite.com",
    "wordpress.com",
    "youtube.com",
]
