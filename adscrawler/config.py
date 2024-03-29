import logging
import pathlib
import sys
import tomllib
import typing
from logging.handlers import RotatingFileHandler

HOME = pathlib.Path.home()
TOP_CONFIGDIR = pathlib.Path(HOME, pathlib.Path(".config"))
CONFIG_DIR = pathlib.Path(TOP_CONFIGDIR, pathlib.Path("adscrawler"))
LOG_DIR = pathlib.Path(CONFIG_DIR, pathlib.Path("logs"))
MODULE_DIR = pathlib.Path(__file__).resolve().parent.parent


@typing.no_type_check
def handle_exception(exc_type, exc_value, exc_traceback) -> None:
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


def check_config_dirs() -> None:
    dirs = [TOP_CONFIGDIR, CONFIG_DIR, LOG_DIR]
    for _dir in dirs:
        if not pathlib.Path.exists(_dir):
            pathlib.Path.mkdir(_dir, exist_ok=True)


def get_logger(mod_name: str, filename: str = "adscrawler") -> logging.Logger:
    log_format = "%(asctime)s: %(name)s: %(levelname)s: %(message)s"
    check_config_dirs()
    filename = f"{LOG_DIR}/{filename}.log"
    # Writes to file
    rotate_handler = RotatingFileHandler(
        filename=filename,
        maxBytes=50000000,
        backupCount=10,
    )
    # Stream handler for stdout
    logging.basicConfig(
        format=log_format,
        level=logging.INFO,
        handlers=[rotate_handler, logging.StreamHandler()],
    )
    logger = logging.getLogger(mod_name)
    return logger


# Set global handling of uncaught exceptions
sys.excepthook = handle_exception

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
