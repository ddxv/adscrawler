from adscrawler.connection import get_db_connection
from adscrawler.config import get_logger
from adscrawler.scrape import (
    crawl_app_ads,
    scrape_ios_frontpage,
    scrape_gp_for_app_ids,
    update_app_details,
)
import argparse
import os

logger = get_logger(__name__)

"""
    Top level script for managing getting app-ads.txt
"""


def script_has_process() -> bool:
    already_running = False
    processes = [x for x in os.popen("ps aux")]
    my_processes = [
        x for x in processes if "/adscrawler/main.py" in x and "/bin/sh" not in x
    ]
    if len(my_processes) > 1:
        logger.warning(f"Already running {my_processes}")
        already_running = True
    return already_running


def manage_cli_args() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--platforms",
        action="append",
        help="String as portion of android or ios",
        default=["android", "ios"],
    )
    parser.add_argument(
        "-l",
        "--is-local-db",
        help="Connect to local db on port 5432",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-s",
        "--single-run-check",
        help="If included prevent running if script already running",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-n",
        "--new-apps-check",
        help="Scrape the iTunes and Play Store front pages to find new apps",
        default=False,
        action="store_true",
    )
    args, leftovers = parser.parse_known_args()

    if args.single_run_check and script_has_process():
        logger.warning("Script already running, exiting")
        quit()
    return args


def main(args) -> None:
    logger.info(f"Main starting with args: {args}")
    platforms = args.platforms if "args" in locals() else ["android", "ios"]
    new_apps_check = args.new_apps_check if "args" in locals() else False
    stores = []
    stores.append(1) if "android" in platforms else None
    stores.append(2) if "ios" in platforms else None

    # Scrape Store for new apps
    if new_apps_check:
        if 1 in stores:
            scrape_ios_frontpage(PGCON)
        if 2 in stores:
            scrape_gp_for_app_ids(PGCON)

    # Update the app details
    update_app_details(stores, PGCON)

    # Crawl developwer websites to check for app ads
    crawl_app_ads(PGCON)


if __name__ == "__main__":
    logger.info("Starting app-ads.txt crawler")
    args = manage_cli_args()
    PGCON = get_db_connection(args.is_local_db)
    PGCON.set_engine()

    main(args)
