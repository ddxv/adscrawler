from adscrawler.connection import get_db_connection
from adscrawler.config import get_logger
from adscrawler.scrape import crawl_app_ads
from adscrawler.app_stores.scrape_stores import (
    update_app_details,
    crawl_developers_for_new_store_ids,
    scrape_stores_frontpage,
)
import argparse
import os

logger = get_logger(__name__)

"""
    Top level script for managing getting app-ads.txt
"""


def script_has_process() -> bool:
    already_running = False
    processes = [x for x in os.popen("ps aux ww")]
    my_processes = [
        x for x in processes if "/adscrawler/main.py" in x and "/bin/sh" not in x
    ]
    if len(my_processes) > 1:
        logger.info(f"Already running {my_processes}")
        already_running = True
    return already_running


def manage_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--platforms",
        action="append",
        help="String as portion of android or ios",
        default=["android", "ios"],
    )
    parser.add_argument(
        "-t",
        "--use-ssh-tunnel",
        help="Use SSH tunnel with port fowarding to connect",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-l",
        "--limit-processes",
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
    parser.add_argument(
        "-u",
        "--update-app-store-details",
        help="Scrape app stores for app details, ie downloads",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-a",
        "--app-ads-txt-scrape",
        help="Scrape app stores for app details, ie downloads",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--no-limits",
        help="Run queries without limits",
        default=False,
        action="store_true",
    )
    args, leftovers = parser.parse_known_args()
    if args.limit_processes and script_has_process():
        logger.info("Script already running, exiting")
        quit()
    return args


def main(args) -> None:
    logger.info(f"Main starting with args: {args}")
    platforms = args.platforms if "args" in locals() else ["android", "ios"]
    new_apps_check = args.new_apps_check if "args" in locals() else False
    no_limits = args.no_limits if "args" in locals() else False
    update_app_store_details = (
        args.update_app_store_details if "args" in locals() else False
    )
    app_ads_txt_scrape = args.app_ads_txt_scrape if "args" in locals() else False
    stores = []
    stores.append(1) if "android" in platforms else None
    stores.append(2) if "ios" in platforms else None

    # Scrape Store for new apps
    if new_apps_check:
        scrape_stores_frontpage(database_connection=PGCON, stores=stores)
        crawl_developers_for_new_store_ids(database_connection=PGCON, store=2)

    # Update the app details
    if update_app_store_details:
        if no_limits:
            limit = None
        else:
            limit = 20000
        update_app_details(stores, PGCON, limit=limit)

    # Crawl developwer websites to check for app ads
    if app_ads_txt_scrape:
        if no_limits:
            limit = None
        else:
            limit = 5000
        crawl_app_ads(PGCON, limit=limit)
    logger.info("Adscrawler exiting main")


if __name__ == "__main__":
    logger.info("Starting app-ads.txt crawler")
    args = manage_cli_args()
    PGCON = get_db_connection(use_ssh_tunnel=args.use_ssh_tunnel)
    PGCON.set_engine()

    main(args)
