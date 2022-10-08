from adscrawler.connection import get_db_connection
from adscrawler.config import get_logger
from adscrawler.scrape import (
    crawl_app_ads,
    scrape_ios_frontpage,
    scrape_gp_for_app_ids,
    update_app_details,
    crawl_developers_for_new_store_ids,
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
    args, leftovers = parser.parse_known_args()
    if args.limit_processes and script_has_process():
        logger.info("Script already running, exiting")
        quit()
    return args


def main(args) -> None:
    logger.info(f"Main starting with args: {args}")
    platforms = args.platforms if "args" in locals() else ["android", "ios"]
    new_apps_check = args.new_apps_check if "args" in locals() else False
    update_app_store_details = (
        args.update_app_store_details if "args" in locals() else False
    )
    app_ads_txt_scrape = args.app_ads_txt_scrape if "args" in locals() else False
    stores = []
    stores.append(1) if "android" in platforms else None
    stores.append(2) if "ios" in platforms else None

    # Scrape Store for new apps
    if new_apps_check:
        if 1 in stores:
            scrape_ios_frontpage(PGCON, collection_keyword="NEW")
        if 2 in stores:
            scrape_gp_for_app_ids(PGCON)
            crawl_developers_for_new_store_ids(database_connection=PGCON, store=2)

    # Update the app details
    if update_app_store_details:
        update_app_details(stores, PGCON, limit=20000)

    # Crawl developwer websites to check for app ads
    if app_ads_txt_scrape:
        crawl_app_ads(PGCON, limit=5000)


if __name__ == "__main__":
    logger.info("Starting app-ads.txt crawler")
    args = manage_cli_args()
    PGCON = get_db_connection(use_ssh_tunnel=args.use_ssh_tunnel)
    PGCON.set_engine()

    main(args)
