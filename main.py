import argparse
import logging
import os
import sys

from adscrawler.app_stores.scrape_stores import (
    crawl_developers_for_new_store_ids,
    scrape_store_ranks,
    update_app_details,
)
from adscrawler.connection import PostgresCon, get_db_connection
from adscrawler.scrape import crawl_app_ads
from adscrawler.tools.get_manifest import manifest_main
from adscrawler.tools.get_plist import plist_main

logger = logging.getLogger(__name__)


class ProcessManager:
    def __init__(self) -> None:
        self.args: argparse.Namespace = self.parse_arguments()
        self.pgcon: PostgresCon

    def parse_arguments(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-p",
            "--platforms",
            action="append",
            help="String of google and/or apple",
            default=[],
        )
        parser.add_argument(
            "-t",
            "--use-ssh-tunnel",
            help="Use SSH tunnel with port fowarding to connect",
            action="store_true",
        )
        parser.add_argument(
            "-l",
            "--limit-processes",
            help="If included prevent running if script already running",
            action="store_true",
        )
        parser.add_argument(
            "-n",
            "--new-apps-check",
            help="Scrape the iTunes and Play Store front pages to find new apps",
            action="store_true",
        )
        parser.add_argument(
            "-d",
            "--new-apps-check-devs",
            help="Scrape devs for new apps",
            action="store_true",
        )
        parser.add_argument(
            "-m",
            "--manifests",
            help="Scrape the iTunes and Play Store front pages to find new apps",
            action="store_true",
        )
        parser.add_argument(
            "-u",
            "--update-app-store-details",
            help="Scrape app stores for app details, ie downloads",
            action="store_true",
        )
        parser.add_argument(
            "-g",
            "--update-app-store-details-group",
            default="",
            type=str,
            help="Interval group to update, string of short or long, if left blank updates all",
        )
        parser.add_argument(
            "-a",
            "--app-ads-txt-scrape",
            help="Scrape app stores for app details, ie downloads",
            action="store_true",
        )
        parser.add_argument(
            "--no-limits", help="Run queries without limits", action="store_true"
        )

        args, _ = parser.parse_known_args()
        return args

    def get_running_processes(self) -> list[str]:
        return [x for x in os.popen("ps aux ww")]

    def filter_processes(self, processes: list[str], filter_string: str) -> list[str]:
        return [x for x in processes if filter_string in x and "/bin/sh" not in x]

    def check_app_update_processes(self) -> bool:
        processes = self.get_running_processes()
        my_processes = self.filter_processes(processes, "/adscrawler/main.py")
        app_update_processes = [
            x
            for x in my_processes
            if any([" -u " in x, " --update-app-store-details" in x])
        ]

        if self.args.platforms:
            app_update_processes = [
                x
                for x in app_update_processes
                if any(p in x for p in self.args.platforms)
            ]

        if self.args.update_app_store_details_group:
            app_update_processes = [
                x
                for x in app_update_processes
                if self.args.update_app_store_details_group in x
            ]

        return len(app_update_processes) > 1

    def check_apk_download_processes(self) -> bool:
        processes = self.get_running_processes()
        my_processes = self.filter_processes(processes, "/adscrawler/main.py")
        apk_download_processes = [
            x for x in my_processes if any([" -m" in x, " --manifests" in x])
        ]
        if self.args.platforms:
            apk_download_processes = [
                x
                for x in apk_download_processes
                if any(p in x for p in self.args.platforms)
            ]

        if self.args.update_app_store_details_group:
            apk_download_processes = [
                x
                for x in apk_download_processes
                if self.args.update_app_store_details_group in x
            ]
        return len(apk_download_processes) > 1

    def check_ads_txt_download_processes(self) -> bool:
        processes = self.get_running_processes()
        my_processes = self.filter_processes(processes, "/adscrawler/main.py")
        download_processes = [
            x for x in my_processes if any([" -a" in x, " --app-ads-txt-scrape" in x])
        ]
        return len(download_processes) > 1

    def is_script_already_running(self) -> bool:
        if self.args.update_app_store_details:
            return self.check_app_update_processes()
        elif self.args.manifests:
            return self.check_apk_download_processes()
        elif self.args.app_ads_txt_scrape:
            return self.check_ads_txt_download_processes()
        return False

    def setup_database_connection(self) -> None:
        self.pgcon = get_db_connection(use_ssh_tunnel=self.args.use_ssh_tunnel)
        self.pgcon.set_engine()

    def main(self) -> None:
        logger.info(f"Main starting with args: {self.args}")
        platforms: list[str] = self.args.platforms or ["google", "apple"]
        _stores: list[int | None] = [
            1 if "google" in platforms else None,
            2 if "apple" in platforms else None,
        ]
        stores: list[int] = [x for x in _stores if x]
        stores = [s for s in stores if s is not None]

        if self.args.new_apps_check:
            self.scrape_new_apps(stores)

        if self.args.new_apps_check_devs:
            self.scrape_new_apps_devs(stores)

        if self.args.update_app_store_details:
            self.update_app_details(stores)

        if self.args.app_ads_txt_scrape:
            self.crawl_app_ads()

        if self.args.manifests:
            self.scrape_manifests(stores)

        logger.info("Adscrawler exiting main")

    def scrape_new_apps(self, stores: list[int]) -> None:
        try:
            scrape_store_ranks(database_connection=self.pgcon, stores=stores)
        except Exception:
            logger.exception("Crawling front pages failed")

    def scrape_new_apps_devs(self, stores: list[int]) -> None:
        for store in stores:
            try:
                crawl_developers_for_new_store_ids(
                    database_connection=self.pgcon, store=store
                )
            except Exception:
                logger.exception(f"Crawling developers for {store=} failed")

    def update_app_details(self, stores: list[int]) -> None:
        limit: int | None = None if self.args.no_limits else 5000
        update_app_details(
            stores,
            self.pgcon,
            group=self.args.update_app_store_details_group,
            limit=limit,
        )

    def crawl_app_ads(self) -> None:
        limit: int | None = None if self.args.no_limits else 5000
        crawl_app_ads(self.pgcon, limit=limit)

    def scrape_manifests(self, stores: list[int]) -> None:
        if 2 in stores:
            try:
                plist_main(database_connection=self.pgcon, number_of_apps_to_pull=20)
            except Exception:
                logger.exception("iTunes scrape plist failing")
        if 1 in stores:
            try:
                manifest_main(database_connection=self.pgcon, number_of_apps_to_pull=20)
            except Exception:
                logger.exception("Android scrape manifests failing")

    def run(self) -> None:
        if self.args.limit_processes and self.is_script_already_running():
            logger.info("Script already running, exiting")
            sys.exit()

        self.setup_database_connection()
        self.main()


if __name__ == "__main__":
    logger.info("Starting app-ads.txt crawler")
    process_manager = ProcessManager()
    process_manager.run()
