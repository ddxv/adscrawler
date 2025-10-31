import argparse
import datetime
import os
import sys

from adscrawler.app_stores.process_from_s3 import (
    import_app_metrics_from_s3,
    import_ranks_from_s3,
)
from adscrawler.app_stores.scrape_stores import (
    crawl_developers_for_new_store_ids,
    crawl_keyword_cranks,
    scrape_store_ranks,
    update_app_details,
)
from adscrawler.config import get_logger
from adscrawler.dbcon.connection import PostgresCon, get_db_connection
from adscrawler.mitm_ad_parser.mitm_scrape_ads import (
    parse_store_id_mitm_log,
    scan_all_apps,
)
from adscrawler.packages.apks.waydroid import (
    manual_waydroid_process,
    process_apks_for_waydroid,
)
from adscrawler.packages.process_files import (
    download_apps,
    manual_download_app,
    process_sdks,
)
from adscrawler.scrape import crawl_app_ads
from adscrawler.tools.geo import update_geo_dbs

logger = get_logger(__name__)

STORES_MAP = {"google": 1, "apple": 2}


class ProcessManager:
    def __init__(self) -> None:
        self.args: argparse.Namespace = self.parse_arguments()
        self.pgcon: PostgresCon

    def parse_arguments(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-p",
            "--platform",
            type=str,
            help="String of google or apple",
            default=None,
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
        parser.add_argument("--limit-query-rows", type=int, default=200_000)
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
            "--download-apks",
            help="Download apk files",
            action="store_true",
        )
        parser.add_argument(
            "--process-sdks",
            help="Process APKs, IPAS, and manifest files for sdks",
            action="store_true",
        )
        parser.add_argument(
            "-w",
            "--waydroid",
            help="Run waydroid app",
            action="store_true",
        )
        parser.add_argument(
            "-u",
            "--update-app-store-details",
            help="Scrape app stores for app details, ie downloads",
            action="store_true",
        )
        parser.add_argument(
            "--process-icons",
            help="When running update app store details, resize and upload app icon to S3",
            action="store_true",
        )
        parser.add_argument(
            "--workers",
            default="1",
            type=str,
            help="Number of workers to use for updating app store details",
        )
        parser.add_argument(
            "--country-priority-group",
            help="Country priority group to use when updating app store details",
            type=int,
        )
        parser.add_argument(
            "-a",
            "--app-ads-txt-scrape",
            help="Scrape app stores for app details, ie downloads",
            action="store_true",
        )
        parser.add_argument(
            "-k",
            "--crawl-keywords",
            help="Crawl keywords",
            action="store_true",
        )
        parser.add_argument(
            "-s",
            "--store-id",
            help="String of store id to launch in waydroid",
        )
        ### OPTIONS FOR CREATIVE SCAN
        parser.add_argument(
            "--creative-scan-all-apps",
            help="Scan apps for creatives from MITM logs",
            action="store_true",
        )
        parser.add_argument(
            "--creative-scan-new-apps",
            help="Only scan new apps (requires --creative-scan-all-apps)",
            action="store_true",
        )
        parser.add_argument(
            "--creative-scan-recent-months",
            help="Only scan apps from last two months (requires --creative-scan-all-apps)",
            action="store_true",
        )
        parser.add_argument(
            "--creative-scan-single-app",
            help="Scan a single app for creatives, requires --store-id and --run-id",
            action="store_true",
        )
        parser.add_argument(
            "--run-id",
            help="String of run id to scan for creatives",
        )
        ### OPTIONS FOR IMPORT RANKS FROM S3
        parser.add_argument(
            "--daily-s3-imports",
            help="Import app ranks and metrics from s3",
            action="store_true",
        )
        parser.add_argument(
            "--ranks-period",
            help="Period to group imported ranks from s3, default week, options are week or day",
            type=str,
            default="week",
        )
        ### OPTIONS FOR MANUAL/LOCAL WAYDROID PROCESSING
        parser.add_argument(
            "--redownload-geo-dbs",
            help="Redownload geo dbs",
            action="store_true",
        )
        parser.add_argument(
            "--timeout-waydroid",
            help="timeout in seconds for waydroid app to run",
            type=int,
            default=180,
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
        found_processes = [
            x
            for x in my_processes
            if any([" -u " in x, " --update-app-store-details" in x])
        ]

        found_processes = [
            x
            for x in found_processes
            if any(
                [
                    f"--country-priority-group={self.args.country_priority_group}" in x,
                    f"--country-priority-group {self.args.country_priority_group}" in x,
                ]
            )
        ]

        if self.args.platform:
            found_processes = [x for x in found_processes if self.args.platform in x]

        return len(found_processes) > 1

    def check_apk_download_processes(self) -> bool:
        processes = self.get_running_processes()
        my_processes = self.filter_processes(processes, "/adscrawler/main.py")
        found_processes = [x for x in my_processes if any([" --download-apks" in x])]
        if self.args.platform:
            found_processes = [x for x in found_processes if self.args.platform in x]

        return len(found_processes) > 1

    def check_process_sdks_processes(self) -> bool:
        processes = self.get_running_processes()
        my_processes = self.filter_processes(processes, "/adscrawler/main.py")
        found_processes = [x for x in my_processes if any([" --process-sdks" in x])]
        if self.args.platform:
            found_processes = [x for x in found_processes if self.args.platform in x]

        return len(found_processes) > 1

    def check_waydroid_processes(self) -> bool:
        processes = self.get_running_processes()
        my_processes = self.filter_processes(processes, "/adscrawler/main.py")
        found_processes = [
            x for x in my_processes if any([" -w" in x, " --waydroid" in x])
        ]
        if self.args.platform:
            found_processes = [x for x in found_processes if self.args.platform in x]

        return len(found_processes) > 1

    def check_ads_txt_download_processes(self) -> bool:
        processes = self.get_running_processes()
        my_processes = self.filter_processes(processes, "/adscrawler/main.py")
        download_processes = [
            x for x in my_processes if any([" -a" in x, " --app-ads-txt-scrape" in x])
        ]
        return len(download_processes) > 1

    def check_crawl_keywords_processes(self) -> bool:
        processes = self.get_running_processes()
        my_processes = self.filter_processes(processes, "/adscrawler/main.py")
        crawl_processes = [
            x for x in my_processes if any([" -k" in x, " --crawl-keywords" in x])
        ]
        return len(crawl_processes) > 1

    def is_script_already_running(self) -> bool:
        if self.args.update_app_store_details:
            return self.check_app_update_processes()
        elif self.args.download_apks:
            return self.check_apk_download_processes()
        elif self.args.process_sdks:
            return self.check_process_sdks_processes()
        elif self.args.waydroid:
            return self.check_waydroid_processes()
        elif self.args.app_ads_txt_scrape:
            return self.check_ads_txt_download_processes()
        elif self.args.crawl_keywords:
            return self.check_crawl_keywords_processes()
        return False

    def setup_database_connection(self) -> None:
        self.pgcon = get_db_connection(use_ssh_tunnel=self.args.use_ssh_tunnel)

    def main(self) -> None:
        logger.info(f"Main starting with args: {self.args}")
        platform: str = self.args.platform or None

        store = STORES_MAP.get(platform) if platform else None

        if self.args.new_apps_check:
            self.scrape_new_apps(store)

        if self.args.daily_s3_imports:
            self.daily_s3_imports()

        if self.args.new_apps_check_devs:
            self.scrape_new_apps_devs(store)

        if self.args.update_app_store_details:
            self.update_app_details(store, self.args.country_priority_group)

        if self.args.app_ads_txt_scrape:
            self.crawl_app_ads()

        if self.args.download_apks:
            self.download_apks(store)

        if self.args.process_sdks:
            self.process_sdks(store)

        if self.args.waydroid:
            self.waydroid_mitm()

        if self.args.creative_scan_all_apps:
            self.creative_scan_all_apps()

        if self.args.creative_scan_single_app:
            self.creative_scan_single_app()

        if self.args.crawl_keywords:
            self.crawl_keywords()

        logger.info("Adscrawler exiting main")

    def daily_s3_imports(self) -> None:
        period = self.args.ranks_period
        if period == "week":
            start_date = datetime.date.today() - datetime.timedelta(days=8)
            end_date = datetime.date.today()
        elif period == "day":
            start_date = datetime.date.today() - datetime.timedelta(days=3)
            end_date = datetime.date.today()
        else:
            raise ValueError(f"Invalid period {period}")
        try:
            import_ranks_from_s3(
                database_connection=self.pgcon,
                start_date=start_date,
                end_date=end_date,
                period=period,
            )
        except Exception:
            logger.exception(
                f"Importing {self.args.ranks_period} ranks from s3 for failed"
            )
        try:
            start_date = datetime.date.today() - datetime.timedelta(days=3)
            end_date = datetime.date.today() - datetime.timedelta(days=1)
            import_app_metrics_from_s3(
                database_connection=self.pgcon,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception:
            logger.exception("Importing app metrics from s3 for failed")

    def scrape_new_apps(self, store: int) -> None:
        try:
            scrape_store_ranks(database_connection=self.pgcon, store=store)
        except Exception:
            logger.exception("Crawling front pages failed")

    def scrape_new_apps_devs(self, store: int) -> None:
        try:
            crawl_developers_for_new_store_ids(
                database_connection=self.pgcon, store=store
            )
        except Exception:
            logger.exception(f"Crawling developers for {store=} failed")

    def update_app_details(self, store: int, country_priority_group: int) -> None:
        if not country_priority_group:
            logger.error(
                "No country priority group provided, ie country-priority-group=1"
            )
            return
        update_app_details(
            store=store,
            database_connection=self.pgcon,
            use_ssh_tunnel=self.args.use_ssh_tunnel,
            workers=int(self.args.workers),
            process_icon=self.args.process_icons,
            country_crawl_priority=self.args.country_priority_group,
            limit=self.args.limit_query_rows,
        )

    def crawl_app_ads(self) -> None:
        crawl_app_ads(self.pgcon, limit=self.args.limit_query_rows)

    def download_apks(self, store: int) -> None:
        if self.args.store_id:
            # For manually processing a single app
            manual_download_app(
                database_connection=self.pgcon,
                store_id=self.args.store_id,
                store=1,
            )
            return
        try:
            download_apps(
                store=store, database_connection=self.pgcon, number_of_apps_to_pull=30
            )
        except Exception:
            logger.exception(f"Download app/decompile failing {store=}")

    def process_sdks(self, store: int) -> None:
        process_sdks(
            store=store, database_connection=self.pgcon, number_of_apps_to_pull=20
        )

    def creative_scan_all_apps(self) -> None:
        scan_all_apps(
            database_connection=self.pgcon,
            only_new_apps=self.args.creative_scan_new_apps,
            recent_months=self.args.creative_scan_recent_months,
            use_ssh_tunnel=self.args.use_ssh_tunnel,
            max_workers=int(self.args.workers),
        )

    def creative_scan_single_app(self) -> None:
        error_messages = parse_store_id_mitm_log(
            database_connection=self.pgcon,
            pub_store_id=self.args.store_id,
            run_id=self.args.run_id,
        )
        logger.info(f"Error messages: {error_messages}")

    def waydroid_mitm(self) -> None:
        if self.args.redownload_geo_dbs:
            update_geo_dbs(redownload=self.args.redownload_geo_dbs)
            return
        if self.args.store_id:
            # Manual waydroid process, launches app for 5 minutes for user to interact
            store_id = self.args.store_id
            store_id = self.args.store_id if self.args.store_id else None
            timeout = self.args.timeout_waydroid
            run_name = "manual"
            manual_waydroid_process(
                database_connection=self.pgcon,
                store_id=store_id,
                timeout=timeout,
                run_name=run_name,
            )
        else:
            # Default processing of apk/xapk files that need to be processed
            try:
                process_apks_for_waydroid(database_connection=self.pgcon)
            except Exception:
                logger.exception("Process APKs with Waydroid failed")

    def crawl_keywords(self) -> None:
        crawl_keyword_cranks(database_connection=self.pgcon)

    def run(self) -> None:
        if self.args.limit_processes and self.is_script_already_running():
            logger.info("Script already running, exiting")
            sys.exit()

        self.setup_database_connection()
        self.main()


if __name__ == "__main__":
    process_manager = ProcessManager()
    process_manager.run()
