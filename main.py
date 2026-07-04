import argparse
import datetime
import os
import sys

from adscrawler.process.app_details import (
    import_app_details_from_s3_into_db,
    import_keywords_from_s3,
)
from adscrawler.process.app_rankings import import_ranks_from_s3
from adscrawler.process.app_domain_history import (
    process_company_history,
)
from adscrawler.process.app_metrics_history import (
    clean_history_tables,
    delete_and_aggregate_s3_agg,
)
from adscrawler.app_stores.process_keywords import process_app_keywords
from adscrawler.app_stores.scrape_stores import (
    crawl_developers_for_new_store_ids,
    crawl_keyword_ranks,
    scrape_store_ranks,
    update_app_details,
)
from adscrawler.config import get_logger
from adscrawler.dbcon.connection import PostgresEngine, get_db_connection
from adscrawler.packages.process_files import (
    download_apps,
    manual_download_app,
    process_sdks,
)
from adscrawler.scrape import crawl_app_ads
from adscrawler.tools.get_company_logos import refresh_metadata

logger = get_logger(__name__)

STORES_MAP = {"google": 1, "apple": 2}


class ProcessManager:
    def __init__(self) -> None:
        self.args: argparse.Namespace = self.parse_arguments()
        self.pgcon: PostgresEngine

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
        (
            parser.add_argument(
                "--dispatch-all",
                help="Dispatch all 4 queue combinations (google/apple × group 1/2) in a single run",
                action="store_true",
            ),
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
            "--process-company-history",
            help="Export combined domain-app history and run change-detection for all quarters",
            action="store_true",
        )
        parser.add_argument(
            "--extract-app-keywords",
            help="Extract app keywords from database descriptions",
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
        parser.add_argument(
            "--run-name",
            help="Run name to use for Waydroid processing",
            type=str,
            default="regular",
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
            "--refresh-metadata",
            help="Refresh company metadata (logos, LinkedIn, country, GitHub)",
            action="store_true",
        )
        parser.add_argument(
            "--refresh-metadata-all",
            help="Refresh ALL company metadata (not just missing)",
            action="store_true",
        )
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

    def check_extract_app_keywords_processes(self) -> bool:
        processes = self.get_running_processes()
        my_processes = self.filter_processes(processes, "/adscrawler/main.py")
        extract_processes = [
            x for x in my_processes if any([" --extract-app-keywords" in x])
        ]
        return len(extract_processes) > 1

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
        elif self.args.extract_app_keywords:
            return self.check_extract_app_keywords_processes()
        return False

    def setup_pgdb(self) -> None:
        self.pgcon = get_db_connection()

    def main(self) -> None:
        logger.info(f"Main starting with args: {self.args}")
        platform: str = self.args.platform or None
        country_priority_group: int = self.args.country_priority_group or None

        store = STORES_MAP.get(platform) if platform else None

        if self.args.new_apps_check:
            self.scrape_new_apps(store)

        if self.args.daily_s3_imports:
            self.daily_s3_imports()

        if self.args.new_apps_check_devs:
            self.scrape_new_apps_devs(store)

        if self.args.update_app_store_details:
            self.update_app_details(store, country_priority_group)

        if self.args.app_ads_txt_scrape:
            self.crawl_app_ads()

        if self.args.download_apks:
            self.download_apks(store)

        if self.args.process_sdks:
            self.process_sdks(store)

        if self.args.refresh_metadata:
            self.refresh_metadata()

        if self.args.refresh_metadata_all:
            self.refresh_metadata_all()

        if self.args.waydroid:
            self.waydroid_mitm()

        if self.args.creative_scan_all_apps:
            self.creative_scan_all_apps()

        if self.args.creative_scan_single_app:
            self.creative_scan_single_app()

        if self.args.process_company_history:
            self.process_company_history()

        if self.args.crawl_keywords:
            self.crawl_keywords()

        if self.args.extract_app_keywords:
            self.extract_app_keywords()

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
                pgdb=self.pgcon,
                start_date=start_date,
                end_date=end_date,
                period=period,
            )
        except Exception:
            logger.exception(
                f"Importing {self.args.ranks_period} ranks from s3 for failed"
            )
        for store in [1, 2]:
            try:
                delete_and_aggregate_s3_agg(store=store, pgdb=self.pgcon)
            except Exception:
                logger.exception(f"Importing {store=} app metrics from s3 for failed")
        try:
            clean_history_tables(pgdb=self.pgcon)
        except Exception:
            logger.exception("Cleaning history tables failed")
        try:
            start_date = datetime.date.today() - datetime.timedelta(days=3)
            end_date = datetime.date.today() - datetime.timedelta(days=1)
            import_keywords_from_s3(
                pgdb=self.pgcon,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception:
            logger.exception("Importing keywords from s3 for failed")
        for store in [1, 2]:
            for crawled_date in [
                (datetime.date.today() - datetime.timedelta(days=1)).isoformat(),
                datetime.date.today().isoformat(),
            ]:
                try:
                    import_app_details_from_s3_into_db(
                        store=store,
                        crawled_date=crawled_date,
                        pgdb=self.pgcon,
                    )
                except Exception:
                    logger.exception(
                        f"Importing app_details from s3 failed {store=} {crawled_date=}"
                    )
        try:
            process_company_history(pgdb=self.pgcon)
        except Exception:
            logger.exception("Exporting combined domain history to s3 failed")

    def scrape_new_apps(self, store: int) -> None:
        try:
            scrape_store_ranks(pgdb=self.pgcon, store=store)
        except Exception:
            logger.exception("Crawling front pages failed")

    def process_company_history(self) -> None:
        process_company_history(pgdb=self.pgcon)

    def scrape_new_apps_devs(self, store: int) -> None:
        try:
            crawl_developers_for_new_store_ids(pgdb=self.pgcon, store=store)
        except Exception:
            logger.exception(f"Crawling developers for {store=} failed")

    def update_app_details(self, store: int, country_priority_group: int) -> None:
        if not country_priority_group and not self.args.dispatch_all:
            logger.error(
                "No country priority group provided, ie --country-priority-group=1"
            )
            return

        if self.args.dispatch_all:
            from adscrawler.dramatiq.dispatcher import (
                dispatch_all_queues,  # noqa: PLC0415
            )

            logger.info("Using Dramatiq dispatcher for ALL queues")
            dispatch_all_queues(
                pgdb=self.pgcon,
                process_icon=self.args.process_icons,
                limit=self.args.limit_query_rows,
            )
            return

        if self.args.dispatch:
            from adscrawler.dramatiq.dispatcher import (
                dispatch_app_details_jobs,  # noqa: PLC0415
            )

            logger.info(
                f"Using Dramatiq dispatcher for {store=} group={country_priority_group}"
            )
            dispatch_app_details_jobs(
                pgdb=self.pgcon,
                store=store,
                process_icon=self.args.process_icons,
                app_limit=self.args.limit_query_rows,
                country_priority_group=country_priority_group,
            )
            return

        update_app_details(
            store=store,
            pgdb=self.pgcon,
            workers=int(self.args.workers),
            process_icon=self.args.process_icons,
            country_priority_group=self.args.country_priority_group,
            limit=self.args.limit_query_rows,
        )

    def crawl_app_ads(self) -> None:
        crawl_app_ads(self.pgcon, limit=self.args.limit_query_rows)

    def download_apks(self, store: int) -> None:
        if self.args.store_id:
            # For manually processing a single app
            manual_download_app(
                pgdb=self.pgcon,
                store_id=self.args.store_id,
                store=1,
            )
            return
        try:
            download_apps(store=store, pgdb=self.pgcon, number_of_apps_to_pull=50)
        except Exception:
            logger.exception(f"Download app/decompile failing {store=}")

    def process_sdks(self, store: int) -> None:
        process_sdks(store=store, pgdb=self.pgcon, number_of_apps_to_pull=20)

    def creative_scan_all_apps(self) -> None:
        from adscrawler.mitm_ad_parser.mitm_scrape_ads import (  # noqa: PLC0415
            scan_all_apps,
        )

        scan_all_apps(
            pgdb=self.pgcon,
            only_new_apps=self.args.creative_scan_new_apps,
            recent_months=self.args.creative_scan_recent_months,
            max_workers=int(self.args.workers),
        )

    def creative_scan_single_app(self) -> None:
        from adscrawler.mitm_ad_parser.mitm_scrape_ads import (  # noqa: PLC0415
            parse_store_id_mitm_log,
        )

        error_messages = parse_store_id_mitm_log(
            pgdb=self.pgcon,
            pub_store_id=self.args.store_id,
            run_id=self.args.run_id,
        )
        logger.info(f"Error messages: {error_messages}")

    def refresh_metadata(self) -> None:
        refresh_metadata(missing_only=True)

    def refresh_metadata_all(self) -> None:
        refresh_metadata(missing_only=False)

    def waydroid_mitm(self) -> None:
        from adscrawler.packages.apks.waydroid import (  # noqa: PLC0415
            manual_waydroid_process,
            process_apks_for_waydroid,
        )
        from adscrawler.tools.geo import update_geo_dbs  # noqa: PLC0415

        if self.args.redownload_geo_dbs:
            update_geo_dbs(redownload=self.args.redownload_geo_dbs)
            return
        run_name = self.args.run_name
        if self.args.store_id:
            # Manual waydroid process, launches app for 5 minutes for user to interact
            store_id = self.args.store_id
            store_id = self.args.store_id if self.args.store_id else None
            timeout = self.args.timeout_waydroid
            manual_waydroid_process(
                pgdb=self.pgcon,
                store_id=store_id,
                timeout=timeout,
                run_name=run_name,
            )
        else:
            # Default processing of apk/xapk files that need to be processed
            try:
                process_apks_for_waydroid(pgdb=self.pgcon, run_name=run_name)
            except Exception:
                logger.exception("Process APKs with Waydroid failed")

    def crawl_keywords(self) -> None:
        crawl_keyword_ranks(pgdb=self.pgcon)

    def extract_app_keywords(self) -> None:
        process_app_keywords(pgdb=self.pgcon, limit=self.args.limit_query_rows)

    def run(self) -> None:
        if self.args.limit_processes and self.is_script_already_running():
            logger.info("Script already running, exiting")
            sys.exit()

        self.setup_pgdb()
        self.main()


if __name__ == "__main__":
    process_manager = ProcessManager()
    process_manager.run()
