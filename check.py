from adscrawler.app_stores.scrape_stores import (
    crawl_developers_for_new_store_ids,
    crawl_ios_developers,
    update_all_app_info,
)
from adscrawler.config import get_logger
from adscrawler.connection import get_db_connection
from adscrawler.scrape import (
    scrape_app_ads_url,
)

logger = get_logger(__name__)

"""
    Top level script for managing getting app-ads.txt
"""


def main() -> None:
    # scrape_ios_frontpage(PGCON)
    # scrape_gp_for_app_ids(PGCON)

    # Scrape singel app
    store = 1  # Android
    store_id = "com.wifi.proshield"
    store = 2  # iOS
    store_id = "310633997"
    database_connection = PGCON
    update_all_app_info(store, store_id, database_connection=database_connection)

    # Crawl developwer websites to check for app ads
    url = "a-spy.com"
    url = "isenseplus.com"
    url = "kitetdev.com"
    url = "ardroidsoftware.com"
    url = "btraining.rf.gd"
    database_connection = PGCON
    scrape_app_ads_url(url=url, database_connection=database_connection)

    developer_id = "310633997"
    apps_df = crawl_ios_developers(developer_db_id, developer_id, store_ids)

    database_connection = PGCON
    store = 1
    crawl_developers_for_new_store_ids(
        database_connection=database_connection, store=store
    )


if __name__ == "__main__":
    logger.info("Starting app-ads.txt crawler")
    PGCON = get_db_connection(use_ssh_tunnel=True)
    PGCON.set_engine()
    database_connection = PGCON

    main()
