import os

import google_play_scraper
import pandas as pd

from adscrawler.config import MODULE_DIR, get_logger
from adscrawler.connection import PostgresCon
from adscrawler.queries import query_store_ids, upsert_df

logger = get_logger(__name__)


def scrape_app_gp(store_id: str, country: str, language: str = "") -> dict:
    # Note language seems not to change the number of reviews, but country does
    # Note country does not change number of installs
    # NOTE: Histogram, Ratings, score and installs are SOMETIMES country specific
    # NOTE: Reviews are always country specific?
    # NOTE: Installs are never country specific
    # Example: 'ratings'
    # dom_nl = scrape_app_gp('com.nexonm.dominations.adk', 'nl')
    # dom_us = scrape_app_gp('com.nexonm.dominations.adk', 'us')
    # dom_us['ratings']==dom_nl['ratings']
    # In the case above NL and US both have the same number of ratings
    # paw_nl = scrape_app_gp('com.originatorkids.paw', 'nl')
    # paw_us = scrape_app_gp('com.originatorkids.paw', 'us')
    # paw_us['ratings']==paw_nl['ratings']
    # In the case above NL and US both have very different number of ratings

    result: dict = google_play_scraper.app(
        store_id, lang=language, country=country  # defaults to 'en'  # defaults to 'us'
    )
    return result


def clean_google_play_app_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "title": "name",
            "installs": "min_installs",
            "realInstalls": "installs",
            # "appId": "store_id",
            "score": "rating",
            "updated": "store_last_updated",
            "reviews": "review_count",
            "ratings": "rating_count",
            "containsAds": "ad_supported",
            "offersIAP": "in_app_purchases",
            "url": "store_url",
            "icon": "icon_url_512",
            "developerWebsite": "url",
            "developerId": "developer_id",
            "developer": "developer_name",
            "genreId": "category",
        }
    )
    df.loc[df["min_installs"].isnull(), "min_installs"] = df.loc[
        df["min_installs"].isnull(), "installs"
    ].astype(str)
    df = df.assign(
        min_installs=df["min_installs"]
        .str.replace(r"[,+]", "", regex=True)
        .fillna(0)
        .astype(int),
        category=df["category"].str.lower(),
        store_last_updated=pd.to_datetime(
            df["store_last_updated"], unit="s"
        ).dt.strftime("%Y-%m-%d %H:%M"),
    )
    return df


def js_update_ids_file(filepath: str) -> None:
    if os.path.exists(filepath):
        os.remove(filepath)
    os.system(f"node {MODULE_DIR}/pullAppIds.js")
    logger.info("Js pull finished")


def get_js_ids(filepath: str) -> list[str]:
    with open(filepath, mode="r") as file:
        ids = file.readlines()
    ids = [x.replace("\n", "") for x in ids]
    return ids


def scrape_gp_for_app_ids(database_connection: PostgresCon) -> None:
    logger.info("Scrape GP frontpage for new apps start")
    filepath = "/tmp/googleplay_ids.txt"
    try:
        js_update_ids_file(filepath)
    except Exception as error:
        logger.warning(f"JS pull failed with {error=}")
    ids = get_js_ids(filepath)
    ids = list(set(ids))
    existing_store_ids = query_store_ids(database_connection, store=1, store_ids=ids)
    only_new = [x for x in ids if x not in existing_store_ids]
    df = pd.DataFrame({"store": 1, "store_id": only_new})
    insert_columns = ["store", "store_id"]
    logger.info(f"Scrape GP frontpage for new apps: insert to db {df.shape=}")
    upsert_df(
        table_name="store_apps",
        insert_columns=insert_columns,
        df=df,
        key_columns=insert_columns,
        database_connection=database_connection,
    )
    logger.info("Scrape GP frontpage for new apps finished")
