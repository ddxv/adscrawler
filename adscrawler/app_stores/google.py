import json
import os

import google_play_scraper
import pandas as pd

from adscrawler.config import MODULE_DIR, get_logger

logger = get_logger(__name__)


def scrape_app_gp(store_id: str, country: str, language: str = "") -> dict:
    # Note language seems not to change the number of reviews, but country does
    # Note country does not change number of installs
    # NOTE: Histogram, Ratings, score are SOMETIMES country specific
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


def get_js_data(filepath: str) -> list[dict]:
    with open(filepath, mode="r") as file:
        data = [json.loads(line) for line in file if line.strip()]
    return data


def scrape_gp_for_app_ids() -> list[dict]:
    logger.info("Scrape GP frontpage for new apps start")
    filepath = "/tmp/googleplay_json.txt"
    try:
        js_update_ids_file(filepath)
    except Exception as error:
        logger.exception(f"JS pull failed with {error=}")
    ranked_dicts = get_js_data(filepath)
    logger.info("Scrape GP frontpage for new apps finished")
    return ranked_dicts
