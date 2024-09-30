import datetime
import re

import pandas as pd
import requests
from itunes_app_scraper.scraper import AppStoreScraper
from itunes_app_scraper.util import AppStoreCategories, AppStoreCollections

from adscrawler.config import get_logger

logger = get_logger(__name__, "scrape_apple")

ITUNES_LOOKUP_API = "https://itunes.apple.com/lookup"


def lookupby_id(app_id: str) -> dict:
    response = requests.get(ITUNES_LOOKUP_API, params={"id": app_id})
    if response.status_code != 200:
        msg = "Response code not 200: {response.text}"
        logger.error(msg)
        raise requests.HTTPError(msg)
    try:
        resp_dict: dict = response.json()["results"][0]
    except Exception as err:
        logger.exception("Unable to parse response")
        raise requests.HTTPError(err) from err
    return resp_dict


def scrape_ios_ranks(
    category_keyword: str | None = None,
    collection_keyword: str | None = None,
) -> list[dict]:
    logger.info(f"Scrape iOS ranks for {collection_keyword=} {category_keyword=}")
    country = "us"
    scraper = AppStoreScraper()
    # Eg: MAGAZINES_MEN, GAMES_ADVENTURE
    if category_keyword:
        category_keyword = category_keyword.upper()
        categories = {
            k: v
            for k, v in AppStoreCategories.__dict__.items()
            if (category_keyword in k.upper()) and (not k.startswith("__"))
        }
    else:
        categories = {
            k: v
            for k, v in AppStoreCategories.__dict__.items()
            if not k.startswith("__")
        }
    # Eg: TOP_PAID / TOP_FREE
    if collection_keyword:
        collection_keyword = collection_keyword.upper()
        collections = {
            k: v
            for k, v in AppStoreCollections.__dict__.items()
            if not k.startswith("__")
            and "_MAC" not in k
            and (collection_keyword in k.upper())
        }
    else:
        collections = {
            k: v
            for k, v in AppStoreCollections.__dict__.items()
            if not k.startswith("__") and "_MAC" not in k
        }
    ranked_dicts: list[dict] = []
    for _coll_key, coll_value in collections.items():
        logger.info(f"Collection: {_coll_key}")
        for cat_key, cat_value in categories.items():
            logger.info(f"Collection: {_coll_key}, category: {cat_key}")
            scraped_ids = scraper.get_app_ids_for_collection(
                collection=coll_value,
                category=cat_value,
                country=country,
                num=200,
                timeout=10,
            )
            ranked_dicts += [
                {
                    "store": 2,
                    "country": country,
                    "collection": _coll_key,
                    "category": cat_key,
                    "rank": rank + 1,
                    "store_id": app,
                    "crawled_date": datetime.datetime.now(
                        tz=datetime.UTC,
                    ).date(),
                }
                for rank, app in enumerate(scraped_ids)
            ]
    return ranked_dicts


def crawl_ios_developers(
    developer_db_id: int,
    developer_id: str,
    store_ids: list[str],
) -> pd.DataFrame:
    store = 2
    ios_scraper = AppStoreScraper()
    apps = ios_scraper.get_apps_for_developer(developer_id=developer_id, timeout=10)
    my_devices = ["iphone", "ipad"]
    apps = [
        x
        for x in apps
        if "supportedDevices" in x.keys()
        and any(map("".join(x["supportedDevices"]).lower().__contains__, my_devices))
    ]
    apps_df = pd.DataFrame(apps).rename(columns={"trackId": "store_id"})
    if not apps_df.empty:
        apps_df["store_id"] = apps_df["store_id"].astype(str)
        apps_df = apps_df[~apps_df["store_id"].isin(store_ids)]
        apps_df["crawl_result"] = 1
        apps_df["store"] = store
        apps_df["developer"] = developer_db_id
    return apps_df


def scrape_app_ios(store_id: str, country: str) -> dict:
    # NOTE: averageUserRating, Rating_count, Histogram are country specific
    logger.info(f"Scrape app start {store_id=} {country=}")
    scraper = AppStoreScraper()
    result: dict = scraper.get_app_details(store_id, country=country, add_ratings=True, timeout=10)
    logger.info(f"Scrape app finish {store_id=} {country=}")
    return result


def clean_ios_app_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            # "trackId": "store_id",
            "trackName": "name",
            "averageUserRating": "rating",
            "sellerUrl": "url",
            "minimum_OsVersion": "minimum_android",
            "primaryGenreName": "category",
            "bundleId": "bundle_id",
            "releaseDate": "release_date",
            "currentVersionReleaseDate": "store_last_updated",
            "artistId": "developer_id",
            "artistName": "developer_name",
            "userRatingCount": "rating_count",
            "artworkUrl512": "icon_url_512",
            "screenshotUrls": "phone_image_urls",
            "ipadScreenshotUrls": "tablet_image_urls",
        },
    )
    if "price" not in df.columns:
        df["price"] = 0
    try:
        # Complicated way to get around many games having very random selections like "games + shopping"
        # TODO: Just store categories as list!
        df.loc[df["category"] == "Games", "category"] = "game_" + df.loc[
            df["category"] == "Games",
            "genres",
        ].apply(
            lambda x: [
                cat.lower() for cat in x.split(",") if cat.lower() in GAME_CATEGORIES
            ][0],
        )
    except Exception as e:
        logger.warning(
            f"store_id={df['store_id'].to_numpy()[0]} split genre IDs failed {e}",
        )
    df = df.assign(
        free=df["price"] == 0,
        developer_id=df["developer_id"].astype(str),
        store_id=df["store_id"].astype(str),
        category=df["category"].str.lower().str.replace(" & ", "_and_"),
        store_last_updated=pd.to_datetime(df["store_last_updated"]).dt.strftime(
            "%Y-%m-%d %H:%M",
        ),
        release_date=pd.to_datetime(
            df["release_date"],
            format="%Y-%m-%dT%H:%M:%SZ",
        ).dt.date,
    )
    list_cols = ["phone_image_url", "tablet_image_url"]
    for list_col in list_cols:
        urls_empty = ((df[f"{list_col}s"].isna()) | (df[f"{list_col}s"] == "")).all()
        if not urls_empty:
            columns = {x: f"{list_col}_{x+1}" for x in range(3)}
            df = pd.concat(
                [
                    df,
                    df[f"{list_col}s"]
                    .str.split(",")
                    .apply(pd.Series)
                    .rename(columns=columns),
                ],
                axis=1,
            )
        else:
            for x in range(3):
                df[f"{list_col}_{x}"] = None
    try:
        df["histogram"] = df["user_ratings"].apply(
            lambda x: [int(num) for num in re.findall(r"\d+", x)[1::2]],
        )
    except Exception:
        logger.warning("Unable to parse histogram")
        df["histogram"] = None
    return df


GAME_CATEGORIES = [
    "arcade",
    "simulation",
    "action",
    "adventure",
    "educational",
    "role_playing",
    "racing",
    "trivia",
    "board",
    "strategy",
    "puzzle",
    "casual",
    "word",
    "card",
    "sports",
    "casino",
    "music",
]
