import datetime
import re

import pandas as pd
from itunes_app_scraper.scraper import AppStoreScraper
from itunes_app_scraper.util import AppStoreCategories, AppStoreCollections

from adscrawler.config import get_logger
from adscrawler.connection import PostgresCon
from adscrawler.queries import query_store_id_map, upsert_df

logger = get_logger(__name__)


def scrape_ios_frontpage(
    database_connection: PostgresCon,
    collections_map: pd.DataFrame,
    categories_map: pd.DataFrame,
    category_keyword: str | None = None,
    collection_keyword: str | None = None,
) -> None:
    logger.info("Scrape iOS frontpage for new apps")
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
    all_scraped_ids = []
    ranked_dicts: list[dict] = []
    country = "us"
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
            all_scraped_ids += scraped_ids
            ranked_dicts += [
                {
                    "collection": _coll_key,
                    "category": cat_key,
                    "rank": rank + 1,
                    "app": app,
                }
                for rank, app in enumerate(scraped_ids)
            ]
    all_scraped_ids = list(set(all_scraped_ids))
    existing_ids_map = query_store_id_map(
        database_connection, store=2, store_ids=all_scraped_ids
    )
    existing_store_ids = existing_ids_map["store_id"].tolist()

    only_new = [x for x in all_scraped_ids if str(x) not in existing_store_ids]
    apps_df = pd.DataFrame({"store": 2, "store_id": only_new})
    insert_columns = ["store", "store_id"]
    logger.info(f"Scrape iOS frontpage for new apps: insert to db {apps_df.shape=}")
    upsert_df(
        table_name="store_apps",
        insert_columns=insert_columns,
        df=apps_df,
        key_columns=insert_columns,
        database_connection=database_connection,
    )
    insert_full_rankings(
        database_connection=database_connection,
        collections_map=collections_map,
        categories_map=categories_map,
        all_scraped_ids=all_scraped_ids,
        ranked_dicts=ranked_dicts,
    )
    logger.info("Scrape iOS frontpage for new apps finished")


def insert_full_rankings(
    database_connection: PostgresCon,
    collections_map: pd.DataFrame,
    categories_map: pd.DataFrame,
    all_scraped_ids: list[str],
    ranked_dicts: list[dict],
) -> None:
    logger.info("Store rankings start")
    new_existing_ids_map = query_store_id_map(
        database_connection, store=2, store_ids=all_scraped_ids
    ).rename(columns={"id": "store_app"})
    df = pd.DataFrame.from_dict(ranked_dicts)
    df = df.rename(columns={"app": "store_id"})
    df = pd.merge(
        df, new_existing_ids_map, how="left", on="store_id", validate="m:1"
    ).drop("store_id", axis=1)
    df = pd.merge(
        df, collections_map, how="left", on=["store", "collection"], validate="m:1"
    ).drop("collection", axis=1)
    df = pd.merge(
        df, categories_map, how="left", on=["store", "category"], validate="m:1"
    ).drop("category", axis=1)
    df["crawled_date"] = datetime.datetime.utcnow().date()
    upsert_df(
        database_connection=database_connection,
        df=df,
        table_name="app_rankings",
        key_columns=[
            "crawled_date",
            "rank",
            "store",
            "store_category",
            "store_collection",
        ],
        insert_columns=["store_app"],
    )


def crawl_ios_developers(
    developer_db_id: int,
    developer_id: str,
    store_ids: list[str],
) -> pd.DataFrame:
    store = 2
    ios_scraper = AppStoreScraper()
    apps = ios_scraper.get_apps_for_developer(developer_id=developer_id)
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
    scraper = AppStoreScraper()
    result: dict = scraper.get_app_details(store_id, country=country, add_ratings=True)

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
            "currentVersionReleaseDate": "store_last_updated",
            "artistId": "developer_id",
            "artistName": "developer_name",
            "userRatingCount": "rating_count",
            "artworkUrl512": "icon_url_512",
        }
    )
    if "price" not in df.columns:
        df["price"] = 0
    try:
        # Complicated way to get around many games having very random selections like "games + shopping"
        # TODO: Just store categories as list!
        df.loc[df["category"] == "Games", "category"] = "game_" + df.loc[
            df["category"] == "Games", "genres"
        ].apply(
            lambda x: [
                cat.lower() for cat in x.split(",") if cat.lower() in GAME_CATEGORIES
            ][0]
        )
    except Exception as e:
        logger.warning(
            f"store_id={df['store_id'].values[0]} split genre IDs failed {e}"
        )
    df = df.assign(
        free=df["price"] == 0,
        developer_id=df["developer_id"].astype(str),
        store_id=df["store_id"].astype(str),
        category=df["category"].str.lower().str.replace(" & ", "_and_"),
        store_last_updated=pd.to_datetime(df["store_last_updated"]).dt.strftime(
            "%Y-%m-%d %H:%M"
        ),
    )
    try:
        df["histogram"] = df["user_ratings"].apply(
            lambda x: [int(num) for num in re.findall(r"\d+", x)[1::2]]
        )
    except Exception:
        logger.exception("Unable to parse histogram")
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
