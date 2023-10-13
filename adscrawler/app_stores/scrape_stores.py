import datetime

import google_play_scraper
import pandas as pd
import tldextract
from itunes_app_scraper.util import AppStoreException

from adscrawler.app_stores.apple import (
    clean_ios_app_df,
    crawl_ios_developers,
    scrape_app_ios,
    scrape_ios_frontpage,
)
from adscrawler.app_stores.google import (
    clean_google_play_app_df,
    crawl_google_developers,
    scrape_app_gp,
    scrape_gp_for_app_ids,
)
from adscrawler.config import get_logger
from adscrawler.connection import PostgresCon
from adscrawler.queries import (
    delete_app_url_mapping,
    query_categories,
    query_collections,
    query_countries,
    query_developers,
    query_store_apps,
    query_store_id_map,
    query_store_ids,
    upsert_df,
)

logger = get_logger(__name__)


def scrape_stores_frontpage(
    database_connection: PostgresCon, stores: list[int]
) -> None:
    collection_keywords = ["NEW", "TOP"]
    collections_map = query_collections(database_connection)
    categories_map = query_categories(database_connection)
    countries_map = query_countries(database_connection)
    collections_map = collections_map.rename(columns={"id": "store_collection"})
    categories_map = categories_map.rename(columns={"id": "store_category"})
    for collection_keyword in collection_keywords:
        if 2 in stores:
            try:
                ranked_dicts = scrape_ios_frontpage(
                    collection_keyword=collection_keyword,
                )
                process_scraped(
                    database_connection=database_connection,
                    ranked_dicts=ranked_dicts,
                    collections_map=collections_map,
                    categories_map=categories_map,
                    countries_map=countries_map,
                )
            except Exception:
                logger.exception(f"collection={collection_keyword} hit error, skipping")

    if 1 in stores:
        ranked_dicts = scrape_gp_for_app_ids()
        process_scraped(
            database_connection=database_connection,
            ranked_dicts=ranked_dicts,
            collections_map=collections_map,
            categories_map=categories_map,
            countries_map=countries_map,
        )
    return


def process_scraped(
    database_connection: PostgresCon,
    ranked_dicts: list[dict],
    collections_map: pd.DataFrame,
    categories_map: pd.DataFrame,
    countries_map: pd.DataFrame,
) -> None:
    df = pd.DataFrame(ranked_dicts)
    all_scraped_ids = df["store_id"].unique().tolist()
    existing_ids_map = query_store_id_map(
        database_connection, store_ids=all_scraped_ids
    )
    existing_store_ids = existing_ids_map["store_id"].tolist()
    new_apps_df = df[~(df["store_id"].isin(existing_store_ids))][
        ["store", "store_id"]
    ].drop_duplicates()
    if not new_apps_df.empty:
        logger.info(f"Scrape store: insert new apps to db {new_apps_df.shape=}")
        insert_columns = ["store", "store_id"]
        upsert_df(
            table_name="store_apps",
            insert_columns=insert_columns,
            df=new_apps_df,
            key_columns=insert_columns,
            database_connection=database_connection,
        )
    logger.info("Store rankings start")
    new_existing_ids_map = query_store_id_map(
        database_connection, store_ids=all_scraped_ids
    ).rename(columns={"id": "store_app"})
    df = pd.merge(
        df, new_existing_ids_map, how="left", on=["store", "store_id"], validate="m:1"
    ).drop("store_id", axis=1)
    df = pd.merge(
        df, collections_map, how="left", on=["store", "collection"], validate="m:1"
    ).drop("collection", axis=1)
    df = pd.merge(
        df, categories_map, how="left", on=["store", "category"], validate="m:1"
    ).drop("category", axis=1)
    df["country"] = df["country"].str.upper()
    df = (
        pd.merge(
            df,
            countries_map[["id", "alpha2"]],
            how="left",
            left_on=["country"],
            right_on="alpha2",
            validate="m:1",
        )
        .drop(["country", "alpha2"], axis=1)
        .rename(columns={"id": "country"})
    )
    upsert_df(
        database_connection=database_connection,
        df=df,
        table_name="app_rankings",
        key_columns=[
            "crawled_date",
            "country",
            "rank",
            "store",
            "store_category",
            "store_collection",
        ],
        insert_columns=["store_app"],
    )


def extract_domains(x: str) -> str:
    ext = tldextract.extract(x)
    use_top_domain = any(
        ["m" == ext.subdomain, "www" in ext.subdomain.split("."), ext.subdomain == ""]
    )
    if use_top_domain:
        url = ".".join([ext.domain, ext.suffix])
    else:
        url = ".".join(part for part in ext if part)
    url = url.lower()
    return url


def crawl_developers_for_new_store_ids(
    database_connection: PostgresCon, store: int
) -> None:
    store_ids = query_store_ids(database_connection, store=store)
    df = query_developers(database_connection, store=store)
    if store == 1:
        developer_ids = df["developer_id"].unique().tolist()
        apps_df = crawl_google_developers(developer_ids, store_ids)
        dev_dict = {
            "developer": df["id"].tolist(),
            "apps_crawled_at": datetime.datetime.utcnow(),
        }
        dev_df = pd.DataFrame([dev_dict])
        insert_columns = dev_df.columns.tolist()
        key_columns = ["developer"]
        upsert_df(
            table_name="developers_crawled_at",
            schema="logging",
            insert_columns=insert_columns,
            df=dev_df,
            key_columns=key_columns,
            database_connection=database_connection,
        )
        logger.info(f"{store=} crawled, {apps_df.shape[0]} new store ids")
    if store == 2:
        for _id, row in df.iterrows():
            developer_db_id = row["id"]
            developer_id = row["developer_id"]
            row_info = f"{store=} {developer_id=}"
            try:
                if store == 2:
                    apps_df = crawl_ios_developers(
                        developer_db_id, developer_id, store_ids
                    )

                if not apps_df.empty:
                    apps_df = clean_scraped_df(df=apps_df, store=store)
                    save_apps_df(
                        apps_df,
                        database_connection,
                        update_developer=False,
                        country="us",
                    )
                dev_dict = {
                    "developer": developer_db_id,
                    "apps_crawled_at": datetime.datetime.utcnow(),
                }
                dev_df = pd.DataFrame([dev_dict])
                insert_columns = dev_df.columns.tolist()
                key_columns = ["developer"]
                upsert_df(
                    table_name="developers_crawled_at",
                    schema="logging",
                    insert_columns=insert_columns,
                    df=dev_df,
                    key_columns=key_columns,
                    database_connection=database_connection,
                )
                logger.info(f"{row_info=} crawled, {apps_df.shape[0]} new store ids")
            except Exception:
                logger.exception(f"{row_info=} failed!")


def update_app_details(
    stores: list[int], database_connection: PostgresCon, limit: int | None = 1000
) -> None:
    logger.info("Update App Details: start with oldest first")
    df = query_store_apps(stores, database_connection=database_connection, limit=limit)
    crawl_stores_for_app_details(df, database_connection)


def crawl_stores_for_app_details(
    df: pd.DataFrame, database_connection: PostgresCon
) -> None:
    logger.info(f"Update App Details: df: {df.shape}")
    for _index, row in df.iterrows():
        store_id = row.store_id
        store = row.store
        update_all_app_info(store, store_id, database_connection)


def update_all_app_info(
    store: int, store_id: str, database_connection: PostgresCon
) -> None:
    info = f"{store=} {store_id=}"
    app_df = scrape_and_save_app(store, store_id, database_connection)
    if "store_app" not in app_df.columns:
        logger.error(f"{info} store_app db id not in app_df columns")
        return
    if (
        "store_app" in app_df.columns
        and "url" not in app_df.columns
        or not app_df["url"].values
    ):
        store_app = app_df["store_app"].values[0]
        delete_app_url_mapping(store_app, database_connection)
        return
    if app_df["crawl_result"].values[0] != 1:
        logger.info(f"{info} crawl not successful, don't update further")
        return
    app_df["url"] = app_df["url"].apply(lambda x: extract_domains(x))
    insert_columns = ["url"]
    app_urls_df = upsert_df(
        table_name="pub_domains",
        df=app_df,
        insert_columns=insert_columns,
        key_columns=["url"],
        database_connection=database_connection,
        return_rows=True,
    )
    if app_urls_df is not None and not app_urls_df.empty:
        app_df["pub_domain"] = app_urls_df["id"].astype(object)[0]
        insert_columns = ["store_app", "pub_domain"]
        key_columns = ["store_app"]
        upsert_df(
            table_name="app_urls_map",
            insert_columns=insert_columns,
            df=app_df,
            key_columns=key_columns,
            database_connection=database_connection,
        )
        logger.info(f"{info} finished")


def scrape_from_store(store: int, store_id: str, country: str) -> dict:
    if store == 1:
        result_dict = scrape_app_gp(store_id, country=country)
    elif store == 2:
        result_dict = scrape_app_ios(store_id, country=country)
    else:
        logger.error(f"Store not supported {store=}")
    return result_dict


def clean_scraped_df(df: pd.DataFrame, store: int) -> pd.DataFrame:
    if store == 1:
        df = clean_google_play_app_df(df)
    if store == 2:
        df = clean_ios_app_df(df)
    return df


def scrape_app(store: int, store_id: str, country: str) -> pd.DataFrame:
    scrape_info = f"{store=}, {store_id=}, {country=}"
    try:
        result_dict = scrape_from_store(store=store, store_id=store_id, country=country)
        crawl_result = 1
    except google_play_scraper.exceptions.NotFoundError:
        crawl_result = 3
        logger.warning(f"{scrape_info} failed to find app")
    except AppStoreException as error:
        if "No app found" in str(error):
            crawl_result = 3
            logger.warning(f"{scrape_info} failed to find app")
        else:
            crawl_result = 4
            logger.error(f"{scrape_info} unexpected error: {error=}")
    except Exception as error:
        logger.error(f"{scrape_info} unexpected error: {error=}")
        crawl_result = 4
    if crawl_result != 1:
        result_dict = {}
    if "kind" in result_dict.keys() and "mac" in result_dict["kind"].lower():
        logger.error(f"{scrape_info} Crawled app is not Mac Software, not iOS!")
        crawl_result = 5
    result_dict["crawl_result"] = crawl_result
    result_dict["store"] = store
    result_dict["store_id"] = store_id
    df = pd.DataFrame([result_dict])
    if crawl_result == 1:
        df = clean_scraped_df(df=df, store=store)
    return df


def save_developer_info(
    app_df: pd.DataFrame, database_connection: PostgresCon
) -> pd.DataFrame:
    assert app_df["developer_id"].values[
        0
    ], f"{app_df['store_id']} Missing Developer ID"
    df = (
        app_df[["store", "developer_id", "developer_name"]]
        .rename(columns={"developer_name": "name"})
        .drop_duplicates()
    )
    table_name = "developers"
    insert_columns = ["store", "developer_id", "name"]
    key_columns = ["store", "developer_id"]

    try:
        dev_df = upsert_df(
            table_name=table_name,
            df=df,
            insert_columns=insert_columns,
            key_columns=key_columns,
            database_connection=database_connection,
            return_rows=True,
        )
        assert dev_df is not None, "Dev_df is none!"
        app_df["developer"] = dev_df["id"].astype(object)[0]
    except Exception as error:
        logger.error(f"Developer insert failed with error {error}")
    return app_df


def scrape_and_save_app(
    store: int, store_id: str, database_connection: PostgresCon
) -> pd.DataFrame:
    country_list = ["us"]
    for country in country_list:
        info = f"{store=}, {store_id=}, {country=}"
        app_df = scrape_app(store=store, store_id=store_id, country=country)
        app_df = save_apps_df(
            apps_df=app_df, database_connection=database_connection, country=country
        )
    crawl_result = app_df["crawl_result"].values[0]
    logger.info(f"{info} {crawl_result=} scraped and saved app")
    return app_df


def save_apps_df(
    apps_df: pd.DataFrame,
    database_connection: PostgresCon,
    country: str,
    update_developer: bool = True,
) -> pd.DataFrame:
    table_name = "store_apps"
    key_columns = ["store", "store_id"]
    if (
        (apps_df["crawl_result"] == 1).all()
        and apps_df["developer_id"].notnull().all()
        and update_developer
    ):
        apps_df = save_developer_info(apps_df, database_connection)
    insert_columns = [x for x in STORE_APP_COLUMNS if x in apps_df.columns]
    store_apps_df = upsert_df(
        table_name=table_name,
        df=apps_df,
        insert_columns=insert_columns,
        key_columns=key_columns,
        database_connection=database_connection,
        return_rows=True,
    )

    if (
        store_apps_df is not None
        and not store_apps_df[store_apps_df["crawl_result"] == 1].empty
    ):
        store_apps_df = store_apps_df.rename(columns={"id": "store_app"})
        store_apps_history = store_apps_df[store_apps_df["crawl_result"] == 1].copy()

        store_apps_history = pd.merge(
            store_apps_history,
            apps_df[["store_id", "histogram", "rating_count"]],
            on="store_id",
        )

        store_apps_history["country"] = country.upper()
        store_apps_history["crawled_date"] = (
            datetime.datetime.utcnow().date().strftime("%Y-%m-%d")
        )
        table_name = "store_apps_country_history"
        insert_columns = [
            "installs",
            "review_count",
            "rating",
            "rating_count",
            "histogram",
        ]
        key_columns = ["store_app", "country", "crawled_date"]
        upsert_df(
            table_name=table_name,
            df=store_apps_history,
            insert_columns=insert_columns,
            key_columns=key_columns,
            database_connection=database_connection,
        )

    if store_apps_df is not None and not store_apps_df.empty:
        store_apps_df = store_apps_df.rename(columns={"id": "store_app"})
        apps_df = pd.merge(
            apps_df,
            store_apps_df[["store_id", "store_app"]],
            how="left",
            validate="1:1",
        )
    log_crawl_results(apps_df, database_connection=database_connection)
    return apps_df


def log_crawl_results(app_df: pd.DataFrame, database_connection: PostgresCon) -> None:
    app_df["crawled_at"] = datetime.datetime.utcnow()
    insert_columns = ["crawl_result", "store", "store_id", "store_app", "crawled_at"]
    app_df = app_df[insert_columns]
    app_df.to_sql(
        name="store_apps_crawl",
        schema="logging",
        con=database_connection.engine,
        if_exists="append",
    )


STORE_APP_COLUMNS = [
    "developer",
    "name",
    "store_id",
    "store",
    "category",
    "rating",
    "installs",
    "free",
    "price",
    "size",
    "minimum_android",
    "review_count",
    "content_rating",
    "store_last_updated",
    "developer_email",
    "ad_supported",
    "in_app_purchases",
    "editors_choice",
    "crawl_result",
    "icon_url_512",
]
