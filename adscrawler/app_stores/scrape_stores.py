from adscrawler.connection import PostgresCon
from itunes_app_scraper.util import AppStoreException
from adscrawler.app_stores.apple import (
    scrape_app_ios,
    clean_ios_app_df,
    crawl_ios_developers,
    scrape_ios_frontpage,
)
from .google import scrape_app_gp, clean_google_play_app_df, scrape_gp_for_app_ids
import google_play_scraper
import pandas as pd
from adscrawler.config import get_logger
from adscrawler.queries import (
    upsert_df,
    delete_app_url_mapping,
    query_store_apps,
    query_store_ids,
    query_developers,
)
import datetime
import tldextract

logger = get_logger(__name__)


def scrape_stores_frontpage(database_connection, stores: list[int]) -> None:
    if 2 in stores:
        scrape_ios_frontpage(database_connection, collection_keyword="NEW")

    if 1 in stores:
        scrape_gp_for_app_ids(database_connection)
    return


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


def crawl_developers_for_new_store_ids(database_connection, store: int):
    store_ids = query_store_ids(database_connection, store=store)
    df = query_developers(database_connection, store=store)
    for _id, row in df.iterrows():
        developer_db_id = row["id"]
        developer_id = row["developer_id"]
        row_info = f"{store=} {developer_id=}"
        if store == 2:
            apps_df = crawl_ios_developers(
                database_connection, developer_db_id, developer_id, store_ids
            )
        if not apps_df.empty:
            apps_df = clean_scraped_df(df=apps_df, store=store)
            save_apps_df(apps_df, database_connection, update_developer=False)
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


def update_app_details(
    stores: list[int], database_connection, limit: int | None = 1000
) -> None:
    logger.info("Update App Details: start with oldest first")
    df = query_store_apps(stores, database_connection=database_connection, limit=limit)
    crawl_stores_for_app_details(df, database_connection)


def crawl_stores_for_app_details(df: pd.DataFrame, database_connection) -> None:
    logger.info(f"Update App Details: df: {df.shape}")
    for _index, row in df.iterrows():
        store_id = row.store_id
        store = row.store
        update_all_app_info(store, store_id, database_connection)


def update_all_app_info(store: int, store_id: str, database_connection) -> None:
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
        logger.info(f"{info} no developer url")
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


def scrape_from_store(store: int, store_id: str) -> dict:
    if store == 1:
        result_dict = scrape_app_gp(store_id)
    elif store == 2:
        result_dict = scrape_app_ios(store_id)
    else:
        logger.error(f"Store not supported {store=}")
    return result_dict


def clean_scraped_df(df: pd.DataFrame, store: int) -> pd.DataFrame:
    if store == 1:
        df = clean_google_play_app_df(df)
    if store == 2:
        df = clean_ios_app_df(df)
    return df


def scrape_app(store: int, store_id: str) -> pd.DataFrame:
    scrape_info = f"{store=}, {store_id=}"
    try:
        result_dict = scrape_from_store(store=store, store_id=store_id)
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


def save_developer_info(app_df: pd.DataFrame, database_connection) -> pd.DataFrame:
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
        app_df["developer"] = dev_df["id"].astype(object)[0]
    except Exception as error:
        logger.error(f"Developer insert failed with error {error}")
    return app_df


def scrape_and_save_app(
    store: int, store_id: str, database_connection: PostgresCon
) -> pd.DataFrame:
    info = f"{store=}, {store_id=}"
    app_df = scrape_app(store=store, store_id=store_id)
    app_df = save_apps_df(apps_df=app_df, database_connection=database_connection)
    crawl_result = app_df["crawl_result"].values[0]
    logger.info(f"{info} {crawl_result=} scraped and saved app")
    return app_df


def save_apps_df(
    apps_df: pd.DataFrame, database_connection, update_developer=True
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
    store_apps_df = store_apps_df.rename(columns={"id": "store_app"})
    apps_df = pd.merge(
        apps_df, store_apps_df[["store_id", "store_app"]], how="left", validate="1:1"
    )
    log_crawl_results(apps_df, database_connection=database_connection)
    return apps_df


def log_crawl_results(app_df: pd.DataFrame, database_connection):
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
]