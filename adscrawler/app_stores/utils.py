"""Shared utilities for app stores."""

import pandas as pd

from adscrawler.config import get_logger
from adscrawler.dbcon.connection import PostgresCon
from adscrawler.dbcon.queries import query_store_id_map, upsert_df

logger = get_logger(__name__, "scrape_stores")


def truncate_utf8_bytes(s: str, max_bytes: int = 2400) -> str:
    if s is None:
        return ""
    encoded = s.encode("utf-8")
    if len(encoded) <= max_bytes:
        return s
    # Truncate bytes, then decode safely
    truncated = encoded[:max_bytes]
    while True:
        try:
            return truncated.decode("utf-8")
        except UnicodeDecodeError:
            truncated = truncated[:-1]


def insert_new_apps(
    dicts: list[dict], database_connection: PostgresCon, crawl_source: str, store: int
) -> None:
    df = pd.DataFrame(dicts)
    df["store_id"].str.match(r"^[0-9].*\.")
    found_bad_ids = df[df["store"] == store, "store_id"].str.match(r"^[0-9].*\.").any()
    if found_bad_ids:
        logger.error(f"Scrape {store=} {crawl_source=} found bad store_ids")
        raise ValueError("Found bad store_ids")
    all_scraped_ids = df["store_id"].unique().tolist()
    existing_ids_map = query_store_id_map(
        database_connection,
        store_ids=all_scraped_ids,
    )
    existing_store_ids = existing_ids_map["store_id"].tolist()
    new_apps_df = df[~(df["store_id"].isin(existing_store_ids))][
        ["store", "store_id"]
    ].drop_duplicates()
    if new_apps_df.empty:
        logger.info(f"Scrape {store=} {crawl_source=} no new apps")
        return
    else:
        logger.info(
            f"Scrape {store=} {crawl_source=} insert new apps to db {new_apps_df.shape=}",
        )
        insert_columns = ["store", "store_id"]
        inserted_apps: pd.DataFrame = upsert_df(
            table_name="store_apps",
            insert_columns=insert_columns,
            df=new_apps_df,
            key_columns=insert_columns,
            database_connection=database_connection,
            return_rows=True,
        )
        if inserted_apps is not None and not inserted_apps.empty:
            logger.warning(
                f"No IDs returned for {store=} {crawl_source=} inserted apps {inserted_apps.shape=}"
            )
            inserted_apps["crawl_source"] = crawl_source
            inserted_apps = inserted_apps.rename(columns={"id": "store_app"})
            insert_columns = ["store", "store_app"]
            upsert_df(
                table_name="store_app_sources",
                insert_columns=insert_columns + ["crawl_source"],
                df=inserted_apps,
                key_columns=insert_columns,
                database_connection=database_connection,
                schema="logging",
            )
