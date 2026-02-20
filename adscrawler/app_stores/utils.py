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


def check_and_insert_new_apps(
    dicts: list[dict],
    database_connection: PostgresCon,
    crawl_source: str,
    store: int,
) -> None:
    df = pd.DataFrame(dicts)
    if store in [1, 2]:
        df["store"] = store
    else:
        raise ValueError(f"Invalid store: {store}")
    found_bad_ids = df.loc[df["store"] == 2, "store_id"].str.match(r"^[0-9].*\.").any()
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
    logger.info(
        f"Scrape {store=} {crawl_source=} insert new apps to db {new_apps_df.shape[0]}",
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
    return None


def get_parquet_paths_by_prefix(bucket: str, prefix: str) -> list[str]:
    from adscrawler.packages.storage import get_s3_client

    s3 = get_s3_client()
    continuation_token = None
    all_parquet_paths = []
    while True:
        params = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        response = s3.list_objects_v2(**params)
        # Extract parquet paths from this page
        if "Contents" in response:
            parquet_paths = [
                f"s3://{bucket}/{obj['Key']}"
                for obj in response["Contents"]
                if obj["Key"].endswith(".parquet")
            ]
            all_parquet_paths += parquet_paths
        if "NextContinuationToken" in response:
            continuation_token = response["NextContinuationToken"]
        else:
            break
    return all_parquet_paths
