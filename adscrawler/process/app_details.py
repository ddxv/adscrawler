"""Raw app details and keywords – upload to & import from S3 parquet."""

import datetime
import time
import uuid
from io import BytesIO

import pandas as pd

from adscrawler.app_stores.utils import (
    check_and_insert_new_apps,
    get_parquet_paths_by_prefix,
)
from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import (
    delete_and_insert,
    query_countries,
    query_languages,
    query_store_id_map,
    query_store_id_map_cached,
)
from adscrawler.process import (
    RAW_DATA_APP_DETAILS,
    RAW_DATA_APP_DETAILS_INCOMING,
    RAW_DATA_KEYWORDS,
)
from adscrawler.process.storage import (
    get_duckdb_connection,
    get_s3_client,
)

logger = get_logger(__name__, "scrape_stores")


def raw_keywords_to_s3(df: pd.DataFrame) -> None:
    """Upload keyword-rank data to ``raw-data/keywords/`` on S3."""
    logger.info(f"S3 upload keywords rows={df.shape[0]:,} start")
    s3_client = get_s3_client()
    bucket = CONFIG["s3"]["bucket"]
    df["store_id"] = df["store_id"].astype(str)
    for store, store_df in df.groupby("store"):
        for crawled_date, date_df in store_df.groupby("crawled_date"):
            if isinstance(crawled_date, datetime.date):
                crawled_date = crawled_date.strftime("%Y-%m-%d")
            for country, country_df in date_df.groupby("country"):
                epoch_ms = int(time.time() * 1000)
                suffix = uuid.uuid4().hex[:8]
                file_name = f"keywords_{epoch_ms}_{suffix}.parquet"
                s3_key = f"{RAW_DATA_KEYWORDS}/store={store}/crawled_date={crawled_date}/country={country}/{file_name}"
                buffer = BytesIO()
                country_df.to_parquet(buffer, index=False)
                buffer.seek(0)
                s3_client.upload_fileobj(buffer, bucket, s3_key)
    logger.info(f"S3 upload keywords {store=} finished")


def app_details_to_s3(df: pd.DataFrame, store: int) -> None:
    """Upload app-detail scrapes to ``raw-data/app_details/`` on S3."""
    logger.info(f"S3 upload app_details {store=}, rows={df.shape[0]:,} start")
    if store is None:
        raise ValueError("store is required")
    s3_client = get_s3_client()
    bucket = CONFIG["s3"]["bucket"]
    df["store_id"] = df["store_id"].astype(str)
    for crawled_date, date_df in df.groupby("crawled_date"):
        if isinstance(crawled_date, datetime.date):
            crawled_date = crawled_date.strftime("%Y-%m-%d")
        for country, country_df in date_df.groupby("country"):
            epoch_ms = int(time.time() * 1000)
            suffix = uuid.uuid4().hex[:8]
            file_name = f"app_details_{epoch_ms}_{suffix}.parquet"
            s3_key = f"{RAW_DATA_APP_DETAILS_INCOMING}/store={store}/crawled_date={crawled_date}/country={country}/{file_name}"
            buffer = BytesIO()
            country_df.to_parquet(buffer, index=False)
            buffer.seek(0)
            s3_client.upload_fileobj(buffer, bucket, s3_key)
    logger.info(f"S3 upload app details {store=} finished")


def import_app_details_from_s3_into_db(
    store: int,
    crawled_date: str,
    pgdb: PostgresEngine,
    process_icon: bool = False,
) -> None:
    """Read app-detail parquets from S3 for ``store``/``crawled_date``/US and
    upsert into the database via ``process_live_app_details``.

    The function reads all parquet files under
    ``raw-data/app_details/store={store}/crawled_date={crawled_date}/country=US/``,
    resolves the ``store_app`` primary key for each row, and delegates to
    :func:`adscrawler.app_stores.scrape_stores.process_live_app_details`.

    Args:
        store: Store ID (1 = Google Play, 2 = App Store).
        crawled_date: ISO-format date string (e.g. ``"2026-07-02"``).
        pgdb: Database connection.
        process_icon: Whether to process app icons (passed through).
    """
    logger.info(f"Importing app_details from S3 {store=} {crawled_date=} country=US")

    bucket = CONFIG["s3"]["bucket"]
    prefix = (
        f"{RAW_DATA_APP_DETAILS}/store={store}/crawled_date={crawled_date}/country=US/"
    )
    parquet_paths = get_parquet_paths_by_prefix(bucket, prefix)
    if not parquet_paths:
        logger.warning(f"No app_details parquet files found at {prefix}")
        return

    with get_duckdb_connection("s3") as duckdb_con:
        df = duckdb_con.execute(
            f"SELECT * FROM read_parquet({parquet_paths}, union_by_name=true)"
        ).df()

    if df.empty:
        logger.warning(f"Empty dataset at {prefix}")
        return

    df = df[df["crawl_result"] == 1]

    df["store_app"] = df["store_app_db_id"].astype(int)

    missing = df["store_app"].isna()
    if missing.any():
        logger.warning(
            f"DROPPING {missing.sum()} rows with unknown store_ids "
            f"(not yet in the store_apps table)"
        )
        df = df[~missing]

    if df.empty:
        logger.warning("No rows left after resolving store_app IDs")
        return

    from adscrawler.app_stores.scrape_stores import (
        process_live_app_details,  # noqa: PLC0415
    )

    process_live_app_details(
        store=store,
        results_df=df,
        pgdb=pgdb,
        process_icon=False,
    )


def import_keywords_from_s3(
    start_date: datetime.date,
    end_date: datetime.date,
    pgdb: PostgresEngine,
) -> None:
    language = "en"
    country_map = query_countries(pgdb)
    languages_map = query_languages(pgdb)
    language_dict = languages_map.set_index("language_slug")["id"].to_dict()
    _language_key = language_dict[language]
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    for snapshot_date in pd.date_range(start_date, end_date, freq="D"):
        snapshot_date = snapshot_date.date()
        for store in [1, 2]:
            s3_key = f"{RAW_DATA_KEYWORDS}/store={store}/crawled_date={snapshot_date}/"
            parquet_paths = get_parquet_paths_by_prefix(bucket, s3_key)
            if len(parquet_paths) == 0:
                logger.warning(f"No parquet paths found for {s3_key}")
                continue
            df = query_keywords_from_s3(parquet_paths, s3_config_key)
            store_id_map = query_store_id_map_cached(pgdb, store)
            df["store_app"] = df["store_id"].map(
                store_id_map.set_index("store_id")["id"].to_dict()
            )
            df["country"] = df["country"].map(
                country_map.set_index("alpha2")["id"].to_dict()
            )
            if df["store_app"].isna().any():
                check_and_insert_new_apps(
                    pgdb=pgdb,
                    dicts=df.to_dict(orient="records"),
                    crawl_source="keywords",
                    store=store,
                )
                store_id_map = query_store_id_map(pgdb, store)
                df["store_app"] = df["store_id"].map(
                    store_id_map.set_index("store_id")["id"].to_dict()
                )
            assert not df["store_app"].isna().any(), "Missing store_app rows"
            df["store"] = store
            logger.info(
                f"Keywords from S3 insert {snapshot_date} {store=} {df.shape[0]:,} rows"
            )
            delete_and_insert(
                df=df,
                table_name="app_keyword_ranks_daily",
                schema="frontend",
                pgdb=pgdb,
                delete_by_keys=["crawled_date", "store"],
                insert_columns=[
                    "country",
                    "keyword_id",
                    "store",
                    "crawled_date",
                    "store_app",
                    "app_rank",
                ],
                delete_keys_have_duplicates=True,
            )


def query_keywords_from_s3(
    parquet_paths: list[str],
    s3_config_key: str,
) -> pd.DataFrame:
    """Query keywords from S3 parquet files."""
    period_query = f"""WITH all_data AS (
               SELECT * FROM read_parquet({parquet_paths})
               WHERE store_id IS NOT NULL
           ),
           latest_per_keyword AS (
               SELECT
                   store,
                   country,
                   keyword_id,
                   rank,
                   MAX(crawled_at) AS latest_crawled_at
               FROM all_data
               GROUP BY store, country, keyword_id, rank
           )
           SELECT
               ar.crawled_date,
               ar.country,
               ar.store,
               ar.rank AS app_rank,
               ar.keyword_id,
               ar.store_id
           FROM all_data ar
           JOIN latest_per_keyword lp
             ON ar.keyword_id = lp.keyword_id
            AND ar.store = lp.store
            AND ar.country = lp.country
            AND ar.rank = lp.rank
            AND ar.crawled_at = lp.latest_crawled_at;
            """
    with get_duckdb_connection(s3_config_key) as duckdb_con:
        return duckdb_con.execute(period_query).df()
