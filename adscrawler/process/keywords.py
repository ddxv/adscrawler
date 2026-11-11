"""Upload and import keyword-rank data through S3 parquet files."""

import datetime
import time
import uuid
from io import BytesIO

import pandas as pd

from adscrawler.app_stores.utils import check_and_insert_new_apps
from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import (
    delete_and_insert,
    query_countries,
    query_store_id_map,
    query_store_id_map_cached,
)
from adscrawler.process import RAW_DATA_KEYWORDS
from adscrawler.process.storage import (
    filter_unprocessed_s3_files,
    get_duckdb_connection,
    get_parquet_paths_by_prefix,
    get_s3_client,
    record_s3_file_status,
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


def import_keywords_from_s3(
    start_date: datetime.date,
    end_date: datetime.date,
    pgdb: PostgresEngine,
) -> None:
    pipeline_name = "import_keywords_from_s3"
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    for snapshot_date in pd.date_range(start_date, end_date, freq="D"):
        snapshot_date = snapshot_date.date()
        for store in [1, 2]:
            log_info = f"{pipeline_name} {snapshot_date=} {store=}"
            s3_key = f"{RAW_DATA_KEYWORDS}/store={store}/crawled_date={snapshot_date}/"
            parquet_paths = get_parquet_paths_by_prefix(bucket, s3_key)
            if len(parquet_paths) == 0:
                logger.warning(f"No parquet paths found for {s3_key}")
                continue
            unprocessed_parquets = filter_unprocessed_s3_files(
                parquet_paths, pipeline_name=pipeline_name, pgdb=pgdb
            )
            # If any files not processed, need to reprocess all for that day
            if len(unprocessed_parquets) == 0:
                logger.info(f"{log_info} all parquets already processed, skipping")
                continue
            logger.info(f"{log_info} start")
            err_msg = None
            status = "failed"
            try:
                process_keywords(
                    parquet_paths=parquet_paths,
                    s3_config_key=s3_config_key,
                    store=store,
                    pgdb=pgdb,
                )
                status = "completed"
            except Exception as e:
                err_msg = str(e)[0:500]
            record_s3_file_status(
                pipeline_name=pipeline_name,
                file_path=parquet_paths,
                status=status,
                error_message=err_msg,
                pgdb=pgdb,
            )


def process_keywords(
    parquet_paths: list[str], s3_config_key: str, pgdb: PostgresEngine, store: 1
):
    country_map = query_countries(pgdb)
    df = query_keywords_from_s3(parquet_paths, s3_config_key)
    store_id_map = query_store_id_map_cached(pgdb, store)
    df["store_app"] = df["store_id"].map(
        store_id_map.set_index("store_id")["id"].to_dict()
    )
    df["country"] = df["country"].map(country_map.set_index("alpha2")["id"].to_dict())
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
    logger.info(f"Keywords from S3 insert  {store=} {df.shape[0]:,} rows")
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
