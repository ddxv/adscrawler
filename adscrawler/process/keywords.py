"""Upload and import keyword-rank data through S3 parquet files."""

import datetime
import time
import uuid
from io import BytesIO

import pandas as pd

from adscrawler.app_stores.utils import check_and_insert_new_apps
from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.atomic_swap import atomic_swap_date_partition
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import (
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
    """Rebuild dates with unprocessed files from all available keyword parquets."""
    pipeline_name = "import_keywords_from_s3_v2"
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    for snapshot_date in pd.date_range(start_date, end_date, freq="D"):
        snapshot_date = snapshot_date.date()
        paths_by_store = {}
        for store in [1, 2]:
            s3_key = f"{RAW_DATA_KEYWORDS}/store={store}/crawled_date={snapshot_date}/"
            parquet_paths = get_parquet_paths_by_prefix(bucket, s3_key)
            if not parquet_paths:
                logger.warning(f"No parquet paths found for {s3_key}")
                continue
            paths_by_store[store] = parquet_paths
        if not paths_by_store:
            continue

        all_paths = [path for paths in paths_by_store.values() for path in paths]
        if not filter_unprocessed_s3_files(
            all_paths, pipeline_name=pipeline_name, pgdb=pgdb
        ):
            logger.info(
                "%s %s: all parquet files already processed, skipping",
                pipeline_name,
                snapshot_date,
            )
            continue
        err_msg = None
        status = "failed"
        try:
            frames = [
                process_keywords(paths, s3_config_key, pgdb, store)
                for store, paths in paths_by_store.items()
            ]
            if any(frame.empty for frame in frames):
                raise ValueError("Keyword ranks are empty for a store with files")
            ranks = pd.concat(frames, ignore_index=True)
            columns = [
                "crawled_date",
                "store",
                "country",
                "keyword_id",
                "store_app",
                "app_rank",
            ]
            if ranks.empty or ranks[columns].isna().any().any():
                raise ValueError(
                    "Keyword ranks are empty or contain missing required values"
                )
            dates = pd.to_datetime(ranks["crawled_date"], errors="raise").dt.date
            if not dates.eq(snapshot_date).all():
                raise ValueError("Keyword ranks contain rows for a different date")
            atomic_swap_date_partition(
                ranks[columns],
                pgdb,
                schema="frontend",
                table="app_keyword_ranks_daily",
                partition_prefix="app_keyword_ranks",
                partition_date=snapshot_date,
            )
            status = "completed"
        except Exception as e:
            err_msg = str(e)[:500]
            logger.exception("%s failed for %s", pipeline_name, snapshot_date)
        record_s3_file_status(
            pipeline_name=pipeline_name,
            file_path=all_paths,
            status=status,
            error_message=err_msg,
            pgdb=pgdb,
        )


def process_keywords(
    parquet_paths: list[str], s3_config_key: str, pgdb: PostgresEngine, store: int
) -> pd.DataFrame:
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
    logger.info(f"Keywords from S3 processed {store=} {df.shape[0]:,} rows")
    return df


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
