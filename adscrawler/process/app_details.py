"""Raw app details and keywords – upload to & import from S3 parquet."""

import datetime
import time
import uuid
from io import BytesIO

import pandas as pd

from adscrawler.app_stores.utils import (
    check_and_insert_new_apps,
)
from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import (
    delete_and_insert,
    query_countries,
    query_store_id_map,
    query_store_id_map_cached,
)
from adscrawler.process import (
    RAW_DATA_APP_DETAILS,
    RAW_DATA_APP_DETAILS_INCOMING,
    RAW_DATA_KEYWORDS,
)
from adscrawler.process.storage import (
    delete_s3_objects_by_keys,
    filter_unprocessed_s3_files,
    get_duckdb_connection,
    get_parquet_paths_by_prefix,
    get_s3_client,
    get_s3_dirs_by_prefix,
    get_s3_objects_metadata,
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


def compact_incoming_app_details(
    store: int,
    crawled_date: str,
) -> None:
    """Compact all incoming app-detail parquets into single larger parquet files.

    Reads all parquet files from ``raw-data/_incoming/app_details/`` for the given
    ``store``/``crawled_date``, compacts them per country into larger parquet files
    using DuckDB, and writes them to ``raw-data/app_details/``.

    Args:
        store: Store ID (1 = Google Play, 2 = App Store).
        crawled_date: ISO-format date string (e.g. ``"2026-07-09"``).
    """
    log_info = f"compacting: {crawled_date=} {store=}"
    logger.info(f"{log_info} start")
    bucket = CONFIG["s3"]["bucket"]

    prefix = (
        f"{RAW_DATA_APP_DETAILS_INCOMING}/store={store}/crawled_date={crawled_date}/"
    )
    dirs = get_s3_dirs_by_prefix(bucket, prefix)
    countries = [x.split("country=")[-1].replace("/", "") for x in dirs]

    if not countries:
        logger.warning(f"No incoming directories found at {prefix}")
        return

    for country in countries:
        try:
            incoming_prefix = (
                f"{RAW_DATA_APP_DETAILS_INCOMING}"
                f"/store={store}/crawled_date={crawled_date}/country={country}/"
            )
            output_dir = (
                f"{RAW_DATA_APP_DETAILS}"
                f"/store={store}/crawled_date={crawled_date}/country={country}"
            )

            country_parquet_paths = get_parquet_paths_by_prefix(bucket, incoming_prefix)
            glob_path = f"s3://{bucket}/{incoming_prefix}*.parquet"
            epoch_ms = int(time.time() * 1000)

            with get_duckdb_connection("s3") as duckdb_con:
                res = duckdb_con.execute(f"""
                    COPY (
                        SELECT * FROM read_parquet('{glob_path}', union_by_name=true)
                    ) TO 's3://{bucket}/{output_dir}' (
                        FORMAT PARQUET,
                        PARTITION_BY (crawl_result),
                        FILENAME_PATTERN 'compacted_{epoch_ms}_{{i}}',
                        OVERWRITE_OR_IGNORE 1,
                        COMPRESSION 'zstd'
                    );
                """)
                copied_count = res.fetchone()[0]

                # Read back from source glob to verify row counts match
                source_count = duckdb_con.execute(
                    f"SELECT count(*) FROM read_parquet('{glob_path}', union_by_name=true)"
                ).fetchone()[0]

                logger.info(
                    f"{log_info} {country=} compacted {copied_count}/{source_count} rows"
                )

                if source_count != copied_count or copied_count == 0:
                    raise ValueError(
                        f"Row count mismatch! Source had {source_count} rows, but {copied_count} were copied."
                    )

            delete_s3_objects_by_keys(bucket=bucket, s3_paths=country_parquet_paths)
            logger.info(
                f"{log_info} {country=} Deleted {len(country_parquet_paths)} incoming files"
            )

        except Exception as e:
            logger.exception(f"Error compacting {log_info} {country=}: {e}")

    logger.info(f"{log_info} finished")


def import_app_details_from_s3_into_db(
    store: int,
    crawled_date: str,
    pgdb: PostgresEngine,
) -> None:
    """Read app-detail parquets from S3 for ``store``/``crawled_date``/US and
    upsert into the database via ``process_live_app_details``.

    The function reads all parquet files under
    ``raw-data/app_details/store={store}/crawled_date={crawled_date}/country=US/crawl_result=1/``,
    resolves the ``store_app`` primary key for each row, and delegates to
    :func:`adscrawler.app_stores.scrape_stores.process_live_app_details`.

    Args:
        store: Store ID (1 = Google Play, 2 = App Store).
        crawled_date: ISO-format date string (e.g. ``"2026-07-02"``).
        pgdb: Database connection.
    """
    pipeline_name = "import_app_details_from_s3"
    log_info = f"{pipeline_name} {store=} {crawled_date=} country=US"
    logger.info(f"{log_info} start")

    bucket = CONFIG["s3"]["bucket"]

    prefix = f"{RAW_DATA_APP_DETAILS}/store={store}/crawled_date={crawled_date}/country=US/crawl_result=1/"
    parquet_paths = get_parquet_paths_by_prefix(bucket, prefix)
    if not parquet_paths:
        logger.warning(f"No app_details parquet files found at {prefix}")
        return

    # Skip files already recorded as completed for this pipeline
    unprocessed_parquets = filter_unprocessed_s3_files(
        parquet_paths, pipeline_name=pipeline_name, pgdb=pgdb
    )
    if not unprocessed_parquets:
        logger.info(
            "{log_info} all app_details parquet files already processed, skipping"
        )
        return

    s3_metadata_map = get_s3_objects_metadata(bucket, unprocessed_parquets)

    for parquet_path in unprocessed_parquets:
        file_meta = s3_metadata_map.get(parquet_path, {})
        etag = file_meta.get("etag")
        size = file_meta.get("size")
        rows_processed = 0
        err_msg = None
        with get_duckdb_connection("s3") as duckdb_con:
            # Check if the parquet file is empty before processing
            count_query = f"SELECT COUNT(*) FROM read_parquet('{parquet_path}', union_by_name=true)"
            row_count = duckdb_con.execute(count_query).fetchone()[0]
        chunk_est = max(1, row_count // 2048)
        try:
            with get_duckdb_connection("s3") as duckdb_con:
                # Execute query without calling .df() immediately
                rel = duckdb_con.execute(
                    f"SELECT * FROM read_parquet('{parquet_path}', union_by_name=true)"
                )
                # vectors * 2,048 = rows
                i = 0
                while True:
                    i += 1
                    chunk_info = (
                        f"chunk {i}/{chunk_est} rows {rows_processed}/{row_count}"
                    )
                    logger.info(f"{log_info} {chunk_info}")
                    df_chunk = rel.fetch_df_chunk(vectors_per_chunk=5)
                    if df_chunk.empty:
                        break
                    process_chunk(df_chunk, store, pgdb)
                    rows_processed += len(df_chunk)
            status = "completed"
        except Exception as e:
            status = "failed"
            err_msg = str(e)[:1000]
            logger.exception(f"Error processing parquet {parquet_path}: {e}")
        record_s3_file_status(
            pipeline_name=pipeline_name,
            file_path=parquet_path,
            status=status,
            pgdb=pgdb,
            row_count=rows_processed,
            error_message=err_msg,
            e_tag=etag,
            file_size_bytes=size,
        )


def process_chunk(df_chunk: pd.DataFrame, store: int, pgdb: PostgresEngine) -> None:
    """Process a chunk of app details DataFrame."""
    if df_chunk.empty:
        logger.warning("Empty dataset!")
        return

    df_chunk = df_chunk[df_chunk["crawl_result"] == 1]
    df_chunk["store_app"] = df_chunk["store_app_db_id"].astype(int)

    # Some data is pulled specifically for new apps, but since other crawls don't have it, they have null values
    cols_to_drop = ["icon_url_100", "icon_128", "icon_64"]
    for col in cols_to_drop:
        if col in df_chunk.columns:
            df_chunk = df_chunk.drop(columns=[col])

    missing = df_chunk["store_app"].isna()
    if missing.any():
        logger.warning(
            f"DROPPING {missing.sum()} rows with unknown store_ids "
            f"(not yet in the store_apps table)"
        )
        df_chunk = df_chunk[~missing]

    if df_chunk.empty:
        logger.warning("No rows left after resolving store_app IDs")
        return

    from adscrawler.app_stores.scrape_stores import (
        process_live_app_details,  # noqa: PLC0415
    )

    process_live_app_details(
        store=store,
        results_df=df_chunk,
        pgdb=pgdb,
        process_icon=False,
    )


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
