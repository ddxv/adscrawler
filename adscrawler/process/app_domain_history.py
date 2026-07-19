"""Combined domain-app history export + change-detection (domain_app_changes_quarterly)."""

import datetime
import pathlib
import time
import uuid
from io import BytesIO
from typing import Any

import pandas as pd

from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.atomic_swap import atomic_swap_partition
from adscrawler.dbcon.queries import (
    delete_combined_history_by_quarter,
    insert_bulk,
    query_report_combined_domains,
    CREATE_DOMAIN_APP_CHANGES,
    PG_CACHE_TABLES,
    CREATE_TREND_DOMAINS,
    CREATE_TREND_COMPANIES,
    CREATE_TREND_PARENT_COMPANIES,
)
from adscrawler.process import (
    AGG_COMBINED_DOMAIN_HISTORY,
    AGG_STORE_APPS_RELEASE_DATES,
)
from adscrawler.process.storage import (
    delete_s3_objects_by_prefix,
    get_parquet_paths_by_prefix,
    get_s3_client,
    get_duckdb_connection,
)

logger = get_logger(__name__, "scrape_stores")


def pg_db_uri():
    """Return a Postgres connection URI string for the configured database."""
    db_config = CONFIG["madrone"]
    user = db_config["db_user"]
    password = db_config["db_password"]
    host = db_config["host"]
    database = db_config["db"]
    return f"dbname={database} host={host} user={user} password={password}"


def _run_multi_statement(conn: Any, raw_sql: str, params: dict[str, str]) -> None:
    """Execute a multi-statement SQL blob with manual param substitution.

    DuckDB prepared parameters only work with single-statement SQL, so we
    substitute ``$var`` and ``{var}`` placeholders manually before splitting.
    """
    substituted = raw_sql
    for key, value in params.items():
        substituted = substituted.replace(f"${key}", value).replace(f"{{{key}}}", value)
    for stmt in (s.strip() for s in substituted.split(";") if s.strip()):
        conn.execute(stmt)


def run_changes(pgdb: PostgresEngine) -> None:

    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    parquet_files = get_parquet_paths_by_prefix(
        bucket=bucket, prefix=AGG_COMBINED_DOMAIN_HISTORY
    )

    tmp_domain_changes = pathlib.Path("/tmp/domain_app_changes.parquet")
    tmp_domain_changes.unlink(missing_ok=True)
    tmp_trend_domains = pathlib.Path("/tmp/trend_domains.parquet")
    tmp_trend_domains.unlink(missing_ok=True)
    tmp_trend_companies = pathlib.Path("/tmp/trend_companies.parquet")
    tmp_trend_companies.unlink(missing_ok=True)
    tmp_trend_parent_companies = pathlib.Path("/tmp/trend_parent_companies.parquet")
    tmp_trend_parent_companies.unlink(missing_ok=True)

    # Convert list of parquet paths into a DuckDB list literal: ['p1', 'p2', ...]
    parquet_files_literal = "[" + ", ".join(f"'{p}'" for p in parquet_files) + "]"

    db_uri = pg_db_uri()
    store_apps_key = f"s3://{bucket}/{AGG_STORE_APPS_RELEASE_DATES}/store_apps.parquet"

    with get_duckdb_connection(s3_config_key) as duckdb_con:
        logger.info("Create Domain Changes")
        _run_multi_statement(
            duckdb_con,
            CREATE_DOMAIN_APP_CHANGES,
            {
                "parquet_files": parquet_files_literal,
                "store_apps_key": f"['{store_apps_key}']",
            },
        )
        logger.info("Caching Postgres lookup tables into DuckDB...")
        _run_multi_statement(
            duckdb_con,
            PG_CACHE_TABLES,
            {"db_uri": db_uri},
        )
        logger.info("Computing domain trends...")
        _run_multi_statement(duckdb_con, CREATE_TREND_DOMAINS, {})
        logger.info("Computing company trends...")
        _run_multi_statement(duckdb_con, CREATE_TREND_COMPANIES, {})
        logger.info("Computing parent-company trends...")
        _run_multi_statement(duckdb_con, CREATE_TREND_PARENT_COMPANIES, {})

    batch_date = datetime.date.today()

    df = pd.read_parquet(tmp_domain_changes)
    df["batch_date"] = batch_date
    atomic_swap_partition(df, pgdb, schema="adtech", table="domain_app_changes")

    df = pd.read_parquet(tmp_trend_domains)
    df["batch_date"] = batch_date
    atomic_swap_partition(df, pgdb, schema="adtech", table="trend_domains_test")

    df = pd.read_parquet(tmp_trend_companies)
    df["batch_date"] = batch_date
    atomic_swap_partition(df, pgdb, schema="adtech", table="trend_companies_test")

    df = pd.read_parquet(tmp_trend_parent_companies)
    df["batch_date"] = batch_date
    atomic_swap_partition(
        df, pgdb, schema="adtech", table="trend_parent_companies_test"
    )


def combined_domain_history_to_s3(
    pgdb: PostgresEngine,
    start_date: str,
    start_of_next_period: str,
    year: int,
    quarter: int,
    chunk_size: int = 1_000_000,
) -> None:
    """Stream combined domain app history data from Postgres to S3 as parquet.

    The query_report_combined_domains query can return 100M+ rows (GB+ of data).
    This function streams it in chunks to avoid loading everything into memory,
    writing each chunk as a separate parquet file to::

      s3://bucket/agg-data/combined-domain-app-history-quarter/year={year}/quarter={quarter}/
    """
    s3_client = get_s3_client()
    bucket = CONFIG["s3"]["bucket"]
    prefix = f"{AGG_COMBINED_DOMAIN_HISTORY}/year={year}/quarter={quarter}"

    # Delete any existing files for this year/quarter before writing fresh data
    logger.info(f"Clearing existing history parquets at s3://{bucket}/{prefix}/")
    delete_s3_objects_by_prefix(bucket=bucket, prefix=f"{prefix}/")

    logger.info(
        "Streaming combined domain history to "
        f"s3://{bucket}/{prefix}/  "
        f"start={start_date} next={start_of_next_period}"
    )

    chunk_iter = query_report_combined_domains(
        pgdb=pgdb,
        start_date=start_date,
        start_of_next_period=start_of_next_period,
        chunksize=chunk_size,
    )

    part = 0
    for chunk in chunk_iter:
        chunk["year"] = year
        chunk["quarter"] = quarter

        epoch_ms = int(time.time() * 1000)
        suffix = uuid.uuid4().hex[:8]
        file_name = f"history_{epoch_ms}_{suffix}.parquet"
        s3_key = f"{prefix}/{file_name}"

        buffer = BytesIO()
        chunk.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3_client.upload_fileobj(buffer, bucket, s3_key)

        part += 1
        logger.info(
            f"Uploaded part {part} ({len(chunk):,} rows) to s3://{bucket}/{s3_key}"
        )

    logger.info(f"Finished {part=} written to s3://{bucket}/{prefix}/")


def store_apps_release_dates_to_s3(pgdb: PostgresEngine) -> None:
    """Export store_apps (id, release_date, store) to a static parquet in S3.

    Used by both ``build_domain_app_changes_quarterly_query`` (release_date
    for added_initial logic) and ``build_domain_app_market_share_query``
    (store for per-store breakdowns).
    """
    s3_client = get_s3_client()
    bucket = CONFIG["s3"]["bucket"]
    s3_key = f"{AGG_STORE_APPS_RELEASE_DATES}/store_apps.parquet"

    logger.info("Exporting store_apps (id, release_date, store) to S3")
    df = pd.read_sql(
        "SELECT id, release_date, store FROM store_apps;",
        con=pgdb.engine,
    )
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, bucket, s3_key)
    logger.info(f"Uploaded {len(df):,} store_app rows to s3://{bucket}/{s3_key}")


def process_company_history(pgdb: PostgresEngine) -> None:
    """Export combined domain-app history + store_apps release-dates lookup to S3.

    Iterates from Q1 2025 up to the current quarter, exporting each quarter's
    combined domain-app history to S3 as partitioned parquet files.
    """

    today = datetime.date.today()
    quarters = pd.date_range(start="2025-01-01", end=today, freq="QS")
    # Export lookup tables once (idempotent)
    store_apps_release_dates_to_s3(pgdb)
    for start_date in quarters:
        start_of_next_period = (
            start_date + pd.offsets.QuarterEnd() + datetime.timedelta(days=1)
        )
        year = start_date.year
        quarter = start_date.quarter
        logger.info(f"Company history: exporting {year=} {quarter=}")
        # Check if 'today' falls within this quarter's standard range
        buffer_start_date = start_date - pd.Timedelta(weeks=3)
        if start_date <= pd.to_datetime(today) < buffer_start_date:
            # Apply the 3-week buffer only for the ongoing quarter
            print(
                "Too early in current quarter detected. Wait for 3-week buffer to start."
            )
            continue  # Skip the ongoing quarter to avoid incomplete data

        combined_domain_history_to_s3(
            pgdb=pgdb,
            start_date=str(start_date.date()),
            start_of_next_period=str(start_of_next_period.date()),
            year=year,
            quarter=quarter,
        )
    run_changes(pgdb=pgdb)
