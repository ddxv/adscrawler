"""Combined domain-app history export + change-detection (domain_app_changes_quarterly)."""

import datetime
import time
import uuid
from io import BytesIO

import pandas as pd

from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import (
    delete_combined_history_by_quarter,
    insert_bulk,
    query_report_combined_domains,
)
from adscrawler.process import (
    AGG_COMBINED_DOMAIN_HISTORY,
    AGG_STORE_APPS_RELEASE_DATES,
)
from adscrawler.process.storage import (
    delete_s3_objects_by_prefix,
    get_s3_client,
)

logger = get_logger(__name__, "scrape_stores")


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


def combined_domain_history_db_to_db(
    pgdb: PostgresEngine,
    start_date: str,
    start_of_next_period: str,
) -> None:
    """Stream combined domain app history from Postgres query directly into
    the adtech.combined_domain_app_history table (DB-to-DB).

    Deletes existing data for the same year/quarter first, then bulk-inserts.
    """
    logger.info(
        "Inserting combined domain history into adtech.combined_domain_app_history "
        f"start={start_date} next={start_of_next_period}"
    )

    df = query_report_combined_domains(
        pgdb=pgdb,
        start_date=start_date,
        start_of_next_period=start_of_next_period,
    )
    year = pd.Timestamp(start_date).year
    quarter = pd.Timestamp(start_date).quarter
    df["year"] = year
    df["quarter"] = quarter

    delete_combined_history_by_quarter(
        pgdb=pgdb,
        delete_year=year,
        delete_quarter=quarter,
    )
    insert_bulk(
        df=df,
        schema="adtech",
        table_name="combined_domain_app_history",
        pgdb=pgdb,
        chunk_size=5000000,
    )


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
    # store_apps_release_dates_to_s3(pgdb)
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

        combined_domain_history_db_to_db(
            pgdb=pgdb,
            start_date=str(start_date.date()),
            start_of_next_period=str(start_of_next_period.date()),
        )
