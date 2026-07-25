"""Process version details to matched store_app + sdk_id"""

import datetime
import io
import time
import urllib.parse
import uuid

import pandas as pd

from adscrawler.config import CONFIG, get_logger
from adscrawler.process import (
    AGG_MATCHED_SDKS,
    AGG_PATTERN_MATCHES,
    AGG_VERSION_DETAILS,
    RAW_DATA_VERSION_DETAILS,
    RAW_DATA_VERSION_DETAILS_INCOMING,
)
from adscrawler.process.storage import (
    delete_s3_objects_by_prefix,
    get_duckdb_connection,
    get_parquet_paths_by_prefix,
    get_s3_client,
    get_s3_dirs_by_prefix,
)

logger = get_logger(__name__, "version_details")

# Default row-group size for DuckDB parquet writer.
_ROW_GROUP_SIZE = 100_000

# Shared SQL snippet that produces the string_bucket partition label from a `sid` column.
# Groups every 5 Mio string_id values into labels like ``00M-05M``, ``05M-10M``, etc.
_STRING_BUCKET_SQL = (
    "LPAD((DIV(sid, 5000000) * 5)::VARCHAR, 2, '0')"
    " || 'M-'"
    " || LPAD(((DIV(sid, 5000000) + 1) * 5)::VARCHAR, 2, '0')"
    " || 'M'"
)


def compact_incoming_to_raw_archive(date_str: str) -> None:
    """Read incoming micro-files and write to daily raw partition storage.

    The glob picks up *all* incoming parquet files under
    ``raw-data/_incoming/version-details-map/`` regardless of their individual upload
    dates.  All rows are stamped with the single ``date_str`` passed in (one compaction
    run = one logical batch), NOT filtered by upload date.
    """
    bucket = CONFIG["s3"]["bucket"]
    raw_output_path = f"s3://{bucket}/{RAW_DATA_VERSION_DETAILS}/"

    # Collect *all* incoming parquet paths up front.
    # We delete only these exact keys later so that any file written by
    # a concurrent write_version_details_to_s3() between now and the delete
    # is NOT silently dropped.
    prefix = f"{RAW_DATA_VERSION_DETAILS_INCOMING}/"
    dirs = get_s3_dirs_by_prefix(bucket, prefix)
    if not dirs:
        logger.info("No incoming directories found.")
        return

    all_incoming_paths: list[str] = []
    all_incoming_keys: list[str] = []
    for s3_dir in dirs:
        for s3_path in get_parquet_paths_by_prefix(bucket, s3_dir):
            all_incoming_paths.append(f"s3://{bucket}/{s3_path}")
            all_incoming_keys.append(s3_path)

    if not all_incoming_paths:
        logger.info("No incoming parquet files found.")
        return

    with get_duckdb_connection("s3") as duckdb_con:
        duckdb_con.execute(f"""
            COPY (
                WITH prepared AS (
                    SELECT 
                        CAST(string_id AS BIGINT) AS sid,
                        version_code_id
                    FROM read_parquet({all_incoming_paths}, union_by_name=true)
                    WHERE string_id IS NOT NULL
                )
                SELECT
                    {_STRING_BUCKET_SQL} AS string_bucket,
                    '{date_str}' AS date,
                    sid AS string_id,
                    version_code_id
                FROM prepared
                ORDER BY string_id ASC, version_code_id ASC
            ) TO '{raw_output_path}' (
                FORMAT PARQUET,
                PARTITION_BY (string_bucket, date),
                COMPRESSION 'zstd',
                ROW_GROUP_SIZE {_ROW_GROUP_SIZE}
            )
        """)

    # Delete only the specific files we just archived — not the whole prefix.
    # This avoids the race where a file written between the COPY and this
    # delete would be removed without being archived.
    s3 = get_s3_client()
    for i in range(0, len(all_incoming_keys), 1000):
        batch = [{"Key": k} for k in all_incoming_keys[i : i + 1000]]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
    logger.info(f"Archived {len(all_incoming_paths)} incoming files for {date_str}")


def write_version_details_to_s3(
    version_details_df: pd.DataFrame,
    store_id: str,
) -> None:
    """Write ``(version_code_id, string_id)`` pairs to an incoming S3 parquet.

    The DataFrame must have at least ``version_code_id`` and ``string_id`` columns.

    Args:
        version_details_df: DataFrame with ``version_code_id`` and ``string_id`` columns.
        store_id: Human-readable store ID for logging (e.g. ``"com.example.app"``).
    """
    version_details_df = version_details_df[
        ["version_code_id", "string_id"]
    ].drop_duplicates()
    s3_client = get_s3_client()
    bucket = CONFIG["s3"]["bucket"]
    date_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    epoch_ms = int(time.time() * 1000)
    suffix = uuid.uuid4().hex[:8]
    file_name = f"version_details_{epoch_ms}_{suffix}.parquet"
    s3_key = f"{RAW_DATA_VERSION_DETAILS_INCOMING}/date={date_str}/{file_name}"
    buffer = io.BytesIO()

    version_details_df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, bucket, s3_key)
    logger.info(
        f"{store_id=} wrote {len(version_details_df)} rows to s3://{bucket}/{s3_key}"
    )


def build_aggregated_master_buckets() -> None:
    """Rebuild the deduplicated, globally sorted master query files in agg-data."""
    bucket = CONFIG["s3"]["bucket"]

    # Read everything under raw-data (backfill + daily date subfolders)
    agg_tmp_output = f"s3://{bucket}/{AGG_VERSION_DETAILS}_tmp/"
    agg_final_output = f"s3://{bucket}/{AGG_VERSION_DETAILS}/"

    logger.info("Rebuilding aggregated master buckets in agg-data...")

    # Wipe any stale remnants from a prior failed run before writing new data.
    delete_s3_objects_by_prefix(bucket, f"{AGG_VERSION_DETAILS}_tmp/")

    # Collect all parquet paths under raw-data.
    raw_prefix = f"{RAW_DATA_VERSION_DETAILS}/"
    dirs = get_s3_dirs_by_prefix(bucket, raw_prefix)
    if not dirs:
        logger.warning("No raw-data directories found; nothing to aggregate.")
        return
    raw_paths: list[str] = []
    for s3_dir in dirs:
        for s3_path in get_parquet_paths_by_prefix(bucket, s3_dir):
            raw_paths.append(f"s3://{bucket}/{s3_path}")
    if not raw_paths:
        logger.warning("No parquet files found under raw-data; nothing to aggregate.")
        return

    with get_duckdb_connection("s3") as duckdb_con:
        duckdb_con.execute(f"""
            COPY (
                WITH prepared AS (
                    SELECT DISTINCT
                        CAST(string_id AS BIGINT) AS sid,
                        version_code_id
                    FROM read_parquet({raw_paths}, union_by_name=true)
                    WHERE string_id IS NOT NULL
                )
                SELECT
                    {_STRING_BUCKET_SQL} AS string_bucket,
                    sid AS string_id,
                    version_code_id
                FROM prepared
                ORDER BY string_id ASC, version_code_id ASC
            ) TO '{agg_tmp_output}' (
                FORMAT PARQUET,
                PARTITION_BY (string_bucket),
                COMPRESSION 'zstd',
                ROW_GROUP_SIZE {_ROW_GROUP_SIZE},
                OVERWRITE_OR_IGNORE true
            )
        """)

        # Calculate row counts for validation (from the deduplicated tmp output)
        row_count = duckdb_con.execute(f"""
            SELECT count(*)
            FROM read_parquet('{agg_tmp_output}*/*.parquet', hive_partitioning=true)
        """).fetchone()[0]

        logger.info(f"Aggregated master dataset contains {row_count:,} unique rows.")

    # Validation: refuse to publish an empty or trivially-small dataset.
    if row_count == 0:
        logger.error("Refusing to publish: aggregated master dataset is empty.")
        return
    # Compare against existing agg-data row count if available.
    try:
        with get_duckdb_connection("s3") as duckdb_con:
            existing_count = duckdb_con.execute(f"""
                SELECT count(*)
                FROM read_parquet('{agg_final_output}*/*.parquet', hive_partitioning=true)
            """).fetchone()[0]
        if existing_count > 0 and row_count < existing_count * 0.5:
            logger.warning(
                f"New dataset ({row_count:,} rows) is <50%% of existing "
                f"({existing_count:,} rows) — likely incomplete, refusing publish."
            )
            return
    except Exception:
        logger.info("No existing agg-data to compare against; proceeding.")

    # Swap tmp directory to final query location
    replace_s3_prefix(bucket, f"{AGG_VERSION_DETAILS}_tmp/", f"{AGG_VERSION_DETAILS}/")
    logger.info("Master aggregation successfully updated in agg-data.")


def replace_s3_prefix(bucket: str, src_prefix: str, dst_prefix: str) -> None:
    """Safely replace dst_prefix with contents of src_prefix using boto3.

    Order of operations for atomic-like swap safety:
    1. Copy src -> dst (creates/overwrites all incoming files).
    2. Identify and delete any stale keys in dst that were NOT part of the new copy.
    3. Delete src prefix.
    """
    s3 = get_s3_client()

    src_prefix = src_prefix.rstrip("/") + "/"
    dst_prefix = dst_prefix.rstrip("/") + "/"

    paginator = s3.get_paginator("list_objects_v2")

    # Step 1: Copy src -> dst & track all newly written keys in dst
    new_dst_keys: set[str] = set()
    src_keys: list[dict[str, str]] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=src_prefix):
        for obj in page.get("Contents", []):
            src_key = obj["Key"]
            relative_path = src_key[len(src_prefix) :]
            dst_key = dst_prefix + relative_path

            # Track destination key so we don't accidently delete it in Step 2
            new_dst_keys.add(dst_key)

            encoded_src_key = urllib.parse.quote(src_key)
            s3.copy_object(
                Bucket=bucket,
                CopySource=f"{bucket}/{encoded_src_key}",
                Key=dst_key,
            )

            src_keys.append({"Key": src_key})

    # Step 2: Delete stale leftovers in dst (keys that exist in dst but were NOT copied in Step 1)
    stale_dst_keys: list[dict[str, str]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=dst_prefix):
        for obj in page.get("Contents", []):
            dst_key = obj["Key"]
            if dst_key not in new_dst_keys:
                stale_dst_keys.append({"Key": dst_key})
                if len(stale_dst_keys) == 1000:
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": stale_dst_keys})
                    stale_dst_keys = []

    if stale_dst_keys:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": stale_dst_keys})

    # Step 3: Delete all src keys now that both copy and dst cleanup are verified
    for i in range(0, len(src_keys), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": src_keys[i : i + 1000]})


def build_aggregated_pattern_matches() -> None:
    """Run pattern matching over lookups and save bucketed results to S3."""

    bucket = CONFIG["s3"]["bucket"]

    strings_path = f"s3://{bucket}/lookups/version_strings.parquet"
    pkg_path = f"s3://{bucket}/lookups/sdk_packages.parquet"
    paths_path = f"s3://{bucket}/lookups/sdk_paths.parquet"
    med_path = f"s3://{bucket}/lookups/sdk_mediation_patterns.parquet"

    agg_tmp_output = f"s3://{bucket}/{AGG_PATTERN_MATCHES}_tmp/"

    logger.info("Starting aggregated pattern matching build...")

    # Wipe any stale remnants from a prior failed run before writing new data.
    delete_s3_objects_by_prefix(bucket, f"{AGG_PATTERN_MATCHES}_tmp/")

    with get_duckdb_connection("s3") as duckdb_con:
        duckdb_con.execute(f"""
            COPY (
                WITH strings AS ( 
                    SELECT 
                        id AS string_id, 
                        lower(value_name) AS val, 
                        lower(xml_path) AS path 
                    FROM read_parquet('{strings_path}')
                    WHERE value_name IS NOT NULL OR xml_path IS NOT NULL
                ),
                raw_matches AS (
                    -- 1. Package patterns
                    SELECT 
                        s.string_id, 
                        sp.sdk_id AS sdk_id
                    FROM strings s 
                    JOIN read_parquet('{pkg_path}') sp 
                      ON s.val IS NOT NULL 
                     AND STARTS_WITH(s.val, lower(sp.package_pattern)) 

                    UNION ALL

                    -- 2. Path patterns 
                    SELECT 
                        s.string_id, 
                        ptm.sdk_id AS sdk_id
                    FROM strings s 
                    JOIN read_parquet('{paths_path}') ptm 
                      ON s.path IS NOT NULL 
                     AND s.path = lower(ptm.path_pattern) 

                    UNION ALL

                    -- 3. Mediation patterns 
                    SELECT 
                        s.string_id, 
                        cmp.sdk_id AS sdk_id
                    FROM strings s 
                    JOIN read_parquet('{med_path}') cmp 
                      ON s.val IS NOT NULL 
                     AND STARTS_WITH(s.val, lower(cmp.mediation_pattern) || '.')
                ),
                deduped AS (
                    SELECT DISTINCT 
                        CAST(string_id AS BIGINT) AS sid,
                        CAST(sdk_id AS INTEGER) AS sdk_id
                    FROM raw_matches
                )
                SELECT
                    {_STRING_BUCKET_SQL} AS string_bucket,
                    sid AS string_id,
                    sdk_id
                FROM deduped
                ORDER BY string_id ASC, sdk_id ASC
            ) TO '{agg_tmp_output}' (
                FORMAT PARQUET,
                PARTITION_BY (string_bucket),
                COMPRESSION 'zstd',
                ROW_GROUP_SIZE {_ROW_GROUP_SIZE},
                OVERWRITE_OR_IGNORE true
            )
        """)

        # Validation count
        match_count = duckdb_con.execute(f"""
            SELECT count(*)
            FROM read_parquet('{agg_tmp_output}*/*.parquet', hive_partitioning=true)
        """).fetchone()[0]

        logger.info(
            f"Pattern matching complete. Generated {match_count:,} unique matches."
        )

    # Validation: refuse to publish an empty dataset.
    if match_count == 0:
        logger.error("Refusing to publish: pattern matches dataset is empty.")
        return

    # Atomic swap to final query zone
    replace_s3_prefix(bucket, f"{AGG_PATTERN_MATCHES}_tmp/", f"{AGG_PATTERN_MATCHES}/")
    logger.info("Successfully updated agg-data/pattern-matches in S3.")


def build_aggregated_matched_sdks() -> None:
    """Build matched SDKs artifact by joining version-details-map with pattern-matches."""
    bucket = CONFIG["s3"]["bucket"]

    vdm_path = f"s3://{bucket}/{AGG_VERSION_DETAILS}/"
    pm_path = f"s3://{bucket}/{AGG_PATTERN_MATCHES}/"
    vc_path = f"s3://{bucket}/lookups/version_codes.parquet"

    agg_tmp_output = f"s3://{bucket}/{AGG_MATCHED_SDKS}_tmp/"
    agg_final_output = f"s3://{bucket}/{AGG_MATCHED_SDKS}/"

    logger.info("Building aggregated matched SDKs...")

    # Wipe any stale remnants from a prior failed run.
    delete_s3_objects_by_prefix(bucket, f"{AGG_MATCHED_SDKS}_tmp/")

    with get_duckdb_connection("s3") as duckdb_con:
        # Check that both input datasets exist.
        has_vdm = (
            duckdb_con.execute(
                "SELECT count(*) FROM glob(:path)",
                {"path": f"{vdm_path}*/*.parquet"},
            ).fetchone()[0]
            > 0
        )
        has_pm = (
            duckdb_con.execute(
                "SELECT count(*) FROM glob(:path)",
                {"path": f"{pm_path}*/*.parquet"},
            ).fetchone()[0]
            > 0
        )
        if not has_vdm or not has_pm:
            logger.warning(
                "Missing parquet files in VDM or pattern-matches; "
                "cannot build matched SDKs."
            )
            return

        duckdb_con.execute(f"""
            COPY (
                WITH raw_version_sdks AS (
                    SELECT DISTINCT
                        vc.store_app_id,
                        vdm.version_code_id,
                        vc.created_at::DATE AS version_code_created_at,
                        pm.sdk_id
                    FROM read_parquet('{vdm_path}*/*.parquet', hive_partitioning=true) vdm
                    JOIN read_parquet('{pm_path}*/*.parquet', hive_partitioning=true) pm
                      ON vdm.string_id = pm.string_id
                    JOIN read_parquet('{vc_path}') vc
                      ON vdm.version_code_id = vc.id
                )
                SELECT 
                    store_app_id,
                    version_code_id,
                    version_code_created_at,
                    sdk_id
                FROM raw_version_sdks
                ORDER BY store_app_id ASC, version_code_created_at ASC
            ) TO '{agg_tmp_output}' (
                FORMAT PARQUET,
                PARTITION_BY (sdk_id),
                COMPRESSION 'zstd',
                ROW_GROUP_SIZE {_ROW_GROUP_SIZE},
                OVERWRITE_OR_IGNORE true
            )
        """)

        # Validate output has rows before swapping.
        matched_count = duckdb_con.execute(f"""
            SELECT count(*)
            FROM read_parquet('{agg_tmp_output}*/*.parquet', hive_partitioning=true)
        """).fetchone()[0]
        logger.info(f"Matched SDKs dataset contains {matched_count:,} rows.")

    if matched_count == 0:
        logger.error("Refusing to publish: matched SDKs dataset is empty.")
        return

    # Swap tmp to final
    replace_s3_prefix(bucket, f"{AGG_MATCHED_SDKS}_tmp/", f"{AGG_MATCHED_SDKS}/")
    logger.info("Successfully updated agg-data/matched-sdks in S3.")


def map_version_details(
    date_str: str | None = None,
    run_compaction: bool = True,
    rebuild_master: bool = True,
    rebuild_patterns: bool = False,
    rebuild_matched_sdks: bool = True,
    sync_postgres: bool = False,
) -> None:
    """Orchestrate the entire version details processing pipeline.

    Flow:
    1. Compact micro-batch incoming files to raw partition storage.
    2. Rebuild master deduplicated agg-data buckets for version details.
    3. (Optional) Rebuild pattern matches lookup if rules changed.
    4. Build aggregated matched SDKs artifact (joins version details & pattern matches).
    5. (Optional) Compute window functions and sync state changes to Postgres.

    Args:
        date_str: Logical partition date for incoming compaction (YYYY-MM-DD).
                 Defaults to current UTC date if None.
        run_compaction: Compact `raw-data/_incoming/` into `raw-data/`.
        rebuild_master: Re-aggregate master `agg-data/version-details-map/`.
        rebuild_patterns: Re-run pattern matcher engine against lookups.
        rebuild_matched_sdks: Re-join VDM with pattern-matches into `agg-data/matched-sdks/`.
        sync_postgres: Compute presence diffs/LAG and push to `adtech.store_app_sdk_changes`.
    """
    if date_str is None:
        date_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")

    start_time = time.time()
    logger.info(f"Starting map_version_details entrypoint run for date={date_str}")

    # Stage 1: Compact Incoming Buffer -> Raw Daily Storage
    if run_compaction:
        logger.info("--- Stage 1: Compacting incoming version details ---")
        try:
            compact_incoming_to_raw_archive(date_str=date_str)
        except Exception as e:
            logger.error(f"Stage 1 failed (compaction): {e}")
            raise

    # Stage 2: Master Bucket Aggregation
    if rebuild_master:
        logger.info("--- Stage 2: Rebuilding aggregated master version details ---")
        try:
            build_aggregated_master_buckets()
        except Exception as e:
            logger.error(f"Stage 2 failed (master aggregation): {e}")
            raise

    # Stage 3: Pattern Matcher Engine (Optional / Rule Changes)
    if rebuild_patterns:
        logger.info("--- Stage 3: Rebuilding aggregated pattern matches ---")
        try:
            build_aggregated_pattern_matches()
        except Exception as e:
            logger.error(f"Stage 3 failed (pattern matching): {e}")
            raise

    # Stage 4: Matched SDKs Join
    if rebuild_matched_sdks:
        logger.info("--- Stage 4: Building aggregated matched SDKs ---")
        try:
            build_aggregated_matched_sdks()
        except Exception as e:
            logger.error(f"Stage 4 failed (matched SDKs build): {e}")
            raise

    # Stage 5: Postgres B2B API Layer Sync (Window Functions)
    if sync_postgres:
        logger.info("--- Stage 5: Syncing SDK changes to Postgres API Layer ---")
        try:
            # Place holder call to your windowing & PG insertion logic
            # e.g., sync_store_app_sdk_changes_to_postgres()
            pass
        except Exception as e:
            logger.error(f"Stage 5 failed (Postgres sync): {e}")
            raise

    elapsed = time.time() - start_time
    logger.info(f"Completed map_version_details run in {elapsed:.2f}s")
