"""Process version details to matched store_app + sdk_id"""

import datetime
import io
import time
import uuid

import pandas as pd

from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.atomic_swap import atomic_swap_partition_stream
from adscrawler.process import (
    AGG_MATCHED_SDK_STRINGS,
    AGG_MATCHED_SDK_STRINGS_LATEST,
    AGG_PATTERN_MATCHES,
    AGG_VERSION_DETAILS,
    LOOKUP_SDK_MEDIATION_PATTERNS,
    LOOKUP_SDK_PACKAGE_PATTERNS,
    LOOKUP_SDK_PATH_PATTERNS,
    LOOKUP_VERSION_CODES,
    LOOKUP_VERSION_STRINGS,
    RAW_DATA_VERSION_DETAILS,
    RAW_DATA_VERSION_DETAILS_INCOMING,
    RAW_DATA_VERSION_DETAILS_INITIAL,
    TMP_MATCHED_SDK_STRINGS,
    TMP_MATCHED_SDK_STRINGS_LATEST,
    TMP_PATTERN_MATCHES,
    TMP_VERSION_DETAILS,
)
from adscrawler.process.storage import (
    delete_s3_objects_by_keys,
    delete_s3_objects_by_prefix,
    get_duckdb_connection,
    get_parquet_paths_by_prefix,
    get_s3_client,
    pg_db_uri,
    stream_duckdb_tsv,
)

logger = get_logger(__name__, "version_details")

# Default row-group size for DuckDB parquet writer.
_ROW_GROUP_SIZE = 100_000
_LARGE_ROW_GROUP_SIZE = 1_000_000

# Groups every 5 Mio string_id values into labels like ``000M-005M``, ``005M-100M``, etc.
# Manual boundaries reflecting real density: narrow near the dense low end,
# progressively wider as ids get sparse. Adjust freely.
_BUCKET_BOUNDARIES = [
    0,
    50_000_000,
    100_000_000,
    200_000_000,
    500_000_000,
    1_000_000_000,
    2_000_000_000,
]


def _bucket_label(lo: int, hi: int) -> str:
    # width-4 zero pad supports up to 9999M (~10B), well past the 2B ceiling
    fmt = lambda n: f"{n // 1_000_000:04d}M"
    return f"{fmt(lo)}-{fmt(hi)}"


def _build_bucket_case_sql(boundaries: list[int]) -> str:
    clauses = [
        f"WHEN sid >= {lo} AND sid < {hi} THEN '{_bucket_label(lo, hi)}'"
        for lo, hi in zip(boundaries[:-1], boundaries[1:])
    ]
    overflow_label = f"{boundaries[-1] // 1_000_000:04d}M-plus"
    return "CASE " + " ".join(clauses) + f" ELSE '{overflow_label}' END"


_STRING_BUCKET_SQL = _build_bucket_case_sql(_BUCKET_BOUNDARIES)


def compact_incoming_version_details(date_str: str) -> None:
    """Read incoming micro-files and write to daily raw partition storage.

    The glob picks up incoming parquet files under
    ``raw-data/_incoming/version-details-map/`` .
    """
    if date_str >= datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d"):
        logger.info("Do not run compaction for 'today', need to wait for all data")
        return
    logger.info(f"Compacting incoming version details for {date_str}")
    bucket = CONFIG["s3"]["bucket"]
    raw_output_path = f"s3://{bucket}/{RAW_DATA_VERSION_DETAILS}/"

    # Collect incoming parquet paths up front.
    prefix = f"{RAW_DATA_VERSION_DETAILS_INCOMING}/date={date_str}"
    incoming_keys = get_parquet_paths_by_prefix(bucket, prefix)

    if not incoming_keys:
        logger.info("No incoming parquet files found.")
        return

    with get_duckdb_connection("s3") as duckdb_con:
        res = duckdb_con.execute(f"""
            COPY (
                WITH prepared AS (
                    SELECT 
                        CAST(string_id AS BIGINT) AS sid,
                        version_code_id
                    FROM read_parquet({incoming_keys}, union_by_name=true)
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
                OVERWRITE_OR_IGNORE true,
                COMPRESSION 'zstd',
                ROW_GROUP_SIZE {_ROW_GROUP_SIZE}
            )
        """)
        row_count = res.fetchone()[0]

    # Delete only the specific files we just archived
    delete_s3_objects_by_keys(bucket, incoming_keys)


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


def build_aggregated_version_details() -> None:
    """Rebuild the deduplicated, globally sorted master query files in agg-data."""
    bucket = CONFIG["s3"]["bucket"]

    # Wipe any stale remnants from a prior failed run before writing new data.
    delete_s3_objects_by_prefix(bucket, TMP_VERSION_DETAILS)

    logger.info("Rebuilding aggregated master buckets in agg-data...")
    # Collect all parquet paths under raw-data.
    raw_paths = get_parquet_paths_by_prefix(bucket, f"{RAW_DATA_VERSION_DETAILS}/")
    if not raw_paths:
        logger.warning("No parquet files found under raw-data; nothing to aggregate.")
        return

    raw_vd_glob = f"s3://{bucket}/{RAW_DATA_VERSION_DETAILS}/*/*/*.parquet"

    agg_tmp_output = f"s3://{bucket}/{TMP_VERSION_DETAILS}/"
    with get_duckdb_connection("s3") as duckdb_con:
        duckdb_con.execute(f"""COPY (
                SELECT DISTINCT
                    string_bucket,
                    string_id,
                    version_code_id
                FROM read_parquet('{raw_vd_glob}', hive_partitioning=true)
            ) TO '{agg_tmp_output}' (
                FORMAT PARQUET,
                PARTITION_BY (string_bucket),
                COMPRESSION 'zstd',
                ROW_GROUP_SIZE {_LARGE_ROW_GROUP_SIZE},
                OVERWRITE_OR_IGNORE true
            )
        """)
        # Calculate row counts for validation (from the deduplicated tmp output)
        tmp_parqs = get_parquet_paths_by_prefix(bucket, TMP_VERSION_DETAILS)
        row_count = duckdb_con.execute(f"""
            SELECT count(*)
            FROM read_parquet({tmp_parqs})
        """).fetchone()[0]
        logger.info(f"Aggregated master dataset contains {row_count:,} unique rows.")

    # Validation: refuse to publish an empty or trivially-small dataset.
    if row_count == 0:
        logger.error("Refusing to publish: aggregated master dataset is empty.")
        return
    # Compare against existing agg-data row count if available.
    try:
        with get_duckdb_connection("s3") as duckdb_con:
            agg_parqs = get_parquet_paths_by_prefix(bucket, AGG_VERSION_DETAILS)
            existing_count = duckdb_con.execute(f"""
                SELECT count(*)
                FROM read_parquet({agg_parqs})
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
    replace_s3_prefix(
        bucket, src_prefix=TMP_VERSION_DETAILS, dst_prefix=AGG_VERSION_DETAILS
    )
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

    # Step 1: Copy src -> dst & track all newly written dst keys
    new_dst_keys: set[str] = set()

    src = f"s3://{bucket}/{src_prefix}"
    src_len = len(src)

    src_keys = get_parquet_paths_by_prefix(bucket, src_prefix)

    if len(src_keys) == 0:
        logger.error("no tmp files found!")
        return

    for src_key in src_keys:
        relative_path = src_key[src_len:]
        src_key = src_key.replace("s3://", "")
        dst_key = dst_prefix + relative_path
        # Track destination key so we don't accidently delete it in Step 2
        new_dst_keys.add(f"s3://{bucket}/{dst_key}")
        # encoded_src_key = urllib.parse.quote(src_key)
        s3.copy_object(
            Bucket=bucket,
            CopySource=src_key,
            Key=dst_key,
        )

    # Step 2: Delete stale leftovers in dst (keys not in new_dst_keys)
    stale_keys = [
        {"Key": k}
        for k in get_parquet_paths_by_prefix(bucket, dst_prefix)
        if k not in new_dst_keys
    ]
    for i in range(0, len(stale_keys), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": stale_keys[i : i + 1000]})

    # Step 3: Delete all src keys now that both copy and dst cleanup are verified
    delete_s3_objects_by_prefix(bucket, src_prefix)


def build_aggregated_pattern_matches() -> None:
    """Run pattern matching over lookups and save bucketed results to S3."""

    bucket = CONFIG["s3"]["bucket"]

    strings_path = f"s3://{bucket}/{LOOKUP_VERSION_STRINGS}"
    pkg_path = f"s3://{bucket}/{LOOKUP_SDK_PACKAGE_PATTERNS}"
    paths_path = f"s3://{bucket}/{LOOKUP_SDK_PATH_PATTERNS}"
    med_path = f"s3://{bucket}/{LOOKUP_SDK_MEDIATION_PATTERNS}"

    agg_tmp_output = f"s3://{bucket}/{TMP_PATTERN_MATCHES}/"

    logger.info("Starting aggregated pattern matching build...")

    # Wipe any stale remnants from a prior failed run before writing new data.
    delete_s3_objects_by_prefix(bucket, f"{TMP_PATTERN_MATCHES}/")

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
        tmp_parqs = get_parquet_paths_by_prefix(bucket, TMP_PATTERN_MATCHES)
        match_count = duckdb_con.execute(f"""
            SELECT count(*)
            FROM read_parquet({tmp_parqs})
        """).fetchone()[0]

        logger.info(
            f"Pattern matching complete. Generated {match_count:,} unique matches."
        )

    # Validation: refuse to publish an empty dataset.
    if match_count == 0:
        logger.error("Refusing to publish: pattern matches dataset is empty.")
        return

    # Atomic swap to final query zone
    replace_s3_prefix(bucket, TMP_PATTERN_MATCHES, AGG_PATTERN_MATCHES)
    logger.info("Successfully updated agg-data/pattern-matches in S3.")


def build_matched_app_sdk_strings() -> None:
    """Build matched SDKs artifact by joining version-details-map with pattern-matches."""
    bucket = CONFIG["s3"]["bucket"]
    vc_path = f"s3://{bucket}/{LOOKUP_VERSION_CODES}"

    # Build S3 glob paths directly instead of fetching file lists via S3 API
    vdm_glob = f"s3://{bucket}/{AGG_VERSION_DETAILS}/*/*.parquet"
    pm_glob = f"s3://{bucket}/{AGG_PATTERN_MATCHES}/*/*.parquet"

    tmp_output_glob = f"s3://{bucket}/{TMP_MATCHED_SDK_STRINGS}/*.parquet"
    agg_tmp_output = f"s3://{bucket}/{TMP_MATCHED_SDK_STRINGS}"

    # Wipe any stale remnants from a prior failed run.
    delete_s3_objects_by_prefix(bucket, f"{TMP_MATCHED_SDK_STRINGS}/")

    logger.info("Building aggregated matched SDK strings...")
    query = f"""COPY (
                 SELECT 
                     vc.store_app,
                     vdm.version_code_id,
                     vdm.string_id,
                     pm.sdk_id,
                     vc.created_at as version_code_created_at
                 FROM read_parquet('{vdm_glob}') vdm
                 JOIN read_parquet('{vc_path}') vc
                   ON vdm.version_code_id = vc.id
                 LEFT JOIN read_parquet('{pm_glob}') pm
                   ON vdm.string_id = pm.string_id
         ) TO '{agg_tmp_output}' (
             FORMAT PARQUET,
             FILE_SIZE_BYTES '128MB',
             COMPRESSION 'zstd',
             ROW_GROUP_SIZE {_LARGE_ROW_GROUP_SIZE},
             OVERWRITE_OR_IGNORE true
         )
         """

    with get_duckdb_connection("s3") as duckdb_con:
        duckdb_con.execute(query)
        # Validate output using glob + hive_partitioning
        matched_count = duckdb_con.execute(f"""
            SELECT count(*)
            FROM read_parquet('{tmp_output_glob}')
        """).fetchone()[0]
        logger.info(f"Matched SDKs dataset contains {matched_count:,} rows.")

    if matched_count == 0:
        logger.error("Refusing to publish: matched SDK strings dataset is empty.")
        return

    # Swap tmp to final
    replace_s3_prefix(
        bucket, f"{TMP_MATCHED_SDK_STRINGS}/", f"{AGG_MATCHED_SDK_STRINGS}/"
    )
    logger.info("Successfully updated agg-data/matched-sdk-strings in S3.")


def build_matched_app_sdk_strings_latest() -> None:
    """Build latest matched SDKs artifact by picking the newest version_code per store_app."""
    bucket = CONFIG["s3"]["bucket"]

    # S3 Glob locations
    input_glob = f"s3://{bucket}/{AGG_MATCHED_SDK_STRINGS}/*.parquet"
    agg_tmp_output = f"s3://{bucket}/{TMP_MATCHED_SDK_STRINGS_LATEST}"
    tmp_output_glob = f"s3://{bucket}/{TMP_MATCHED_SDK_STRINGS_LATEST}/*.parquet"
    vc_path = f"s3://{bucket}/{LOOKUP_VERSION_CODES}"

    delete_s3_objects_by_prefix(bucket, f"{TMP_MATCHED_SDK_STRINGS_LATEST}/")

    logger.info("Building latest matched SDK strings...")

    query = f"""
        COPY (
            WITH latest_vc AS (
                SELECT 
                    store_app, 
                    id AS version_code_id
                FROM read_parquet('{vc_path}')
                QUALIFY DENSE_RANK() OVER (
                    PARTITION BY store_app 
                    ORDER BY created_at DESC, id DESC
                ) = 1
            )
            SELECT 
                ap.store_app,
                ap.string_id,
                ap.sdk_id
            FROM read_parquet('{input_glob}') ap
            JOIN latest_vc lvc
              ON ap.store_app = lvc.store_app
             AND ap.version_code_id = lvc.version_code_id
        ) TO '{agg_tmp_output}' (
            FORMAT PARQUET,
            FILE_SIZE_BYTES '128MB',
            COMPRESSION 'zstd'
        )
    """
    with get_duckdb_connection("s3") as duckdb_con:
        duckdb_con.execute(query)

        # Validate output has rows using S3 glob instead of fetching Python lists
        matched_count = duckdb_con.execute(f"""
            SELECT count(*)
            FROM read_parquet('{tmp_output_glob}')
        """).fetchone()[0]

        logger.info(f"Latest matched SDKs dataset contains {matched_count:,} rows.")

    if matched_count == 0:
        logger.error(
            "Refusing to publish: matched SDK strings latest dataset is empty."
        )
        return

    # Swap tmp to final
    replace_s3_prefix(
        bucket,
        f"{TMP_MATCHED_SDK_STRINGS_LATEST}/",
        f"{AGG_MATCHED_SDK_STRINGS_LATEST}/",
    )
    logger.info(f"Successfully updated {AGG_MATCHED_SDK_STRINGS_LATEST} in S3.")


def initial_backfill_version_details_map() -> None:
    bucket = CONFIG["s3"]["bucket"]
    s3_path = f"s3://{bucket}/{RAW_DATA_VERSION_DETAILS_INITIAL}"
    pg_conn_str = pg_db_uri()
    con = get_duckdb_connection("s3")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{pg_conn_str}' AS pg (TYPE POSTGRES);")
    logger.info(f"Streaming public.version_details_map directly to {s3_path}")
    con.execute(f"""
        COPY (
            SELECT version_code as version_code_id, string_id 
            FROM pg.public.version_details_map 
            ORDER BY version_code ASC, string_id ASC
        ) 
        TO '{s3_path}' 
        (FORMAT PARQUET, PER_THREAD_OUTPUT TRUE, COMPRESSION 'ZSTD',
    ROW_GROUP_SIZE 100000,
    OVERWRITE_OR_IGNORE true)
        ;
    """)
    logger.info("Finished writing parquet files to S3.")
    raw_initial_paths = get_parquet_paths_by_prefix(
        bucket, RAW_DATA_VERSION_DETAILS_INITIAL
    )
    raw_output_path = f"s3://{bucket}/{RAW_DATA_VERSION_DETAILS}/"
    with get_duckdb_connection("s3") as duckdb_con:
        duckdb_con.execute(f"""
            COPY (
                WITH prepared AS (
                    SELECT 
                        CAST(string_id AS BIGINT) AS sid,
                        version_code_id
                    FROM read_parquet({raw_initial_paths})
                    WHERE string_id IS NOT NULL
                )
                SELECT
                    {_STRING_BUCKET_SQL} AS string_bucket,
                     -- EARLY DATE before other raw data
                    '2026-07-20' AS date, 
                    sid AS string_id,
                    version_code_id
                FROM prepared
                ORDER BY string_id ASC, version_code_id ASC
            ) TO '{raw_output_path}' (
                FORMAT PARQUET,
                PARTITION_BY (string_bucket, date),
                COMPRESSION 'zstd',
                ROW_GROUP_SIZE {_LARGE_ROW_GROUP_SIZE},
                OVERWRITE_OR_IGNORE true
            )
        """)


def _pg_table_to_s3(sql_query: str, s3_key: str, description: str) -> None:
    """Stream a Postgres query result directly to a single S3 parquet file.

    Uses DuckDB's ``ATTACH`` + ``COPY`` to stream data from Postgres to S3
    without loading it into Python memory — suitable for large tables.

    Args:
        sql_query: Full SQL query to run against the attached Postgres database.
        s3_key: Destination S3 key (relative to bucket, e.g. ``"lookups/foo.parquet"``).
        description: Human-readable label for logging (e.g. ``"version_strings"``).
    """
    bucket = CONFIG["s3"]["bucket"]
    s3_path = f"s3://{bucket}/{s3_key}"
    pg_conn_str = pg_db_uri()

    con = get_duckdb_connection("s3")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{pg_conn_str}' AS pg (TYPE POSTGRES);")

    logger.info("Streaming %s directly to %s", description, s3_path)
    con.execute(f"""
        COPY ({sql_query})
        TO '{s3_path}'
        (FORMAT PARQUET, COMPRESSION 'ZSTD')
    """)
    logger.info("Finished writing %s to %s", description, s3_path)


def copy_lookups() -> None:
    """Export lookup tables to static parquet files in S3.

    Streams ``version_strings``, ``sdk_packages``, ``sdk_paths``, and
    ``sdk_mediation_patterns`` to their respective ``lookups/`` keys, where
    downstream DuckDB build functions read them.
    """
    _pg_table_to_s3(
        "SELECT id, xml_path, tag, value_name FROM pg.public.version_strings ORDER BY id ASC",
        LOOKUP_VERSION_STRINGS,
        "version_strings",
    )
    _pg_table_to_s3(
        "SELECT id, sdk_id, package_pattern FROM pg.adtech.sdk_packages ORDER BY id ASC",
        LOOKUP_SDK_PACKAGE_PATTERNS,
        "sdk_packages",
    )
    _pg_table_to_s3(
        "SELECT id, sdk_id, path_pattern FROM pg.adtech.sdk_paths ORDER BY id ASC",
        LOOKUP_SDK_PATH_PATTERNS,
        "sdk_paths",
    )
    _pg_table_to_s3(
        "SELECT sdk_id, mediation_pattern FROM pg.adtech.sdk_mediation_patterns ORDER BY sdk_id ASC",
        LOOKUP_SDK_MEDIATION_PATTERNS,
        "sdk_mediation_patterns",
    )
    _pg_table_to_s3(
        "SELECT id, created_at, store_app, version_code FROM pg.public.version_codes WHERE version_code != '-1' ORDER BY id ASC",
        LOOKUP_VERSION_CODES,
        "version_codes",
    )


def swap_matched_app_strings_latest_todb(pgdb):
    bucket = CONFIG["s3"]["bucket"]
    app_sdk_latest_glob = f"s3://{bucket}/{AGG_MATCHED_SDK_STRINGS_LATEST}/*.parquet"
    batch_date = datetime.date.today()
    batch_date_str = batch_date.strftime("%Y-%m-%d")
    # Inject batch_date directly into DuckDB projection
    query = f"""
        SELECT
            store_app,
            string_id,
            sdk_id,
            '{batch_date_str}'::DATE AS batch_date
        FROM read_parquet('{app_sdk_latest_glob}')
    """
    columns = ["store_app", "string_id", "sdk_id", "batch_date"]
    atomic_swap_partition_stream(
        stream=stream_duckdb_tsv(query),
        columns=columns,
        batch_date=batch_date,
        pgdb=pgdb,
        schema="adtech",
        table="app_sdk_strings",
    )


def swap_matched_app_sdks_todb(pgdb):
    bucket = CONFIG["s3"]["bucket"]
    batch_date = datetime.date.today()
    batch_date_str = batch_date.strftime("%Y-%m-%d")
    app_sdks_glob = f"s3://{bucket}/{AGG_MATCHED_SDK_STRINGS}/*.parquet"
    query = f"""
        SELECT DISTINCT
            store_app,
            version_code_id,
            version_code_created_at,
            sdk_id,
            '{batch_date_str}'::DATE AS batch_date
        FROM read_parquet('{app_sdks_glob}')
        WHERE sdk_id IS NOT NULL and version_code_id IS NOT NULL
    """

    columns = [
        "store_app",
        "version_code_id",
        "version_code_created_at",
        "sdk_id",
        "batch_date",
    ]

    atomic_swap_partition_stream(
        stream=stream_duckdb_tsv(query),
        columns=columns,
        batch_date=batch_date,
        pgdb=pgdb,
        schema="adtech",
        table="app_sdks",
    )


def map_version_details(pgdb) -> None:
    """Orchestrate the entire version details processing pipeline.

    Args:
        date_str: Logical partition date for incoming compaction (YYYY-MM-DD).
    """

    start_time = time.time()
    logger.info("Starting map_version_details entrypoint run for past days")

    logger.info("--- Stage 2: Rebuilding aggregated master version details ---")
    build_aggregated_version_details()

    logger.info("--- Stage 3: Rebuilding aggregated pattern matches ---")
    copy_lookups()
    build_aggregated_pattern_matches()
    build_matched_app_sdk_strings()
    build_matched_app_sdk_strings_latest()

    logger.info("--- Stage 5: Syncing SDK agg to Postgres ---")
    swap_matched_app_sdks_todb(pgdb)
    swap_matched_app_strings_latest_todb(pgdb)

    elapsed = time.time() - start_time
    logger.info(f"Completed map_version_details run in {elapsed:.2f}s")
