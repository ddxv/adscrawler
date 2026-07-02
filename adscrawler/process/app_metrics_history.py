"""App metrics history — raw → hashed daily → weekly → interpolated → DB."""

import datetime

import duckdb
import numpy as np
import pandas as pd

from adscrawler.app_stores.utils import get_parquet_paths_by_prefix
from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import (
    clean_app_metrics_history_table,
    delete_app_metrics_by_date_and_apps,
    get_ecpm_benchmarks,
    get_retention_benchmarks,
    insert_bulk,
    query_countries,
    query_live_apps,
    query_store_app_categories,
    upsert_df,
)
from adscrawler.process import (
    AGG_APP_HASH_BUCKETS_DAILY,
    AGG_APP_HASH_BUCKETS_FILLED,
    AGG_APP_HASH_BUCKETS_WEEKLY,
    RAW_DATA_APP_DETAILS,
)
from adscrawler.process.storage import (
    delete_s3_objects_by_date_range,
    delete_s3_objects_by_prefix,
    get_duckdb_connection,
)

logger = get_logger(__name__, "scrape_stores")

# ---------------------------------------------------------------------------
# Default conversion assumptions (TODO: replace with store + category benchmarks)
# ---------------------------------------------------------------------------
NEW_USER_CONVERSION = 0.02  # 2% of new installs
RETENTION_CONVERSION = 0.002  # 0.2% of returning users (2 in 1,000)
AVG_TICKET = 5.0  # $5 average

# ---------------------------------------------------------------------------
# Column constants
# ---------------------------------------------------------------------------
STAR_COLS = [
    "one_star",
    "two_star",
    "three_star",
    "four_star",
    "five_star",
]

METRIC_COLS = [
    "review_count",
    "rating",
    "rating_count",
    "installs",
] + STAR_COLS

GLOBAL_HISTORY_KEYS = [
    "week_start",
    "store_app",
]
COUNTRY_HISTORY_KEYS = [
    "week_start",
    "country_id",
    "store_app",
]

COUNTRY_HISTORY_COLS = COUNTRY_HISTORY_KEYS + [
    x for x in METRIC_COLS if x != "review_count"
]

TIER_PCT_COLS = ["tier1_pct", "tier2_pct", "tier3_pct"]

GLOBAL_HISTORY_COLS = GLOBAL_HISTORY_KEYS + METRIC_COLS + TIER_PCT_COLS

GLOBAL_FINAL_COLS = [
    "store_app",
    "week_start",
    "weekly_installs",
    "weekly_ratings",
    "weekly_active_users",
    "monthly_active_users",
    "weekly_iap_revenue",
    "weekly_ad_revenue",
    "total_installs",
    "total_ratings",
    "rating",
    "one_star",
    "two_star",
    "three_star",
    "four_star",
    "five_star",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def check_for_duplicates(df: pd.DataFrame, key_columns: list[str]) -> None:
    any_duplicates = df.duplicated(subset=key_columns).any()
    if any_duplicates:
        dups = df[df.duplicated(subset=key_columns)][
            key_columns + ["store_id", "crawled_at"]
        ]
        logger.error(f"Duplicates found in app history data! {dups}")
        raise ValueError("Duplicates found in app history data")


def handle_missing_trackid_files(
    duckdb_con: duckdb.DuckDBPyConnection,
    app_detail_parquets: list[str],
    store: int,
) -> None:
    required_id_column = "trackId" if store == 2 else "appId"
    if store == 2:
        bad_parquets = []
        ok_parquets = []
        for parquet in app_detail_parquets:
            try:
                duckdb_con.execute(
                    f"SELECT {required_id_column} FROM read_parquet({[parquet]})"
                )
                logger.info(f"{required_id_column} column found in file")
                ok_parquets.append(parquet)
            except duckdb.BinderException:
                logger.info(f"{required_id_column} column not found in file {parquet}")
                bad_parquets.append(parquet)
        for parquet in bad_parquets:
            logger.warning(f"Wanting to deleting raw bad parquet file {parquet}")
    else:
        logger.error(
            "trackId column missing but store is not Apple, investigate data issue"
        )


def clean_history_tables(pgdb: PostgresEngine) -> None:
    clean_app_metrics_history_table(pgdb=pgdb, table_name="app_global_metrics_history")
    clean_app_metrics_history_table(pgdb=pgdb, table_name="app_country_metrics_history")


# ---------------------------------------------------------------------------
# Full aggregation pipeline
# ---------------------------------------------------------------------------


def delete_and_aggregate_s3_agg(
    store: int,
    pgdb: PostgresEngine,
) -> None:
    end_date = datetime.date.today()
    raw_data_lookback_days = 3
    raw_weekly_lookback_days = 8
    interpolate_query_lookback_days = 180
    interpolate_delete_lookback_days = 90
    db_delete_lookback_days = 30

    # Raw data → agg by DAY
    raw_start = end_date - datetime.timedelta(days=raw_data_lookback_days)
    for snapshot_date in pd.date_range(raw_start, end_date, freq="D"):
        log_info = f"{store=} {snapshot_date.date()} S3 raw app details agg"
        logger.info(f"{log_info} start")
        make_s3_app_hash_metrics_history_daily(
            store=store, snapshot_date=snapshot_date
        )

    # Agg DAY → WEEK
    raw_weekly_start = end_date - pd.Timedelta(days=raw_weekly_lookback_days)
    start_date_mon = raw_weekly_start - pd.Timedelta(days=raw_weekly_start.weekday())
    for hash_bucket in [f"{i:02x}" for i in range(256)]:
        for week_start in pd.date_range(start_date_mon, end_date, freq="W-MON"):
            log_info = (
                f"{store=} hash={hash_bucket} {week_start.date()} S3 weekly agg"
            )
            logger.info(f"{log_info} start")
            make_s3_app_hash_metrics_history_weekly(
                store=store,
                range_start=week_start.date(),
                range_end=week_start.date() + pd.Timedelta(days=6),
                hash_bucket=hash_bucket,
                clear_bucket_by_week=True,
            )

    # WEEK → interpolated (filled) weekly
    for hash_bucket in [f"{i:02x}" for i in range(256)]:
        query_start = end_date - datetime.timedelta(
            days=interpolate_query_lookback_days
        )
        query_start_mon = query_start - datetime.timedelta(days=query_start.weekday())
        delete_start = end_date - datetime.timedelta(
            days=interpolate_delete_lookback_days
        )
        delete_start_mon = delete_start - datetime.timedelta(
            days=delete_start.weekday()
        )
        log_info = f"{store=} hash={hash_bucket} {query_start_mon} S3 interpolate"
        logger.info(f"{log_info} start")
        write_app_hash_buckets_interpolated_to_s3(
            store=store,
            query_start_mon=query_start_mon,
            delete_start_mon=delete_start_mon,
            hash_bucket=hash_bucket,
            end_date=end_date,
        )
        db_delete_start = end_date - datetime.timedelta(days=db_delete_lookback_days)
        process_app_metrics_to_db(
            hash_bucket=hash_bucket,
            pgdb=pgdb,
            store=store,
            db_delete_start=db_delete_start,
        )


# ---------------------------------------------------------------------------
# Weekly aggregation (DAY → WEEK)
# ---------------------------------------------------------------------------


def copy_daily_to_weekly_hash_buckets(
    store: int,
    daily_parquet_paths: list[str],
) -> str:
    bucket = CONFIG["s3"]["bucket"]
    if store == 1:
        sel_metrics = "installs, rating, rating_count, review_count, histogram, store_last_updated"
        fin_metrics = "installs, rating, rating_count, review_count, histogram, store_last_updated"
        post_deduped_select = """
              SELECT
                  store_id,
                  country,
                  installs,
                  rating,
                  rating_count,
                  review_count,
                  COALESCE(TRY_CAST(list_extract(histogram, 1) AS BIGINT), 0) AS one_star,
                  COALESCE(TRY_CAST(list_extract(histogram, 2) AS BIGINT), 0) AS two_star,
                  COALESCE(TRY_CAST(list_extract(histogram, 3) AS BIGINT), 0) AS three_star,
                  COALESCE(TRY_CAST(list_extract(histogram, 4) AS BIGINT), 0) AS four_star,
                  COALESCE(TRY_CAST(list_extract(histogram, 5) AS BIGINT), 0) AS five_star,
                  store_last_updated,
                  week_start,
                  days_since_monday,
                  hash_bucket
              FROM deduped
        """
        extra_sort_column = "review_count DESC"
    elif store == 2:
        sel_metrics = "rating_count, rating, user_ratings, store_last_updated"
        fin_metrics = "rating_count, rating, user_ratings, store_last_updated"
        post_deduped_select = """
              SELECT
                  store_id,
                  country,
                  rating_count,
                  rating,
                  COALESCE(TRY_CAST(REPLACE(list_extract(star_vals, 1), ',', '') AS BIGINT), 0) AS one_star,
                  COALESCE(TRY_CAST(REPLACE(list_extract(star_vals, 2), ',', '') AS BIGINT), 0) AS two_star,
                  COALESCE(TRY_CAST(REPLACE(list_extract(star_vals, 3), ',', '') AS BIGINT), 0) AS three_star,
                  COALESCE(TRY_CAST(REPLACE(list_extract(star_vals, 4), ',', '') AS BIGINT), 0) AS four_star,
                  COALESCE(TRY_CAST(REPLACE(list_extract(star_vals, 5), ',', '') AS BIGINT), 0) AS five_star,
                  store_last_updated,
                  week_start,
                  days_since_monday,
                  hash_bucket
              FROM (
                  SELECT
                      *,
                      regexp_extract_all(COALESCE(user_ratings, ''), ':\\s*([0-9,]+)', 1) AS star_vals
                  FROM deduped
              ) parsed
        """
        extra_sort_column = "rating_count DESC"
    query = f"""COPY (
              WITH raw_data AS (
                  SELECT
                      store_id,
                      country,
                      {sel_metrics},
                      crawled_at,
                      hash_bucket,
                      DATE_TRUNC('week', crawled_date)::DATE AS week_start,
                      CAST(crawled_date - DATE_TRUNC('week', crawled_date)::DATE AS INTEGER) AS days_since_monday
                  FROM read_parquet({daily_parquet_paths}, union_by_name=true)
              ),
              deduped AS (
                  SELECT
                      store_id,
                      country,
                      {fin_metrics},
                      week_start,
                      days_since_monday,
                      hash_bucket
                  FROM raw_data
                  QUALIFY ROW_NUMBER() OVER (
                      PARTITION BY store_id, country, week_start
                      ORDER BY crawled_at DESC, {extra_sort_column}
                  ) = 1
              )
              {post_deduped_select}
          ) TO 's3://{bucket}/{AGG_APP_HASH_BUCKETS_WEEKLY}/store={store}/'
          (
              FORMAT PARQUET,
              PARTITION_BY (hash_bucket, week_start),
              ROW_GROUP_SIZE 100000,
              COMPRESSION 'zstd',
              OVERWRITE_OR_IGNORE true
          );
    """
    return query


def make_s3_app_hash_metrics_history_weekly(
    store: int,
    range_start: datetime.date,
    range_end: datetime.date,
    hash_bucket: str,
    clear_bucket_by_week: bool = True,
) -> None:
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    range_start_str = range_start.strftime("%Y-%m-%d")
    range_end_str = range_end.strftime("%Y-%m-%d")

    all_parquet_paths = []
    for ddt in pd.date_range(range_start_str, range_end_str, freq="D"):
        ddt_str = ddt.strftime("%Y-%m-%d")
        prefix = f"{AGG_APP_HASH_BUCKETS_DAILY}/store={store}/hash_bucket={hash_bucket}/snapshot_date={ddt_str}/"
        all_parquet_paths += get_parquet_paths_by_prefix(bucket, prefix)

    week_agg_prefix = f"{AGG_APP_HASH_BUCKETS_WEEKLY}/store={store}/hash_bucket={hash_bucket}/week_start={range_start_str}/"
    if not clear_bucket_by_week:
        week_agg_prefix = (
            f"{AGG_APP_HASH_BUCKETS_WEEKLY}/store={store}/hash_bucket={hash_bucket}/"
        )
    delete_s3_objects_by_prefix(
        bucket=bucket, prefix=week_agg_prefix, key_name=s3_config_key
    )

    if len(all_parquet_paths) == 0:
        logger.error(
            f"No daily parquet files found for store={store} week_start={range_start_str}"
        )
        return

    query = copy_daily_to_weekly_hash_buckets(
        store=store,
        daily_parquet_paths=all_parquet_paths,
    )
    with get_duckdb_connection(s3_config_key) as duckdb_con:
        duckdb_con.execute(query)


# ---------------------------------------------------------------------------
# Daily aggregation (RAW → DAY)
# ---------------------------------------------------------------------------


def make_s3_app_hash_metrics_history_daily(
    store: int, snapshot_date: pd.DatetimeIndex
) -> None:
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")
    prefix = (
        f"{RAW_DATA_APP_DETAILS}/store={store}/crawled_date={snapshot_date_str}/country="
    )
    all_parquet_paths = get_parquet_paths_by_prefix(bucket, prefix)
    hex_values = [f"{i:02x}" for i in range(256)]
    for hex_val in hex_values:
        agg_prefix = f"{AGG_APP_HASH_BUCKETS_DAILY}/store={store}/hash_bucket={hex_val}/snapshot_date={snapshot_date_str}/"
        delete_s3_objects_by_prefix(
            bucket=bucket, prefix=agg_prefix, key_name=s3_config_key
        )
    if len(all_parquet_paths) == 0:
        logger.error(
            f"No app detail parquet files found for store={store} snapshot_date={snapshot_date_str}"
        )
        return
    query = copy_raw_details_to_hash_buckets(
        store=store,
        app_detail_parquets=all_parquet_paths,
        snapshot_date_str=snapshot_date_str,
    )
    with get_duckdb_connection(s3_config_key) as duckdb_con:
        try:
            duckdb_con.execute(query)
        except duckdb.BinderException as e:
            if store == 2 and """"trackId" not found""" in str(e):
                logger.error(
                    f"trackId column not found in parquets for store={store}, skipping"
                )
                handle_missing_trackid_files(duckdb_con, all_parquet_paths, store)
            elif store == 1 and """"appId" not found""" in str(e):
                logger.error(
                    f"appId column not found in parquets for store={store}, skipping"
                )
                handle_missing_trackid_files(duckdb_con, all_parquet_paths, store)
            else:
                logger.warning(
                    f"Unexpected BinderException: {e}, investigate data issue"
                )


def estimate_ios_installs(df: pd.DataFrame) -> pd.DataFrame:
    conversion_rate = 0.02
    df["installs_est"] = (df["rating_count"] / conversion_rate).fillna(0).astype(int)
    return df


def merge_in_db_ids(
    df: pd.DataFrame, store: int, pgdb: PostgresEngine
) -> pd.DataFrame:
    store_id_map = query_live_apps(store=store, pgdb=pgdb)
    df = pd.merge(
        df,
        store_id_map[["store_id", "id"]].rename(columns={"id": "store_app"}),
        on="store_id",
        how="inner",
    )
    if df["store_app"].isna().any():
        logger.warning(f"Found new store ids: {len(df[df['store_app'].isna()])}")
        raise ValueError("New store ids found in S3 app history data")
    country_map = query_countries(pgdb)
    df = pd.merge(
        df,
        country_map[["id", "alpha2", "tier"]].rename(
            columns={"id": "country_id", "alpha2": "country"}
        ),
        on="country",
        how="left",
        validate="m:1",
    )
    return df


# ---------------------------------------------------------------------------
# Per-store metrics processing
# ---------------------------------------------------------------------------


def process_metrics_google(df: pd.DataFrame) -> pd.DataFrame:
    global_df = df[df["country_id"] == 840].copy()
    df = df.rename(
        columns={
            "installs": "global_installs",
            "rating_count": "global_rating_count",
        }
    )
    gb = df.groupby(["store_app", "week_start"])
    df["max_reviews"] = gb["review_count"].transform("max")
    df["global_installs"] = gb["global_installs"].transform("max")
    df["global_rating_count"] = gb["global_rating_count"].transform("max").fillna(0)
    df["is_max_candidate"] = (df["review_count"] >= df["max_reviews"] * 0.96) & (
        df["max_reviews"] > 200
    )
    candidate_counts = gb["is_max_candidate"].transform("sum")
    df["is_global_fallback"] = (
        df["is_max_candidate"].notna() & df["is_max_candidate"] & (candidate_counts > 1)
    )
    df["true_review_count"] = np.where(df["is_global_fallback"], 0, df["review_count"])
    us_lookup = (
        df[df["country_id"] == 840]
        .groupby(["store_app", "week_start"])["true_review_count"]
        .max()
        .rename("us_review_count")
        .reset_index()
    )
    df = df.merge(us_lookup, on=["store_app", "week_start"], how="left")
    df["pct_of_us_reviews"] = df["true_review_count"] / df["us_review_count"].replace(
        0, np.nan
    )
    df = df.sort_values("week_start")
    df["pct_of_us_reviews"] = df.groupby(["store_app", "country_id"])[
        ["pct_of_us_reviews"]
    ].bfill()
    df["true_review_count"] = np.where(
        df["true_review_count"].isna(),
        df["us_review_count"] * df["pct_of_us_reviews"],
        df["true_review_count"],
    )
    local_sums_df = (
        df[~df["is_global_fallback"]]
        .groupby(["store_app", "week_start"])["true_review_count"]
        .sum()
        .rename("grc_summed")
        .reset_index()
    )
    df = df.merge(local_sums_df, on=["store_app", "week_start"], how="left")
    df["has_global_fallback"] = (
        df.groupby(["store_app", "week_start"])["is_global_fallback"].transform("sum")
        > 0
    )
    df["summed_to_global_fallback_multiplier"] = np.where(
        df["has_global_fallback"], df["max_reviews"] / df["grc_summed"], np.nan
    )
    df["summed_to_global_fallback_multiplier"] = df.groupby(["store_app"])[
        "summed_to_global_fallback_multiplier"
    ].bfill()
    df["global_review_count"] = np.where(
        df["has_global_fallback"],
        df["max_reviews"],
        df["grc_summed"] * df["summed_to_global_fallback_multiplier"].fillna(1),
    )
    df = df.drop(columns=["grc_summed"])
    df["pct_of_global"] = (
        (df["true_review_count"] / df["global_review_count"])
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )
    df["installs_est"] = (
        (df["global_installs"] * df["pct_of_global"]).round().astype(int)
    )
    df["rating_count_est"] = (
        (df["global_rating_count"] * df["pct_of_global"]).round().astype(int)
    )
    df["true_review_count"] = pd.to_numeric(
        df["true_review_count"], errors="coerce"
    ).fillna(0)
    df = df.rename(columns={"review_count": "original_review_count"})
    df = df.rename(columns={"true_review_count": "review_count"})
    global_df = global_df.set_index(GLOBAL_HISTORY_KEYS)
    for col in STAR_COLS:
        if col not in global_df.columns:
            global_df[col] = 0
        global_df[col] = pd.to_numeric(global_df[col], errors="coerce").fillna(0)
    tier_pct = df.pivot_table(
        index=GLOBAL_HISTORY_KEYS,
        columns="tier",
        values="pct_of_global",
        aggfunc="sum",
        fill_value=0,
        dropna=False,
    ).rename(columns={"tier1": "tier1_pct", "tier2": "tier2_pct", "tier3": "tier3_pct"})
    if not all(col in tier_pct.columns for col in TIER_PCT_COLS):
        for col in TIER_PCT_COLS:
            if col not in tier_pct.columns:
                tier_pct[col] = 0
    global_df = global_df.join(tier_pct)
    df["rating"] = df["rating"].replace(0, np.nan)
    df["rating_count_est"] = df["rating_count_est"].replace(0, np.nan)
    df.loc[
        (df["rating_count_est"] > 0) & (df["rating"] > 0), "rating_count_counter"
    ] = df["rating_count_est"]
    df["rating_prod"] = df["rating"] * df["rating_count_est"]
    grouped = df.groupby(GLOBAL_HISTORY_KEYS)
    agg = grouped.agg(
        total_prod=("rating_prod", "sum"), total_counts=("rating_count_counter", "sum")
    )
    agg["rating"] = agg["total_prod"] / agg["total_counts"].replace(0, np.nan)
    global_df["rating"] = agg["rating"]
    tier_cols = TIER_PCT_COLS
    row_sums = global_df[tier_cols].sum(axis=1)
    global_df[tier_cols] = global_df[tier_cols].div(row_sums.replace(0, np.nan), axis=0)
    global_df = global_df.reset_index()
    df = df.rename(
        columns={
            "true_review_count": "review_count",
            "installs_est": "installs",
            "rating_count_est": "rating_count",
        }
    )
    return df, global_df


def process_metrics_apple(df: pd.DataFrame) -> pd.DataFrame:
    us_lookup = (
        df[df["country_id"] == 840]
        .groupby(["store_app", "week_start"])["rating_count"]
        .max()
    )
    df = df.merge(
        us_lookup.rename("us_rating_count").reset_index(),
        on=["store_app", "week_start"],
        how="left",
    )
    df["pct_of_us_ratings"] = df["rating_count"] / df["us_rating_count"].replace(
        0, np.nan
    )
    df = df.sort_values("week_start")
    gbc = df.groupby(["store_app", "country_id"])
    df["pct_of_us_ratings"] = gbc[["pct_of_us_ratings"]].bfill()
    df["true_review_count"] = np.where(
        df["rating_count"].isna(),
        df["us_rating_count"] * df["pct_of_us_ratings"],
        df["rating_count"],
    )
    for col in STAR_COLS:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df = estimate_ios_installs(df)
    df["rating_prod"] = df["rating"] * df["rating_count"]
    df["weekly_total_ratings"] = df.groupby(["week_start", "store_app"])[
        "rating_count"
    ].transform("sum")
    latest_ratio = (
        df.sort_values("week_start")
        .groupby(["store_app", "country_id"])[["rating_count", "weekly_total_ratings"]]
        .last()
    )
    latest_ratio["weekly_total_ratings"] = latest_ratio["weekly_total_ratings"].replace(
        0, np.nan
    )
    latest_ratio["rating_ratio"] = (
        latest_ratio["rating_count"] / latest_ratio["weekly_total_ratings"]
    )
    latest_ratio = latest_ratio.reset_index()
    df = pd.merge(
        df,
        latest_ratio[["store_app", "country_id", "rating_ratio"]],
        how="left",
        on=["store_app", "country_id"],
        validate="m:1",
    )
    gbw = df.groupby(["week_start", "store_app"])
    df["global_rating_count"] = gbw["rating_count"].transform("sum")
    df["grc_future_est_a"] = df["rating_count"] / df["rating_ratio"]
    df["grc_est_prod"] = df["rating_count"] * df["grc_future_est_a"]
    df["grc_future_est"] = gbw["grc_est_prod"].transform("sum") / gbw[
        "rating_count"
    ].transform("sum")
    estimate_available = (df["grc_future_est"] > 0).fillna(False) & (
        df["rating_ratio"] > 0
    ).fillna(False)
    needs_estimate = df["rating_count"].isna() | (df["rating_count"] == 0)
    to_estimate = estimate_available & needs_estimate
    df["rating_count"] = np.where(
        to_estimate,
        df["grc_future_est"] * df["rating_ratio"],
        df["rating_count"].fillna(0),
    )
    df["rating_count"] = (
        df["rating_count"].replace([np.inf, -np.inf], 0).round(0).astype(int)
    )
    app_mean_rating = gbw["rating"].transform("mean")
    df["rating"] = df["rating"].fillna(app_mean_rating)
    df["rating_prod"] = df["rating"] * df["rating_count"]
    df = estimate_ios_installs(df)
    global_df = df.groupby(["week_start", "store_app"]).agg(
        rating_count=("rating_count", "sum"),
        rating_prod=("rating_prod", "sum"),
        installs=("installs_est", "sum"),
        **{col: (col, "sum") for col in STAR_COLS},
    )
    global_df["rating"] = (
        global_df["rating_prod"] / global_df["rating_count"].replace(0, np.nan)
    ).astype("float64")
    tier_installs = df.pivot_table(
        index=["week_start", "store_app"],
        columns="tier",
        values="installs_est",
        aggfunc="sum",
        fill_value=0,
    )
    tier_pct = tier_installs.div(tier_installs.sum(axis=1), axis=0)
    tier_pct = tier_pct.add_suffix("_pct")
    tier_pct = tier_pct.fillna(0)
    global_df = global_df.join(tier_pct)
    global_df = global_df.reset_index()
    global_df["installs"] = global_df["installs"].astype("Int64")
    global_df["rating_count"] = global_df["rating_count"].astype("Int64")
    global_df[STAR_COLS] = global_df[STAR_COLS].astype("Int64")
    df = df.rename(columns={"installs_est": "installs"})
    return df, global_df


# ---------------------------------------------------------------------------
# Interpolation helpers
# ---------------------------------------------------------------------------


def _build_interpolated_metrics_sql(metrics: list[str]) -> str:
    interpolated_lines = []
    for metric in metrics:
        interpolated_lines.append(
            f"MIN_BY({metric}, observed_at) OVER w_future AS {metric}_y2,"
        )
    return "\n" + "\n".join(interpolated_lines)


def _build_coalesce_metric_sql(metric: str) -> str:
    float_metrics = {"rating"}
    cast = "DOUBLE" if metric in float_metrics else "BIGINT"
    if metric == "installs":
        coalesce_string = f"""COALESCE(
                    a_exact.{metric},
                    a_prev.{metric} +
                        DATE_DIFF('day', a_prev.observed_at, m.week_start)::DOUBLE *
                        GREATEST((a_prev.{metric}_y2 - a_prev.{metric})::DOUBLE, 0.0) /
                        NULLIF(DATE_DIFF('day', a_prev.observed_at, a_prev.x2)::DOUBLE, 0.0)
                )::{cast} AS {metric},"""
    else:
        coalesce_string = f"""COALESCE(
                    a_exact.{metric},
                    a_prev.{metric} +
                        DATE_DIFF('day', a_prev.observed_at, m.week_start)::DOUBLE *
                        (a_prev.{metric}_y2 - a_prev.{metric})::DOUBLE /
                        NULLIF(DATE_DIFF('day', a_prev.observed_at, a_prev.x2)::DOUBLE, 0.0)
                )::{cast} AS {metric},"""
    return coalesce_string


def _build_coalesce_metrics_sql(metrics: list[str]) -> str:
    coalesce_blocks = [_build_coalesce_metric_sql(m) for m in metrics]
    return "\n                " + "\n                ".join(coalesce_blocks)


def _get_store_metrics_config(store: int) -> dict:
    if store == 1:
        metrics = ["installs", "rating", "rating_count", "review_count", *STAR_COLS]
    elif store == 2:
        metrics = ["rating_count", "rating", *STAR_COLS]
    else:
        raise ValueError(f"Unsupported store: {store}")
    return {
        "metrics": metrics,
        "interpolated_metrics": _build_interpolated_metrics_sql(metrics),
        "coalesce_metrics": _build_coalesce_metrics_sql(metrics),
    }


def write_app_hash_buckets_interpolated_to_s3(
    query_start_mon: datetime.date,
    delete_start_mon: datetime.date,
    end_date: datetime.date,
    store: int,
    hash_bucket: str,
) -> None:
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    delete_s3_objects_by_date_range(
        bucket=bucket,
        start_date_mon=delete_start_mon,
        end_date=end_date,
        prefix=f"{AGG_APP_HASH_BUCKETS_FILLED}/store={store}/hash_bucket={hash_bucket}",
        key_name=s3_config_key,
    )
    all_parquet_paths = []
    for ddt in pd.date_range(query_start_mon, end_date, freq="W-MON"):
        ddt_str = ddt.strftime("%Y-%m-%d")
        prefix = f"{AGG_APP_HASH_BUCKETS_WEEKLY}/store={store}/hash_bucket={hash_bucket}/week_start={ddt_str}/"
        all_parquet_paths += get_parquet_paths_by_prefix(bucket, prefix)
    if len(all_parquet_paths) == 0:
        logger.warning(
            f"No parquet paths found for agg app hash buckets {store=} {query_start_mon=} {end_date=}"
        )
        return
    config = _get_store_metrics_config(store)
    metrics = config["metrics"]
    interpolated_metrics = config["interpolated_metrics"]
    coalesce_metrics = config["coalesce_metrics"]
    query_start_mon_str = query_start_mon.strftime("%Y-%m-%d")
    delete_start_mon_str = delete_start_mon.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    msv_query = f"""COPY (
    WITH 
        weekly_data AS (
            SELECT
                store_id,
                country,
                week_start,
                days_since_monday,
                CAST(week_start + days_since_monday AS DATE) AS observed_at,
                {",".join(metrics)},
                store_last_updated
            FROM read_parquet({all_parquet_paths}, union_by_name=true)
        ),
        target_mondays AS (
            SELECT CAST(range AS DATE) AS week_start
            FROM range(
                DATE '{query_start_mon_str}',
                DATE '{end_date_str}' + INTERVAL 7 DAY,
                INTERVAL 7 DAY
            )
        ),
        anchors AS (
            SELECT
                store_id,
                country,
                week_start,
                days_since_monday,
                observed_at,
                {",".join(metrics)},
                store_last_updated,
               {interpolated_metrics} 
                MIN(observed_at) OVER w_future AS x2,
                MAX_BY(store_last_updated, observed_at) OVER w_past_inclusive AS store_last_updated_carry
            FROM weekly_data
            WINDOW
                w_future AS (PARTITION BY store_id, country
                             ORDER BY observed_at
                             ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),
                w_past_inclusive AS (PARTITION BY store_id, country
                                     ORDER BY observed_at
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ),
       interpolated AS (
        SELECT
            dims.store_id,
            left(md5(dims.store_id), 2) AS hash_bucket,
            dims.country,
            m.week_start,
            {coalesce_metrics} 
            COALESCE(a_exact.store_last_updated, a_prev.store_last_updated_carry) AS store_last_updated
        FROM (SELECT DISTINCT store_id, country FROM weekly_data) dims
        CROSS JOIN target_mondays m
        LEFT JOIN anchors a_exact
                ON a_exact.store_id      = dims.store_id
                AND a_exact.country       = dims.country
                AND a_exact.observed_at = m.week_start
        LEFT JOIN anchors a_prev
                ON a_prev.store_id      = dims.store_id
                AND a_prev.country       = dims.country
                AND a_prev.observed_at = (
                        SELECT MAX(observed_at)
                        FROM weekly_data
                        WHERE store_id      = dims.store_id
                        AND country       = dims.country
                        AND observed_at < m.week_start
                    )
        WHERE a_exact.{metrics[0]} IS NOT NULL
            OR (a_prev.observed_at IS NOT NULL AND a_prev.x2 IS NOT NULL)
        )
        SELECT * FROM interpolated
        WHERE week_start >= DATE '{delete_start_mon_str}'
        ORDER BY store_id, country, week_start
    ) TO 's3://{bucket}/{AGG_APP_HASH_BUCKETS_FILLED}/store={store}/'
    (
        FORMAT PARQUET,
        PARTITION_BY (hash_bucket, week_start),
        ROW_GROUP_SIZE 100000,
        COMPRESSION 'zstd',
        OVERWRITE_OR_IGNORE true
    );
        """
    with get_duckdb_connection(s3_config_key) as duckdb_con:
        duckdb_con.execute(msv_query)


# ---------------------------------------------------------------------------
# Metrics processing → DB
# ---------------------------------------------------------------------------


def process_metrics(
    store: int,
    df: pd.DataFrame,
    pgdb: PostgresEngine,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    log_info = f"{store=} process metrics for {df.shape[0]:,} rows"
    logger.info(f"{log_info} start")
    if store == 1:
        country_df, global_df = process_metrics_google(df=df)
    elif store == 2:
        country_df, global_df = process_metrics_apple(df=df)
    country_df = country_df.convert_dtypes(dtype_backend="pyarrow")
    country_df = country_df.replace({pd.NA: None})
    check_for_duplicates(
        df=country_df,
        key_columns=COUNTRY_HISTORY_KEYS,
    )
    for col in TIER_PCT_COLS:
        if col not in global_df.columns:
            global_df[col] = 0.0
        global_df[col] = global_df[col].fillna(0.0)
    global_df = global_df[[x for x in GLOBAL_HISTORY_COLS if x in global_df.columns]]
    global_df["tier_pct_sum"] = (
        global_df["tier1_pct"] + global_df["tier2_pct"] + global_df["tier3_pct"]
    )
    incomplete_tier_pct = global_df["tier_pct_sum"] < 0.5
    all_t1 = global_df["tier1_pct"] == 1
    tiers_to_fill = incomplete_tier_pct | all_t1
    global_df.loc[tiers_to_fill, "tier1_pct"] = 0.34
    global_df.loc[tiers_to_fill, "tier2_pct"] = 0.33
    global_df.loc[tiers_to_fill, "tier3_pct"] = 0.33
    global_df = calculate_derived_metrics(pgdb, global_df, store=store)
    check_for_duplicates(
        df=global_df,
        key_columns=["store_app", "week_start"],
    )
    global_df = global_df.replace({pd.NA: None, np.nan: None})
    country_df = country_df[
        [x for x in COUNTRY_HISTORY_COLS if x in country_df.columns]
    ]
    cols_as_int = [
        "weekly_installs",
        "weekly_active_users",
        "monthly_active_users",
        "total_installs",
        "total_ratings",
        *STAR_COLS,
        "installs",
        "rating_count",
    ]
    for col in cols_as_int:
        if col in country_df.columns:
            country_df[col] = country_df[col].fillna(0).round().astype(int)
        if col in global_df.columns:
            global_df[col] = global_df[col].fillna(0).round().astype(int)
    for col in TIER_PCT_COLS:
        global_df[col] = (global_df[col] * 10000).round().fillna(0).astype("int16")
    global_df = global_df[GLOBAL_FINAL_COLS]
    logger.info(f"{log_info} finished")
    return country_df, global_df


def delete_and_insert_app_metrics(
    pgdb: PostgresEngine,
    country_df: pd.DataFrame,
    global_df: pd.DataFrame,
    store: int,
    delete_from_date: datetime.date,
) -> None:
    log_info = f"{store=} {delete_from_date=} delete_and_insert_app_metrics"
    table_name = "app_country_metrics_history"
    logger.info(f"{log_info} {table_name=} start")
    delete_app_metrics_by_date_and_apps(
        pgdb=pgdb,
        delete_from_date=delete_from_date,
        store_apps=country_df["store_app"].unique().tolist(),
        table_name=table_name,
    )
    insert_bulk(df=country_df, table_name=table_name, pgdb=pgdb)
    table_name = "app_global_metrics_history"
    logger.info(f"{log_info} {table_name=} start")
    delete_app_metrics_by_date_and_apps(
        pgdb=pgdb,
        delete_from_date=delete_from_date,
        store_apps=global_df["store_app"].unique().tolist(),
        table_name=table_name,
    )
    insert_bulk(df=global_df, table_name=table_name, pgdb=pgdb)


def ffill_app_metrics(
    df: pd.DataFrame, store: int, pgdb: PostgresEngine
) -> pd.DataFrame:
    app_birth_dates = df.groupby("store_app")["week_start"].min()
    app_end_dates = df.groupby("store_app")["week_start"].max()
    observed_app_countries = df[["store_app", "country_id"]].drop_duplicates()
    all_weeks = pd.DataFrame({"week_start": df["week_start"].unique()})
    full_df = observed_app_countries.merge(all_weeks, how="cross")
    full_df = full_df[
        (full_df["week_start"] >= full_df["store_app"].map(app_birth_dates))
        & (full_df["week_start"] <= full_df["store_app"].map(app_end_dates))
    ]
    full_index = pd.MultiIndex.from_frame(
        full_df[["store_app", "week_start", "country_id"]]
    )
    df = (
        df.set_index(["store_app", "week_start", "country_id"])
        .reindex(full_index)
        .sort_index()
        .reset_index()
    )
    if store == 1:
        cols_to_ffill = [
            "review_count",
            "installs",
            "rating_count",
            "rating",
            *STAR_COLS,
        ]
    elif store == 2:
        cols_to_ffill = [
            "rating_count",
            "rating",
            *STAR_COLS,
        ]
    df = df.sort_values("week_start")
    df[cols_to_ffill] = df.groupby(["store_app", "country_id"])[cols_to_ffill].ffill()
    df = df.reset_index()
    country_map = query_countries(pgdb=pgdb)
    df["tier"] = df["country_id"].map(country_map.set_index("id")["tier"])
    return df


def drop_unwanted_rows(
    country_df: pd.DataFrame,
    global_df: pd.DataFrame,
    store: int,
    delete_from_date: datetime.date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    no_rating_count = (country_df["rating_count"].isna()) | (
        country_df["rating_count"] == 0
    )
    no_installs = (country_df["installs"].isna()) | (country_df["installs"] == 0)
    no_rating = (country_df["rating"].isna()) | (country_df["rating"] == 0)
    all_null = no_rating_count & no_installs & no_rating
    country_df = country_df[
        (~all_null)
        & (country_df["week_start"] >= pd.to_datetime(delete_from_date).normalize())
    ]
    global_df = global_df[
        (global_df["week_start"] >= pd.to_datetime(delete_from_date).normalize())
    ]
    latest_week = country_df.groupby(["store_app", "country_id"])[
        "week_start"
    ].transform("max")
    installs_min = 200 if store == 1 else 100
    valid_pairs = country_df.loc[
        (country_df["week_start"] == latest_week)
        & (country_df["installs"] > installs_min),
        ["store_app", "country_id"],
    ].drop_duplicates()
    country_df = country_df.merge(
        valid_pairs, on=["store_app", "country_id"], how="inner"
    )
    return country_df, global_df


def get_app_hash_buckets_filled_from_s3(store: int, app_hash: str) -> pd.DataFrame:
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    prefix = f"{AGG_APP_HASH_BUCKETS_FILLED}/store={store}/hash_bucket={app_hash}/"
    all_parquet_paths = get_parquet_paths_by_prefix(bucket, prefix)
    if len(all_parquet_paths) == 0:
        logger.warning(f"No filled parquet paths found for {store=} {app_hash=}")
        return pd.DataFrame()
    sel_query = f"""
        SELECT * FROM read_parquet({all_parquet_paths}, union_by_name=true)
        ORDER BY store_id, country, week_start
    """
    with get_duckdb_connection(s3_config_key) as duckdb_con:
        return duckdb_con.execute(sel_query).df()


def store_app_updated_history(
    df: pd.DataFrame, pgdb: PostgresEngine, store: int
) -> pd.DataFrame:
    logger.info(f"store={store} store_app_updated_history start")
    if store == 1:
        df.loc[df["store_last_updated"] <= 0, "store_last_updated"] = None
        df["store_last_updated"] = pd.to_datetime(df["store_last_updated"], unit="s")
    elif store == 2:
        df["store_last_updated"] = pd.to_datetime(
            df["store_last_updated"], format="ISO8601", utc=True
        )
    udf = (
        df[df["store_last_updated"].notna()]
        .groupby(["store_app", "store_last_updated"])["week_start"]
        .max()
        .rename("latest_crawl_week")
        .reset_index()
    )
    upsert_df(
        df=udf,
        table_name="app_updated_history",
        pgdb=pgdb,
        key_columns=["store_app", "store_last_updated"],
        insert_columns=["latest_crawl_week"],
    )
    df = df.drop(columns=["store_last_updated"])
    logger.info(f"store={store} store_app_updated_history finished")
    return df


def process_app_metrics_to_db(
    hash_bucket: str,
    pgdb: PostgresEngine,
    store: int,
    db_delete_start: datetime.date,
) -> None:
    log_info = f"{store=} date={db_delete_start} {hash_bucket=} process to DB"
    logger.info(f"{log_info} start")
    df = get_app_hash_buckets_filled_from_s3(store=store, app_hash=hash_bucket)
    logger.info(f"{log_info} got {df.shape[0]:,} interpolated rows from S3")
    if df.empty:
        logger.warning(f"{log_info} no data found for S3 agg app metrics, skipping")
        raise ValueError(f"{log_info} no data found for S3 agg app metrics")
    if store == 1:
        problem_rows = df["installs"].isna()
    elif store == 2:
        problem_rows = df["store_id"].str.contains(".0", regex=False)
    if problem_rows.any():
        df = df[~problem_rows]
        logger.warning(f"{log_info} {problem_rows.sum()} bad rows! removed them")
    df = merge_in_db_ids(df, store, pgdb)
    df = df.drop(["store_id", "country"], axis=1)
    df = store_app_updated_history(df, pgdb, store)
    df = ffill_app_metrics(df, store, pgdb)
    country_df, global_df = process_metrics(store=store, df=df, pgdb=pgdb)
    country_df, global_df = drop_unwanted_rows(
        country_df, global_df, store, db_delete_start
    )
    delete_and_insert_app_metrics(
        pgdb=pgdb,
        country_df=country_df,
        global_df=global_df,
        store=store,
        delete_from_date=db_delete_start,
    )


# ---------------------------------------------------------------------------
# Raw → daily-hash-buckets COPY query
# ---------------------------------------------------------------------------


def copy_raw_details_to_hash_buckets(
    store: int,
    app_detail_parquets: list[str],
    snapshot_date_str: str,
) -> str:
    bucket = CONFIG["s3"]["bucket"]
    crawl_result_filter = "WHERE crawl_result = 1"
    if snapshot_date_str < "2025-11-01":
        crawl_result_filter = ""
    if store == 2:
        if snapshot_date_str < "2025-11-01":
            store_id_col = "CAST(trackId AS VARCHAR)"
        else:
            store_id_col = "store_id"
        data_cols = f"""
             {store_id_col} AS store_id,
             country,
             crawled_date,
             averageUserRating AS rating,
             userRatingCount AS rating_count,
             user_ratings,
             currentVersionReleaseDate AS store_last_updated,
             crawled_at
        """
        export_cols = """
             store_id,
             country,
             crawled_date,
             rating,
             rating_count,
             user_ratings,
             store_last_updated,
             crawled_at
             """
        extra_sort_column = "rating_count DESC"
    elif store == 1:
        if snapshot_date_str < "2025-11-01":
            store_id_col = "CAST(appId AS VARCHAR)"
            crawl_result_filter = "WHERE installs > 0"
        else:
            store_id_col = "store_id"
        data_cols = f"""
             {store_id_col} AS store_id,
             country,
             crawled_date,
             realInstalls as installs,
             ratings as rating_count,
             score AS rating,
             reviews AS review_count,
             histogram,
             updated AS store_last_updated,
             crawled_at
             """
        export_cols = """
                store_id,
                country,
                crawled_date,
                installs,
                rating,
                rating_count,
                review_count,
                histogram,
                store_last_updated,
                crawled_at
        """
        extra_sort_column = "review_count DESC"
    query = f"""COPY (
              WITH raw_data AS (
                  SELECT 
                      {data_cols},
                      left(md5(store_id), 2) AS hash_bucket,
                      '{snapshot_date_str}' AS snapshot_date
                  FROM read_parquet({app_detail_parquets}, union_by_name=true)
                    {crawl_result_filter}
              ),
              deduped AS (
                  SELECT 
                      {export_cols},
                      snapshot_date,
                      hash_bucket
                  FROM raw_data
                  QUALIFY ROW_NUMBER() OVER (
                      PARTITION BY store_id, country
                      ORDER BY crawled_at DESC, {extra_sort_column}
                  ) = 1
              )
              SELECT * FROM deduped
          ) TO 's3://{bucket}/{AGG_APP_HASH_BUCKETS_DAILY}/store={store}/'
          (
              FORMAT PARQUET, 
              PARTITION_BY (hash_bucket, snapshot_date),
              ROW_GROUP_SIZE 100000, 
              COMPRESSION 'zstd',
              OVERWRITE_OR_IGNORE true
          );
          """
    return query


# ---------------------------------------------------------------------------
# Derived metrics (cohort retention, revenue estimates)
# ---------------------------------------------------------------------------


def calculate_derived_metrics(
    pgdb: PostgresEngine, global_df: pd.DataFrame, store: int
) -> pd.DataFrame:
    metrics = [
        "installs",
        "rating",
        "rating_count",
        *STAR_COLS,
        *TIER_PCT_COLS,
    ]
    xaxis_col = "week_start"
    global_df[xaxis_col] = pd.to_datetime(global_df[xaxis_col])
    global_df = global_df.sort_values(["store_app", xaxis_col])
    global_df["installs_diff"] = (
        global_df.groupby("store_app")["installs"]
        .diff()
        .fillna(global_df["installs"])
        .fillna(0)
    )
    global_df["installs_diff"] = global_df["installs_diff"].clip(lower=0)
    global_df["weekly_ratings"] = (
        global_df.groupby("store_app")["rating_count"]
        .diff()
        .fillna(global_df["rating_count"])
    )
    drop_rows = global_df["week_start"] == global_df["week_start"].min()
    global_df = global_df[~drop_rows]
    if store == 2:
        global_df["installs_diff"] = (
            global_df.groupby("store_app")["installs_diff"]
            .rolling(window=3, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )
    app_cats = query_store_app_categories(
        pgdb, store_apps=global_df["store_app"].unique().tolist()
    )
    global_df = global_df.merge(app_cats, on="store_app", how="left", validate="m:1")
    retention_benchmarks = get_retention_benchmarks(pgdb)
    retention_benchmarks = retention_benchmarks[retention_benchmarks["store"] == store]
    cohorts = global_df.merge(
        retention_benchmarks, on=["app_category"], how="left", validate="m:1"
    )
    cohorts["k"] = np.log(
        cohorts["d30"].replace(0, np.nan) / cohorts["d7"].replace(0, np.nan)
    ) / np.log(30.0 / 7.0)
    cohorts = cohorts[["store_app", "week_start", "installs_diff", "d1", "d7", "k"]]
    logger.info("Historical merge, memory intensive step")
    cohorts = cohorts.merge(
        cohorts[["store_app", "week_start", "installs_diff"]],
        on="store_app",
        suffixes=("", "_historical"),
    )
    cohorts = cohorts[cohorts["week_start"] >= cohorts["week_start_historical"]]
    cohorts["weeks_passed"] = (
        (cohorts["week_start"] - cohorts["week_start_historical"]).dt.days / 7
    ).astype(int)
    wau_mult = 2.0
    cohorts["retention_rate"] = np.where(
        cohorts["weeks_passed"] == 0,
        1.0,
        (
            cohorts["d7"]
            * wau_mult
            * (cohorts["weeks_passed"].replace(0, 1) ** cohorts["k"])
        ).clip(upper=1.0),
    )
    cohorts["surviving_users"] = (
        cohorts["installs_diff_historical"] * cohorts["retention_rate"]
    )
    mau_mult = 3.5
    cohorts["retention_rate_mau"] = np.where(
        cohorts["weeks_passed"] == 0,
        1.0,
        (
            cohorts["d7"]
            * mau_mult
            * (cohorts["weeks_passed"].replace(0, 1) ** cohorts["k"])
        ).clip(upper=1.0),
    )
    cohorts["surviving_mau"] = (
        cohorts["installs_diff_historical"] * cohorts["retention_rate_mau"]
    )
    cohorts = (
        cohorts.groupby(["store_app", "week_start"])[
            ["surviving_users", "surviving_mau"]
        ]
        .sum()
        .reset_index()
    )
    cohorts = cohorts.rename(columns={"surviving_users": "wau", "surviving_mau": "mau"})
    dfcols = [
        "store_app",
        "in_app_purchases",
        "ad_supported",
        "week_start",
        "installs_diff",
        "weekly_ratings",
    ] + metrics
    global_df = pd.merge(
        global_df[dfcols],
        cohorts,
        on=["store_app", "week_start"],
        how="left",
        validate="1:1",
    )
    rename_map = {
        "installs_diff": "weekly_installs",
        "wau": "weekly_active_users",
        "mau": "monthly_active_users",
        "installs": "total_installs",
        "rating_count": "total_ratings",
    }
    global_df = global_df.rename(columns=rename_map)
    global_df = calculate_revenue_cols(pgdb, global_df)
    return global_df


def calculate_revenue_cols(
    pgdb: PostgresEngine, global_df: pd.DataFrame
) -> pd.DataFrame:
    global_df["wau_tier1"] = global_df["weekly_active_users"] * global_df["tier1_pct"]
    global_df["wau_tier2"] = global_df["weekly_active_users"] * global_df["tier2_pct"]
    global_df["wau_tier3"] = global_df["weekly_active_users"] * global_df["tier3_pct"]
    global_df["mau_tier1"] = global_df["monthly_active_users"] * global_df["tier1_pct"]
    global_df["mau_tier2"] = global_df["monthly_active_users"] * global_df["tier2_pct"]
    global_df["mau_tier3"] = global_df["monthly_active_users"] * global_df["tier3_pct"]
    new_rev = global_df["weekly_installs"] * NEW_USER_CONVERSION * AVG_TICKET
    returning_users = (
        global_df["weekly_active_users"] - global_df["weekly_installs"]
    ).clip(lower=0)
    base_rev = returning_users * RETENTION_CONVERSION * AVG_TICKET
    global_df["weekly_iap_revenue"] = np.where(
        global_df["in_app_purchases"], new_rev + base_rev, 0.0
    )
    ecpm_benchmarks = get_ecpm_benchmarks(pgdb)
    avg_ecpm = ecpm_benchmarks.groupby("tier_slug")["ecpm"].mean()
    imps_per_user = 3
    global_df["weekly_ad_revenue"] = np.where(
        global_df["ad_supported"],
        (
            (global_df["wau_tier1"] * imps_per_user * avg_ecpm.get("tier1", 0))
            + (global_df["wau_tier2"] * imps_per_user * avg_ecpm.get("tier2", 0))
            + (global_df["wau_tier3"] * imps_per_user * avg_ecpm.get("tier3", 0))
        )
        / 1000,
        0.0,
    )
    return global_df
