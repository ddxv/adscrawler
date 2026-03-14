import datetime

import duckdb
import numpy as np
import pandas as pd

from adscrawler.app_stores.utils import (
    get_parquet_paths_by_prefix,
)
from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import (
    PostgresCon,
)
from adscrawler.dbcon.queries import (
    delete_app_metrics_by_date_and_apps,
    get_ecpm_benchmarks,
    get_retention_benchmarks,
    insert_bulk,
    query_countries,
    query_live_apps,
    query_store_app_categories,
)
from adscrawler.packages.storage import (
    delete_s3_objects_by_date_range,
    delete_s3_objects_by_prefix,
    get_duckdb_connection,
)

logger = get_logger(__name__, "scrape_stores")

# TODO: Replace with category and store benchmarks
NEW_USER_CONVERSION = 0.02  # 2% of new installs
RETENTION_CONVERSION = 0.002  # 0.2% of returning users (2 in 1,000)
AVG_TICKET = 5.0  # $5 average


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

GLOBAL_HISTORY_COLS = (
    GLOBAL_HISTORY_KEYS
    + METRIC_COLS
    + ["store_last_updated", "tier1_pct", "tier2_pct", "tier3_pct"]
)

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
    "store_last_updated",
]


RAW_DATA_PREFIX = "raw-data/app_details"
AGG_APP_HASH_BUCKETS_DAILY = "agg-data/app-hash-daily"
AGG_APP_HASH_BUCKETS_WEEKLY = "agg-data/app-hash-weekly"
AGG_APP_HASH_BUCKETS_FILLED = "agg-data/app-hash-weekly-filled"


def check_for_duplicates(df: pd.DataFrame, key_columns: list[str]) -> None:
    any_duplicates = df.duplicated(subset=key_columns).any()
    if any_duplicates:
        dups = df[df.duplicated(subset=key_columns)][
            key_columns + ["store_id", "crawled_at"]
        ]
        logger.error(f"Duplicates found in app history data! {dups}")
        raise ValueError("Duplicates found in app history data")
    return


def handle_missing_trackid_files(
    duckdb_con: duckdb.DuckDBPyConnection, app_detail_parquets: list[str], store: int
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
            # delete_s3_objects_by_prefix(bucket='adscrawler', prefix=parquet)
    else:
        logger.error(
            "trackId column missing but store is not Apple, investigate data issue"
        )


def delete_and_aggregate_s3_agg(
    store: int,
    database_connection: PostgresCon,
) -> None:

    end_date = datetime.date.today()
    raw_data_lookback_days = 3
    # Rerun this week daily and last week at least once
    raw_weekly_lookback_days = 8
    interpolate_query_lookback_days = 180
    # Delete window: only remove what we're confident we'll rewrite
    interpolate_delete_lookback_days = 90
    db_delete_lookback_days = 30

    # Raw data to agg by DAY
    # Purely additive, only needs to be run once for 'yesterday'
    raw_start = end_date - datetime.timedelta(days=raw_data_lookback_days)
    for snapshot_date in pd.date_range(raw_start, end_date, freq="D"):
        log_info = f"{store=} {snapshot_date.date()} S3 raw app details agg"
        logger.info(f"{log_info} start")
        make_s3_app_hash_metrics_history_daily(store=store, snapshot_date=snapshot_date)

    # Agg DAY -> WEEK
    raw_weekly_start = end_date - pd.Timedelta(days=raw_weekly_lookback_days)
    start_date_mon = raw_weekly_start - pd.Timedelta(days=raw_weekly_start.weekday())
    for hash_bucket in [f"{i:02x}" for i in range(256)]:
        for week_start in pd.date_range(start_date_mon, end_date, freq="W-MON"):
            log_info = f"{store=} hash={hash_bucket} {week_start.date()} S3 weekly agg"
            logger.info(f"{log_info} start")
            make_s3_app_hash_metrics_history_weekly(
                store=store,
                range_start=week_start.date(),
                range_end=week_start.date() + pd.Timedelta(days=6),
                hash_bucket=hash_bucket,
                clear_bucket_by_week=True,
            )

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
        # This can run for last 30 days only
        db_delete_start = end_date - datetime.timedelta(days=db_delete_lookback_days)
        process_app_metrics_to_db(
            hash_bucket=hash_bucket,
            database_connection=database_connection,
            store=store,
            db_delete_start=db_delete_start,
        )


def copy_daily_to_weekly_hash_buckets(
    store: int,
    daily_parquet_paths: list[str],
) -> str:
    bucket = CONFIG["s3"]["bucket"]
    if store == 1:
        sel_metrics = "installs, rating, rating_count, review_count, histogram, store_last_updated"
        fin_metrics = "installs, rating, rating_count, review_count, histogram, store_last_updated"
        extra_sort_column = "review_count DESC"
    elif store == 2:
        sel_metrics = (
            "rating_count, rating, user_ratings AS histogram, store_last_updated"
        )
        fin_metrics = "rating_count, rating, histogram, store_last_updated"
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
              SELECT * FROM deduped
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
    # has 1s delays
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
    duckdb_con = get_duckdb_connection(s3_config_key)
    duckdb_con.execute(query)
    duckdb_con.close()


def make_s3_app_hash_metrics_history_daily(
    store: int, snapshot_date: pd.DatetimeIndex
) -> None:
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")
    prefix = (
        f"{RAW_DATA_PREFIX}/store={store}/crawled_date={snapshot_date_str}/country="
    )
    all_parquet_paths = get_parquet_paths_by_prefix(bucket, prefix)
    # hex_values = [f"{i:02x}" for i in range(256)]
    # for hex_val in hex_values:
    #     agg_prefix = f"{AGG_APP_HASH_BUCKETS}/store={store}/hash_bucket={hex_val}/snapshot_date={snapshot_date_str}/"
    #     delete_s3_objects_by_prefix(
    #         bucket=bucket, prefix=agg_prefix, key_name=s3_config_key
    #     )
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
    duckdb_con = get_duckdb_connection(s3_config_key)
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
            # raise
            logger.warning(f"Unexpected BinderException: {e}, investigate data issue")
    duckdb_con.close()


def estimate_ios_installs(df: pd.DataFrame) -> pd.DataFrame:
    # For iOS we don't have installs, but we can estimate them using the review count and a conversion rate
    # This is a very rough estimate and should be replaced with actual install data if possible
    conversion_rate = 0.02
    df["installs_est"] = (df["rating_count"] / conversion_rate).fillna(0).astype(int)
    return df


def merge_in_db_ids(
    df: pd.DataFrame, store: int, database_connection: PostgresCon
) -> pd.DataFrame:
    store_id_map = query_live_apps(store=store, database_connection=database_connection)
    df = pd.merge(
        df,
        store_id_map[["store_id", "id"]].rename(columns={"id": "store_app"}),
        on="store_id",
        how="inner",
    )
    if df["store_app"].isna().any():
        logger.warning(f"Found new store ids: {len(df[df['store_app'].isna()])}")
        raise ValueError("New store ids found in S3 app history data")
    country_map = query_countries(database_connection)
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


def process_metrics_google(df: pd.DataFrame) -> pd.DataFrame:
    # Take the US, crawled most, installs, rating_count are already global
    # Review_count is a max or sum depending on global_fallback logic
    # Stars should follow the global_fallback logic
    # At the end, after the countries pct_of_global is calculated
    # Merge the country tiers as sum of pct_of_global back to global_df
    global_df = df[df["country_id"] == 840].copy()
    global_df = global_df.rename(
        columns={
            "global_installs": "installs",
            "global_rating_count": "rating_count",
        }
    )
    df["store_last_updated"] = np.where(
        (df["store_last_updated"].isna() | df["store_last_updated"] < 0),
        None,
        df["store_last_updated"],
    )
    df.loc[df["store_last_updated"].notna(), "store_last_updated"] = pd.to_datetime(
        df.loc[df["store_last_updated"].notna(), "store_last_updated"],
        unit="s",
    )
    # Calculate each pct of global based on review_count
    # This is then used to estimate installs per country
    # pct_of_global will also be used for tiers
    df["max_reviews"] = df.groupby(["store_app", "week_start"])[
        "review_count"
    ].transform("max")
    df["global_installs"] = df.groupby(["store_app", "week_start"])[
        "global_installs"
    ].transform("max")
    df["global_rating_count"] = (
        df.groupby(["store_app", "week_start"])["global_rating_count"]
        .transform("max")
        .fillna(0)
    )
    df["is_max_candidate"] = (df["review_count"] >= df["max_reviews"] * 0.96) & (
        df["max_reviews"] > 200
    )
    candidate_counts = df.groupby(["store_app", "week_start"])[
        "is_max_candidate"
    ].transform("sum")
    # Multiple countries all near the same large max → they're all receiving the global value
    df["is_global_fallback"] = (
        df["is_max_candidate"].notna() & df["is_max_candidate"] & (candidate_counts > 1)
    )
    # For fallback rows: we don't know their true country split → set pct to 0
    df["true_review_count"] = np.where(df["is_global_fallback"], 0, df["review_count"])
    # Since the US has a long history of being queried
    # Use country ratio of review_counts to backfill missing review_counts
    # Found useful for musically
    us_lookup = (
        df[df["country_id"] == 840]
        .groupby(["store_app", "week_start"])["true_review_count"]
        .max()
    )
    df["us_review_count"] = df.set_index(["store_app", "week_start"]).index.map(
        us_lookup
    )
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
    local_sums = (
        df[~df["is_global_fallback"]]
        .groupby(["store_app", "week_start"])["true_review_count"]
        .sum()
    )
    # has_any_candidate = candidate_counts > 0
    has_global_fallback = df.groupby(["store_app", "week_start"])[
        "is_global_fallback"
    ].transform("sum")
    # Multiple countries all nea
    df["global_review_count"] = np.where(
        has_global_fallback,
        df["max_reviews"],  # best single-source estimate of global
        df.set_index(["store_app", "week_start"]).index.map(local_sums).values,
    )
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
    # TODO: Stars need to be fixed per is_global_fallback logic
    histo_nulls = global_df["histogram"].isna()
    global_df.loc[histo_nulls, "histogram"] = pd.Series(
        [[0, 0, 0, 0, 0]] * histo_nulls.sum(), index=global_df.index[histo_nulls]
    )
    star_df = pd.DataFrame(global_df["histogram"].tolist(), index=global_df.index)
    try:
        star_df.columns = STAR_COLS
    except ValueError:
        for col in STAR_COLS:
            if col not in star_df.columns:
                star_df[col] = 0
    global_df = pd.concat([global_df, star_df], axis=1)
    tier_pct = df.pivot_table(
        index=GLOBAL_HISTORY_KEYS,
        columns="tier",
        values="pct_of_global",
        aggfunc="sum",
        fill_value=0,
        dropna=False,
    ).rename(columns={"tier1": "tier1_pct", "tier2": "tier2_pct", "tier3": "tier3_pct"})
    tier_pct_cols = ["tier1_pct", "tier2_pct", "tier3_pct"]
    if not all(col in tier_pct.columns for col in tier_pct_cols):
        for col in tier_pct_cols:
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
    global_df[["tier1_pct", "tier2_pct", "tier3_pct"]]
    tier_cols = ["tier1_pct", "tier2_pct", "tier3_pct"]
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


def process_metrics_apple(
    df: pd.DataFrame,
) -> pd.DataFrame:
    # Since the US has a long history of being queried
    # Use country ratio of review_counts to backfill missing review_counts
    # Found useful for musically
    us_lookup = (
        df[df["country_id"] == 840]
        .groupby(["store_app", "week_start"])["rating_count"]
        .max()
    )
    df["us_rating_count"] = df.set_index(["store_app", "week_start"]).index.map(
        us_lookup
    )
    df["pct_of_us_ratings"] = df["rating_count"] / df["us_rating_count"].replace(
        0, np.nan
    )
    df = df.sort_values("week_start")
    df["pct_of_us_ratings"] = df.groupby(["store_app", "country_id"])[
        ["pct_of_us_ratings"]
    ].bfill()
    df["true_review_count"] = np.where(
        df["rating_count"].isna(),
        df["us_rating_count"] * df["pct_of_us_ratings"],
        df["rating_count"],
    )
    ratings_str = (
        df["histogram"]
        .str.extractall(r"(\d+)")
        .reset_index()
        .pivot_table(index="level_0", columns="match", values=0, aggfunc="first")
        .astype("Int64")
        .reindex(df.index, fill_value=0)
    )
    df[STAR_COLS] = ratings_str.iloc[:, 1::2].astype(int).to_numpy()
    df["store_last_updated"] = pd.to_datetime(
        df["store_last_updated"], format="ISO8601", utc=True
    )
    df = estimate_ios_installs(df)
    df["rating_prod"] = df["rating"] * df["rating_count"]
    # future_ios_ratios = get_ios_cached_future_country_ratios(database_connection)
    df["weekly_total_ratings"] = df.groupby(["week_start", "store_app"])[
        "rating_count"
    ].transform("sum")
    # Latest rating_count per store_app + country_id (by week_start)
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
    df["global_rating_count"] = df.groupby(["week_start", "store_app"])[
        "rating_count"
    ].transform("sum")
    # Here CDF is the latest row per country per app, some countries older
    df["grc_future_est_a"] = df["rating_count"] / df["rating_ratio"]
    df["grc_est_prod"] = df["rating_count"] * df["grc_future_est_a"]
    df["grc_future_est"] = df.groupby(["week_start", "store_app"])[
        "grc_est_prod"
    ].transform("sum") / df.groupby(["week_start", "store_app"])[
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
    app_mean_rating = df.groupby(["week_start", "store_app"])["rating"].transform(
        "mean"
    )
    df["rating"] = df["rating"].fillna(app_mean_rating)
    df["rating_prod"] = df["rating"] * df["rating_count"]
    # overwrites installs_est from db, but quick fix for missing, same value
    df = estimate_ios_installs(df)
    global_df = df.groupby(["week_start", "store_app"]).agg(
        rating_count=("rating_count", "sum"),
        rating_prod=("rating_prod", "sum"),
        installs=("installs_est", "sum"),
        store_last_updated=("store_last_updated", "max"),
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
    if store == 1:
        metrics = [
            "installs",
            "rating_count",
            "review_count",
        ]
        interpolated_metrics = """
                MIN_BY(installs,     week_start) OVER w_future AS installs_y2,
                MIN_BY(rating_count, week_start) OVER w_future AS rating_count_y2,
                MIN_BY(review_count, week_start) OVER w_future AS review_count_y2,
                """
        coalesce_metrics = """
                COALESCE(
                    a_exact.installs,
                    a_week.installs,
                    a_prev.installs +
                        DATE_DIFF('day', a_prev.week_start, m.week_start)::DOUBLE *
                        (a_prev.installs_y2 - a_prev.installs)::DOUBLE /
                        NULLIF(DATE_DIFF('day', a_prev.week_start, a_prev.x2)::DOUBLE, 0.0)
                )::BIGINT AS installs,
                COALESCE(
                    a_exact.rating_count,
                    a_week.rating_count,
                    a_prev.rating_count +
                        DATE_DIFF('day', a_prev.week_start, m.week_start)::DOUBLE *
                        (a_prev.rating_count_y2 - a_prev.rating_count)::DOUBLE /
                        NULLIF(DATE_DIFF('day', a_prev.week_start, a_prev.x2)::DOUBLE, 0.0)
                )::BIGINT AS rating_count,
                COALESCE(
                    a_exact.review_count,
                    a_week.review_count,
                    a_prev.review_count +
                        DATE_DIFF('day', a_prev.week_start, m.week_start)::DOUBLE *
                        (a_prev.review_count_y2 - a_prev.review_count)::DOUBLE /
                        NULLIF(DATE_DIFF('day', a_prev.week_start, a_prev.x2)::DOUBLE, 0.0)
                )::BIGINT AS review_count,
                """
    elif store == 2:
        metrics = [
            "rating_count",
        ]
        interpolated_metrics = """
                MIN_BY(rating_count, week_start) OVER w_future AS rating_count_y2,
                """
        coalesce_metrics = """
               COALESCE(
                    a_exact.rating_count,
                    a_week.rating_count,
                    a_prev.rating_count +
                        DATE_DIFF('day', a_prev.week_start, m.week_start)::DOUBLE *
                        (a_prev.rating_count_y2 - a_prev.rating_count)::DOUBLE /
                        NULLIF(DATE_DIFF('day', a_prev.week_start, a_prev.x2)::DOUBLE, 0.0)
                )::BIGINT AS rating_count, 
                """
    else:
        raise ValueError(f"Unsupported store: {store}")
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
                {",".join(metrics)},
                rating,
                histogram,
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

        -- ============================================================
        -- Anchors — window functions over real rows only
        -- ============================================================
        anchors AS (
            SELECT
                store_id,
                country,
                week_start,
                days_since_monday,
                -- Actuals (used when a real crawl lands on a Monday)
                {",".join(metrics)},
                rating,
                histogram,
                store_last_updated,
                -- Interpolation anchors
               {interpolated_metrics} 
                -- Shared x-axis
                MIN(week_start) OVER w_future AS x2,
                -- Carry-forward
                MAX_BY(rating,             week_start) OVER w_past_inclusive AS rating_carry,
                MAX_BY(histogram,          week_start) OVER w_past_inclusive AS histogram_carry,
                MAX_BY(store_last_updated, week_start) OVER w_past_inclusive AS store_last_updated_carry
            FROM weekly_data
            WINDOW
                w_future AS (PARTITION BY store_id, country
                             ORDER BY week_start
                             ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),
                w_past_inclusive AS (PARTITION BY store_id, country
                                     ORDER BY week_start
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ),

        -- ============================================================
        -- 4. Interpolate onto Mondays, using real Mondays when available, otherwise using the bracketing anchors to interpolate, or carry forward if at start/end of series
        -- ============================================================
       interpolated AS (
        SELECT
            dims.store_id,
            left(md5(dims.store_id), 2) AS hash_bucket,
            dims.country,
            m.week_start,
            {coalesce_metrics} 
            COALESCE(a_exact.rating,             a_week.rating,             a_prev.rating_carry)            AS rating,
            COALESCE(a_exact.histogram,          a_week.histogram,          a_prev.histogram_carry)         AS histogram,
            COALESCE(a_exact.store_last_updated, a_week.store_last_updated, a_prev.store_last_updated_carry) AS store_last_updated

        FROM (SELECT DISTINCT store_id, country FROM weekly_data) dims
        CROSS JOIN target_mondays m

        -- Exact hit: crawl lands exactly on Monday
        LEFT JOIN anchors a_exact
                ON a_exact.store_id      = dims.store_id
                AND a_exact.country       = dims.country
                AND a_exact.week_start = m.week_start
                AND a_exact.days_since_monday = 0

        -- Intra-week hit: best crawl within Tue-Sun of this week
        LEFT JOIN anchors a_week
                ON a_week.store_id      = dims.store_id
                AND a_week.country       = dims.country
                AND a_week.week_start = m.week_start
                AND a_week.days_since_monday BETWEEN 1 AND 6 

        -- Bracketing anchor: finds the row where target Monday falls [anchor_week, next_anchor_week)
        -- Interpolation uses: 
        LEFT JOIN anchors a_prev
                ON a_prev.store_id      = dims.store_id
                AND a_prev.country       = dims.country
                AND a_prev.week_start = (
                        SELECT MAX(week_start)
                        FROM weekly_data
                        WHERE store_id      = dims.store_id
                        AND country       = dims.country
                        AND week_start < m.week_start
                    )

        -- Only emit a week if there is real data somewhere to anchor it
        WHERE a_exact.{metrics[0]} IS NOT NULL        -- crawl on Monday
            OR a_week.{metrics[0]}  IS NOT NULL        -- crawl later in week
            OR (a_prev.week_start IS NOT NULL AND a_prev.x2 IS NOT NULL) -- interpolatable: both brackets exist
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
    duckdb_con = get_duckdb_connection(s3_config_key)
    duckdb_con.execute(msv_query)
    duckdb_con.close()


def process_metrics(
    store: int,
    df: pd.DataFrame,
    database_connection: PostgresCon,
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
    tiers = ["tier1_pct", "tier2_pct", "tier3_pct"]
    for col in tiers:
        if col not in global_df.columns:
            global_df[col] = 0.0
        global_df[col] = global_df[col].fillna(0.0)
    global_df = global_df[[x for x in GLOBAL_HISTORY_COLS if x in global_df.columns]]
    app_cats = query_store_app_categories(
        database_connection, store_apps=global_df["store_app"].unique().tolist()
    )
    global_df = global_df.merge(app_cats, on="store_app", how="left", validate="m:1")
    # Fill in blank rows with default
    global_df["tier_pct_sum"] = (
        global_df["tier1_pct"] + global_df["tier2_pct"] + global_df["tier3_pct"]
    )
    incomplete_tier_pct = global_df["tier_pct_sum"] < 0.5
    # Because for US is default, many apps end up with 1.0 t1
    all_t1 = global_df["tier1_pct"] == 1
    tiers_to_fill = incomplete_tier_pct | all_t1
    global_df.loc[tiers_to_fill, "tier1_pct"] = 0.34
    global_df.loc[tiers_to_fill, "tier2_pct"] = 0.33
    global_df.loc[tiers_to_fill, "tier3_pct"] = 0.33
    global_df = calculate_active_users(database_connection, global_df, store=store)
    global_df = calculate_revenue_cols(database_connection, global_df)
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
    for col in tiers:
        global_df[col] = (global_df[col] * 10000).round().fillna(0).astype("int16")
    if store == 1:
        global_df.loc[global_df["store_last_updated"].notna(), "store_last_updated"] = (
            pd.to_datetime(
                global_df.loc[
                    global_df["store_last_updated"].notna(), "store_last_updated"
                ],
                unit="s",
            )
        )
    global_df = global_df[GLOBAL_FINAL_COLS]
    logger.info(f"{log_info} finished")
    return country_df, global_df


def delete_and_insert_app_metrics(
    database_connection: PostgresCon,
    country_df: pd.DataFrame,
    global_df: pd.DataFrame,
    store: int,
    delete_from_date: datetime.date,
) -> None:
    log_info = f"{store=} {delete_from_date=} delete_and_insert_app_metrics"
    table_name = "app_country_metrics_history"
    logger.info(f"{log_info} {table_name=} start")
    delete_app_metrics_by_date_and_apps(
        database_connection=database_connection,
        delete_from_date=delete_from_date,
        store_apps=country_df["store_app"].unique().tolist(),
        table_name=table_name,
    )
    insert_bulk(
        df=country_df,
        table_name=table_name,
        database_connection=database_connection,
    )
    table_name = "app_global_metrics_history"
    logger.info(f"{log_info} {table_name=} start")
    delete_app_metrics_by_date_and_apps(
        database_connection=database_connection,
        delete_from_date=delete_from_date,
        store_apps=global_df["store_app"].unique().tolist(),
        table_name=table_name,
    )
    insert_bulk(
        df=global_df,
        table_name=table_name,
        database_connection=database_connection,
    )


def ffill_app_metrics(
    df: pd.DataFrame, store: int, database_connection: PostgresCon
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
            "global_installs",
            "global_rating_count",
            "rating",
            "store_last_updated",
            "histogram",
        ]
    elif store == 2:
        cols_to_ffill = [
            "rating_count",
            "rating",
            "store_last_updated",
            "histogram",
        ]
    df = df.sort_values("week_start")
    df[cols_to_ffill] = df.groupby(["store_app", "country_id"])[cols_to_ffill].ffill()
    df = df.reset_index()
    country_map = query_countries(database_connection=database_connection)
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
        (country_df["week_start"] == latest_week) & (country_df["installs"] > 100),
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
    duckdb_con = get_duckdb_connection(s3_config_key)
    df = duckdb_con.execute(
        f"""
        SELECT * FROM read_parquet({all_parquet_paths}, union_by_name=true)
        ORDER BY store_id, country, week_start
    """
    ).df()
    duckdb_con.close()
    return df


def process_app_metrics_to_db(
    hash_bucket: str,
    database_connection: PostgresCon,
    store: int,
    db_delete_start: datetime.date,
) -> None:
    log_info = (
        f"store={store} date={db_delete_start} hash_bucket={hash_bucket} process to DB"
    )
    logger.info(f"{log_info} start")
    df = get_app_hash_buckets_filled_from_s3(
        store=store,
        app_hash=hash_bucket,
    )
    logger.info(f"{log_info} got {df.shape[0]:,} interpolated rows from S3")
    if df.empty:
        logger.warning(f"{log_info} no data found for S3 agg app metrics, skipping")
        raise ValueError(f"{log_info} no data found for S3 agg app metrics")
    if store == 2:
        # This is an issue and needs to be resolved in the way the iOS store_id is stored into S3
        problem_rows = df["store_id"].str.contains(".0", regex=False)
        if problem_rows.any():
            df = df[~problem_rows]
            logger.warning(
                f"{log_info} found {problem_rows.sum()} rows with .0 suffix in store_id for Apple, removed them"
            )
    df = merge_in_db_ids(df, store, database_connection)
    df = df.drop(["store_id", "country"], axis=1)
    if store == 1:
        df.loc[df["store_last_updated"] <= 0, "store_last_updated"] = None
        df = df.rename(
            columns={
                "installs": "global_installs",
                "rating_count": "global_rating_count",
            }
        )
        # very rare some chrome os apps got downloaded
        df = df[~df["global_installs"].isna()]
    df = ffill_app_metrics(df, store, database_connection)

    country_df, global_df = process_metrics(
        store=store,
        df=df,
        database_connection=database_connection,
    )
    country_df, global_df = drop_unwanted_rows(
        country_df, global_df, store, db_delete_start
    )

    delete_and_insert_app_metrics(
        database_connection=database_connection,
        country_df=country_df,
        global_df=global_df,
        store=store,
        delete_from_date=db_delete_start,
    )


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
                      -- Generate a 2-character hex hash bucket from the store_id
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
          )
          ;
          """
    return query


def calculate_active_users(
    database_connection: PostgresCon, global_df: pd.DataFrame, store: int
) -> pd.DataFrame:
    """Process app global metrics weekly."""
    tier_pct_cols = ["tier1_pct", "tier2_pct", "tier3_pct"]
    metrics = [
        "installs",
        "rating",
        "rating_count",
        *STAR_COLS,
        *tier_pct_cols,
    ]
    xaxis_col = "week_start"
    # Convert to date to datetime and sort by country and date, required
    global_df[xaxis_col] = pd.to_datetime(global_df[xaxis_col])
    global_df = global_df.sort_values(["store_app", xaxis_col])
    global_df["installs_diff"] = (
        global_df.groupby("store_app")["installs"].diff().fillna(0)
    )
    global_df["installs_diff"] = (
        global_df.groupby("store_app")["installs"]
        .diff()
        .fillna(global_df["installs"])
        .fillna(0)
    )
    global_df["weekly_ratings"] = (
        global_df.groupby("store_app")["rating_count"]
        .diff()
        .fillna(global_df["rating_count"])
    )
    # This drops the earliest week, since the diff will count all metrics as weekly
    # This is ok for apps which are new to the range, that week all those metrics are new
    drop_rows = global_df["week_start"] == global_df["week_start"].min()
    global_df = global_df[~drop_rows]
    if store == 2:
        # iOS data is very estimated and noisy, so we smooth
        global_df["installs_diff"] = (
            global_df.groupby("store_app")["installs_diff"]
            .rolling(window=3, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        )
    retention_benchmarks = get_retention_benchmarks(database_connection)
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
    mau_mult = 3.5  # Standard estimate for Monthly Reach
    # Calculate MAU Retention Rate
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
        "store_last_updated",
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
    return global_df


def calculate_revenue_cols(
    database_connection: PostgresCon, df: pd.DataFrame
) -> pd.DataFrame:
    ecpm_benchmarks = get_ecpm_benchmarks(database_connection)
    df["wau_tier1"] = df["weekly_active_users"] * df["tier1_pct"]
    df["wau_tier2"] = df["weekly_active_users"] * df["tier2_pct"]
    df["wau_tier3"] = df["weekly_active_users"] * df["tier3_pct"]
    df["mau_tier1"] = df["monthly_active_users"] * df["tier1_pct"]
    df["mau_tier2"] = df["monthly_active_users"] * df["tier2_pct"]
    df["mau_tier3"] = df["monthly_active_users"] * df["tier3_pct"]

    def estimate_revenue(row: pd.Series) -> float:
        if not row["in_app_purchases"]:
            return 0.0
        # Revenue from new users
        new_rev: float = row["weekly_installs"] * NEW_USER_CONVERSION * AVG_TICKET
        # Revenue from the "sticky" base (WAU minus the new installs)
        returning_users = max(0, row["weekly_active_users"] - row["weekly_installs"])
        base_rev: float = returning_users * RETENTION_CONVERSION * AVG_TICKET
        return new_rev + base_rev

    df["weekly_iap_revenue"] = df.apply(estimate_revenue, axis=1)
    # 2. Calculate Ad Revenue
    avg_ecpm = ecpm_benchmarks.groupby("tier_slug")["ecpm"].mean()

    def calculate_ad_rev(row: pd.Series) -> float:
        if not row["ad_supported"]:
            return 0.0
        # Need benchmark for impressions per user
        imps_per_user = 3
        rev_t1 = (row["wau_tier1"] * imps_per_user * avg_ecpm.get("tier1", 0)) / 1000
        rev_t2 = (row["wau_tier2"] * imps_per_user * avg_ecpm.get("tier2", 0)) / 1000
        rev_t3 = (row["wau_tier3"] * imps_per_user * avg_ecpm.get("tier3", 0)) / 1000
        rev: float = rev_t1 + rev_t2 + rev_t3
        return rev

    df["weekly_ad_revenue"] = df.apply(calculate_ad_rev, axis=1)
    return df
