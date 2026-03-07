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
    delete_and_insert,
    delete_app_metrics_by_date_and_apps,
    get_ecpm_benchmarks,
    get_retention_benchmarks,
    insert_bulk,
    query_apps_to_process_global_metrics,
    query_countries,
    query_store_id_map_cached,
)
from adscrawler.packages.storage import (
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
    "snapshot_date",
    "store_app",
]
COUNTRY_HISTORY_KEYS = [
    "snapshot_date",
    "country_id",
    "store_app",
]

COUNTRY_HISTORY_COLS = (
    COUNTRY_HISTORY_KEYS
    + [x for x in METRIC_COLS if x != "installs"]
    + ["installs_est"]
)

GLOBAL_HISTORY_COLS = (
    GLOBAL_HISTORY_KEYS
    + METRIC_COLS
    + ["store_last_updated", "tier1_pct", "tier2_pct", "tier3_pct"]
)

# TABLE_KEYS = {
#     "app_country_metrics_history": COUNTRY_HISTORY_KEYS,
#     "app_global_metrics_history": GLOBAL_HISTORY_KEYS,
# }

# AGG_APP_COUNTRY_WEEKLY_PREFIX = "agg-data/app_country_metrics/weekly"
# AGG_APP_COUNTRY_DAILY_PREFIX = "agg-data/app_country_metrics/daily"
RAW_DATA_PREFIX = "raw-data/app_details"
AGG_APP_HASH_BUCKETS = "agg-data/app-hash"


# def get_s3_agg_weekly_snapshots(
#     start_date: datetime.date, end_date: datetime.date, store: int
# ) -> pd.DataFrame:
#     s3_config_key = "s3"
#     bucket = CONFIG[s3_config_key]["bucket"]
#     week_parquets = []
#     for ddt in pd.date_range(start_date, end_date, freq="D"):
#         ddt_str = ddt.strftime("%Y-%m-%d")
#         prefix = f"{AGG_APP_COUNTRY_WEEKLY_PREFIX}/store={store}/snapshot_date={ddt_str}/country="
#         week_parquets += get_parquet_paths_by_prefix(bucket, prefix)
#     if len(week_parquets) == 0:
#         logger.warning(
#             f"No parquet paths found for agg app snapshots {store=} {start_date=} {end_date=}"
#         )
#         return pd.DataFrame()
#     query = f"""SELECT
#        *
#       FROM read_parquet({week_parquets})
#       WHERE store_id IS NOT NULL
#     """
#     duckdb_con = get_duckdb_connection(s3_config_key)
#     df = duckdb_con.execute(query).df()
#     duckdb_con.close()
#     na_rows = df["store_id"].isna().sum()
#     if na_rows > 0:
#         logger.warning("Missing store_id values found")
#     return df


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
    store: int, start_date: datetime.date, end_date: datetime.date
) -> None:
    # Raw data to agg by DAY
    for snapshot_date in pd.date_range(start_date, end_date, freq="D"):
        log_info = f"{store=} {snapshot_date.date()} S3 raw app details agg"
        logger.info(f"{log_info} start")
        # make_s3_app_country_metrics_history_daily(
        #     store,
        #     snapshot_date=snapshot_date,
        # )
        make_s3_app_hash_metrics_history_daily(store=store, snapshot_date=snapshot_date)


# def make_s3_app_country_metrics_history_daily(
#     store: int, snapshot_date: pd.DatetimeIndex
# ) -> None:
#     s3_config_key = "s3"
#     bucket = CONFIG[s3_config_key]["bucket"]
#     snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")
#     raw_prefix = (
#         f"{RAW_DATA_PREFIX}/store={store}/crawled_date={snapshot_date_str}/country="
#     )
#     raw_parquets = get_parquet_paths_by_prefix(bucket, raw_prefix)
#     agg_prefix = f"{AGG_APP_COUNTRY_DAILY_PREFIX}/store={store}/snapshot_date={snapshot_date_str}/"
#     time.sleep(1)
#     delete_s3_objects_by_prefix(
#         bucket=bucket, prefix=agg_prefix, key_name=s3_config_key
#     )
#     time.sleep(1)
#     if len(raw_parquets) == 0:
#         logger.error(
#             f"No app detail parquet files found for store={store} snapshot_date={snapshot_date_str}"
#         )
#         return
#     query = copy_raw_details_to_daily_agg(
#         store=store,
#         app_detail_parquets=raw_parquets,
#         snapshot_date_str=snapshot_date_str,
#     )
#     duckdb_con = get_duckdb_connection(s3_config_key)
#     try:
#         duckdb_con.execute(query)
#     except duckdb.BinderException as e:
#         if store == 2 and """"trackId" not found""" in str(e):
#             logger.error(
#                 f"trackId column not found in parquets for store={store}, skipping"
#             )
#             handle_missing_trackid_files(duckdb_con, raw_parquets, store)
#         if store == 1 and """"appId" not found""" in str(e):
#             logger.error(
#                 f"appId column not found in parquets for store={store}, skipping"
#             )
#             handle_missing_trackid_files(duckdb_con, raw_parquets, store)
#         else:
#             raise
#     duckdb_con.close()


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
    #     agg_prefix = f"{AGG_APP_HASH_BUCKETS}/store={store}/app-hash={hex_val}/snapshot_date={snapshot_date_str}/"
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


# def make_s3_app_country_metrics_history_week(
#     store: int, snapshot_date: pd.DatetimeIndex, snapshot_end_date: pd.DatetimeIndex
# ) -> None:
#     s3_config_key = "s3"
#     bucket = CONFIG[s3_config_key]["bucket"]
#     snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")
#     all_parquet_paths = []
#     for ddt in pd.date_range(snapshot_date, snapshot_end_date, freq="D"):
#         ddt_str = ddt.strftime("%Y-%m-%d")
#         prefix = f"{AGG_APP_COUNTRY_DAILY_PREFIX}/store={store}/snapshot_date={ddt_str}/country="
#         all_parquet_paths += get_parquet_paths_by_prefix(bucket, prefix)
#     agg_prefix = f"{AGG_APP_COUNTRY_WEEKLY_PREFIX}/store={store}/snapshot_date={snapshot_date_str}/"
#     delete_s3_objects_by_prefix(
#         bucket=bucket, prefix=agg_prefix, key_name=s3_config_key
#     )
#     if len(all_parquet_paths) == 0:
#         logger.error(
#             f"No app detail parquet files found for store={store} snapshot_date={snapshot_date_str}"
#         )
#         return
#     query = copy_daily_agg_to_weekly(
#         store=store,
#         app_detail_parquets=all_parquet_paths,
#         snapshot_date_str=snapshot_date_str,
#     )
#     duckdb_con = get_duckdb_connection(s3_config_key)
#     duckdb_con.execute(query)
#     duckdb_con.close()


def estimate_ios_installs(df: pd.DataFrame) -> pd.DataFrame:
    # For iOS we don't have installs, but we can estimate them using the review count and a conversion rate
    # This is a very rough estimate and should be replaced with actual install data if possible
    conversion_rate = 0.02
    df["installs_est"] = (df["rating_count"] / conversion_rate).fillna(0).astype(int)
    return df


def merge_in_db_ids(
    df: pd.DataFrame, store: int, database_connection: PostgresCon
) -> pd.DataFrame:
    store_id_map = query_store_id_map_cached(
        store=store, database_connection=database_connection
    )
    df = pd.merge(
        df,
        store_id_map[["store_id", "id"]].rename(columns={"id": "store_app"}),
        on="store_id",
        how="left",
        validate="m:1",
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
            "week_start": "snapshot_date",
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
    # df["review_count"] = df["review_count"].replace(np.nan, 0).astype(int)
    # df["global_rating_count"] = df["global_rating_count"].fillna(0).astype(int)
    # df["global_review_count"] = df["global_review_count"].fillna(0).astype(int)
    # df["global_installs"] = df["global_installs"].fillna(0).astype(int)
    # Db currently does not have a rating_count_est column just for country
    # df["rating_count"] = df["rating_count_est"]
    # assert (
    #     df["global_rating_count"].ge(df["rating_count"]).all()
    # ), "global_rating_count should be >= rating_count"
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
    tier_pct = (
        df.rename(columns={"week_start": "snapshot_date"})
        .pivot_table(
            index=GLOBAL_HISTORY_KEYS,
            columns="tier",
            values="pct_of_global",
            aggfunc="sum",
            fill_value=0,
            dropna=False,
        )
        .rename(
            columns={"tier1": "tier1_pct", "tier2": "tier2_pct", "tier3": "tier3_pct"}
        )
    )
    global_df = global_df.join(tier_pct)
    df["rating"] = df["rating"].replace(0, np.nan)
    df["rating_count_est"] = df["rating_count_est"].replace(0, np.nan)
    df.loc[
        (df["rating_count_est"] > 0) & (df["rating"] > 0), "rating_count_counter"
    ] = df["rating_count_est"]
    df["rating_prod"] = df["rating"] * df["rating_count_est"]
    grouped = df.rename(columns={"week_start": "snapshot_date"}).groupby(
        GLOBAL_HISTORY_KEYS
    )
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
            "week_start": "snapshot_date",
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
    ].transform(
        "sum"
    )
    df["rating_count"] = (
        df["rating_count"]
        .fillna(df["grc_future_est"] * df["rating_ratio"])
        .replace([np.inf, -np.inf], 0)
        .fillna(0)
        .round(0)
        .astype(int)
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
    df = df.rename(columns={"week_start": "snapshot_date"})
    global_df = global_df.rename(columns={"week_start": "snapshot_date"})
    # Note, we return the df which for iOS is the country level data
    return df, global_df


def import_app_metrics_from_s3(
    database_connection: PostgresCon,
    start_date: datetime.date,
    end_date: datetime.date,
    store: int,
) -> None:
    start_date_mon = start_date - pd.Timedelta(days=start_date.weekday())
    for hash_bucket in [f"{i:02x}" for i in range(256)]:
        log_info = f"{store=} {start_date_mon=} {end_date=} hash_bucket={hash_bucket}"
        logger.info(f"{log_info} start")
        try:
            process_app_metrics_to_db(
                hash_bucket=hash_bucket,
                database_connection=database_connection,
                store=store,
                start_date=start_date_mon,
                end_date=end_date,
            )
        except Exception as e:
            logger.error(f"{log_info} error: {e}")


def get_raw_app_hash_buckets_from_s3(
    start_date_mon: datetime.date, end_date: datetime.date, store: int, app_hash: str
) -> pd.DataFrame:
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    all_parquet_paths = []
    for ddt in pd.date_range(start_date_mon, end_date, freq="D"):
        ddt_str = ddt.strftime("%Y-%m-%d")
        prefix = f"{AGG_APP_HASH_BUCKETS}/store={store}/hash_bucket={app_hash}/snapshot_date={ddt_str}/"
        all_parquet_paths += get_parquet_paths_by_prefix(bucket, prefix)
    if len(all_parquet_paths) == 0:
        logger.warning(
            f"No parquet paths found for agg app hash buckets {store=} {start_date_mon=} {end_date=}"
        )
        return pd.DataFrame()
    if store == 1:
        metrics = [
            "installs",
            "rating",
            "rating_count",
            "review_count",
            "histogram",
            "store_last_updated",
        ]
    query = f"""SELECT 
       snapshot_date, store_id, country, {",".join(metrics)}
      FROM read_parquet({all_parquet_paths}, union_by_name=true)
    """
    duckdb_con = get_duckdb_connection(s3_config_key)
    df = duckdb_con.execute(query).df()
    duckdb_con.close()
    return df


def get_app_hash_buckets_from_s3(
    start_date_mon: datetime.date, end_date: datetime.date, store: int, app_hash: str
) -> pd.DataFrame:
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    all_parquet_paths = []
    for ddt in pd.date_range(start_date_mon, end_date, freq="D"):
        ddt_str = ddt.strftime("%Y-%m-%d")
        prefix = f"{AGG_APP_HASH_BUCKETS}/store={store}/hash_bucket={app_hash}/snapshot_date={ddt_str}/"
        all_parquet_paths += get_parquet_paths_by_prefix(bucket, prefix)
    if len(all_parquet_paths) == 0:
        logger.warning(
            f"No parquet paths found for agg app hash buckets {store=} {start_date_mon=} {end_date=}"
        )
        return pd.DataFrame()
    if store == 1:
        metrics = [
            "installs",
            "rating_count",
            "review_count",
        ]
        interpolated_metrics = """
                MAX_BY(installs,     snapshot_date) OVER w_past   AS installs_y1,
                MIN_BY(installs,     snapshot_date) OVER w_future AS installs_y2,
                MAX_BY(rating_count, snapshot_date) OVER w_past   AS rating_count_y1,
                MIN_BY(rating_count, snapshot_date) OVER w_future AS rating_count_y2,
                MAX_BY(review_count, snapshot_date) OVER w_past   AS review_count_y1,
                MIN_BY(review_count, snapshot_date) OVER w_future AS review_count_y2,
                """
        coalesce_metrics = """
                COALESCE(
                    a_exact.installs,
                    a_prev.installs_y1 +
                        (m.snapshot_date - a_prev.x1) *
                        (a_prev.installs_y2 - a_prev.installs_y1) /
                        (a_prev.x2 - a_prev.x1)::DOUBLE
                )::BIGINT AS installs,
                COALESCE(
                    a_exact.rating_count,
                    a_prev.rating_count_y1 +
                        (m.snapshot_date - a_prev.x1) *
                        (a_prev.rating_count_y2 - a_prev.rating_count_y1) /
                        (a_prev.x2 - a_prev.x1)::DOUBLE
                )::BIGINT AS rating_count,
                COALESCE(
                    a_exact.review_count,
                    a_prev.review_count_y1 +
                        (m.snapshot_date - a_prev.x1) *
                        (a_prev.review_count_y2 - a_prev.review_count_y1) /
                        (a_prev.x2 - a_prev.x1)::DOUBLE
                )::BIGINT AS review_count,
                """
        histogram = "histogram,"
    if store == 2:
        histogram = "user_ratings AS histogram,"
        metrics = [
            "rating_count",
        ]
        interpolated_metrics = """
                MAX_BY(rating_count, snapshot_date) OVER w_past   AS rating_count_y1,
                MIN_BY(rating_count, snapshot_date) OVER w_future AS rating_count_y2,
                """
        coalesce_metrics = """
               COALESCE(
                    a_exact.rating_count,
                    a_prev.rating_count_y1 +
                        (m.snapshot_date - a_prev.x1) *
                        (a_prev.rating_count_y2 - a_prev.rating_count_y1) /
                        (a_prev.x2 - a_prev.x1)::DOUBLE
                )::BIGINT AS rating_count, 
                """
    start_date_mon_str = start_date_mon.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    msv_query = f"""WITH 
        raw_data AS (
            SELECT
                store_id,
                country,
                CAST(snapshot_date AS DATE) AS snapshot_date,
                {",".join(metrics)},
                rating,
                {histogram}
                store_last_updated
            FROM read_parquet({all_parquet_paths}, hive_partitioning = true, union_by_name = true)
        ),
        raw_deduped AS (
            SELECT
                store_id, country, snapshot_date,
                {",".join(["MAX(" + metric + ") AS " + metric for metric in metrics])},
                MAX(rating)            AS rating,
                MAX(histogram)         AS histogram,
                MAX(store_last_updated) AS store_last_updated
            FROM raw_data
            GROUP BY store_id, country, snapshot_date
        ),

        target_mondays AS (
            SELECT CAST(range AS DATE) AS snapshot_date
            FROM range(DATE '{start_date_mon_str}', DATE '{end_date_str}', INTERVAL 7 DAY)
        ),

        -- ============================================================
        -- Anchors — window functions over real rows only
        -- ============================================================
        anchors AS (
            SELECT
                store_id,
                country,
                snapshot_date,
                -- Actuals (used when a real crawl lands on a Monday)
                {",".join(metrics)},
                rating,
                histogram,
                store_last_updated,
                -- Interpolation anchors
               {interpolated_metrics} 
                -- Shared x-axis
                MAX(snapshot_date) OVER w_past   AS x1,
                MIN(snapshot_date) OVER w_future AS x2,
                -- Carry-forward
                MAX_BY(rating,             snapshot_date) OVER w_past_inclusive AS rating_carry,
                MAX_BY(histogram,          snapshot_date) OVER w_past_inclusive AS histogram_carry,
                MAX_BY(store_last_updated, snapshot_date) OVER w_past_inclusive AS store_last_updated_carry
            FROM raw_deduped
            WINDOW
                w_past AS (PARTITION BY store_id, country
                           ORDER BY snapshot_date
                           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                w_future AS (PARTITION BY store_id, country
                             ORDER BY snapshot_date
                             ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),
                w_past_inclusive AS (PARTITION BY store_id, country
                                     ORDER BY snapshot_date
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ),

        -- ============================================================
        -- 4. Interpolate onto Mondays
        -- ============================================================
        interpolated AS (
            SELECT
                dims.store_id,
                dims.country,
                m.snapshot_date AS week_start,
                {coalesce_metrics}
                COALESCE(a_exact.rating,             a_prev.rating_carry)              AS rating,
                COALESCE(a_exact.histogram,          a_prev.histogram_carry)           AS histogram,
                COALESCE(a_exact.store_last_updated, a_prev.store_last_updated_carry)  AS store_last_updated

            FROM (SELECT DISTINCT store_id, country FROM raw_deduped) dims
            CROSS JOIN target_mondays m

            -- Exact hit: real crawl on this Monday
            LEFT JOIN anchors a_exact
                   ON a_exact.store_id      = dims.store_id
                  AND a_exact.country       = dims.country
                  AND a_exact.snapshot_date = m.snapshot_date

            -- Bracketing anchor: the real row whose gap contains this Monday
            LEFT JOIN anchors a_prev
               ON a_prev.store_id      = dims.store_id
              AND a_prev.country       = dims.country
              AND a_prev.snapshot_date = (
                      SELECT MAX(snapshot_date) 
                      FROM raw_deduped
                      WHERE store_id      = dims.store_id
                        AND country       = dims.country
                        AND snapshot_date < m.snapshot_date
                  )

            WHERE a_exact.{metrics[0]} IS NOT NULL             -- exact hit, OR
               OR (a_prev.x1 IS NOT NULL                  -- bracketed: drop leading/trailing
                   AND a_prev.x2 IS NOT NULL)
        )

        SELECT * FROM interpolated
        ORDER BY store_id, country, week_start;
        """
    duckdb_con = get_duckdb_connection(s3_config_key)
    df = duckdb_con.execute(msv_query).df()
    duckdb_con.close()
    return df


def process_metrics(
    store: int,
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    log_info = f"{store=} process metrics for {df.shape[0]} rows"
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
    for col in ["tier1_pct", "tier2_pct", "tier3_pct"]:
        if col not in global_df.columns:
            global_df[col] = 0.0
        global_df[col] = (global_df[col] * 10000).round().fillna(0).astype("int16")
    check_for_duplicates(
        df=global_df,
        key_columns=GLOBAL_HISTORY_KEYS,
    )
    global_df = global_df.replace({pd.NA: None, np.nan: None})
    country_df = country_df[
        [x for x in COUNTRY_HISTORY_COLS if x in country_df.columns]
    ]
    global_df = global_df[[x for x in GLOBAL_HISTORY_COLS if x in global_df.columns]]
    logger.info(f"{log_info} finished")
    return country_df, global_df


def delete_and_insert_app_metrics(
    database_connection: PostgresCon,
    country_df: pd.DataFrame,
    global_df: pd.DataFrame,
    store: int,
    snapshot_date: datetime.date,
    end_date: datetime.date,
) -> None:
    log_info = f"{store=} {snapshot_date=} delete_and_insert_app_metrics"
    table_name = "app_country_metrics_history"
    logger.info(f"{log_info} {table_name=} start")
    delete_app_metrics_by_date_and_apps(
        database_connection=database_connection,
        snapshot_start_date=snapshot_date,
        snapshot_end_date=end_date,
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
        snapshot_start_date=snapshot_date,
        snapshot_end_date=end_date,
        store_apps=global_df["store_app"].unique().tolist(),
        table_name=table_name,
    )
    insert_bulk(
        df=global_df,
        table_name=table_name,
        database_connection=database_connection,
    )


def process_app_metrics_to_db(
    hash_bucket: str,
    database_connection: PostgresCon,
    store: int,
    start_date_mon: datetime.date,
    end_date: datetime.date,
) -> None:
    log_info = f"date={start_date_mon} store={store} hash_bucket={hash_bucket}"
    logger.info(f"{log_info} start")
    df = get_app_hash_buckets_from_s3(
        start_date_mon=start_date_mon,
        end_date=end_date,
        store=store,
        app_hash=hash_bucket,
    )
    if df.empty:
        logger.warning(f"{log_info} no data found for S3 agg app metrics, skipping")
        raise ValueError(f"{log_info} no data found for S3 agg app metrics")
    if store == 2:
        # This is an issue and needs to be resolved in the way the iOS store_id is stored into S3
        problem_rows = df["store_id"].str.contains(".0", regex=False)
        if problem_rows.any():
            raise ValueError(
                f"Found {problem_rows.sum()} rows with .0 suffix in store_id for Apple"
            )
    df = merge_in_db_ids(df, store, database_connection)
    df = df.drop(["store_id", "country"], axis=1)
    if store == 1:
        df = df.rename(
            columns={
                "installs": "global_installs",
                "rating_count": "global_rating_count",
            }
        )
    app_birth_dates = df.groupby("store_app")["week_start"].min()
    observed_app_countries = df[["store_app", "country_id"]].drop_duplicates()
    all_weeks = pd.DataFrame({"week_start": df["week_start"].unique()})
    full_df = observed_app_countries.merge(all_weeks, how="cross")
    full_df = full_df[
        full_df["week_start"] >= full_df["store_app"].map(app_birth_dates)
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
    country_df, global_df = process_metrics(
        store=store,
        df=df,
    )
    delete_and_insert_app_metrics(
        database_connection=database_connection,
        country_df=country_df,
        global_df=global_df,
        store=store,
        snapshot_date=start_date_mon,
        end_date=end_date,
    )


# def copy_daily_agg_to_weekly(
#     store: int,
#     app_detail_parquets: list[str],
#     snapshot_date_str: str,
# ) -> str:
#     bucket = CONFIG["s3"]["bucket"]
#     if store == 2:
#         export_cols = """
#              store_id,
#              country,
#              crawled_date,
#              rating,
#              rating_count,
#              user_ratings,
#              store_last_updated,
#              crawled_at
#              """
#     elif store == 1:
#         export_cols = """
#                 store_id,
#                 country,
#                 crawled_date,
#                 installs,
#                 rating,
#                 rating_count,
#                 review_count,
#                 histogram,
#                 store_last_updated,
#                 crawled_at
#         """
#     query = f"""COPY (
#             SELECT
#                 {export_cols}
#             FROM read_parquet({app_detail_parquets}, union_by_name=true)
#             QUALIFY ROW_NUMBER() OVER (
#                 PARTITION BY store_id, country
#                 ORDER BY crawled_at DESC
#             ) = 1
#         ) TO 's3://{bucket}/{AGG_APP_COUNTRY_WEEKLY_PREFIX}/store={store}/snapshot_date={snapshot_date_str}/'
#     (FORMAT PARQUET,
#      PARTITION_BY (country),
#      OVERWRITE_OR_IGNORE true,
#      COMPRESSION 'zstd');
#     """
#     return query


# def copy_raw_details_to_daily_agg(
#     store: int,
#     app_detail_parquets: list[str],
#     snapshot_date_str: str,
# ) -> str:
#     bucket = CONFIG["s3"]["bucket"]
#     crawl_result_filter = "WHERE crawl_result = 1"
#     if snapshot_date_str < "2024-11-01":
#         crawl_result_filter = ""
#     if store == 2:
#         if snapshot_date_str < "2024-11-01":
#             store_id_col = "CAST(trackId AS VARCHAR)"
#         else:
#             store_id_col = "store_id"
#         data_cols = f"""
#              {store_id_col} AS store_id,
#              country,
#              crawled_date,
#              averageUserRating AS rating,
#              userRatingCount AS rating_count,
#              user_ratings,
#              currentVersionReleaseDate AS store_last_updated,
#              crawled_at
#         """
#         export_cols = """
#              store_id,
#              country,
#              crawled_date,
#              rating,
#              rating_count,
#              user_ratings,
#              store_last_updated,
#              crawled_at
#              """
#         extra_sort_column = "rating_count DESC"
#     elif store == 1:
#         if snapshot_date_str < "2025-11-01":
#             store_id_col = "CAST(appId AS VARCHAR)"
#         else:
#             store_id_col = "store_id"
#         data_cols = f"""
#              {store_id_col} AS store_id,
#              country,
#              crawled_date,
#              realInstalls as installs,
#              ratings as rating_count,
#              score AS rating,
#              reviews AS review_count,
#              histogram,
#              updated AS store_last_updated,
#              crawled_at
#              """
#         export_cols = """
#                 store_id,
#                 country,
#                 crawled_date,
#                 installs,
#                 rating,
#                 rating_count,
#                 review_count,
#                 histogram,
#                 store_last_updated,
#                 crawled_at
#         """
#         extra_sort_column = "review_count DESC"
#     query = f"""COPY (
#     with data  AS (
#     SELECT {data_cols}
#      FROM read_parquet({app_detail_parquets}, union_by_name=true)
#         {crawl_result_filter}
#      )
#       SELECT
#             {export_cols}
#       FROM data
#       QUALIFY ROW_NUMBER() OVER (
#         PARTITION BY store_id, country
#         ORDER BY crawled_at DESC, {extra_sort_column}
#       ) = 1
#     ) TO 's3://{bucket}/{AGG_APP_COUNTRY_DAILY_PREFIX}/store={store}/snapshot_date={snapshot_date_str}/'
#     (FORMAT PARQUET,
#     PARTITION_BY (country),
#     ROW_GROUP_SIZE 100000,
#     COMPRESSION 'zstd',
#     OVERWRITE_OR_IGNORE true);
#     """
#     return query


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
          ) TO 's3://{bucket}/{AGG_APP_HASH_BUCKETS}/store={store}/'
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
    database_connection: PostgresCon, df: pd.DataFrame
) -> pd.DataFrame:
    """Process app global metrics weekly."""
    star_cols = ["one_star", "two_star", "three_star", "four_star", "five_star"]
    tier_pct_cols = ["tier1_pct", "tier2_pct", "tier3_pct"]
    metrics = [
        "installs",
        "rating",
        "review_count",
        "rating_count",
        *star_cols,
        *tier_pct_cols,
    ]
    xaxis_col = "snapshot_date"
    # Convert to date to datetime and sort by country and date, required
    df[xaxis_col] = pd.to_datetime(df[xaxis_col])
    df = df.sort_values(["store_app", xaxis_col])
    df = (
        df.set_index(xaxis_col)
        .groupby(
            [
                "store_app",
                "store",
                "app_category",
                "ad_supported",
                "in_app_purchases",
            ]
        )[metrics]
        .resample("W-MON")
        .last()
        .apply(pd.to_numeric, errors="coerce")
        .interpolate(method="linear", limit_direction="forward")
        .fillna(0)
        .reset_index()
    )
    df["installs_diff"] = df.groupby("store_app")["installs"].diff().fillna(0)
    df["installs_diff"] = (
        df.groupby("store_app")["installs"].diff().fillna(df["installs"]).fillna(0)
    )
    df["weekly_ratings"] = (
        df.groupby("store_app")["rating_count"].diff().fillna(df["rating_count"])
    )
    df["weekly_reviews"] = (
        df.groupby("store_app")["review_count"].diff().fillna(df["review_count"])
    )
    # This drops the earliest week, since the diff will count all metrics as weekly
    # This is ok for apps which are new to the range, that week all those metrics are new
    drop_rows = df["snapshot_date"] == df["snapshot_date"].min()
    df = df[~drop_rows]
    # iOS data is very estimated and noisy, so we smooth
    mask = df["store"] == 2
    df.loc[mask, "installs_diff"] = (
        df[mask]
        .groupby("store_app")["installs_diff"]
        .rolling(window=3, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )
    retention_benchmarks = get_retention_benchmarks(database_connection)
    cohorts = df.merge(
        retention_benchmarks, on=["app_category", "store"], how="left", validate="m:1"
    )
    cohorts["k"] = np.log(
        cohorts["d30"].replace(0, np.nan) / cohorts["d7"].replace(0, np.nan)
    ) / np.log(30.0 / 7.0)
    cohorts = cohorts[["store_app", "snapshot_date", "installs_diff", "d1", "d7", "k"]]
    logger.info("Historical merge, memory intensive step")
    cohorts = cohorts.merge(
        cohorts[["store_app", "snapshot_date", "installs_diff"]],
        on="store_app",
        suffixes=("", "_historical"),
    )
    cohorts = cohorts[cohorts["snapshot_date"] >= cohorts["snapshot_date_historical"]]
    cohorts["weeks_passed"] = (
        (cohorts["snapshot_date"] - cohorts["snapshot_date_historical"]).dt.days / 7
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
        cohorts.groupby(["store_app", "snapshot_date"])[
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
        "snapshot_date",
        "installs_diff",
        "weekly_ratings",
        "weekly_reviews",
    ] + metrics
    df = pd.merge(
        df[dfcols],
        cohorts,
        on=["store_app", "snapshot_date"],
        how="left",
        validate="1:1",
    )
    df[["snapshot_date", "installs", "installs_diff", "wau"]]
    rename_map = {
        "snapshot_date": "week_start",
        "installs_diff": "weekly_installs",
        "wau": "weekly_active_users",
        "mau": "monthly_active_users",
        "installs": "total_installs",
        "rating_count": "total_ratings",
    }
    df = df.rename(columns=rename_map)
    return df


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


def app_global_metrics_derive_latest_weekly(
    database_connection: PostgresCon,
    logged_last_at: str,
    store_app_ids: list[int] | None,
) -> None:
    i = 0
    while True:
        log_info = f"Global weekly derived metrics batch={i}"
        logger.info(f"{log_info} start")
        query_apps = None
        if store_app_ids is not None:
            query_apps = store_app_ids[i * 10000 : (i + 1) * 10000]
            if len(query_apps) == 0:
                break
        df = query_apps_to_process_global_metrics(
            database_connection,
            batch_size=10000,
            start_datetime=logged_last_at,
            store_app_ids=query_apps,
        )
        if df.empty:
            break
        log_apps = df["store_app"].unique().tolist()
        # Fill in blank rows with default
        df["tier_pct_sum"] = df["tier1_pct"] + df["tier2_pct"] + df["tier3_pct"]
        incomplete_tier_pct = df["tier_pct_sum"] < 0.5
        # Because for US is default, many apps end up with 1.0 t1
        all_t1 = df["tier1_pct"] == 1
        tiers_to_fill = incomplete_tier_pct | all_t1
        df.loc[tiers_to_fill, "tier1_pct"] = 0.34
        df.loc[tiers_to_fill, "tier2_pct"] = 0.33
        df.loc[tiers_to_fill, "tier3_pct"] = 0.33
        logger.info(f"{log_info} start calculating WAU")
        df = calculate_active_users(database_connection, df)
        logger.info(f"{log_info} finished calculating WAU")
        logger.info(f"{log_info} start calculating revenue columns")
        df = calculate_revenue_cols(database_connection, df)
        logger.info(f"{log_info} finished calculating revenue columns")
        final_cols = [
            "store_app",
            "week_start",
            "weekly_installs",
            "weekly_ratings",
            "weekly_reviews",
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
        df = df[final_cols]
        logger.info(f"{log_info} insert rows={df.shape[0]}")
        delete_and_insert(
            df=df,
            schema="public",
            table_name="app_global_metrics_weekly",
            database_connection=database_connection,
            delete_by_keys=["store_app", "week_start"],
            insert_columns=df.columns.tolist(),
        )
        logger.info(f"{log_info} finished inserting rows")
        logger.info(f"{log_info} logging batch...")
        log_df = pd.DataFrame({"store_app": log_apps})
        log_df["updated_at"] = datetime.datetime.now()
        log_df.to_sql(
            name="app_global_metrics_weekly",
            con=database_connection.engine,
            schema="logging",
            if_exists="append",
            index=False,
        )
        i += 1
