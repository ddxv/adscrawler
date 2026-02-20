import datetime
import time

import numpy as np
import pandas as pd

from adscrawler.app_stores.utils import (
    get_parquet_paths_by_prefix,
)
from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import (
    PostgresCon,
    get_db_connection,
)
from adscrawler.dbcon.queries import (
    get_ecpm_benchmarks,
    get_latest_app_country_history,
    get_retention_benchmarks,
    query_apps_to_process_global_metrics,
    query_countries,
    query_store_id_map_cached,
    upsert_bulk,
    upsert_df,
)
from adscrawler.packages.storage import (
    delete_s3_objects_by_prefix,
    get_duckdb_connection,
)

logger = get_logger(__name__, "scrape_stores")

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


def get_s3_agg_app_snapshots_parquet_paths(
    bucket: str,
    start_date: pd.DatetimeIndex,
    end_date: pd.DatetimeIndex,
    store: int,
    freq: str,
) -> list[str]:
    all_parquet_paths = []
    for ddt in pd.date_range(start_date, end_date, freq=freq):
        ddt_str = ddt.strftime("%Y-%m-%d")
        prefix = f"agg-data/app_country_metrics/store={store}/snapshot_date={ddt_str}/country="
        all_parquet_paths += get_parquet_paths_by_prefix(bucket, prefix)
    return all_parquet_paths


def get_s3_agg_daily_snapshots(
    start_date: datetime.date, end_date: datetime.date, store: int
) -> pd.DataFrame:
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    parquet_paths = get_s3_agg_app_snapshots_parquet_paths(
        bucket=bucket,
        start_date=start_date,
        end_date=end_date,
        store=store,
        freq="D",
    )
    if len(parquet_paths) == 0:
        logger.warning(
            f"No parquet paths found for agg app snapshots {store=} {start_date=} {end_date=}"
        )
        return pd.DataFrame()
    query = f"""SELECT
       * 
      FROM read_parquet({parquet_paths})
      WHERE store_id IS NOT NULL
    """
    duckdb_con = get_duckdb_connection(s3_config_key)
    df = duckdb_con.execute(query).df()
    duckdb_con.close()
    na_rows = df["store_id"].isna().sum()
    if na_rows > 0:
        logger.warning("Missing store_id values found")
    return df


def check_for_duplicates(df: pd.DataFrame, key_columns: list[str]) -> None:
    any_duplicates = df.duplicated(subset=key_columns).any()
    if any_duplicates:
        dups = df[df.duplicated(subset=key_columns)][
            key_columns + ["store_id", "crawled_at"]
        ]
        logger.error(f"Duplicates found in app history data! {dups}")
        raise ValueError("Duplicates found in app history data")
    return


def make_s3_app_country_metrics_history(
    store: int, snapshot_date: pd.DatetimeIndex
) -> None:
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")
    prefix = (
        f"raw-data/app_details/store={store}/crawled_date={snapshot_date_str}/country="
    )
    app_detail_parquets = get_parquet_paths_by_prefix(bucket, prefix)
    agg_prefix = (
        f"agg-data/app_country_metrics/store={store}/snapshot_date={snapshot_date_str}/"
    )
    delete_s3_objects_by_prefix(bucket, prefix=agg_prefix)
    if len(app_detail_parquets) == 0:
        logger.error(
            f"No app detail parquet files found for store={store} snapshot_date={snapshot_date_str}"
        )
        return
    query = app_details_country_history_query(
        store=store,
        app_detail_parquets=app_detail_parquets,
        snapshot_date_str=snapshot_date_str,
    )
    duckdb_con = get_duckdb_connection(s3_config_key)
    duckdb_con.execute(query)
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


def prep_app_google_metrics(
    df: pd.DataFrame,
    app_country_db_latest: pd.DataFrame,
) -> pd.DataFrame:
    df["store_last_updated"] = np.where(
        (df["store_last_updated"].isna() | df["store_last_updated"] < 0),
        None,
        df["store_last_updated"],
    )
    df.loc[df["store_last_updated"].notna(), "store_last_updated"] = pd.to_datetime(
        df.loc[df["store_last_updated"].notna(), "store_last_updated"],
        unit="s",
    )
    df = df.rename(
        columns={
            "installs": "global_installs",
            "rating_count": "global_rating_count",
        }
    )
    # If data is being rerun, prefer 'newer' data from S3
    df["crawled_date"] = df["crawled_date"] + pd.Timedelta(seconds=1)
    cdf = pd.concat(
        [
            df,
            app_country_db_latest[
                app_country_db_latest["store_app"].isin(df["store_app"].unique())
            ],
        ],
        axis=0,
    )
    cdf = cdf.sort_values(by=["crawled_date"], ascending=True).drop_duplicates(
        subset=["store_app", "country_id"], keep="last"
    )
    cdf["review_count"] = pd.to_numeric(cdf["review_count"], errors="coerce").fillna(0)
    cdf["global_installs"] = pd.to_numeric(
        cdf["global_installs"], errors="coerce"
    ).fillna(0)
    cdf["global_rating_count"] = pd.to_numeric(
        cdf["global_rating_count"], errors="coerce"
    ).fillna(0)
    cdf["max_reviews"] = cdf.groupby("store_app")["review_count"].transform("max")
    cdf["is_max_candidate"] = (cdf["review_count"] >= cdf["max_reviews"] * 0.99) & (
        cdf["max_reviews"] > 200
    )
    candidate_counts = cdf.groupby("store_app")["is_max_candidate"].transform("sum")
    cdf["is_global_fallback"] = cdf["is_max_candidate"] & (candidate_counts > 1)
    local_app_sums = (
        cdf[~cdf["is_global_fallback"]].groupby("store_app")["review_count"].sum()
    )
    cdf["global_review_count"] = np.where(
        candidate_counts > 1,
        cdf["max_reviews"],
        cdf["store_app"].map(local_app_sums),
    )
    # Remove review_count where is_global_fallback is VERY tempting, but deletes data
    # cdf["review_count"] = np.where(
    #     cdf["is_global_fallback"], 0, cdf["review_count"]
    # )
    cdf["pct_of_global"] = (
        (
            np.where(cdf["is_global_fallback"], np.nan, cdf["review_count"])
            / cdf["global_review_count"]
        )
        .replace(0, np.nan)
        .fillna(0)
    )
    cdf["installs_est"] = (
        (cdf["global_installs"] * cdf["pct_of_global"]).round().astype(int)
    )
    cdf["rating_count_est"] = (
        (cdf["global_rating_count"] * cdf["pct_of_global"]).round().astype(int)
    )
    country_df = pd.merge(
        df,
        cdf[
            [
                "store_app",
                "country_id",
                "installs_est",
                "rating_count_est",
                "global_review_count",
            ]
        ],
        on=["store_app", "country_id"],
        how="left",
        validate="1:1",
    )
    country_df["review_count"] = (
        country_df["review_count"].replace(np.nan, 0).astype(int)
    )
    country_df["global_rating_count"] = (
        country_df["global_rating_count"].fillna(0).astype(int)
    )
    country_df["global_review_count"] = (
        country_df["global_review_count"].fillna(0).astype(int)
    )
    country_df["global_installs"] = country_df["global_installs"].fillna(0).astype(int)
    # Db currently does not have a rating_count_est column just for country
    country_df["rating_count"] = country_df["rating_count_est"]
    assert country_df["global_rating_count"].ge(country_df["rating_count"]).all(), (
        "global_rating_count should be >= rating_count"
    )
    global_df = country_df[country_df["country"] == "US"].copy()
    global_df = global_df.drop(columns=["rating_count", "review_count"]).rename(
        columns={
            "global_installs": "installs",
            "global_rating_count": "rating_count",
            "global_review_count": "review_count",
        }
    )
    global_df = global_df.set_index(GLOBAL_HISTORY_KEYS)
    # TODO: Stars need to be fixed per is_global_fallback logic
    star_df = pd.DataFrame(global_df["histogram"].tolist(), index=global_df.index)
    try:
        star_df.columns = STAR_COLS
    except ValueError:
        for col in STAR_COLS:
            if col not in star_df.columns:
                star_df[col] = 0
    global_df = pd.concat([global_df, star_df], axis=1)
    tier_pct = cdf.pivot_table(
        index=["store_app"],
        columns="tier",
        values="pct_of_global",
        aggfunc="sum",
        fill_value=0,
        dropna=False,
    )
    global_df = global_df.join(tier_pct)
    global_df = global_df.reset_index()
    return country_df, global_df


def prep_app_apple_metrics(
    df: pd.DataFrame,
    app_country_db_latest: pd.DataFrame,
) -> pd.DataFrame:
    ratings_str = (
        df["user_ratings"]
        .str.extractall(r"(\d+)")
        .unstack()
        .astype("Int64")
        .reindex(df.index, fill_value=0)
    )
    df[STAR_COLS] = ratings_str.iloc[:, 1::2].astype(int).to_numpy()
    df["store_last_updated"] = pd.to_datetime(
        df["store_last_updated"], format="ISO8601", utc=True
    )
    df = estimate_ios_installs(df)
    cdf = pd.concat(
        [
            app_country_db_latest[
                app_country_db_latest["store_app"].isin(df["store_app"].unique())
            ],
            df,
        ],
        axis=0,
    )
    # overwrites installs_est from db, but quick fix for missing, same value
    cdf = estimate_ios_installs(cdf)
    cdf = cdf.sort_values(by=["crawled_date"], ascending=True).drop_duplicates(
        subset=["store_app", "country_id"], keep="last"
    )
    cdf["rating_prod"] = cdf["rating"] * cdf["rating_count"]
    global_df = cdf.groupby(["store_app"]).agg(
        rating_count=("rating_count", "sum"),
        rating_prod=("rating_prod", "sum"),
        installs=("installs_est", "sum"),
        store_last_updated=("store_last_updated", "max"),
        **{col: (col, "sum") for col in STAR_COLS},
    )
    global_df["snapshot_date"] = df["snapshot_date"].iloc[0]
    global_df["rating"] = (
        global_df["rating_prod"] / global_df["rating_count"].replace(0, np.nan)
    ).astype("float64")
    tier_installs = cdf.pivot_table(
        index=["store_app"],
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
    # Note, we return the df which for iOS is the country level data
    return df, global_df


def manual_import_app_metrics_from_s3(
    start_date: datetime.date, end_date: datetime.date
) -> None:
    use_tunnel = False
    rerun_s3_agg = True
    database_connection = get_db_connection(
        use_ssh_tunnel=use_tunnel, config_key="madrone"
    )
    start_date = datetime.datetime.fromisoformat("2025-11-22").date()
    end_date = datetime.datetime.today() - datetime.timedelta(days=1)
    for store in [2]:
        last_history_df = pd.DataFrame()
        for snapshot_date in pd.date_range(start_date, end_date, freq="D"):
            # snapshot_date = datetime.datetime.fromisoformat(snapshot_date).date()
            snapshot_date = snapshot_date.date()
            if rerun_s3_agg:
                make_s3_app_country_metrics_history(store, snapshot_date=snapshot_date)
            last_history_df = process_app_metrics_to_db(
                database_connection, store, snapshot_date, last_history_df
            )


def import_app_metrics_from_s3(
    start_date: datetime.date, end_date: datetime.date, database_connection: PostgresCon
) -> None:
    for store in [1, 2]:
        last_history_df = pd.DataFrame()
        for snapshot_date in pd.date_range(start_date, end_date, freq="D"):
            snapshot_date = snapshot_date.date()
            make_s3_app_country_metrics_history(store, snapshot_date=snapshot_date)
            try:
                last_history_df = process_app_metrics_to_db(
                    database_connection, store, snapshot_date, last_history_df
                )
            except Exception as e:
                logger.error(
                    f"Error processing S3 app metrics for {snapshot_date} {store}: {e}"
                )
    import_all_app_global_metrics_weekly(database_connection)


def process_store_metrics(
    store: int,
    app_country_db_latest: pd.DataFrame,
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    log_info = f"process metrics for {df.shape[0]} rows for store={store}"
    logger.info(f"{log_info} start")
    if store == 1:
        country_df, global_df = prep_app_google_metrics(
            df=df,
            app_country_db_latest=app_country_db_latest,
        )
    elif store == 2:
        country_df, global_df = prep_app_apple_metrics(
            df=df,
            app_country_db_latest=app_country_db_latest,
        )
    country_df = country_df.convert_dtypes(dtype_backend="pyarrow")
    country_df = country_df.replace({pd.NA: None})
    check_for_duplicates(
        df=country_df,
        key_columns=COUNTRY_HISTORY_KEYS,
    )
    for col in ["tier1_pct", "tier2_pct", "tier3_pct"]:
        if col not in global_df.columns:
            global_df[col] = 0.0
        global_df[col] = (global_df[col] * 10000).round().astype("int16")
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


def process_app_metrics_to_db(
    database_connection: PostgresCon,
    store: int,
    snapshot_date: datetime.date,
    last_history_df: pd.DataFrame,
) -> pd.DataFrame:
    log_info = f"date={snapshot_date} store={store}"
    logger.info(f"{log_info} start")
    df = get_s3_agg_daily_snapshots(snapshot_date, snapshot_date, store)
    if df.empty:
        logger.warning(
            f"No data found for S3 agg app metrics {store=} {snapshot_date=}"
        )
        return last_history_df
    if store == 2:
        # This is an issue and needs to be resolved in the way the iOS store_id is stored into S3
        problem_rows = df["store_id"].str.contains(".0")
        if problem_rows.any():
            logger.warning(
                f'Apple App IDs: Found {problem_rows.sum()}/{df.shape[0]} store_id with ".0" suffix, fixing'
            )
            df.loc[problem_rows, "store_id"] = (
                df.loc[problem_rows, "store_id"].str.split(".").str[0]
            )
            df["crawled_at"] = df["crawled_at"].sort_values(ascending=True)
            df = df.drop_duplicates(
                ["snapshot_date", "country", "store_id"], keep="last"
            )
    df = merge_in_db_ids(df, store, database_connection)
    if last_history_df.empty:
        days_back = 180
    else:
        days_back = 1
    app_country_db_latest = get_latest_app_country_history(
        database_connection,
        snapshot_date=snapshot_date,
        days_back=days_back,
        chunk_size=50000,
        store_app_ids=df["store_app"].unique(),
        store=store,
    )
    if days_back == 1:
        app_country_db_latest = pd.concat(
            [last_history_df, app_country_db_latest], ignore_index=True
        ).drop_duplicates(subset=["store_app", "country_id"], keep="last")
        start_date = snapshot_date - datetime.timedelta(days=180)
        app_country_db_latest = app_country_db_latest[
            app_country_db_latest["crawled_date"] > pd.to_datetime(start_date)
        ]
    country_df, global_df = process_store_metrics(
        store=store, app_country_db_latest=app_country_db_latest, df=df
    )
    start_time = time.time()
    upsert_bulk(
        df=country_df,
        table_name="app_country_metrics_history",
        database_connection=database_connection,
        key_columns=COUNTRY_HISTORY_KEYS,
    )
    logger.info(f"Upserted country_df in {time.time() - start_time:.2f} seconds")
    start_time = time.time()
    upsert_bulk(
        df=global_df,
        table_name="app_global_metrics_history",
        database_connection=database_connection,
        key_columns=GLOBAL_HISTORY_KEYS,
    )
    logger.info(f"Upserted global_df in {time.time() - start_time:.2f} seconds")
    return app_country_db_latest


def app_details_country_history_query(
    store: int,
    app_detail_parquets: list[str],
    snapshot_date_str: str,
) -> str:
    bucket = CONFIG["s3"]["bucket"]
    if store == 2:
        data_cols = """
             CAST(trackId AS VARCHAR) AS store_id,
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
        data_cols = """
             appId AS store_id,
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
    with data  AS (
    SELECT {data_cols}
     FROM read_parquet({app_detail_parquets}, union_by_name=true)
     )
      SELECT
            {export_cols}
      FROM data
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY store_id, country
        ORDER BY crawled_at DESC, {extra_sort_column}
      ) = 1
    ) TO 's3://{bucket}/agg-data/app_country_metrics/store={store}/snapshot_date={snapshot_date_str}/'
    (FORMAT PARQUET, 
    PARTITION_BY (country), 
    ROW_GROUP_SIZE 100000, 
    COMPRESSION 'zstd',
    OVERWRITE_OR_IGNORE true);
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
    retention_benchmarks = get_retention_benchmarks(database_connection)
    merged_df = df.merge(
        retention_benchmarks, on=["app_category", "store"], how="left", validate="m:1"
    )
    merged_df["k"] = np.log(
        merged_df["d30"].replace(0, np.nan) / merged_df["d7"].replace(0, np.nan)
    ) / np.log(30.0 / 7.0)
    cohorts = merged_df[
        ["store_app", "snapshot_date", "installs_diff", "d1", "d7", "k"]
    ].copy()
    logger.info("Historical merge, memory intensive step")
    ddf = cohorts.merge(
        cohorts[["store_app", "snapshot_date", "installs_diff"]],
        on="store_app",
        suffixes=("", "_historical"),
    )
    ddf = ddf[ddf["snapshot_date"] >= ddf["snapshot_date_historical"]]
    ddf["weeks_passed"] = (
        (ddf["snapshot_date"] - ddf["snapshot_date_historical"]).dt.days / 7
    ).astype(int)
    wau_mult = 2.0
    ddf["retention_rate"] = np.where(
        ddf["weeks_passed"] == 0,
        1.0,
        (ddf["d7"] * wau_mult * (ddf["weeks_passed"].replace(0, 1) ** ddf["k"])).clip(
            upper=1.0
        ),
    )
    ddf["surviving_users"] = ddf["installs_diff_historical"] * ddf["retention_rate"]
    mau_mult = 3.5  # Standard estimate for Monthly Reach
    # Calculate MAU Retention Rate
    ddf["retention_rate_mau"] = np.where(
        ddf["weeks_passed"] == 0,
        1.0,
        (ddf["d7"] * mau_mult * (ddf["weeks_passed"].replace(0, 1) ** ddf["k"])).clip(
            upper=1.0
        ),
    )
    ddf["surviving_mau"] = ddf["installs_diff_historical"] * ddf["retention_rate_mau"]
    ddf = (
        ddf.groupby(["store_app", "snapshot_date"])[
            ["surviving_users", "surviving_mau"]
        ]
        .sum()
        .reset_index()
    )
    ddf.rename(columns={"surviving_users": "wau", "surviving_mau": "mau"}, inplace=True)
    cols = [
        "store_app",
        "in_app_purchases",
        "ad_supported",
        "snapshot_date",
        "installs_diff",
    ] + metrics
    df = pd.merge(
        df[cols], ddf, on=["store_app", "snapshot_date"], how="left", validate="1:1"
    )
    logger.info("Finished calculating WAU")
    df["weekly_ratings"] = (
        df.groupby("store_app")["rating_count"].diff().fillna(df["rating_count"])
    )
    df["weekly_reviews"] = (
        df.groupby("store_app")["review_count"].diff().fillna(df["review_count"])
    )
    rename_map = {
        "snapshot_date": "week_start",
        "installs_diff": "weekly_installs",
        "wau": "weekly_active_users",
        "mau": "monthly_active_users",
        "installs": "total_installs",
        "rating_count": "total_ratings_count",
    }
    df = df.rename(columns=rename_map)
    return df


def calculate_revenue_cols(
    database_connection: PostgresCon, df: pd.DataFrame
) -> pd.DataFrame:
    ecpm_benchmarks = get_ecpm_benchmarks(database_connection)
    # Fill in blank rows with default
    df["tier_pct_sum"] = df["tier1_pct"] + df["tier2_pct"] + df["tier3_pct"]
    df.loc[df["tier_pct_sum"] < 0.5, "tier1_pct"] = 0.33
    df.loc[df["tier_pct_sum"] < 0.5, "tier2_pct"] = 0.33
    df.loc[df["tier_pct_sum"] < 0.5, "tier3_pct"] = 0.34
    df["wau_tier1"] = df["weekly_active_users"] * df["tier1_pct"]
    df["wau_tier2"] = df["weekly_active_users"] * df["tier2_pct"]
    df["wau_tier3"] = df["weekly_active_users"] * df["tier3_pct"]
    df["mau_tier1"] = df["monthly_active_users"] * df["tier1_pct"]
    df["mau_tier2"] = df["monthly_active_users"] * df["tier2_pct"]
    df["mau_tier3"] = df["monthly_active_users"] * df["tier3_pct"]
    # Placeholder ARPU. Replace with Paying User rate or ARPU benchmarks by tier if available
    df["weekly_iap_revenue"] = df.apply(
        lambda x: x["weekly_active_users"] * 0.05 if x["in_app_purchases"] else 0.0,
        axis=1,
    )
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


def import_all_app_global_metrics_weekly(database_connection: PostgresCon) -> None:
    i = 0
    while True:
        logger.info(f"batch {i} of app global metrics weekly start")
        df = query_apps_to_process_global_metrics(database_connection, batch_size=5000)
        if df.empty:
            break
        log_apps = df["store_app"].tolist()
        df = calculate_active_users(database_connection, df)
        df = calculate_revenue_cols(database_connection, df)
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
            "total_ratings_count",
            "rating",
            "one_star",
            "two_star",
            "three_star",
            "four_star",
            "five_star",
        ]
        df = df[final_cols]

        upsert_df(
            df=df,
            table_name="app_global_metrics_weekly",
            database_connection=database_connection,
            key_columns=["store_app", "week_start"],
            insert_columns=df.columns.tolist(),
        )
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
