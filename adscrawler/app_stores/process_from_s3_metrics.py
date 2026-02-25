import datetime
import time

import duckdb
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
    delete_and_insert,
    delete_app_metrics_by_date_and_store,
    get_ecpm_benchmarks,
    get_latest_app_country_history,
    get_retention_benchmarks,
    insert_bulk,
    query_apps_to_process_global_metrics,
    query_countries,
    query_store_id_map_cached,
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

TABLE_KEYS = {
    "app_country_metrics_history": COUNTRY_HISTORY_KEYS,
    "app_global_metrics_history": GLOBAL_HISTORY_KEYS,
}

APP_COUNTRY_WEEKLY_PREFIX = "agg-data/app_country_metrics/weekly"
APP_COUNTRY_DAILY_PREFIX = "agg-data/app_country_metrics/daily"
RAW_DATA_PREFIX = "raw-data/app_details"


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
        prefix = f"{APP_COUNTRY_WEEKLY_PREFIX}/store={store}/snapshot_date={ddt_str}/country="
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


def make_s3_app_country_metrics_history_daily(
    store: int, snapshot_date: pd.DatetimeIndex
) -> None:
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")
    prefix = (
        f"{RAW_DATA_PREFIX}/store={store}/crawled_date={snapshot_date_str}/country="
    )
    all_parquet_paths = get_parquet_paths_by_prefix(bucket, prefix)
    agg_prefix = (
        f"{APP_COUNTRY_DAILY_PREFIX}/store={store}/snapshot_date={snapshot_date_str}/"
    )
    time.sleep(1)
    delete_s3_objects_by_prefix(
        bucket=bucket, prefix=agg_prefix, key_name=s3_config_key
    )
    time.sleep(1)
    if len(all_parquet_paths) == 0:
        logger.error(
            f"No app detail parquet files found for store={store} snapshot_date={snapshot_date_str}"
        )
        return
    query = copy_raw_details_to_daily_agg(
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
    except duckdb.BinderException as e:
        if store == 1 and """"appId" not found""" in str(e):
            logger.error(
                f"appId column not found in parquets for store={store}, skipping"
            )
            handle_missing_trackid_files(duckdb_con, all_parquet_paths, store)
    duckdb_con.close()


def make_s3_app_country_metrics_history_week(
    store: int, snapshot_date: pd.DatetimeIndex, snapshot_end_date: pd.DatetimeIndex
) -> None:
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")
    all_parquet_paths = []
    for ddt in pd.date_range(snapshot_date, snapshot_end_date, freq="D"):
        ddt_str = ddt.strftime("%Y-%m-%d")
        prefix = (
            f"{APP_COUNTRY_DAILY_PREFIX}/store={store}/snapshot_date={ddt_str}/country="
        )
        all_parquet_paths += get_parquet_paths_by_prefix(bucket, prefix)
    agg_prefix = (
        f"{APP_COUNTRY_WEEKLY_PREFIX}/store={store}/snapshot_date={snapshot_date_str}/"
    )
    delete_s3_objects_by_prefix(
        bucket=bucket, prefix=agg_prefix, key_name=s3_config_key
    )
    if len(all_parquet_paths) == 0:
        logger.error(
            f"No app detail parquet files found for store={store} snapshot_date={snapshot_date_str}"
        )
        return
    query = copy_daily_agg_to_weekly(
        store=store,
        app_detail_parquets=all_parquet_paths,
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
    cdf = pd.concat(
        [
            app_country_db_latest[
                app_country_db_latest["store_app"].isin(df["store_app"].unique())
            ],
            df,
        ],
        axis=0,
        ignore_index=True,
    ).drop_duplicates(subset=["store_app", "country_id"], keep="last")
    cdf["review_count"] = pd.to_numeric(cdf["review_count"], errors="coerce").fillna(0)
    cdf["global_installs"] = pd.to_numeric(
        cdf["global_installs"], errors="coerce"
    ).fillna(0)
    cdf["global_rating_count"] = pd.to_numeric(
        cdf["global_rating_count"], errors="coerce"
    ).fillna(0)
    cdf["max_reviews"] = cdf.groupby("store_app")["review_count"].transform("max")
    cdf["global_installs"] = cdf.groupby("store_app")["global_installs"].transform(
        "max"
    )
    cdf["global_rating_count"] = cdf.groupby("store_app")[
        "global_rating_count"
    ].transform("max")
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
        df.drop(columns=["global_installs", "global_rating_count"]),
        cdf[
            [
                "store_app",
                "country_id",
                "installs_est",
                "rating_count_est",
                "global_review_count",
                "global_installs",
                "global_rating_count",
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
    ).rename(columns={"tier1": "tier1_pct", "tier2": "tier2_pct", "tier3": "tier3_pct"})
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
        ignore_index=True,
        axis=0,
    ).drop_duplicates(subset=["store_app", "country_id"], keep="last")
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
    database_connection = get_db_connection(
        use_ssh_tunnel=use_tunnel, config_key="madrone"
    )
    stores = [1, 2]
    start_date = datetime.datetime.fromisoformat("2023-02-14").date()
    # start_date = datetime.datetime.fromisoformat("2026-01-05").date()
    end_date = datetime.datetime.fromisoformat("2026-02-23").date()
    for store in stores:
        last_history_df = pd.DataFrame()
        for snapshot_date in pd.date_range(start_date, end_date, freq="D"):
            logger.info(f"Raw to daily agg {store=} {snapshot_date.date()} start")
            make_s3_app_country_metrics_history_daily(
                store,
                snapshot_date=snapshot_date,
            )
            time.sleep(1)

    # end_date = datetime.datetime.today() - datetime.timedelta(days=1)
    for store in stores:
        last_history_df = pd.DataFrame()
        for snapshot_date in pd.date_range(start_date, end_date, freq="W-MON"):
            logger.info(
                f"Processing weekly agg for {store=} {snapshot_date.date()} start"
            )
            # snapshot_date = datetime.datetime.fromisoformat(snapshot_date).date()
            snapshot_date = snapshot_date.date()  # Monday
            snapshot_end_date = snapshot_date + datetime.timedelta(days=6)  # Sunday
            make_s3_app_country_metrics_history_week(
                store,
                snapshot_date=snapshot_date,
                snapshot_end_date=snapshot_end_date,
            )

    start_date = datetime.datetime.fromisoformat("2026-02-02").date()
    end_date = datetime.datetime.fromisoformat("2026-02-24").date()
    stores = [1, 2]
    use_tunnel = False
    database_connection = get_db_connection(
        use_ssh_tunnel=use_tunnel, config_key="madrone"
    )
    for store in stores:
        last_history_df = pd.DataFrame()
        for snapshot_date in pd.date_range(start_date, end_date, freq="W-MON"):
            logger.info(
                f"Processing weekly agg for {store=} {snapshot_date.date()} start"
            )
            # snapshot_date = datetime.datetime.fromisoformat(snapshot_date).date()
            snapshot_date = snapshot_date.date()  # Monday
            snapshot_end_date = snapshot_date + datetime.timedelta(days=6)  # Sunday
            last_history_df = process_app_metrics_to_db(
                database_connection,
                store,
                snapshot_date,
                snapshot_end_date,
                last_history_df,
            )


def import_app_metrics_from_s3(
    start_date: datetime.date, end_date: datetime.date
) -> None:
    use_tunnel = False
    database_connection = get_db_connection(
        use_ssh_tunnel=use_tunnel, config_key="madrone"
    )
    stores = [1, 2]
    # Raw data to agg by DAY
    for store in stores:
        last_history_df = pd.DataFrame()
        for snapshot_date in pd.date_range(start_date, end_date, freq="D"):
            log_info = f"Raw app metrics for {store=} {snapshot_date.date()}"
            logger.info(f"{log_info} start")
            make_s3_app_country_metrics_history_daily(
                store,
                snapshot_date=snapshot_date,
            )
            time.sleep(1)

    # Agg data by WEEK
    for store in stores:
        last_history_df = pd.DataFrame()
        for snapshot_date in pd.date_range(start_date, end_date, freq="W-MON"):
            log_info = f"App metrics for {store=} {snapshot_date.date()}"
            logger.info(f"{log_info} start")
            # snapshot_date = datetime.datetime.fromisoformat(snapshot_date).date()
            snapshot_date = snapshot_date.date()  # Monday
            snapshot_end_date = snapshot_date + datetime.timedelta(days=6)  # Sunday
            make_s3_app_country_metrics_history_week(
                store,
                snapshot_date=snapshot_date,
                snapshot_end_date=snapshot_end_date,
            )
            logger.info(f"{log_info} import agg to db")
            last_history_df = process_app_metrics_to_db(
                database_connection,
                store,
                snapshot_date,
                snapshot_end_date,
                last_history_df,
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


def delete_and_insert_app_metrics(
    database_connection: PostgresCon,
    country_df: pd.DataFrame,
    global_df: pd.DataFrame,
    table_name: str,
    store: int,
    snapshot_date: datetime.date,
    snapshot_end_date: datetime.date,
) -> None:
    start_time = time.time()
    table_name = "app_country_metrics_history"
    delete_app_metrics_by_date_and_store(
        database_connection=database_connection,
        snapshot_start_date=snapshot_date,
        snapshot_end_date=snapshot_end_date,
        store=store,
        table_name=table_name,
    )
    insert_bulk(
        df=country_df,
        table_name=table_name,
        database_connection=database_connection,
    )
    # upsert_bulk(df=country_df, table_name=table_name, database_connection=database_connection, key_columns=TABLE_KEYS[table_name])
    mytime: float = time.time() - start_time
    logger.info(f"{table_name} insert {country_df.shape[0]} rows in {mytime:.2f}s")
    start_time = time.time()
    table_name = "app_global_metrics_history"
    delete_app_metrics_by_date_and_store(
        database_connection=database_connection,
        snapshot_start_date=snapshot_date,
        snapshot_end_date=snapshot_end_date,
        store=store,
        table_name=table_name,
    )
    # upsert_bulk(df=global_df, table_name=table_name, database_connection=database_connection, key_columns=TABLE_KEYS[table_name])
    insert_bulk(
        df=global_df,
        table_name=table_name,
        database_connection=database_connection,
    )
    mytime: float = time.time() - start_time
    logger.info(f"{table_name} insert {global_df.shape[0]} rows in {mytime:.2f}s")


def process_app_metrics_to_db(
    database_connection: PostgresCon,
    store: int,
    snapshot_date: datetime.date,
    snapshot_end_date: datetime.date,
    last_history_df: pd.DataFrame,
) -> pd.DataFrame:
    log_info = f"date={snapshot_date} store={store}"
    logger.info(f"{log_info} start")
    # df = get_s3_agg_daily_snapshots(snapshot_date, snapshot_end_date, store)
    df = get_s3_agg_daily_snapshots(snapshot_date, snapshot_end_date, store)
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
    max_days_back = 180
    rolling_days = 7
    current_ids = df["store_app"].unique()
    if last_history_df.empty:
        missing_app_ids = current_ids
        current_existing_ids = []
    else:
        existing_ids = last_history_df["store_app"].unique()
        missing_app_ids = np.setdiff1d(current_ids, existing_ids)
        current_existing_ids = np.setdiff1d(current_ids, missing_app_ids)
    # Fetch recent only for apps that we already pulled 180 days
    if len(current_existing_ids) > 0:
        recent_history = get_latest_app_country_history(
            database_connection,
            snapshot_date=snapshot_date,
            days_back=rolling_days,
            chunk_size=25000,
            store_app_ids=current_existing_ids,
            store=store,
        )
    else:
        recent_history = pd.DataFrame()
    # Deep fetch only for new apps
    if len(missing_app_ids) > 0:
        new_apps_history = get_latest_app_country_history(
            database_connection,
            snapshot_date=snapshot_date,
            days_back=max_days_back,
            chunk_size=25000,
            store_app_ids=missing_app_ids,
            store=store,
        )
    else:
        new_apps_history = pd.DataFrame()
    app_country_db_latest = pd.concat(
        [last_history_df, new_apps_history, recent_history],
        ignore_index=True,
    )
    app_country_db_latest = app_country_db_latest.sort_values(
        ["store_app", "country_id", "crawled_date"]
    )
    app_country_db_latest = app_country_db_latest.drop_duplicates(
        subset=["store_app", "country_id"],
        keep="last",
    )
    start_date = snapshot_date - datetime.timedelta(days=max_days_back)
    app_country_db_latest = app_country_db_latest[
        app_country_db_latest["crawled_date"] > pd.to_datetime(start_date)
    ]
    country_df, global_df = process_store_metrics(
        store=store, app_country_db_latest=app_country_db_latest, df=df
    )
    delete_and_insert_app_metrics(
        database_connection=database_connection,
        country_df=country_df,
        global_df=global_df,
        table_name="app_country_metrics_history",
        store=store,
        snapshot_date=snapshot_date,
        snapshot_end_date=snapshot_end_date,
    )
    return app_country_db_latest


def copy_daily_agg_to_weekly(
    store: int,
    app_detail_parquets: list[str],
    snapshot_date_str: str,
) -> str:
    bucket = CONFIG["s3"]["bucket"]
    if store == 2:
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
    elif store == 1:
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
    query = f"""COPY (
            SELECT 
                {export_cols}
            FROM read_parquet({app_detail_parquets}, union_by_name=true)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY store_id, country 
                ORDER BY crawled_at DESC
            ) = 1
        ) TO 's3://{bucket}/{APP_COUNTRY_WEEKLY_PREFIX}/store={store}/snapshot_date={snapshot_date_str}/'
    (FORMAT PARQUET,
     PARTITION_BY (country),
     OVERWRITE_OR_IGNORE true,
     COMPRESSION 'zstd');
    """
    return query


def copy_raw_details_to_daily_agg(
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
    ) TO 's3://{bucket}/{APP_COUNTRY_DAILY_PREFIX}/store={store}/snapshot_date={snapshot_date_str}/'
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
    cols = [
        "store_app",
        "in_app_purchases",
        "ad_supported",
        "snapshot_date",
        "installs_diff",
    ] + metrics
    df = pd.merge(
        df[cols], cohorts, on=["store_app", "snapshot_date"], how="left", validate="1:1"
    )
    logger.info("Finished calculating WAU")

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
    # TODO: Placeholder ARPU.
    paying_user_rate = 0.05
    paying_user_arpu = 10.0
    df["weekly_iap_revenue"] = df.apply(
        lambda x: (
            x["weekly_active_users"] * paying_user_rate * paying_user_arpu
            if x["in_app_purchases"]
            else 0.0
        ),
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
        df = query_apps_to_process_global_metrics(
            database_connection, batch_size=5000, days_back=2
        )
        if df.empty:
            break
        log_apps = df["store_app"].unique().tolist()

        odf = df.copy()
        df = odf[odf["store_app"] == 48466790]

        # Fill in blank rows with default
        df["tier_pct_sum"] = df["tier1_pct"] + df["tier2_pct"] + df["tier3_pct"]
        incomplete_tier_pct = df["tier_pct_sum"] < 0.5
        # Because for US is default, many apps end up with 1.0 t1
        all_t1 = df["tier1_pct"] == 1
        tiers_to_fill = incomplete_tier_pct | all_t1
        df.loc[tiers_to_fill, "tier1_pct"] = 0.34
        df.loc[tiers_to_fill, "tier2_pct"] = 0.33
        df.loc[tiers_to_fill, "tier3_pct"] = 0.33
        df = calculate_active_users(database_connection, df)
        df = calculate_revenue_cols(database_connection, df)
        df
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
        delete_and_insert(
            df=df,
            schema="public",
            table_name="app_global_metrics_weekly",
            database_connection=database_connection,
            delete_by_keys=["store_app", "week_start"],
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
