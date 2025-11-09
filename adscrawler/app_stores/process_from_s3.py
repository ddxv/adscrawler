import datetime
import os
import pathlib
import time
import uuid
from io import BytesIO

import numpy as np
import pandas as pd

from adscrawler.app_stores.utils import check_and_insert_new_apps
from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import (
    PostgresCon,
    get_db_connection,
)
from adscrawler.dbcon.queries import (
    query_categories,
    query_collections,
    query_countries,
    query_store_id_map,
    query_store_id_map_cached,
    upsert_df,
    delete_and_insert,
    query_languages,
)
from adscrawler.packages.storage import get_duckdb_connection, get_s3_client

logger = get_logger(__name__, "scrape_stores")

STAR_COLS = [
    "one_star",
    "two_star",
    "three_star",
    "four_star",
    "five_star",
]


METRIC_COLS = [
    "snapshot_date",
    "store_app",
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

COUNTRY_HISTORY_COLS = COUNTRY_HISTORY_KEYS + METRIC_COLS

GLOBAL_HISTORY_COLS = GLOBAL_HISTORY_KEYS + METRIC_COLS + ["store_last_updated"]


def raw_keywords_to_s3(
    df: pd.DataFrame,
) -> None:
    logger.info(f"S3 upload keywords rows={df.shape[0]} start")
    s3_client = get_s3_client()
    bucket = CONFIG["s3"]["bucket"]
    df["store_id"] = df["store_id"].astype(str)
    for store, store_df in df.groupby("store"):
        # handles edge case of a crawl spanning a date change
        for crawled_date, date_df in store_df.groupby("crawled_date"):
            if isinstance(crawled_date, datetime.date):
                crawled_date = crawled_date.strftime("%Y-%m-%d")
            for country, country_df in date_df.groupby("country"):
                epoch_ms = int(time.time() * 1000)
                suffix = uuid.uuid4().hex[:8]
                file_name = f"keywords_{epoch_ms}_{suffix}.parquet"
                s3_loc = "raw-data/keywords"
                s3_key = f"{s3_loc}/store={store}/crawled_date={crawled_date}/country={country}/{file_name}"
                buffer = BytesIO()
                country_df.to_parquet(buffer, index=False)
                buffer.seek(0)
                s3_client.upload_fileobj(buffer, bucket, s3_key)
    logger.info(f"S3 upload keywords {store=} finished")


def app_details_to_s3(
    df: pd.DataFrame,
    store: int,
) -> None:
    logger.info(f"S3 upload app_details {store=}, rows={df.shape[0]} start")
    if store is None:
        raise ValueError("store is required")
    s3_client = get_s3_client()
    bucket = CONFIG["s3"]["bucket"]
    # handles edge case of a crawl spanning a date change
    df["store_id"] = df["store_id"].astype(str)
    for crawled_date, date_df in df.groupby("crawled_date"):
        if isinstance(crawled_date, datetime.date):
            crawled_date = crawled_date.strftime("%Y-%m-%d")
        for country, country_df in date_df.groupby("country"):
            epoch_ms = int(time.time() * 1000)
            suffix = uuid.uuid4().hex[:8]
            file_name = f"app_details_{epoch_ms}_{suffix}.parquet"
            s3_loc = "raw-data/app_details"
            s3_key = f"{s3_loc}/store={store}/crawled_date={crawled_date}/country={country}/{file_name}"
            buffer = BytesIO()
            country_df.to_parquet(buffer, index=False)
            buffer.seek(0)
            s3_client.upload_fileobj(buffer, bucket, s3_key)
    logger.info(f"S3 upload app details {store=} finished")


def process_store_rankings(
    df: pd.DataFrame,
    store: int,
) -> None:
    logger.info(f"Process and save rankings start {store=}")
    if store is None:
        raise ValueError("store is required")
    output_dir = f"/tmp/exports/app_rankings/store={store}"
    s3_client = get_s3_client()
    bucket = CONFIG["s3"]["bucket"]
    for crawled_date, df_crawled_date in df.groupby("crawled_date"):
        if isinstance(crawled_date, datetime.date):
            crawled_date = crawled_date.strftime("%Y-%m-%d")
        for country, df_country in df_crawled_date.groupby("country"):
            local_path = pathlib.Path(
                f"{output_dir}/crawled_date={crawled_date}/country={country}/rankings.parquet"
            )
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            df_country.to_parquet(local_path, index=False)
            s3_key = f"raw-data/app_rankings/store={store}/crawled_date={crawled_date}/country={country}/rankings.parquet"
            logger.info(f"Uploading to S3: {s3_key}")
            s3_client.upload_file(str(local_path), bucket, s3_key)
            local_path.unlink()


def get_s3_rank_parquet_paths(
    s3_config_key: str, dt: pd.DatetimeIndex, days: int, store: int
) -> list[str]:
    s3 = get_s3_client()
    all_parquet_paths = []
    bucket = CONFIG[s3_config_key]["bucket"]
    for ddt in pd.date_range(dt, dt + datetime.timedelta(days=days), freq="D"):
        ddt_str = ddt.strftime("%Y-%m-%d")
        prefix = f"raw-data/app_rankings/store={store}/crawled_date={ddt_str}/country="
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        parquet_paths = [
            f"s3://{bucket}/{obj['Key']}"
            for obj in objects.get("Contents", [])
            if obj["Key"].endswith("rankings.parquet")
        ]
        all_parquet_paths += parquet_paths
    return all_parquet_paths


def get_s3_app_details_parquet_paths(
    snapshot_date: pd.DatetimeIndex,
    store: int,
) -> list[str]:
    bucket = CONFIG["s3"]["bucket"]
    all_parquet_paths = []
    ddt_str = snapshot_date.strftime("%Y-%m-%d")
    prefix = f"raw-data/app_details/store={store}/crawled_date={ddt_str}/country="
    all_parquet_paths += get_parquet_paths_by_prefix(bucket, prefix)
    return all_parquet_paths


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
    app_detail_parquets = get_s3_app_details_parquet_paths(
        snapshot_date=snapshot_date,
        store=store,
    )
    snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")
    if len(app_detail_parquets) == 0:
        logger.error(
            f"No app detail parquet files found for store={store} snapshot_date={snapshot_date_str}"
        )
        return
    s3_config_key = "s3"
    query = app_details_country_history_query(
        store=store,
        app_detail_parquets=app_detail_parquets,
        snapshot_date_str=snapshot_date_str,
    )
    duckdb_con = get_duckdb_connection(s3_config_key)
    duckdb_con.execute(query)
    duckdb_con.close()


def prep_app_metrics_history(
    df: pd.DataFrame, store: int, database_connection: PostgresCon
) -> pd.DataFrame:
    if store == 1:
        df["store_last_updated"] = np.where(
            (df["store_last_updated"] < 0) | (df["store_last_updated"].isna()),
            None,
            df["store_last_updated"],
        )
        df.loc[df["store_last_updated"].notna(), "store_last_updated"] = pd.to_datetime(
            df.loc[df["store_last_updated"].notna(), "store_last_updated"],
            unit="s",
        )
    if store == 2:
        ratings_str = df["user_ratings"].str.extractall(r"(\d+)").unstack()
        ratings_str = ratings_str.reindex(df.index, fill_value=0)
        df[STAR_COLS] = ratings_str.iloc[:, 1::2].astype(int).to_numpy()
        df["store_last_updated"] = pd.to_datetime(
            df["store_last_updated"], format="ISO8601", utc=True
        )
    store_id_map = query_store_id_map_cached(
        store=store, database_connection=database_connection
    )
    country_map = query_countries(database_connection)
    df["country_id"] = df["country"].map(
        country_map.set_index("alpha2")["id"].to_dict()
    )
    new_ids = df[~df["store_id"].isin(store_id_map["store_id"])]["store_id"].unique()
    if len(new_ids) > 0:
        logger.warning(f"Found new store ids: {len(new_ids)}")
        raise ValueError("New store ids found in S3 app history data")
    df["store_app"] = df["store_id"].map(
        store_id_map.set_index("store_id")["id"].to_dict()
    )
    df = df.convert_dtypes(dtype_backend="pyarrow")
    df = df.replace({pd.NA: None})
    return df


def manual_import_app_metrics_from_s3(
    start_date: datetime.date, end_date: datetime.date
) -> None:
    use_tunnel = False
    database_connection = get_db_connection(
        use_ssh_tunnel=use_tunnel, config_key="madrone"
    )

    start_date = datetime.datetime.fromisoformat("2025-10-01").date()
    for snapshot_date in pd.date_range(start_date, end_date, freq="D"):
        snapshot_date = snapshot_date.date()
        for store in [1, 2]:
            try:
                process_app_metrics_to_db(database_connection, store, snapshot_date)
            except:
                process_app_metrics_to_db(database_connection, store, snapshot_date)


def import_app_metrics_from_s3(
    start_date: datetime.date, end_date: datetime.date, database_connection: PostgresCon
) -> None:
    for snapshot_date in pd.date_range(start_date, end_date, freq="D"):
        snapshot_date = snapshot_date.date()
        for store in [1, 2]:
            try:
                process_app_metrics_to_db(database_connection, store, snapshot_date)
            except Exception as e:
                logger.error(
                    f"Error processing S3 app metrics for {snapshot_date} {store}: {e}"
                )


def process_app_metrics_to_db(
    database_connection: PostgresCon, store: int, snapshot_date: datetime.date
) -> None:
    logger.info(f"date={snapshot_date}, store={store} Make S3 agg")
    make_s3_app_country_metrics_history(store, snapshot_date=snapshot_date)
    logger.info(f"date={snapshot_date}, store={store} agg df load")
    df = get_s3_agg_daily_snapshots(snapshot_date, snapshot_date, store)
    if df.empty:
        logger.warning(
            f"No data found for S3 agg app metrics {store=} {snapshot_date=}"
        )
        return
    if store == 2:
        # Should be resolved from 11/1/2025
        problem_rows = df["store_id"].str.contains(".0")
        if problem_rows.any():
            logger.warning(
                f'Apple App IDs: Found {problem_rows.sum()} store_id with ".0" suffix, fixing'
            )
            df.loc[problem_rows, "store_id"] = (
                df.loc[problem_rows, "store_id"].str.split(".").str[0]
            )
            df["crawled_at"] = df["crawled_at"].sort_values(ascending=True)
            df = df.drop_duplicates(
                ["snapshot_date", "country", "store_id"], keep="last"
            )
    logger.info(f"date={snapshot_date}, store={store} agg df prep")
    df = prep_app_metrics_history(
        df=df, store=store, database_connection=database_connection
    )
    if not df[df["store_id"].isna()].empty:
        # Why are there many records with missing store_id?
        logger.warning("Found records with missing store_id")
        raise ValueError("Records with missing store_id found in S3 app history data")
    check_for_duplicates(
        df=df,
        key_columns=COUNTRY_HISTORY_KEYS,
    )
    # TESTING ONLY, ignore new apps since devdb is not updated
    df = df[df["store_app"].notna()]
    insert_columns = [x for x in COUNTRY_HISTORY_COLS if x in df.columns]
    if store == 1:
        # TODO: Can get installs per Country by getting review_count sum for all countries
        # Cannot do with this data set since it is snapshot_date only
        # and not every app for every country crawled on this date
        insert_columns = [x for x in insert_columns if x != "installs"]
    logger.info(f"date={snapshot_date}, store={store} agg df country upsert")
    upsert_df(
        df=df,
        table_name="app_country_metrics_history",
        database_connection=database_connection,
        key_columns=COUNTRY_HISTORY_KEYS,
        insert_columns=insert_columns,
    )
    if store == 1:
        df = df[df["country"] == "US"]
        df[STAR_COLS] = df["histogram"].apply(pd.Series)
    if store == 2:
        weighted_sum = (
            (df["rating"] * df["rating_count"])
            .groupby([df[k] for k in GLOBAL_HISTORY_KEYS])
            .sum()
        )
        weight_total = (
            df["rating_count"].groupby([df[k] for k in GLOBAL_HISTORY_KEYS]).sum()
        )
        df = df.groupby(GLOBAL_HISTORY_KEYS).agg(
            rating_count=("rating_count", "sum"),
            store_last_updated=("store_last_updated", "max"),
            **{col: (col, "sum") for col in STAR_COLS},
        )
        df["rating"] = weighted_sum / weight_total
        df["rating"] = df["rating"].astype("float64")
        df = df.reset_index()
    check_for_duplicates(
        df=df,
        key_columns=GLOBAL_HISTORY_KEYS,
    )
    df = df.replace({pd.NA: None, np.nan: None})
    insert_columns = [x for x in GLOBAL_HISTORY_COLS if x in df.columns]
    logger.info(f"date={snapshot_date}, store={store} agg df global upsert")
    upsert_df(
        df=df,
        table_name="app_global_metrics_history",
        database_connection=database_connection,
        key_columns=GLOBAL_HISTORY_KEYS,
        insert_columns=insert_columns,
    )


def get_parquet_paths_by_prefix(bucket: str, prefix: str) -> list[str]:
    s3 = get_s3_client()
    continuation_token = None
    all_parquet_paths = []
    while True:
        params = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        response = s3.list_objects_v2(**params)
        # Extract parquet paths from this page
        if "Contents" in response:
            parquet_paths = [
                f"s3://{bucket}/{obj['Key']}"
                for obj in response["Contents"]
                if obj["Key"].endswith(".parquet")
            ]
            all_parquet_paths += parquet_paths
        if "NextContinuationToken" in response:
            continuation_token = response["NextContinuationToken"]
        else:
            break
    return all_parquet_paths


def app_details_country_history_query(
    store: int,
    app_detail_parquets: list[str],
    # lookback_date_str: str,
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
    # WHERE crawled_date BETWEEN DATE '{lookback_date_str}' AND DATE '{snapshot_date_str}'
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


def import_ranks_from_s3(
    start_date: datetime.date,
    end_date: datetime.date,
    database_connection: PostgresCon,
    period: str = "week",
) -> None:
    if period == "week":
        freq = "W-MON"
    elif period == "day":
        freq = "D"
    else:
        raise ValueError(f"Invalid period {period}")
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    s3_region = CONFIG[s3_config_key]["region_name"]
    logger.info(f"Importing ranks from S3 {bucket=} in {s3_region=}")
    for dt in pd.date_range(start_date, end_date, freq=freq):
        for store in [1, 2]:
            process_ranks_from_s3(
                store=store,
                dt=dt,
                database_connection=database_connection,
                period=period,
                s3_config_key=s3_config_key,
            )


def process_ranks_from_s3(
    store: int,
    dt: datetime.date,
    database_connection: PostgresCon,
    period: str,
    s3_config_key: str,
) -> None:
    if period == "week":
        days = 6
        table_suffix = "weekly"
    elif period == "day":
        days = 1
        table_suffix = "daily"
    else:
        raise ValueError(f"Invalid period {period}")
    period_date_str = dt.strftime("%Y-%m-%d")
    logger.info(f"Processing store={store} period_start={period_date_str}")
    all_parquet_paths = get_s3_rank_parquet_paths(s3_config_key, dt, days, store)
    if len(all_parquet_paths) == 0:
        logger.info(f"No parquet files found for period_start={period_date_str}")
        return
    countries_in_period = [
        x.split("country=")[1].split("/")[0]
        for x in all_parquet_paths
        if "country=" in x
    ]
    countries_in_period = list(set(countries_in_period))
    for country in countries_in_period:
        country_parquet_paths = [
            x for x in all_parquet_paths if f"country={country}" in x
        ]
        logger.info(
            f"DuckDB {store=} period_start={period_date_str} {country=} files={len(country_parquet_paths)}"
        )
        wdf = query_store_collection_ranks(
            country_parquet_paths=country_parquet_paths,
            period=period,
            s3_config_key=s3_config_key,
        )
        store_id_map = query_store_id_map_cached(
            store=store, database_connection=database_connection
        )
        country_map = query_countries(database_connection)
        collection_map = query_collections(database_connection)
        category_map = query_categories(database_connection)
        wdf["country"] = wdf["country"].map(
            country_map.set_index("alpha2")["id"].to_dict()
        )
        wdf["store_collection"] = wdf["collection"].map(
            collection_map.set_index("collection")["id"].to_dict()
        )
        wdf["store_category"] = wdf["category"].map(
            category_map.set_index("category")["id"].to_dict()
        )
        new_ids = wdf[~wdf["store_id"].isin(store_id_map["store_id"])][
            "store_id"
        ].unique()
        if len(new_ids) > 0:
            logger.info(f"Found new store ids: {len(new_ids)}")
            new_ids = [{"store": store, "store_id": store_id} for store_id in new_ids]
            check_and_insert_new_apps(
                database_connection=database_connection,
                dicts=new_ids,
                crawl_source="process_ranks_parquet",
                store=store,
            )
            store_id_map = query_store_id_map(database_connection, store)
        wdf["store_app"] = wdf["store_id"].map(
            store_id_map.set_index("store_id")["id"].to_dict()
        )
        wdf = wdf.drop(columns=["store_id", "collection", "category"])
        upsert_df(
            df=wdf,
            table_name=f"store_app_ranks_{table_suffix}",
            schema="frontend",
            database_connection=database_connection,
            key_columns=[
                "country",
                "store_collection",
                "store_category",
                "crawled_date",
                "rank",
            ],
            insert_columns=[
                "country",
                "store_collection",
                "store_category",
                "crawled_date",
                "rank",
                "store_app",
                "best_rank",
            ],
        )


def query_store_collection_ranks(
    country_parquet_paths: list[str],
    period: str,
    s3_config_key: str,
) -> pd.DataFrame:
    """Process parquet files for a specific country and insert into the database."""
    period_query = f"""WITH all_data AS (
                SELECT * FROM read_parquet({country_parquet_paths})
                 ),
             period_max_dates AS (
                          SELECT ar_1.country,
                             ar_1.collection,
                             ar_1.category,
                             date_trunc('{period}'::text, ar_1.crawled_date::VARCHAR::DATE) AS period_start,
                             max(ar_1.crawled_date) AS max_crawled_date
                            FROM all_data ar_1
                           GROUP BY ar_1.country, ar_1.collection, ar_1.category, (date_trunc('{period}'::text, ar_1.crawled_date::VARCHAR::DATE))
                         ),
             period_app_ranks as (SELECT ar.rank,
                min(ar.rank) AS best_rank,
                ar.country,
                ar.collection,
                ar.category,
                ar.crawled_date,
                ar.store_id
               FROM all_data ar
                 JOIN period_max_dates pmd ON ar.country = pmd.country 
                 AND ar.collection = pmd.collection 
                 AND ar.category = pmd.category 
                 AND ar.crawled_date = pmd.max_crawled_date
                 GROUP BY ar.rank, ar.country, ar.collection, ar.category, ar.crawled_date, ar.store_id
                 )
            SELECT par.rank, par.best_rank, par.country, par.collection, par.category, par.crawled_date, par.store_id FROM period_app_ranks par
              ORDER BY par.country, par.collection, par.category, par.crawled_date, par.rank
            """
    duckdb_con = get_duckdb_connection(s3_config_key)
    return duckdb_con.execute(period_query).df()


def manual_download_rankings(
    store: int, crawled_date: datetime.date, country: str
) -> pd.DataFrame:
    s3_client = get_s3_client()
    bucket = CONFIG["s3"]["bucket"]
    store = 1
    country = "US"
    crawled_date = datetime.date(2025, 8, 15)
    s3_key = f"raw-data/app_rankings/store={store}/crawled_date={crawled_date}/country={country}/rankings.parquet"
    local_path = pathlib.Path(
        f"/tmp/exports/app_rankings/store={store}/crawled_date={crawled_date}/country={country}/rankings.parquet"
    )
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3_client.download_file(bucket, s3_key, str(local_path))
    df = pd.read_parquet(local_path)
    return df


def import_keywords_from_s3(
    start_date: datetime.date, end_date: datetime.date, database_connection: PostgresCon
) -> None:
    language = "en"
    country_map = query_countries(database_connection)
    languages_map = query_languages(database_connection)
    language_dict = languages_map.set_index("language_slug")["id"].to_dict()
    _language_key = language_dict[language]
    s3_config_key = "s3"
    bucket = CONFIG[s3_config_key]["bucket"]
    for snapshot_date in pd.date_range(start_date, end_date, freq="D"):
        snapshot_date = snapshot_date.date()
        for store in [1, 2]:
            s3_loc = "raw-data/keywords"
            s3_key = f"{s3_loc}/store={store}/crawled_date={snapshot_date}/"
            parquet_paths = get_parquet_paths_by_prefix(bucket, s3_key)
            if len(parquet_paths) == 0:
                logger.warning(f"No parquet paths found for {s3_key}")
                continue
            df = query_keywords_from_s3(parquet_paths, s3_config_key)
            store_id_map = query_store_id_map_cached(database_connection, store)
            df["store_app"] = df["store_id"].map(
                store_id_map.set_index("store_id")["id"].to_dict()
            )
            df["country"] = df["country"].map(
                country_map.set_index("alpha2")["id"].to_dict()
            )
            if df["store_app"].isna().any():
                check_and_insert_new_apps(
                    database_connection=database_connection,
                    dicts=df.to_dict(orient="records"),
                    crawl_source="keywords",
                    store=store,
                )
                store_id_map = query_store_id_map_cached(database_connection, store)
                df["store_app"] = df["store_id"].map(
                    store_id_map.set_index("store_id")["id"].to_dict()
                )
            delete_and_insert(
                df=df,
                table_name="app_keyword_ranks_daily",
                schema="frontend",
                database_connection=database_connection,
                delete_by_keys=["crawled_date"],
                insert_columns=[
                    "country",
                    "keyword_id",
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
    duckdb_con = get_duckdb_connection(s3_config_key)
    return duckdb_con.execute(period_query).df()
