import datetime
import os
import pathlib
import re
import time
import uuid
from io import BytesIO

import numpy as np
import pandas as pd

from adscrawler.app_stores.utils import insert_new_apps
from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import (
    PostgresCon,
)
from adscrawler.dbcon.queries import (
    query_categories,
    query_collections,
    query_countries,
    query_store_id_map,
    query_store_id_map_cached,
    upsert_df,
)
from adscrawler.packages.storage import get_duckdb_connection, get_s3_client

logger = get_logger(__name__, "scrape_stores")


def app_details_to_s3(
    df: pd.DataFrame,
    store: int,
) -> None:
    logger.info(f"S3 upload app details {store=} start")
    if store is None:
        raise ValueError("store is required")
    s3_client = get_s3_client()
    bucket = CONFIG["s3"]["bucket"]
    # handles edge case of a crawl spanning a date change
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
    s3, bucket: str, dt: pd.DatetimeIndex, days: int, store: int
) -> list[str]:
    all_parquet_paths = []
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
        prefix = f"agg-data/store_app_country_history/store={store}/snapshot_date={ddt_str}/country="
        print(prefix)
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
    duckdb_con = get_duckdb_connection(s3_config_key)
    query = f"""
      SELECT *
      FROM read_parquet({parquet_paths})
    """
    df = duckdb_con.execute(query).df()
    duckdb_con.close()
    return df


def s3_store_app_global_history_to_db(
    df: pd.DataFrame,
    store: int,
    database_connection: PostgresCon,
) -> None:
    df = df[df["store_id"].notna()]
    star_cols = ["one_star", "two_star", "three_stars", "four_stars", "five_stars"]
    if store == 1:
        df[star_cols] = df["histogram"].apply(pd.Series)
        df["store_last_updated"] = np.where(
            (df["store_last_updated"] < 0) | (df["store_last_updated"].isna()),
            None,
            df["store_last_updated"],
        )
        df.loc[df["store_last_updated"].notna(), "store_last_updated"] = pd.to_datetime(
            df.loc[df["store_last_updated"].notna(), "store_last_updated"], unit="s"
        )
    if store == 2:
        df["user_ratings"].tail()
        df[star_cols] = df["user_ratings"].str.findall(r":\s*(\d+)").apply(pd.Series)
        df[star_cols] = (
            df["user_ratings"]
            .apply(lambda x: [int(num) for num in re.findall(r"\d+", x)[1::2]])
            .apply(pd.Series)
        )
        ratings_str = df["user_ratings"].str.extractall(r"(\d+)").unstack()
        ratings_str = ratings_str.reindex(df.index, fill_value=0)
        df[star_cols] = ratings_str.iloc[:, 1::2].astype(int).values
        df["store_last_updated"] = pd.to_datetime(
            df["store_last_updated"], format="ISO8601", utc=True
        )
        df.groupby(["store_id", "country"])[
            ["rating_count"] + star_cols
        ].sum().reset_index()
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
    all_columns = [
        "snapshot_date",
        "store_app",
        "review_count",
        "rating",
        "rating_count",
        "one_star",
        "two_star",
        "three_stars",
        "four_stars",
        "five_stars",
    ]
    key_columns = ["snapshot_date", "store_app"]
    insert_columns = [x for x in all_columns if x in df.columns]
    any_duplicates = df[insert_columns].duplicated(subset=key_columns).any()
    if any_duplicates:
        df[df[insert_columns].duplicated(subset=key_columns)][
            ["store_id", "snapshot_date", "crawled_at"]
        ]
        logger.error("Duplicates found in app history data, dropping duplicates")
        raise ValueError("Duplicates found in app history data")
    # TESTING ONLY
    df = df[df["store_app"].notna()]
    upsert_df(
        df=df,
        table_name="store_app_global_history",
        database_connection=database_connection,
        key_columns=key_columns,
        insert_columns=insert_columns,
    )


def s3_store_app_country_history_to_db(
    df: pd.DataFrame,
    store: int,
    database_connection: PostgresCon,
) -> None:
    df = df[df["store_id"].notna()]
    star_cols = ["one_star", "two_star", "three_stars", "four_stars", "five_stars"]
    if store == 1:
        df["store_last_updated"] = np.where(
            (df["store_last_updated"] < 0) | (df["store_last_updated"].isna()),
            None,
            df["store_last_updated"],
        )
        df.loc[df["store_last_updated"].notna(), "store_last_updated"] = pd.to_datetime(
            df.loc[df["store_last_updated"].notna(), "store_last_updated"], unit="s"
        )
    if store == 2:
        df[star_cols] = (
            df["user_ratings"]
            .apply(lambda x: [int(num) for num in re.findall(r"\d+", x)[1::2]])
            .apply(pd.Series)
        )
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
    # snapshot date?
    all_columns = [
        "snapshot_date",
        "country_id",
        "store_app",
        "review_count",
        "rating",
        "rating_count",
        "one_star",
        "two_star",
        "three_stars",
        "four_stars",
        "five_stars",
    ]
    key_columns = ["snapshot_date", "country_id", "store_app"]
    insert_columns = [x for x in all_columns if x in df.columns]
    any_duplicates = df[insert_columns].duplicated(subset=key_columns).any()
    if any_duplicates:
        df[df[insert_columns].duplicated(subset=key_columns)][
            ["store_id", "country_id", "snapshot_date", "crawled_at"]
        ]
        logger.error("Duplicates found in app history data, dropping duplicates")
        raise ValueError("Duplicates found in app history data")
    # TESTING ONLY
    df = df[df["store_app"].notna()]
    upsert_df(
        df=df,
        table_name="store_app_country_history",
        database_connection=database_connection,
        key_columns=key_columns,
        insert_columns=insert_columns,
    )


def make_s3_store_app_country_history(store, snapshot_date) -> None:
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


def snapshot_rerun_store_app_country_history(database_connection: PostgresCon) -> None:
    start_date = datetime.date(2025, 10, 10)
    end_date = datetime.date(2025, 10, 28)
    # lookback_days = 60
    for snapshot_date in pd.date_range(start_date, end_date, freq="D"):
        snapshot_date = snapshot_date.date()
        for store in [1, 2]:
            logger.info(f"date={snapshot_date}, store={store} Make S3 agg")
            make_s3_store_app_country_history(store, snapshot_date=snapshot_date)
            logger.info(f"date={snapshot_date}, store={store} Load to db")
            df = get_s3_agg_daily_snapshots(snapshot_date, snapshot_date, store)
            s3_store_app_country_history_to_db(
                store=store,
                df=df,
                database_connection=database_connection,
            )
            if store == 1:
                df = df[df["country"] == "US"]
            s3_store_app_global_history_to_db(
                store,
                df=df,
                database_connection=database_connection,
            )


def get_parquet_paths_by_prefix(bucket, prefix: str) -> list[str]:
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
    if store == 2:
        data_cols = """
             trackId AS store_id,
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
    ) TO 's3://adscrawler/agg-data/store_app_country_history/store={store}/snapshot_date={snapshot_date_str}/'
    (FORMAT PARQUET, 
    PARTITION_BY (country), 
    ROW_GROUP_SIZE 100000, 
    COMPRESSION 'zstd',
    OVERWRITE_OR_IGNORE true);
    """
    return query


def import_ranks_from_s3(
    store: int,
    start_date: datetime.date,
    end_date: datetime.date,
    database_connection: PostgresCon,
    period: str = "week",
) -> None:
    if period == "week":
        days = 6
        freq = "W-MON"
        table_suffix = "weekly"
    elif period == "day":
        days = 1
        freq = "D"
        table_suffix = "daily"
    else:
        raise ValueError(f"Invalid period {period}")
    s3_config_key = "s3"
    duckdb_con = get_duckdb_connection(s3_config_key)
    s3 = get_s3_client()
    bucket = CONFIG[s3_config_key]["bucket"]
    s3_region = CONFIG[s3_config_key]["region_name"]
    logger.info(f"Importing ranks from S3 {bucket=} in {s3_region=}")
    for dt in pd.date_range(start_date, end_date, freq=freq):
        period_date_str = dt.strftime("%Y-%m-%d")
        logger.info(f"Processing store={store} period_start={period_date_str}")
        all_parquet_paths = get_s3_rank_parquet_paths(s3, bucket, dt, days, store)
        if len(all_parquet_paths) == 0:
            logger.info(f"No parquet files found for period_start={period_date_str}")
            continue
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
            wdf = process_parquets_and_insert(
                country_parquet_paths=country_parquet_paths,
                period=period,
                duckdb_con=duckdb_con,
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
                new_ids = [
                    {"store": store, "store_id": store_id} for store_id in new_ids
                ]
                insert_new_apps(
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


def process_parquets_and_insert(
    country_parquet_paths: list[str],
    period: str,
    duckdb_con,
):
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
