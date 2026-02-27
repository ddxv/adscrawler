import datetime
import os
import pathlib
import time
import uuid
from io import BytesIO

import pandas as pd

from adscrawler.app_stores.utils import (
    check_and_insert_new_apps,
    get_parquet_paths_by_prefix,
)
from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import (
    PostgresCon,
)
from adscrawler.dbcon.queries import (
    clean_app_ranks_weekly_table,
    delete_and_insert,
    query_categories,
    query_collections,
    query_countries,
    query_languages,
    query_store_id_map,
    query_store_id_map_cached,
    upsert_df,
)
from adscrawler.packages.storage import get_duckdb_connection, get_s3_client

logger = get_logger(__name__, "scrape_stores")


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
    clean_app_ranks_weekly_table(database_connection)


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
    logger.info(
        f"S3 Ranks {store=} {period_date_str=} {countries_in_period=} files={len(all_parquet_paths)}"
    )
    df = query_store_collection_ranks(
        country_parquet_paths=all_parquet_paths,
        period=period,
        s3_config_key=s3_config_key,
    )
    store_id_map = query_store_id_map_cached(
        store=store, database_connection=database_connection
    )
    country_map = query_countries(database_connection)
    collection_map = query_collections(database_connection)
    category_map = query_categories(database_connection)
    df["country"] = df["country"].map(country_map.set_index("alpha2")["id"].to_dict())
    df["store_collection"] = df["collection"].map(
        collection_map.set_index("collection")["id"].to_dict()
    )
    df["store_category"] = df["category"].map(
        category_map.set_index("category")["id"].to_dict()
    )
    new_ids = df[~df["store_id"].isin(store_id_map["store_id"])]["store_id"].unique()
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
    df["store_app"] = df["store_id"].map(
        store_id_map.set_index("store_id")["id"].to_dict()
    )
    df = df.drop(columns=["store_id", "collection", "category"])
    table_name = f"store_app_ranks_{table_suffix}"
    upsert_df(
        df=df,
        table_name=table_name,
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
    """Query parquet files fora app country ranks."""
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
                store_id_map = query_store_id_map(database_connection, store)
                df["store_app"] = df["store_id"].map(
                    store_id_map.set_index("store_id")["id"].to_dict()
                )
            df["store"] = store
            logger.info(
                f"Keywords from S3 insert {snapshot_date} {store=} {df.shape[0]} rows"
            )
            delete_and_insert(
                df=df,
                table_name="app_keyword_ranks_daily",
                schema="frontend",
                database_connection=database_connection,
                delete_by_keys=["crawled_date", "store"],
                insert_columns=[
                    "country",
                    "keyword_id",
                    "store",
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
