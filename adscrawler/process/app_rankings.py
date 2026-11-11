"""Upload rankings to & import rankings from S3 parquet."""

import datetime
import os
import pathlib

import pandas as pd

from adscrawler.app_stores.utils import check_and_insert_new_apps
from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.atomic_swap import atomic_swap_date_partition
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import (
    clean_app_ranks_weekly_table,
    query_categories,
    query_collections,
    query_countries,
    query_store_id_map,
    query_store_id_map_cached,
)
from adscrawler.process import RAW_DATA_APP_RANKINGS
from adscrawler.process.storage import (
    get_duckdb_connection,
    get_parquet_paths_by_prefix,
    get_s3_client,
)

logger = get_logger(__name__, "scrape_stores")


def process_store_rankings(df: pd.DataFrame, store: int) -> None:
    """Upload store-rankings to ``raw-data/app_rankings/`` on S3."""
    log_info = f"Upload rank parquets {store=}"
    logger.info(f"{log_info} start")
    if store is None:
        raise ValueError("store is required")
    output_dir = f"/tmp/exports/app_rankings/store={store}"
    s3_client = get_s3_client()
    bucket = CONFIG["s3"]["bucket"]
    for crawled_date, df_crawled_date in df.groupby("crawled_date"):
        if isinstance(crawled_date, datetime.date):
            crawled_date = crawled_date.strftime("%Y-%m-%d")
        for country, df_country in df_crawled_date.groupby("country"):
            if df_country.empty:
                logger.warning(
                    f"{log_info} {crawled_date=} country={country=} empty dataframe, skipping upload"
                )
                continue
            local_path = pathlib.Path(
                f"{output_dir}/crawled_date={crawled_date}/country={country}/rankings.parquet"
            )
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            df_country.to_parquet(local_path, index=False)
            s3_key = f"{RAW_DATA_APP_RANKINGS}/store={store}/crawled_date={crawled_date}/country={country}/rankings.parquet"
            logger.info(f"{log_info} {crawled_date=} {country=} uploading to S3")
            s3_client.upload_file(str(local_path), bucket, s3_key)
            local_path.unlink()


def get_s3_rank_parquet_paths(
    s3_config_key: str, dt: pd.DatetimeIndex, days: int, store: int
) -> list[str]:
    all_parquet_paths = []
    bucket = CONFIG[s3_config_key]["bucket"]
    for ddt in pd.date_range(dt, dt + datetime.timedelta(days=days), freq="D"):
        ddt_str = ddt.strftime("%Y-%m-%d")
        prefix = (
            f"{RAW_DATA_APP_RANKINGS}/store={store}/crawled_date={ddt_str}/country="
        )
        parquet_paths = get_parquet_paths_by_prefix(bucket=bucket, prefix=prefix)
        all_parquet_paths += parquet_paths
    return all_parquet_paths


def import_ranks_from_s3(
    start_date: datetime.date,
    end_date: datetime.date,
    pgdb: PostgresEngine,
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
    # Collect every period before swapping: the same output date can occur in
    # multiple periods, and a partition must contain data from both stores.
    frames = []
    for dt in pd.date_range(start_date, end_date, freq=freq):
        for store in [1, 2]:
            df = process_ranks_from_s3(
                store=store,
                dt=dt,
                pgdb=pgdb,
                period=period,
                s3_config_key=s3_config_key,
            )
            if not df.empty:
                frames.append(df)
    if not frames:
        clean_app_ranks_weekly_table(pgdb)
        return

    ranks = pd.concat(frames, ignore_index=True)
    keys = ["crawled_date", "country", "store_collection", "store_category", "rank"]
    required = keys + ["store_app", "best_rank", "source_crawled_date"]
    if ranks[required].isna().any().any():
        raise ValueError("Ranking rows contain missing required values")
    # Historical files lack crawl times; store_app makes same-date ties repeatable.
    ranks = ranks.sort_values(
        ["source_crawled_date", "source_crawled_at", "store_app"],
        kind="stable",
        na_position="first",
    ).drop_duplicates(subset=keys, keep="last")
    ranks = ranks.drop(columns=["source_crawled_date", "source_crawled_at"])
    for crawled_date, date_ranks in ranks.groupby("crawled_date"):
        atomic_swap_date_partition(
            date_ranks,
            pgdb,
            schema="frontend",
            table="app_ranks_weekly",
            partition_prefix="store_app_ranks",
            partition_date=pd.Timestamp(crawled_date).date(),
        )
    clean_app_ranks_weekly_table(pgdb)


def process_ranks_from_s3(
    store: int,
    dt: datetime.date,
    pgdb: PostgresEngine,
    period: str,
    s3_config_key: str,
) -> pd.DataFrame:
    if period == "week":
        days = 6
    elif period == "day":
        days = 0
    else:
        raise ValueError(f"Invalid period {period}")
    period_date_str = dt.strftime("%Y-%m-%d")
    log_info = "process_ranks_from_s3"
    logger.info(f"{log_info} {store=} {period_date_str=}")
    all_parquet_paths = get_s3_rank_parquet_paths(s3_config_key, dt, days, store)
    if len(all_parquet_paths) == 0:
        logger.info(f"{log_info} no parquet files found for {period_date_str=}")
        return pd.DataFrame()
    countries_in_period = [
        x.split("country=")[1].split("/")[0]
        for x in all_parquet_paths
        if "country=" in x
    ]
    countries_in_period = list(set(countries_in_period))
    logger.info(
        f"{log_info} {store=} {period_date_str=} countries={len(countries_in_period)} files={len(all_parquet_paths)}"
    )
    df = query_store_collection_ranks(
        country_parquet_paths=all_parquet_paths,
        period=period,
        s3_config_key=s3_config_key,
    )
    store_id_map = query_store_id_map_cached(store=store, pgdb=pgdb)
    country_map = query_countries(pgdb)
    collection_map = query_collections(pgdb)
    category_map = query_categories(pgdb)
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
            pgdb=pgdb,
            dicts=new_ids,
            crawl_source="process_ranks_parquet",
            store=store,
        )
        store_id_map = query_store_id_map(pgdb, store)
    df["store_app"] = df["store_id"].map(
        store_id_map.set_index("store_id")["id"].to_dict()
    )
    df = df.drop(columns=["store_id", "collection", "category"])
    return df


def query_store_collection_ranks(
    country_parquet_paths: list[str],
    period: str,
    s3_config_key: str,
) -> pd.DataFrame:
    """Query parquet files for app country ranks."""
    period_query = f"""WITH all_data AS (
                SELECT * FROM read_parquet({country_parquet_paths}, union_by_name=true)
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
                CASE 
                    WHEN '{period}' = 'week' AND ANY_VALUE(pmd.period_start) < DATE_TRUNC('week', CURRENT_DATE)
                    THEN ANY_VALUE(pmd.period_start)
                    ELSE ar.crawled_date 
                END AS crawled_date,
                ar.store_id,
                ar.crawled_date AS source_crawled_date,
                max(try_cast(to_json(ar)->>'crawled_at' AS TIMESTAMPTZ)) AS source_crawled_at
               FROM all_data ar
                 JOIN period_max_dates pmd ON ar.country = pmd.country 
                 AND ar.collection = pmd.collection 
                 AND ar.category = pmd.category 
                 AND ar.crawled_date = pmd.max_crawled_date
                 GROUP BY ar.rank, ar.country, ar.collection, ar.category, ar.crawled_date, ar.store_id
                 )
            SELECT par.rank, par.best_rank, par.country, par.collection, par.category,
                   par.crawled_date, par.store_id, par.source_crawled_date, par.source_crawled_at
            FROM period_app_ranks par
              ORDER BY par.country, par.collection, par.category, par.crawled_date, par.rank
            """
    with get_duckdb_connection(s3_config_key) as duckdb_con:
        return duckdb_con.execute(period_query).df()


def manual_download_rankings(
    store: int, crawled_date: datetime.date, country: str
) -> pd.DataFrame:
    s3_client = get_s3_client()
    bucket = CONFIG["s3"]["bucket"]
    store = 1
    country = "US"
    crawled_date = datetime.date(2025, 8, 15)
    s3_key = f"{RAW_DATA_APP_RANKINGS}/store={store}/crawled_date={crawled_date}/country={country}/rankings.parquet"
    local_path = pathlib.Path(
        f"/tmp/exports/app_rankings/store={store}/crawled_date={crawled_date}/country={country}/rankings.parquet"
    )
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3_client.download_file(bucket, s3_key, str(local_path))
    df = pd.read_parquet(local_path)
    return df
