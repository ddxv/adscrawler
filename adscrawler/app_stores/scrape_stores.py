import datetime
import os
import pathlib
import re
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from io import BytesIO
from urllib.error import URLError
from urllib.parse import unquote_plus

import google_play_scraper
import imagehash
import numpy as np
import pandas as pd
import requests
import tldextract
from itunes_app_scraper.util import AppStoreException
from PIL import Image

from adscrawler.app_stores.apkcombo import get_apkcombo_android_apps
from adscrawler.app_stores.appbrain import get_appbrain_android_apps
from adscrawler.app_stores.apple import (
    clean_ios_app_df,
    crawl_ios_developers,
    scrape_app_ios,
    scrape_ios_ranks,
    search_app_store_for_ids,
)
from adscrawler.app_stores.google import (
    clean_google_play_app_df,
    crawl_google_developers,
    scrape_app_gp,
    scrape_google_ranks,
    search_play_store,
)
from adscrawler.config import APP_ICONS_TMP_DIR, CONFIG, get_logger
from adscrawler.dbcon.connection import (
    PostgresCon,
    get_db_connection,
)
from adscrawler.dbcon.queries import (
    get_crawl_scenario_countries,
    get_store_app_columns,
    query_categories,
    query_collections,
    query_countries,
    query_developers,
    query_keywords_to_crawl,
    query_languages,
    query_store_apps_to_update,
    query_store_id_map,
    query_store_id_map_cached,
    query_store_ids,
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


def process_chunk(
    df_chunk: pd.DataFrame,
    store: int,
    use_ssh_tunnel: bool,
    process_icon: bool,
    total_rows: int,
):
    chunk_info = f"Chunk {df_chunk.index[0]}-{df_chunk.index[-1]}/{total_rows}"
    database_connection = get_db_connection(use_ssh_tunnel=use_ssh_tunnel)
    logger.info(f"{chunk_info} start")
    try:
        chunk_results = []
        for _, row in df_chunk.iterrows():
            try:
                result_dict = scrape_app(
                    store=store,
                    store_id=row["store_id"],
                    country=row["country_code"],
                    language=row["language"],
                )
                chunk_results.append(result_dict)
            except Exception as e:
                logger.exception(
                    f"store={row.store}, store_id={row.store_id} update_all_app_info failed with {e}"
                )
        results_df = pd.DataFrame(chunk_results)
        results_df["crawled_date"] = results_df["crawled_at"].dt.date
        app_details_to_s3(results_df, store=store)
        log_crawl_results(results_df, store, database_connection=database_connection)
        for crawl_result, apps_df in results_df.groupby("crawl_result"):
            if crawl_result != 1:
                apps_df = apps_df[["store_id", "store", "crawled_at", "crawl_result"]]
            else:
                apps_df = clean_scraped_df(df=apps_df, store=row["store"])
                if "description_short" not in apps_df.columns:
                    apps_df["description_short"] = ""
                apps_df.loc[
                    apps_df["description_short"].isna(), "description_short"
                ] = ""
                if process_icon:
                    try:
                        apps_df = pd.merge(
                            apps_df,
                            df_chunk[["store_id", "icon_url_100"]],
                            on="store_id",
                            how="inner",
                            validate="1:1",
                        )
                        no_icon = apps_df["icon_url_100"].isna()
                        if apps_df[no_icon].empty:
                            pass
                        else:
                            apps_df.loc[no_icon, "icon_url_100"] = apps_df.loc[
                                no_icon
                            ].apply(
                                lambda x: process_app_icon(
                                    x["store_id"], x["icon_url_512"]
                                ),
                                axis=1,
                            )
                    except Exception:
                        logger.exception(
                            f"{row['store_id']} failed to process app icon"
                        )
            apps_df = apps_df.convert_dtypes(dtype_backend="pyarrow")
            apps_df = apps_df.replace({pd.NA: None})
            apps_details_to_db(
                apps_df=apps_df,
                database_connection=database_connection,
            )
            logger.info(f"{chunk_info} upserted {crawl_result=} apps={len(apps_df)} ")
    except Exception as e:
        logger.exception(f"{chunk_info} error processing with {e}")
    finally:
        logger.info(f"{chunk_info} finished")
        if database_connection and hasattr(database_connection, "engine"):
            database_connection.engine.dispose()
            logger.debug(f"{chunk_info} database connection disposed")


def update_app_details(
    database_connection,
    store,
    use_ssh_tunnel,
    workers,
    process_icon,
    limit,
    country_crawl_priority,
):
    """Process apps with dynamic work queue - simple and efficient."""
    log_info = f"Update app details: {store=}"

    df = query_store_apps_to_update(
        store=store,
        database_connection=database_connection,
        limit=limit,
        log_query=True,
        country_crawl_priority=country_crawl_priority,
    )
    logger.info(f"{log_info} start {len(df)} apps")

    # Calculate optimal chunk size: aim for 3-10x more chunks than workers
    chunk_size = max(50, min(1000, len(df) // (workers * 5)))
    chunks = [df.iloc[i : i + chunk_size] for i in range(0, len(df), chunk_size)]

    total_chunks = len(chunks)
    total_rows = len(df)
    logger.info(
        f"Processing {total_rows} apps in {total_chunks} chunks of ~{chunk_size} apps each"
    )

    completed_count = 0
    failed_count = 0

    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Submit all chunks, but stagger the first wave to avoid API thundering herd
        future_to_idx = {}
        for idx, df_chunk in enumerate(chunks):
            future = executor.submit(
                process_chunk, df_chunk, store, use_ssh_tunnel, process_icon, total_rows
            )
            future_to_idx[future] = idx

            # Only stagger the initial batch to avoid simultaneous API burst
            if idx < workers:
                time.sleep(0.5)  # 500ms between initial worker starts

        logger.info(f"All {total_chunks} chunks submitted (first {workers} staggered)")

        # Process results as they complete
        for future in as_completed(future_to_idx):
            chunk_idx = future_to_idx[future]

            try:
                _result = future.result()
                completed_count += 1

                if completed_count % 10 == 0 or completed_count == total_chunks:
                    logger.info(
                        f"Progress: {completed_count}/{total_chunks} chunks "
                        f"({completed_count / total_chunks * 100:.1f}%) | "
                        f"Failed: {failed_count}"
                    )

            except Exception as e:
                failed_count += 1
                logger.error(f"Chunk {chunk_idx} failed: {e}")

    logger.info(
        f"{log_info} finished | Completed: {completed_count} | Failed: {failed_count}"
    )

    return completed_count, failed_count


def crawl_keyword_cranks(database_connection: PostgresCon) -> None:
    countries_map = query_countries(database_connection)
    languages_map = query_languages(database_connection)
    country = "us"
    language = "en"
    language_dict = languages_map.set_index("language_slug")["id"].to_dict()
    language_key = language_dict[language]
    kdf = query_keywords_to_crawl(database_connection, limit=1000)
    for _id, row in kdf.iterrows():
        logger.info(
            f"Crawling keywords: {_id}/{kdf.shape[0]} keyword={row.keyword_text}"
        )
        keyword = row.keyword_text
        keyword_id = row.keyword_id
        try:
            df = scrape_keyword(
                country=country,
                language=language,
                keyword=keyword,
            )
            df["keyword"] = keyword_id
            df["lang"] = language_key
            process_scraped(
                database_connection=database_connection,
                ranked_dicts=df.to_dict(orient="records"),
                crawl_source="scrape_keywords",
                countries_map=countries_map,
            )
        except Exception:
            logger.exception(f"Scrape keyword={keyword} hit error, skipping")
        key_columns = ["keyword"]
        upsert_df(
            table_name="keywords_crawled_at",
            schema="logging",
            insert_columns=["keyword", "crawled_at"],
            df=pd.DataFrame(
                {
                    "keyword": [keyword_id],
                    "crawled_at": datetime.datetime.now(tz=datetime.UTC),
                }
            ),
            key_columns=key_columns,
            database_connection=database_connection,
        )


def scrape_store_ranks(database_connection: PostgresCon, store: int) -> None:
    collections_map = query_collections(database_connection)
    categories_map = query_categories(database_connection)
    countries_map = query_countries(database_connection)
    collections_map = collections_map.rename(columns={"id": "store_collection"})
    categories_map = categories_map.rename(columns={"id": "store_category"})
    ranks_country_list = get_crawl_scenario_countries(
        database_connection=database_connection, scenario_name="app_ranks"
    )
    country_codes = ranks_country_list["country_code"].str.lower().unique().tolist()
    if store == 2:
        collection_keyword = "TOP"
        for country in country_codes:
            try:
                ranked_dicts = scrape_ios_ranks(
                    collection_keyword=collection_keyword, country=country
                )
                process_scraped(
                    database_connection=database_connection,
                    ranked_dicts=ranked_dicts,
                    crawl_source="scrape_frontpage_top",
                    collections_map=collections_map,
                    categories_map=categories_map,
                    countries_map=countries_map,
                    store=2,
                )
            except Exception as e:
                logger.exception(
                    f"Srape iOS collection={collection_keyword} hit error={e}, skipping",
                )

    if store == 1:
        for country in country_codes:
            try:
                ranked_dicts = scrape_google_ranks(country=country)
                process_scraped(
                    database_connection=database_connection,
                    ranked_dicts=ranked_dicts,
                    crawl_source="scrape_frontpage_top",
                    collections_map=collections_map,
                    categories_map=categories_map,
                    countries_map=countries_map,
                    store=1,
                )
            except Exception as e:
                logger.exception(f"Scrape google ranks hit error={e}, skipping")
        try:
            dicts = get_apkcombo_android_apps()
            process_scraped(
                database_connection=database_connection,
                ranked_dicts=dicts,
                crawl_source="scrape_rss_apkcombo",
                store=1,
            )
        except Exception:
            logger.exception("ApkCombo RSS feed failed")
        try:
            dicts = get_appbrain_android_apps()
            process_scraped(
                database_connection=database_connection,
                ranked_dicts=dicts,
                crawl_source="scrape_appbrain",
                store=1,
            )
        except Exception:
            logger.exception("ApkCombo RSS feed failed")


def insert_new_apps(
    dicts: list[dict], database_connection: PostgresCon, crawl_source: str, store: int
) -> None:
    df = pd.DataFrame(dicts)
    df["store_id"].str.match(r"^[0-9].*\.")
    found_bad_ids = df[df["store"] == store, "store_id"].str.match(r"^[0-9].*\.").any()
    if found_bad_ids:
        logger.error(f"Scrape {store=} {crawl_source=} found bad store_ids")
        raise ValueError("Found bad store_ids")
    all_scraped_ids = df["store_id"].unique().tolist()
    existing_ids_map = query_store_id_map(
        database_connection,
        store_ids=all_scraped_ids,
    )
    existing_store_ids = existing_ids_map["store_id"].tolist()
    new_apps_df = df[~(df["store_id"].isin(existing_store_ids))][
        ["store", "store_id"]
    ].drop_duplicates()
    if new_apps_df.empty:
        logger.info(f"Scrape {store=} {crawl_source=} no new apps")
        return
    else:
        logger.info(
            f"Scrape {store=} {crawl_source=} insert new apps to db {new_apps_df.shape=}",
        )
        insert_columns = ["store", "store_id"]
        inserted_apps: pd.DataFrame = upsert_df(
            table_name="store_apps",
            insert_columns=insert_columns,
            df=new_apps_df,
            key_columns=insert_columns,
            database_connection=database_connection,
            return_rows=True,
        )
        if inserted_apps is not None and not inserted_apps.empty:
            logger.warning(
                f"No IDs returned for {store=} {crawl_source=} inserted apps {inserted_apps.shape=}"
            )
            inserted_apps["crawl_source"] = crawl_source
            inserted_apps = inserted_apps.rename(columns={"id": "store_app"})
            insert_columns = ["store", "store_app"]
            upsert_df(
                table_name="store_app_sources",
                insert_columns=insert_columns + ["crawl_source"],
                df=inserted_apps,
                key_columns=insert_columns,
                database_connection=database_connection,
                schema="logging",
            )


def scrape_keyword(
    country: str,
    language: str,
    keyword: str,
) -> pd.DataFrame:
    logger.info(f"{keyword=} start")
    try:
        google_apps = search_play_store(keyword, country=country, language=language)
        gdf = pd.DataFrame(google_apps)
        gdf["store"] = 1
        gdf["rank"] = range(1, len(gdf) + 1)
    except Exception:
        gdf = pd.DataFrame()
        logger.exception(f"{keyword=} google failed")
    try:
        apple_apps = search_app_store_for_ids(
            keyword, country=country, language=language
        )
        adf = pd.DataFrame(
            {
                "store": 2,
                "store_id": apple_apps,
                "rank": range(1, len(apple_apps) + 1),
            }
        )
    except Exception:
        adf = pd.DataFrame()
        logger.exception(f"{keyword=} apple failed")
    df = pd.concat([gdf, adf])
    logger.info(
        f"{keyword=} apple_apps:{adf.shape[0]} google_apps:{gdf.shape[0]} finished"
    )
    df["country"] = country
    df["crawled_date"] = datetime.datetime.now(tz=datetime.UTC).date()
    df = df[["store_id", "store", "country", "rank", "crawled_date"]]
    return df


def process_scraped(
    database_connection: PostgresCon,
    ranked_dicts: list[dict],
    crawl_source: str,
    collections_map: pd.DataFrame | None = None,
    categories_map: pd.DataFrame | None = None,
    countries_map: pd.DataFrame | None = None,
    store: int | None = None,
) -> None:
    insert_new_apps(
        database_connection=database_connection,
        dicts=ranked_dicts,
        crawl_source=crawl_source,
        store=store,
    )
    df = pd.DataFrame(ranked_dicts)
    if "rank" in df.columns:
        save_app_ranks(
            df,
            database_connection,
            store,
            collections_map,
            categories_map,
            countries_map,
        )


def save_app_ranks(
    df: pd.DataFrame,
    database_connection: PostgresCon,
    store: int | None,
    collections_map: pd.DataFrame | None = None,
    categories_map: pd.DataFrame | None = None,
    countries_map: pd.DataFrame | None = None,
) -> None:
    all_scraped_ids = df["store_id"].unique().tolist()
    new_existing_ids_map = query_store_id_map(
        database_connection,
        store_ids=all_scraped_ids,
    ).rename(columns={"id": "store_app"})
    df = pd.merge(
        df,
        new_existing_ids_map,
        how="left",
        on=["store", "store_id"],
        validate="m:1",
    )
    df["country"] = df["country"].str.upper()
    if collections_map is not None and categories_map is not None and store is not None:
        process_store_rankings(
            store=store,
            df=df,
        )
    if "keyword" in df.columns:
        df = df.drop("store_id", axis=1)
        df = (
            pd.merge(
                df,
                countries_map[["id", "alpha2"]],
                how="left",
                left_on=["country"],
                right_on="alpha2",
                validate="m:1",
            )
            .drop(["country", "alpha2"], axis=1)
            .rename(columns={"id": "country"})
        )
        process_keyword_rankings(
            df=df,
            database_connection=database_connection,
        )


def process_keyword_rankings(
    df: pd.DataFrame,
    database_connection: PostgresCon,
) -> None:
    upsert_df(
        database_connection=database_connection,
        df=df,
        table_name="app_keyword_rankings",
        key_columns=["crawled_date", "country", "lang", "keyword", "rank", "store_app"],
        insert_columns=[],
    )
    logger.info("keyword rankings inserted")


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
    bucket: str,
    snapshot_date: pd.DatetimeIndex,
    # end_date: pd.DatetimeIndex,
    store: int,
) -> list[str]:
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
        all_parquet_paths += get_parquet_paths_by_prefix(bucket, prefix)
    return all_parquet_paths


def s3_store_app_history_to_db(
    store: int,
    start_date: datetime.date,
    end_date: datetime.date,
    database_connection: PostgresCon,
) -> None:
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
        "installs",
        "store_last_updated",
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
    bucket = CONFIG["s3"]["bucket"]
    # lookback_date = snapshot_date - datetime.timedelta(days=lookback_days)
    # lookback_date_str = lookback_date.strftime("%Y-%m-%d")
    snapshot_date_str = snapshot_date.strftime("%Y-%m-%d")
    app_detail_parquets = get_s3_app_details_parquet_paths(
        bucket=bucket,
        # start_date=lookback_date,
        snapshot_date=snapshot_date,
        store=store,
    )
    if len(app_detail_parquets) == 0:
        logger.error(
            f"No app detail parquet files found for store={store} snapshot_date={snapshot_date_str}"
        )
        return
    s3_config_key = "s3"
    query = get_app_details_duck_query(
        store=store,
        app_detail_parquets=app_detail_parquets,
        # lookback_date_str=lookback_date_str,
        snapshot_date_str=snapshot_date_str,
    )
    duckdb_con = get_duckdb_connection(s3_config_key)
    duckdb_con.execute(query)
    duckdb_con.close()


def snapshot_rerun_store_app_country_history(database_connection: PostgresCon) -> None:
    start_date = datetime.date(2025, 10, 10)
    snapshot_date = datetime.date(2025, 10, 28)
    # lookback_days = 60
    for single_date in pd.date_range(start_date, snapshot_date, freq="D"):
        for store in [1, 2]:
            snapshot_date = single_date.date()
            logger.info(f"date={snapshot_date}, store={store} Make S3 agg")
            make_s3_store_app_country_history(store, snapshot_date=snapshot_date)
            logger.info(f"date={snapshot_date}, store={store} Load to db")
            s3_store_app_history_to_db(
                store,
                start_date=snapshot_date,
                end_date=snapshot_date,
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


def get_app_details_duck_query(
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
            process_parquets_and_insert(
                country_parquet_paths=country_parquet_paths,
                period=period,
                store=store,
                table_suffix=table_suffix,
                duckdb_con=duckdb_con,
                database_connection=database_connection,
            )


def process_parquets_and_insert(
    country_parquet_paths: list[str],
    period: str,
    store: int,
    table_suffix: str,
    duckdb_con,
    database_connection: PostgresCon,
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
    wdf = duckdb_con.execute(period_query).df()
    store_id_map = query_store_id_map_cached(
        store=store, database_connection=database_connection
    )
    country_map = query_countries(database_connection)
    collection_map = query_collections(database_connection)
    category_map = query_categories(database_connection)
    wdf["country"] = wdf["country"].map(country_map.set_index("alpha2")["id"].to_dict())
    wdf["store_collection"] = wdf["collection"].map(
        collection_map.set_index("collection")["id"].to_dict()
    )
    wdf["store_category"] = wdf["category"].map(
        category_map.set_index("category")["id"].to_dict()
    )
    new_ids = wdf[~wdf["store_id"].isin(store_id_map["store_id"])]["store_id"].unique()
    if len(new_ids) > 0:
        logger.info(f"Found new store ids: {len(new_ids)}")
        new_ids = [{"store": store, "store_id": store_id} for store_id in new_ids]
        process_scraped(
            database_connection=database_connection,
            ranked_dicts=new_ids,
            crawl_source="process_ranks_parquet",
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


def extract_domains(x: str) -> str:
    ext = tldextract.extract(x)
    use_top_domain = any(
        [ext.subdomain == "m", "www" in ext.subdomain.split("."), ext.subdomain == ""],
    )
    if use_top_domain:
        url = ".".join([ext.domain, ext.suffix])
    else:
        url = ".".join([ext.subdomain, ext.domain, ext.suffix])
    url = url.lower()
    return url


def crawl_developers_for_new_store_ids(
    database_connection: PostgresCon,
    store: int,
) -> None:
    logger.info(f"Crawl devevelopers for {store=} start")
    store_ids = query_store_ids(database_connection, store=store)
    df = query_developers(database_connection, store=store, limit=10000)

    if store == 1:
        developer_ids = df["developer_id"].unique().tolist()
        developer_ids = [unquote_plus(x) for x in developer_ids]
        apps_df = crawl_google_developers(developer_ids, store_ids)
        if not apps_df.empty:
            process_scraped(
                database_connection=database_connection,
                ranked_dicts=apps_df.to_dict(orient="records"),
                crawl_source="crawl_developers",
            )
        dev_df = pd.DataFrame(
            [
                {
                    "developer": _id,
                    "apps_crawled_at": datetime.datetime.now(tz=datetime.UTC),
                }
                for _id in df["id"].tolist()
            ],
        )
        insert_columns = dev_df.columns.tolist()
        key_columns = ["developer"]
        upsert_df(
            table_name="developers_crawled_at",
            schema="logging",
            insert_columns=insert_columns,
            df=dev_df,
            key_columns=key_columns,
            database_connection=database_connection,
        )
        logger.info(f"{store=} crawled, {apps_df.shape[0]} new store ids")
    if store == 2:
        for _id, row in df.iterrows():
            developer_db_id = row["id"]
            developer_id = row["developer_id"]
            row_info = f"{store=} {developer_id=}"
            logger.info(f"{row_info=} start")
            try:
                apps_df = crawl_ios_developers(developer_db_id, developer_id, store_ids)

                if not apps_df.empty:
                    process_scraped(
                        database_connection=database_connection,
                        ranked_dicts=apps_df[["store", "store_id"]].to_dict(
                            orient="records",
                        ),
                        crawl_source="crawl_developers",
                    )
                dev_df = pd.DataFrame(
                    [
                        {
                            "developer": developer_db_id,
                            "apps_crawled_at": datetime.datetime.now(
                                datetime.UTC,
                            ),
                        },
                    ],
                )
                insert_columns = dev_df.columns.tolist()
                key_columns = ["developer"]
                upsert_df(
                    table_name="developers_crawled_at",
                    schema="logging",
                    insert_columns=insert_columns,
                    df=dev_df,
                    key_columns=key_columns,
                    database_connection=database_connection,
                )
                logger.info(f"{row_info=} crawled, {apps_df.shape[0]} new store ids")
            except Exception:
                logger.exception(f"{row_info=} failed!")


def save_app_domains(
    apps_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> None:
    if "url" not in apps_df.columns or apps_df["url"].isna().all():
        logger.warning("No app urls found, finished")
        return
    urls_na = apps_df["url"].isna()
    app_urls = apps_df[~urls_na][["store_app", "url"]].drop_duplicates()
    app_urls["url"] = app_urls["url"].apply(lambda x: extract_domains(x))
    insert_columns = ["domain_name"]
    domain_ids_df = upsert_df(
        table_name="domains",
        df=app_urls.rename(columns={"url": "domain_name"}),
        insert_columns=insert_columns,
        key_columns=["domain_name"],
        database_connection=database_connection,
        return_rows=True,
    ).rename(columns={"id": "pub_domain"})
    if domain_ids_df is not None and not domain_ids_df.empty:
        app_urls = pd.merge(
            app_urls,
            domain_ids_df,
            how="left",
            left_on="url",
            right_on="domain_name",
            validate="m:1",
        )
        insert_columns = ["store_app", "pub_domain"]
        key_columns = ["store_app"]
        upsert_df(
            table_name="app_urls_map",
            insert_columns=insert_columns,
            df=app_urls,
            key_columns=key_columns,
            database_connection=database_connection,
        )
    logger.info("Finished inserting app domains")


def scrape_from_store(
    store: int,
    store_id: str,
    country: str,
    language: str,
) -> dict:
    if store == 1:
        result_dict = scrape_app_gp(store_id, country=country, language=language)
    elif store == 2:
        result_dict = scrape_app_ios(store_id, country=country, language=language)
    else:
        logger.error(f"Store not supported {store=}")
    return result_dict


def clean_scraped_df(df: pd.DataFrame, store: int) -> pd.DataFrame:
    if store == 1:
        df = clean_google_play_app_df(df)
    if store == 2:
        df = clean_ios_app_df(df)
    return df


def scrape_app(
    store: int,
    store_id: str,
    country: str,
    language: str,
) -> dict:
    scrape_info = f"{store=}, {country=}, {language=}, {store_id=}, "
    max_retries = 2
    base_delay = 1
    retries = 0
    logger.debug(f"{scrape_info} scrape start")
    while retries <= max_retries:
        retries += 1
        try:
            result_dict = scrape_from_store(
                store=store,
                store_id=store_id,
                country=country,
                language=language,
            )
            crawl_result = 1
            break  # If successful, break out of the retry loop
        except google_play_scraper.exceptions.NotFoundError:
            crawl_result = 3
            logger.warning(f"{scrape_info} failed to find app")
            break
        except AppStoreException as error:
            if "No app found" in str(error):
                crawl_result = 3
                logger.warning(f"{scrape_info} failed to find app")
            else:
                crawl_result = 4
                logger.exception(f"{scrape_info} unexpected error: {error=}")
            break
        except URLError as error:
            logger.warning(f"{scrape_info} {error=}")
            crawl_result = 4
            if retries <= max_retries:
                sleep_time = base_delay * (2**retries)
                logger.info(f"{scrape_info} Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
                continue
            else:
                logger.error(f"{scrape_info} Max retries reached. Giving up.")
                break
        except Exception as error:
            logger.error(f"{scrape_info} unexpected error: {error=}")
            crawl_result = 4
            break
    if crawl_result != 1:
        result_dict = {}
    if "kind" in result_dict.keys() and "mac" in result_dict["kind"].lower():
        logger.error(f"{scrape_info} Crawled app is Mac Software, not iOS!")
        crawl_result = 5
    result_dict["crawl_result"] = crawl_result
    result_dict["crawled_at"] = datetime.datetime.now(tz=datetime.UTC)
    result_dict["store"] = store
    result_dict["store_id"] = store_id
    result_dict["queried_language"] = language.lower()
    result_dict["country"] = country.upper()
    logger.info(f"{scrape_info} result={crawl_result} scrape finished")
    return result_dict


def save_developer_info(
    apps_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    assert apps_df["developer_id"].to_numpy()[
        0
    ], f"{apps_df['store_id']} Missing Developer ID"
    df = (
        apps_df[["store", "developer_id", "developer_name"]]
        .rename(columns={"developer_name": "name"})
        .drop_duplicates()
    )
    table_name = "developers"
    insert_columns = ["store", "developer_id", "name"]
    key_columns = ["store", "developer_id"]

    try:
        dev_df = upsert_df(
            table_name=table_name,
            df=df,
            insert_columns=insert_columns,
            key_columns=key_columns,
            database_connection=database_connection,
            return_rows=True,
        )
        apps_df = pd.merge(
            apps_df,
            dev_df.rename(columns={"id": "developer"})[
                ["store", "developer_id", "developer"]
            ],
            how="left",
            left_on=["store", "developer_id"],
            right_on=["store", "developer_id"],
            validate="m:1",
        )
    except Exception as error:
        logger.error(f"Developer insert failed with error {error}")
    return apps_df


def apps_details_to_db(
    apps_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> None:
    table_name = "store_apps"
    key_columns = ["store", "store_id"]
    if (apps_df["crawl_result"] == 1).all() and apps_df["developer_id"].notna().all():
        apps_df = save_developer_info(apps_df, database_connection)
    insert_columns = [
        x for x in get_store_app_columns(database_connection) if x in apps_df.columns
    ]
    store_apps_df = upsert_df(
        table_name=table_name,
        df=apps_df,
        insert_columns=insert_columns,
        key_columns=key_columns,
        database_connection=database_connection,
        return_rows=True,
    )
    if (
        store_apps_df is not None
        and not store_apps_df[store_apps_df["crawl_result"] == 1].empty
    ):
        store_apps_descriptions = store_apps_df[
            store_apps_df["crawl_result"] == 1
        ].copy()
        store_apps_descriptions = pd.merge(
            store_apps_descriptions,
            apps_df[
                [
                    "store_id",
                    "description",
                    "description_short",
                    "queried_language",
                    "store_language_code",
                ]
            ],
            on="store_id",
        )
        upsert_store_apps_descriptions(store_apps_descriptions, database_connection)
    if store_apps_df is not None and not store_apps_df.empty:
        store_apps_df = store_apps_df.rename(columns={"id": "store_app"})
        apps_df = pd.merge(
            apps_df,
            store_apps_df[["store_id", "store_app"]],
            how="left",
            validate="1:1",
        )
        save_app_domains(
            apps_df=apps_df,
            database_connection=database_connection,
        )
    return


def upsert_store_apps_descriptions(
    store_apps_descriptions: pd.DataFrame,
    database_connection: PostgresCon,
) -> None:
    table_name = "store_apps_descriptions"
    languages_map = query_languages(database_connection)
    store_apps_descriptions = pd.merge(
        store_apps_descriptions,
        languages_map[["id", "language_slug"]],
        how="left",
        left_on="store_language_code",
        right_on="language_slug",
        validate="m:1",
    ).rename(columns={"id": "language_id"})
    if store_apps_descriptions["language_id"].isna().any():
        null_ids = store_apps_descriptions["language_id"].isna()
        null_langs = store_apps_descriptions[
            ["store_id", "store_language_code"]
        ].drop_duplicates()
        logger.error(f"App descriptions dropping unknown language codes: {null_langs}")
        store_apps_descriptions = store_apps_descriptions[~null_ids]
        if store_apps_descriptions.empty:
            logger.debug("Dropped all descriptions, no language id found")
            return
    if "description_short" not in store_apps_descriptions.columns:
        store_apps_descriptions["description_short"] = ""
    key_columns = ["store_app", "language_id", "description", "description_short"]
    upsert_df(
        table_name=table_name,
        df=store_apps_descriptions,
        insert_columns=key_columns,
        key_columns=key_columns,
        md5_key_columns=["description", "description_short"],
        database_connection=database_connection,
        # return_rows=True,
    )
    # .rename(columns={"id": "description_id"})
    # description_df = pd.merge(
    #     description_df,
    #     store_apps_descriptions[["store_app", "name"]],
    #     how="inner",
    #     on="store_app",
    #     validate="1:1",
    # )
    # if description_df is not None and not description_df.empty:
    #     for _i, row in description_df.iterrows():
    #         description = row["description"]
    #         description_short = row["description_short"]
    #         language_id = row["language_id"]
    #         description_id = row["description_id"]
    #         app_name = row["name"]
    #         text = ". ".join([app_name, description_short, description])
    #         keywords = extract_keywords(text, database_connection=database_connection)
    #         keywords_df = pd.DataFrame(keywords, columns=["keyword_text"])
    #         keywords_df["language_id"] = language_id
    #         keywords_df["description_id"] = description_id
    #         upsert_keywords(keywords_df, database_connection)


def upsert_keywords(keywords_df: pd.DataFrame, database_connection: PostgresCon):
    table_name = "keywords"
    insert_columns = ["keyword_text"]
    key_columns = ["keyword_text"]
    upserted_keywords = upsert_df(
        table_name=table_name,
        df=keywords_df,
        insert_columns=insert_columns,
        key_columns=key_columns,
        database_connection=database_connection,
        return_rows=True,
    )
    keywords_df = pd.merge(
        keywords_df,
        upserted_keywords,
        on=["keyword_text"],
        how="left",
        validate="m:1",
    ).rename(columns={"id": "keyword_id"})
    keywords_df = keywords_df[["keyword_id", "description_id"]]
    table_name = "description_keywords"
    insert_columns = ["description_id", "keyword_id"]
    key_columns = ["description_id", "keyword_id"]
    upsert_df(
        table_name=table_name,
        df=keywords_df,
        insert_columns=insert_columns,
        key_columns=key_columns,
        database_connection=database_connection,
    )


def log_crawl_results(
    app_df: pd.DataFrame, store: int, database_connection: PostgresCon
) -> None:
    store_id_map = query_store_id_map_cached(
        store=store, database_connection=database_connection
    )
    country_map = query_countries(database_connection)
    app_df["country_id"] = app_df["country"].map(
        country_map.set_index("alpha2")["id"].to_dict()
    )
    app_df["store_app"] = app_df["store_id"].map(
        store_id_map.set_index("store_id")["id"].to_dict()
    )
    insert_columns = [
        "crawl_result",
        "store_app",
        "country_id",
        "crawled_at",
    ]
    app_df = app_df[insert_columns]
    app_df.to_sql(
        schema="logging",
        name="store_app_country_crawl",
        con=database_connection.engine,
        if_exists="append",
        index=False,
    )


def process_app_icon(store_id: str, url: str) -> str | None:
    # Fetch image
    f_name = None
    try:
        response = requests.get(url, timeout=10)
    except Exception:
        logger.error(f"Failed to fetch image from {url}")
        return None
    img = Image.open(BytesIO(response.content))
    ext = response.headers["Content-Type"].split("/")[1]
    # Resize to 100x100
    img_resized = img.resize((100, 100), Image.LANCZOS)
    phash = str(imagehash.phash(img_resized))
    f_name = f"{phash}.{ext}"
    file_path = pathlib.Path(APP_ICONS_TMP_DIR, f"{phash}.{ext}")
    # Save as PNG
    img_resized.save(file_path, format="PNG")
    # Upload to S3
    image_format = "image/" + ext
    s3_key = "digi-cloud"
    s3_client = get_s3_client(s3_key)
    response = s3_client.put_object(
        Bucket=CONFIG[s3_key]["bucket"],
        Key=f"app-icons/{store_id}/{f_name}",
        ACL="public-read",
        Body=file_path.read_bytes(),
        ContentType=image_format,
    )
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        logger.info(f"S3 uploaded {store_id} app icon")
    else:
        logger.error(f"S3 failed to upload {store_id} app icon")
    return f_name
