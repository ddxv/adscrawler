import datetime
import pathlib
import random
import ssl
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from io import BytesIO
from urllib.error import URLError
from urllib.parse import unquote_plus

import google_play_scraper
import imagehash
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
from adscrawler.app_stores.process_from_s3 import (
    app_details_to_s3,
    process_store_rankings,
    raw_keywords_to_s3,
)
from adscrawler.app_stores.utils import check_and_insert_new_apps
from adscrawler.config import APP_ICONS_TMP_DIR, CONFIG, get_logger
from adscrawler.dbcon.connection import (
    PostgresCon,
    get_db_connection,
)
from adscrawler.dbcon.queries import (
    get_crawl_scenario_countries,
    get_store_app_columns,
    prepare_for_psycopg,
    query_all_developers,
    query_all_domains,
    query_categories,
    query_collections,
    query_countries,
    query_developers,
    query_keywords_to_crawl,
    query_languages,
    query_store_apps_to_update,
    query_store_id_map,
    query_store_ids,
    update_from_df,
    upsert_df,
)
from adscrawler.packages.storage import get_s3_client

logger = get_logger(__name__, "scrape_stores")


def _scrape_single_app(
    row: pd.Series,
    store: int,
    process_icon: bool,
    chunk_info: str,
    use_thread_jitter: bool,
) -> dict | None:
    """Helper function to scrape a single app - used by ThreadPoolExecutor."""
    if use_thread_jitter:
        # Add small random jitter to avoid SSL connection conflicts
        time.sleep(random.uniform(0.05, 0.2))

    try:
        result = scrape_app(
            store=store,
            store_id=row["store_id"],
            country=row["country_code"].lower(),
            language=row["language"].lower(),
            html_last_scraped_at=row.get("html_last_scraped_at", None),
        )
        result["store_app_db_id"] = row["store_app"]
        if process_icon:
            result["icon_url_100"] = row.get("icon_url_100", None)
        return result
    except Exception as e:
        logger.exception(
            f"{chunk_info} store_id={row['store_id']} scrape_app failed: {e}"
        )
        return None


def process_scrape_apps_and_save(
    df_chunk: pd.DataFrame,
    store: int,
    use_ssh_tunnel: bool,
    process_icon: bool,
    thread_workers: int,
    total_rows: int | None = None,
) -> None:
    """Process a chunk of apps, scrape app, store to S3 and if country === US store app details to db store_apps table.

    Args:
        df_chunk: DataFrame of apps to process, needs to have columns: store_id, country_code, language, icon_url_100
        store: Store ID
        use_ssh_tunnel: Whether to use SSH tunnel
        process_icon: Whether to process app icons
        thread_workers: Number of threads to use for parallel scraping within this process
        total_rows: Total number of apps in the chunk, if None, will be calculated from df_chunk
    """
    if total_rows is None:
        total_rows = len(df_chunk)
    chunk_info = f"{store=} process_scrape_apps_and_save chunk={df_chunk.index[0]}-{df_chunk.index[-1]}/{total_rows}"

    # Store 1 (Google Play) uses sequential processing to avoid SSL issues
    # Store 2 (Apple) can use threading
    use_threading = store == 2 and thread_workers > 1

    if use_threading:
        logger.info(f"{chunk_info} start with {thread_workers} threads")
    else:
        logger.info(f"{chunk_info} start (sequential)")

    database_connection = get_db_connection(use_ssh_tunnel=use_ssh_tunnel)
    chunk_results = []

    try:
        if use_threading:
            # Threading approach for Apple App Store
            use_thread_jitter = True
            with ThreadPoolExecutor(max_workers=thread_workers) as executor:
                # Submit all scraping tasks
                future_to_row = {
                    executor.submit(
                        _scrape_single_app,
                        row,
                        store,
                        process_icon,
                        chunk_info,
                        use_thread_jitter,
                    ): idx
                    for idx, row in df_chunk.iterrows()
                }

                # Collect results as they complete
                for future in as_completed(future_to_row):
                    try:
                        result = future.result()
                        if result is not None:
                            chunk_results.append(result)
                        # Add slight jitter so threads don't pick up next task simultaneously
                        time.sleep(random.uniform(0.01, 0.05))
                    except Exception as e:
                        row_idx = future_to_row[future]
                        logger.exception(
                            f"{chunk_info} row_idx={row_idx} thread processing failed: {e}"
                        )
        else:
            # Sequential approach for Google Play Store (avoids SSL EOF errors)
            for _, row in df_chunk.iterrows():
                try:
                    result = scrape_app(
                        store=store,
                        store_id=row["store_id"],
                        country=row["country_code"].lower(),
                        language=row["language"].lower(),
                    )
                    result["store_app_db_id"] = row["store_app"]
                    if process_icon:
                        result["icon_url_100"] = row.get("icon_url_100", None)
                    chunk_results.append(result)
                except Exception as e:
                    logger.exception(
                        f"{chunk_info} store_id={row['store_id']} scrape_app failed: {e}"
                    )

        if not chunk_results:
            logger.warning(f"{chunk_info} produced no results.")
            return
        results_df = pd.DataFrame(chunk_results)
        results_df["crawled_date"] = results_df["crawled_at"].dt.date
        app_details_to_s3(results_df, store=store)
        results_df["store_app"] = results_df["store_app_db_id"].astype(int)
        log_crawl_results(results_df, database_connection=database_connection)
        results_df = results_df[(results_df["country"] == "US")]
        process_live_app_details(
            store=store,
            results_df=results_df,
            database_connection=database_connection,
            process_icon=process_icon,
        )
        logger.info(f"{chunk_info} finished")
    finally:
        if database_connection and hasattr(database_connection, "engine"):
            database_connection.engine.dispose()
            logger.debug(f"{chunk_info} database connection disposed")


def update_app_details(
    database_connection: PostgresCon,
    store: int,
    use_ssh_tunnel: bool,
    workers: int,
    process_icon: bool,
    limit: int,
    country_priority_group: int,
) -> None:
    """Process apps with dynamic work queue

    Args:
        database_connection: Database connection
        store: Store ID
        use_ssh_tunnel: Whether to use SSH tunnel
        workers: Number of processes to use
        process_icon: Whether to process app icons
        limit: Limit on number of apps to process
        country_priority_group: Country priority group
    """
    log_info = f"{store=} group={country_priority_group} update_app_details"

    # Store 1 (Google Play): No threading due to urllib SSL issues
    # Store 2 (Apple): Use threading as it has slower response times
    if store == 1:
        thread_workers = 1
    elif store == 2:
        thread_workers = 3
    else:
        thread_workers = 1

    df = query_store_apps_to_update(
        store=store,
        database_connection=database_connection,
        limit=limit,
        country_priority_group=country_priority_group,
    )
    df = df.sort_values("country_code").reset_index(drop=True)
    if df.empty:
        logger.info(f"{log_info} no apps to update")
        return
    logger.info(f"{log_info} start apps={len(df)}")

    # Keep chunk size large for efficient S3 parquet files
    # For store 1 (sequential), processes provide parallelism
    # For store 2 (threading), threads within chunks provide parallelism
    max_chunk_size = 3000
    chunks = []
    # Try keeping countries together for larger end S3 files
    for _country, country_df in df.groupby("country_code"):
        country_size = len(country_df)
        if country_size <= max_chunk_size:
            # Country fits in one chunk
            chunks.append(country_df)
        else:
            # Split large country into multiple chunks
            num_chunks = (country_size + max_chunk_size - 1) // max_chunk_size
            chunk_size = country_size // num_chunks
            for i in range(0, country_size, chunk_size):
                chunks.append(country_df.iloc[i : i + chunk_size])
    total_chunks = len(chunks)
    total_rows = len(df)

    if thread_workers > 1:
        logger.info(
            f"{log_info} processing {total_rows} apps in {total_chunks} chunks "
            f"({workers} processes × {thread_workers} threads = {workers * thread_workers} concurrent)"
        )
    else:
        logger.info(
            f"{log_info} processing {total_rows} apps in {total_chunks} chunks "
            f"({workers} processes, sequential per process)"
        )

    completed_count = 0
    failed_count = 0

    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Submit all chunks, but stagger the first wave to avoid API bursts
        future_to_idx = {}
        for idx, df_chunk in enumerate(chunks):
            future = executor.submit(
                process_scrape_apps_and_save,
                df_chunk,
                store,
                use_ssh_tunnel,
                process_icon,
                total_rows,
                thread_workers,
            )
            future_to_idx[future] = idx
            # Only stagger the initial batch to avoid simultaneous API burst
            if idx <= workers:
                time.sleep(0.5)  # 500ms between initial worker starts
        logger.info(f"{log_info} all {total_chunks} chunks submitted")
        # Process results as they complete
        for future in as_completed(future_to_idx):
            chunk_idx = future_to_idx[future]
            try:
                _result = future.result()
                completed_count += 1
                logger.info(
                    f"{log_info} finished: {completed_count}/{total_chunks} failed: {failed_count}"
                )
            except Exception as e:
                failed_count += 1
                logger.exception(f"Chunk {chunk_idx} failed: {e}")
    logger.info(f"{log_info} completed={completed_count} failed={failed_count}")


def crawl_keyword_ranks(database_connection: PostgresCon) -> None:
    country = "us"
    language = "en"
    kdf = query_keywords_to_crawl(database_connection, limit=1000)
    all_keywords = pd.DataFrame()
    for _id, row in kdf.iterrows():
        logger.info(
            f"Crawling keywords: {_id}/{kdf.shape[0]} keyword={row.keyword_text}"
        )
        keyword = row.keyword_text
        try:
            df = scrape_keyword(
                country=country,
                language=language,
                keyword=keyword,
            )
            df["keyword_text"] = keyword
            df["keyword_id"] = row["keyword_id"]
            df["language"] = language.lower()
            df["country"] = country.upper()
            df["crawled_at"] = datetime.datetime.now(tz=datetime.UTC)
            df["crawled_date"] = df["crawled_at"].dt.date
            all_keywords = pd.concat([all_keywords, df], ignore_index=True)
        except Exception:
            logger.exception(f"Scrape keyword={keyword} hit error, skipping")
    raw_keywords_to_s3(all_keywords)
    all_keywords = all_keywords.rename(columns={"keyword_id": "keyword"})
    key_columns = ["keyword"]
    upsert_df(
        table_name="keywords_crawled_at",
        schema="logging",
        insert_columns=["keyword", "crawled_at"],
        df=all_keywords[["keyword", "crawled_at"]],
        key_columns=key_columns,
        database_connection=database_connection,
    )


def scrape_store_ranks(database_connection: PostgresCon, store: int) -> None:
    collections_map = query_collections(database_connection)
    categories_map = query_categories(database_connection)
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
                    store=2,
                )
            except Exception as e:
                logger.exception(
                    f"Srape iOS collection={collection_keyword} {country=} hit error={e}, skipping",
                )

    if store == 1:
        for country in country_codes:
            try:
                ranked_dicts = scrape_google_ranks(country=country)
                if len(ranked_dicts) > 0:
                    process_scraped(
                        database_connection=database_connection,
                        ranked_dicts=ranked_dicts,
                        crawl_source="scrape_frontpage_top",
                        collections_map=collections_map,
                        categories_map=categories_map,
                        store=1,
                    )
                else:
                    logger.warning(
                        f"Scrape google ranks {country=} produced no results, skipping"
                    )
            except Exception as e:
                logger.exception(
                    f"Scrape google ranks {country=} hit error={e}, skipping"
                )
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


def scrape_keyword(
    country: str,
    language: str,
    keyword: str,
) -> pd.DataFrame:
    logger.info(f"{keyword=} start")
    retry_delay = 0.5
    retry_delays = (0, retry_delay, 1.0)
    try:
        google_apps = None
        last_google_error = None
        for delay in retry_delays:
            try:
                if delay:
                    time.sleep(delay)
                google_apps = search_play_store(
                    keyword, country=country, language=language
                )
                break
            except Exception as exc:
                last_google_error = exc
        if google_apps is None and last_google_error is not None:
            raise last_google_error
        gdf = pd.DataFrame(google_apps)
        gdf["store"] = 1
        gdf["rank"] = range(1, len(gdf) + 1)
    except Exception:
        gdf = pd.DataFrame()
        logger.exception(f"{keyword=} google failed")
    try:
        apple_apps = None
        last_apple_error = None
        for delay in retry_delays:
            try:
                if delay:
                    time.sleep(delay)
                apple_apps = search_app_store_for_ids(
                    keyword, country=country, language=language
                )
                break
            except Exception as exc:
                last_apple_error = exc
        if apple_apps is None and last_apple_error is not None:
            raise last_apple_error
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
    df = df[["store", "store_id", "rank"]]
    return df


def process_scraped(
    database_connection: PostgresCon,
    ranked_dicts: list[dict],
    crawl_source: str,
    store: int,
    collections_map: pd.DataFrame | None = None,
    categories_map: pd.DataFrame | None = None,
) -> None:
    check_and_insert_new_apps(
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
        )


def save_app_ranks(
    df: pd.DataFrame,
    database_connection: PostgresCon,
    store: int,
    collections_map: pd.DataFrame | None = None,
    categories_map: pd.DataFrame | None = None,
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
            check_and_insert_new_apps(
                database_connection=database_connection,
                dicts=apps_df.to_dict(orient="records"),
                crawl_source="crawl_developers",
                store=store,
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
                    check_and_insert_new_apps(
                        database_connection=database_connection,
                        dicts=apps_df[["store", "store_id"]].to_dict(orient="records"),
                        crawl_source="crawl_developers",
                        store=store,
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


def check_and_insert_developers(
    developers_df: pd.DataFrame,
    apps_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    """Adds missing developers to the database and returns updated developer DataFrame."""
    missing_devs = apps_df[
        (~apps_df["developer_id"].isin(developers_df["developer_id"]))
        & (apps_df["developer_id"].notna())
    ]
    if not missing_devs.empty:
        new_devs = missing_devs[
            ["store", "developer_id", "developer_name"]
        ].drop_duplicates()
        new_devs = upsert_df(
            table_name="developers",
            df=new_devs.rename(columns={"developer_name": "name"}),
            insert_columns=["store", "developer_id", "name"],
            key_columns=["store", "developer_id"],
            database_connection=database_connection,
            return_rows=True,
        )
        developers_df = pd.concat([new_devs, developers_df])
    return developers_df


def check_and_insert_domains(
    domains_df: pd.DataFrame,
    app_urls: pd.DataFrame,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    """Adds missing ad domains to the database and returns updated domain DataFrame."""
    missing_ad_domains = app_urls[
        (~app_urls["url"].isin(domains_df["domain_name"])) & (app_urls["url"].notna())
    ]
    if not missing_ad_domains.empty:
        new_ad_domains = (
            missing_ad_domains[["url"]]
            .drop_duplicates()
            .rename(columns={"url": "domain_name"})
        )
        new_ad_domains = upsert_df(
            table_name="domains",
            df=new_ad_domains,
            insert_columns=["domain_name"],
            key_columns=["domain_name"],
            database_connection=database_connection,
            return_rows=True,
        )
        domains_df = pd.concat([new_ad_domains, domains_df])
    return domains_df


def save_app_domains(
    apps_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> None:
    if "url" not in apps_df.columns or apps_df["url"].isna().all():
        logger.warning("No app urls found, finished")
        return
    urls_na = apps_df["url"].isna()
    app_urls = apps_df[~urls_na][["store_app", "url"]].drop_duplicates()
    if app_urls.empty:
        logger.warning("No app urls found")
        return
    app_urls["url"] = app_urls["url"].apply(lambda x: extract_domains(x))
    all_domains_df = query_all_domains(database_connection=database_connection)
    all_domains_df = check_and_insert_domains(
        domains_df=all_domains_df,
        app_urls=app_urls,
        database_connection=database_connection,
    )
    domain_ids_df = app_urls.merge(
        all_domains_df.rename(columns={"id": "pub_domain"}),
        left_on="url",
        right_on="domain_name",
        how="left",
        validate="m:1",
    )
    if not domain_ids_df.empty:
        insert_columns = ["store_app", "pub_domain"]
        key_columns = ["store_app"]
        upsert_df(
            table_name="app_urls_map",
            insert_columns=insert_columns,
            df=domain_ids_df[["store_app", "pub_domain"]],
            key_columns=key_columns,
            database_connection=database_connection,
        )
    logger.info("Finished inserting app domains")


def scrape_from_store(
    store: int,
    store_id: str,
    country: str,
    language: str,
    html_last_scraped_at: datetime.datetime | None = None,
) -> dict:
    if store == 1:
        result_dict = scrape_app_gp(store_id, country=country, language=language)
    elif store == 2:
        scrape_html = False
        if country == "us" and (
            html_last_scraped_at is None
            or html_last_scraped_at
            < datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(days=30)
        ):
            scrape_html = True
        result_dict = scrape_app_ios(
            store_id, country=country, language=language, scrape_html=scrape_html
        )
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
    html_last_scraped_at: datetime.datetime | None = None,
) -> dict:
    scrape_info = f"{store=}, {country=}, {language=}, {store_id=} scrape_app"
    max_retries = 2
    base_delay = 0.5
    retries = 0
    logger.debug(f"{scrape_info} start")
    # Satisfy mypy
    crawl_result = 0
    while retries <= max_retries:
        retries += 1
        try:
            result_dict = scrape_from_store(
                store=store,
                store_id=store_id,
                country=country,
                language=language,
                html_last_scraped_at=html_last_scraped_at,
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
        except (URLError, ssl.SSLError, requests.exceptions.SSLError) as error:
            logger.warning(f"{scrape_info} Network/SSL error: {error=}")
            crawl_result = 4
            if retries <= max_retries:
                # Add extra jitter for SSL errors to avoid connection conflicts
                sleep_time = base_delay * (2**retries) + random.uniform(0.1, 0.5)
                logger.info(f"{scrape_info} Retrying in {sleep_time:.2f} seconds...")
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
    logger.info(f"{scrape_info} {crawl_result=} finished")
    return result_dict


def save_developer_info(
    apps_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    all_developers_df = query_all_developers(database_connection=database_connection)
    all_developers_df = check_and_insert_developers(
        developers_df=all_developers_df,
        apps_df=apps_df,
        database_connection=database_connection,
    )
    apps_df = pd.merge(
        apps_df,
        all_developers_df.rename(columns={"id": "developer"})[
            ["store", "developer_id", "developer"]
        ],
        how="left",
        left_on=["store", "developer_id"],
        right_on=["store", "developer_id"],
        validate="m:1",
    )
    return apps_df


def process_live_app_details(
    store: int,
    results_df: pd.DataFrame,
    database_connection: PostgresCon,
    process_icon: bool,
) -> None:
    for crawl_result, apps_df in results_df.groupby("crawl_result"):
        logger.info(f"{store=} {crawl_result=} processing {len(apps_df)} apps for db")
        if crawl_result != 1:
            # If bad crawl result, only save minimal info to avoid overwriting good data, ie name
            apps_df = apps_df[["store_id", "store", "crawled_at", "crawl_result"]]
        else:
            apps_df = clean_scraped_df(df=apps_df, store=store)
            if "description_short" not in apps_df.columns:
                apps_df["description_short"] = ""
            apps_df.loc[apps_df["description_short"].isna(), "description_short"] = ""
            if process_icon:
                try:
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
                    logger.exception("failed to process app icon")
        apps_details_to_db(
            apps_df=apps_df,
            database_connection=database_connection,
            crawl_result=crawl_result,
        )


def apps_details_to_db(
    apps_df: pd.DataFrame,
    database_connection: PostgresCon,
    crawl_result: int,
) -> None:
    key_columns = ["store", "store_id"]
    if (apps_df["crawl_result"] == 1).all() and apps_df["developer_id"].notna().all():
        apps_df = save_developer_info(apps_df, database_connection)
    insert_columns = [
        x for x in get_store_app_columns(database_connection) if x in apps_df.columns
    ]
    apps_df = prepare_for_psycopg(apps_df)
    logger.info(f"{crawl_result=} update store_apps table for {len(apps_df)} apps")
    update_from_df(
        table_name="store_apps",
        df=apps_df,
        update_columns=insert_columns,
        key_columns=key_columns,
        database_connection=database_connection,
    )
    if apps_df is None or apps_df.empty or crawl_result != 1:
        return
    upsert_store_apps_descriptions(apps_df, database_connection)
    save_app_domains(
        apps_df=apps_df,
        database_connection=database_connection,
    )
    return


def upsert_store_apps_descriptions(
    apps_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> None:
    table_name = "store_apps_descriptions"
    languages_map = query_languages(database_connection)
    apps_df = pd.merge(
        apps_df,
        languages_map[["id", "language_slug"]],
        how="left",
        left_on="store_language_code",
        right_on="language_slug",
        validate="m:1",
    ).rename(columns={"id": "language_id"})
    if apps_df["language_id"].isna().any():
        null_ids = apps_df["language_id"].isna()
        null_langs = apps_df[null_ids][
            ["store_id", "store_language_code"]
        ].drop_duplicates()
        logger.error(f"App descriptions dropping unknown language codes: {null_langs}")
        apps_df = apps_df[~null_ids]
        if apps_df.empty:
            logger.debug("Dropped all descriptions, no language id found")
            return
    if "description_short" not in apps_df.columns:
        apps_df["description_short"] = ""
    key_columns = ["store_app", "language_id", "description", "description_short"]
    upsert_df(
        table_name=table_name,
        df=apps_df,
        insert_columns=key_columns,
        key_columns=key_columns,
        md5_key_columns=["description", "description_short"],
        database_connection=database_connection,
        on_conflict_update=False,
    )


def log_crawl_results(app_df: pd.DataFrame, database_connection: PostgresCon) -> None:
    country_map = query_countries(database_connection)
    app_df["country_id"] = app_df["country"].map(
        country_map.set_index("alpha2")["id"].to_dict()
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
        name="app_country_crawls",
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
