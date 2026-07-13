import datetime
import random
import ssl
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from urllib.error import URLError
from urllib.parse import unquote_plus

import appgoblin_play_scraper
from appgoblin_play_scraper.exceptions import ExtraHTTPError
import pandas as pd
import requests
from appgoblin_itunes_scraper.exceptions import (
    AppStoreException,
    NotFoundError,
    TemporaryBlockException,
)

from adscrawler.app_stores.apkcombo import get_apkcombo_android_apps
from adscrawler.app_stores.appbrain import get_appbrain_android_apps
from adscrawler.app_stores.apple import (
    clean_ios_app_df,
    crawl_apple_developers,
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
from adscrawler.app_stores.utils import (
    build_country_map,
    check_and_insert_new_apps,
    extract_domains_with_sub,
    extract_root_domain,
    resolve_country_id,
)
from adscrawler.app_stores.process_icons import process_app_icon
from adscrawler.config import get_logger
from adscrawler.dbcon.connection import (
    PostgresEngine,
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
from adscrawler.process.app_details import (
    app_details_to_s3,
    raw_keywords_to_s3,
)
from adscrawler.process.app_rankings import process_store_rankings
from adscrawler.process.storage import rankings_parquet_exists_in_s3
from prometheus_client import Counter

logger = get_logger(__name__, "scrape_stores")


CRAWL_RESULTS_COUNTER = Counter(
    name="app_crawl_results_total",
    documentation="Total number of app crawls processed by store and outcome",
    labelnames=["store", "crawl_result"],
)


def process_scrape_apps_and_save(
    df_chunk: pd.DataFrame,
    store: int,
    process_icon: bool,
    total_rows: int | None = None,
    do_pg_update: bool = False,
) -> None:
    """Process a chunk of apps, scrape app, store to S3 and if country === US store app details to db store_apps table.

    Args:
        df_chunk: DataFrame of apps to process, needs to have columns: store_id, country_code, language, icon_url_100
        store: Store ID
        process_icon: Whether to process app icons
        total_rows: Total number of apps in the chunk, if None, will be calculated from df_chunk
    """
    if total_rows is None:
        total_rows = len(df_chunk)
    chunk_info = f"{store=} process_scrape_apps_and_save {total_rows=}"
    logger.info(f"{chunk_info} start")
    pgdb = get_db_connection()
    chunk_results = []
    try:
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
        log_crawl_results(results_df, pgdb=pgdb, store=store)
        logger.info(f"{chunk_info} S3 and logging upload finished")
        if do_pg_update:
            results_df = results_df[(results_df["country"] == "US")]
            process_live_app_details(
                store=store,
                results_df=results_df,
                pgdb=pgdb,
                process_icon=process_icon,
            )
        logger.info(f"{chunk_info} finished")
    finally:
        if pgdb and hasattr(pgdb, "engine"):
            pgdb.engine.dispose()
            logger.debug(f"{chunk_info} database connection disposed")


def update_app_details(
    pgdb: PostgresEngine,
    store: int,
    workers: int,
    process_icon: bool,
    limit: int,
    country_priority_group: int,
) -> None:
    """Process apps with dynamic work queue

    Args:
        pgdb: Database connection
        store: Store ID
        workers: Number of processes to use
        process_icon: Whether to process app icons
        limit: Limit on number of apps to process
        country_priority_group: Country priority group
    """
    log_info = f"{store=} group={country_priority_group} update_app_details"

    df = query_store_apps_to_update(
        store=store,
        pgdb=pgdb,
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

    logger.info(
        f"{log_info} processing {total_rows} apps in {total_chunks} chunks "
        f"({workers} processes, sequential per process)"
    )

    completed_count = 0
    failed_count = 0

    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_idx = {}
        for idx, df_chunk in enumerate(chunks):
            future = executor.submit(
                process_scrape_apps_and_save,
                df_chunk=df_chunk,
                store=store,
                process_icon=process_icon,
                total_rows=total_rows,
                do_pg_update=True,
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


def crawl_keyword_ranks(pgdb: PostgresEngine) -> None:
    country = "us"
    language = "en"
    kdf = query_keywords_to_crawl(pgdb, limit=500)
    all_keywords = pd.DataFrame()
    crawl_log = []
    consecutive_errors = 0
    for _id, row in kdf.iterrows():
        logger.info(
            f"Crawling keywords: {_id}/{kdf.shape[0]:,} keyword={row.keyword_text}"
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
            consecutive_errors = 0
        except Exception:
            consecutive_errors += 1
            logger.exception(f"Scrape keyword={keyword} hit error, skipping")
        finally:
            crawl_log.append(
                {
                    "keyword_id": row["keyword_id"],
                    "crawled_at": datetime.datetime.now(tz=datetime.UTC),
                }
            )
        # Backoff: 1–3 seconds depending on recent errors
        backoff = min(1.0 + consecutive_errors * 0.5, 3.0)
        sleep_time = random.uniform(backoff - 0.25, backoff + 0.25)
        time.sleep(sleep_time)
    raw_keywords_to_s3(all_keywords)
    if crawl_log:
        crawl_log_df = pd.DataFrame(crawl_log).rename(columns={"keyword_id": "keyword"})
        upsert_df(
            table_name="keywords_crawled_at",
            schema="logging",
            insert_columns=["keyword", "crawled_at"],
            df=crawl_log_df[["keyword", "crawled_at"]],
            key_columns=["keyword"],
            pgdb=pgdb,
        )


def scrape_store_ranks(pgdb: PostgresEngine, store: int) -> None:
    collections_map = query_collections(pgdb)
    categories_map = query_categories(pgdb)
    collections_map = collections_map.rename(columns={"id": "store_collection"})
    categories_map = categories_map.rename(columns={"id": "store_category"})
    all_countries = get_crawl_scenario_countries(pgdb=pgdb, scenario_name="app_ranks")
    all_country_codes = all_countries["country_code"].str.lower().unique().tolist()
    today = datetime.date.today().strftime("%Y-%m-%d")

    if store == 2:
        collection_keyword = "TOP"
        for country in all_country_codes:
            if rankings_parquet_exists_in_s3(
                store=store, crawled_date=today, country=country
            ):
                logger.info(f"Skipping iOS ranks {country=}, already uploaded today")
                continue
            try:
                ranked_dicts = scrape_ios_ranks(
                    collection_keyword=collection_keyword, country=country
                )
                if len(ranked_dicts) > 0:
                    process_scraped(
                        pgdb=pgdb,
                        ranked_dicts=ranked_dicts,
                        crawl_source="scrape_frontpage_top",
                        collections_map=collections_map,
                        categories_map=categories_map,
                        store=2,
                    )
                else:
                    logger.warning(
                        f"Scrape iOS ranks {country=} produced no results, skipping"
                    )
            except Exception as e:
                logger.exception(
                    f"Srape iOS collection={collection_keyword} {country=} hit error={e}, skipping",
                )

    if store == 1:
        for country in all_country_codes:
            if rankings_parquet_exists_in_s3(
                store=store, crawled_date=today, country=country
            ):
                logger.info(f"Skipping Google ranks {country=}, already uploaded today")
                continue
            try:
                ranked_dicts = scrape_google_ranks(country=country)
                if len(ranked_dicts) > 0:
                    process_scraped(
                        pgdb=pgdb,
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
                pgdb=pgdb,
                ranked_dicts=dicts,
                crawl_source="scrape_rss_apkcombo",
                store=1,
            )
        except Exception:
            logger.exception("ApkCombo RSS feed failed")
        try:
            dicts = get_appbrain_android_apps()
            process_scraped(
                pgdb=pgdb,
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
        f"{keyword=} apple_apps:{adf.shape[0]:,} google_apps:{gdf.shape[0]:,} finished"
    )
    if df.empty:
        return pd.DataFrame(columns=["store", "store_id", "rank"])
    df = df[["store", "store_id", "rank"]]
    return df


def process_scraped(
    pgdb: PostgresEngine,
    ranked_dicts: list[dict],
    crawl_source: str,
    store: int,
    collections_map: pd.DataFrame | None = None,
    categories_map: pd.DataFrame | None = None,
) -> None:
    check_and_insert_new_apps(
        pgdb=pgdb,
        dicts=ranked_dicts,
        crawl_source=crawl_source,
        store=store,
    )
    df = pd.DataFrame(ranked_dicts)
    if "rank" in df.columns:
        save_app_ranks(
            df,
            pgdb,
            store,
            collections_map,
            categories_map,
        )


def save_app_ranks(
    df: pd.DataFrame,
    pgdb: PostgresEngine,
    store: int,
    collections_map: pd.DataFrame | None = None,
    categories_map: pd.DataFrame | None = None,
) -> None:
    all_scraped_ids = df["store_id"].unique().tolist()
    new_existing_ids_map = query_store_id_map(
        pgdb,
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


def crawl_developers_for_new_store_ids(
    pgdb: PostgresEngine,
    store: int,
) -> None:
    logger.info(f"Crawl devevelopers for {store=} start")
    store_ids = query_store_ids(pgdb, store=store)
    df = query_developers(pgdb, store=store, limit=10000)
    if store == 1:
        developer_ids = df["developer_id"].unique().tolist()
        developer_ids = [unquote_plus(x) for x in developer_ids]
        apps_df = crawl_google_developers(developer_ids, store_ids)
        db_ids_crawled = df["id"].tolist()
    elif store == 2:
        apps_df, db_ids_crawled = crawl_apple_developers(df, store_ids)

    if not apps_df.empty:
        check_and_insert_new_apps(
            pgdb=pgdb,
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
            for _id in db_ids_crawled
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
        pgdb=pgdb,
    )
    logger.info(f"{store=} crawled, {apps_df.shape[0]:,} new store ids")


def check_and_insert_developers(
    developers_df: pd.DataFrame,
    apps_df: pd.DataFrame,
    pgdb: PostgresEngine,
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
            pgdb=pgdb,
            return_rows=True,
        )
        developers_df = pd.concat([new_devs, developers_df])
    return developers_df


def check_and_insert_domains(
    domains_df: pd.DataFrame,
    apps_df: pd.DataFrame,
    pgdb: PostgresEngine,
) -> pd.DataFrame:
    """Adds missing ad domains to the database and returns updated domain DataFrame.

    For URLs with subdomains, ensures the root domain exists first and links
    the subdomain entry to it via ``root_domain_id``.
    """

    # --- 1. Insert any missing root domains first ---
    root_urls = (
        apps_df[["root_url"]]
        .drop_duplicates()
        .rename(columns={"root_url": "domain_name"})
    )
    root_urls = root_urls[root_urls["domain_name"].notna()]

    missing_roots = root_urls[~root_urls["domain_name"].isin(domains_df["domain_name"])]
    if not missing_roots.empty:
        new_roots = upsert_df(
            table_name="domains",
            df=missing_roots,
            insert_columns=["domain_name"],
            key_columns=["domain_name"],
            pgdb=pgdb,
            return_rows=True,
        )
        domains_df = pd.concat([new_roots, domains_df], ignore_index=True)

    # --- 2. Build domain_name -> id lookup (now includes newly inserted roots) ---
    domain_id_map = dict(zip(domains_df["domain_name"], domains_df["id"]))

    # --- 3. Backfill root_domain_id on existing subdomain entries that lack it ---
    stale_subs = domains_df[
        domains_df["root_domain_id"].isna() & domains_df["domain_name"].notna()
    ].copy()
    if not stale_subs.empty:
        stale_subs["root_domain"] = stale_subs["domain_name"].apply(extract_root_domain)
        stale_subs = stale_subs[
            (stale_subs["root_domain"].notna())
            & (stale_subs["root_domain"] != stale_subs["domain_name"])
        ]
        stale_subs["root_domain_id"] = stale_subs["root_domain"].map(domain_id_map)
        to_update = stale_subs[stale_subs["root_domain_id"].notna()][
            ["domain_name", "root_domain_id"]
        ]
        if not to_update.empty:
            upsert_df(
                table_name="domains",
                df=to_update,
                insert_columns=["domain_name", "root_domain_id"],
                key_columns=["domain_name"],
                pgdb=pgdb,
                on_conflict_update=True,
            )
            # Refresh in-memory copy with updated root_domain_ids
            domains_df = query_all_domains(pgdb=pgdb)
            domain_id_map = dict(zip(domains_df["domain_name"], domains_df["id"]))
            logger.info(
                f"Backfilled root_domain_id for {len(to_update)} existing subdomain entries"
            )

    # --- 4. Insert subdomain URLs with root_domain_id ---
    missing_subdomains = apps_df[
        (~apps_df["url"].isin(domains_df["domain_name"]))
        & (apps_df["url"].notna())
        & (apps_df["root_url"].notna())
        & (apps_df["url"] != apps_df["root_url"])
    ]
    if not missing_subdomains.empty:
        subs_to_insert = (
            missing_subdomains[["url", "root_url"]]
            .drop_duplicates()
            .rename(columns={"url": "domain_name"})
        )
        subs_to_insert["root_domain_id"] = subs_to_insert["root_url"].map(domain_id_map)
        subs_to_insert = subs_to_insert.drop(columns=["root_url"])

        new_subs = upsert_df(
            table_name="domains",
            df=subs_to_insert,
            insert_columns=["domain_name", "root_domain_id"],
            key_columns=["domain_name"],
            pgdb=pgdb,
            return_rows=True,
        )
        domains_df = pd.concat([new_subs, domains_df], ignore_index=True)

    return domains_df


def save_app_domains(
    apps_df: pd.DataFrame,
    pgdb: PostgresEngine,
) -> None:
    apps_df["url"] = apps_df["url"].apply(extract_domains_with_sub)
    apps_df["root_url"] = apps_df["url"].apply(extract_root_domain)
    # Drop IPs / malfromed urls
    apps_df = apps_df[~apps_df["root_url"].isna()]
    # This would mean that urls are frozen if 'removed' but more likely they failed a crawl
    apps_df = apps_df[~apps_df["url"].isna()]
    all_domains_df = query_all_domains(pgdb=pgdb)
    all_domains_df = check_and_insert_domains(
        domains_df=all_domains_df,
        apps_df=apps_df,
        pgdb=pgdb,
    )
    domain_ids_df = apps_df.merge(
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
            pgdb=pgdb,
        )
    logger.info("Finished inserting app domains")


def scrape_from_store(
    store: int,
    store_id: str,
    country: str,
    language: str,
    html_recently_scraped: bool | None = None,
) -> dict:
    if store == 1:
        result_dict = scrape_app_gp(store_id, country=country, language=language)
    elif store == 2:
        scrape_html = False
        # Watch for pd.NaT
        if country == "us" and not html_recently_scraped:
            scrape_html = True
        result_dict = scrape_app_ios(
            store_id, country=country, language=language, scrape_html=scrape_html
        )
    else:
        logger.error(f"Store not supported {store=}")
    return result_dict


def clean_scraped_df(df: pd.DataFrame, store: int, process_icon: bool) -> pd.DataFrame:
    if store == 1:
        df = clean_google_play_app_df(df)
    if store == 2:
        df = clean_ios_app_df(df)
    if "description_short" not in df.columns:
        df["description_short"] = ""
    df.loc[df["description_short"].isna(), "description_short"] = ""
    if process_icon:
        try:
            no_icon = df["icon_url_100"].isna()
            if df[no_icon].empty:
                pass
            else:
                icon_tuples = df.loc[no_icon].apply(
                    lambda x: process_app_icon(x["store_id"], x["icon_url_512"]),
                    axis=1,
                )
                # Drop rows where icon generation failed, then extract the
                # individual filenames into icon_url_100 (legacy), icon_128 and icon_64
                icon_tuples = icon_tuples.dropna()
                idx = icon_tuples.index
                df.loc[idx, "icon_url_100"] = icon_tuples.apply(lambda t: t[0])
                df.loc[idx, "icon_128"] = icon_tuples.apply(lambda t: t[0])
                df.loc[idx, "icon_64"] = icon_tuples.apply(lambda t: t[1])
        except Exception:
            logger.warning("failed to process app icon")
    return df


def scrape_app(
    store: int,
    store_id: str,
    country: str,
    language: str,
    html_recently_scraped: bool | None = None,
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
                html_recently_scraped=html_recently_scraped,
            )
            crawl_result = 1
            break  # If successful, break out of the retry loop
        except (
            appgoblin_play_scraper.exceptions.NotFoundError,
            NotFoundError,
        ):
            crawl_result = 3
            logger.warning(f"{scrape_info} failed to find app")
            break
        except (ExtraHTTPError, TemporaryBlockException) as error:
            logger.warning(
                f"{scrape_info} HTTP error / temporary block (rate limit / server error): {error=}"
            )
            crawl_result = 4
            if retries <= max_retries:
                # Add extra jitter for rate-limit errors to avoid conflicts
                sleep_time = base_delay * (2**retries) + random.uniform(0.5, 1.5)
                logger.info(f"{scrape_info} Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
                continue
            else:
                logger.error(
                    f"{scrape_info} Max retries reached for HTTP error. Giving up."
                )
                break
        except AppStoreException as error:
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
        if store == 2:
            result_dict["additional_html_crawl_result"] = 2
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
    pgdb: PostgresEngine,
) -> pd.DataFrame:
    all_developers_df = query_all_developers(pgdb=pgdb)
    all_developers_df = check_and_insert_developers(
        developers_df=all_developers_df,
        apps_df=apps_df,
        pgdb=pgdb,
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
    pgdb: PostgresEngine,
    process_icon: bool,
) -> None:
    if store == 1:
        results_df["additional_html_crawl_result"] = 0
    for (crawl_result, additional_html_crawl_result), apps_df in results_df.groupby(
        ["crawl_result", "additional_html_crawl_result"]
    ):
        num_apps = apps_df.shape[0]
        log_info = (
            f"{store=} {crawl_result=} {additional_html_crawl_result=} {num_apps=}"
        )
        logger.info(f"{log_info} process for db start")
        if crawl_result != 1:
            # If bad crawl result, only save minimal info to avoid overwriting good data, ie name
            apps_df = apps_df[["store_id", "store", "crawled_at", "crawl_result"]]
        else:
            logger.info(f"{log_info} clean df")
            apps_df = clean_scraped_df(
                df=apps_df, store=store, process_icon=process_icon
            )

        if additional_html_crawl_result != 1:
            # If additional html crawl failed, drop fields that rely on it to avoid overwriting good data with nulls
            if store == 2:
                cols_to_drop = [
                    "additional_html_crawled_at",
                    "ad_supported",
                    "in_app_purchases",
                    "url",
                ]
            else:
                cols_to_drop = ["additional_html_crawled_at"]
            apps_df = apps_df.drop(
                cols_to_drop,
                axis=1,
                errors="ignore",
            )
        key_columns = ["store", "store_id"]

        if (apps_df["crawl_result"] == 1).all() and apps_df[
            "developer_id"
        ].notna().all():
            logger.info(f"{log_info} update devs")
            apps_df = save_developer_info(apps_df, pgdb)
        insert_columns = [
            x for x in get_store_app_columns(pgdb) if x in apps_df.columns
        ]
        apps_df = prepare_for_psycopg(apps_df)

        logger.info(f"{log_info} update store_apps table")

        update_from_df(
            table_name="store_apps",
            df=apps_df,
            update_columns=insert_columns,
            key_columns=key_columns,
            pgdb=pgdb,
        )
        if apps_df is None or apps_df.empty or crawl_result != 1:
            continue
        logger.info(f"{log_info} update descriptions table")
        upsert_store_apps_descriptions(apps_df, pgdb)
        if store == 1:
            logger.info(f"{log_info} update countries table")
            upsert_app_country_evidence(apps_df, pgdb)
        if "url" not in apps_df.columns or apps_df["url"].isna().all():
            logger.warning(f"{log_info} No app urls found")
            continue
        urls_na = apps_df["url"].isna()
        app_urls = apps_df[~urls_na][["store_app", "url"]].drop_duplicates()
        if app_urls.empty:
            logger.warning(f"{log_info} No app urls found")
            continue
        logger.info(f"{log_info} update domains table")
        save_app_domains(
            apps_df=apps_df,
            pgdb=pgdb,
        )


def upsert_store_apps_descriptions(
    apps_df: pd.DataFrame,
    pgdb: PostgresEngine,
) -> None:
    table_name = "store_apps_descriptions"
    languages_map = query_languages(pgdb)
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
        pgdb=pgdb,
        on_conflict_update=False,
    )


def upsert_app_country_evidence(
    apps_df: pd.DataFrame,
    pgdb: PostgresEngine,
) -> None:
    """Guess country from developer_address / developer_legal_address and
    upsert into app_country_evidence.

    Prefers ``developer_address`` over ``developer_legal_address`` but takes
    whichever resolves first.  Rows without any address are silently skipped.
    """
    if apps_df.empty:
        return

    # Build country maps once
    country_id_map, name_to_alpha2 = build_country_map(pgdb)

    dev_addr = apps_df["developer_address"].astype(str).str.strip()
    legal_addr = apps_df["developer_legal_address"].astype(str).str.strip()

    # Replace empty strings/nan-strings with actual NaN so .fillna works
    dev_addr = dev_addr.replace(["", "nan", "None"], pd.NA)
    legal_addr = legal_addr.replace(["", "nan", "None"], pd.NA)

    apps_df["raw_address"] = dev_addr.fillna(legal_addr)

    # Drop rows that ended up with no address at all
    apps_df = apps_df.dropna(subset=["raw_address"])

    def get_country_id(raw_address: str) -> int | None:
        country_id = resolve_country_id(raw_address, country_id_map, name_to_alpha2)
        return country_id

    apps_df["country_id"] = apps_df["raw_address"].apply(get_country_id)

    evidence_df = apps_df[["store_app", "raw_address", "country_id"]]

    if evidence_df.empty:
        return

    key_columns = ["store_app"]
    insert_columns = ["store_app", "raw_address", "country_id"]
    evidence_df = evidence_df[evidence_df["raw_address"].notna()]
    evidence_df["country_id"] = evidence_df["country_id"].astype("Int64")
    evidence_df = evidence_df.replace({pd.NA: None})
    upsert_df(
        table_name="app_country_evidence",
        df=evidence_df,
        insert_columns=insert_columns,
        key_columns=key_columns,
        pgdb=pgdb,
        on_conflict_update=True,
    )
    logger.info(
        f"Upserted {len(evidence_df)} app_country_evidence rows "
        f"({evidence_df['country_id'].notna().sum()} resolved)"
    )


def log_crawl_results(app_df: pd.DataFrame, pgdb: PostgresEngine, store: int) -> None:
    country_map = query_countries(pgdb)
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
        con=pgdb.engine,
        if_exists="append",
        index=False,
    )
    counts = app_df["crawl_result"].value_counts()
    for result_type, count in counts.items():
        CRAWL_RESULTS_COUNTER.labels(store=store, crawl_result=str(result_type)).inc(
            int(count)
        )
