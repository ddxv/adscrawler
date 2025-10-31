import datetime
import pathlib
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
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
)
from adscrawler.app_stores.utils import insert_new_apps
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
from adscrawler.packages.storage import get_s3_client

logger = get_logger(__name__, "scrape_stores")


def process_chunk(
    df_chunk: pd.DataFrame,
    store: int,
    use_ssh_tunnel: bool,
    process_icon: bool,
    total_rows: int,
):
    chunk_info = f"Chunk {df_chunk.index[0]}-{df_chunk.index[-1]}/{total_rows}"
    logger.info(f"{chunk_info} start")
    database_connection = get_db_connection(use_ssh_tunnel=use_ssh_tunnel)
    try:
        chunk_results = []
        for _, row in df_chunk.iterrows():
            try:
                result_dict = scrape_app(
                    store=store,
                    store_id=row["store_id"],
                    country=row["country_code"].lower(),
                    language=row["language"].lower(),
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
        results_df = results_df[(results_df["country"] == "US")]
        process_live_app_details(
            store=store,
            results_df=results_df,
            database_connection=database_connection,
            process_icon=process_icon,
            df_chunk=df_chunk,
        )
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
        country_crawl_priority=country_crawl_priority,
    )
    df = df.sort_values("country_code").reset_index(drop=True)
    logger.info(f"{log_info} start {len(df)} apps")

    max_chunk_size = 10000
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
    logger.info(f"Processing {total_rows} apps in {total_chunks} chunks")

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
            #  _countries_map = query_countries(database_connection)
            # process_scraped(
            #     database_connection=database_connection,
            #     ranked_dicts=df.to_dict(orient="records"),
            #     crawl_source="scrape_keywords",
            #     countries_map=countries_map,
            # )
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
            insert_new_apps(
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
                    insert_new_apps(
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


def process_live_app_details(
    store: int,
    results_df: pd.DataFrame,
    database_connection: PostgresCon,
    process_icon: bool,
    df_chunk: pd.DataFrame,
) -> None:
    for crawl_result, apps_df in results_df.groupby("crawl_result"):
        if crawl_result != 1:
            apps_df = apps_df[["store_id", "store", "crawled_at", "crawl_result"]]
        else:
            apps_df = clean_scraped_df(df=apps_df, store=store)
            if "description_short" not in apps_df.columns:
                apps_df["description_short"] = ""
            apps_df.loc[apps_df["description_short"].isna(), "description_short"] = ""
            if process_icon:
                try:
                    apps_df = pd.merge(
                        apps_df,
                        df_chunk[["store_id", "icon_url_100"]],
                        on="store_id",
                        how="inner",
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
                    logger.exception("failed to process app icon")
        apps_df = apps_df.convert_dtypes(dtype_backend="pyarrow")
        apps_df = apps_df.replace({pd.NA: None})
        apps_details_to_db(
            apps_df=apps_df,
            database_connection=database_connection,
        )


def apps_details_to_db(
    apps_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> None:
    key_columns = ["store", "store_id"]
    if (apps_df["crawl_result"] == 1).all() and apps_df["developer_id"].notna().all():
        apps_df = save_developer_info(apps_df, database_connection)
    insert_columns = [
        x for x in get_store_app_columns(database_connection) if x in apps_df.columns
    ]
    # Update columns we always want the latest of
    # Eg name, developer_id
    store_apps_df = upsert_df(
        table_name="store_apps",
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
