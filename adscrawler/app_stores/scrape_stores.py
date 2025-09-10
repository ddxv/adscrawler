import datetime
import os
import pathlib
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.error import URLError
from urllib.parse import unquote_plus

import duckdb
import google_play_scraper
import pandas as pd
import tldextract
from itunes_app_scraper.util import AppStoreException

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
from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import PostgresCon, get_db_connection
from adscrawler.dbcon.queries import (
    get_store_app_columns,
    query_categories,
    query_collections,
    query_countries,
    query_developers,
    query_keywords_to_crawl,
    query_languages,
    query_store_apps,
    query_store_id_map,
    query_store_ids,
    upsert_df,
)
from adscrawler.packages.storage import get_s3_client, get_s3_endpoint

logger = get_logger(__name__, "scrape_stores")


COUNTRY_LIST = [
    "us",
    "cn",
    "de",
    "in",
    "ca",
    "gb",
    "au",
    "fr",
    "es",
    "it",
    "kr",
    "jp",
    "ru",
    "mx",
    "nl",
    "pl",
    "pt",
    "sa",
    "tr",
    "ua",
    "za",
    "br",
    "be",
    "ch",
    "cz",
    "dk",
    "gr",
    "hu",
    "id",
    "ie",
    "il",
    "my",
    "no",
    "nz",
    "ph",
    "ro",
    "se",
    "sg",
    "vn",
    "pk",
    "ng",
    "bd",
    "et",
    "eg",
    "cd",
    "ir",
    "th",
    "tz",
    "mm",
    "ke",
    "co",
    "ar",
    "ug",
    "dz",
    "sd",
    "iq",
    "af",
    "ma",
    "uz",
    "pe",
    "ao",
    "gh",
    "mz",
    "ve",
    "ye",
    "np",
    "mg",
    "kp",
    "cm",
    "ci",
    "tw",
    "ne",
    "lk",
    "bf",
    "ml",
    "cl",
    "kz",
    "mw",
    "zm",
    "gt",
    "ec",
    "sy",
    "kh",
    "sn",
    "td",
    "so",
    "zw",
    "gn",
    "rw",
    "tn",
    "bj",
    "bo",
    "cu",
    "bi",
    "ht",
    "do",
    "jo",
    "az",
    "ae",
    "hn",
    "by",
    "tj",
    "at",
    "pg",
    "ss",
    "tg",
    "sl",
    "hk",
    "la",
    "bg",
    "rs",
    "lb",
    "ly",
    "ni",
    "sv",
    "kg",
    "er",
    "tm",
    "fi",
    "sk",
    "cg",
    "cr",
    "om",
    "lr",
    "cf",
    "ps",
    "mr",
    "pa",
    "kw",
    "hr",
    "ge",
    "md",
    "uy",
    "ba",
    "pr",
    "mn",
    "am",
    "jm",
    "al",
    "lt",
    "qa",
    "na",
    "gm",
    "bw",
    "ga",
    "ls",
    "mk",
    "si",
]


def process_chunk(df_chunk, use_ssh_tunnel):
    database_connection = get_db_connection(use_ssh_tunnel=use_ssh_tunnel)
    logger.info(
        f"Processing chunk {df_chunk.index[0]}-{df_chunk.index[-1]} ({len(df_chunk)} apps)"
    )
    try:
        for _, row in df_chunk.iterrows():
            try:
                update_all_app_info(
                    row.store,
                    row.store_id,
                    database_connection,
                )
            except Exception as e:
                logger.exception(
                    f"store={row.store}, store_id={row.store_id} update_all_app_info failed with {e}"
                )
    except Exception as e:
        logger.exception(
            f"Error processing chunk {df_chunk.index[0]}-{df_chunk.index[-1]} with {e}"
        )
    finally:
        logger.info(f"Finished chunk {df_chunk.index[0]}-{df_chunk.index[-1]}")
        if database_connection and hasattr(database_connection, "engine"):
            database_connection.engine.dispose()
            logger.debug(
                f"Disposed database engine for chunk {df_chunk.index[0]}-{df_chunk.index[-1]}"
            )


def update_app_details(database_connection, stores, use_ssh_tunnel, workers, limit):
    log_info = f"Update app details: {stores=}"
    df = query_store_apps(stores, database_connection, limit=limit, log_query=False)
    logger.info(f"{log_info} start {len(df)} apps")

    chunk_size = 1000
    chunks = [df.iloc[i : i + chunk_size] for i in range(0, len(df), chunk_size)]

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(process_chunk, chunk, use_ssh_tunnel) for chunk in chunks
        ]
        for f in futures:
            f.result()

    logger.info(f"{log_info} finished")


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


def scrape_store_ranks(database_connection: PostgresCon, stores: list[int]) -> None:
    collections_map = query_collections(database_connection)
    categories_map = query_categories(database_connection)
    countries_map = query_countries(database_connection)
    collections_map = collections_map.rename(columns={"id": "store_collection"})
    categories_map = categories_map.rename(columns={"id": "store_category"})
    if 2 in stores:
        collection_keyword = "TOP"
        for country in COUNTRY_LIST:
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
        try:
            process_weekly_ranks(
                store=2,
                start_date=datetime.date.today() - datetime.timedelta(days=15),
                end_date=datetime.date.today(),
                database_connection=database_connection,
            )
        except Exception:
            logger.exception("Weekly ranks failed")

    if 1 in stores:
        for country in COUNTRY_LIST:
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
        try:
            process_weekly_ranks(
                store=1,
                start_date=datetime.date.today() - datetime.timedelta(days=15),
                end_date=datetime.date.today(),
                database_connection=database_connection,
            )
        except Exception:
            logger.exception("Weekly ranks failed")


def insert_new_apps(
    dicts: list[dict], database_connection: PostgresCon, crawl_source: str, store: int
) -> None:
    df = pd.DataFrame(dicts)
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
    store: int,
    collections_map: pd.DataFrame | None = None,
    categories_map: pd.DataFrame | None = None,
    countries_map: pd.DataFrame | None = None,
):
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
    if collections_map is not None and categories_map is not None:
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


def map_database_ids(
    df: pd.DataFrame,
    database_connection: PostgresCon,
    store_id_map: pd.DataFrame,
    store: int,
) -> pd.DataFrame:
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
        logger.info(f"New store ids: {new_ids}")
        new_ids = [{"store": store, "store_id": store_id} for store_id in new_ids]
        process_scraped(
            database_connection=database_connection,
            ranked_dicts=new_ids,
            crawl_source="process_ranks_parquet",
        )
        store_id_map = query_store_id_map(database_connection, store)
    df["store_app"] = df["store_id"].map(
        store_id_map.set_index("store_id")["id"].to_dict()
    )
    return df


def process_weekly_ranks(
    store: int,
    start_date: datetime.date,
    end_date: datetime.date,
    database_connection: PostgresCon,
) -> None:
    key_name = "s3"
    bucket = CONFIG[key_name]["bucket"]
    s3_region = CONFIG[key_name]["region_name"]
    # DuckDB uses S3 endpoint url
    endpoint = get_s3_endpoint(key_name)
    # DuckDB may
    store_id_map = query_store_id_map(database_connection, store)
    duckdb_con = duckdb.connect()
    if "http://" in endpoint:
        duckdb_con.execute("SET s3_use_ssl=false;")
        endpoint = endpoint.replace("http://", "")
    elif "https://" in endpoint:
        endpoint = endpoint.replace("https://", "")
    duckdb_con.execute("INSTALL httpfs; LOAD httpfs;")
    duckdb_con.execute(f"SET s3_region='{s3_region}';")
    duckdb_con.execute(f"SET s3_endpoint='{endpoint}';")
    duckdb_con.execute("SET s3_url_style='path';")
    duckdb_con.execute("SET s3_url_compatibility_mode=true;")
    duckdb_con.execute(f"SET s3_access_key_id='{CONFIG[key_name]['access_key_id']}';")
    duckdb_con.execute(f"SET s3_secret_access_key='{CONFIG[key_name]['secret_key']}';")
    duckdb_con.execute("SET temp_directory = '/tmp/duckdb.tmp/';")
    duckdb_con.execute("SET preserve_insertion_order = false;")

    s3 = get_s3_client()
    for dt in pd.date_range(start_date, end_date, freq="W-MON"):
        week_str = dt.strftime("%Y-%m-%d")
        logger.info(f"Processing store={store} week_start={week_str}")

        all_parquet_paths = []
        for ddt in pd.date_range(dt, dt + datetime.timedelta(days=6), freq="D"):
            ddt_str = ddt.strftime("%Y-%m-%d")
            prefix = (
                f"raw-data/app_rankings/store={store}/crawled_date={ddt_str}/country="
            )
            objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            parquet_paths = [
                f"s3://{bucket}/{obj['Key']}"
                for obj in objects.get("Contents", [])
                if obj["Key"].endswith("rankings.parquet")
            ]
            all_parquet_paths += parquet_paths
        if len(all_parquet_paths) == 0:
            logger.info(f"No parquet files found for week_start={week_str}")
            continue

        week_query = f"""WITH all_data AS (
            SELECT * FROM read_parquet({all_parquet_paths})
             ),
         weekly_max_dates AS (
                      SELECT ar_1.country,
                         ar_1.collection,
                         ar_1.category,
                         date_trunc('week'::text, ar_1.crawled_date::timestamp with time zone) AS week_start,
                         max(ar_1.crawled_date) AS max_crawled_date
                        FROM all_data ar_1
                       GROUP BY ar_1.country, ar_1.collection, ar_1.category, (date_trunc('week'::text, ar_1.crawled_date::timestamp with time zone))
                     ),
         weekly_app_ranks as (SELECT ar.rank,
            min(ar.rank) AS best_rank,
            ar.country,
            ar.collection,
            ar.category,
            ar.crawled_date,
            ar.store_id
           FROM all_data ar
             JOIN weekly_max_dates wmd ON ar.country = wmd.country 
             AND ar.collection = wmd.collection 
             AND ar.category = wmd.category 
             AND ar.crawled_date = wmd.max_crawled_date
             GROUP BY ar.rank, ar.country, ar.collection, ar.category, ar.crawled_date, ar.store_id
             )
        SELECT war.rank, war.best_rank, war.country, war.collection, war.category, war.crawled_date, war.store_id FROM weekly_app_ranks war
          ORDER BY war.country, war.collection, war.category, war.crawled_date, war.rank
        """

        wdf = duckdb_con.execute(week_query).df()

        wdf = map_database_ids(wdf, database_connection, store_id_map, store)
        wdf = wdf.drop(columns=["store_id", "collection", "category"])

        upsert_df(
            df=wdf,
            table_name="store_app_ranks_weekly",
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


def update_all_app_info(
    store: int,
    store_id: str,
    database_connection: PostgresCon,
) -> None:
    info = f"{store=} {store_id=}"
    app_df = scrape_and_save_app(store, store_id, database_connection)
    if "store_app" not in app_df.columns:
        logger.error(f"{info} store_app db id not in app_df columns")
        return
    if app_df["crawl_result"].to_numpy()[0] != 1:
        logger.info(f"{info} crawl not successful, don't update further")
        return
    if "url" not in app_df.columns or not app_df["url"].to_numpy():
        logger.info(f"{info} no app url, finished")
        return

    app_df["url"] = app_df["url"].apply(lambda x: extract_domains(x))

    insert_columns = ["url"]
    app_urls_df = upsert_df(
        table_name="pub_domains",
        df=app_df,
        insert_columns=insert_columns,
        key_columns=["url"],
        database_connection=database_connection,
        return_rows=True,
    )
    if app_urls_df is not None and not app_urls_df.empty:
        app_df["pub_domain"] = app_urls_df["id"].astype(object)[0]
        insert_columns = ["store_app", "pub_domain"]
        key_columns = ["store_app"]
        upsert_df(
            table_name="app_urls_map",
            insert_columns=insert_columns,
            df=app_df,
            key_columns=key_columns,
            database_connection=database_connection,
        )
    logger.info(f"{info} finished inserting pub_domains")


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
) -> pd.DataFrame:
    scrape_info = f"{store=}, {store_id=}, {country=}, {language=}"
    max_retries = 2
    base_delay = 1
    retries = 0

    logger.info(f"{scrape_info} scrape start")
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
    result_dict["store"] = store
    result_dict["store_id"] = store_id
    result_dict["queried_language"] = language.lower()
    result_dict["country"] = country.upper()

    df = pd.DataFrame([result_dict])
    if crawl_result == 1:
        df = clean_scraped_df(df=df, store=store)

    if "description_short" not in df.columns:
        df["description_short"] = ""
    df.loc[df["description_short"].isna(), "description_short"] = ""

    logger.info(f"{scrape_info} scrape finished")
    return df


def save_developer_info(
    app_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    assert app_df["developer_id"].to_numpy()[0], (
        f"{app_df['store_id']} Missing Developer ID"
    )
    df = (
        app_df[["store", "developer_id", "developer_name"]]
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
        assert dev_df is not None, "Dev_df is none!"
        app_df["developer"] = dev_df["id"].astype(object)[0]
    except Exception as error:
        logger.error(f"Developer insert failed with error {error}")
    return app_df


def scrape_and_save_app(
    store: int,
    store_id: str,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    # Pulling for more countries will want to track rating, review count, and histogram
    app_country_list = ["us"]
    # Pulling for more languages to track descriptions, reviews, titles, etc.
    app_language_list = ["en"]
    for country in app_country_list:
        for language in app_language_list:
            info = f"{store=}, {store_id=}, {country=}, {language=}"
            app_df = scrape_app(
                store=store,
                store_id=store_id,
                country=country,
                language=language,
            )
            logger.info(f"{info} save to db start")
            app_df = save_apps_df(
                apps_df=app_df,
                database_connection=database_connection,
            )
            logger.info(f"{info} save to db finish")
    crawl_result = int(app_df["crawl_result"].to_numpy()[0])
    logger.info(f"{info} {crawl_result=} scraped and saved app")
    return app_df


def save_apps_df(
    apps_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> pd.DataFrame:
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
        store_apps_df = store_apps_df.rename(columns={"id": "store_app"})
        store_apps_history = store_apps_df[store_apps_df["crawl_result"] == 1].copy()
        store_apps_history = pd.merge(
            store_apps_history,
            apps_df[["store_id", "histogram", "country"]],
            on="store_id",
        )
        upsert_store_apps_history(store_apps_history, database_connection)
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
    log_crawl_results(apps_df, database_connection=database_connection)
    return apps_df


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
        logger.error(
            f"Store apps descriptions language id is NA {store_apps_descriptions[['store_id', 'store_language_code']]}"
        )
        store_apps_descriptions = store_apps_descriptions[
            ~store_apps_descriptions["language_id"].isna()
        ]
        if store_apps_descriptions.empty:
            logger.error("Dropped all descriptions, no language id found")
            return

    if "description_short" not in store_apps_descriptions.columns:
        store_apps_descriptions["description_short"] = ""
    key_columns = ["store_app", "language_id", "description", "description_short"]
    description_df = upsert_df(
        table_name=table_name,
        df=store_apps_descriptions,
        insert_columns=key_columns,
        key_columns=key_columns,
        database_connection=database_connection,
        return_rows=True,
    ).rename(columns={"id": "description_id"})
    description_df = pd.merge(
        description_df,
        store_apps_descriptions[["store_app", "name"]],
        how="left",
        on="store_app",
        validate="1:1",
    )
    if description_df is not None and not description_df.empty:
        for _i, row in description_df.iterrows():
            description = row["description"]
            description_short = row["description_short"]
            language_id = row["language_id"]
            description_id = row["description_id"]
            app_name = row["name"]
            text = ". ".join([app_name, description_short, description])
            # keywords = extract_keywords(text, database_connection=database_connection)
            # keywords_df = pd.DataFrame(keywords, columns=["keyword_text"])
            # keywords_df["language_id"] = language_id
            # keywords_df["description_id"] = description_id
            # upsert_keywords(keywords_df, database_connection)


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


def insert_global_keywords(database_connection: PostgresCon) -> None:
    """Insert global keywords into the database.
    NOTE: This takes about ~5-8GB of RAM for 50k keywords and 200k descriptions. For now run manually.
    """
    from adscrawler.tools.extract_keywords import get_global_keywords  # noqa: PLC0415

    global_keywords = get_global_keywords(database_connection)
    global_keywords_df = pd.DataFrame(global_keywords, columns=["keyword_text"])
    table_name = "keywords"
    insert_columns = ["keyword_text"]
    key_columns = ["keyword_text"]
    keywords_df = upsert_df(
        table_name=table_name,
        df=global_keywords_df,
        insert_columns=insert_columns,
        key_columns=key_columns,
        database_connection=database_connection,
        return_rows=True,
    )
    keywords_df = keywords_df.rename(columns={"id": "keyword_id"})
    keywords_df = keywords_df[["keyword_id"]]
    table_name = "keywords_base"
    insert_columns = ["keyword_id"]
    key_columns = ["keyword_id"]
    keywords_df.to_sql(
        name=table_name,
        con=database_connection.engine,
        if_exists="replace",
        index=False,
        schema="public",
    )


def upsert_store_apps_history(
    store_apps_history: pd.DataFrame, database_connection: PostgresCon
) -> None:
    store_apps_history["crawled_date"] = (
        datetime.datetime.now(datetime.UTC).date().strftime("%Y-%m-%d")
    )
    table_name = "store_apps_country_history"
    countries_map = query_countries(database_connection)
    store_apps_history = pd.merge(
        store_apps_history,
        countries_map[["id", "alpha2"]],
        how="left",
        left_on="country",
        right_on="alpha2",
        validate="m:1",
    ).rename(columns={"id": "country_id"})
    insert_columns = [
        "installs",
        "review_count",
        "rating",
        "rating_count",
        "histogram",
    ]
    key_columns = ["store_app", "country_id", "crawled_date"]
    upsert_df(
        table_name=table_name,
        df=store_apps_history,
        insert_columns=insert_columns,
        key_columns=key_columns,
        database_connection=database_connection,
    )


def log_crawl_results(app_df: pd.DataFrame, database_connection: PostgresCon) -> None:
    app_df["crawled_at"] = datetime.datetime.now(datetime.UTC)
    insert_columns = ["crawl_result", "store", "store_id", "store_app", "crawled_at"]
    app_df = app_df[insert_columns]
    app_df.to_sql(
        name="store_apps_crawl",
        schema="logging",
        con=database_connection.engine,
        if_exists="append",
    )
