from dbcon.queries import insert_get, upsert_df, query_store_apps, query_pub_domains
from dbcon.connection import get_db_connection
from itunes_app_scraper.util import (
    AppStoreCollections,
    AppStoreCategories,
    AppStoreException,
)
from itunes_app_scraper.scraper import AppStoreScraper
import google_play_scraper
from config import get_logger
import pandas as pd
import tldextract
import argparse
import requests
import csv
import io
import os

logger = get_logger(__name__)


def js_update_ids_file(filepath: str) -> None:
    if os.path.exists(filepath):
        os.remove(filepath)
    os.system("node pullAppIds.js")
    logger.info("Js pull finished")


def get_js_ids(filepath: str) -> list[str]:
    with open(filepath, mode="r") as file:
        ids = file.readlines()
    ids = [x.replace("\n", "") for x in ids]
    return ids


def scrape_gp_for_app_ids():
    logger.info("Scrape GP frontpage for new apps start")
    filepath = "/tmp/googleplay_ids.txt"
    try:
        js_update_ids_file(filepath)
    except Exception as error:
        logger.warning(f"JS pull failed with {error=}")
    ids = get_js_ids(filepath)
    ids = list(set(ids))
    df = pd.DataFrame({"store": 1, "store_id": ids})
    insert_columns = ["store", "store_id"]
    logger.info(f"Scrape GP frontpage for new apps: insert to db {df.shape=}")
    upsert_df(
        "store_apps",
        insert_columns=insert_columns,
        df=df,
        key_columns=insert_columns,
        database_connection=PGCON,
    )
    logger.info("Scrape GP frontpage for new apps finished")


def script_has_process() -> bool:
    already_running = False
    processes = [x for x in os.popen("ps aux")]
    my_processes = [
        x for x in processes if "app_ads_crawler.py" in x and "/bin/sh" not in x
    ]
    if len(my_processes) > 1:
        logger.warning(f"Already running {my_processes}")
        already_running = True
    return already_running


def request_app_ads(ads_url: str) -> requests.Response:
    if not "http" == ads_url[0:4]:
        ads_url = "http://" + ads_url
    response = requests.get(ads_url, timeout=2)
    if response.status_code == 403:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0)",
        }
        response = requests.get(ads_url, headers=headers, timeout=2)
    if response.status_code != 200:
        err = f"{ads_url} status_code: {response.status_code}"
        raise NoAdsTxt(err)
    if "<head>" in response.text:
        err = f"{ads_url} HTML in adstxt"
        raise NoAdsTxt(err)
    if not any(term in response.text.upper() for term in ["DIRECT", "RESELLER"]):
        err = "DIRECT, RESELLER not in ads.txt"
        raise NoAdsTxt(err)
    return response


class NoAdsTxt(Exception):
    pass


class AdsTxtEmpty(Exception):
    pass


def get_app_ads_text(app_url: str) -> str:
    ext_url = tldextract.extract(app_url)
    sub_domains_url = ""
    if ext_url.subdomain:
        # Note contains all subdomains, even m
        # TODO parse & exclude for m subdomain
        sub_domains_url = ".".join(ext_url) + "/" + "app-ads.txt"
        sub_domains_url = "http://" + sub_domains_url
    top_domain_url = ".".join([ext_url.domain, ext_url.suffix]) + "/" + "app-ads.txt"
    top_domain_url = "http://" + top_domain_url
    if sub_domains_url:
        try:
            response = request_app_ads(ads_url=sub_domains_url)
            return response.text
        except NoAdsTxt as error:
            logger.error(f"{error}")
    response = request_app_ads(ads_url=top_domain_url)
    return response.text


def parse_ads_txt(txt: str) -> pd.DataFrame:
    txt = txt.replace(" ", "")
    csv_header = [
        "domain",
        "publisher_id",
        "relationship",
        "certification_auth",
        "notes",
    ]
    rows = []
    input_stream_lines = txt.split("\n")
    output_stream = ""
    for line in input_stream_lines:
        if not line or line[0] == "#":
            continue
        else:
            output_stream = output_stream + line + "\n"
    for row in csv.DictReader(
        io.StringIO(output_stream),
        delimiter=",",
        fieldnames=csv_header[:-1],
        restkey=csv_header[-1],
        quoting=csv.QUOTE_NONE,
    ):
        try:
            if len(row) == 4:
                rows.append(
                    [
                        row["domain"],
                        row["publisher_id"],
                        row["relationship"],
                        row["certification_auth"],
                    ]
                )
            elif len(row) > 4:
                rows.append(
                    [
                        row["domain"],
                        row["publisher_id"],
                        row["relationship"],
                        row["certification_auth"],
                        ",".join(row["notes"]),
                    ]
                )
            else:
                rows.append([row["domain"], row["publisher_id"], row["relationship"]])
        except Exception as err:
            logger.error(f"Parser skipping row: {row}, error: {err}")
            continue
    if pd.DataFrame(rows).shape[1] == len(csv_header) - 1:
        df = pd.DataFrame(rows, columns=csv_header[:-1])
    else:
        df = pd.DataFrame(rows, columns=csv_header)
    return df


def clean_raw_txt_df(txt_df: pd.DataFrame) -> pd.DataFrame:
    # Domain
    txt_df["domain"] = txt_df["domain"].str.lower()
    txt_df["domain"] = txt_df["domain"].apply(
        lambda x: ".".join([tldextract.extract(x).domain, tldextract.extract(x).suffix])
    )
    standard_str_cols = ["domain", "publisher_id", "relationship", "certification_auth"]
    txt_df[standard_str_cols] = txt_df[standard_str_cols].replace(
        "[^a-zA-Z0-9_\\-\\.]", "", regex=True
    )
    # Clean Relationship
    txt_df["relationship"] = txt_df["relationship"].str.upper()
    txt_df.loc[
        txt_df.relationship.notnull() & txt_df.relationship.str.contains("DIRECT"),
        "relationship",
    ] = "DIRECT"
    txt_df.loc[
        txt_df.relationship.notnull() & txt_df.relationship.str.contains("RESELLER"),
        "relationship",
    ] = "RESELLER"
    # Drop unwanted rows
    keep_rows = (
        (txt_df.domain.notnull())
        & (txt_df.domain != "")
        & (txt_df.publisher_id.notnull())
        & (txt_df.publisher_id != "")
        & (txt_df.relationship.isin(["DIRECT", "RESELLER"]))
    )
    dropped_rows = txt_df.shape[0] - keep_rows.sum()
    if dropped_rows > 0:
        logger.warning(f"Dropped rows: {dropped_rows}")
    txt_df = txt_df[keep_rows]
    if txt_df.empty:
        raise AdsTxtEmpty("AdsTxtDF Empty")
    return txt_df


def scrape_from_store(store: int, store_id: str) -> dict:
    if store == 1:
        result_dict = scrape_app_gp(store_id)
    elif store == 2:
        result_dict = scrape_app_ios(store_id)
    else:
        logger.error(f"Store not supported {store=}")
    return result_dict


def scrape_any_app(store: int, store_id: str) -> pd.DataFrame:
    scrape_info = f"{store=}, {store_id=}"
    try:
        result_dict = scrape_from_store(store=store, store_id=store_id)
        crawl_result = 1
    except google_play_scraper.exceptions.NotFoundError:
        crawl_result = 3
        logger.warning(f"{scrape_info} failed to find app")
    except AppStoreException as error:
        if "No app found" in str(error):
            crawl_result = 3
            logger.warning(f"{scrape_info} failed to find app")
        else:
            crawl_result = 4
            logger.error(f"{scrape_info} unexpected error: {error=}")
    except Exception as error:
        logger.error(f"{scrape_info} unexpected error: {error=}")
        crawl_result = 4
    if crawl_result != 1:
        result_dict = {}
    result_dict["crawl_result"] = crawl_result
    result_dict["store"] = store
    result_dict["store_id"] = store_id
    df = pd.DataFrame([result_dict])
    if crawl_result == 1:
        df = clean_scraped_df(df=df, store=store)
    return df


def clean_scraped_df(df: pd.DataFrame, store: int) -> pd.DataFrame:
    if store == 1:
        df = clean_google_play_app_df(df)
    if store == 2:
        df = clean_ios_app_df(df)
    return df


def scrape_app_gp(store_id: str) -> dict:
    result = google_play_scraper.app(
        store_id, lang="en", country="us"  # defaults to 'en'  # defaults to 'us'
    )
    return result


def clean_google_play_app_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "title": "name",
            "installs": "min_installs",
            "realInstalls": "installs",
            # "appId": "store_id",
            "score": "rating",
            "updated": "store_last_updated",
            "reviews": "review_count",
            "containsAds": "ad_supported",
            "offersIAP": "in_app_purchases",
            "url": "store_url",
            "developerWebsite": "url",
            "developerId": "developer_id",
            "developer": "developer_name",
            "reviews": "review_count",
            "genreId": "category",
        }
    )
    df.loc[df["min_installs"].isnull(), "min_installs"] = df.loc[
        df["min_installs"].isnull(), "installs"
    ].astype(str)
    df = df.assign(
        min_installs=df["min_installs"]
        .str.replace(r"[,+]", "", regex=True)
        .astype(int),
        category=df["category"].str.lower(),
        store_last_updated=pd.to_datetime(
            df["store_last_updated"], unit="s"
        ).dt.strftime("%Y-%m-%d %H:%M"),
    )
    return df


def scrape_app_ios(store_id: str) -> dict:
    scraper = AppStoreScraper()
    result = scraper.get_app_details(store_id)
    return result


def clean_ios_app_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            # "trackId": "store_id",
            "trackName": "name",
            "averageUserRating": "rating",
            "sellerUrl": "url",
            "minimum_OsVersion": "minimum_android",
            "primaryGenreName": "category",
            "bundleId": "bundle_id",
            "currentVersionReleaseDate": "store_last_updated",
            "artistId": "developer_id",
            "artistName": "developer_name",
            "userRatingCount": "review_count",
        }
    )
    df = df.assign(
        free=df.price == 0,
        developer_id=df["developer_id"].astype(str),
        store_id=df["store_id"].astype(str),
        store_last_updated=pd.to_datetime(df["store_last_updated"]).dt.strftime(
            "%Y-%m-%d %H:%M"
        ),
    )
    return df


def extract_domains(x: str) -> str:
    ext = tldextract.extract(x)
    use_top_domain = any(
        ["m" == ext.subdomain, "www" in ext.subdomain.split("."), ext.subdomain == ""]
    )
    if use_top_domain:
        url = ".".join([ext.domain, ext.suffix])
    else:
        url = ".".join(part for part in ext if part)
    url = url.lower()
    return url


def save_developer_info(app_df: pd.DataFrame) -> pd.DataFrame:
    assert app_df["developer_id"].values[
        0
    ], f"{app_df['store_id']} Missing Developer ID"
    dev_df = app_df[["store", "developer_id", "developer_name"]].rename(
        columns={"developer_name": "name"}
    )
    insert_columns = ["store", "developer_id", "name"]
    try:
        dev_df = insert_get(
            table_name="developers",
            df=dev_df,
            insert_columns=insert_columns,
            key_columns=["store", "developer_id"],
            database_connection=PGCON,
        )
        app_df["developer"] = dev_df["id"].astype(object)[0]
    except Exception as error:
        logger.error(f"Insert failed with error {error}")
    return app_df


def scrape_and_save_app(store, store_id):
    info = f"{store=}, {store_id=}"
    logger.info(f"{info} start scrape")
    app_df = scrape_any_app(store=store, store_id=store_id)
    crawl_result = app_df["crawl_result"].values[0]
    if crawl_result != 1:
        logger.warning(
            f"{info} {crawl_result=} bad crawl result, updating table without details"
        )
        upsert_df(
            table_name="store_apps",
            df=app_df,
            insert_columns=["store", "store_id", "crawl_result"],
            key_columns=["store", "store_id"],
            database_connection=PGCON,
        )
        return app_df
    app_df = save_developer_info(app_df)
    insert_columns = [x for x in STORE_APP_COLUMNS if x in app_df.columns]
    try:
        store_apps_df = insert_get(
            table_name="store_apps",
            df=app_df,
            insert_columns=insert_columns,
            key_columns=["store", "store_id"],
            database_connection=PGCON,
        )
        app_df["store_app_id"] = store_apps_df["id"].astype(object)[0]
    except Exception as error:
        logger.error(f"Error on insert app_df: {error=}")
    return app_df


def crawl_stores_for_app_details(df: pd.DataFrame) -> None:
    logger.info(f"Update App Details: df: {df.shape}")
    rows = df.shape[0]
    for index, row in df.iterrows():
        logger.info(f"Update App Details row {index} of {rows} start")
        store_id = row.store_id
        store = row.store
        update_all_app_info(store, store_id)
        logger.info(f"Update App Details row {index} of {rows} finish")


def update_all_app_info(store: int, store_id: str) -> None:
    info = f"{store=} {store_id=}"
    app_df = scrape_and_save_app(store, store_id)
    if "store_app" not in app_df.columns:
        return
    if "url" not in app_df.columns or not app_df["url"].values:
        logger.info(f"{info} no developer url")
        return
    app_df["url"] = app_df["url"].apply(lambda x: extract_domains(x))
    insert_columns = ["url"]
    app_urls_df = insert_get(
        "pub_domains",
        app_df,
        insert_columns,
        key_columns=["url"],
        database_connection=PGCON,
    )
    app_df["pub_domain"] = app_urls_df["id"].astype(object)[0]
    insert_columns = ["store_app", "pub_domain"]
    upsert_df(
        "app_urls_map",
        insert_columns,
        app_df,
        key_columns=["store_app"],
        database_connection=PGCON,
    )


def crawl_app_ads() -> None:
    df = query_pub_domains(database_connection=PGCON)
    logger.info("Crawl app-ads from pub domains")
    i = 0
    for index, row in df.iterrows():
        i += 1
        app_url = row.url
        row_info = f"{i=}, {app_url=}"
        logger.info(f"{row_info} START")
        # Get App Ads.txt Text File
        try:
            raw_txt = get_app_ads_text(app_url)
            raw_txt_df = parse_ads_txt(txt=raw_txt)
            txt_df = clean_raw_txt_df(txt_df=raw_txt_df.copy())
            row["crawl_result"] = 1
        except NoAdsTxt as error:
            logger.error(f"{row_info} ads.txt not found {error}")
            row["crawl_result"] = 3
        except AdsTxtEmpty as error:
            logger.error(f"{row_info} ads.txt parsing error {error}")
            row["crawl_result"] = 2
        except ConnectionError as error:
            logger.error(f"{row_info} domain not found {error}")
            row["crawl_result"] = 3
        except Exception as error:
            logger.error(f"{row_info} unknown error: {error}")
            row["crawl_result"] = 4
        insert_columns = ["url", "crawl_result"]
        pub_domain_df = insert_get(
            "pub_domains",
            row,
            insert_columns,
            key_columns="url",
            database_connection=PGCON,
        )
        if row.crawl_result != 1:
            continue
        insert_columns = ["domain"]
        ad_domains = txt_df[["domain"]].drop_duplicates()
        domain_df = insert_get(
            table_name="ad_domains",
            df=ad_domains,
            insert_columns=insert_columns,
            key_columns="domain",
            database_connection=PGCON,
        )
        app_df = pd.merge(
            txt_df, domain_df, how="left", on=["domain"], validate="many_to_one"
        ).rename(columns={"id": "ad_domain"})
        app_df["pub_domain"] = pub_domain_df["id"].astype(object)[0]
        insert_columns = [
            "ad_domain",
            "publisher_id",
            "relationship",
            "certification_auth",
        ]
        if "notes" in df.columns:
            insert_columns = [
                "ad_domain",
                "publisher_id",
                "relationship",
                "certification_auth",
                "notes",
            ]
        key_cols = ["ad_domain", "publisher_id", "relationship"]
        app_df = app_df.drop_duplicates(subset=key_cols)
        entrys_df = insert_get(
            "app_ads_entrys",
            app_df,
            insert_columns,
            key_columns=key_cols,
            database_connection=PGCON,
        )
        entrys_df = entrys_df.rename(columns={"id": "app_ads_entry"})
        app_df_final = pd.merge(
            app_df,
            entrys_df,
            how="left",
            on=["ad_domain", "publisher_id", "relationship"],
            validate="many_to_one",
        )
        insert_columns = ["pub_domain", "app_ads_entry"]
        null_df = app_df_final[app_df_final.app_ads_entry.isnull()]
        if not null_df.empty:
            logger.warning(f"{null_df=} NULLs in app_ads_entry")
        upsert_df(
            "app_ads_map",
            insert_columns,
            app_df_final,
            key_columns=insert_columns,
            database_connection=PGCON,
        )
        logger.info(f"{row_info} DONE")
    logger.info("Crawl app-ads from pub domains finished")


def scrape_ios_frontpage() -> None:
    logger.info("Scrape iOS frontpage for new apps")
    scraper = AppStoreScraper()
    categories = {k: v for k, v in AppStoreCategories.__dict__.items() if "GAME" in k}
    collections = {
        k: v
        for k, v in AppStoreCollections.__dict__.items()
        if "_I" in k and "PAID" not in k
    }
    store_ids = []
    for coll_key, coll_value in collections.items():
        logger.info(f"Collection: {coll_value}")
        for cat_key, cat_value in categories.items():
            logger.info(f"Collection: {coll_value}, category: {cat_value}")
            coll_key, coll_value, cat_key, cat_value
            new_ids = scraper.get_app_ids_for_collection(
                collection=coll_value, category=cat_value, num=200, timeout=10
            )
            store_ids += new_ids
    store_ids = list(set(store_ids))
    apps_df = pd.DataFrame({"store": 2, "store_id": store_ids})
    insert_columns = ["store", "store_id"]
    logger.info(f"Scrape iOS frontpage for new apps: insert to db {apps_df.shape=}")
    upsert_df(
        table_name="store_apps",
        insert_columns=insert_columns,
        df=apps_df,
        key_columns=insert_columns,
        database_connection=PGCON,
    )
    logger.info("Scrape iOS frontpage for new apps finished")


def update_app_details(stores: list[int]) -> None:
    logger.info("Update App Details: start with oldest first")
    df = query_store_apps(stores, database_connection=PGCON, limit=20000)
    crawl_stores_for_app_details(df)


def main(args) -> None:
    logger.info(f"Main starting with args: {args}")
    platforms = args.platforms if "args" in locals() else ["android", "ios"]
    new_apps_check = args.new_apps_check if "args" in locals() else False
    stores = []
    stores.append(1) if "android" in platforms else None
    stores.append(2) if "ios" in platforms else None

    # Scrape Store for new apps
    if new_apps_check:
        if 1 in stores:
            scrape_ios_frontpage()
        if 2 in stores:
            scrape_gp_for_app_ids()

    # Update the app details
    update_app_details(stores)

    # Crawl developwer websites to check for app ads
    crawl_app_ads()


STORE_APP_COLUMNS = [
    "developer",
    "name",
    "store_id",
    "store",
    "category",
    "rating",
    "installs",
    "free",
    "price",
    "size",
    "minimum_android",
    "review_count",
    "content_rating",
    "store_last_updated",
    "developer_email",
    "ad_supported",
    "in_app_purchases",
    "editors_choice",
    "crawl_result",
]


def manage_cli_args() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--platforms",
        action="append",
        help="String as portion of android or ios",
        default=["android", "ios"],
    )
    parser.add_argument(
        "-l",
        "--is-local-db",
        help="Connect to local db on port 5432",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-s",
        "--single-run-check",
        help="If included prevent running if script already running",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-n",
        "--new-apps-check",
        help="Scrape the iTunes and Play Store front pages to find new apps",
        default=False,
        action="store_true",
    )
    args, leftovers = parser.parse_known_args()

    if args.single_run_check and script_has_process():
        logger.warning("Script already running, exiting")
        quit()
    return args


if __name__ == "__main__":
    logger.info("Starting app-ads.txt crawler")
    args = manage_cli_args()
    PGCON = get_db_connection(args.is_local_db)
    PGCON.set_engine()

    main(args)
