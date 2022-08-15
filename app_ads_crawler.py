from itunes_app_scraper.util import AppStoreCollections, AppStoreCategories
from itunes_app_scraper.scraper import AppStoreScraper
from google_play_scraper import app
import argparse
import io
import requests
import pandas as pd
import csv
from dbcon.connection import get_db_connection
from dbcon.queries import insert_get, upsert_df, query_store_apps, query_pub_domains
import tldextract
from config import get_logger

logger = get_logger(__name__)


def request_app_ads(ads_url):
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
    if not any(term in response.text for term in ["DIRECT", "RESELLER"]):
        err = "DIRECT, RESELLER not in ads.txt"
        raise NoAdsTxt(err)
    return response


class NoAdsTxt(Exception):
    pass


class AdsTxtEmpty(Exception):
    pass


def get_app_ads_text(app_url):
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


def parse_ads_txt(txt):
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


def clean_raw_txt_df(txt_df):
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


def scrape_app(store, store_id):
    if store == 1:
        app_df = scrape_app_gp(store_id)
    elif store == 2:
        app_df = scrape_app_ios(store_id)
    else:
        logger.error(f"Store not supported {store=}")
    app_df["store"] = store
    return app_df


def scrape_app_gp(store_id):
    result = app(
        store_id, lang="en", country="us"  # defaults to 'en'  # defaults to 'us'
    )
    app_df = pd.DataFrame([result])
    app_df["installs"] = pd.to_numeric(
        app_df["installs"].str.replace(r"[,+]", "", regex=True)
    )
    app_df = app_df.rename(
        columns={
            "title": "name",
            "installs": "min_installs",
            "realInstalls": "installs",
            "appId": "store_id",
            "score": "rating",
            "updated": "store_last_updated",
            "reviews": "review_count",
            "containsAds": "ad_supported",
            "offersIAP": "in_app_purchases",
            "url": "store_url",
            "developerWebsite": "url",
            "developerId": "developer_id",
            "reviews": "review_count",
        }
    )
    app_df["category"] = app_df["genreId"].str.lower()
    app_df["store_last_updated"] = pd.to_datetime(
        app_df["store_last_updated"], unit="s"
    )
    return app_df


def scrape_app_ios(store_id):
    scraper = AppStoreScraper()
    app = scraper.get_app_details(store_id)
    app_df = pd.DataFrame([app])
    app_df = app_df.rename(
        columns={
            "trackId": "store_id",
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
    app_df["free"] = app_df.price == 0
    app_df[["developer_id", "store_id"]] = app_df[["developer_id", "store_id"]].astype(
        str
    )
    return app_df


def extract_domains(x):
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


def crawl_stores_for_app_details(df):
    for index, row in df.iterrows():
        store_id = row.store_id
        store = row.store
        row_info = f"{index=} {store=} {store_id=}"
        logger.info(f"{row_info} start")
        try:
            app_df = scrape_app(store=store, store_id=store_id)
            assert app_df["developer_id"].values[0], f"{row_info} Missing Developer ID"
            app_df["crawl_result"] = 1
        except Exception as error:
            error_message = f"{row_info} {error=}"
            logger.error(error_message)
            if "404" in error_message or "No app found" in error_message:
                crawl_result = 3
            else:
                crawl_result = 4
            row["crawl_result"] = crawl_result
            app_df = pd.DataFrame([row])
        else:
            insert_columns = ["store", "developer_id"]
            dev_df = insert_get(
                table_name="developers",
                df=app_df,
                insert_columns=insert_columns,
                key_columns=["store", "developer_id"],
                database_connection=PGCON,
            )
            app_df["developer"] = dev_df["id"].astype(object)[0]
        insert_columns = [x for x in STORE_APP_COLUMNS if x in app_df.columns]
        store_apps_df = insert_get(
            "store_apps",
            app_df,
            insert_columns,
            key_columns=["store", "store_id"],
            database_connection=PGCON,
        )
        if "url" not in app_df.columns or not app_df["url"].values:
            logger.info(f"{row_info} no developer url")
            continue
        app_df["url"] = app_df["url"].apply(lambda x: extract_domains(x))
        app_df["store_app"] = store_apps_df["id"].astype(object)[0]
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


def crawl_app_ads():
    df = query_pub_domains(database_connection=PGCON)
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
        domain_df = insert_get(
            "ad_domains",
            txt_df,
            insert_columns,
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


def scrape_ios_frontpage():
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
                collection=coll_value, category=cat_value, num=200
            )
            store_ids += new_ids
    store_ids = list(set(store_ids))
    apps_df = pd.DataFrame({"store": 2, "store_id": store_ids})
    insert_columns = ["store", "store_id"]
    my_df = insert_get(
        "store_apps",
        insert_columns=insert_columns,
        df=apps_df,
        key_columns=insert_columns,
        database_connection=PGCON,
    )
    crawl_stores_for_app_details(my_df)


def update_app_details(stores):
    i = 0
    while i < 100:
        df = query_store_apps(stores, database_connection=PGCON)
        crawl_stores_for_app_details(df)
        i += 1


def main(args):

    platforms = args.platforms if "args" in locals() else ["android"]
    platforms = ["ios", "android"]
    stores = []
    stores.append(1) if "android" in platforms else None
    stores.append(2) if "ios" in platforms else None

    # Scrape Store for new apps
    scrape_ios_frontpage()

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


if __name__ == "__main__":
    logger.info("Starting app-ads.txt crawler")

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
    args, leftovers = parser.parse_known_args()
    PGCON = get_db_connection(args.is_local_db)
    PGCON.set_engine()

    main(args)
