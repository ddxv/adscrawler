import play_scraper
from typing import Union, List, Optional
from sshtunnel import SSHTunnelForwarder
import numpy as np
import yaml
import argparse
import io
import re
import urllib
import datetime
import requests
import pandas as pd
import csv
import dbconn
import pathlib
import logging
import logging.handlers

logger = logging.getLogger(__name__)

# Get module directory
try:
    print(
        "Parent directory from __file__ Path:",
        pathlib.Path(__file__).parent.parent.absolute(),
    )
    MY_DIR = pathlib.Path(pathlib.Path(__file__).parent.parent.absolute())
except Exception as error:
    print(
        f"""Error: {error} \n 
            Guess running from interpreter and home dir:
            """,
        pathlib.Path().home().absolute(),
    )
    MY_DIR = pathlib.Path(pathlib.Path.home(), "adscrawler/")


CONFIG_PATH = pathlib.Path(MY_DIR, "config.yml")
with CONFIG_PATH.open() as f:
    CONFIG = yaml.safe_load(f)

LOG_PATH = pathlib.Path(f"{MY_DIR}/logs/crawler.log")
LOG_FORMAT = "%(asctime)s: %(name)s: %(levelname)s: %(message)s"

logger.setLevel(logging.INFO)

streamhandler = logging.StreamHandler()
streamhandler.setFormatter(logging.Formatter(LOG_FORMAT))
logger.addHandler(streamhandler)
log_rotate = logging.handlers.RotatingFileHandler(
    filename=LOG_PATH, maxBytes=10000000, backupCount=4
)
log_rotate.setFormatter(logging.Formatter(LOG_FORMAT))
logger.addHandler(log_rotate)


def OpenSSHTunnel():
    with SSHTunnelForwarder(
        (CONFIG["ssh"]["host"], 22),  # Remote server IP and SSH port
        ssh_username=CONFIG["ssh"]["username"],
        ssh_pkey=CONFIG["ssh"]["pkey"],
        ssh_private_key_password=CONFIG["ssh"]["pkey_password"],
        remote_bind_address=(f"127.0.0.1", 5432),
    ) as server:  # PostgreSQL server IP and sever port on remote machine
        server.start()  # start ssh sever
        logger.info("Connecting via SSH")
        # connect to PostgreSQL
    return server


def get_url_dev_id(bundle, store):
    url = None
    dev_id = None
    dev_name = None
    if store == 2:
        store_url = "http://itunes.apple.com/lookup?id=%s" % bundle
        # Get '1.txt' from itunes store, process result to grab sellerUrl
        try:
            response = requests.get(store_url, timeout=2)
            payload = response.json()
            if payload:
                results = payload["results"][0]
                if results["sellerUrl"]:
                    url = results["sellerUrl"]
                    logger.info(f"iOS app url is {url}\n")
        except Exception as err:
            logger.error(f"Failed to get URL, error occurred: {err}")
    elif store == 1:
        store_url = f"https://play.google.com/store/apps/details?id={bundle}"
        response = requests.get(store_url, timeout=2)
        assert (
            response.status_code == 200
        ), f"Store: {store} response code: {response.status_code}"
        rawhtml = re.search("appstore:developer_url.*>", response.text)
        # Get the First Developer Name
        dev_html = re.search('/store/apps/dev.*"', response.text)[0]
        dev_html = re.split("<", dev_html)[0]
        dev_name = re.split(">", dev_html)[1]
        # Next Developer ID
        dev_html = re.split("=", dev_html)[1]
        dev_id = re.split('"', dev_html)[0]
        assert rawhtml, "Did not find appstore:developer_url in app store HTML"
        rawhtml = rawhtml[0]
        url = re.search('"http[^"]+"', rawhtml)
        assert url, "Did not find URL http in app store HTML content"
        url = url[0].strip('"')
        url = urllib.parse.urlparse(url).netloc
        # Drop first subdomain if it is mobile
        if url.split(".")[0] == "m":
            url = ".".join(url.split(".")[1:])
        url = "http://" + url
    else:
        logger.error('Invalid Platform Name - send "itunes" or "google_play"\n')
    return url, dev_id, dev_name


def get_app_ads_text(app_url):
    if app_url[-1] == "/":
        ads_url = app_url + "app-ads.txt"
    else:
        ads_url = app_url + "/" + "app-ads.txt"
    try:
        response = requests.get(ads_url, timeout=2)
        if response.status_code == 403:
            headers = {
                "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0)",
            }
            response = requests.get(ads_url, headers=headers, timeout=2)
        assert (
            response.status_code == 200
        ), f"GET {ads_url} status_code: {response.status_code}, skipping"
        assert "<head>" not in response.text, f"WARN: {ads_url} HTML in adstxt"
        assert any(
            term in response.text for term in ["DIRECT", "RESELLER"]
        ), f"DIRECT, RESELLER not in ads.txt, skipping"
        txt = response.text.replace(" ", "")
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
                    rows.append(
                        [row["domain"], row["publisher_id"], row["relationship"]]
                    )
            except Exception as err:
                logger.error(f"Parser skipping row: {row}, error: {err}")
                continue
        if pd.DataFrame(rows).shape[1] == len(csv_header) - 1:
            df = pd.DataFrame(rows, columns=csv_header[:-1])
        else:
            df = pd.DataFrame(rows, columns=csv_header)
        return df
    except Exception as err:
        logger.error(f"{err}")
        return pd.DataFrame()


def insert_get(
    table_name: str,
    df: Union[pd.DataFrame, pd.Series],
    insert_columns: Union[str, List[str]],
    key_columns: Union[str, List[str]],
    log: Optional[bool] = None,
) -> pd.DataFrame:
    logger.info(f"insert_get table: {table_name}")
    if isinstance(insert_columns, str):
        insert_columns = [insert_columns]
    if isinstance(key_columns, str):
        key_columns = [key_columns]
    if isinstance(df, pd.Series):
        df = pd.DataFrame(df).T
    insert_df(table_name, insert_columns, df, key_columns)
    get_df = query_all(table_name, key_columns, df)
    return get_df


def clean_raw_txt_df(txt_df):
    # Domain
    txt_df["domain"] = txt_df["domain"].str.lower()
    txt_df["domain"] = txt_df["domain"].str.replace("http://", "", regex=False)
    txt_df["domain"] = txt_df["domain"].str.replace("https://", "", regex=False)
    # txt_df["relationship"] = (
    #    txt_df["relationship"].str.encode("ascii", errors="ignore").str.decode("ascii")
    # )
    # txt_df["publisher_id"] = (
    #    txt_df["publisher_id"].str.encode("ascii", errors="ignore").str.decode("ascii")
    # )
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
    return txt_df


def google_play(store_id):
    app = play_scraper.details(store_id)
    app_df = pd.DataFrame.from_dict(app, orient="index").T
    app_df["installs"] = pd.to_numeric(app_df["installs"].str.replace(r"[,+]", ""))
    app_df = app_df.rename(
        columns={
            "title": "name",
            "app_id": "store_id",
            "score": "rating",
            "updated": "store_last_updated",
            "reviews": "review_count",
            "contains_ads": "ad_supported",
            "iap": "in_app_purchases",
            "url": "store_url",
            "developer_url": "url",
            "reviews": "review_count",
            "required_android_version": "minimum_android",
        }
    )
    return app_df


def crawl_stores(df):
    i = skip_rows
    for index, row in df[skip_rows:].iterrows():
        i += 1
        row_info = f"{i=}, {row.store_id=}"
        logger.info(f"{row_info} START")
        app_df = google_play(row.store_id)
        app_df["store"] = row.store
        insert_columns = ["store", "developer_id"]
        dev_df = insert_get(
            "developers", app_df, insert_columns, key_columns=["store", "developer_id"]
        )
        app_df["developer"] = dev_df["id"].astype(object)[0]
        insert_columns = [
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
        ]
        store_apps_df = insert_get(
            "store_apps", app_df, insert_columns, key_columns=["store", "store_id"]
        )
        app_df["store_app"] = store_apps_df["id"].astype(object)[0]
        insert_columns = ["url"]
        app_urls_df = insert_get(
            "pub_domains", app_df, insert_columns, key_columns=["url"]
        )
        app_df["pub_domain"] = app_urls_df["id"].astype(object)[0]
        insert_columns = ["store_app", "pub_domain"]
        insert_df("app_urls_map", insert_columns, app_df, key_columns=["store_app"])


def crawl_app_ads(df):
    i = skip_rows
    for index, row in df[skip_rows:].iterrows():
        i += 1
        row_info = f"{i=}, {row.store_id=}"
        logger.info(f"{row_info} START")
        # Get App Ads.txt
        raw_txt_df = get_app_ads_text(row.app_url)
        if raw_txt_df.empty:
            logger.warning(f"{row_info} Skipping, DF empty")
            continue
        txt_df = clean_raw_txt_df(txt_df=raw_txt_df.copy())
        txt_df["store_app"] = row.store_app
        txt_df["app_url"] = row.app_url
        txt_df["store_id"] = row.store_id
        # txt_df["developer_store_id"] = dev_id
        # txt_df["developer_store_name"] = dev_name
        txt_df["updated_at"] = datetime.datetime.now()
        if txt_df.empty:
            logger.warning(f"{row_info} Cleaned DF empty")
            continue
        insert_columns = ["domain"]
        domain_df = insert_get(
            "ad_domains", txt_df, insert_columns, key_columns="domain"
        )
        app_df = pd.merge(
            txt_df, domain_df, how="left", on=["domain"], validate="many_to_one"
        ).rename(columns={"id": "ad_domain"})
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
            "app_ads_entrys", app_df, insert_columns, key_columns=key_cols
        )
        entrys_df = entrys_df.rename(columns={"id": "app_ads_entry"})
        app_df_final = pd.merge(
            app_df,
            entrys_df,
            how="left",
            on=["ad_domain", "publisher_id", "relationship"],
            validate="many_to_one",
        )
        insert_columns = ["app_url", "app_ads_entry"]
        null_df = app_df_final[app_df_final.app_ads_entry.isnull()]
        if not null_df.empty:
            logger.warning(f"{null_df=} NULLs in app_ads_entry")
        insert_df(
            "app_ads_map", insert_columns, app_df_final, key_columns=insert_columns
        )
        logger.info(f"{row_info} DONE")


def get_store_developer(store, dev_id, dev_name):
    sel_query = f"""SELECT * FROM developers 
    WHERE store = 1
    AND developer_id = '{dev_id}';
    """
    df = pd.read_sql(sel_query, MADRONE.engine)
    if df.empty:
        ins_query = f"""INSERT INTO developers
        (store, name, developer_id)
        VALUES ({store}, '{dev_name}', '{dev_id}');
        """
        MADRONE.engine.execute(ins_query)
        df = pd.read_sql(sel_query, MADRONE.engine)
    return df


def query_all(
    table_name: str, key_cols: Union[List[str], str], df: pd.DataFrame
) -> pd.DataFrame:
    if isinstance(key_cols, str):
        key_cols = [key_cols]
    wheres = []
    for key_col in key_cols:
        keys = df[key_col].unique().tolist()
        if all([isinstance(x, (np.integer, int)) for x in keys]):
            values_str = f"(" + (", ").join([str(x) for x in keys]) + ")"
            values_str = values_str.replace("%", "%%")
        else:
            values_str = f"('" + ("', '").join(keys) + "')"
            values_str = values_str.replace("%", "%%")
        where = f"{key_col} IN {values_str}"
        wheres.append(where)
    where_str = " AND ".join(wheres)
    sel_query = f"""SELECT *
    FROM {table_name}
    WHERE {where_str}
    """
    # logger.info(sel_query)
    df = pd.read_sql(sel_query, MADRONE.engine)
    return df


def get_store_app_ids(store, store_ids):
    if isinstance(store_ids, str):
        store_ids = [store_ids]
    store_ids_str = f"('" + ("', '").join(store_ids) + "')"
    sel_query = f"""SELECT *
    FROM store_apps
    WHERE store = {store}
    AND store_id IN {store_ids_str};
    """
    df = pd.read_sql(sel_query, MADRONE.engine)
    return df


def insert_df(table_name, insert_columns, df, key_columns, log=None):
    db_cols_str = ", ".join([f'"{col}"' for col in insert_columns])
    key_cols_str = ", ".join([f'"{col}"' for col in key_columns])
    values_str = ", ".join([f"%({col})s" for col in insert_columns])
    set_update = ", ".join([f"{col} = excluded.{col}" for col in insert_columns])
    if isinstance(df, pd.Series):
        df = pd.DataFrame(df).T
    insert_query = f""" 
        INSERT INTO {table_name} ({db_cols_str})
        VALUES ({values_str})
        ON CONFLICT ({key_cols_str})
        DO UPDATE SET {set_update}
        """
    values = df[insert_columns].to_dict("records")
    if log:
        logger.info(f"MY INSERT QUERY: {insert_query.format(values)}")
    with MADRONE.engine.begin() as connection:
        connection.execute(insert_query, values)


def get_existing_app_ads():
    sel_query = """select app_id, max(updated_at) as last_updated from app_ads group by app_id;
    """
    df = pd.read_sql(sel_query, MADRONE.engine)
    return df


def check_app_ads():
    sel_query = """select app_id, updated_at from app_ads limit 10;
    """
    df = pd.read_sql(sel_query, MADRONE.engine)
    return df


def reinsert_from_csv():
    filename = f"{MY_DIR}/store-data/Google-Playstore_kaggle.csv"
    chunksize = 100000
    i = 0
    with pd.read_csv(filename, chunksize=chunksize) as reader:
        for chunk in reader:
            logger.info(f"chunk {i}")
            chunk["platform"] = 1
            chunk["store"] = 1
            chunk.columns = [x.replace(" ", "_").lower() for x in chunk.columns]
            insert_df(
                table_name="app_store_csv_dump",
                insert_columns=chunk.columns.tolist(),
                df=chunk,
                key_columns=["platform", "store", "app_id"],
            )
            i += 1


def main():
    while True:
        # Query Apps table
        sel_query = """SELECT store, id as store_app, store_id, updated_at  
        FROM store_apps
        WHERE ad_supported = true
        ORDER BY updated_at
        limit 1000
        """
        df = pd.read_sql(sel_query, MADRONE.engine)
        crawl_stores(df)


if __name__ == "__main__":

    # FORMAT = "%(asctime)s: %(name)s: %(levelname)s: %(message)s"
    # logging.basicConfig(format=FORMAT, level=logging.INFO)
    # formatter = logging.Formatter(FORMAT)
    # logger.setLevel(logging.DEBUG)
    # ch = logging.StreamHandler()
    # ch.setLevel(logging.INFO)
    # ch.setFormatter(formatter)
    # logger.addHandler(ch)

    logger.info("Starting app-ads.txt crawler")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--platforms",
        action="append",
        help="String as portion of android or ios",
        default=[],
    )
    parser.add_argument(
        "-a",
        "--update-all",
        action="store_true",
        help="if included will update ALL bundles provided",
        default=False,
    )
    parser.add_argument(
        "-s", "--skip-rows", help="integer of rows to skip", default=0,
    )

    args, leftovers = parser.parse_known_args()

    platforms = args.platforms if "args" in locals() else ["android"]
    update_all = args.update_all if "args" in locals() else False
    skip_rows = args.skip_rows if "args" in locals() else 0
    skip_rows = int(skip_rows)

    if "james" in f"{CONFIG_PATH}":
        server = OpenSSHTunnel()
        server.start()
        local_port = str(server.local_bind_port)
    else:
        local_port = 5432
    MADRONE = dbconn.PostgresCon("madrone", "127.0.0.1", local_port)
    MADRONE.set_engine()

    main()
