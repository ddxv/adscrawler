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


def get_url_dev_id(bundle, store_name):
    url = None
    dev_id = None
    dev_name = None
    if store_name == "itunes":
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
    elif store_name == "google_play":
        store_url = f"https://play.google.com/store/apps/details?id={bundle}"
        response = requests.get(store_url, timeout=2)
        assert (
            response.status_code == 200
        ), f"{store_name} response code: {response.status_code}"
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
        if len(re.findall(r"\.", url)) > 1:
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
        # TODO Does later cleaning solve this?, likely domains not cleaned...
        # txt = txt.replace("Â\xa0", "")
        # txt = txt.replace("Ã\x82", "")
        # txt = txt.replace("\t", "")
        # txt = txt.replace("\u202a", "")
        # txt = txt.replace("\u202c", "")
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


def crawl_apps_df(df, skip_rows):
    # TODO where to get this from
    df["store_name"] = "google_play"
    df["platform"] = 1
    df["store"] = 1
    i = skip_rows
    for index, row in df[skip_rows:].iterrows():
        i += 1
        row_info = f"{i=}, {row.bundle_id=}"
        logger.info(f"{row_info} START")
        # if bundle == 'com.outfit7.mytalkingtomfree':
        #    break
        # if bundle == 'com.gameloft.android.ANMP.GloftDMHM':
        #   break
        insert_columns = ["platform", "bundle_id"]
        dev_df = insert_get(
            "apps", row, insert_columns, key_columns=["platform", "bundle_id"]
        )
        try:
            app_url, dev_id, dev_name = get_url_dev_id(row.bundle_id, row.store_name)
            assert dev_id, f"missing {dev_id=}"
        except Exception as err:
            logger.error(f"Failed to get AppStore dev and app_url, error: {err}")
            continue
        logger.info(f"{row_info} {dev_id=}")
        row["developer_id"] = dev_id
        row["developer_name"] = dev_name
        row["app_ads_url"] = app_url
        insert_columns = ["store", "developer_id"]
        dev_df = insert_get(
            "developers", row, insert_columns, key_columns=["store", "developer_id"]
        )
        row["developer"] = dev_df["id"].astype(object)[0]
        # NOTE This should not be bundle for iTunes?
        if row.store_name == "google_play":
            row["store_id"] = row.bundle_id
        insert_columns = ["developer", "store_id", "store", "app"]
        store_apps_df = insert_get(
            "store_apps", row, insert_columns, key_columns="store_id"
        )
        row["store_app"] = store_apps_df["id"].astype(object)[0]
        if not app_url:
            logger.warning(f"{row_info} Skipping, no URL")
            continue
        insert_columns = ["store_app", "app_ads_url"]
        app_urls_df = insert_get(
            "app_urls", row, insert_columns, key_columns=["store_app", "app_ads_url"]
        )
        row["app_url"] = app_urls_df.id.values[0]
        logger.info(f"{row_info} scrape ads.txt")
        # Get App Ads.txt
        raw_txt_df = get_app_ads_text(app_url)
        if raw_txt_df.empty:
            logger.warning(f"{row_info} Skipping, DF empty")
            continue
        txt_df = clean_raw_txt_df(txt_df=raw_txt_df.copy())
        txt_df["store_app"] = row.store_app
        txt_df["app_url"] = row.app_url
        txt_df["store_id"] = row.bundle_id
        txt_df["developer_store_id"] = dev_id
        txt_df["developer_store_name"] = dev_name
        txt_df["updated_at"] = datetime.datetime.now()
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


def get_store_developer(store_name, dev_id, dev_name):
    if store_name == "google_play":
        store = 1
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


def get_android_df_from_csv():
    filename = f"{MY_DIR}/store-data/Google-Playstore_kaggle.csv"
    filename = f"{MY_DIR}/store-data/Google-Playstore_kaggle_ONLYGAMES.csv"
    df = pd.read_csv(filename)
    skip_categories = [
        "Video Players & Editors",
        "Social",
        "Tools",
        "News & Magazines",
        "Travel & Local",
        "Communication",
        "Music & Audio",
        "Productivity",
        "Personalization",
        "Shopping",
        "Photography",
        "Business",
        "Education",
        "Books & Reference",
        "Health & Fitness",
        "Finance",
        "Medical",
        "Maps & Navigation",
        "Food & Drink",
        "Beauty",
        "Libraries & Demo",
        "Weather",
        "Events",
        "Parenting",
        "House & Home",
        "Lifestyle",
        "Art & Design",
    ]
    df = df[~df["Category"].isin(skip_categories)]
    df = df[df["Ad Supported"]]
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


def get_app_ids(bundles, platform):
    bundle_str = f"('" + ("', '").join(bundles) + "')"
    sel_query = f"""SELECT id as app, bundle_id 
    FROM apps 
    WHERE platform = {platform}
    AND bundle_id IN {bundle_str};
    """
    app_ids = pd.read_sql(sel_query, MADRONE.engine)
    return app_ids


def insert_df(table_name, insert_columns, df, key_columns, log=None):
    cols_str = ", ".join([f'"{col}"' for col in insert_columns])
    key_cols_str = ", ".join([f'"{col}"' for col in key_columns])
    values_str = ", ".join([f"%({col})s" for col in insert_columns])
    set_update = ", ".join([f"{col} = excluded.{col}" for col in insert_columns])
    if isinstance(df, pd.Series):
        df = pd.DataFrame(df).T
    insert_query = f""" 
        INSERT INTO {table_name} ({cols_str})
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
    reinsert_from_csv = args.update_all if "args" in locals() else False
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

    if "android" in platforms:
        if reinsert_from_csv:
            df = get_android_df_from_csv()
            df["platform"] = 1
            df = df.rename(columns={"App Id": "bundle_id", "App Name": "name"})
            df = df.sort_values("Minimum Installs", ascending=False)
            df = insert_get(
                "apps",
                df,
                insert_columns=["platform", "bundle_id", "name"],
                key_columns=["platform", "bundle_id"],
            )

            df = df.rename(columns={"id": "app"})

    sel_query = """SELECT ap.id as app, ap.bundle_id, ap.updated_at  
    FROM apps ap
    """
    df = pd.read_sql(sel_query, MADRONE.engine)
    df = df.sort_values("updated_at")
    crawl_apps_df(df, skip_rows)
