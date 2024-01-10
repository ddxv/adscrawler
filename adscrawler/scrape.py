import csv
import io
from typing import TypedDict

import pandas as pd
import requests
import tldextract

from adscrawler.config import get_logger
from adscrawler.connection import PostgresCon
from adscrawler.queries import query_pub_domains, upsert_df

"""
    Pulling, parsing and save to db for app-ads.txt
"""


logger = get_logger(__name__)


def request_app_ads(ads_url: str) -> str:
    max_bytes = 100000
    if ads_url[0:4] != "http":
        ads_url = "http://" + ads_url
    response = requests.get(ads_url, stream=True, timeout=2)
    if response.status_code == 403:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0)",
        }
        response = requests.get(ads_url, headers=headers, timeout=2, stream=True)
    if response.status_code != 200:
        err = f"{ads_url} status_code: {response.status_code}"
        raise NoAdsTxtError(err)
    # Maximum amount we want to read
    content_bytes = response.headers.get("Content-Length")
    if content_bytes and int(content_bytes) < max_bytes:
        # Expected body is smaller than our maximum, read the whole thing
        text = response.text
    else:
        # Alternatively, stream until we hit our limit
        amount_read = 0
        mybytes = b""
        for chunk in response.iter_content():
            amount_read += len(chunk)
            # Save chunk
            mybytes += chunk
            if amount_read > max_bytes:
                logger.warning("Encountered large file, quitting")
                raise NoAdsTxtError("File too large")
        text = mybytes.decode("utf-8")
    if "<head>" in text:
        err = f"{ads_url} HTML in adstxt"
        raise NoAdsTxtError(err)
    if not any(term in text.upper() for term in ["DIRECT", "RESELLER"]):
        err = "DIRECT, RESELLER not in ads.txt"
        raise NoAdsTxtError(err)
    return text


class NoAdsTxtError(Exception):
    pass


class AdsTxtEmptyError(Exception):
    pass


def get_app_ads_text(url: str) -> str:
    ext_url = tldextract.extract(url)
    sub_domains_url = ""
    if ext_url.subdomain:
        # Note contains all subdomains, even m
        # TODO: parse & exclude for m subdomain
        sub_domains_url = ".".join(ext_url) + "/" + "app-ads.txt"
        sub_domains_url = "http://" + sub_domains_url
    top_domain_url = ".".join([ext_url.domain, ext_url.suffix]) + "/" + "app-ads.txt"
    top_domain_url = "http://" + top_domain_url
    if sub_domains_url:
        try:
            text = request_app_ads(ads_url=sub_domains_url)
            return text
        except NoAdsTxtError as error:
            info = f"{top_domain_url=}, {sub_domains_url=} {error=}"
            logger.warning(f"Subdomain has no ads.txt {info}")
    tld_dont_run = any([True if x in top_domain_url else False for x in IGNORE_TLDS])
    if tld_dont_run:
        raise NoAdsTxtError
    text = request_app_ads(ads_url=top_domain_url)
    return text


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
                    ],
                )
            elif len(row) > 4:
                rows.append(
                    [
                        row["domain"],
                        row["publisher_id"],
                        row["relationship"],
                        row["certification_auth"],
                        ",".join(row["notes"]),
                    ],
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
        lambda x: ".".join(
            [tldextract.extract(x).domain, tldextract.extract(x).suffix],
        ),
    )
    standard_str_cols = ["domain", "publisher_id", "relationship", "certification_auth"]
    txt_df[standard_str_cols] = txt_df[standard_str_cols].replace(
        "[^a-zA-Z0-9_\\-\\.]",
        "",
        regex=True,
    )
    # Clean Relationship
    txt_df["relationship"] = txt_df["relationship"].str.upper()
    txt_df.loc[
        txt_df.relationship.notna() & txt_df.relationship.str.contains("DIRECT"),
        "relationship",
    ] = "DIRECT"
    txt_df.loc[
        txt_df.relationship.notna() & txt_df.relationship.str.contains("RESELLER"),
        "relationship",
    ] = "RESELLER"
    # Drop unwanted rows
    keep_rows = (
        (txt_df.domain.notna())
        & (txt_df.domain != "")
        & (txt_df.publisher_id.notna())
        & (txt_df.publisher_id != "")
        & (txt_df.relationship.isin(["DIRECT", "RESELLER"]))
    )
    dropped_rows = txt_df.shape[0] - keep_rows.sum()
    if dropped_rows > 0:
        logger.warning(f"Dropped rows: {dropped_rows}")
    txt_df = txt_df[keep_rows]
    if txt_df.empty:
        raise AdsTxtEmptyError("AdsTxtDF Empty")
    return txt_df


def crawl_app_ads(database_connection: PostgresCon, limit: int | None = 5000) -> None:
    df = query_pub_domains(database_connection=database_connection, limit=limit)
    logger.info("Crawl app-ads from pub domains")
    for _i, row in df.iterrows():
        url = row.url
        scrape_app_ads_url(url=url, database_connection=database_connection)
    logger.info("Crawl app-ads from pub domains finished")


class ResultDict(TypedDict):
    crawl_result: int
    url: str


def scrape_app_ads_url(url: str, database_connection: PostgresCon) -> None:
    info = f"{url=} scrape app-ads.txt"
    logger.info(f"{info} start")
    result_dict = ResultDict(url=url, crawl_result=4)
    # Get App Ads.txt Text File
    try:
        raw_txt = get_app_ads_text(url)
        raw_txt_df = parse_ads_txt(txt=raw_txt)
        txt_df = clean_raw_txt_df(txt_df=raw_txt_df.copy())
        result_dict["crawl_result"] = 1
    except NoAdsTxtError as error:
        logger.warning(f"{info} ads.txt not found {error}")
        result_dict["crawl_result"] = 3
    except AdsTxtEmptyError as error:
        logger.exception(f"{info} ads.txt parsing ERROR {error}")
        result_dict["crawl_result"] = 2
    except requests.exceptions.ConnectionError as error:
        logger.warning(f"{info} domain not found {error}")
        result_dict["crawl_result"] = 3
    except Exception as error:
        logger.exception(f"{info} unknown ERROR: {error}")
        result_dict["crawl_result"] = 4
    insert_columns = ["url", "crawl_result"]
    pub_domain_df = pd.DataFrame([result_dict])
    pub_domain_df["crawl_result"] = pub_domain_df["crawl_result"].astype(int)
    pub_domain_df = upsert_df(
        table_name="pub_domains",
        df=pub_domain_df,
        insert_columns=insert_columns,
        key_columns=["url"],
        database_connection=database_connection,
        return_rows=True,
    )
    if result_dict["crawl_result"] != 1:
        return
    insert_columns = ["domain"]
    ad_domains = txt_df[["domain"]].drop_duplicates()
    domain_df = upsert_df(
        table_name="ad_domains",
        df=ad_domains,
        insert_columns=insert_columns,
        key_columns=["domain"],
        database_connection=database_connection,
        return_rows=True,
    )
    app_df = pd.merge(
        txt_df,
        domain_df,
        how="left",
        on=["domain"],
        validate="many_to_one",
    ).rename(columns={"id": "ad_domain"})
    app_df["pub_domain"] = pub_domain_df["id"].astype(object)[0]
    insert_columns = [
        "ad_domain",
        "publisher_id",
        "relationship",
        "certification_auth",
    ]
    if "notes" in app_df.columns:
        insert_columns = [
            "ad_domain",
            "publisher_id",
            "relationship",
            "certification_auth",
            "notes",
        ]
    key_cols = ["ad_domain", "publisher_id", "relationship"]
    app_df = app_df.drop_duplicates(subset=key_cols)
    entrys_df = upsert_df(
        table_name="app_ads_entrys",
        df=app_df,
        insert_columns=insert_columns,
        key_columns=key_cols,
        database_connection=database_connection,
        return_rows=True,
    )
    if entrys_df is not None and not entrys_df.empty:
        entrys_df = entrys_df.rename(columns={"id": "app_ads_entry"})
        app_df_final = pd.merge(
            app_df,
            entrys_df,
            how="left",
            on=["ad_domain", "publisher_id", "relationship"],
            validate="many_to_one",
        )
        insert_columns = ["pub_domain", "app_ads_entry"]
        null_df = app_df_final[app_df_final.app_ads_entry.isna()]
        if not null_df.empty:
            logger.warning(f"{null_df=} NULLs in app_ads_entry")
        upsert_df(
            table_name="app_ads_map",
            insert_columns=insert_columns,
            df=app_df_final,
            key_columns=insert_columns,
            database_connection=database_connection,
        )
        logger.info(f"{info} finished")


IGNORE_TLDS = [
    "00webhostapp.com",
    "bitballoon.com",
    "blogger.com",
    "linkedin.com",
    "blogspot.com",
    "blogspot.co.id",
    "blogspot.in",
    "bytehost6.com",
    "facebook.com",
    "flycricket.io",
    "github.io",
    "netlify.com",
    "page.link",
    "site123.me",
    "simplesite.com",
    "tumblr.com",
    "weebly.com",
    "wix.com",
    "wixsite.com",
    "wordpress.com",
]
