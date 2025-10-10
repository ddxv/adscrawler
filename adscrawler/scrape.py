import csv
import io
from typing import TypedDict

import pandas as pd
import requests
import tldextract

from .config import DEVLEOPER_IGNORE_TLDS, get_logger
from .dbcon.connection import PostgresCon
from .dbcon.queries import query_pub_domains, upsert_df

"""
    Pulling, parsing and save to db for app-ads.txt
"""


logger = get_logger(__name__)


class NoAdsTxtError(Exception):
    pass


class AdsTxtEmptyError(Exception):
    pass


def get_text_from_response(response: requests.Response) -> str:
    max_bytes = 1000000
    # Check the content length
    content_length = response.headers.get("Content-Length")
    if content_length and int(content_length) > max_bytes:
        err = f"content exceeds maximum allowed size of {max_bytes} bytes"
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
    return text


def try_http_request(ads_url: str, headers: dict | None = None) -> requests.Response:
    if not ads_url.startswith("http"):
        ads_url = "http://" + ads_url
    if ads_url.startswith("https://"):
        ads_url = ads_url.replace("https://", "http://", 1)
    try:
        if headers:
            response = requests.get(ads_url, headers=headers, stream=True, timeout=4)
        else:
            response = requests.get(ads_url, stream=True, timeout=4)
    except requests.exceptions.ConnectionError:
        err = f"{ads_url} type: requests ConnectionError"
        raise NoAdsTxtError(err) from ConnectionError
    if response.status_code != 200:
        err = f"{ads_url} status_code: {response.status_code}"
        raise NoAdsTxtError(err)
    return response


def try_https_request(ads_url: str, headers: dict | None = None) -> requests.Response:
    if not ads_url.startswith("http"):
        ads_url = "http://" + ads_url
    ads_url = ads_url.replace("http://", "https://", 1)
    try:
        if headers:
            response = requests.get(ads_url, headers=headers, stream=True, timeout=4)
        else:
            response = requests.get(ads_url, stream=True, timeout=4)
    except Exception:
        err = f"{ads_url} type: requests Exception"
        raise NoAdsTxtError(err) from Exception
    if response.status_code != 200:
        err = f"{ads_url} status_code: {response.status_code}"
        logger.error(err)
    return response


def request_app_ads(ads_url: str) -> str:
    try:
        response = try_http_request(ads_url)
    except Exception:
        try:
            response = try_https_request(ads_url)
        except Exception:
            err = f"{ads_url} type: requests Exception"
            raise NoAdsTxtError(err) from Exception
    # Handle 403/406 (Forbidden) by modifying the User-Agent
    if response.status_code >= 400 and response.status_code < 500:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0",
        }
        try:
            response = try_http_request(ads_url, headers=headers)
        except Exception:
            try:
                response = try_https_request(ads_url, headers=headers)
            except Exception:
                err = f"{ads_url} type: requests Exception"
                raise NoAdsTxtError(err) from Exception
    # If still not successful, raise an error for non-200 status codes
    if response.status_code != 200:
        err = f"{response.status_code}"
        try:
            response = try_https_request(ads_url)
        except Exception:
            err = f"{ads_url} status_code: {response.status_code}"
            raise NoAdsTxtError(err) from Exception
    text = get_text_from_response(response)
    if "<head>" in text:
        err = f"{ads_url} HTML in adstxt"
        if ads_url.startswith("http://"):
            ads_url = ads_url.replace("http://", "https://", 1)
            response = requests.get(ads_url, stream=True, timeout=4)
            text = get_text_from_response(response)
        raise NoAdsTxtError(err)
    if not any(term in text.upper() for term in ["DIRECT", "RESELLER"]):
        err = "DIRECT, RESELLER not in ads.txt"
        raise NoAdsTxtError(err)
    return text


def get_app_ads_text(url: str) -> str:
    ext = tldextract.extract(url)
    use_top_domain = any(
        [ext.subdomain == "m", "www" in ext.subdomain.split("."), ext.subdomain == ""],
    )
    sub_domains_url = ""
    if ext.subdomain and not use_top_domain:
        sub_domains_url = ".".join([ext.subdomain, ext.domain, ext.suffix])
        sub_domains_url = "http://" + sub_domains_url + "/" + "app-ads.txt"
    top_domain_url = ".".join([ext.domain, ext.suffix]) + "/" + "app-ads.txt"
    top_domain_url = "http://" + top_domain_url
    if sub_domains_url:
        try:
            text = request_app_ads(ads_url=sub_domains_url)
            return text
        except NoAdsTxtError as error:
            info = f"{top_domain_url=}, {sub_domains_url=} {error=}"
            logger.warning(f"Subdomain has no ads.txt {info}")
    tld_dont_run = any(
        [True if x in top_domain_url else False for x in DEVLEOPER_IGNORE_TLDS]
    )
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
    df = query_pub_domains(
        database_connection=database_connection, limit=limit, exclude_recent_days=7
    )
    logger.info(f"Start crawl app-ads from pub domains: {df.shape[0]}")
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
    found_domains = (
        txt_df[["domain"]].drop_duplicates().rename(columns={"domain": "domain_name"})
    )
    domain_df = upsert_df(
        table_name="domains",
        df=found_domains,
        insert_columns=insert_columns,
        key_columns=["domain_name"],
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
