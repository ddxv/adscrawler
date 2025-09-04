import base64
import dataclasses
import datetime
import hashlib
import html
import json
import os
import pathlib
import re
import struct
import subprocess
import urllib
import uuid
import xml.etree.ElementTree as ET

import boto3
import imagehash
import numpy as np
import pandas as pd
import protod
import requests
import tldextract
from bs4 import BeautifulSoup
from mitmproxy import http
from mitmproxy.io import FlowReader
from PIL import Image
from protod import Renderer

from adscrawler.config import CONFIG, CREATIVES_DIR, MITM_DIR, get_logger
from adscrawler.dbcon.connection import PostgresCon
from adscrawler.dbcon.queries import (
    get_click_url_redirect_chains,
    log_creative_scan_results,
    query_ad_domains,
    query_apps_to_creative_scan,
    query_store_app_by_store_id,
    query_store_apps_no_creatives,
    upsert_df,
)
from adscrawler.packages.storage import (
    download_mitm_log_by_key,
    download_mitm_log_by_store_id,
    get_app_creatives_s3_keys,
    get_store_id_mitm_s3_keys,
    upload_ad_creative_to_s3,
)

try:
    from adscrawler.packages.apks.decrypt_applovin import decode_from
except ImportError:
    decode_from = None

logger = get_logger(__name__, "mitm_scrape_ads")

ANDROID_USER_AGENT = "Mozilla/5.0 (Linux; Android 13; Pixel 7 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"

ABS_TOL = 1024  # bytes
PCT_TOL = 0.03  # 3%

IGNORE_PRIVACY_URLS = [
    "/policy.html",
    "/legal",
    "/policy",
    "/privacy",
    "privacy_policy",
    "/your-personal-data",
    "private-policy.html",
    "/privacypolicy",
    "privacy-policy",
    "data-protection",
    "/data-privacy",
]


class MultipleAdvertiserIdError(Exception):
    """Raised when multiple advertiser store IDs are found for the same ad."""

    def __init__(self, found_adv_store_ids):
        self.found_adv_store_ids = found_adv_store_ids
        super().__init__(f"multiple adv_store_id found for {found_adv_store_ids}")


IGNORE_CREATIVE_IDS = ["privacy", "google_play_icon_grey_2022", "favicon"]

MMP_TLDS = [
    "appsflyer.com",
    "adjust.com",
    "adj.st",
    "adjust.io",
    "singular.com",
    "singular.net",
    "kochava.com",
    "openattribution.dev",
    "airbridge.com",
    "arb.ge",
    "branch.io",
    "impression.link",
    "sng.link",
    "onelink.me",
]

PLAYSTORE_URL_PARTS = ["play.google.com/store", "market://", "intent://"]


@dataclasses.dataclass
class AdInfo:
    adv_store_id: str | None
    host_ad_network_tld: str | None = None
    init_tld: str | None = None
    found_ad_network_tlds: list[str] | None = None
    found_mmp_urls: list[str] | None = None

    def __getitem__(self, key: str):
        """Support dictionary-style access to dataclass fields"""
        return getattr(self, key)

    def __setitem__(self, key: str, value):
        """Support dictionary-style assignment to dataclass fields"""
        setattr(self, key, value)

    @property
    def mmp_tld(self) -> str | None:
        if self.found_mmp_urls and len(self.found_mmp_urls) > 0:
            return (
                tldextract.extract(self.found_mmp_urls[0]).domain
                + "."
                + tldextract.extract(self.found_mmp_urls[0]).suffix
            )
        return None


def decode_network_request(
    url: str, flowpart: http.HTTPFlow, database_connection: PostgresCon
) -> str | None:
    if decode_from and "applovin.com" in url:
        try:
            text = decode_from(
                blob=flowpart.content, database_connection=database_connection
            )
            if text is None:
                logger.error(f"Decode {url[:40]=} failed")
                try:
                    text = flowpart.get_text()
                except Exception:
                    text = "Unknown decode error"
        except Exception:
            text = flowpart.get_text()
    else:
        logger.error(f"Unknown Decode Network Request URL: {url}")
        text = flowpart.get_text()
    return text


def parse_mitm_log(
    pub_store_id: str,
    run_id: int,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    mitm_log_path = pathlib.Path(MITM_DIR, f"{pub_store_id}_{run_id}.log")
    if mitm_log_path.exists():
        logger.info("mitm log already exists")
    else:
        key = f"mitm_logs/android/{pub_store_id}/{run_id}.log"
        mitms = get_store_id_mitm_s3_keys(store_id=pub_store_id)
        key = mitms[mitms["run_id"] == str(run_id)]["key"].to_numpy()[0]
        mitm_log_path = download_mitm_log_by_key(key, mitm_log_path)
    # Define the log file path
    if not mitm_log_path.exists():
        logger.error(f"mitm log file not found at {mitm_log_path}")
        raise FileNotFoundError
    # Parse the flows
    mitm_requests = []
    try:
        with open(mitm_log_path, "rb") as f:
            reader = FlowReader(f)
            for flow in reader.stream():
                if isinstance(flow, http.HTTPFlow):
                    try:
                        if hasattr(flow, "timestamp_start"):
                            timestamp = datetime.datetime.fromtimestamp(
                                flow.timestamp_start
                            )
                        elif hasattr(flow.client_conn, "timestamp_start"):
                            timestamp = datetime.datetime.fromtimestamp(
                                flow.client_conn.timestamp_start
                            )
                        else:
                            timestamp = None
                        tld_url = get_tld(flow.request.pretty_url)
                        url = flow.request.pretty_url
                        # Extract useful data from each flow
                        request_data = {
                            "start_time": flow.timestamp_start,
                            "method": flow.request.method,
                            "url": url,
                            "tld_url": tld_url,
                            "host": flow.request.host,
                            "path": flow.request.path,
                            "query_params": dict(flow.request.query),
                            "post_params": flow.request.urlencoded_form,
                            "headers": dict(flow.request.headers),
                            "content_type": flow.request.headers.get(
                                "Content-Type", ""
                            ),
                            "timestamp": timestamp,
                        }
                        if "applovin.com" in tld_url:
                            request_data["request_text"] = decode_network_request(
                                url,
                                flowpart=flow.request,
                                database_connection=database_connection,
                            )
                        else:
                            try:
                                request_data["request_text"] = flow.request.get_text()
                            except Exception:
                                request_data["request_text"] = ""
                        try:
                            request_data["content"] = flow.request.content
                        except Exception:
                            request_data["content"] = ""
                        # Add response info if available
                        if flow.response:
                            try:
                                request_data["status_code"] = flow.response.status_code
                            except Exception:
                                request_data["status_code"] = None
                            request_data["response_headers"] = dict(
                                flow.response.headers
                            )
                            request_data["response_content_type"] = (
                                flow.response.headers.get("Content-Type", "")
                            )
                            request_data["response_size"] = flow.response.headers.get(
                                "Content-Length", "0"
                            )
                            if "applovin.com" in tld_url:
                                request_data["response_text"] = decode_network_request(
                                    url,
                                    flowpart=flow.response,
                                    database_connection=database_connection,
                                )
                            else:
                                try:
                                    request_data["response_text"] = (
                                        flow.response.get_text()
                                    )
                                except Exception:
                                    request_data["response_text"] = ""
                            try:
                                request_data["response_content"] = flow.response.content
                            except Exception:
                                pass
                        mitm_requests.append(request_data)
                    except Exception as e:
                        logger.exception(f"Error parsing flow: {e}")
                        continue
    except Exception as e:
        logger.exception(e)
    # Convert to DataFrame
    if not mitm_requests:
        logger.error("No HTTP requests found in the log file")
        return pd.DataFrame()
    df = pd.DataFrame(mitm_requests)
    if "response_text" in df.columns:
        df["response_text"] = df["response_text"].astype(str)
    df["run_id"] = (
        mitm_log_path.as_posix().split("/")[-1].split("_")[-1].replace(".log", "")
    )
    df["pub_store_id"] = pub_store_id
    return df


def get_sent_video_df(
    df: pd.DataFrame, row: pd.Series, video_id: str
) -> pd.DataFrame | None:
    sent_video_df = df[
        (df["response_text"].astype(str).str.contains(video_id, regex=False))
        & (df["start_time"] <= row.start_time)
    ].copy()
    if sent_video_df.empty:
        sent_video_df = df[
            (df["response_text"].astype(str).str.contains(video_id, regex=False))
        ].copy()
    if sent_video_df.empty:
        return None
    if sent_video_df.shape[0] > 1:
        logger.info(f"Multiple responses for {video_id=}")
    sent_video_df["tld_url"] = sent_video_df["url"].apply(lambda x: get_tld(x))
    return sent_video_df


def extract_and_decode_urls(
    text: str, run_id: int, database_connection: PostgresCon
) -> list[str]:
    """
    Extracts all URLs from a given text, handles HTML entities and URL encoding.
    """
    vast_urls = []
    if "<?xml version" in text[0:13]:
        vast_tree = None
        try:
            vast_tree = ET.fromstring(text)
        except ET.ParseError:
            try:
                vast_tree = ET.fromstring(html.unescape(text))
            except ET.ParseError:
                pass
        if vast_tree is not None:
            try:
                # Now extract URLs from the inner VAST tree
                for tag in [
                    "Impression",
                    "ClickThrough",
                    "ClickTracking",
                    "MediaFile",
                    "Tracking",
                ]:
                    for el in vast_tree.iter(tag):
                        if el.text:
                            vast_urls.append(el.text.strip())
            except Exception:
                pass
    soup = BeautifulSoup(text, "html.parser")
    # The URLs are located inside a <meta> tag with name="video_fields"
    # The content of this tag contains VAST XML data.
    video_fields_meta = soup.find("meta", {"name": "video_fields"})
    if video_fields_meta:
        vast_xml_string = html.unescape(video_fields_meta["content"])
        vast_urls += re.findall(r"<!\[CDATA\[(.*?)\]\]>", vast_xml_string)
    if soup.find("vast"):
        vast_urls += re.findall(r"<!\[CDATA\[(.*?)\]\]>", text)
        # vast_urls = [x for x in vast_urls if "http" in x]
    # 1. Broad regex for URLs (http, https, fybernativebrowser, etc.)
    # This pattern is made more flexible to capture various URL formats
    urls = []
    unesc_text = html.unescape(text)
    # Sometimes JSON decoded to string became //u0026, this is a workaround to fix it
    url_pattern = re.compile(
        r"""(?:
        (?:https?|intent|market|fybernativebrowser):\/\/  # allowed schemes
        [^\s'"<>\]\)\}]+?                                 # non-greedy match
    )
    (?=[\s"\\;'<>\]\)\}\{},]|$)                           # must be followed by separator or end
    """,
        re.VERBOSE,
    )
    try:
        enc_text = text.encode("utf-8").decode("unicode_escape")
        enc_found_urls = url_pattern.findall(enc_text)
    except Exception:
        enc_found_urls = []
    unesc_found_urls = url_pattern.findall(unesc_text)
    orig_found_urls = url_pattern.findall(text)
    found_urls = list(
        set(vast_urls + unesc_found_urls + enc_found_urls + orig_found_urls)
    )
    encoded_delimiters = [
        "%5D",  # ]
        "%3E",  # >
        "%5B",  # [
        "%3C",  # <
    ]
    for url in found_urls:
        # print("--------------------------------")
        # print("RAW:", url)
        # Find the earliest encoded delimiter
        cut_index = len(url)
        for delim in encoded_delimiters:
            idx = url.upper().find(delim)  # case-insensitive match
            if idx != -1 and idx < cut_index:
                cut_index = idx
        if cut_index != len(url):
            url = url[:cut_index]  # Trim at the first encoded delimiter
        decoded_url = urllib.parse.unquote(url)
        urls.append(decoded_url)
        # print("DECODED:", decoded_url)
    all_urls = list(set(vast_urls + urls))
    click_urls = check_click_urls(all_urls, run_id, database_connection)
    all_urls = list(set(all_urls + click_urls))
    return all_urls


def check_click_urls(
    all_urls: list[str], run_id: int, database_connection: PostgresCon
):
    click_urls = []
    for url in all_urls:
        redirect_urls = []
        if "youappi.com/v1/e/click" in url:
            redirect_urls = follow_url_redirects(url, run_id, database_connection)
        elif "tpbid.com" in url and "/click" in url:
            url = url.replace("fybernativebrowser://navigate?url=", "")
            if "/click" in url:
                redirect_urls = follow_url_redirects(url, run_id, database_connection)
        if len(redirect_urls) > 0:
            click_urls += redirect_urls
    click_urls = list(set(click_urls))
    logger.info(f"Found {len(click_urls)} click URLs")
    return click_urls


def parse_fyber_html(
    inner_ad_element: str, run_id: int, database_connection: PostgresCon
):
    # Extract all URLs from the raw HTML content first
    all_extracted_urls = extract_and_decode_urls(
        inner_ad_element,
        run_id=run_id,
        database_connection=database_connection,
    )
    # We're looking for URLs that start with 'fybernativebrowser://navigate?url='
    # or 'https://gotu.tpbid.com/click'
    click_urls = []
    for url in all_extracted_urls:
        if url.startswith("fybernativebrowser://navigate?url="):
            pattern = r".*?(https?://.*)"
            match = re.search(pattern, url)
            if match:
                url = match.group(1)
                click_urls.append(url)
        elif "https://gotu.tpbid.com/click" in url:
            click_urls.append(url)
        elif get_tld(url) in MMP_TLDS:
            click_urls.append(url)
    return click_urls


def parse_fyber_ad_response(
    ad_response_text: str, run_id: int, database_connection: PostgresCon
) -> list[str]:
    outer_tree = ET.fromstring(ad_response_text)
    ns = {"tns": "http://www.inner-active.com/SimpleM2M/M2MResponse"}
    ad_element = outer_tree.find(".//tns:Ad", ns)
    urls = []
    vast_tree = None
    if ad_element is not None and ad_element.text:
        # Clean up and parse the inner VAST XML
        inner_ad_element = ad_element.text.strip()
        try:
            vast_tree = ET.fromstring(inner_ad_element)
        except ET.ParseError:
            try:
                vast_tree = ET.fromstring(html.unescape(inner_ad_element))
            except ET.ParseError:
                urls = parse_fyber_html(
                    inner_ad_element,
                    run_id=run_id,
                    database_connection=database_connection,
                )
        if vast_tree is not None:
            # Now extract URLs from the inner VAST tree
            for tag in [
                "Impression",
                "ClickThrough",
                "ClickTracking",
                "MediaFile",
                "Tracking",
            ]:
                for el in vast_tree.iter(tag):
                    if el.text:
                        urls.append(el.text.strip())
    return urls


def adv_id_from_play_url(url: str) -> str:
    parsed_gplay = urllib.parse.urlparse(url)
    try:
        adv_store_id = urllib.parse.parse_qs(parsed_gplay.query)["id"][0]
        adv_store_id = adv_store_id.rstrip("!@#$%^&*()+=[]{}|\\:;\"'<>?,/")
    except Exception:
        adv_store_id = None
    return adv_store_id


def follow_url_redirects(
    url: str, run_id: int, database_connection: PostgresCon
) -> list[str]:
    """
    Follows redirects and returns the final URL.

    Cache the results to avoid repeated requests.
    """
    existing_chain_df = get_click_url_redirect_chains(run_id, database_connection)
    if not existing_chain_df.empty and url in existing_chain_df["url"].to_list():
        redirect_urls = existing_chain_df[existing_chain_df["url"] == url][
            "redirect_url"
        ].to_list()
    else:
        redirect_urls = get_redirect_chain(url)
        if len(redirect_urls) > 0:
            chain_df = pd.DataFrame(
                {"run_id": run_id, "url": url, "redirect_url": redirect_urls}
            )
            logger.info(f"Inserting {chain_df.shape[0]} redirect URLs")
            upsert_df(
                df=chain_df,
                database_connection=database_connection,
                schema="adtech",
                table_name="click_url_redirect_chains",
                key_columns=["run_id", "url", "redirect_url"],
                insert_columns=["run_id", "url", "redirect_url"],
            )
    return redirect_urls


def get_redirect_chain(url):
    chain = []
    cur_url = url
    while cur_url:
        try:
            headers = {"User-Agent": ANDROID_USER_AGENT}
            # Do NOT allow requests to auto-follow
            response = requests.get(
                cur_url, headers=headers, allow_redirects=False, timeout=10
            )
            next_url = response.headers.get("Location")
            chain.append(next_url)
        except Exception:
            next_url = None
            pass
        if not next_url or not next_url.startswith("http"):
            break
        cur_url = next_url
    return chain


def parse_urls_for_known_parts(
    all_urls: list[str], database_connection: PostgresCon, pub_store_id: str
) -> AdInfo:
    found_mmp_urls = []
    found_adv_store_ids = []
    found_ad_network_urls = []
    ad_network_urls = query_ad_domains(database_connection=database_connection)
    for url in all_urls:
        adv_store_id = None
        tld_url = tldextract.extract(url).domain + "." + tldextract.extract(url).suffix
        if tld_url in MMP_TLDS:
            if any(
                x in url.lower()
                for x in [
                    "/privacy-policy",
                    "support.appsflyer.com",
                    "/terms-of-use",
                ]
            ):
                continue
            found_mmp_urls.append(url)
            if "websdk.appsflyer.com" in url:
                continue
            if "appsflyer.com" in tld_url:
                adv_store_id = re.search(
                    r"http.*\.appsflyer\.com/([a-zA-Z0-9_.]+)[\?\-]", url
                )[1]
                if adv_store_id:
                    found_adv_store_ids.append(adv_store_id)
        elif match := re.search(r"intent://details\?id=([a-zA-Z0-9._]+)", url):
            adv_store_id = match.group(1)
            found_adv_store_ids.append(adv_store_id)
        elif match := re.search(r"intent://.*package=([a-zA-Z0-9._]+)", url):
            adv_store_id = match.group(1)
            found_adv_store_ids.append(adv_store_id)
        elif match := re.search(r"market://details\?id=([a-zA-Z0-9._]+)", url):
            adv_store_id = match.group(1)
            found_adv_store_ids.append(adv_store_id)
        elif "play.google.com" in url and "google.com" in tld_url:
            if "apps/developer?" in url:
                continue
            resp_adv_id = adv_id_from_play_url(url)
            if resp_adv_id:
                found_adv_store_ids.append(resp_adv_id)
        elif "fybernativebrowser://" in url:
            url = url.replace("fybernativebrowser://navigate?url=", "")
            found_ad_network_urls.append(url)
        if (
            tld_url in ad_network_urls["domain"].to_list()
            and tld_url not in MMP_TLDS
            and not any(ignore_url in url.lower() for ignore_url in IGNORE_PRIVACY_URLS)
        ):
            found_ad_network_urls.append(url)
    found_mmp_urls = list(set(found_mmp_urls))
    found_adv_store_ids = list(set(found_adv_store_ids))
    found_adv_store_ids = [x for x in found_adv_store_ids if x != pub_store_id]
    found_ad_network_tlds = [
        tldextract.extract(url).domain + "." + tldextract.extract(url).suffix
        for url in found_ad_network_urls
    ]
    found_ad_network_tlds = list(set(found_ad_network_tlds))
    if len(found_adv_store_ids) == 0:
        adv_store_id = None
    elif len(found_adv_store_ids) == 1:
        adv_store_id = found_adv_store_ids[0]
    else:
        raise MultipleAdvertiserIdError(
            f"multiple adv_store_id found for {found_adv_store_ids=}"
        )
    return AdInfo(
        adv_store_id=adv_store_id,
        found_mmp_urls=found_mmp_urls,
        found_ad_network_tlds=found_ad_network_tlds,
    )


def parse_youappi_ad(sent_video_dict: dict, database_connection: PostgresCon) -> AdInfo:
    init_ad_network_tld = "youappi.com"
    all_urls = extract_and_decode_urls(
        sent_video_dict["response_text"],
        run_id=sent_video_dict["run_id"],
        database_connection=database_connection,
    )
    ad_info = parse_urls_for_known_parts(
        all_urls, database_connection, sent_video_dict["pub_store_id"]
    )
    ad_info.init_tld = init_ad_network_tld
    return ad_info


def parse_yandex_ad(
    sent_video_dict: dict, database_connection: PostgresCon, video_id: str
) -> AdInfo:
    init_ad_network_tld = "yandex.ru"
    json_text = json.loads(sent_video_dict["response_text"])
    if "native" in json_text:
        matched_ads = [x for x in json_text["native"]["ads"] if video_id in str(x)]
        if len(matched_ads) == 0:
            return AdInfo(
                adv_store_id=None,
                init_tld=init_ad_network_tld,
            )
        text = str(matched_ads)
    else:
        text = sent_video_dict["response_text"]
    all_urls = extract_and_decode_urls(
        text, run_id=sent_video_dict["run_id"], database_connection=database_connection
    )
    ad_info = parse_urls_for_known_parts(
        all_urls, database_connection, sent_video_dict["pub_store_id"]
    )
    ad_info.init_tld = init_ad_network_tld
    return ad_info


def parse_mtg_ad(sent_video_dict: dict, database_connection: PostgresCon) -> AdInfo:
    init_ad_network_tld = "mtgglobals.com"
    text = sent_video_dict["response_text"]
    try:
        ad_response = json.loads(text)
        adv_store_id = ad_response["data"]["ads"][0]["package_name"]
        if adv_store_id:
            ad_info = AdInfo(
                adv_store_id=adv_store_id,
                init_tld=init_ad_network_tld,
            )
            return ad_info
    except Exception:
        pass
    all_urls = extract_and_decode_urls(
        text, run_id=sent_video_dict["run_id"], database_connection=database_connection
    )
    ad_info = parse_urls_for_known_parts(
        all_urls, database_connection, sent_video_dict["pub_store_id"]
    )
    return ad_info


class JsonRenderer(Renderer):
    def __init__(self):
        self.result = dict()
        self.current = self.result

    def _add(self, id, item):
        self.current[id] = item

    def _build_tmp_item(self, chunk):
        # use a temporary renderer to build
        jr = JsonRenderer()
        chunk.render(jr)
        tmp_dict = jr.build_result()

        # the tmp_dict only contains 1 item
        for _, item in tmp_dict.items():
            return item

    def build_result(self):
        return self.result

    def render_repeated_fields(self, repeated):
        arr = []
        for ch in repeated.items:
            arr.append(self._build_tmp_item(ch))
        self._add(repeated.idtype.id, arr)

    def render_varint(self, varint):
        self._add(varint.idtype.id, varint.i64)

    def render_fixed(self, fixed):
        self._add(fixed.idtype.id, fixed.i)

    def render_struct(self, struct):
        curr = None

        if struct.as_fields:
            curr = {}
            for ch in struct.as_fields:
                curr[ch.idtype.id] = self._build_tmp_item(ch)
        elif struct.is_str:
            curr = struct.as_str

        else:
            curr = " ".join(format(x, "02x") for x in struct.view)

        self._add(struct.idtype.id, curr)


# return (
#   decoded bytes: bytes
#   encoding name: str
#   decoding succeeded: bool
# )
def decode_utf8(view) -> tuple[bytes, str, bool]:
    view_bytes = view.tobytes()
    try:
        utf8 = "UTF-8"
        decoded = view_bytes.decode(utf8)
        return decoded, utf8, True
    except Exception:
        return view_bytes, "", False


def base64decode(s: str) -> str:
    missing_padding = len(s) % 4
    if missing_padding:
        s += "=" * (4 - missing_padding)
    return base64.urlsafe_b64decode(s)


def parse_bidmachine_ad(
    sent_video_dict: dict, database_connection: PostgresCon
) -> AdInfo:
    init_ad_network_tld = "bidmachine.com"
    adv_store_id = None
    additional_ad_network_tld = None
    ad_info = AdInfo(
        adv_store_id=adv_store_id,
        init_tld=init_ad_network_tld,
    )
    if isinstance(sent_video_dict["response_content"], str):
        import ast

        sent_video_dict["response_content"] = ast.literal_eval(
            sent_video_dict["response_content"]
        )
    ret = protod.dump(
        sent_video_dict["response_content"],
        renderer=JsonRenderer(),
        str_decoder=decode_utf8,
    )
    try:
        adv_store_id = ret[5][6][3][13][2][3]
        additional_ad_network_tld = ret[5][6][3][13][2][2]
        text = str(ret[5][6][3][13][2][17])
        urls = extract_and_decode_urls(
            text,
            run_id=sent_video_dict["run_id"],
            database_connection=database_connection,
        )
        ad_info = parse_urls_for_known_parts(
            urls, database_connection, sent_video_dict["pub_store_id"]
        )
        if not ad_info.adv_store_id and adv_store_id:
            ad_info.adv_store_id = adv_store_id
    except Exception:
        pass

    if not ad_info.adv_store_id:
        # s = ret[11][2]
        # missing_padding = len(s) % 4
        # if missing_padding:
        #     s += "=" * (4 - missing_padding)
        # # Wait, this is is the pub_store_id
        # adv_store_id = json.loads(base64.urlsafe_b64decode(s))[
        # "sessionCallbackContext"
        # ]["bundle"]
        try:
            text = str(ret)
            urls = extract_and_decode_urls(
                text,
                run_id=sent_video_dict["run_id"],
                database_connection=database_connection,
            )
            ad_info = parse_urls_for_known_parts(
                urls, database_connection, sent_video_dict["pub_store_id"]
            )
        except Exception:
            pass
    if additional_ad_network_tld is not None and not ad_info.found_ad_network_tlds:
        ad_info.found_ad_network_tlds.append(additional_ad_network_tld)
    ad_info.init_tld = init_ad_network_tld
    return ad_info


def parse_everestop_ad(sent_video_dict: dict) -> AdInfo:
    init_ad_network_tld = "everestop.io"

    if isinstance(sent_video_dict["response_content"], str):
        import ast

        sent_video_dict["response_content"] = ast.literal_eval(
            sent_video_dict["response_content"]
        )
    ret = protod.dump(
        sent_video_dict["response_content"],
        renderer=JsonRenderer(),
        str_decoder=decode_utf8,
    )
    try:
        adv_store_id = ret[5][6][3][13][2][3]
        additional_ad_network_tld = ret[5][6][3][13][2][2]
    except Exception:
        pass

    ad_info = AdInfo(
        adv_store_id=adv_store_id,
        init_tld=init_ad_network_tld,
        found_ad_network_tlds=[additional_ad_network_tld],
    )
    return ad_info


def parse_unity_ad(
    sent_video_dict: dict, database_connection: PostgresCon
) -> tuple[AdInfo, str]:
    init_ad_network_tld = "unity3d.com"
    error_msg = None
    found_mmp_urls = []
    adv_store_id = None
    if "auction-load.unityads.unity3d.com" in sent_video_dict["url"]:
        ad_response_text = sent_video_dict["response_text"]
        ad_response = json.loads(ad_response_text)
        mykey = list(ad_response["media"].keys())[0]
        keyresp = ad_response["media"][mykey]
        if "bundleId" in keyresp:
            adv_store_id = keyresp["bundleId"]
        try:
            adcontent = str(ad_response["media"][mykey]["content"])
            # This is the ad html
            # jsadcontent = json.loads(adcontent)
            # with open("adhtml.html", "w") as f:
            #     f.write(jsadcontent["ad_networks"][0]["ad"]["ad_html"])
            if "referrer" in adcontent:
                referrer = adcontent.split("referrer=")[1].split(",")[0]
                if "adjust_external" in referrer:
                    found_mmp_urls.append("adjust.com")
        except Exception:
            pass
    text = sent_video_dict["response_text"]
    all_urls = extract_and_decode_urls(
        text, run_id=sent_video_dict["run_id"], database_connection=database_connection
    )
    try:
        ad_info = parse_urls_for_known_parts(
            all_urls, database_connection, sent_video_dict["pub_store_id"]
        )
    except MultipleAdvertiserIdError:
        error_msg = "multiple adv_store_id found for unity"
        ad_info = AdInfo(
            adv_store_id=None, found_ad_network_tlds=None, found_mmp_urls=None
        )
        return ad_info, error_msg
    if ad_info.adv_store_id is None and adv_store_id is not None:
        ad_info.adv_store_id = adv_store_id
    if ad_info.found_mmp_urls is None and found_mmp_urls:
        ad_info.found_mmp_urls = found_mmp_urls
    ad_info.init_tld = init_ad_network_tld
    return ad_info, error_msg


def get_tld(url: str) -> str:
    tld = tldextract.extract(url).domain + "." + tldextract.extract(url).suffix
    return tld


def parse_generic_adnetwork(
    sent_video_dict: dict, database_connection: PostgresCon
) -> tuple[AdInfo, str]:
    error_msg = None
    init_tld = get_tld(sent_video_dict["tld_url"])
    text = sent_video_dict["response_text"]
    all_urls = extract_and_decode_urls(
        text, run_id=sent_video_dict["run_id"], database_connection=database_connection
    )
    try:
        ad_info = parse_urls_for_known_parts(
            all_urls, database_connection, sent_video_dict["pub_store_id"]
        )
    except MultipleAdvertiserIdError as e:
        error_msg = (
            f"multiple adv_store_id found for {init_tld}: {e.found_adv_store_ids}"
        )
        logger.error(error_msg)
        return AdInfo(
            adv_store_id=None, found_ad_network_tlds=None, found_mmp_urls=None
        ), error_msg
    ad_info.init_tld = init_tld
    return ad_info, error_msg


def parse_vungle_ad(sent_video_dict: dict, database_connection: PostgresCon) -> AdInfo:
    init_ad_network_tld = "vungle.com"
    found_mmp_urls = None
    adv_store_id = None
    ad_response_text = sent_video_dict["response_text"]
    try:
        response_dict = json.loads(ad_response_text)
        adv_store_id = response_dict["ads"][0]["ad_markup"]["ad_market_id"]
        check_urls = ["clickUrl", "checkpoint.0", "checkpoint.100"]
        urlkeys = response_dict["ads"][0]["ad_markup"]["tpat"]
        for x in check_urls:
            try:
                these_urls = urlkeys[x]
                for url in these_urls:
                    if get_tld(url) in MMP_TLDS:
                        found_mmp_urls.append(url)
            except Exception:
                pass
    except Exception:
        pass
    if not adv_store_id:
        extracted_urls = extract_and_decode_urls(
            ad_response_text,
            run_id=sent_video_dict["run_id"],
            database_connection=database_connection,
        )
        ad_info = parse_urls_for_known_parts(
            extracted_urls, database_connection, sent_video_dict["pub_store_id"]
        )
    else:
        ad_info = AdInfo(
            adv_store_id=adv_store_id,
            init_tld=init_ad_network_tld,
            found_mmp_urls=found_mmp_urls,
        )
    return ad_info


def parse_fyber_ad(sent_video_dict: dict, database_connection: PostgresCon) -> AdInfo:
    init_ad_network_tld = "fyber.com"
    parsed_urls = []
    text = sent_video_dict["response_text"]
    try:
        parsed_urls = parse_fyber_ad_response(
            text,
            run_id=sent_video_dict["run_id"],
            database_connection=database_connection,
        )
    except Exception:
        pass
    all_urls = extract_and_decode_urls(
        text=text,
        run_id=sent_video_dict["run_id"],
        database_connection=database_connection,
    )
    all_urls = list(set(all_urls + parsed_urls))
    if "inner-active.mobi" in sent_video_dict["tld_url"]:
        if "x-ia-app-bundle" in sent_video_dict["response_headers"].keys():
            adv_store_id = sent_video_dict["response_headers"]["x-ia-app-bundle"]
            ad_info = AdInfo(
                adv_store_id=adv_store_id,
                init_tld=init_ad_network_tld,
            )
            return ad_info
    ad_info = parse_urls_for_known_parts(
        all_urls, database_connection, sent_video_dict["pub_store_id"]
    )
    ad_info.init_tld = init_ad_network_tld
    return ad_info


def parse_google_ad(
    sent_video_dict: dict, video_id: str, database_connection: PostgresCon
) -> tuple[AdInfo, str]:
    ad_info = AdInfo(adv_store_id=None, found_ad_network_tlds=None, found_mmp_urls=None)
    error_msg = None
    if sent_video_dict["response_text"] is None:
        error_msg = "doubleclick no response_text"
        logger.error(f"{error_msg} for {sent_video_dict['tld_url']}")
        return ad_info, error_msg
    response_text = sent_video_dict["response_text"]
    try:
        google_response = json.loads(response_text)
        big_html = ""
        if "ad_networks" in google_response:
            for gadn in google_response["ad_networks"]:
                ad_html = " "
                if "ad" in gadn:
                    gad = gadn["ad"]
                    if "ad_html" in gad:
                        ad_html = gad["ad_html"]
                    elif "ad_json" in gad:
                        ad_html = json.dumps(gad["ad_json"])
                    else:
                        pass
                    big_html += ad_html
            all_urls = extract_and_decode_urls(
                big_html, sent_video_dict["run_id"], database_connection
            )
            try:
                ad_info = parse_urls_for_known_parts(
                    all_urls, database_connection, sent_video_dict["pub_store_id"]
                )
            except MultipleAdvertiserIdError:
                error_msg = "multiple adv_store_id found for doubleclick"
                logger.error(error_msg)
                return ad_info, error_msg
        elif "slots" in google_response:
            slot_adv = None
            for slot in google_response["slots"]:
                if slot_adv is not None:
                    continue
                if video_id in str(slot):
                    for ad in slot["ads"]:
                        if video_id in str(ad):
                            all_urls = extract_and_decode_urls(
                                str(slot),
                                run_id=sent_video_dict["run_id"],
                                database_connection=database_connection,
                            )
                            try:
                                ad_info = parse_urls_for_known_parts(
                                    all_urls,
                                    database_connection,
                                    sent_video_dict["pub_store_id"],
                                )
                            except MultipleAdvertiserIdError:
                                error_msg = (
                                    "multiple adv_store_id found for doubleclick"
                                )
                                logger.error(error_msg)
                                return ad_info, error_msg
                            if ad_info["adv_store_id"] is not None:
                                slot_adv = True
            if slot_adv is None:
                error_msg = "doubleclick failing to parse for slots response"
                logger.error(error_msg)
                return ad_info, error_msg
        else:
            error_msg = "doubleclick new format"
            logger.error(error_msg)
            return ad_info, error_msg
    except json.JSONDecodeError:
        if (
            response_text[0:14] == "<?xml version="
            or response_text[0:15] == "<!DOCTYPE html>"
            or response_text[0:15] == "document.write("
            or response_text[0:3] == "if "
        ):
            # These are usually HTML web ads or minified JS and I havent successfully seen any yet
            # XML does have apps
            all_urls = extract_and_decode_urls(
                response_text,
                run_id=sent_video_dict["run_id"],
                database_connection=database_connection,
            )
            try:
                ad_info = parse_urls_for_known_parts(
                    all_urls, database_connection, sent_video_dict["pub_store_id"]
                )
            except MultipleAdvertiserIdError as e:
                error_msg = f"multiple adv_store_id found for doubleclick: {e.found_adv_store_ids}"
                logger.error(error_msg)
                return ad_info, error_msg
        else:
            error_msg = "doubleclick new format"
            logger.error(error_msg)
            return ad_info, error_msg
    return ad_info, error_msg


def add_is_creative_content_column(df: pd.DataFrame) -> pd.DataFrame:
    is_creative_content_response = (
        df["response_content_type"]
        .fillna("")
        .str.contains(
            r"\b(?:image|video)/(?:jpeg|jpg|png|gif|webp|webm|mp4|mpeg|avi|quicktime)\b",
            case=False,
            regex=True,
        )
    )
    is_creative_content_request = (
        df["response_content_type"]
        .fillna("")
        .str.contains(
            r"\b(?:image|video)/(?:jpeg|jpg|png|gif|webp|webm|mp4|mpeg|avi|quicktime)\b",
            case=False,
            regex=True,
        )
    )
    is_creative_content = is_creative_content_response | is_creative_content_request
    df["is_creative_content"] = is_creative_content
    return df


def extract_frame_at(local_path: pathlib.Path, second: int) -> Image.Image:
    tmp_path = pathlib.Path(f"/tmp/frame_{uuid.uuid4()}.jpg")
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-ss",
            str(second),
            "-i",
            str(local_path),
            "-vframes",
            "1",
            "-q:v",
            "2",
            "-f",
            "image2",
            str(tmp_path),
        ],
        check=True,
    )
    img = Image.open(tmp_path)
    return img


def average_hashes(hashes):
    # imagehash returns a numpy-like object; sum and majority voting works
    bits = sum([h.hash.astype(int) for h in hashes])
    majority = (bits >= (len(hashes) / 2)).astype(int)
    return str(imagehash.ImageHash(majority))


def compute_phash_multiple_frames(local_path: pathlib.Path, seconds: list[int]) -> str:
    hashes = []
    for second in seconds:
        try:
            hashes.append(imagehash.phash(extract_frame_at(local_path, second)))
        except Exception:
            pass
    phash = average_hashes(hashes)
    return str(phash)


def get_phash(md5_hash: str, adv_store_id: str, file_extension: str) -> str:
    phash = None
    local_path = (
        pathlib.Path(CREATIVES_DIR, adv_store_id) / f"{md5_hash}.{file_extension}"
    )
    seekable_formats = {"mp4", "webm", "gif"}
    if file_extension in seekable_formats:
        try:
            seconds = [1, 3, 5, 10]
            phash = str(compute_phash_multiple_frames(local_path, seconds))
        except Exception:
            logger.error("Failed to compute multiframe phash")
    if phash is None:
        phash = str(imagehash.phash(Image.open(local_path)))
    return phash


def store_creatives(row: pd.Series, adv_store_id: str, file_extension: str) -> str:
    thumbnail_width = 320
    local_dir = pathlib.Path(CREATIVES_DIR, adv_store_id)
    local_dir.mkdir(parents=True, exist_ok=True)
    thumbs_dir = CREATIVES_DIR / "thumbs"
    thumbs_dir.mkdir(parents=True, exist_ok=True)
    md5_hash = hashlib.md5(row["response_content"]).hexdigest()
    local_path = local_dir / f"{md5_hash}.{file_extension}"
    with open(local_path, "wb") as creative_file:
        creative_file.write(row["response_content"])
    thumb_path = thumbs_dir / f"{md5_hash}.jpg"
    # Only generate thumbnail if not already present
    seekable_formats = {"mp4", "webm", "gif"}
    static_formats = {"jpg", "jpeg", "png", "webp"}
    if not thumb_path.exists():
        try:
            ext = file_extension.lower()
            if ext in seekable_formats:
                try:
                    # Attempt to extract a thumbnail at 5s (works for video or animated gif/webp)
                    subprocess.run(
                        [
                            "ffmpeg",
                            "-y",
                            "-ss",
                            "5",
                            "-i",
                            str(local_path),
                            "-vframes",
                            "1",
                            "-vf",
                            f"scale={thumbnail_width}:-1",
                            "-q:v",
                            "2",
                            "-update",
                            "1",
                            str(thumb_path),
                        ],
                        check=True,
                        stdout=subprocess.DEVNULL,
                    )
                except subprocess.CalledProcessError:
                    # Fallback: use first frame (or static image frame)
                    subprocess.run(
                        [
                            "ffmpeg",
                            "-y",
                            "-i",
                            str(local_path),
                            "-vframes",
                            "1",
                            "-vf",
                            f"scale={thumbnail_width}:-1",
                            "-q:v",
                            "2",
                            "-update",
                            "1",
                            str(thumb_path),
                        ],
                        check=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
            elif ext in static_formats:
                # Static images: no need to seek, just resize
                subprocess.run(
                    [
                        "ffmpeg",
                        "-y",
                        "-i",
                        str(local_path),
                        "-vf",
                        f"scale={thumbnail_width}:-1",
                        "-q:v",
                        "2",
                        "-update",
                        "1",
                        str(thumb_path),
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                )
            else:
                logger.error(f"Unknown file extension: {file_extension} for thumbnail!")
        except Exception:
            logger.error(f"Failed to create thumbnail for {local_path}")
            pass
    return md5_hash


def filter_creatives(df: pd.DataFrame) -> pd.DataFrame:
    status_code_200 = df["status_code"] == 200
    df = add_is_creative_content_column(df)
    creatives_df = df[(df["is_creative_content"]) & status_code_200].copy()
    creatives_df["creative_size"] = creatives_df["response_size"].fillna(0).astype(int)
    creatives_df = creatives_df[creatives_df["creative_size"] > 50000]
    creatives_df = creatives_df[creatives_df["response_content"].notna()]
    creatives_df["actual_size"] = creatives_df["response_content"].str.len()
    size_diff = abs(creatives_df["creative_size"] - creatives_df["actual_size"])
    creatives_df["size_match"] = (size_diff <= ABS_TOL) | (
        size_diff / creatives_df["creative_size"] <= PCT_TOL
    )
    creatives_df = creatives_df[creatives_df["size_match"]]
    # Lots of creatives of the publishing app icon
    # If this cuts out the advertisers icon as well that seems OK?
    widths = creatives_df["response_content"].apply(
        lambda x: struct.unpack(">II", x[16:24])[0]
        if x.startswith(b"\x89PNG\r\n\x1a\n")
        else None
    )
    heights = creatives_df["response_content"].apply(
        lambda x: struct.unpack(">II", x[16:24])[1]
        if x.startswith(b"\x89PNG\r\n\x1a\n")
        else None
    )
    is_square = widths == heights
    is_png = creatives_df["file_extension"] == "png"
    is_googleusercontent = creatives_df["tld_url"] == "googleusercontent.com"
    creatives_df = creatives_df[~(is_png & is_googleusercontent & is_square)]
    return creatives_df


def add_file_extension(df: pd.DataFrame) -> pd.DataFrame:
    df["file_extension"] = df["url"].apply(lambda x: x.split(".")[-1])
    ext_too_long = (df["file_extension"].str.len() > 4) & (
        df["response_content_type"].fillna("").str.contains("/")
    )

    def get_subtype(x):
        parts = x.split("/")
        return parts[1] if len(parts) > 1 else None

    df["file_extension"] = np.where(
        ext_too_long,
        df["response_content_type"].fillna("").apply(get_subtype),
        df["file_extension"],
    )
    return df


def parse_sent_video_df(
    row: pd.Series,
    pub_store_id: str,
    sent_video_df: pd.DataFrame,
    database_connection: PostgresCon,
    video_id: str,
) -> tuple[list[AdInfo], list[str]]:
    error_messages = []
    run_id = row["run_id"]
    sent_video_dicts = sent_video_df.to_dict(orient="records")
    found_ad_infos = []
    for sent_video_dict in sent_video_dicts:
        init_url = sent_video_dict["url"]
        init_tld = (
            tldextract.extract(init_url).domain
            + "."
            + tldextract.extract(init_url).suffix
        )
        if "vungle.com" in init_url:
            ad_info = parse_vungle_ad(sent_video_dict, database_connection)
        elif "bidmachine.io" in init_url:
            ad_info = parse_bidmachine_ad(sent_video_dict, database_connection)
        elif (
            "fyber.com" in init_url
            or "tpbid.com" in init_url
            or "inner-active.mobi" in init_url
        ):
            ad_info = parse_fyber_ad(sent_video_dict, database_connection)
        elif "everestop.io" in init_url:
            ad_info = parse_everestop_ad(sent_video_dict)
        elif "doubleclick.net" in init_url:
            ad_info, error_msg = parse_google_ad(
                sent_video_dict, video_id, database_connection
            )
            if error_msg:
                row["error_msg"] = error_msg
                error_messages.append(row)
                continue
        elif "unityads.unity3d.com" in init_url:
            ad_info, error_msg = parse_unity_ad(sent_video_dict, database_connection)
            if error_msg:
                row["error_msg"] = error_msg
                error_messages.append(row)
                continue
        elif "mtgglobals.com" in init_url:
            ad_info = parse_mtg_ad(sent_video_dict, database_connection)
        elif "yandex.ru" in init_url:
            ad_info = parse_yandex_ad(sent_video_dict, database_connection, video_id)
        elif "youappi.com" in init_url:
            ad_info = parse_youappi_ad(sent_video_dict, database_connection)
        else:
            real_tld = get_tld(init_url)
            error_msg = f"Not a recognized ad network: {real_tld}"
            logger.warning(f"{error_msg} for video {video_id[0:10]}")
            row["error_msg"] = error_msg
            error_messages.append(row)
            ad_info, error_msg = parse_generic_adnetwork(
                sent_video_dict, database_connection
            )
            if error_msg:
                row["error_msg"] = error_msg
                error_messages.append(row)
                continue
        if ad_info["adv_store_id"] is None:
            text = sent_video_dict["response_text"]
            all_urls = extract_and_decode_urls(text, run_id, database_connection)
            if any([x in all_urls for x in MMP_TLDS + PLAYSTORE_URL_PARTS]):
                error_msg = "found potential app! mmp or playstore"
                row["error_msg"] = error_msg
                error_messages.append(row)
                continue
            ad_parts = parse_urls_for_known_parts(
                all_urls, database_connection, sent_video_dict["pub_store_id"]
            )
            if ad_parts["adv_store_id"] is not None:
                error_msg = "found potential app! adv_store_id"
                row["error_msg"] = error_msg
                error_messages.append(row)
                continue
            else:
                ad_info["adv_store_id"] = "unknown"
        if ad_info["adv_store_id"] == pub_store_id:
            error_msg = "Incorrect adv_store_id, identified pub ID as adv ID"
            logger.error(
                f"Incorrect adv_store_id, identified pub ID as adv ID for video {video_id[0:10]}"
            )
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        if ad_info["adv_store_id"] == "unknown":
            error_msg = f"Unknown adv_store_id for {init_tld=} {video_id=}"
            logger.error(error_msg)
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        try:
            adv_db_id = query_store_app_by_store_id(
                store_id=ad_info["adv_store_id"],
                database_connection=database_connection,
                case_insensitive=True,
            )
            ad_info["adv_store_app_id"] = adv_db_id
        except Exception:
            error_msg = f"found potential app! but failed to get db id {ad_info['adv_store_id']}"
            logger.error(error_msg)
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        ad_info["init_tld"] = init_tld
        found_ad_infos.append(ad_info)
    return found_ad_infos, error_messages


def get_video_id(row: pd.Series) -> str:
    if "2mdn" in row["tld_url"]:
        if "/id/" in row["url"]:
            url_parts = urllib.parse.urlparse(row["url"])
            video_id = url_parts.path.split("/id/")[1].split("/")[0]
        elif "simgad" in row["url"]:
            video_id = row["url"].split("/")[-1]
        else:
            url_parts = urllib.parse.urlparse(row["url"])
            video_id = url_parts.path.split("/")[-1]
    elif "googlevideo" in row["tld_url"]:
        url_parts = urllib.parse.urlparse(row["url"])
        query_params = urllib.parse.parse_qs(url_parts.query)
        video_id = query_params["ei"][0]
    elif row["tld_url"] == "unity3dusercontent.com":
        # this isn't the video name, but the id of the content as the name is the quality
        video_id = row["url"].split("/")[-2]
    elif row["tld_url"] == "adcolony.com":
        video_id = row["url"].split("/")[-2]
        if len(video_id) < 10:
            video_id = row["url"].split("/")[-1]
    elif "bigabidserv.com" in row["tld_url"]:
        video_id = row["url"].split("/")[-1]
        if "." in video_id:
            video_id = video_id.split(".")[0]
    elif "yandex.net" in row["tld_url"]:
        # /id123/orig
        video_id = row["url"].split("/")[-2]
    else:
        url_parts = urllib.parse.urlparse(row["url"])
        video_id = url_parts.path.split("/")[-1]
    return video_id


def attribute_creatives(
    df: pd.DataFrame,
    creatives_df: pd.DataFrame,
    pub_store_id: str,
    database_connection: PostgresCon,
) -> tuple[pd.DataFrame, list]:
    error_messages = []
    i = 0
    adv_creatives = []
    row_count = creatives_df.shape[0]
    for _i, row in creatives_df.iterrows():
        i += 1
        host_ad_network_tld = get_tld(row["tld_url"])
        video_id = get_video_id(row)
        if video_id == "":
            error_msg = "Bad creative parsing video ID is empty"
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        file_extension = row["file_extension"]
        # store_creatives(row, 'cheeck', file_extension)
        if video_id in IGNORE_CREATIVE_IDS:
            logger.info(f"Ignoring video {video_id}.{file_extension}")
            continue
        sent_video_df = get_sent_video_df(df, row, video_id)
        if sent_video_df is None or sent_video_df.empty:
            error_msg = f"No source bidrequest found for {row['tld_url']}"
            logger.error(f"{error_msg} {video_id}")
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        sent_video_df["pub_store_id"] = pub_store_id
        found_ad_infos, found_error_messages = parse_sent_video_df(
            row, pub_store_id, sent_video_df, database_connection, video_id
        )
        for found_error_message in found_error_messages:
            row_copy = row.copy()
            row_copy["error_msg"] = found_error_message["error_msg"]
            error_messages.append(row_copy)
        found_ad_infos = [x for x in found_ad_infos if x["adv_store_id"] != "unknown"]
        found_advs = list(set([x["adv_store_id"] for x in found_ad_infos]))
        mmp_tlds = [
            x["mmp_tld"] for x in found_ad_infos if x["found_mmp_urls"] is not None
        ]
        mmp_tlds = list(set([x for x in mmp_tlds if x is not None]))
        mmp_tld = None
        if len(mmp_tlds) > 0:
            mmp_tld = mmp_tlds[0]
            if len(mmp_tlds) > 1:
                error_msg = "Multiple mmp_urls"
                logger.error(f"{error_msg} for {row['tld_url']} {video_id}")
                row["error_msg"] = error_msg
                error_messages.append(row)
        found_advs = list(set(found_advs))
        if len(found_advs) > 1:
            error_msg = "Found potential app! Multiple adv_store_id"
            logger.error(f"{error_msg} for {row['tld_url']} {video_id}")
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        elif len(found_advs) == 0:
            error_msg = f"No adv_store_id found for {row['tld_url']} {video_id=}"
            logger.error(error_msg)
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        else:
            adv_store_id = found_advs[0]
        found_mmp_urls = [
            x["found_mmp_urls"]
            for x in found_ad_infos
            if x["found_mmp_urls"] is not None
        ]
        found_mmp_urls = list(
            set([item for sublist in found_mmp_urls for item in sublist])
        )
        found_ad_network_tlds = [
            x["found_ad_network_tlds"]
            for x in found_ad_infos
            if x["found_ad_network_tlds"] is not None
        ]
        found_ad_network_tlds = list(
            set([item for sublist in found_ad_network_tlds for item in sublist])
        )
        try:
            md5_hash = store_creatives(
                row,
                adv_store_id=adv_store_id,
                file_extension=file_extension,
            )
        except Exception:
            error_msg = "Found potential creative but failed to store!"
            logger.error(error_msg)
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        try:
            phash = get_phash(
                md5_hash=md5_hash,
                adv_store_id=adv_store_id,
                file_extension=file_extension,
            )
        except Exception:
            error_msg = "Found potential creative but failed to compute phash"
            logger.error(f"{error_msg} for {row['tld_url']} {video_id}")
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        if phash is None:
            error_msg = "Found potential creative but failed to compute phash"
            logger.error(f"{error_msg} for {row['tld_url']} {video_id}")
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        init_tlds = [x["init_tld"] for x in found_ad_infos]
        init_tld = init_tlds[0]
        adv_store_app_ids = [x["adv_store_app_id"] for x in found_ad_infos]
        adv_store_app_id = adv_store_app_ids[0]
        adv_creatives.append(
            {
                "pub_store_id": pub_store_id,
                "adv_store_id": adv_store_id,
                "adv_store_app_id": adv_store_app_id,
                "host_ad_network_tld": host_ad_network_tld,
                "creative_initial_domain_tld": init_tld,
                "mmp_urls": found_mmp_urls,
                "found_ad_network_tlds": found_ad_network_tlds,
                "mmp_tld": mmp_tld,
                "md5_hash": md5_hash,
                "phash": phash,
                "file_extension": file_extension,
            }
        )
        logger.debug(
            f"{i}/{row_count}: {host_ad_network_tld} adv={adv_store_id} init_tld={init_tld}"
        )
    adv_creatives_df = pd.DataFrame(adv_creatives)
    if adv_creatives_df.empty:
        msg = (
            f"No matched for creatives {pub_store_id}, {len(error_messages)} unmatched"
        )
        logger.warning(msg)
    return adv_creatives_df, error_messages


def upload_creatives(adv_creatives_df: pd.DataFrame) -> None:
    upload_creatives_to_s3(adv_creatives_df)


def upload_creatives_to_s3(adv_creatives_df: pd.DataFrame) -> None:
    for adv_id, adv_creatives_df_adv in adv_creatives_df.groupby("adv_store_id"):
        try:
            s3_keys = get_app_creatives_s3_keys(store=1, store_id=adv_id)
            s3_keys_md5_hashes = s3_keys["md5_hash"].to_numpy()
        except FileNotFoundError:
            s3_keys_md5_hashes = []
        for _i, row in adv_creatives_df_adv.iterrows():
            if row["md5_hash"] in s3_keys_md5_hashes:
                logger.info(f"Creative {row['md5_hash']} already in S3")
                continue
            upload_ad_creative_to_s3(
                store=1,
                adv_store_id=adv_id,
                md5_hash=row["md5_hash"],
                extension=row["file_extension"],
            )
            logger.info(f"Uploaded {row['md5_hash']} to S3")


def get_latest_local_mitm(
    store_id: str, force_re_download: bool = False
) -> tuple[pathlib.Path, int]:
    mitm_logs = list(pathlib.Path(MITM_DIR).glob(f"{store_id}_*.log"))
    if mitm_logs and not force_re_download:
        max_run_id = 0
        mitm_log_path = None
        for mitm_log in mitm_logs:
            run_id = mitm_log.as_posix().split("_")[-1].replace(".log", "")
            if int(run_id) > max_run_id:
                max_run_id = int(run_id)
                mitm_log_path = mitm_log
    else:
        mitm_log_path = download_mitm_log_by_store_id(store_id)
    logger.debug(f"Using {mitm_log_path} for {store_id}")
    run_id = mitm_log_path.as_posix().split("_")[-1].replace(".log", "")
    run_id = int(run_id)
    return mitm_log_path, run_id


def append_missing_domains(
    ad_domains_df: pd.DataFrame,
    creative_records_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    check_cols = ["creative_initial_domain_tld", "host_ad_network_tld"]
    for col in check_cols:
        missing_ad_domains = creative_records_df[
            ~creative_records_df[col].isin(ad_domains_df["domain"])
        ]
        if not missing_ad_domains.empty:
            new_ad_domains = (
                missing_ad_domains[[col]]
                .drop_duplicates()
                .rename(columns={col: "domain"})
            )
            new_ad_domains = upsert_df(
                table_name="ad_domains",
                df=new_ad_domains,
                insert_columns=["domain"],
                key_columns=["domain"],
                database_connection=database_connection,
                return_rows=True,
            )
            ad_domains_df = pd.concat([new_ad_domains, ad_domains_df])
    return ad_domains_df


def add_additional_domain_id_column(
    creative_records_df: pd.DataFrame, ad_domains_df: pd.DataFrame
) -> pd.DataFrame:
    cr = creative_records_df.copy()

    # Ensure missing values in the list column become empty lists
    cr["found_ad_network_tlds"] = cr["found_ad_network_tlds"].apply(
        lambda x: x if isinstance(x, list) else []
    )

    # Explode and keep the original row index in a column
    exploded = (
        cr.explode("found_ad_network_tlds")
        .reset_index()
        .rename(columns={"index": "orig_idx"})
    )

    # Merge on the exploded domain value
    merged = exploded.merge(
        ad_domains_df[["domain", "id"]],
        left_on="found_ad_network_tlds",
        right_on="domain",
        how="left",
    )

    # Group back the matching ids by the original row index
    grouped = merged.groupby("orig_idx")["id"].apply(
        lambda ids: [int(i) for i in ids.dropna().unique()]
    )

    # Ensure every original row has an entry (empty list if no matches)
    grouped = grouped.reindex(cr.index, fill_value=[])

    # Assign back (aligned by index)
    cr["additional_ad_domain_ids"] = grouped.values

    return cr


def make_creative_records_df(
    adv_creatives_df: pd.DataFrame,
    assets_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    creative_records_df = adv_creatives_df.merge(
        assets_df[["store_app_id", "md5_hash", "creative_asset_id"]],
        left_on=["adv_store_app_id", "md5_hash"],
        right_on=["store_app_id", "md5_hash"],
        how="left",
    )
    ad_domains_df = query_ad_domains(database_connection=database_connection)
    ad_domains_df = append_missing_domains(
        ad_domains_df, creative_records_df, database_connection
    )
    creative_records_df = add_additional_domain_id_column(
        creative_records_df, ad_domains_df
    )
    creative_records_df = creative_records_df.merge(
        ad_domains_df[["id", "domain"]].rename(columns={"id": "mmp_domain_id"}),
        left_on="mmp_tld",
        right_on="domain",
        how="left",
    )
    creative_records_df = creative_records_df.merge(
        ad_domains_df[["id", "domain"]].rename(
            columns={"id": "creative_host_domain_id"}
        ),
        left_on="host_ad_network_tld",
        right_on="domain",
        how="left",
    )
    creative_records_df = creative_records_df.merge(
        ad_domains_df[["id", "domain"]].rename(
            columns={"id": "creative_initial_domain_id"}
        ),
        left_on="creative_initial_domain_tld",
        right_on="domain",
        how="left",
    )
    creative_records_df = creative_records_df[
        [
            "store_app_pub_id",
            "creative_asset_id",
            "run_id",
            "creative_initial_domain_id",
            "creative_host_domain_id",
            "mmp_domain_id",
            "mmp_urls",
            "additional_ad_domain_ids",
        ]
    ]
    check_cols = ["creative_host_domain_id", "mmp_domain_id"]
    for col in check_cols:
        creative_records_df[col] = np.where(
            creative_records_df[col].isna(), None, creative_records_df[col]
        )
    return creative_records_df


def get_creatives_df(
    pub_store_id: str, run_id: int, database_connection: PostgresCon
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    df = parse_mitm_log(pub_store_id, run_id, database_connection)
    error_msg = None
    if df.empty:
        error_msg = "No data in mitm df"
        logger.error(error_msg)
        return pd.DataFrame(), pd.DataFrame(), error_msg
    if "status_code" not in df.columns:
        error_msg = "No status code found in df, skipping"
        logger.error(error_msg)
        return pd.DataFrame(), pd.DataFrame(), error_msg
    df = add_file_extension(df)
    creatives_df = filter_creatives(df)
    if creatives_df.empty:
        error_msg = "No creatives to check"
        logger.error(error_msg)
        return df, pd.DataFrame(), error_msg
    return df, creatives_df, error_msg


def parse_store_id_mitm_log(
    pub_store_id: str,
    run_id: int,
    database_connection: PostgresCon,
) -> list:
    df, creatives_df, error_message = get_creatives_df(
        pub_store_id, run_id, database_connection
    )
    if error_message:
        logger.error(error_message)
        error_message_info = {
            "run_id": run_id,
            "pub_store_id": pub_store_id,
            "error_msg": error_message,
        }
        return [error_message_info]
    adv_creatives_df, error_messages = attribute_creatives(
        df, creatives_df, pub_store_id, database_connection
    )
    if adv_creatives_df.empty:
        if len(error_messages) == 0:
            error_msg = "No creatives or errors"
            logger.error(f"{error_msg}")
            row = {
                "run_id": run_id,
                "pub_store_id": pub_store_id,
                "error_msg": error_msg,
            }
            error_messages.append(row)
        return error_messages
    else:
        msg = "Success found creatives!"
        row = {
            "run_id": run_id,
            "pub_store_id": pub_store_id,
            "error_msg": msg,
        }
        error_messages.append(row)
    pub_db_id = query_store_app_by_store_id(
        store_id=pub_store_id, database_connection=database_connection
    )
    adv_creatives_df["store_app_pub_id"] = pub_db_id
    adv_creatives_df["run_id"] = run_id
    assets_df = adv_creatives_df[
        ["adv_store_app_id", "md5_hash", "file_extension", "phash"]
    ].drop_duplicates()
    assets_df = assets_df.rename(columns={"adv_store_app_id": "store_app_id"})
    assets_df = upsert_df(
        assets_df,
        table_name="creative_assets",
        database_connection=database_connection,
        key_columns=["store_app_id", "md5_hash"],
        insert_columns=["store_app_id", "md5_hash", "file_extension", "phash"],
        return_rows=True,
    )
    assets_df = assets_df.rename(columns={"id": "creative_asset_id"})
    creative_records_df = make_creative_records_df(
        adv_creatives_df, assets_df, database_connection
    )
    key_columns = [
        "store_app_pub_id",
        "creative_asset_id",
        "run_id",
        "creative_initial_domain_id",
        "creative_host_domain_id",
    ]
    creative_records_df["updated_at"] = datetime.datetime.now()
    upsert_df(
        creative_records_df,
        table_name="creative_records",
        database_connection=database_connection,
        key_columns=key_columns,
        insert_columns=key_columns
        + [
            "updated_at",
            "mmp_domain_id",
            "additional_ad_domain_ids",
            "mmp_urls",
        ],
    )
    upload_creatives(adv_creatives_df)
    return error_messages


def parse_all_runs_for_store_id(pub_store_id: str, database_connection: PostgresCon):
    mitms = get_store_id_mitm_s3_keys(store_id=pub_store_id)
    for _i, mitm in mitms.iterrows():
        run_id = mitm["run_id"]
        try:
            error_messages = parse_store_id_mitm_log(
                pub_store_id, run_id, database_connection
            )
        except Exception:
            error_msg = "CRITICAL uncaught error"
            logger.exception(f"{error_msg}")
            row = {
                "run_id": run_id,
                "pub_store_id": pub_store_id,
                "error_msg": error_msg,
            }
            error_messages = [row]
        if len(error_messages) == 0:
            continue
        d = [x for x in error_messages if type(x) is dict]
        s = [x for x in error_messages if type(x) is pd.Series]
        error_msg_df = pd.concat([pd.DataFrame(d), pd.DataFrame(s)], ignore_index=True)
        mycols = [
            x
            for x in error_msg_df.columns
            if x
            in [
                "url",
                "tld_url",
                "path",
                "content_type",
                "run_id",
                "pub_store_id",
                "file_extension",
                "creative_size",
                "error_msg",
            ]
        ]
        error_msg_df = error_msg_df[mycols]
        log_creative_scan_results(error_msg_df, database_connection)


def scan_all_apps(
    database_connection: PostgresCon, limit_store_apps_no_creatives: bool = True
) -> None:
    apps_to_scan = query_apps_to_creative_scan(database_connection=database_connection)
    if limit_store_apps_no_creatives:
        store_apps_no_creatives = query_store_apps_no_creatives(
            database_connection=database_connection
        )
        store_apps_no_creatives["no_creatives"] = True
        store_apps_no_creatives["run_id"] = store_apps_no_creatives["run_id"].astype(
            int
        )
        filtered_apps = pd.merge(
            apps_to_scan,
            store_apps_no_creatives,
            left_on=["store_id", "run_id"],
            right_on=["pub_store_id", "run_id"],
            how="left",
        )
        apps_to_scan = filtered_apps[filtered_apps["no_creatives"].isna()]
        apps_to_scan = apps_to_scan[["store_id", "api_calls"]].drop_duplicates()
    apps_count = apps_to_scan.shape[0]
    for i, app in apps_to_scan.iterrows():
        pub_store_id = app["store_id"]
        logger.info(f"{i}/{apps_count}: {pub_store_id} start")
        try:
            parse_all_runs_for_store_id(pub_store_id, database_connection)
        except Exception as e:
            logger.exception(f"Error parsing {pub_store_id}: {e}")
            continue
    subprocess.run(
        [
            "s3cmd",
            "sync",
            str(CREATIVES_DIR / "thumbs") + "/",
            "s3://appgoblin-data/creatives/thumbs/",
            "--acl-public",
        ],
        check=False,
    )


def download_all_mitms(database_connection: PostgresCon) -> None:
    apps_to_download = query_apps_to_creative_scan(
        database_connection=database_connection
    )
    for i, app in apps_to_download.iterrows():
        logger.info(f"{i}/{apps_to_download.shape[0]}: {app['store_id']} start")
        pub_store_id = app["store_id"]
        # Check if any log files exist for this pub_store_id
        if list(pathlib.Path(MITM_DIR).glob(f"{pub_store_id}_*.log")):
            logger.info(f"{pub_store_id} already downloaded")
            continue
        try:
            mitms = get_store_id_mitm_s3_keys(store_id=pub_store_id)
        except FileNotFoundError:
            logger.error(f"{pub_store_id} not found in s3")
            continue
        for _i, mitm in mitms.iterrows():
            key = mitm["key"]
            run_id = mitm["run_id"]
            filename = f"{pub_store_id}_{run_id}.log"
            if not pathlib.Path(MITM_DIR, filename).exists():
                _mitm_log_path = download_mitm_log_by_key(key, filename)
            else:
                pass


def open_all_local_mitms(database_connection: PostgresCon) -> pd.DataFrame:
    all_mitms_df = pd.DataFrame()
    i = 0
    num_files = len(list(pathlib.Path(MITM_DIR).glob("*.log")))
    logger.info(f"Opening {num_files} local mitm logs")
    for mitm_log_path in pathlib.Path(MITM_DIR).glob("*.log"):
        i += 1
        logger.info(f"{i}/{num_files}: {mitm_log_path}")
        pub_store_id = mitm_log_path.name.split("_")[0]
        run_id = mitm_log_path.name.split("_")[1].replace(".log", "")
        df = parse_mitm_log(pub_store_id, run_id, database_connection)
        if "response_content_type" in df.columns:
            df = add_is_creative_content_column(df)
            df["response_text"] = np.where(
                df["is_creative_content"], "", df["response_text"]
            )
            df["response_content"] = np.where(
                df["is_creative_content"], "", df["response_content"]
            )
        if "response_size" in df.columns:
            df["response_text"] = np.where(
                df["response_size"].fillna("0").astype(int) > 500000,
                "",
                df["response_text"],
            )
            df["response_content"] = np.where(
                df["response_size"].fillna("0").astype(int) > 500000,
                "",
                df["response_content"],
            )
        df["pub_store_id"] = pub_store_id
        df["run_id"] = run_id
        all_mitms_df = pd.concat([all_mitms_df, df], ignore_index=True)
    return all_mitms_df


def upload_all_mitms_to_s3() -> None:
    bucket_name = "appgoblin-data"
    client = get_cloud_s3_client()
    client.upload_file(
        "all_mitms.tsv.xz", Bucket=bucket_name, Key="mitmcsv/all_mitms.tsv.xz"
    )
    os.system("s3cmd setacl s3://appgoblin-data/mitmcsv/ --acl-public --recursive")


def get_cloud_s3_client():
    """Create and return an S3 client."""
    session = boto3.session.Session()
    return session.client(
        "s3",
        region_name="sgp1",
        endpoint_url="https://sgp1.digitaloceanspaces.com",
        aws_access_key_id=CONFIG["digi-cloud"]["access_key_id"],
        aws_secret_access_key=CONFIG["digi-cloud"]["secret_key"],
    )
