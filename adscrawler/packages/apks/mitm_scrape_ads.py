import dataclasses
import datetime
import hashlib
import html
import json
import pathlib
import re
import subprocess
import urllib
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import protod
import requests
import tldextract
from bs4 import BeautifulSoup
from mitmproxy import http
from mitmproxy.io import FlowReader
from protod import Renderer

from adscrawler.config import CREATIVES_DIR, MITM_DIR, get_logger
from adscrawler.dbcon.connection import PostgresCon
from adscrawler.dbcon.queries import (
    log_creative_scan_results,
    query_ad_domains,
    query_apps_to_creative_scan,
    query_store_app_by_store_id,
    upsert_df,
)
from adscrawler.packages.storage import (
    download_mitm_log_by_key,
    download_mitm_log_by_store_id,
    get_app_creatives_s3_keys,
    get_store_id_mitm_s3_keys,
    upload_ad_creative_to_s3,
)

logger = get_logger(__name__, "mitm_scrape_ads")


IGNORE_CREATIVE_IDS = ["privacy", "google_play_icon_grey_2022", "favicon"]

MMP_TLDS = [
    "appsflyer.com",
    "adjust.com",
    "adjust.io",
    "singular.com",
    "singular.net",
    "kochava.com",
    "openattribution.dev",
    "airbridge.com",
    "arb.ge",
    "branch.io",
    "impression.link",
]

PLAYSTORE_URL_PARTS = ["play.google.com/store", "market://", "intent://"]


@dataclasses.dataclass
class AdInfo:
    adv_store_id: str
    ad_network_tld: str
    mmp_url: str | None = None

    def __getitem__(self, key: str):
        """Support dictionary-style access to dataclass fields"""
        return getattr(self, key)

    def __setitem__(self, key: str, value):
        """Support dictionary-style assignment to dataclass fields"""
        setattr(self, key, value)

    @property
    def mmp_tld(self) -> str | None:
        if self.mmp_url:
            return (
                tldextract.extract(self.mmp_url).domain
                + "."
                + tldextract.extract(self.mmp_url).suffix
            )
        return None


def parse_mitm_log(
    mitm_log_path: pathlib.Path,
) -> pd.DataFrame:
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
                        # Extract useful data from each flow
                        request_data = {
                            "start_time": flow.timestamp_start,
                            "method": flow.request.method,
                            "url": flow.request.pretty_url,
                            "tld_url": tldextract.extract(
                                flow.request.pretty_url
                            ).domain
                            + "."
                            + tldextract.extract(flow.request.pretty_url).suffix,
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
                        try:
                            request_data["request_text"] = flow.request.get_text()
                        except Exception:
                            request_data["request_text"] = ""
                        try:
                            request_data["content"] = flow.request.get_text()
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
                            try:
                                request_data["response_content"] = flow.response.content
                                response_text = flow.response.get_text()
                                request_data["response_text"] = response_text
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
    return df


def get_first_sent_video_df(
    df: pd.DataFrame, row: pd.Series, video_id: str
) -> pd.Series | None:
    sent_video_df = df[
        (df["response_text"].astype(str).str.contains(video_id))
        & (df["start_time"] <= row.start_time)
    ].copy()
    if sent_video_df.empty:
        sent_video_df = df[
            (df["response_text"].astype(str).str.contains(video_id))
        ].copy()
    if sent_video_df.empty:
        return None
    if sent_video_df.shape[0] > 1:
        logger.info("Multiple responses for video found, selecting 1")
        sent_video_df = sent_video_df.head(1).reset_index()
    sent_video_df["tld_url"] = sent_video_df["url"].apply(lambda x: get_tld(x))
    return sent_video_df


def extract_and_decode_urls(text: str):
    """
    Extracts all URLs from a given text, handles HTML entities and URL encoding.
    """
    soup = BeautifulSoup(text, "html.parser")
    # The URLs are located inside a <meta> tag with name="video_fields"
    # The content of this tag contains VAST XML data.
    video_fields_meta = soup.find("meta", {"name": "video_fields"})
    vast_urls = []
    if video_fields_meta:
        vast_xml_string = html.unescape(video_fields_meta["content"])
        vast_urls = re.findall(r"<!\[CDATA\[(.*?)\]\]>", vast_xml_string)
    if soup.find("vast"):
        vast_urls = re.findall(r"<!\[CDATA\[(.*?)\]\]>", text)
        vast_urls = [x for x in vast_urls if "http" in x]
    # 1. Broad regex for URLs (http, https, fybernativebrowser, etc.)
    # This pattern is made more flexible to capture various URL formats
    urls = []
    text = html.unescape(text)
    url_pattern = re.compile(
        r"""(?:
        (?:https?|intent|market|fybernativebrowser):\/\/      # allowed schemes
        [^\s'"<>\]\)\}]+?                                     # non-greedy match
    )
    (?=[\s"'<>\]\)\},]|$)                                     # must be followed by separator or end
    """,
        re.VERBOSE,
    )
    found_urls = url_pattern.findall(text)
    for url in found_urls:
        # print("--------------------------------")
        # print(url)
        decoded_url = urllib.parse.unquote(url)
        urls.append(decoded_url)
        # print(decoded_url)
    all_urls = list(set(vast_urls + urls))
    return all_urls


def parse_fyber_html(inner_ad_element: str):
    # Extract all URLs from the raw HTML content first
    all_extracted_urls = extract_and_decode_urls(inner_ad_element)
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


def parse_fyber_ad_response(ad_response_text: str) -> list[str]:
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
                urls = parse_fyber_html(inner_ad_element)
        if vast_tree:
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
    except Exception:
        adv_store_id = None
    return adv_store_id


def parse_urls_for_known_parts(
    all_urls: list[str], database_connection: PostgresCon
) -> dict:
    mmp_url = None
    adv_store_id = None
    ad_network_url = None
    ad_network_tld = None
    ad_network_urls = query_ad_domains(database_connection=database_connection)
    for url in all_urls:
        tld_url = tldextract.extract(url).domain + "." + tldextract.extract(url).suffix
        if tld_url in MMP_TLDS:
            mmp_url = url
        elif match := re.search(r"intent://details\?id=([a-zA-Z0-9._]+)", url):
            adv_store_id = match.group(1)
        elif match := re.search(r"market://details\?id=([a-zA-Z0-9._]+)", url):
            adv_store_id = match.group(1)
        elif "play.google.com" in url and "google.com" in tld_url:
            resp_adv_id = adv_id_from_play_url(url)
            if not resp_adv_id:
                continue
            if adv_store_id:
                if adv_store_id != resp_adv_id:
                    raise Exception(
                        f"multiple adv_store_id found for {resp_adv_id=} & {adv_store_id=}"
                    )
            adv_store_id = resp_adv_id
        elif "appsflyer.com" in url:
            adv_store_id = re.search(r"http.*\.appsflyer\.com/([a-zA-Z0-9_.]+)\?", url)[
                1
            ]
            mmp_url = url
        elif tld_url in MMP_TLDS:
            mmp_url = url
        elif "tpbid.com" in url:
            url = url.replace("fybernativebrowser://navigate?url=", "")
            ad_network_url = url
            ad_network_tld = tld_url
            if "/click" in url:
                response = requests.get(url, allow_redirects=True)
                final_url = response.url
                if "play.google.com" in final_url:
                    resp_adv_id = adv_id_from_play_url(final_url)
                    if resp_adv_id:
                        adv_store_id = resp_adv_id
                else:
                    logger.error(f"Cant find advertiser Unrecognized click {url=}")
        elif "inner-active.mobi" in url:
            ad_network_url = url
            ad_network_tld = tld_url
        if tld_url in ad_network_urls["domain"].to_list():
            # last effort
            ad_network_url = url
            ad_network_tld = tld_url
        # Stop if we have found both URLs
        if ad_network_url and mmp_url and adv_store_id:
            break
    return {
        "mmp_url": mmp_url,
        "adv_store_id": adv_store_id,
        "ad_network_url": ad_network_url,
        "ad_network_tld": ad_network_tld,
    }


def parse_mtg_ad(sent_video_dict: dict) -> AdInfo:
    ad_network_tld = "mtgglobals.com"
    mmp_url = None
    ad_response_text = sent_video_dict["response_text"]
    ad_response = json.loads(ad_response_text)
    adv_store_id = ad_response["data"]["ads"][0]["package_name"]
    ad_info = AdInfo(
        adv_store_id=adv_store_id,
        ad_network_tld=ad_network_tld,
        mmp_url=mmp_url,
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


def parse_bidmachine_ad(
    sent_video_dict: dict, database_connection: PostgresCon
) -> AdInfo:
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
    mmp_url = None
    adv_store_id = None
    ad_network_tld = None
    try:
        adv_store_id = ret[5][6][3][13][2][3]
        ad_network_tld = ret[5][6][3][13][2][2]
    except Exception:
        pass
    if adv_store_id is None:
        text = str(ret[5][6][3][13][2][17])
        urls = extract_and_decode_urls(text)
        ad_parts = parse_urls_for_known_parts(urls, database_connection)
    else:
        ad_parts = {
            "adv_store_id": adv_store_id,
            "ad_network_tld": ad_network_tld,
            "mmp_url": mmp_url,
        }
    return ad_parts


def parse_everestop_ad(sent_video_dict: dict) -> AdInfo:
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
    adv_store_id = ret[5][6][3][13][2][3]
    ad_network_tld = ret[5][6][3][13][2][2]
    mmp_url = None
    ad_info = AdInfo(
        adv_store_id=adv_store_id,
        ad_network_tld=ad_network_tld,
        mmp_url=mmp_url,
    )
    return ad_info


def parse_unity_ad(sent_video_dict: dict, database_connection: PostgresCon) -> AdInfo:
    ad_network_tld = "unity3d.com"
    mmp_url = None
    adv_store_id = None
    if "auction-load.unityads.unity3d.com" in sent_video_dict["url"]:
        ad_response_text = sent_video_dict["response_text"]
        ad_response = json.loads(ad_response_text)
        mykey = list(ad_response["media"].keys())[0]
        keyresp = ad_response["media"][mykey]
        if "bundleId" in keyresp:
            adv_store_id = keyresp["bundleId"]
        else:
            content = keyresp["content"]
            decoded_content = urllib.parse.unquote(content)
            all_urls = extract_and_decode_urls(decoded_content)
            ad_parts = parse_urls_for_known_parts(all_urls, database_connection)
            adv_store_id = ad_parts["adv_store_id"]
            mmp_url = ad_parts["mmp_url"]
        adcontent: str = ad_response["media"][mykey]["content"]
        if "referrer" in adcontent:
            referrer = adcontent.split("referrer=")[1].split(",")[0]
            if "adjust_external" in referrer:
                mmp_url = "adjust.com"
    else:
        all_urls = extract_and_decode_urls(sent_video_dict["response_text"])
        ad_parts = parse_urls_for_known_parts(all_urls, database_connection)
        adv_store_id = ad_parts["adv_store_id"]
        mmp_url = ad_parts["mmp_url"]
    ad_info = AdInfo(
        adv_store_id=adv_store_id,
        ad_network_tld=ad_network_tld,
        mmp_url=mmp_url,
    )
    return ad_info


def get_tld(url: str) -> str:
    tld = tldextract.extract(url).domain + "." + tldextract.extract(url).suffix
    return tld


def parse_vungle_ad(sent_video_dict: dict, database_connection: PostgresCon) -> AdInfo:
    ad_network_tld = "vungle.com"
    mmp_url = None
    adv_store_id = None
    ad_response_text = sent_video_dict["response_text"]
    response_dict = json.loads(ad_response_text)
    try:
        adv_store_id = response_dict["ads"][0]["ad_markup"]["ad_market_id"]
        check_urls = ["clickUrl", "checkpoint.0", "checkpoint.100"]
        urlkeys = response_dict["ads"][0]["ad_markup"]["tpat"]
        for x in check_urls:
            try:
                these_urls = urlkeys[x]
                for url in these_urls:
                    if get_tld(url) in MMP_TLDS:
                        mmp_url = url
                        break
            except Exception:
                pass
    except Exception:
        pass
    if not adv_store_id:
        extracted_urls = extract_and_decode_urls(ad_response_text)
        ad_parts = parse_urls_for_known_parts(extracted_urls, database_connection)
        adv_store_id = ad_parts["adv_store_id"]
        mmp_url = ad_parts["mmp_url"]
    ad_info = AdInfo(
        adv_store_id=adv_store_id,
        ad_network_tld=ad_network_tld,
        mmp_url=mmp_url,
    )
    return ad_info


def parse_fyber_ad(sent_video_dict: dict, database_connection: PostgresCon) -> AdInfo:
    ad_network_tld = "fyber.com"
    mmp_url = None
    parsed_urls = []
    text = sent_video_dict["response_text"]
    try:
        parsed_urls = parse_fyber_ad_response(text)
    except Exception:
        pass
    all_urls = extract_and_decode_urls(text=text)
    all_urls = list(set(all_urls + parsed_urls))
    ad_parts = parse_urls_for_known_parts(all_urls, database_connection)
    adv_store_id = ad_parts["adv_store_id"]
    ad_network_tld = ad_parts["ad_network_tld"]
    mmp_url = ad_parts["mmp_url"]
    ad_info = AdInfo(
        adv_store_id=adv_store_id,
        ad_network_tld=ad_network_tld,
        mmp_url=mmp_url,
    )
    return ad_info


def parse_google_ad(text: str, database_connection: PostgresCon) -> AdInfo:
    # with open(f"google_ad_{video_id}.html", "w") as f:
    #     f.write(ad_html)
    all_urls = extract_and_decode_urls(text)
    ad_parts = parse_urls_for_known_parts(all_urls, database_connection)
    try:
        adv_store_id = ad_parts["adv_store_id"]
        ad_network_tld = ad_parts["ad_network_tld"]
        mmp_url = ad_parts["mmp_url"]
    except Exception:
        logger.error("No adv_store_id found")
        adv_store_id = None
    # parsed_url = urllib.parse.urlparse(mmp_url)
    # query_params = urllib.parse.parse_qs(parsed_url.query)
    # campaign_name = query_params["c"][0]
    # af_siteid = query_params["af_siteid"][0]
    # af_network_id = query_params["pid"][0]
    ad_info = AdInfo(
        adv_store_id=adv_store_id,
        ad_network_tld=ad_network_tld,
        mmp_url=mmp_url,
    )
    return ad_info


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


def store_creatives(
    row: pd.Series, adv_store_id: str, file_extension: str
) -> pathlib.Path:
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
    if not thumb_path.exists():
        # Use ffmpeg to create thumbnail (frame at 5s, resized)
        try:
            if file_extension.lower() in ("mp4", "webm"):
                subprocess.run(
                    [
                        "ffmpeg",
                        "-y",
                        "-ss",
                        "5",  # seek to 5 seconds
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
            elif file_extension.lower() in ("jpg", "jpeg", "png"):
                # Resize image directly
                subprocess.run(
                    [
                        "ffmpeg",
                        "-y",
                        "-i",
                        str(thumbnail_width),
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
            else:
                logger.error(f"Unknown file extension: {file_extension} for thumbnail!")

        except Exception:
            logger.error(f"Failed to create thumbnail for {local_path}")
            pass
    return local_path


def get_creatives(
    df: pd.DataFrame,
    pub_store_id: str,
    database_connection: PostgresCon,
    do_store_creatives: bool = True,
) -> tuple[pd.DataFrame, list]:
    error_messages = []
    if "status_code" not in df.columns:
        run_id = df["run_id"].to_numpy()[0]
        pub_store_id = df["pub_store_id"].to_numpy()[0]
        error_msg = "No status code found in df, skipping"
        logger.error(error_msg)
        row = {"run_id": run_id, "pub_store_id": pub_store_id, "error_msg": error_msg}
        error_messages.append(row)
        return pd.DataFrame(), error_messages
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
    status_code_200 = df["status_code"] == 200
    # response_content_not_na = df["response_content"].notna()
    df = add_is_creative_content_column(df)
    # creatives_df = df[
    #     response_content_not_na & (df["is_creative_content"]) & status_code_200
    # ].copy()
    creatives_df = df[(df["is_creative_content"]) & status_code_200].copy()
    creatives_df["creative_size"] = creatives_df["response_size"].fillna(0).astype(int)
    creatives_df = creatives_df[creatives_df["creative_size"] > 50000]
    if creatives_df.empty:
        run_id = df["run_id"].to_numpy()[0]
        pub_store_id = df["pub_store_id"].to_numpy()[0]
        error_msg = "No creatives to check"
        logger.error(error_msg)
        row = {"run_id": run_id, "pub_store_id": pub_store_id, "error_msg": error_msg}
        error_messages.append(row)
        return pd.DataFrame(), error_messages
    i = 0
    adv_creatives = []
    row_count = creatives_df.shape[0]
    for _i, row in creatives_df.iterrows():
        i += 1
        # video_id = ".".join(row["url"].split("/")[-1].split(".")[:-1])
        url_parts = urllib.parse.urlparse(row["url"])
        video_id = url_parts.path.split("/")[-1]
        # if video_id.startswith("?"):
        # video_id = video_id[1:]
        if "2mdn" in row["tld_url"]:
            if "/id/" in row["url"]:
                url_parts = urllib.parse.urlparse(row["url"])
                video_id = url_parts.path.split("/id/")[1].split("/")[0]
            elif "simgad" in row["url"]:
                video_id = row["url"].split("/")[-1]
        if "googlevideo" in row["tld_url"]:
            url_parts = urllib.parse.urlparse(row["url"])
            query_params = urllib.parse.parse_qs(url_parts.query)
            video_id = query_params["ei"][0]
        if row["tld_url"] == "unity3dusercontent.com":
            # this isn't the video name, but the id of the content as the name is the quality
            video_id = row["url"].split("/")[-2]
        if row["tld_url"] == "adcolony.com":
            video_id = row["url"].split("/")[-2]
        if video_id == "":
            error_msg = "Video ID is empty"
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        file_extension = row["file_extension"]
        if video_id in IGNORE_CREATIVE_IDS:
            logger.info(f"Ignoring video {video_id}.{file_extension}")
            continue
        sent_video_df = get_first_sent_video_df(df, row, video_id)
        if sent_video_df is None or sent_video_df.empty:
            error_msg = f"No source bidrequest found for {row['tld_url']}"
            logger.error(f"{error_msg} {video_id}")
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        sent_video_dicts = sent_video_df.to_dict(orient="records")
        found_ad_infos = []
        for sent_video_dict in sent_video_dicts:
            if "vungle.com" in sent_video_dict["tld_url"]:
                ad_info = parse_vungle_ad(sent_video_dict, database_connection)
            elif "bidmachine.io" in sent_video_dict["tld_url"]:
                ad_info = parse_bidmachine_ad(sent_video_dict, database_connection)
            elif (
                "fyber.com" in sent_video_dict["tld_url"]
                or "tpbid.com" in sent_video_dict["tld_url"]
                or "inner-active.mobi" in sent_video_dict["tld_url"]
            ):
                ad_info = parse_fyber_ad(sent_video_dict, database_connection)
            elif "everestop.io" in sent_video_dict["tld_url"]:
                ad_info = parse_everestop_ad(sent_video_dict)
            elif "doubleclick.net" in sent_video_dict["tld_url"]:
                if sent_video_dict["response_text"] is None:
                    error_msg = "doubleclick no response_text"
                    logger.error(f"{error_msg} for {row['tld_url']} {video_id}")
                    row["error_msg"] = error_msg
                    error_messages.append(row)
                    continue
                response_text = sent_video_dict["response_text"]
                try:
                    google_response = json.loads(response_text)
                    if "ad_networks" in google_response:
                        try:
                            ad_html = google_response["ad_networks"][0]["ad"]["ad_html"]
                        except Exception:
                            error_msg = (
                                "doubleclick failing to parse ad_html for VAST response"
                            )
                            logger.error(error_msg)
                            row["error_msg"] = error_msg
                            error_messages.append(row)
                            continue
                        ad_info = parse_google_ad(
                            text=ad_html, database_connection=database_connection
                        )
                    elif "slots" in google_response:
                        slot_adv = None
                        for slot in google_response["slots"]:
                            if slot_adv is not None:
                                continue
                            if video_id in str(slot):
                                for ad in slot["ads"]:
                                    if video_id in str(ad):
                                        slots_try = parse_google_ad(
                                            str(slot), database_connection
                                        )
                                        if slots_try["adv_store_id"] is not None:
                                            ad_info = slots_try
                                            slot_adv = True
                        if slot_adv is None:
                            error_msg = (
                                "doubleclick failing to parse for slots response"
                            )
                            logger.error(error_msg)
                            row["error_msg"] = error_msg
                            error_messages.append(row)
                            continue
                    else:
                        error_msg = "doubleclick new format"
                        logger.error(error_msg)
                        row["error_msg"] = error_msg
                        error_messages.append(row)
                        continue
                except json.JSONDecodeError:
                    if response_text[0:14] == "<?xml version=":
                        ad_info = parse_google_ad(
                            text=response_text, database_connection=database_connection
                        )
                    elif (
                        response_text[0:15] == "<!DOCTYPE html>"
                        or response_text[0:15] == "document.write("
                        or response_text[0:3] == "if "
                    ):
                        found_potential_app = any(
                            [x in response_text for x in PLAYSTORE_URL_PARTS + MMP_TLDS]
                        )
                        if found_potential_app:
                            error_msg = "found potential app! doubleclick"
                        else:
                            error_msg = "doubleclick html / web ad"
                        logger.info(error_msg)
                        row["error_msg"] = error_msg
                        error_messages.append(row)
                        continue
                    else:
                        error_msg = "doubleclick new format"
                        logger.error(error_msg)
                        row["error_msg"] = error_msg
                        error_messages.append(row)
                        continue
            elif "unityads.unity3d.com" in sent_video_dict["tld_url"]:
                ad_info = parse_unity_ad(sent_video_dict, database_connection)
            elif "mtgglobals.com" in sent_video_dict["tld_url"]:
                ad_info = parse_mtg_ad(sent_video_dict)
            else:
                real_tld = get_tld(sent_video_dict["tld_url"])
                error_msg = f"Not a recognized ad network: {real_tld}"
                logger.error(f"{error_msg} for video {video_id[0:10]}")
                row["error_msg"] = error_msg
                error_messages.append(row)
                continue
            if ad_info["adv_store_id"] is None:
                text = sent_video_dict["response_text"]
                all_urls = extract_and_decode_urls(text)
                if any([x in all_urls for x in MMP_TLDS + PLAYSTORE_URL_PARTS]):
                    error_msg = "found potential app! mmp or playstore"
                    row["error_msg"] = error_msg
                    error_messages.append(row)
                    continue
                ad_parts = parse_urls_for_known_parts(all_urls, database_connection)
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
            if do_store_creatives:
                local_path = store_creatives(
                    row,
                    adv_store_id=ad_info["adv_store_id"],
                    file_extension=file_extension,
                )
                md5_hash = hashlib.md5(row["response_content"]).hexdigest()
            else:
                local_path = local_path / f"{video_id}.{file_extension}"
                md5_hash = video_id
            if ad_info["adv_store_id"] == "unknown":
                error_msg = f"Unknown adv_store_id for {row['tld_url']}"
                logger.error(f"Unknown adv_store_id for {row['tld_url']} {video_id}")
                row["error_msg"] = error_msg
                error_messages.append(row)
                continue
            if (
                ad_info["ad_network_tld"]
                not in query_ad_domains(database_connection=database_connection)[
                    "domain"
                ].to_list()
            ):
                ad_info["ad_network_tld"] = get_tld(sent_video_dict["tld_url"])
            try:
                adv_db_id = query_store_app_by_store_id(
                    store_id=ad_info["adv_store_id"],
                    database_connection=database_connection,
                )
            except Exception:
                error_msg = "found potential app! but failed to get db id"
                logger.error(error_msg)
                row["error_msg"] = error_msg
                error_messages.append(row)
                continue
            init_tld = (
                tldextract.extract(sent_video_dict["tld_url"]).domain
                + "."
                + tldextract.extract(sent_video_dict["tld_url"]).suffix
            )
            found_ad_infos.append(ad_info)
        found_advs = [
            x["adv_store_id"] for x in found_ad_infos if x["adv_store_id"] != "unknown"
        ]
        mmp_tlds = [x["mmp_tld"] for x in found_ad_infos if x["mmp_url"] is not None]
        mmp_tlds = list(set(mmp_tlds))
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
            error_msg = f"No adv_store_id found for {row['tld_url']}"
            logger.error(f"{error_msg} {video_id}")
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        else:
            adv_store_id = found_advs[0]
        ad_network_tlds = [x["ad_network_tld"] for x in found_ad_infos]
        ad_network_tld = ad_network_tlds[0]
        adv_creatives.append(
            {
                "pub_store_id": pub_store_id,
                "adv_store_id": adv_store_id,
                "adv_store_app_id": adv_db_id,
                "ad_network_tld": ad_network_tld,
                "creative_initial_domain_tld": init_tld,
                "mmp_tld": mmp_tld,
                "md5_hash": md5_hash,
                "file_extension": file_extension,
                "local_path": local_path,
            }
        )
        logger.debug(
            f"{i}/{row_count}: {ad_info['ad_network_tld']} adv={ad_info['adv_store_id']} init_tld={init_tld}"
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
                file_path=row["local_path"],
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


def make_creative_records_df(
    adv_creatives_df: pd.DataFrame,
    assets_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    ad_domains_df = query_ad_domains(database_connection=database_connection)
    creative_records_df = adv_creatives_df.merge(
        assets_df[["store_app_id", "md5_hash", "creative_asset_id"]],
        left_on=["adv_store_app_id", "md5_hash"],
        right_on=["store_app_id", "md5_hash"],
        how="left",
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
        left_on="ad_network_tld",
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
        ]
    ]
    check_cols = ["creative_host_domain_id", "mmp_domain_id"]
    for col in check_cols:
        creative_records_df[col] = np.where(
            creative_records_df[col].isna(), None, creative_records_df[col]
        )
    return creative_records_df


def collect_creative_records_df(
    pub_store_id: str,
    run_id: int,
    mitm_log_path: pathlib.Path,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    df = parse_mitm_log(mitm_log_path)
    df["pub_store_id"] = pub_store_id
    if df.empty:
        error_msg = "No data in mitm df"
        logger.error(error_msg)
        row = {"run_id": run_id, "pub_store_id": pub_store_id, "error_msg": error_msg}
        error_messages = [row]
        return pd.DataFrame(), error_messages
    adv_creatives_df, error_messages = get_creatives(
        df, pub_store_id, database_connection
    )
    return adv_creatives_df, error_messages


def parse_store_id_mitm_log(
    pub_store_id: str,
    run_id: int,
    mitm_log_path: pathlib.Path,
    database_connection: PostgresCon,
) -> list:
    pub_db_id = query_store_app_by_store_id(
        store_id=pub_store_id, database_connection=database_connection
    )
    df = parse_mitm_log(mitm_log_path)
    df["pub_store_id"] = pub_store_id
    if df.empty:
        error_msg = "No data in mitm df"
        logger.error(error_msg)
        row = {"run_id": run_id, "pub_store_id": pub_store_id, "error_msg": error_msg}
        error_messages = [row]
        return error_messages
    adv_creatives_df, error_messages = get_creatives(
        df, pub_store_id, database_connection
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
    adv_creatives_df["store_app_pub_id"] = pub_db_id
    adv_creatives_df["run_id"] = run_id
    assets_df = adv_creatives_df[
        ["adv_store_app_id", "md5_hash", "file_extension"]
    ].drop_duplicates()
    assets_df = assets_df.rename(columns={"adv_store_app_id": "store_app_id"})
    assets_df = upsert_df(
        assets_df,
        table_name="creative_assets",
        database_connection=database_connection,
        key_columns=["store_app_id", "md5_hash"],
        insert_columns=["store_app_id", "md5_hash", "file_extension"],
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
    upsert_df(
        creative_records_df,
        table_name="creative_records",
        database_connection=database_connection,
        key_columns=key_columns,
        insert_columns=key_columns,
    )
    upload_creatives(adv_creatives_df)
    return error_messages


def parse_all_runs_for_store_id(
    pub_store_id: str, database_connection: PostgresCon
) -> pd.DataFrame:
    mitms = get_store_id_mitm_s3_keys(store_id=pub_store_id)
    all_failed_df = pd.DataFrame()
    for _i, mitm in mitms.iterrows():
        key = mitm["key"]
        run_id = mitm["run_id"]
        filename = f"{pub_store_id}_{run_id}.log"
        if not pathlib.Path(MITM_DIR, filename).exists():
            mitm_log_path = download_mitm_log_by_key(key, filename)
        else:
            mitm_log_path = pathlib.Path(MITM_DIR, filename)
        try:
            error_messages = parse_store_id_mitm_log(
                pub_store_id, run_id, mitm_log_path, database_connection
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
        all_failed_df = pd.concat([all_failed_df, error_msg_df], ignore_index=True)
        log_creative_scan_results(error_msg_df, database_connection)
    return all_failed_df


def parse_specific_run_for_store_id(
    pub_store_id: str, run_id: int, database_connection: PostgresCon
) -> pd.DataFrame:
    pub_store_id = "com.elt2.sg"
    run_id = 2441
    mitm_log_path = pathlib.Path(MITM_DIR, f"{pub_store_id}_{run_id}.log")
    if not mitm_log_path.exists():
        key = f"mitm_logs/android/{pub_store_id}/{run_id}.log"
        mitms = get_store_id_mitm_s3_keys(store_id=pub_store_id)
        key = mitms[mitms["run_id"] == str(run_id)]["key"].to_numpy()[0]
        mitm_log_path = download_mitm_log_by_key(key, mitm_log_path)
    else:
        logger.error("mitm not found")
    return parse_store_id_mitm_log(
        pub_store_id, run_id, mitm_log_path, database_connection
    )


def scan_all_apps(database_connection: PostgresCon) -> None:
    apps_to_scan = query_apps_to_creative_scan(database_connection=database_connection)
    apps_count = apps_to_scan.shape[0]
    all_failed_df = pd.DataFrame()
    for i, app in apps_to_scan.iterrows():
        pub_store_id = app["store_id"]
        logger.info(f"{i}/{apps_count}: {pub_store_id} start")
        try:
            app_failed_df = parse_all_runs_for_store_id(
                pub_store_id, database_connection
            )
        except Exception as e:
            logger.exception(f"Error parsing {pub_store_id}: {e}")
            continue
        if not app_failed_df.empty:
            all_failed_df = pd.concat([all_failed_df, app_failed_df], ignore_index=True)
            logger.info(f"Saved {all_failed_df.shape[0]} failed creatives to csv")
    subprocess.run(
        [
            "s3cmd",
            "sync",
            str(CREATIVES_DIR / "thumbs"),
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


def open_all_local_mitms() -> pd.DataFrame:
    all_mitms_df = pd.DataFrame()
    i = 0
    num_files = len(list(pathlib.Path(MITM_DIR).glob("*.log")))
    logger.info(f"Opening {num_files} local mitm logs")
    for mitm_log_path in pathlib.Path(MITM_DIR).glob("*.log"):
        i += 1
        logger.info(f"{i}/{num_files}: {mitm_log_path}")
        pub_store_id = mitm_log_path.name.split("_")[0]
        run_id = mitm_log_path.name.split("_")[1].replace(".log", "")
        df = parse_mitm_log(mitm_log_path)
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
    import os

    BUCKET_NAME = "appgoblin-data"
    client = get_cloud_s3_client()
    client.upload_file(
        "all_mitms.tsv.xz", Bucket=BUCKET_NAME, Key="mitmcsv/all_mitms.tsv.xz"
    )
    os.system("s3cmd setacl s3://appgoblin-data/mitmcsv/ --acl-public --recursive")


def get_cloud_s3_client():
    """Create and return an S3 client."""
    import boto3

    from adscrawler.config import CONFIG

    session = boto3.session.Session()
    return session.client(
        "s3",
        region_name="sgp1",
        endpoint_url="https://sgp1.digitaloceanspaces.com",
        aws_access_key_id=CONFIG["digi-cloud"]["access_key_id"],
        aws_secret_access_key=CONFIG["digi-cloud"]["secret_key"],
    )


def test_mitm_scrape_ads(database_connection: PostgresCon):
    all_mitms_df = pd.read_csv("all_mitms.tsv", sep="\t")
    all_creatives_df = pd.DataFrame()
    all_error_messages = []
    row_count = all_mitms_df[["pub_store_id", "run_id"]].drop_duplicates().shape[0]
    i = 0
    for (pub_store_id, run_id), df in all_mitms_df.groupby(["pub_store_id", "run_id"]):
        i += 1
        logger.info(f"{i}/{row_count}: {pub_store_id}, {run_id}")
        df["pub_store_id"] = pub_store_id
        df["run_id"] = run_id
        adv_creatives_df, error_messages = get_creatives(
            df, pub_store_id, database_connection, do_store_creatives=False
        )
        break
        all_creatives_df = pd.concat(
            [all_creatives_df, adv_creatives_df], ignore_index=True
        )
        all_error_messages.append(error_messages)
