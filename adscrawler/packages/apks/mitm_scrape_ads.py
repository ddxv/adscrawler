import dataclasses
import datetime
import hashlib
import html
import json
import pathlib
import re
import urllib
import xml.etree.ElementTree as ET

import pandas as pd
import protod
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


@dataclasses.dataclass
class AdInfo:
    adv_store_id: str
    ad_network_tld: str
    mmp_url: str | None = None

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
    requests = []
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
                            try:
                                request_data["response_content"] = flow.response.content
                                response_text = flow.response.get_text()
                                request_data["response_text"] = response_text
                                request_data["response_text"] = request_data[
                                    "response_text"
                                ]
                            except Exception:
                                pass
                        requests.append(request_data)
                    except Exception as e:
                        logger.exception(f"Error parsing flow: {e}")
                        continue
    except Exception as e:
        logger.exception(e)
    # Convert to DataFrame
    if not requests:
        logger.error("No HTTP requests found in the log file")
        return pd.DataFrame()
    df = pd.DataFrame(requests)
    if "response_text" in df.columns:
        df["response_text"] = df["response_text"].astype(str)
    df["run_id"] = (
        mitm_log_path.as_posix().split("/")[-1].split("_")[-1].replace(".log", "")
    )
    return df


def get_first_sent_video_df(
    df: pd.DataFrame, row: pd.Series, video_id: str
) -> dict | None:
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
        logger.debug("Multiple responses for video found, selecting 1")
        sent_video_df = sent_video_df.head(1).reset_index()
    response = sent_video_df.to_dict(orient="records")[0]
    subdomain = tldextract.extract(response["url"]).subdomain
    if subdomain:
        tld_url = (
            subdomain
            + "."
            + tldextract.extract(response["url"]).domain
            + "."
            + tldextract.extract(response["url"]).suffix
        )
    else:
        tld_url = (
            tldextract.extract(response["url"]).domain
            + "."
            + tldextract.extract(response["url"]).suffix
        )
    response["tld_url"] = tld_url
    return response


def parse_fyber_vast_xml(ad_response_text: str) -> list[str]:
    outer_tree = ET.fromstring(ad_response_text)
    ns = {"tns": "http://www.inner-active.com/SimpleM2M/M2MResponse"}
    ad_element = outer_tree.find(".//tns:Ad", ns)
    urls = []
    if ad_element is not None and ad_element.text:
        # Clean up and parse the inner VAST XML
        inner_vast_xml = ad_element.text.strip()
        try:
            vast_tree = ET.fromstring(inner_vast_xml)
        except ET.ParseError:
            vast_tree = ET.fromstring(html.unescape(inner_vast_xml))
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


def extract_fyber_ad_urls(
    ad_response_text: str, database_connection: PostgresCon
) -> dict | None:
    """
    Parses an HTML ad file to extract Google Play and AppsFlyer URLs
    from the VAST data embedded within it.
    Args:
        html_content (str): The HTML content of the ad.
    Returns:
        dict: A dictionary containing the found 'google_play_url' and
    """
    urls = parse_fyber_vast_xml(ad_response_text)
    google_play_url = None
    google_tracking_url = None
    market_intent_url = None
    intent_url = None
    adv_store_id = None
    ad_network_url = None
    ad_network_tld = None
    ad_network_urls = query_ad_domains(database_connection=database_connection)
    for url in urls:
        tld_url = tldextract.extract(url).domain + "." + tldextract.extract(url).suffix
        if "appsflyer.com" in url:
            adv_store_id = re.search(r"http.*\.appsflyer\.com/([a-zA-Z0-9_.]+)\?", url)[
                1
            ]
            mmp_url = url
        elif "adjust.com" in url:
            mmp_url = url
        elif "abr.ge" in url:
            mmp_url = url
        elif "kochava.com" in url:
            mmp_url = url
        elif "singular.com" in url:
            mmp_url = url
        elif "tpbid.com" in url:
            ad_network_url = url
            ad_network_tld = tld_url
        elif "inner-active.mobi" in url:
            ad_network_url = url
            ad_network_tld = tld_url
        elif tld_url in ad_network_urls["domain"].to_list():
            ad_network_url = url
            ad_network_tld = tld_url
        elif "doubleclick.net" in tld_url:
            google_tracking_url = url
        elif "intent://" in url:
            intent_url = url
            adv_store_id = intent_url.replace("intent://", "")
        elif "market://details?id=" in url:
            market_intent_url = url
            parsed_market_intent = urllib.parse.urlparse(market_intent_url)
            adv_store_id = urllib.parse.parse_qs(parsed_market_intent.query)["id"][0]
        else:
            logger.debug(f"Found unknown URL: {url}")
        # Stop if we have found both URLs
        if google_play_url and mmp_url and adv_store_id:
            break
    return {
        "mmp_url": mmp_url,
        "market_intent_url": market_intent_url,
        "intent_url": intent_url,
        "adv_store_id": adv_store_id,
        "ad_network_url": ad_network_url,
        "ad_network_tld": ad_network_tld,
    }


def google_extract_ad_urls(
    ad_html: str, database_connection: PostgresCon
) -> dict | None:
    """
    Parses an HTML ad file to extract Google Play and AppsFlyer URLs
    from the VAST data embedded within it.
    Args:
        html_content (str): The HTML content of the ad.
    Returns:
        dict: A dictionary containing the found 'google_play_url' and
    """
    soup = BeautifulSoup(ad_html, "html.parser")
    # The URLs are located inside a <meta> tag with name="video_fields"
    # The content of this tag contains VAST XML data.
    video_fields_meta = soup.find("meta", {"name": "video_fields"})
    if not video_fields_meta:
        print("Could not find the meta tag with name='video_fields'")
        return None
    # The content is a string that needs to be parsed as XML.
    # We also need to unescape HTML entities like &lt; and &gt;
    vast_xml_string = html.unescape(video_fields_meta["content"])
    # The actual URLs are inside CDATA sections, so we can use regex
    # to find all URLs within the VAST data.
    urls = re.findall(r"<!\[CDATA\[(.*?)\]\]>", vast_xml_string)
    google_play_url = None
    google_tracking_url = None
    mmp_url = None
    market_intent_url = None
    intent_url = None
    adv_store_id = None
    ad_network_url = None
    ad_network_tld = None
    ad_network_urls = query_ad_domains(database_connection=database_connection)
    for url in urls:
        tld_url = tldextract.extract(url).domain + "." + tldextract.extract(url).suffix
        if tld_url in MMP_TLDS:
            mmp_url = url
        elif "doubleclick.net" in tld_url:
            google_tracking_url = url
        elif "intent://" in url:
            intent_url = url
            adv_store_id = intent_url.replace("intent://", "")
        elif "market://details?id=" in url:
            market_intent_url = url
            parsed_market_intent = urllib.parse.urlparse(market_intent_url)
            adv_store_id = urllib.parse.parse_qs(parsed_market_intent.query)["id"][0]
        elif "play.google.com" in url:
            google_play_url = url
            parsed_gplay = urllib.parse.urlparse(google_play_url)
            adv_store_id = urllib.parse.parse_qs(parsed_gplay.query)["id"][0]
        elif tld_url in ad_network_urls["domain"].to_list():
            # last effort
            ad_network_url = url
            ad_network_tld = tld_url
        else:
            logger.debug(f"Found unknown URL: {url}")
        # Stop if we have found both URLs
        if ad_network_tld and mmp_url and adv_store_id:
            break
    return {
        "google_play_url": google_play_url,
        "mmp_url": mmp_url,
        "google_tracking_url": google_tracking_url,
        "market_intent_url": market_intent_url,
        "intent_url": intent_url,
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


def parse_everestop_ad(sent_video_dict: dict) -> AdInfo:
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


def parse_unity_ad(sent_video_dict: dict) -> AdInfo:
    ad_network_tld = "unity3d.com"
    mmp_url = None
    if "auction-load.unityads.unity3d.com" in sent_video_dict["url"]:
        ad_response_text = sent_video_dict["response_text"]
        ad_response = json.loads(ad_response_text)
        mykey = list(ad_response["media"].keys())[0]
        adv_store_id = ad_response["media"][mykey]["bundleId"]
        adcontent: str = ad_response["media"][mykey]["content"]
        if "referrer" in adcontent:
            referrer = adcontent.split("referrer=")[1].split(",")[0]
            if "adjust_external" in referrer:
                mmp_url = "adjust.com"
    else:
        logger.error(f"Unknown Unity ad domain: {sent_video_dict['url']}")
        adv_store_id = None
    #  and auction-load.unityads.unity3d.com
    # ad_html = sent_video_dict["response_text"]
    # with open(f"unity_ad_{video_id}.html", "w") as f:
    #     f.write(ad_html)
    ad_info = AdInfo(
        adv_store_id=adv_store_id,
        ad_network_tld=ad_network_tld,
        mmp_url=mmp_url,
    )
    return ad_info


def get_tld(url: str) -> str:
    tld = tldextract.extract(url).domain + "." + tldextract.extract(url).suffix
    return tld


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
]


def parse_vungle_ad(sent_video_dict: dict) -> AdInfo:
    ad_network_tld = "vungle.com"
    mmp_url = None
    ad_response_text = sent_video_dict["response_text"]
    response_dict = json.loads(ad_response_text)
    adv_store_id = response_dict["ads"][0]["ad_markup"]["ad_market_id"]
    check_urls = ["clickUrl", "checkpoint.0", "checkpoint.100"]
    urlkeys = response_dict["ads"][0]["ad_markup"]["tpat"]
    mmp_url = None
    for x in check_urls:
        try:
            these_urls = urlkeys[x]
            for url in these_urls:
                if get_tld(url) in MMP_TLDS:
                    mmp_url = url
                    break
        except Exception:
            pass
    mmp_url = mmp_url
    ad_info = AdInfo(
        adv_store_id=adv_store_id,
        ad_network_tld=ad_network_tld,
        mmp_url=mmp_url,
    )
    return ad_info


def parse_fyber_ad(sent_video_dict: dict, database_connection: PostgresCon) -> AdInfo:
    ad_network_tld = "fyber.com"
    mmp_url = None
    ad_response_text = sent_video_dict["response_text"]
    urls = extract_fyber_ad_urls(ad_response_text, database_connection)
    adv_store_id = urls["adv_store_id"]
    ad_network_tld = urls["ad_network_tld"]
    mmp_url = urls["mmp_url"]
    ad_info = AdInfo(
        adv_store_id=adv_store_id,
        ad_network_tld=ad_network_tld,
        mmp_url=mmp_url,
    )
    return ad_info


def parse_google_ad(ad_html: dict, database_connection: PostgresCon) -> AdInfo:
    # with open(f"google_ad_{video_id}.html", "w") as f:
    #     f.write(ad_html)
    urls = google_extract_ad_urls(ad_html, database_connection)
    try:
        adv_store_id = urls["adv_store_id"]
        ad_network_tld = urls["ad_network_tld"]
        mmp_url = urls["mmp_url"]
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


def get_creatives(
    df: pd.DataFrame,
    pub_store_id: str,
    database_connection: PostgresCon,
) -> tuple[pd.DataFrame, list]:
    error_messages = []
    if "status_code" not in df.columns:
        row = df[["run_id", "pub_store_id"]].drop_duplicates().T
        error_msg = "No status code found in df, skipping"
        logger.error(error_msg)
        row["error_msg"] = error_msg
        error_messages.append(row)
        return pd.DataFrame(), error_messages
    df["file_extension"] = df["url"].apply(lambda x: x.split(".")[-1])
    status_code_200 = df["status_code"] == 200
    response_content_not_na = df["response_content"].notna()
    is_creative_content = df["response_content_type"].str.contains(
        "image|video|webm|mp4|jpeg|jpg|png|gif|webp", regex=True
    )
    is_right_ext_len = df["file_extension"].str.len() < 7
    creatives_df = df[
        response_content_not_na
        & is_creative_content
        & status_code_200
        & is_right_ext_len
    ].copy()
    creatives_df["creative_size"] = creatives_df["response_content"].apply(
        lambda x: len(x) if isinstance(x, bytes) else 0
    )
    creatives_df = creatives_df[creatives_df["creative_size"] > 50000]
    i = 0
    adv_creatives = []
    row_count = creatives_df.shape[0]
    for _i, row in creatives_df.iterrows():
        i += 1
        video_id = ".".join(row["url"].split("/")[-1].split(".")[:-1])
        if row["tld_url"] == "unity3dusercontent.com":
            # this isn't the video name, but the id of the content as the name is the quality
            video_id = row["url"].split("/")[-2]
        file_extension = row["file_extension"]
        if video_id in IGNORE_CREATIVE_IDS:
            logger.info(f"Ignoring video {video_id}.{file_extension}")
            continue
        sent_video_dict = get_first_sent_video_df(df, row, video_id)
        if sent_video_dict is None:
            logger.error(f"No video source found for {row['tld_url']} {video_id}")
            error_messages.append(row)
            continue
        if "vungle.com" in sent_video_dict["tld_url"]:
            ad_info = parse_vungle_ad(sent_video_dict)
        elif "fyber.com" in sent_video_dict["tld_url"]:
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
            google_response = json.loads(response_text)
            if "ad_networks" in google_response:
                try:
                    ad_html = google_response["ad_networks"][0]["ad"]["ad_html"]
                    ad_info = parse_google_ad(ad_html, database_connection)
                except Exception:
                    error_msg = "doubleclick failing to parse ad_html for VAST response"
                    logger.error(error_msg)
                    row["error_msg"] = error_msg
                    error_messages.append(row)
                    continue
            elif "slots" in google_response:
                try:
                    for slot in google_response["slots"]:
                        print("a")
                        if video_id in str(slot):
                            print("b")
                            for ad in slot["ads"]:
                                print("c")
                                if video_id in str(ad):
                                    "wtcg" in str(ad)
                                    for clickurl in ad["tracking_urls_and_actions"][
                                        "click_actions"
                                    ]:
                                        logger.info(get_tld(clickurl["url"]))
                    # Haven't found example with adv_id
                    raise Exception
                except Exception:
                    error_msg = "doubleclick failing to parse for slots response"
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
        elif "unityads.unity3d.com" in sent_video_dict["tld_url"]:
            ad_info = parse_unity_ad(sent_video_dict)
        elif "mtgglobals.com" in sent_video_dict["tld_url"]:
            ad_info = parse_mtg_ad(sent_video_dict)
        else:
            error_msg = f"Not a recognized ad network: {sent_video_dict['tld_url']}"
            logger.error(
                f"Not a recognized ad network: {sent_video_dict['tld_url']} for video {video_id[0:10]}"
            )
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        if ad_info.adv_store_id is None:
            ad_info.adv_store_id = "unknown"
        if ad_info.adv_store_id == pub_store_id:
            error_msg = "Incorrect adv_store_id, identified pub ID as adv ID"
            logger.error(
                f"Incorrect adv_store_id, identified pub ID as adv ID for video {video_id[0:10]}"
            )
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        local_path = pathlib.Path(CREATIVES_DIR, ad_info.adv_store_id)
        local_path.mkdir(parents=True, exist_ok=True)
        md5_hash = hashlib.md5(row["response_content"]).hexdigest()
        local_path = local_path / f"{md5_hash}.{file_extension}"
        with open(local_path, "wb") as creative_file:
            creative_file.write(row["response_content"])
        if ad_info.adv_store_id == "unknown":
            error_msg = f"Unknown adv_store_id for {row['tld_url']}"
            logger.error(f"Unknown adv_store_id for {row['tld_url']} {video_id}")
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        adv_db_id = query_store_app_by_store_id(
            store_id=ad_info.adv_store_id, database_connection=database_connection
        )
        init_tld = (
            tldextract.extract(sent_video_dict["tld_url"]).domain
            + "."
            + tldextract.extract(sent_video_dict["tld_url"]).suffix
        )
        adv_creatives.append(
            {
                "pub_store_id": pub_store_id,
                "adv_store_id": ad_info.adv_store_id,
                "adv_store_app_id": adv_db_id,
                "ad_network_tld": ad_info.ad_network_tld,
                "creative_initial_domain_tld": init_tld,
                "mmp_tld": ad_info.mmp_tld if ad_info.mmp_tld else None,
                "md5_hash": md5_hash,
                "file_extension": file_extension,
                "local_path": local_path,
            }
        )
        logger.debug(
            f"{i}/{row_count}: {ad_info.ad_network_tld} adv={ad_info.adv_store_id} init_tld={init_tld}"
        )
    adv_creatives_df = pd.DataFrame(adv_creatives)
    if adv_creatives_df.empty:
        msg = (
            f"No matched for creatives {pub_store_id}, {len(error_messages)} unmatched"
        )
        logger.warning(msg)
    return adv_creatives_df, error_messages


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
    return creative_records_df


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
    adv_creatives_df, error_messages = get_creatives(
        df, pub_store_id, database_connection
    )
    if adv_creatives_df.empty:
        error_msg = "No creatives found"
        logger.error(f"{error_msg}")
        row = {"run_id": run_id, "pub_store_id": pub_store_id, "error_msg": error_msg}
        error_messages.append(row)
        return error_messages
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
    upload_creatives_to_s3(adv_creatives_df)
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
        error_msg_df = pd.DataFrame(error_messages)
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
    pub_store_id = "com.bigstarkids.wordtravelturkish"
    run_id = 10317
    mitm_log_path = pathlib.Path(MITM_DIR, f"{pub_store_id}_{run_id}.log")
    if not mitm_log_path.exists():
        key = f"mitm_logs/android/{pub_store_id}/{run_id}.log"
        mitm_log_path = download_mitm_log_by_key(key, mitm_log_path)
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
            logger.error(f"Error parsing {pub_store_id}: {e}")
            continue
        if not app_failed_df.empty:
            all_failed_df = pd.concat([all_failed_df, app_failed_df], ignore_index=True)
            logger.info(f"Saved {all_failed_df.shape[0]} failed creatives to csv")
