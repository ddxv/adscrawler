import ast
import base64
import html
import json
import re
import urllib
import xml.etree.ElementTree as ET
from typing import Any

import pandas as pd
import protod
import requests
from bs4 import BeautifulSoup
from protod import Renderer

from adscrawler.config import get_logger
from adscrawler.dbcon.connection import PostgresCon
from adscrawler.dbcon.queries import (
    get_all_mmp_tlds,
    get_click_url_redirect_chains,
    query_ad_domains,
    query_api_call_id_for_uuid,
    query_store_app_by_store_id_cached,
    query_urls_id_map,
    query_all_domains,
    upsert_df,
)
from adscrawler.mitm_ad_parser.models import AdInfo, MultipleAdvertiserIdError
from adscrawler.mitm_ad_parser.utils import get_tld

logger = get_logger(__name__, "mitm_scrape_ads")

PLAYSTORE_URL_PARTS = ["play.google.com/store", "market://", "intent://"]

ANDROID_USER_AGENT = "Mozilla/5.0 (Linux; Android 13; Pixel 7 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"

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


def extract_and_decode_urls(
    text: str, run_id: int, mitm_uuid: str, database_connection: PostgresCon
) -> list[str]:
    """Extracts and decodes all URLs from text content, handling various encoding formats."""
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
    if text.strip().startswith("<") and (
        '<meta name="video_fields"' in text.lower() or "<vast" in text.lower()
    ):
        soup = BeautifulSoup(text, "html.parser")
        # The URLs are located inside a <meta> tag with name="video_fields"
        # The content of this tag contains VAST XML data.
        video_fields_meta = soup.find("meta", {"name": "video_fields"})
        if video_fields_meta:
            vast_xml_string = html.unescape(video_fields_meta["content"])
            vast_urls += re.findall(r"<!\[CDATA\[(.*?)\]\]>", vast_xml_string)
        if soup.find("vast"):
            vast_urls += re.findall(r"<!\[CDATA\[(.*?)\]\]>", text)
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

    return all_urls


def store_found_urls_in_db(
    all_urls: list[str], run_id: int, api_call_id: int, database_connection: PostgresCon
) -> pd.DataFrame:
    """Stores the found URLs in the database."""
    all_urls_df = pd.DataFrame(
        {"run_id": run_id, "url": all_urls, "api_call_id": api_call_id}
    )
    # all_urls_df["url_hash"] = all_urls_df["url"].apply(
    #     lambda x: hashlib.md5(str(x).encode()).hexdigest()
    # )
    all_urls_df["scheme"] = all_urls_df["url"].str.split("://").str[0]
    all_urls_df["scheme"] = all_urls_df["scheme"].fillna("unknown")
    urls_df = upsert_df(
        df=all_urls_df,
        database_connection=database_connection,
        schema="adtech",
        table_name="urls",
        md5_key_columns=["url"],
        key_columns=["url"],
        insert_columns=["url", "scheme"],
        return_rows=True,
    )
    urls_df = urls_df.rename(columns={"id": "url_id"})
    urls_df["api_call_id"] = api_call_id
    urls_df["run_id"] = run_id
    all_urls_df = upsert_df(
        df=urls_df[["run_id", "api_call_id", "url_id"]],
        database_connection=database_connection,
        schema="adtech",
        table_name="api_call_urls",
        key_columns=["run_id", "api_call_id", "url_id"],
        insert_columns=["run_id", "api_call_id", "url_id"],
    )


def check_click_urls(
    all_urls: list[str], run_id: int, api_call_id: int, database_connection: PostgresCon
) -> list[str]:
    """Checks URLs for click tracking and follows redirects to find final destination URLs."""
    click_urls = []
    for url in all_urls:
        redirect_urls = []
        if "/click" in url or "/clk" in url:
            if "tpbid.com" in url:
                url = url.replace("fybernativebrowser://navigate?url=", "")
            redirect_urls = follow_url_redirects(
                url, run_id, api_call_id, database_connection
            )
        if len(redirect_urls) > 0:
            click_urls += redirect_urls
    click_urls = list(set(click_urls))
    return click_urls


def parse_fyber_html(
    inner_ad_element: str, run_id: int, mitm_uuid: str, database_connection: PostgresCon
) -> list[str]:
    """Parses Fyber HTML content to extract click URLs and ad network URLs."""
    # Extract all URLs from the raw HTML content first
    all_extracted_urls = extract_and_decode_urls(
        inner_ad_element,
        run_id=run_id,
        mitm_uuid=mitm_uuid,
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
        elif get_tld(url) in get_all_mmp_tlds(database_connection)["mmp_tld"].to_list():
            click_urls.append(url)
    return click_urls


def parse_fyber_ad_response(
    ad_response_text: str, run_id: int, mitm_uuid: str, database_connection: PostgresCon
) -> list[str]:
    """Parses Fyber ad response XML to extract VAST URLs and click tracking URLs."""
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
                    mitm_uuid=mitm_uuid,
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


def adv_id_from_play_url(url: str) -> str | None:
    """Extracts advertiser store ID from Google Play Store URLs."""
    parsed_gplay = urllib.parse.urlparse(url)
    try:
        adv_store_id = urllib.parse.parse_qs(parsed_gplay.query)["id"][0]
        adv_store_id = re.match(r"^[a-zA-Z0-9._-]+", adv_store_id).group(0)
        adv_store_id = adv_store_id.rstrip("!@#$%^&*()+=[]{}|\\:;\"'<>?,/")
    except Exception:
        adv_store_id = None
    return adv_store_id


def check_and_upsert_new_domains(
    domains_df: pd.DataFrame,
    urls_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    """Adds missing domains to the database and returns updated domain DataFrame."""
    col = ["tld_url"]
    missing_domains = urls_df[
        (~urls_df[col].isin(domains_df["domain_name"])) & (urls_df[col].notna())
    ]
    if not missing_domains.empty:
        new_domains = (
            missing_domains[[col]]
            .drop_duplicates()
            .rename(columns={col: "domain_name"})
        )
        new_domains = upsert_df(
            table_name="domains",
            df=new_domains,
            insert_columns=["domain_name"],
            key_columns=["domain_name"],
            database_connection=database_connection,
            return_rows=True,
        )
        domains_df = pd.concat([new_domains, domains_df])
    return domains_df


def upsert_urls(urls: list[str], database_connection: PostgresCon) -> None:
    """Upserts the URLs into the database."""
    new_urls_df = pd.DataFrame({"url": urls})
    http_urls_df = new_urls_df[new_urls_df["url"].str.startswith("http")]
    nonhttp_urls_df = new_urls_df[~new_urls_df["url"].str.startswith("http")]
    http_urls_df["tld_url"] = http_urls_df["url"].apply(lambda x: get_tld(x))
    domains_df = query_all_domains(database_connection=database_connection)
    domains_df = check_and_upsert_new_domains(
        domains_df=domains_df,
        urls_df=http_urls_df,
        database_connection=database_connection,
    )

    http_urls_df = http_urls_df.merge(
        domains_df.rename(columns={"id": "domain_id"}),
        left_on="tld_url",
        right_on="domain_name",
        how="left",
    )
    http_urls_df["scheme"] = "http"

    nonhttp_urls_df["scheme"] = nonhttp_urls_df["url"].str.split("://").str[0]
    nonhttp_urls_df["scheme"] = nonhttp_urls_df["scheme"].fillna("unknown")

    new_urls_df = pd.concat([http_urls_df, nonhttp_urls_df])

    upsert_df(
        df=new_urls_df,
        database_connection=database_connection,
        schema="adtech",
        table_name="urls",
    )


def upsert_click_url_redirect_chains(
    chain_df: pd.DataFrame, database_connection: PostgresCon
) -> None:
    """Upserts the redirect chain into the database."""
    new_urls_df = upsert_df(
        df=chain_df,
        database_connection=database_connection,
        schema="adtech",
        table_name="url_redirect_chains",
        key_columns=["run_id", "url", "redirect_url"],
        insert_columns=["run_id", "api_call_id", "url", "redirect_url"],
        md5_key_columns=["url", "redirect_url"],
    )


def follow_url_redirects(
    url: str, run_id: int, api_call_id: int, database_connection: PostgresCon
) -> list[str]:
    """Follows URL redirects and returns the final destination URL chain."""
    """
    Follows redirects and returns the final URL.

    Cache the results to avoid repeated requests.
    """
    existing_chain_df = get_click_url_redirect_chains(run_id, database_connection)
    if not existing_chain_df.empty and url in existing_chain_df["url"].to_list():
        existing_chain_df = existing_chain_df[existing_chain_df["url"] == url]
        redirect_chain = existing_chain_df["redirect_url"].to_list()
    else:
        redirect_chain = get_redirect_chain(url)
        if len(redirect_chain) > 0:
            chain_df = pd.DataFrame(redirect_chain)
            chain_df["run_id"] = run_id
            chain_df["api_call_id"] = api_call_id
            chain_df["url"] = url
            upsert_click_url_redirect_chains(chain_df, database_connection)
    return redirect_chain


def get_redirect_chain(url: str) -> list[str]:
    """Follows HTTP redirects for a given URL and returns the complete redirect chain."""
    max_redirects = 5
    chain = []
    cur_url = url
    hop_index = 0
    while cur_url and hop_index < max_redirects:
        try:
            headers = {"User-Agent": ANDROID_USER_AGENT}
            # Do NOT allow requests to auto-follow
            response = requests.get(
                cur_url, headers=headers, allow_redirects=False, timeout=(5, 5)
            )
            next_url = response.headers.get("Location")
        except Exception:
            next_url = None
            pass
        if next_url:
            chain.append({"hop_index": hop_index, "next_url": next_url})
            hop_index += 1
        if not next_url or not next_url.startswith("http"):
            break
        cur_url = next_url
    return chain


def parse_urls_for_known_parts(
    all_urls: list[str], database_connection: PostgresCon, pub_store_id: str
) -> AdInfo:
    """Parses URLs to extract advertiser store IDs, MMP URLs, and ad network domains."""
    found_mmp_urls = []
    found_adv_store_ids = []
    found_ad_network_urls = []
    ad_domains_df = query_ad_domains(database_connection=database_connection)
    for url in all_urls:
        adv_store_id = None
        tld_url = get_tld(url)
        if not tld_url:
            # only for use here, likely url like "market://"
            tld_url = ""
        if tld_url in get_all_mmp_tlds(database_connection)["mmp_tld"].to_list():
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
            tld_url in ad_domains_df["domain_name"].to_list()
            and tld_url
            not in get_all_mmp_tlds(database_connection)["mmp_tld"].to_list()
            and not any(ignore_url in url.lower() for ignore_url in IGNORE_PRIVACY_URLS)
        ):
            found_ad_network_urls.append(url)
    found_mmp_urls = list(set(found_mmp_urls))
    found_adv_store_ids = list(set(found_adv_store_ids))
    found_adv_store_ids = [x for x in found_adv_store_ids if x != pub_store_id]
    found_ad_network_tlds = [get_tld(url) for url in found_ad_network_urls]
    found_ad_network_urls = [x for x in found_ad_network_urls if x is not None]
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


def parse_youappi_ad(
    sent_video_dict: dict[str, Any], database_connection: PostgresCon
) -> AdInfo:
    """Parses YouAppi ad response to extract advertiser information and URLs."""
    ad_info, error_msg = parse_text_for_adinfo(
        text=sent_video_dict["response_text"],
        run_id=sent_video_dict["run_id"],
        pub_store_id=sent_video_dict["pub_store_id"],
        mitm_uuid=sent_video_dict["mitm_uuid"],
        database_connection=database_connection,
    )
    return ad_info


def parse_yandex_ad(
    sent_video_dict: dict[str, Any], database_connection: PostgresCon, video_id: str
) -> AdInfo:
    """Parses Yandex ad response to extract advertiser information and URLs."""
    json_text = json.loads(sent_video_dict["response_text"])

    if "native" in json_text:
        # ads = [x for x in json_text["native"]["ads"]]
        # ads[0]
        # video_id in str(ads[1])
        # "com.nomad" in str(ads[1])
        matched_ads = [x for x in json_text["native"]["ads"] if video_id in str(x)]
        if len(matched_ads) == 0:
            return AdInfo(
                adv_store_id=None,
            )
        text = str(matched_ads)
    else:
        text = sent_video_dict["response_text"]
    ad_info, error_msg = parse_text_for_adinfo(
        text=text,
        run_id=sent_video_dict["run_id"],
        pub_store_id=sent_video_dict["pub_store_id"],
        mitm_uuid=sent_video_dict["mitm_uuid"],
        database_connection=database_connection,
    )
    return ad_info


def parse_mtg_ad(
    sent_video_dict: dict[str, Any], database_connection: PostgresCon
) -> AdInfo:
    """Parses MTG ad response to extract advertiser package name and URLs."""
    text = sent_video_dict["response_text"]
    try:
        ad_response = json.loads(text)
        adv_store_id = ad_response["data"]["ads"][0]["package_name"]
        if adv_store_id:
            ad_info = AdInfo(
                adv_store_id=adv_store_id,
            )
            return ad_info
    except Exception:
        pass
    ad_info, _error_msg = parse_text_for_adinfo(
        text=text,
        run_id=sent_video_dict["run_id"],
        pub_store_id=sent_video_dict["pub_store_id"],
        mitm_uuid=sent_video_dict["mitm_uuid"],
        database_connection=database_connection,
    )
    return ad_info


class JsonRenderer(Renderer):
    def __init__(self) -> None:
        self.result: dict[str, Any] = dict()
        self.current: dict[str, Any] = self.result

    def _add(self, field_id: str, item: Any) -> None:
        """Adds an item to the current result dictionary."""
        self.current[field_id] = item

    def _build_tmp_item(self, chunk: Any) -> Any:
        """Builds a temporary item using a temporary renderer."""
        # use a temporary renderer to build
        jr = JsonRenderer()
        chunk.render(jr)
        tmp_dict = jr.build_result()

        # the tmp_dict only contains 1 item
        for _, item in tmp_dict.items():
            return item

    def build_result(self) -> dict[str, Any]:
        """Returns the built result dictionary."""
        return self.result

    def render_repeated_fields(self, repeated: Any) -> None:
        """Renders repeated fields into an array."""
        arr = []
        for ch in repeated.items:
            arr.append(self._build_tmp_item(ch))
        self._add(repeated.idtype.id, arr)

    def render_varint(self, varint: Any) -> None:
        """Renders a varint field."""
        self._add(varint.idtype.id, varint.i64)

    def render_fixed(self, fixed: Any) -> None:
        """Renders a fixed field."""
        self._add(fixed.idtype.id, fixed.i)

    def render_struct(self, struct: Any) -> None:
        """Renders a struct field."""
        curr: Any = None

        if struct.as_fields:
            curr = {}
            for ch in struct.as_fields:
                curr[ch.idtype.id] = self._build_tmp_item(ch)
        elif struct.is_str:
            curr = struct.as_str

        else:
            curr = " ".join(format(x, "02x") for x in struct.view)

        self._add(struct.idtype.id, curr)


def decode_utf8(view: Any) -> tuple[bytes, str, bool]:
    """Attempts to decode bytes as UTF-8 and returns the result with success status."""
    view_bytes = view.tobytes()
    try:
        utf8 = "UTF-8"
        decoded = view_bytes.decode(utf8)
        return decoded, utf8, True
    except Exception:
        return view_bytes, "", False


def base64decode(s: str) -> bytes:
    """Decodes a base64 string with proper padding."""
    missing_padding = len(s) % 4
    if missing_padding:
        s += "=" * (4 - missing_padding)
    return base64.urlsafe_b64decode(s)


def parse_bidmachine_ad(
    sent_video_dict: dict[str, Any], database_connection: PostgresCon
) -> AdInfo:
    """Parses BidMachine ad response using protobuf decoding to extract advertiser information."""
    adv_store_id = None
    additional_ad_network_tld = None
    ad_info = AdInfo(
        adv_store_id=adv_store_id,
    )
    if isinstance(sent_video_dict["response_content"], str):
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
        ad_info, _error_msg = parse_text_for_adinfo(
            text=text,
            run_id=sent_video_dict["run_id"],
            pub_store_id=sent_video_dict["pub_store_id"],
            mitm_uuid=sent_video_dict["mitm_uuid"],
            database_connection=database_connection,
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
            ad_info, _error_msg = parse_text_for_adinfo(
                text=str(ret),
                run_id=sent_video_dict["run_id"],
                pub_store_id=sent_video_dict["pub_store_id"],
                mitm_uuid=sent_video_dict["mitm_uuid"],
                database_connection=database_connection,
            )
        except Exception:
            pass
    if additional_ad_network_tld is not None and not ad_info.found_ad_network_tlds:
        ad_info.found_ad_network_tlds.append(additional_ad_network_tld)
    return ad_info


def parse_everestop_ad(sent_video_dict: dict[str, Any]) -> AdInfo:
    """Parses Everestop ad response using protobuf decoding to extract advertiser information."""
    if isinstance(sent_video_dict["response_content"], str):
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
        found_ad_network_tlds=[additional_ad_network_tld],
    )
    return ad_info


def parse_unity_ad(
    sent_video_dict: dict[str, Any], database_connection: PostgresCon
) -> tuple[AdInfo, str | None]:
    """Parses Unity ad response to extract advertiser information and bundle ID."""
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
    ad_info, error_msg = parse_text_for_adinfo(
        text=text,
        run_id=sent_video_dict["run_id"],
        pub_store_id=sent_video_dict["pub_store_id"],
        mitm_uuid=sent_video_dict["mitm_uuid"],
        database_connection=database_connection,
    )
    if error_msg:
        return ad_info, error_msg
    if ad_info.adv_store_id is None and adv_store_id is not None:
        ad_info.adv_store_id = adv_store_id
    if ad_info.found_mmp_urls is None and found_mmp_urls:
        ad_info.found_mmp_urls = found_mmp_urls
    return ad_info, error_msg


def parse_text_for_adinfo(
    text: str,
    pub_store_id: str,
    run_id: int,
    mitm_uuid: str,
    database_connection: PostgresCon,
) -> tuple[AdInfo, str | None]:
    """Extract URL like strings and parse for app store_ids."""
    all_urls = extract_and_decode_urls(
        text,
        run_id=run_id,
        mitm_uuid=mitm_uuid,
        database_connection=database_connection,
    )
    api_call_id = query_api_call_id_for_uuid(mitm_uuid, database_connection)
    click_urls = check_click_urls(all_urls, run_id, api_call_id, database_connection)
    all_urls = list(set(all_urls + click_urls))
    # This is going to need to be done before check click and after?
    _urls_df = store_found_urls_in_db(
        all_urls, run_id, api_call_id, database_connection
    )
    error_msg = None
    try:
        ad_info = parse_urls_for_known_parts(
            all_urls, database_connection, pub_store_id
        )
    except MultipleAdvertiserIdError as e:
        error_msg = f"multiple adv_store_id found for: {e.found_adv_store_ids}"
        logger.error(error_msg)
        ad_info = AdInfo(
            adv_store_id=None, found_ad_network_tlds=None, found_mmp_urls=None
        )
    return ad_info, error_msg


def parse_generic_adnetwork(
    sent_video_dict: dict[str, Any], database_connection: PostgresCon
) -> tuple[AdInfo, str | None]:
    """Parses generic ad network responses to extract advertiser information."""
    ad_info, error_msg = parse_text_for_adinfo(
        text=sent_video_dict["response_text"],
        pub_store_id=sent_video_dict["pub_store_id"],
        run_id=sent_video_dict["run_id"],
        mitm_uuid=sent_video_dict["mitm_uuid"],
        database_connection=database_connection,
    )
    return ad_info, error_msg


def parse_vungle_ad(
    sent_video_dict: dict[str, Any], database_connection: PostgresCon
) -> AdInfo:
    """Parses Vungle ad response to extract advertiser market ID and tracking URLs."""
    found_mmp_urls = None
    adv_store_id = None
    error_msg = None
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
                    if (
                        get_tld(url)
                        in get_all_mmp_tlds(database_connection)["mmp_tld"].to_list()
                    ):
                        found_mmp_urls.append(url)
            except Exception:
                pass
    except Exception:
        pass
    if not adv_store_id:
        ad_info, error_msg = parse_text_for_adinfo(
            text=ad_response_text,
            pub_store_id=sent_video_dict["pub_store_id"],
            run_id=sent_video_dict["run_id"],
            mitm_uuid=sent_video_dict["mitm_uuid"],
            database_connection=database_connection,
        )
    else:
        ad_info = AdInfo(
            adv_store_id=adv_store_id,
            found_mmp_urls=found_mmp_urls,
        )
    return ad_info, error_msg


def parse_fyber_ad(
    sent_video_dict: dict[str, Any], database_connection: PostgresCon
) -> AdInfo:
    """Parses Fyber ad response to extract advertiser information and URLs."""
    parsed_urls = []
    text = sent_video_dict["response_text"]
    try:
        parsed_urls = parse_fyber_ad_response(
            text,
            run_id=sent_video_dict["run_id"],
            mitm_uuid=sent_video_dict["mitm_uuid"],
            database_connection=database_connection,
        )
    except Exception:
        pass
    all_urls = extract_and_decode_urls(
        text=text,
        run_id=sent_video_dict["run_id"],
        mitm_uuid=sent_video_dict["mitm_uuid"],
        database_connection=database_connection,
    )
    all_urls = list(set(all_urls + parsed_urls))
    if "inner-active.mobi" in sent_video_dict["tld_url"]:
        if "x-ia-app-bundle" in sent_video_dict["response_headers"].keys():
            adv_store_id = sent_video_dict["response_headers"]["x-ia-app-bundle"]
            ad_info = AdInfo(
                adv_store_id=adv_store_id,
            )
            return ad_info
    ad_info = parse_urls_for_known_parts(
        all_urls, database_connection, sent_video_dict["pub_store_id"]
    )
    return ad_info


def parse_google_ad(
    sent_video_dict: dict[str, Any], video_id: str, database_connection: PostgresCon
) -> tuple[AdInfo, str | None]:
    """Parses Google DoubleClick ad response to extract advertiser information."""
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
            ad_info, error_msg = parse_text_for_adinfo(
                text=big_html,
                pub_store_id=sent_video_dict["pub_store_id"],
                run_id=sent_video_dict["run_id"],
                mitm_uuid=sent_video_dict["mitm_uuid"],
                database_connection=database_connection,
            )
            if error_msg:
                return ad_info, error_msg
        elif "slots" in google_response:
            found_adv_in_slots = None
            for slot in google_response["slots"]:
                if found_adv_in_slots:
                    continue
                if video_id in str(slot):
                    for ad in slot["ads"]:
                        if found_adv_in_slots:
                            continue
                        if video_id in str(ad):
                            ad_info, error_msg = parse_text_for_adinfo(
                                # Previously this was checking slot, try just per ad?
                                text=str(ad),
                                pub_store_id=sent_video_dict["pub_store_id"],
                                run_id=sent_video_dict["run_id"],
                                mitm_uuid=sent_video_dict["mitm_uuid"],
                                database_connection=database_connection,
                            )
                            if error_msg:
                                return ad_info, error_msg
                            if ad_info["adv_store_id"] is not None:
                                found_adv_in_slots = True
            if found_adv_in_slots is None:
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
            ad_info, error_msg = parse_text_for_adinfo(
                text=response_text,
                pub_store_id=sent_video_dict["pub_store_id"],
                run_id=sent_video_dict["run_id"],
                mitm_uuid=sent_video_dict["mitm_uuid"],
                database_connection=database_connection,
            )
            if error_msg:
                return ad_info, error_msg
        else:
            error_msg = "doubleclick new format"
            logger.error(error_msg)
            return ad_info, error_msg
    return ad_info, error_msg


def parse_sent_video_df(
    row: pd.Series,
    pub_store_id: str,
    sent_video_df: pd.DataFrame,
    database_connection: PostgresCon,
    video_id: str,
) -> tuple[list[AdInfo], list[dict[str, Any]]]:
    """Parses video data to extract advertiser information from various ad networks."""
    error_messages = []
    run_id = row["run_id"]
    sent_video_dicts = sent_video_df.to_dict(orient="records")
    found_ad_infos = []
    for sent_video_dict in sent_video_dicts:
        error_msg = None
        init_url = sent_video_dict["url"]
        init_tld = sent_video_dict["tld_url"]
        logger.debug(f"Parsing ad network {init_url=} for adv")
        if "vungle.com" == init_tld:
            ad_info, error_msg = parse_vungle_ad(sent_video_dict, database_connection)
        elif "bidmachine.io" == init_tld:
            ad_info = parse_bidmachine_ad(sent_video_dict, database_connection)
        elif (
            "fyber.com" == init_tld
            or "tpbid.com" == init_tld
            or "inner-active.mobi" == init_tld
        ):
            init_tld = "fyber.com"
            ad_info = parse_fyber_ad(sent_video_dict, database_connection)
        elif "everestop.io" == init_tld:
            ad_info = parse_everestop_ad(sent_video_dict)
        elif "doubleclick.net" == init_tld:
            ad_info, error_msg = parse_google_ad(
                sent_video_dict, video_id, database_connection
            )
        elif "unityads.unity3d.com" in init_url:
            ad_info, error_msg = parse_unity_ad(sent_video_dict, database_connection)
        elif "mtgglobals.com" == init_tld:
            ad_info = parse_mtg_ad(sent_video_dict, database_connection)
        elif "yandex.ru" == init_tld:
            ad_info = parse_yandex_ad(sent_video_dict, database_connection, video_id)
        elif "youappi.com" == init_tld:
            ad_info = parse_youappi_ad(sent_video_dict, database_connection)
        else:
            ad_info, error_msg = parse_generic_adnetwork(
                sent_video_dict, database_connection
            )
        if error_msg:
            row["error_msg"] = error_msg
            logger.error(f"{error_msg} for video {video_id[0:10]}")
            error_messages.append(row)
            continue
        if ad_info["adv_store_id"] is None:
            # This is doubling the time for the run as it is parsing the text again
            # but this does seem to could catch misses often enough to keep it
            ad_parts, _error_msg = parse_text_for_adinfo(
                text=sent_video_dict["response_text"],
                pub_store_id=sent_video_dict["pub_store_id"],
                run_id=run_id,
                mitm_uuid=sent_video_dict["mitm_uuid"],
                database_connection=database_connection,
            )
            if ad_parts["adv_store_id"] is not None:
                error_msg = "found potential app! adv_store_id"
                row["error_msg"] = error_msg
                error_messages.append(row)
                continue
            else:
                ad_info["adv_store_id"] = None
        if ad_info["adv_store_id"] == pub_store_id:
            error_msg = "Incorrect adv_store_id, identified pub ID as adv ID"
            logger.error(
                f"Incorrect adv_store_id, identified pub ID as adv ID for video {video_id[0:10]}"
            )
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        if ad_info["adv_store_id"] is None:
            error_msg = "No adv_store_id found"
            logger.debug(error_msg)
        try:
            if ad_info["adv_store_id"] is None:
                adv_db_id = None
            else:
                adv_db_id = query_store_app_by_store_id_cached(
                    store_id=ad_info["adv_store_id"],
                    database_connection=database_connection,
                    case_insensitive=True,
                )
            ad_info["adv_store_app_id"] = adv_db_id
        except Exception:
            error_msg = (
                f"found potential app but failed to get db id {ad_info['adv_store_id']}"
            )
            logger.error(error_msg)
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        ad_info["init_tld"] = init_tld
        found_ad_infos.append(ad_info)
    return found_ad_infos, error_messages
