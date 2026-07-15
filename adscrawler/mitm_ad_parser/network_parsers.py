import ast
import base64
import hashlib
import html
import json
import re
import urllib
import xml.etree.ElementTree as ET
from typing import Any

import numpy as np
import pandas as pd
import protod
import requests
from bs4 import BeautifulSoup
from protod import Renderer

from adscrawler.config import get_logger
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import (
    clear_url_query_caches,
    get_all_mmp_tlds_set,
    get_click_url_redirect_chains,
    query_all_domains,
    query_api_call_id_for_uuid,
    query_domains_set,
    query_store_app_by_store_id_cached,
    query_urls_by_hashes,
    upsert_df,
)
from adscrawler.mitm_ad_parser.models import AdInfo, MultipleAdvertiserIdError
from adscrawler.mitm_ad_parser.utils import get_tld

logger = get_logger(__name__, "mitm_scrape_ads")

PLAYSTORE_URL_PARTS = ["play.google.com/store", "market://", "intent://"]

SUPPORTED_URL_SCHEMES = {"http", "https", "intent", "market", "fybernativebrowser"}
MAX_URL_EXTRACTION_DEPTH = 4

URL_PATTERN = re.compile(
    r"""(?:
    (?:https?|intent|market|fybernativebrowser):\/\/      # allowed schemes
    [^\s'"<>\]\)\}]+?                                   # non-greedy match
)
(?=[\s"\\;'<>"]|[\]\)\}\{,]|$)                      # separator or end
""",
    re.VERBOSE,
)

URL_VALUE_HINT_RE = re.compile(r"(?i)(?:://|%3A%2F%2F)")
ENCODED_SCHEME_RE = re.compile(
    r"(?i)\b(https?|intent|market|fybernativebrowser)%3A%2F%2F"
)
DECODED_CONTEXT_DELIMITERS_RE = re.compile(r"[\[\]<>]")

ANDROID_USER_AGENT = "Mozilla/5.0 (Linux; Android 13; Pixel 7 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"

IGNORE_STORE_IDS = ["com.android.vending"]


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


TRAILING_ENCODED_URL_DELIMITER_RE = re.compile(r"(?i)(%5D|%3E|%5B|%3C)$")


def strip_trailing_encoded_url_delimiters(url: str) -> str:
    """Remove only unmatched encoded wrapper delimiters from the end of a URL."""
    delimiter_pairs = {
        "%5d": ("[", "]"),
        "%5b": ("[", "]"),
        "%3e": ("<", ">"),
        "%3c": ("<", ">"),
    }
    while True:
        match = TRAILING_ENCODED_URL_DELIMITER_RE.search(url)
        if match is None:
            return url
        token = match.group(1).lower()
        open_char, close_char = delimiter_pairs[token]
        decoded_url = urllib.parse.unquote(url)
        open_count = decoded_url.count(open_char)
        close_count = decoded_url.count(close_char)
        should_strip = False
        if token in {"%5d", "%3e"} and close_count > open_count:
            should_strip = True
        elif token in {"%5b", "%3c"} and open_count > close_count:
            should_strip = True
        if not should_strip:
            return url
        url = url[: -len(match.group(1))]


def _has_balanced_delimiters(text: str, open_char: str, close_char: str) -> bool:
    return text.count(open_char) == text.count(close_char)


def _is_reasonable_decoded_url(url: str) -> bool:
    if "<" in url or ">" in url:
        return False
    return _has_balanced_delimiters(url, "[", "]")


def _is_valid_extracted_url(url: str) -> bool:
    if not _has_balanced_delimiters(url, "[", "]"):
        return False
    try:
        parsed = urllib.parse.urlsplit(url)
    except Exception:
        return False
    try:
        hostname = parsed.hostname
    except ValueError:
        return False
    if not hostname or any(char in hostname for char in "%[]<>'\""):
        return False
    return parsed.scheme.lower() in SUPPORTED_URL_SCHEMES


def _decode_scheme_markers(text: str) -> str:
    return ENCODED_SCHEME_RE.sub(lambda match: f"{match.group(1)}://", text)


def _scrub_decoded_context_delimiters(text: str) -> str:
    return DECODED_CONTEXT_DELIMITERS_RE.sub(" ", text)


def _normalize_extracted_url(url: str) -> str | None:
    cleaned_url = strip_trailing_encoded_url_delimiters(url.replace("\x00", ""))
    decoded_url = urllib.parse.unquote(cleaned_url)
    if decoded_url != cleaned_url and _is_reasonable_decoded_url(decoded_url):
        normalized_url = decoded_url
    else:
        normalized_url = cleaned_url
    if _is_valid_extracted_url(normalized_url):
        return normalized_url
    return None


def _extract_query_value_chunks(url: str) -> list[str]:
    values = []
    try:
        parsed = urllib.parse.urlsplit(url)
    except Exception:
        return values
    query_blobs = [parsed.query]
    if parsed.fragment and "=" in parsed.fragment:
        query_blobs.append(parsed.fragment)
    for query_blob in query_blobs:
        if not query_blob:
            continue
        for item in query_blob.split("&"):
            if not item:
                continue
            _, _sep, value = item.partition("=")
            if value and URL_VALUE_HINT_RE.search(value):
                values.append(value)
    return values


def _derive_additional_search_chunks(url: str) -> list[tuple[str, bool]]:
    next_chunks: list[tuple[str, bool]] = []
    for value in _extract_query_value_chunks(url):
        next_chunks.append((value, True))
        scheme_decoded_value = _decode_scheme_markers(value)
        if scheme_decoded_value != value:
            next_chunks.append((scheme_decoded_value, True))
        decoded_value = urllib.parse.unquote(value)
        if decoded_value != value:
            next_chunks.append((decoded_value, True))
            scrubbed_decoded_value = _scrub_decoded_context_delimiters(decoded_value)
            if scrubbed_decoded_value != decoded_value:
                next_chunks.append((scrubbed_decoded_value, True))
        unescaped_value = html.unescape(value)
        if unescaped_value != value:
            next_chunks.append((unescaped_value, True))
        scrubbed_value = _scrub_decoded_context_delimiters(value)
        if scrubbed_value != value:
            next_chunks.append((scrubbed_value, True))
    return next_chunks


def _build_initial_search_chunks(
    text: str, vast_urls: list[str]
) -> list[tuple[str, bool]]:
    search_chunks: list[tuple[str, bool]] = [(text, True)]
    unescaped_text = html.unescape(text)
    if unescaped_text != text:
        search_chunks.append((unescaped_text, True))
    try:
        unicode_decoded_text = text.encode("utf-8").decode("unicode_escape")
    except Exception:
        unicode_decoded_text = None
    if unicode_decoded_text and unicode_decoded_text != text:
        search_chunks.append((unicode_decoded_text, True))
    if "://" not in text:
        scheme_decoded_text = _decode_scheme_markers(text)
        if scheme_decoded_text != text:
            search_chunks.append((scheme_decoded_text, True))
        decoded_text = urllib.parse.unquote(text)
        if decoded_text != text:
            search_chunks.append((decoded_text, True))
            scheme_decoded_decoded_text = _decode_scheme_markers(decoded_text)
            if scheme_decoded_decoded_text != decoded_text:
                search_chunks.append((scheme_decoded_decoded_text, True))
    for url in vast_urls:
        search_chunks.append((url, True))
    return search_chunks


def _extract_urls_from_chunks(search_chunks: list[tuple[str, bool]]) -> list[str]:
    found_urls: set[str] = set()
    seen_chunks: set[tuple[str, bool]] = set()
    processed_candidates: set[str] = set()
    current_chunks = search_chunks
    for _depth in range(MAX_URL_EXTRACTION_DEPTH):
        if not current_chunks:
            break
        next_chunks: list[tuple[str, bool]] = []
        for chunk, allow_full_match in current_chunks:
            chunk_key = (chunk, allow_full_match)
            if not chunk or chunk_key in seen_chunks:
                continue
            seen_chunks.add(chunk_key)
            for match in URL_PATTERN.finditer(chunk):
                raw_candidate = strip_trailing_encoded_url_delimiters(
                    match.group(0).replace("\x00", "")
                )
                if not raw_candidate:
                    continue
                should_emit = allow_full_match or match.start() > 0
                if raw_candidate not in processed_candidates:
                    processed_candidates.add(raw_candidate)
                    normalized_url = _normalize_extracted_url(raw_candidate)
                    if normalized_url is not None and should_emit:
                        found_urls.add(normalized_url)
                    next_chunks.extend(_derive_additional_search_chunks(raw_candidate))
                elif should_emit:
                    normalized_url = _normalize_extracted_url(raw_candidate)
                    if normalized_url is not None:
                        found_urls.add(normalized_url)
        current_chunks = next_chunks
    return list(found_urls)


def extract_and_decode_urls(text: str) -> list[str]:
    """Extracts and decodes all URLs from text content, handling various encoding formats."""
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
    search_chunks = _build_initial_search_chunks(text=text, vast_urls=vast_urls)
    all_urls = _extract_urls_from_chunks(search_chunks)
    return all_urls


def store_found_urls_in_db(
    all_urls: list[str],
    run_id: int,
    api_call_id: int,
    pgdb: PostgresEngine,
) -> None:
    """Stores the found URLs in the database."""
    urls_df = upsert_urls(urls=all_urls, pgdb=pgdb)
    urls_df["api_call_id"] = api_call_id
    urls_df["run_id"] = run_id
    upsert_df(
        df=urls_df[["run_id", "api_call_id", "url_id"]],
        pgdb=pgdb,
        schema="adtech",
        table_name="api_call_urls",
        key_columns=["run_id", "api_call_id", "url_id"],
        insert_columns=["run_id", "api_call_id", "url_id"],
    )


def check_click_urls(
    all_urls: list[str],
    run_id: int,
    api_call_id: int,
    pgdb: PostgresEngine,
) -> list[str]:
    """Checks URLs for click tracking and follows redirects to find final destination URLs."""
    click_urls = []
    for url in all_urls:
        redirect_urls = []
        if (
            "/click" in url
            or "/clk" in url
            or "onelink.me" in url
            or "yandex.ru/an/count/" in url
        ):
            if "tpbid.com" in url:
                url = url.replace("fybernativebrowser://navigate?url=", "")
            redirect_urls = follow_url_redirects(url, run_id, api_call_id, pgdb)
        elif "fybernativebrowser://navigate?url=" in url:
            url = url.replace("fybernativebrowser://navigate?url=", "")
            redirect_urls = follow_url_redirects(url, run_id, api_call_id, pgdb)
        if len(redirect_urls) > 0:
            click_urls += redirect_urls
    click_urls = list(set(click_urls))
    return click_urls


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
    pgdb: PostgresEngine,
) -> pd.DataFrame:
    """Adds missing domains to the database and returns updated domain DataFrame."""
    col = "tld_url"
    urls_df = urls_df[urls_df[col].notna()]
    existing_domains = pd.Index(domains_df["domain_name"])
    new_domains = pd.Index(urls_df[col]).difference(existing_domains)
    if not new_domains.empty:
        new_domains = pd.DataFrame({"domain_name": list(new_domains)}).drop_duplicates()
        new_domains = upsert_df(
            table_name="domains",
            df=new_domains,
            insert_columns=["domain_name"],
            key_columns=["domain_name"],
            pgdb=pgdb,
            return_rows=True,
        )
        domains_df = pd.concat([new_domains, domains_df])
    domains_df["id"] = domains_df["id"].astype(int)
    domains_df = domains_df.rename(columns={"id": "domain_id"})
    return domains_df


def upsert_urls(urls: list[str], pgdb: PostgresEngine) -> pd.DataFrame:
    """Upserts the URLs into the database."""
    urls_df = pd.DataFrame({"url": urls})
    urls_df["url_hash"] = urls_df["url"].apply(
        lambda x: hashlib.md5(x.encode()).hexdigest()
    )
    existing_urls_df = query_urls_by_hashes(urls_df["url_hash"].tolist(), pgdb=pgdb)
    if existing_urls_df.empty:
        existing_urls_df = pd.DataFrame(columns=["url_hash", "url_id"])
    else:
        existing_urls_df = existing_urls_df.rename(columns={"id": "url_id"})
    existing_hashes = set(existing_urls_df["url_hash"].tolist())
    new_urls_df = urls_df[~urls_df["url_hash"].isin(existing_hashes)].copy()
    if new_urls_df.empty:
        urls_df = urls_df.merge(
            existing_urls_df[["url_hash", "url_id"]].drop_duplicates("url_hash"),
            on="url_hash",
            how="left",
            validate="m:1",
        )
        return urls_df
    http_urls_df = new_urls_df[new_urls_df["url"].str.startswith("http")].copy()
    nonhttp_urls_df = new_urls_df[~new_urls_df["url"].str.startswith("http")].copy()
    http_urls_df["tld_url"] = http_urls_df["url"].apply(lambda x: get_tld(x))
    domains_df = query_all_domains(pgdb=pgdb)
    domains_df = check_and_upsert_new_domains(
        domains_df=domains_df,
        urls_df=http_urls_df,
        pgdb=pgdb,
    )
    http_urls_df = http_urls_df.merge(
        domains_df,
        left_on="tld_url",
        right_on="domain_name",
        how="left",
    )
    http_urls_df["scheme"] = "http"
    nonhttp_urls_df["scheme"] = nonhttp_urls_df["url"]\
        .str.split("://").str[0]\
        .str.replace(r"[^a-zA-Z0-9_\-+]", "", regex=True)\
        .str[:128]
    nonhttp_urls_df["scheme"] = nonhttp_urls_df["scheme"].fillna("unknown")
    nonhttp_urls_df.loc[nonhttp_urls_df["scheme"].str.strip() == "", "scheme"] = "unknown"
    new_urls_df = pd.concat([http_urls_df, nonhttp_urls_df])
    new_urls_df["domain_id"] = np.where(
        pd.isna(new_urls_df["domain_id"]), None, new_urls_df["domain_id"]
    )
    # new_urls_df["url_hash"] = new_urls_df["url"].apply(
    #     lambda x: hashlib.md5(x.encode()).hexdigest()
    # )
    logger.info(f"Upserting {new_urls_df.shape[0]:,} new urls")
    new_urls_df: pd.DataFrame = upsert_df(
        df=new_urls_df,
        pgdb=pgdb,
        schema="adtech",
        table_name="urls",
        insert_columns=["url", "url_hash", "domain_id", "scheme"],
        key_columns=["url_hash"],
        return_rows=True,
    )
    new_urls_df = new_urls_df.rename(columns={"id": "url_id"})
    clear_url_query_caches()
    resolved_urls_df = pd.concat(
        [
            existing_urls_df[["url_hash", "url_id"]],
            new_urls_df[["url_hash", "url_id"]],
        ],
        ignore_index=True,
    ).drop_duplicates("url_hash", keep="last")
    urls_df = urls_df.merge(
        resolved_urls_df[["url_hash", "url_id"]],
        on="url_hash",
        how="left",
        validate="m:1",
    )
    return urls_df


def upsert_click_url_redirect_chains(
    chain_df: pd.DataFrame, pgdb: PostgresEngine
) -> None:
    """Upserts the redirect chain into the database."""
    urls = list(set(chain_df["url"].to_list() + chain_df["next_url"].to_list()))
    urls_df = upsert_urls(urls=urls, pgdb=pgdb)
    chain_df = chain_df.merge(
        urls_df[["url", "url_id"]],
        on="url",
        how="left",
        validate="m:1",
    )
    chain_df = chain_df.merge(
        urls_df[["url", "url_id"]].rename(
            columns={"url_id": "next_url_id", "url": "next_url"}
        ),
        on="next_url",
        how="left",
        validate="m:1",
    )
    upsert_df(
        df=chain_df[["run_id", "api_call_id", "url_id", "next_url_id", "hop_index"]],
        pgdb=pgdb,
        schema="adtech",
        table_name="url_redirect_chains",
        key_columns=["run_id", "api_call_id", "url_id", "next_url_id"],
        insert_columns=["run_id", "api_call_id", "url_id", "next_url_id", "hop_index"],
    )


def follow_url_redirects(
    url: str, run_id: int, api_call_id: int, pgdb: PostgresEngine
) -> list[str]:
    """Follows URL redirects and returns the final destination URL chain."""
    """
    Follows redirects and returns the final URL.

    Cache the results in the database to avoid repeated requests.
    """
    existing_chain_df = get_click_url_redirect_chains(run_id, pgdb)
    if not existing_chain_df.empty and url in existing_chain_df["url"].to_list():
        existing_chain_df = existing_chain_df[
            (existing_chain_df["url"] == url)
            & (existing_chain_df["api_call_id"] == api_call_id)
        ]
        if not existing_chain_df.empty:
            redirect_chain: list[str] = existing_chain_df["redirect_url"].to_list()
            return redirect_chain
    # New chain, insert after getting the chain
    redirect_chain_dict = get_redirect_chain(url)
    if len(redirect_chain_dict) > 0:
        logger.info(f"Found new click redirects: {len(redirect_chain_dict)}")
        chain_df = pd.DataFrame(redirect_chain_dict)
        chain_df["run_id"] = run_id
        chain_df["api_call_id"] = api_call_id
        chain_df["url"] = url
        upsert_click_url_redirect_chains(chain_df, pgdb)
        redirect_chain = list(
            set(chain_df["next_url"].to_list() + chain_df["url"].to_list())
        )
    else:
        redirect_chain = []
    return redirect_chain


def get_redirect_chain(url: str) -> list[dict]:
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
            chain.append({"hop_index": hop_index, "url": cur_url, "next_url": next_url})
            hop_index += 1
        if not next_url or not next_url.startswith("http"):
            break
        cur_url = next_url
    return chain


def parse_urls_for_known_parts(
    all_urls: list[str], pgdb: PostgresEngine, pub_store_id: str
) -> AdInfo:
    """Parses URLs to extract advertiser store IDs, MMP URLs, and ad network domains."""
    found_mmp_urls = []
    found_adv_store_ids = []
    found_ad_network_urls = []
    ad_domains_set = query_domains_set(pgdb=pgdb)
    mmps_set = get_all_mmp_tlds_set(pgdb)
    for url in all_urls:
        adv_store_id = None
        tld_url = get_tld(url)
        if not tld_url:
            # only for use here, likely url like "market://"
            tld_url = ""
        if tld_url in mmps_set:
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
                matches = re.search(
                    r"http.*\.appsflyer\.com/([a-zA-Z0-9_.]+)[\?\-]", url
                )
                if matches and matches.group(1):
                    adv_store_id = matches.group(1)
                    found_adv_store_ids.append(adv_store_id)
        elif match := re.search(r"intent://details\?id=([a-zA-Z0-9._]+)", url):
            adv_store_id = match.group(1)
            if adv_store_id.startswith("bidease.com_"):
                found_ad_network_urls.append("bidease.com")
                continue
            else:
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
        if (
            tld_url in ad_domains_set
            and tld_url not in mmps_set
            and not any(ignore_url in url.lower() for ignore_url in IGNORE_PRIVACY_URLS)
        ):
            found_ad_network_urls.append(url)
    found_mmp_urls = list(set(found_mmp_urls))
    found_adv_store_ids = list(set(found_adv_store_ids))
    ignore_adv_ids = IGNORE_STORE_IDS + [pub_store_id]
    found_adv_store_ids = [x for x in found_adv_store_ids if x not in ignore_adv_ids]
    found_ad_network_urls = [x for x in found_ad_network_urls if x is not None]
    found_ad_network_tlds_list = [get_tld(url) for url in found_ad_network_urls]
    found_ad_network_tlds = list(
        set([x for x in found_ad_network_tlds_list if x is not None])
    )
    if len(found_adv_store_ids) == 0:
        adv_store_id = None
    elif len(found_adv_store_ids) == 1:
        adv_store_id = found_adv_store_ids[0]
    else:
        raise MultipleAdvertiserIdError(found_adv_store_ids=found_adv_store_ids)
    return AdInfo(
        adv_store_id=adv_store_id,
        found_mmp_urls=found_mmp_urls,
        found_ad_network_tlds=found_ad_network_tlds,
    )


def get_request_text(sent_video_dict: dict[str, Any]) -> str:
    """Try to match something.
    It is unknown if this matches anything useful.
    """
    text = " ".join(
        [
            str(x)
            for x in [
                sent_video_dict["query_params"],
                sent_video_dict["response_headers"],
                sent_video_dict["post_params"],
                sent_video_dict["request_text"],
            ]
        ]
    )
    return text


def parse_youappi_ad(
    sent_video_dict: dict[str, Any], pgdb: PostgresEngine
) -> tuple[AdInfo, str | None]:
    """Parses YouAppi ad response to extract advertiser information and URLs."""
    if (
        "image" in sent_video_dict["response_mime_type"]
        or "video" in sent_video_dict["response_mime_type"]
    ):
        text = get_request_text(sent_video_dict)
    else:
        text = sent_video_dict["response_text"]
    ad_info, error_msg = parse_text_for_adinfo(
        text=text,
        run_id=sent_video_dict["run_id"],
        pub_store_id=sent_video_dict["pub_store_id"],
        mitm_uuid=sent_video_dict["mitm_uuid"],
        pgdb=pgdb,
    )
    return ad_info, error_msg


def parse_yandex_ad(
    sent_video_dict: dict[str, Any], pgdb: PostgresEngine, video_id: str
) -> AdInfo:
    """Parses Yandex ad response to extract advertiser information and URLs."""
    text_dict = json.loads(sent_video_dict["response_text"])
    if "native" in text_dict:
        # ads = [x for x in json_text["native"]["ads"]]
        # ads[0]
        # video_id in str(ads[1])
        # "com.nomad" in str(ads[1])
        matched_ads = [x for x in text_dict["native"]["ads"] if video_id in str(x)]
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
        pgdb=pgdb,
    )
    return ad_info


def parse_mtg_ad(sent_video_dict: dict[str, Any], pgdb: PostgresEngine) -> AdInfo:
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
        pgdb=pgdb,
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
    sent_video_dict: dict[str, Any], pgdb: PostgresEngine
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
            pgdb=pgdb,
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
                pgdb=pgdb,
            )
        except Exception:
            pass
    if isinstance(additional_ad_network_tld, str) and not ad_info.found_ad_network_tlds:
        ad_info.found_ad_network_tlds = [additional_ad_network_tld]
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
    adv_store_id = None
    additional_ad_network_tld = None
    try:
        adv_store_id = ret[5][6][3][13][2][3]
        additional_ad_network_tld = ret[5][6][3][13][2][2]
    except Exception:
        pass

    if not isinstance(additional_ad_network_tld, str):
        additional_ad_network_tld = None

    ad_info = AdInfo(
        adv_store_id=adv_store_id,
        found_ad_network_tlds=(
            [additional_ad_network_tld] if additional_ad_network_tld else None
        ),
    )
    return ad_info


def parse_unity_ad(
    sent_video_dict: dict[str, Any], pgdb: PostgresEngine
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
                referrer = adcontent.split("referrer=")[1].split(",", maxsplit=1)[0]
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
        pgdb=pgdb,
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
    pgdb: PostgresEngine,
) -> tuple[AdInfo, str | None]:
    """Extract URL like strings and parse for app store_ids."""
    log_info = (
        f"pub_store_id={pub_store_id} run_id={run_id} mitm_uuid={mitm_uuid} parse_text"
    )
    logger.info(f"{log_info} start")
    ad_info = AdInfo(adv_store_id=None, found_ad_network_tlds=None, found_mmp_urls=None)
    error_msg = None
    all_urls = extract_and_decode_urls(text)
    api_call_id = query_api_call_id_for_uuid(mitm_uuid, pgdb)
    click_urls = check_click_urls(all_urls, run_id, api_call_id, pgdb)
    all_urls = list(set(all_urls + click_urls))
    if len(all_urls) > 0:
        store_found_urls_in_db(all_urls, run_id, api_call_id, pgdb)
        try:
            ad_info = parse_urls_for_known_parts(all_urls, pgdb, pub_store_id)
        except MultipleAdvertiserIdError as e:
            error_msg = f"multiple adv_store_id found for: {e.found_adv_store_ids}"
            logger.error(f"{log_info} {error_msg}")
    else:
        error_msg = "No URLs found"
        logger.debug(f"{log_info} {error_msg}")
    if click_urls and len(click_urls) > 0:
        logger.info(f"{log_info} append clicks start")
        click_url_hashes = [hashlib.md5(url.encode()).hexdigest() for url in click_urls]
        cdf = query_urls_by_hashes(click_url_hashes, pgdb)
        if not cdf.empty:
            ad_info.click_url_ids = cdf["id"].tolist()
        else:
            logger.error(f"{log_info} Click URLs found but no URL IDs in DB")
        logger.info(f"{log_info} append clicks end")
    logger.info(f"{log_info} end")
    return ad_info, error_msg


def parse_generic_adnetwork(
    sent_video_dict: dict[str, Any], pgdb: PostgresEngine
) -> tuple[AdInfo, str | None]:
    """Parses generic ad network responses to extract advertiser information."""
    ad_info, error_msg = parse_text_for_adinfo(
        text=sent_video_dict["response_text"],
        pub_store_id=sent_video_dict["pub_store_id"],
        run_id=sent_video_dict["run_id"],
        mitm_uuid=sent_video_dict["mitm_uuid"],
        pgdb=pgdb,
    )
    return ad_info, error_msg


def parse_vungle_ad(
    sent_video_dict: dict[str, Any], pgdb: PostgresEngine
) -> tuple[AdInfo, str | None]:
    """Parses Vungle ad response to extract advertiser market ID and tracking URLs."""
    found_mmp_urls = []
    adv_store_id = None
    error_msg = None
    ad_response_text = sent_video_dict["response_text"]
    try:
        response_dict = json.loads(ad_response_text)
        adv_store_id = response_dict["ads"][0]["ad_markup"]["ad_market_id"]
        check_urls = ["clickUrl", "checkpoint.0", "checkpoint.100"]
        urlkeys = response_dict["ads"][0]["ad_markup"]["tpat"]
        mmps_set = get_all_mmp_tlds_set(pgdb)
        for x in check_urls:
            try:
                these_urls = urlkeys[x]
                for url in these_urls:
                    if get_tld(url) in mmps_set:
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
            pgdb=pgdb,
        )
    else:
        ad_info = AdInfo(
            adv_store_id=adv_store_id,
            found_mmp_urls=found_mmp_urls if found_mmp_urls else None,
        )
    return ad_info, error_msg


def parse_fyber_ad(sent_video_dict: dict[str, Any], pgdb: PostgresEngine) -> AdInfo:
    """Parses Fyber ad response to extract advertiser information and URLs."""
    if "inner-active.mobi" in sent_video_dict["tld_url"]:
        if "x-ia-app-bundle" in sent_video_dict["response_headers"].keys():
            adv_store_id = sent_video_dict["response_headers"]["x-ia-app-bundle"]
            ad_info = AdInfo(
                adv_store_id=adv_store_id,
            )
            return ad_info
    text = sent_video_dict["response_text"]
    all_urls = extract_and_decode_urls(text=text)
    ad_info = parse_urls_for_known_parts(
        all_urls, pgdb, sent_video_dict["pub_store_id"]
    )
    return ad_info


def parse_google_ad(
    sent_video_dict: dict[str, Any], video_id: str, pgdb: PostgresEngine
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
        all_html = ""
        good_html = ""
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
                    all_html += ad_html
                    if video_id in ad_html:
                        logger.info(f"Found {video_id=} in ad html")
                        good_html += ad_html
            ad_info, error_msg = parse_text_for_adinfo(
                text=good_html,
                pub_store_id=sent_video_dict["pub_store_id"],
                run_id=sent_video_dict["run_id"],
                mitm_uuid=sent_video_dict["mitm_uuid"],
                pgdb=pgdb,
            )
            if error_msg:
                ad_info, error_msg = parse_text_for_adinfo(
                    text=all_html,
                    pub_store_id=sent_video_dict["pub_store_id"],
                    run_id=sent_video_dict["run_id"],
                    mitm_uuid=sent_video_dict["mitm_uuid"],
                    pgdb=pgdb,
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
                                pgdb=pgdb,
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
                pgdb=pgdb,
            )
            if error_msg:
                return ad_info, error_msg
        else:
            error_msg = "doubleclick new format"
            logger.error(error_msg)
            return ad_info, error_msg
    return ad_info, error_msg


def parse_creative_request(
    sent_video_dict: dict[str, Any], pgdb: PostgresEngine
) -> tuple[AdInfo, str | None]:
    """Parses creative request to extract advertiser information."""
    text = get_request_text(sent_video_dict)
    return parse_text_for_adinfo(
        text=text,
        pub_store_id=sent_video_dict["pub_store_id"],
        run_id=sent_video_dict["run_id"],
        mitm_uuid=sent_video_dict["mitm_uuid"],
        pgdb=pgdb,
    )


def _lookup_and_insert_store_app(
    adv_store_id: str,
    pgdb: PostgresEngine,
) -> int | None:
    """Try to look up a store app on Play Store and insert it into the DB.

    Returns the DB id if successful, None otherwise.
    """
    try:
        import appgoblin_play_scraper

        result_dict = appgoblin_play_scraper.app(
            adv_store_id,
            lang="en",
            country="us",
            timeout=10,
        )
        good_id = result_dict.get("appId", None)
        if good_id is not None:
            from adscrawler.app_stores.utils import check_and_insert_new_apps

            check_and_insert_new_apps(
                dicts=[{"store": 1, "store_id": adv_store_id}],
                pgdb=pgdb,
                crawl_source="mitm_ads",
                store=1,
            )
            # Retry the query after insertion
            return query_store_app_by_store_id_cached(
                store_id=adv_store_id,
                pgdb=pgdb,
                case_insensitive=True,
            )
    except Exception as e:
        logger.error(f"Failed to lookup and insert {adv_store_id}: {e}")
    return None


def parse_sent_video_df(
    row: pd.Series,
    pub_store_id: str,
    sent_video_df: pd.DataFrame,
    pgdb: PostgresEngine,
    video_id: str,
) -> tuple[list[AdInfo], list[dict[str, Any]]]:
    """Parses video data to extract advertiser information from various ad networks."""
    error_messages = []
    run_id = row["run_id"]
    log_info = f"pub_store_id={pub_store_id} run_id={run_id} parse_sent video_id={video_id[0:10]}"
    sent_video_dicts = sent_video_df.to_dict(orient="records")
    found_ad_infos = []
    for sent_video_dict in sent_video_dicts:
        error_msg = None
        parsed_text = False
        init_url = sent_video_dict["url"]
        init_tld = sent_video_dict["tld_url"]
        logger.debug(f"{log_info} Parsing sent_video_dict {init_tld=}")
        if "vungle.com" == init_tld:
            ad_info, error_msg = parse_vungle_ad(sent_video_dict, pgdb)
        elif "bidmachine.io" == init_tld:
            ad_info = parse_bidmachine_ad(sent_video_dict, pgdb)
        elif (
            "fyber.com" == init_tld
            or "tpbid.com" == init_tld
            or "inner-active.mobi" == init_tld
        ):
            init_tld = "fyber.com"
            ad_info = parse_fyber_ad(sent_video_dict, pgdb)
        elif "everestop.io" == init_tld:
            ad_info = parse_everestop_ad(sent_video_dict)
        elif "doubleclick.net" == init_tld:
            ad_info, error_msg = parse_google_ad(sent_video_dict, video_id, pgdb)
        elif "unityads.unity3d.com" in init_url:
            ad_info, error_msg = parse_unity_ad(sent_video_dict, pgdb)
        elif "mtgglobals.com" == init_tld:
            ad_info = parse_mtg_ad(sent_video_dict, pgdb)
        elif "yandex.ru" == init_tld:
            ad_info = parse_yandex_ad(sent_video_dict, pgdb, video_id)
        elif "youappi.com" == init_tld:
            ad_info, error_msg = parse_youappi_ad(sent_video_dict, pgdb)
        else:
            ad_info, error_msg = parse_generic_adnetwork(sent_video_dict, pgdb)
            parsed_text = True
        if error_msg:
            row["error_msg"] = error_msg
            logger.error(f"{log_info} {error_msg} for video {video_id[0:10]}")
            error_messages.append(row)
            continue
        if ad_info["adv_store_id"] is None and not parsed_text:
            # This is doubling the time for the run as it is parsing the text again
            # but this does seem to could catch misses often enough to keep it
            ad_parts, _error_msg = parse_text_for_adinfo(
                text=sent_video_dict["response_text"],
                pub_store_id=sent_video_dict["pub_store_id"],
                run_id=run_id,
                mitm_uuid=sent_video_dict["mitm_uuid"],
                pgdb=pgdb,
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
                f"{log_info} Incorrect adv_store_id, identified pub ID as adv ID for video {video_id[0:10]}"
            )
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        if ad_info["adv_store_id"] is None:
            error_msg = "No adv_store_id found"
            logger.debug(f"{log_info} {error_msg}")
        try:
            if ad_info["adv_store_id"] is None:
                adv_db_id = None
            else:
                adv_db_id = query_store_app_by_store_id_cached(
                    store_id=ad_info["adv_store_id"],
                    pgdb=pgdb,
                    case_insensitive=True,
                )
            ad_info["adv_store_app_id"] = adv_db_id
        except Exception:
            adv_store_id = ad_info["adv_store_id"]
            adv_db_id = (
                _lookup_and_insert_store_app(adv_store_id, pgdb)
                if adv_store_id
                else None
            )
            if adv_db_id is not None:
                ad_info["adv_store_app_id"] = adv_db_id
            else:
                error_msg = (
                    f"found potential app but failed to get db id {adv_store_id}"
                )
                logger.error(f"{log_info} {error_msg}")
                row["error_msg"] = error_msg
                error_messages.append(row)
                continue
        ad_info["init_tld"] = init_tld
        found_ad_infos.append(ad_info)
    return found_ad_infos, error_messages
