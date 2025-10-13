import datetime
import pathlib
import struct

import numpy as np
import pandas as pd
from mitmproxy import http, tcp
from mitmproxy.exceptions import FlowReadException
from mitmproxy.io import FlowReader

from adscrawler.config import MITM_DIR, get_logger
from adscrawler.dbcon.connection import PostgresCon
from adscrawler.dbcon.queries import query_countries
from adscrawler.mitm_ad_parser.utils import get_tld
from adscrawler.packages.storage import (
    download_mitm_log_by_key,
    get_store_id_mitm_s3_keys,
)
from adscrawler.tools.geo import get_geo

logger = get_logger(__name__, "mitm_scrape_ads")

try:
    from adscrawler.mitm_ad_parser.decrypt_applovin import decode_from
except ImportError:
    decode_from = None

ABS_TOL = 1024  # bytes
PCT_TOL = 0.03  # 3%

IGNORE_URLS = [
    "https://connectivitycheck.gstatic.com/generate_204",
    "https://infinitedata-pa.googleapis.com/mdi.InfiniteData/Lookup",
    "https://android.apis.google.com/c2dm/register3",
    "http://connectivitycheck.gstatic.com/generate_204",
    "https://www.google.com/generate_204",
    "https://ota.waydro.id/system/lineage/waydroid_x86_64/GAPPS.json",
]


def decode_network_request(
    url: str, flowpart: http.HTTPFlow, database_connection: PostgresCon
) -> str | None:
    """Decodes network request content, handling AppLovin-specific decoding when needed."""
    if decode_from and "applovin.com" in url:
        try:
            text = decode_from(
                blob=flowpart.content, database_connection=database_connection
            )
            if text is None:
                logger.warning(f"Decode {url[:40]=} failed")
        except Exception:
            text = None
    else:
        logger.error(f"Unknown Decode Network Request URL: {url}")
        text = None
    return text


def get_mitm_df(
    store_id: str, run_id: int, database_connection: PostgresCon
) -> tuple[pd.DataFrame, str | None]:
    """Retrieves and filters creative content from MITM log data."""
    df = parse_log(store_id, run_id, database_connection, include_all=True)
    error_msg = None
    if df.empty:
        error_msg = "No data in mitm df"
        logger.error(error_msg)
        return pd.DataFrame(), pd.DataFrame(), error_msg
    df = add_file_extension(df)
    df = add_is_creative_column(df)
    if df["is_creative"].sum() == 0:
        error_msg = "No creatives to check"
        logger.error(error_msg)
        return df, error_msg
    return df, error_msg


def make_ip_geo_snapshot_df(
    df: pd.DataFrame, database_connection: PostgresCon
) -> pd.DataFrame:
    """Appends geographical information based on IP addresses to the DataFrame.

    NOTE: This should only be done once when the flows are first parsed.

    IP to geo location changes over time (days/weeks), so subsequent calls to this function could have incorrect values.
    """
    country_map = query_countries(database_connection)
    df[["country_iso", "state_iso", "city_name", "org"]] = df["ip_address"].apply(
        lambda x: pd.Series(get_geo(x))
    )
    df = pd.merge(
        df,
        country_map[["id", "alpha2"]].rename(columns={"id": "country_id"}),
        how="left",
        left_on="country_iso",
        right_on="alpha2",
        validate="m:1",
    )
    return df


def parse_log(
    store_id: str,
    run_id: int | None,
    database_connection: PostgresCon,
    include_all: bool = False,
) -> pd.DataFrame:
    """Parses MITM proxy log files and extracts HTTP request/response data into a DataFrame."""
    if run_id is None:
        mitm_log_path = pathlib.Path(MITM_DIR, f"traffic_{store_id}.log")
    else:
        mitm_log_path = pathlib.Path(MITM_DIR, f"{store_id}_{run_id}.log")
    if not mitm_log_path.exists() and run_id is not None:
        mitms = get_store_id_mitm_s3_keys(store_id=store_id)
        mitms = mitms[mitms["run_id"] == str(run_id)]
        if mitms.empty:
            logger.error(f"No mitm log found for {store_id} {run_id}")
            return pd.DataFrame()
        key = mitms.iloc[0]["key"]
        mitm_log_path = download_mitm_log_by_key(key, mitm_log_path)
    if not mitm_log_path.exists():
        logger.error(f"mitm log file not found at {mitm_log_path}")
        raise FileNotFoundError
    parsed_flows = []
    with open(mitm_log_path, "rb") as f:
        reader = FlowReader(f)
        try:
            for flow in reader.stream():
                flow_data = process_flow(
                    flow,
                    include_all=include_all,
                    database_connection=database_connection,
                )
                parsed_flows.append(flow_data)
        except FlowReadException:
            logger.warning(f"FlowReadException, bad mitm file, {mitm_log_path}")
    no_logs_found_msg = "No HTTP requests found in mitm log"
    if not parsed_flows:
        logger.warning(no_logs_found_msg)
        return pd.DataFrame()
    df = pd.DataFrame(parsed_flows)
    if "url" in df.columns:
        df = df[~df["url"].isin(IGNORE_URLS)]
    if df.empty:
        logger.warning(no_logs_found_msg)
        return pd.DataFrame()
    if "response_text" in df.columns:
        df["response_text"] = df["response_text"].astype(str)
    df["status_code"] = df["status_code"].astype(int)
    df["run_id"] = (
        mitm_log_path.as_posix().split("/")[-1].split("_")[-1].replace(".log", "")
    )
    df["pub_store_id"] = store_id
    return df


def process_flow(
    flow: http.HTTPFlow | tcp.TCPFlow,
    include_all: bool,
    database_connection: PostgresCon,
) -> dict:
    if not isinstance(flow, (http.HTTPFlow, tcp.TCPFlow)):
        # These should be inspected and added
        msg = f"Non HTTPFlow found in mitm log {mitm_log_path}"
        logger.exception(msg)
        raise ValueError(msg)
    mitm_uuid = flow.id
    called_at = datetime.datetime.fromtimestamp(flow.timestamp_start)
    if isinstance(flow, http.HTTPFlow):
        url = flow.request.pretty_url
        if ":" == url[0]:
            # Sometimes pretty url formatting fails
            http_scheme = "http://"
            if flow.server_conn.tls:
                http_scheme = "https://"
            url = (
                http_scheme
                + flow.request.headers.get("Host", flow.request.host)
                + flow.request.path
            )
        tld_url = get_tld(url)
        if flow.response:
            try:
                # Try decoded content first (may fail if encoding is unknown)
                response_size_bytes = len(flow.response.content)
            except Exception:
                # Fall back to raw bytes if decoding fails
                response_size_bytes = len(flow.response.raw_content or b"")
        else:
            response_size_bytes = 0
        # Extract useful data from each flow
        host = flow.request.host
        if host is None or host == "" or host[:3] == "www":
            host = flow.server_conn.address[0]
        flow_data = {
            "mitm_uuid": mitm_uuid,
            "flow_type": "http",
            "tld_url": tld_url,
            "status_code": (flow.response.status_code if flow.response else -1),
            "ip_address": host,
            "request_mime_type": flow.request.headers.get("Content-Type", ""),
            "response_mime_type": (
                flow.response.headers.get("Content-Type", None)
                if flow.response
                else None
            ),
            "response_size_bytes": response_size_bytes,
            "url": url,
            "called_at": called_at,
            # "method": flow.request.method,
        }
        if include_all:
            flow_data = append_additional_mitm_data(
                flow=flow,
                flow_data=flow_data,
                tld_url=tld_url,
                url=url,
                database_connection=database_connection,
            )
    elif isinstance(flow, tcp.TCPFlow):
        flow_data = {
            "mitm_uuid": mitm_uuid,
            "flow_type": "tcp",
            "tld_url": None,
            "status_code": -1,
            "ip_address": flow.server_conn.address[0],
            "request_mime_type": None,
            "response_mime_type": None,
            "response_size_bytes": sum(len(m.content) for m in flow.messages),
            "url": None,
            "called_at": called_at,
        }
    else:
        # These should be inspected and added
        msg = f"Non HTTPFlow found in mitm log {mitm_log_path}"
        logger.exception(msg)
        raise ValueError(msg)
    return flow_data


def append_additional_mitm_data(
    flow: http.HTTPFlow,
    flow_data: dict,
    tld_url: str,
    url: str,
    database_connection: PostgresCon,
) -> dict:
    """Appends additional data from the mitm flow to the flow_data dictionary."""
    if tld_url and "applovin.com" in tld_url:
        decoded_text = decode_network_request(
            url,
            flowpart=flow.request,
            database_connection=database_connection,
        )
        if decoded_text is not None:
            flow_data["request_text"] = decoded_text
        else:
            try:
                flow_data["request_text"] = flow.request.get_text()
            except Exception:
                flow_data["request_text"] = ""
    else:
        try:
            flow_data["request_text"] = flow.request.get_text()
        except Exception:
            flow_data["request_text"] = ""
    try:
        flow_data["content"] = flow.request.content
    except Exception:
        flow_data["content"] = ""
    # Add response info if available
    flow_data["query_params"] = (dict(flow.request.query),)
    flow_data["post_params"] = (flow.request.urlencoded_form,)
    if flow.response:
        try:
            flow_data["status_code"] = flow.response.status_code
        except Exception:
            flow_data["status_code"] = -1
        try:
            flow_data["response_headers"] = dict(flow.response.headers)
        except Exception:
            flow_data["response_headers"] = {}
        if tld_url and "applovin.com" in tld_url:
            decoded_text = decode_network_request(
                url,
                flowpart=flow.response,
                database_connection=database_connection,
            )
            if decoded_text is not None:
                flow_data["response_text"] = decoded_text
            else:
                try:
                    flow_data["response_text"] = flow.response.get_text()
                except Exception:
                    flow_data["response_text"] = ""
        else:
            try:
                flow_data["response_text"] = flow.response.get_text()
            except Exception:
                flow_data["response_text"] = ""
        try:
            flow_data["response_content"] = flow.response.content
        except Exception:
            flow_data["response_content"] = b""
            pass
    return flow_data


def add_file_extension(df: pd.DataFrame) -> pd.DataFrame:
    """Adds file extension column to DataFrame based on URL or content type."""

    ext_too_long = (
        df["url"]
        .apply(lambda x: x.split(".")[-1] if x else "NoExtensionFound")
        .str.len()
        > 4
    )
    has_mimetype = df["response_mime_type"].fillna("").str.contains("/")

    def get_subtype(x):
        parts = x.split("/")
        return parts[1] if len(parts) > 1 else None

    df["file_extension"] = np.where(
        ext_too_long | ~has_mimetype,
        df["response_mime_type"].fillna("").apply(get_subtype),
        "",
    )
    return df


def add_is_creative_column(df: pd.DataFrame) -> pd.DataFrame:
    """Adds a column indicating whether the response contains creative content (images/videos)."""
    creative_types = (
        r"\b(?:image|video)/(?:jpeg|jpg|png|gif|webp|webm|mp4|mpeg|avi|quicktime)\b"
    )
    is_creative_content_response = (
        df["response_mime_type"]
        .fillna("")
        .str.contains(
            creative_types,
            case=False,
            regex=True,
        )
    )
    is_creative_content_request = (
        df["response_mime_type"]
        .fillna("")
        .str.contains(
            creative_types,
            case=False,
            regex=True,
        )
    )
    is_creative_content = is_creative_content_response | is_creative_content_request
    df["is_creative_content"] = is_creative_content
    status_code_200 = df["status_code"] == 200
    is_large_enough = df["response_size_bytes"] > 50000
    response_content_ok = df["response_content"].notna()
    df["is_creative_content"] = (
        df["is_creative_content"]
        & status_code_200
        & is_large_enough
        & response_content_ok
    )
    # Lots of creatives of the publishing app icon
    # If this cuts out the advertisers icon as well that seems OK?
    is_square = df.loc[df["is_creative_content"], "response_content"].apply(
        lambda x: (
            struct.unpack(">II", x[16:24])[0]
            if x.startswith(b"\x89PNG\r\n\x1a\n")
            else None
        )
    ) == df.loc[df["is_creative_content"], "response_content"].apply(
        lambda x: (
            struct.unpack(">II", x[16:24])[1]
            if x.startswith(b"\x89PNG\r\n\x1a\n")
            else None
        )
    )
    is_png = df["file_extension"] == "png"
    is_googleusercontent = df["tld_url"] == "googleusercontent.com"
    df["is_creative"] = np.where(
        df["is_creative_content"] & ~(is_png & is_googleusercontent & is_square),
        True,
        False,
    )
    return df
