import datetime
import pathlib
import shutil
import struct

import numpy as np
import pandas as pd
from mitmproxy import http
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


def get_creatives_df(
    pub_store_id: str, run_id: int, database_connection: PostgresCon
) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    """Retrieves and filters creative content from MITM log data."""
    df = parse_log(pub_store_id, run_id, database_connection)
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


def move_to_processed(store_id: str, run_id: int) -> None:
    flows_file = f"traffic_{store_id}.log"
    final_flows_file = f"{store_id}_{run_id}.log"
    mitmlog_file = pathlib.Path(MITM_DIR, flows_file)
    destination_path = pathlib.Path(MITM_DIR, final_flows_file)
    if not mitmlog_file.exists():
        logger.error(f"mitm log file not found at {mitmlog_file}")
        raise FileNotFoundError
    shutil.move(mitmlog_file, destination_path)


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
    pub_store_id: str,
    run_id: int | None,
    database_connection: PostgresCon,
    include_all: bool = False,
) -> pd.DataFrame:
    """Parses MITM proxy log files and extracts HTTP request/response data into a DataFrame."""
    if run_id is None:
        mitm_log_path = pathlib.Path(MITM_DIR, f"traffic_{pub_store_id}.log")
    else:
        mitm_log_path = pathlib.Path(MITM_DIR, f"{pub_store_id}_{run_id}.log")
    if mitm_log_path.exists():
        logger.info("mitm log already exists")
    else:
        key = f"mitm_logs/android/{pub_store_id}/{run_id}.log"
        mitms = get_store_id_mitm_s3_keys(store_id=pub_store_id)
        key = mitms[mitms["run_id"] == str(run_id)]["key"].to_numpy()[0]
        mitm_log_path = download_mitm_log_by_key(key, mitm_log_path)
    if not mitm_log_path.exists():
        logger.error(f"mitm log file not found at {mitm_log_path}")
        raise FileNotFoundError
    parsed_flows = []
    with open(mitm_log_path, "rb") as f:
        reader = FlowReader(f)
        for flow in reader.stream():
            if isinstance(flow, http.HTTPFlow):
                timestamp = datetime.datetime.fromtimestamp(flow.timestamp_start)
            else:
                # These should be inspected and added
                msg = f"Non HTTPFlow found in mitm log {mitm_log_path}"
                logger.exception(msg)
                raise ValueError(msg)
            try:
                url = flow.request.pretty_url
                tld_url = get_tld(url)
                response_size_bytes = (
                    len(flow.response.content or b"") if flow.response else 0
                )
                # Extract useful data from each flow
                flow_data = {
                    "mitm_uuid": flow.id,
                    "tld_url": tld_url,
                    "status_code": (
                        flow.response.get("status_code", -1) if flow.response else -1
                    ),
                    "host": flow.request.host,
                    "headers": dict(flow.request.headers),
                    "request_mime_type": flow.request.headers.get("Content-Type", ""),
                    "response_mime_type": (
                        flow.response.headers.get("Content-Type", None)
                        if flow.response
                        else None
                    ),
                    "response_size_bytes": response_size_bytes,
                    "url": url,
                    "called_at": timestamp,
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
                parsed_flows.append(flow_data)
            except Exception as e:
                logger.exception(f"Error parsing flow: {e}")
                continue
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
    df["pub_store_id"] = pub_store_id
    return df


def append_additional_mitm_data(
    flow: http.HTTPFlow,
    flow_data: dict,
    tld_url: str,
    url: str,
    database_connection: PostgresCon,
) -> dict:
    """Appends additional data from the mitm flow to the flow_data dictionary."""
    if "applovin.com" in tld_url:
        flow_data["request_text"] = decode_network_request(
            url,
            flowpart=flow.request,
            database_connection=database_connection,
        )
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
        flow_data["response_content_type"] = flow.response.headers.get(
            "Content-Type", ""
        )
        flow_data["response_size"] = flow.response.headers.get("Content-Length", "0")
        if "applovin.com" in tld_url:
            flow_data["response_text"] = decode_network_request(
                url,
                flowpart=flow.response,
                database_connection=database_connection,
            )
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


def filter_creatives(df: pd.DataFrame) -> pd.DataFrame:
    """Filters DataFrame to include only valid creative content based on size and format criteria."""
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
        lambda x: (
            struct.unpack(">II", x[16:24])[0]
            if x.startswith(b"\x89PNG\r\n\x1a\n")
            else None
        )
    )
    heights = creatives_df["response_content"].apply(
        lambda x: (
            struct.unpack(">II", x[16:24])[1]
            if x.startswith(b"\x89PNG\r\n\x1a\n")
            else None
        )
    )
    is_square = widths == heights
    is_png = creatives_df["file_extension"] == "png"
    is_googleusercontent = creatives_df["tld_url"] == "googleusercontent.com"
    creatives_df = creatives_df[~(is_png & is_googleusercontent & is_square)]
    return creatives_df


def add_file_extension(df: pd.DataFrame) -> pd.DataFrame:
    """Adds file extension column to DataFrame based on URL or content type."""
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


def add_is_creative_content_column(df: pd.DataFrame) -> pd.DataFrame:
    """Adds a column indicating whether the response contains creative content (images/videos)."""
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
