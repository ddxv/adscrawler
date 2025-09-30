import datetime
import pathlib
import struct

import numpy as np
import pandas as pd
from mitmproxy import http
from mitmproxy.io import FlowReader

from adscrawler.config import MITM_DIR, get_logger
from adscrawler.dbcon.connection import PostgresCon
from adscrawler.mitm_ad_parser.utils import get_tld
from adscrawler.packages.storage import (
    download_mitm_log_by_key,
    get_store_id_mitm_s3_keys,
)

logger = get_logger(__name__, "mitm_scrape_ads")

try:
    from adscrawler.mitm_ad_parser.decrypt_applovin import decode_from
except ImportError:
    decode_from = None

ABS_TOL = 1024  # bytes
PCT_TOL = 0.03  # 3%


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


def parse_mitm_log(
    pub_store_id: str,
    run_id: int,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    """Parses MITM proxy log files and extracts HTTP request/response data into a DataFrame."""
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
                                request_data["response_content"] = b""
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
