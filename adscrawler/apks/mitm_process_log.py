import datetime
import pathlib

import pandas as pd
import tldextract
from mitmproxy import http
from mitmproxy.io import FlowReader

from adscrawler.config import PACKAGE_DIR, get_logger

logger = get_logger(__name__)


def parse_mitm_log(store_id):
    # Define the log file path
    mitmlog_dir = pathlib.Path(PACKAGE_DIR, "mitmlogs")
    flows_file = f"traffic_{store_id}.log"
    mitmlog_file = pathlib.Path(mitmlog_dir, flows_file)

    if not mitmlog_file.exists():
        logger.error(f"mitm log file not found at {mitmlog_file}")
        raise FileNotFoundError

    # Parse the flows
    logger.info(f"Reading MITM log file for {store_id}...")
    requests = []

    try:
        with open(mitmlog_file, "rb") as f:
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
                            "method": flow.request.method,
                            "url": flow.request.pretty_url,
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

                        # Try to get text content if available
                        try:
                            request_data["content"] = flow.request.get_text()
                        except Exception:
                            # If text extraction fails, store raw content length
                            request_data["content"] = (
                                f"[Binary content: {len(flow.request.content)} bytes]"
                            )

                        # Add response info if available
                        if flow.response:
                            request_data["status_code"] = flow.response.status_code
                            request_data["response_headers"] = dict(
                                flow.response.headers
                            )
                            try:
                                # Limit response content to prevent huge files
                                response_text = flow.response.get_text()
                                request_data["response"] = (
                                    response_text[:1000] + "..."
                                    if len(response_text) > 1000
                                    else response_text
                                )
                            except Exception:
                                request_data["response"] = (
                                    f"[Binary response: {len(flow.response.content)} bytes]"
                                )

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

    # Add TLD extraction
    df["tld_url"] = df["url"].apply(
        lambda x: ".".join([tldextract.extract(x).domain, tldextract.extract(x).suffix])
    )

    return df
