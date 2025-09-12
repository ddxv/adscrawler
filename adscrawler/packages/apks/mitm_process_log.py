import datetime
import pathlib
import shutil

import pandas as pd
import tldextract
from mitmproxy import http
from mitmproxy.io import FlowReader

from adscrawler.config import MITM_DIR, get_logger
from adscrawler.dbcon.connection import PostgresCon
from adscrawler.dbcon.queries import query_countries
from adscrawler.tools.geo import get_geo

logger = get_logger(__name__)


def move_mitm_to_processed(store_id: str, run_id: int) -> None:
    flows_file = f"traffic_{store_id}.log"
    final_flows_file = f"{store_id}_{run_id}.log"
    mitmlog_file = pathlib.Path(MITM_DIR, flows_file)
    destination_path = pathlib.Path(MITM_DIR, final_flows_file)
    if not mitmlog_file.exists():
        logger.error(f"mitm log file not found at {mitmlog_file}")
        raise FileNotFoundError
    shutil.move(mitmlog_file, destination_path)


def parse_mitm_short_log(
    store_id: str,
    database_connection: PostgresCon,
    mitmlog_file: pathlib.Path | None = None,
) -> pd.DataFrame:
    # If the mitmlog_file is not provided, use the default file name from Waydroid
    if mitmlog_file is None:
        flows_file = f"traffic_{store_id}.log"
        mitmlog_file = pathlib.Path(MITM_DIR, flows_file)

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
                            # request_data["content"] = (
                            #     f"[Binary content: {len(flow.request.content)} bytes]"
                            # )
                            pass

                        # Add response info if available
                        if flow.response:
                            request_data["status_code"] = flow.response.status_code
                            request_data["response_headers"] = dict(
                                flow.response.headers
                            )
                            request_data["response"] = flow.response
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

    if "url" in df.columns:
        df = df[~df["url"].isin(IGNORE_URLS)]

    if df.empty:
        logger.error("No HTTP requests found in the log file")
        return pd.DataFrame()

    # Add TLD extraction
    df["tld_url"] = df["url"].apply(
        lambda x: ".".join([tldextract.extract(x).domain, tldextract.extract(x).suffix])
    )

    df.loc[df["status_code"].isna(), "status_code"] = -1
    df["status_code"] = df["status_code"].astype(int)

    country_map = query_countries(database_connection)

    df[["country_iso", "state_iso", "city_name", "org"]] = df["host"].apply(
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


IGNORE_URLS = [
    "https://connectivitycheck.gstatic.com/generate_204",
    "https://infinitedata-pa.googleapis.com/mdi.InfiniteData/Lookup",
    "https://android.apis.google.com/c2dm/register3",
    "http://connectivitycheck.gstatic.com/generate_204",
    "https://www.google.com/generate_204",
    "https://ota.waydro.id/system/lineage/waydroid_x86_64/GAPPS.json",
]
