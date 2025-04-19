import socket

import pandas as pd
import tldextract
from mitmproxy import http

# Store request data here
requests = []


def request(flow: http.HTTPFlow) -> None:
    try:
        # Extract IP from host
        host = flow.request.host
        ip = socket.gethostbyname(host)

        # Store relevant data
        req_data = {
            "method": flow.request.method,
            "url": flow.request.pretty_url,
            "ip": ip,
            "host": host,
            "path": flow.request.path,
            "headers": dict(flow.request.headers),
            "content": flow.request.get_text(),
        }

        requests.append(req_data)
    except Exception as e:
        print(f"Error processing request: {e}")


def done():
    # Called when mitmproxy shuts down
    df = pd.DataFrame(requests)
    df.to_csv("captured_requests.csv", index=False)
    print(f"Saved {len(df)} requests to captured_requests.csv")


from mitmproxy import http
from mitmproxy.io import FlowReader

store_id = "com.water.balls"
flows_file = f"/home/james/adscrawler/mitmlogs/traffic_{store_id}.log"
requests = []

with open(flows_file, "rb") as f:
    reader = FlowReader(f)
    for flow in reader.stream():
        if isinstance(flow, http.HTTPFlow):
            requests.append(
                {
                    "method": flow.request.method,
                    "url": flow.request.pretty_url,
                    "host": flow.request.host,
                    "path": flow.request.path,
                    "headers": dict(flow.request.headers),
                    "content": flow.request.get_text(),
                    "ip": flow.server_conn.ip_address[0]
                    if flow.server_conn.ip_address
                    else None,
                }
            )


df = pd.DataFrame(requests)
df.to_csv("parsed_requests.csv", index=False)
print(f"Parsed {len(df)} requests.")


df[["tld_url", "ip"]]


df["tld_url"] = df["url"].apply(
    lambda x: ".".join(
        [tldextract.extract(x).domain, tldextract.extract(x).suffix],
    ),
)


df["tld_url"].value_counts()


#!/usr/bin/env python3

import datetime
import os
import sys

import pandas as pd
from mitmproxy import http
from mitmproxy.io import FlowReader


def parse_mitm_log(store_id):
    # Define the log file path
    flows_file = f"/home/james/adscrawler/mitmlogs/traffic_{store_id}.log"

    # Check if the file exists
    if not os.path.exists(flows_file):
        print(f"Error: Log file not found at {flows_file}")
        sys.exit(1)

    # Parse the flows
    print(f"Reading MITM log file for {store_id}...")
    requests = []

    try:
        with open(flows_file, "rb") as f:
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
                        print(f"Error parsing flow: {e}")
                        continue
    except Exception as e:
        print(f"Error reading log file: {e}")
        sys.exit(1)

    # Convert to DataFrame
    if not requests:
        print("No HTTP requests found in the log file")
        sys.exit(0)

    df = pd.DataFrame(requests)

    # Add TLD extraction
    df["tld_url"] = df["url"].apply(
        lambda x: ".".join([tldextract.extract(x).domain, tldextract.extract(x).suffix])
    )

    return df


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python parse_mitm_log.py <store_id>")
        sys.exit(1)

    store_id = sys.argv[1]
    parse_mitm_log(store_id)
