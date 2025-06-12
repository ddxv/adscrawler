import dataclasses
import datetime
import hashlib
import html
import json
import pathlib
import re
import urllib

import pandas as pd
import tldextract
from bs4 import BeautifulSoup
from mitmproxy import http
from mitmproxy.io import FlowReader

from adscrawler.config import CREATIVES_DIR, MITM_DIR, get_logger
from adscrawler.dbcon.connection import PostgresCon
from adscrawler.dbcon.queries import (
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

# import av
# import io


logger = get_logger(__name__, "mitm_scrape_ads")


IGNORE_IDS = ["privacy"]

# def check_mp4_with_pyav(content):
#     # Is this really working?
#     try:
#         container = av.open(io.BytesIO(content))
#         video_stream = next(s for s in container.streams if s.type == "video")
#         for packet in container.demux(video_stream):
#             for frame in packet.decode():
#                 pass
#         return True
#     except av.AVError as e:
#         print(f"AV error: {e}")
#         return False


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
                            request_data["status_code"] = flow.response.status_code
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
        logger.warning("Multiple responses for video found, selecting 1")
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
              'appsflyer_url'. Returns None for a URL if it's not found.
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
    appsflyer_url = None
    market_intent_url = None
    intent_url = None
    adv_store_id = None
    ad_network_url = None
    ad_network_tld = None
    ad_network_urls = query_ad_domains(database_connection=database_connection)
    for url in urls:
        tld_url = tldextract.extract(url).domain + "." + tldextract.extract(url).suffix
        if tld_url in ad_network_urls["domain"].to_list():
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
        elif "play.google.com" in url:
            google_play_url = url
            parsed_gplay = urllib.parse.urlparse(google_play_url)
            adv_store_id = urllib.parse.parse_qs(parsed_gplay.query)["id"][0]
        elif "app.appsflyer.com" in url:
            appsflyer_url = url
        else:
            logger.debug(f"Found unknown URL: {url}")
        # Stop if we have found both URLs
        if google_play_url and appsflyer_url:
            break
    return {
        "google_play_url": google_play_url,
        "appsflyer_url": appsflyer_url,
        "google_tracking_url": google_tracking_url,
        "market_intent_url": market_intent_url,
        "intent_url": intent_url,
        "adv_store_id": adv_store_id,
        "ad_network_url": ad_network_url,
        "ad_network_tld": ad_network_tld,
    }


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


def parse_google_ad(
    sent_video_dict: dict, video_id: str, database_connection: PostgresCon
) -> AdInfo:
    google_response = json.loads(sent_video_dict["response_text"])
    g_ad_network_name = google_response["ad_networks"][0]["ad_source_name"]
    ad_html = google_response["ad_networks"][0]["ad"]["ad_html"]
    with open(f"{g_ad_network_name}_{video_id}.html", "w") as f:
        f.write(ad_html)
    urls = google_extract_ad_urls(ad_html, database_connection)
    try:
        adv_store_id = urls["adv_store_id"]
        ad_network_tld = urls["ad_network_tld"]
    except Exception:
        logger.error("No adv_store_id found")
        adv_store_id = None
    if "appsflyer_url" in urls:
        mmp_url = urls["appsflyer_url"]
        # parsed_url = urllib.parse.urlparse(mmp_url)
        # query_params = urllib.parse.parse_qs(parsed_url.query)
        # campaign_name = query_params["c"][0]
        # af_siteid = query_params["af_siteid"][0]
        # af_network_id = query_params["pid"][0]
    else:
        logger.error("Did not find MMP!")
        mmp_url = None
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
) -> pd.DataFrame:
    status_code_200 = df["status_code"] == 200
    is_creative_content = df["response_content_type"].str.contains(
        "image|video|webm|mp4|jpeg|jpg|png|gif|webp", regex=True
    )
    creatives_df = df[is_creative_content & status_code_200].copy()
    creatives_df["creative_size"] = creatives_df["response_content"].apply(
        lambda x: len(x)
    )
    creatives_df = creatives_df[creatives_df["creative_size"] > 10000]
    creatives_df["file_extension"] = creatives_df["url"].apply(
        lambda x: x.split(".")[-1]
    )
    i = 0
    adv_creatives = []
    row_count = creatives_df.shape[0]
    for _i, row in creatives_df.iterrows():
        i += 1
        video_id = row["url"].split("/")[-1].split(".")[0]
        if row["tld_url"] == "unity3dusercontent.com":
            # this isn't the video name, but the id of the content as the name is the quality
            video_id = row["url"].split("/")[-2]
        file_extension = row["file_extension"]
        if video_id in IGNORE_IDS:
            logger.info(f"Ignoring video {video_id}.{file_extension}")
            continue
        sent_video_dict = get_first_sent_video_df(creatives_df, row, video_id)
        if sent_video_dict is None:
            logger.error(f"No video source found for {row['tld_url']} {video_id}")
            continue
        if "doubleclick.net" in sent_video_dict["tld_url"]:
            ad_info = parse_google_ad(sent_video_dict, video_id, database_connection)
        elif "unityads.unity3d.com" in sent_video_dict["tld_url"]:
            ad_info = parse_unity_ad(sent_video_dict)
        else:
            logger.error(
                f"Not a recognized ad network: {sent_video_dict['tld_url']} for video {video_id[0:10]}"
            )
            continue
        if ad_info.adv_store_id is None:
            ad_info.adv_store_id = "unknown"
        if ad_info.adv_store_id == pub_store_id:
            logger.error(
                f"Incorrect adv_store_id, identified pub ID as adv ID for video {video_id[0:10]}"
            )
            continue
        local_path = pathlib.Path(CREATIVES_DIR, ad_info.adv_store_id)
        local_path.mkdir(parents=True, exist_ok=True)
        md5_hash = hashlib.md5(row["response_content"]).hexdigest()
        local_path = local_path / f"{md5_hash}.{file_extension}"
        # if file_extension == "mp4":
        #     if not check_mp4_with_pyav(row["response_content"]):
        #         logger.error(f"Invalid MP4 file: {local_path}")
        #         continue
        with open(local_path, "wb") as creative_file:
            creative_file.write(row["response_content"])
        if ad_info.adv_store_id == "unknown":
            logger.error(f"Unknown adv_store_id for {row['tld_url']} {video_id}")
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
        logger.info(
            f"{i}/{row_count}: {ad_info.ad_network_tld} adv={ad_info.adv_store_id} init_tld={init_tld}"
        )
    adv_creatives_df = pd.DataFrame(adv_creatives)
    if adv_creatives_df.empty:
        logger.error(f"No creatives found for {pub_store_id}")
        return pd.DataFrame()
    return adv_creatives_df


def upload_creatives_to_s3(adv_creatives_df: pd.DataFrame) -> None:
    for adv_id, adv_creatives_df_adv in adv_creatives_df.groupby("adv_store_id"):
        s3_keys = get_app_creatives_s3_keys(store=1, store_id=adv_id)
        for _i, row in adv_creatives_df_adv.iterrows():
            if row["md5_hash"] in s3_keys["md5_hash"].to_numpy():
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
    logger.info(f"Using {mitm_log_path} for {store_id}")
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
) -> None:
    pub_db_id = query_store_app_by_store_id(
        store_id=pub_store_id, database_connection=database_connection
    )
    df = parse_mitm_log(mitm_log_path)
    adv_creatives_df = get_creatives(df, pub_store_id, database_connection)
    if adv_creatives_df.empty:
        logger.error(f"No creatives found for {pub_store_id}")
        return
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


def parse_all_runs_for_store_id(
    pub_store_id: str, database_connection: PostgresCon
) -> None:
    # mitm_log_path, run_id = get_latest_local_mitm(pub_store_id)
    mitms = get_store_id_mitm_s3_keys(
        store_id=pub_store_id, database_connection=database_connection
    )
    for _i, mitm in mitms.iterrows():
        key = mitm["key"].to_numpy()[0]
        run_id = mitm["run_id"].to_numpy()[0]
        filename = f"{pub_store_id}_{run_id}.log"
        mitm_log_path = download_mitm_log_by_key(key, filename)
        parse_store_id_mitm_log(
            pub_store_id, run_id, mitm_log_path, database_connection
        )


def scan_all_apps(database_connection: PostgresCon) -> None:
    apps_to_scan = query_apps_to_creative_scan(database_connection=database_connection)
    for _i, app in apps_to_scan.iterrows():
        pub_store_id = app["store_id"]
        parse_all_runs_for_store_id(pub_store_id, database_connection)
