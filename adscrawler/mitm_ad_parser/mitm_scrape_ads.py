import datetime
import subprocess
import urllib
from concurrent.futures import ProcessPoolExecutor
from typing import Any

import numpy as np
import pandas as pd

from adscrawler.config import CREATIVE_THUMBS_DIR, get_logger
from adscrawler.dbcon.connection import PostgresCon, get_db_connection
from adscrawler.dbcon.queries import (
    log_creative_scan_results,
    query_ad_domains,
    query_api_calls_for_mitm_uuids,
    query_api_calls_to_creative_scan,
    query_creative_records,
    upsert_df,
)
from adscrawler.mitm_ad_parser.creative_processor import (
    get_phash,
    store_creative_and_thumb_to_local,
)
from adscrawler.mitm_ad_parser.mitm_logs import get_mitm_df
from adscrawler.mitm_ad_parser.network_parsers import (
    parse_sent_video_df,
    parse_creative_request,
)
from adscrawler.packages.storage import (
    creative_exists_in_s3,
    get_store_id_mitm_s3_keys,
    upload_ad_creative_to_s3,
)

logger = get_logger(__name__, "mitm_scrape_ads")

# Global variable to store the worker's database connection
_worker_db_connection = None

IGNORE_CREATIVE_IDS = ["privacy", "google_play_icon_grey_2022", "favicon"]


def find_sent_video_df(
    df: pd.DataFrame, row: pd.Series, video_id: str
) -> pd.DataFrame | None:
    """Retrieves DataFrame rows containing the specified video ID from the given DataFrame."""
    sent_video_df = df[
        (df["response_text"].astype(str).str.contains(video_id, regex=False))
        & (df["called_at"] <= row.called_at)
    ].copy()
    if sent_video_df.empty:
        sent_video_df = df[
            (df["response_text"].astype(str).str.contains(video_id, regex=False))
        ].copy()
    if sent_video_df.empty:
        return None
    if sent_video_df.shape[0] > 1:
        logger.debug(f"Multiple responses for {video_id=}")
    return sent_video_df


def get_video_id(row: pd.Series) -> str:
    """Extracts video ID from URL based on the ad network domain."""
    if "2mdn" in row["tld_url"]:
        if "/id/" in row["url"]:
            url_parts = urllib.parse.urlparse(row["url"])
            video_id = url_parts.path.split("/id/")[1].split("/")[0]
        elif "simgad" in row["url"]:
            video_id = row["url"].split("/")[-1]
        else:
            url_parts = urllib.parse.urlparse(row["url"])
            video_id = url_parts.path.split("/")[-1]
    elif "googlevideo" in row["tld_url"]:
        url_parts = urllib.parse.urlparse(row["url"])
        query_params = urllib.parse.parse_qs(url_parts.query)
        video_id = query_params["ei"][0]
    elif row["tld_url"] == "unity3dusercontent.com":
        # this isn't the video name, but the id of the content as the name is the quality
        video_id = row["url"].split("/")[-2]
    elif row["tld_url"] == "adcolony.com":
        video_id = row["url"].split("/")[-2]
        if len(video_id) < 10:
            video_id = row["url"].split("/")[-1]
    elif "bigabidserv.com" in row["tld_url"]:
        video_id = row["url"].split("/")[-1]
        if "." in video_id:
            video_id = video_id.split(".")[0]
    elif "yandex.net" in row["tld_url"]:
        # /id123/orig
        video_id = row["url"].split("/")[-2]
    else:
        url_parts = urllib.parse.urlparse(row["url"])
        video_id = url_parts.path.split("/")[-1]
    return video_id


def attribute_creatives(
    df: pd.DataFrame,
    pub_store_id: str,
    database_connection: PostgresCon,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Attributes creative content to advertisers by analyzing ad network responses."""
    error_messages = []
    adv_creatives = []
    creatives = df[df["is_creative"]].copy()
    creatives["video_id"] = creatives.apply(lambda x: get_video_id(x), axis=1)
    creatives = creatives.drop_duplicates(subset=["video_id", "response_size_bytes"])
    row_count = creatives.shape[0]
    # For duplicate video_id
    sent_video_cache = {}
    parse_results_cache = {}
    i = 0
    for _i, row in creatives.iterrows():
        i += 1
        if not row["tld_url"]:
            error_msg = "Host tld_url is empty"
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        host_ad_network_tld = row["tld_url"]
        video_id = row["video_id"]
        if len(video_id) < 4:
            error_msg = "Bad creative parsing video ID is too short"
            logger.info(f"{error_msg} for {row['tld_url']} {video_id=}")
            row["error_msg"] = error_msg
            error_messages.append(row)
        file_extension = row["file_extension"]
        if video_id in IGNORE_CREATIVE_IDS:
            logger.info(f"Ignoring video {video_id}.{file_extension}")
            continue
        if video_id in sent_video_cache:
            sent_video_df = sent_video_cache[video_id]
            found_ad_infos, found_error_messages = parse_results_cache[video_id]
            logger.debug(f"Cache hit for {video_id}")
        else:
            if len(video_id) < 5:
                # Too short video ids cause false positives and slow downs
                sent_video_df = None
            else:
                logger.info(
                    f"Processing {i}/{row_count} {host_ad_network_tld} {video_id=}"
                )
                sent_video_df = find_sent_video_df(df, row, video_id)
            if sent_video_df is None or sent_video_df.empty:
                error_msg = (
                    f"No requests found as source for {row['tld_url']} {video_id=}"
                )
                row["error_msg"] = error_msg
                error_messages.append(row)
                # Send the row itself to check for advertiser there
                ad_info, error_msg = parse_creative_request(row, database_connection)
                found_ad_infos = [ad_info]
                if error_msg:
                    row_copy = row.copy()
                    row_copy["error_msg"] = error_msg
                    found_error_messages = [row_copy]
                else:
                    found_error_messages = []
            else:
                sent_video_df["pub_store_id"] = pub_store_id
                found_ad_infos, found_error_messages = parse_sent_video_df(
                    row, pub_store_id, sent_video_df, database_connection, video_id
                )
            sent_video_cache[video_id] = sent_video_df
            parse_results_cache[video_id] = (found_ad_infos, found_error_messages)
        for found_error_message in found_error_messages:
            row_copy = row.copy()
            row_copy["error_msg"] = found_error_message["error_msg"]
            error_messages.append(row_copy)
        found_ad_infos = [
            x
            for x in found_ad_infos
            if x["adv_store_id"] is not None
            or (x["init_tld"] is not None and x["init_tld"] != host_ad_network_tld)
        ]
        found_advs = list(set([x["adv_store_id"] for x in found_ad_infos]))
        mmp_tlds = [
            x["mmp_tld"] for x in found_ad_infos if x["found_mmp_urls"] is not None
        ]
        mmp_tlds = list(set([x for x in mmp_tlds if x is not None]))
        mmp_tld = None
        if len(mmp_tlds) > 0:
            mmp_tld = mmp_tlds[0]
            if len(mmp_tlds) > 1:
                error_msg = "Multiple mmp_urls"
                logger.error(f"{error_msg} for {row['tld_url']} {video_id}")
                row["error_msg"] = error_msg
                error_messages.append(row)
        found_advs = list(set(found_advs))
        if len(found_advs) > 1:
            error_msg = "Found potential app! Multiple adv_store_id"
            logger.error(f"{error_msg} for {row['tld_url']} {video_id}")
            row["error_msg"] = error_msg
            error_messages.append(row)
            adv_store_id = None
            adv_store_app_id = None
        elif len(found_advs) == 0:
            error_msg = f"No adv_store_id found for {row['tld_url']} {video_id=}"
            row["error_msg"] = error_msg
            error_messages.append(row)
            adv_store_id = None
            adv_store_app_id = None
        else:
            adv_store_id = found_advs[0]
            adv_store_app_ids = [x["adv_store_app_id"] for x in found_ad_infos]
            adv_store_app_id = adv_store_app_ids[0]
        found_mmp_urls = [
            x["found_mmp_urls"]
            for x in found_ad_infos
            if x["found_mmp_urls"] is not None
        ]
        found_mmp_urls = list(
            set([item for sublist in found_mmp_urls for item in sublist])
        )
        found_ad_network_tlds = [
            x["found_ad_network_tlds"]
            for x in found_ad_infos
            if x["found_ad_network_tlds"] is not None
        ]
        found_ad_network_tlds = list(
            set([item for sublist in found_ad_network_tlds for item in sublist])
        )
        try:
            md5_hash = store_creative_and_thumb_to_local(
                row,
                file_extension=file_extension,
            )
        except Exception:
            error_msg = "Found potential creative but failed to store!"
            logger.error(error_msg)
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        try:
            phash = get_phash(
                md5_hash=md5_hash,
                file_extension=file_extension,
                database_connection=database_connection,
            )
        except Exception:
            error_msg = "Found potential creative but failed to compute phash"
            logger.error(f"{error_msg} for {row['tld_url']} {video_id}")
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        if phash is None:
            error_msg = "Found potential creative but failed to compute phash"
            logger.error(f"{error_msg} for {row['tld_url']} {video_id}")
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        init_tlds = [x["init_tld"] for x in found_ad_infos]
        if len(init_tlds) == 0:
            init_tld = None
            error_msg = "No initial domain found"
            row["error_msg"] = error_msg
            error_messages.append(row)
        else:
            init_tld = init_tlds[0]
        adv_creatives.append(
            {
                "mitm_uuid": row["mitm_uuid"],
                "pub_store_id": pub_store_id,
                "md5_hash": md5_hash,
                "host_ad_network_tld": host_ad_network_tld,
                "creative_initial_domain_tld": init_tld,
                "adv_store_id": adv_store_id,
                "advertiser_store_app_id": adv_store_app_id,
                "mmp_urls": found_mmp_urls,
                "found_ad_network_tlds": found_ad_network_tlds,
                "mmp_tld": mmp_tld,
                "phash": phash,
                "file_extension": file_extension,
            }
        )
        logger.debug(
            f"{i}/{row_count}: {host_ad_network_tld} adv={adv_store_id} init_tld={init_tld}"
        )
    adv_creatives_df = pd.DataFrame(adv_creatives)
    if adv_creatives_df.empty:
        msg = (
            f"No matched for creatives {pub_store_id}, {len(error_messages)} unmatched"
        )
        logger.warning(msg)
    return adv_creatives_df, error_messages


def upload_creatives_to_s3(adv_creatives_df: pd.DataFrame) -> None:
    """Uploads creative files to S3, checking for existing files to avoid duplicates."""
    for _i, row in adv_creatives_df.iterrows():
        md5_hash = row["md5_hash"]
        extension = row["file_extension"]
        if creative_exists_in_s3(md5_hash, extension):
            logger.debug(f"Creative {row['md5_hash']} already in S3")
            continue
        upload_ad_creative_to_s3(
            md5_hash=row["md5_hash"],
            extension=row["file_extension"],
        )
        logger.info(f"Uploaded {row['md5_hash']} to S3")


def append_missing_domains(
    ad_domains_df: pd.DataFrame,
    creative_records_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    """Adds missing ad domains to the database and returns updated domain DataFrame."""
    check_cols = ["creative_initial_domain_tld", "host_ad_network_tld"]
    for col in check_cols:
        missing_ad_domains = creative_records_df[
            (~creative_records_df[col].isin(ad_domains_df["domain_name"]))
            & (creative_records_df[col].notna())
        ]
        if not missing_ad_domains.empty:
            new_ad_domains = (
                missing_ad_domains[[col]]
                .drop_duplicates()
                .rename(columns={col: "domain_name"})
            )
            new_ad_domains = upsert_df(
                table_name="domains",
                df=new_ad_domains,
                insert_columns=["domain_name"],
                key_columns=["domain_name"],
                database_connection=database_connection,
                return_rows=True,
            )
            ad_domains_df = pd.concat(
                [
                    new_ad_domains[["id", "domain_name"]].rename(
                        columns={"id": "domain_id"}
                    ),
                    ad_domains_df,
                ]
            )
    return ad_domains_df


def add_additional_domain_id_column(
    creative_records_df: pd.DataFrame, ad_domains_df: pd.DataFrame
) -> pd.DataFrame:
    """Adds additional ad domain IDs column by exploding and merging domain lists."""
    cr = creative_records_df.copy()
    # Ensure missing values in the list column become empty lists
    cr["found_ad_network_tlds"] = cr["found_ad_network_tlds"].apply(
        lambda x: x if isinstance(x, list) else []
    )
    # Explode and keep the original row index in a column
    exploded = (
        cr.explode("found_ad_network_tlds")
        .reset_index()
        .rename(columns={"index": "orig_idx"})
    )
    # Merge on the exploded domain value
    merged = exploded.merge(
        ad_domains_df[["domain_name", "domain_id"]],
        left_on="found_ad_network_tlds",
        right_on="domain_name",
        how="left",
    )
    # Group back the matching ids by the original row index
    grouped = merged.groupby("orig_idx")["domain_id"].apply(
        lambda ids: [int(i) for i in ids.dropna().unique()]
    )
    # Ensure every original row has an entry (empty list if no matches)
    grouped = grouped.reindex(cr.index, fill_value=[])
    # Assign back (aligned by index)
    cr["additional_ad_domain_ids"] = grouped.to_numpy()
    return cr


def make_creative_records_df(
    adv_creatives_df: pd.DataFrame,
    assets_df: pd.DataFrame,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    """Creates creative records DataFrame with domain IDs and asset relationships."""
    creative_records_df = adv_creatives_df.merge(
        assets_df[["md5_hash", "creative_asset_id"]],
        on="md5_hash",
        how="left",
        validate="m:1",
    )
    api_calls_df = query_api_calls_for_mitm_uuids(
        database_connection=database_connection,
        mitm_uuids=adv_creatives_df["mitm_uuid"].astype(str).tolist(),
    )
    creative_records_df = creative_records_df.merge(
        api_calls_df[["api_call_id", "mitm_uuid"]],
        on=["mitm_uuid"],
        how="left",
        validate="1:1",
    )
    ad_domains_df = query_ad_domains(database_connection=database_connection)
    # For mapping we only want the mapped domains
    ad_domains_df = ad_domains_df[~ad_domains_df["domain_id"].isna()].copy()
    ad_domains_df["domain_id"] = ad_domains_df["domain_id"].astype(int)
    ad_domains_df = append_missing_domains(
        ad_domains_df, creative_records_df, database_connection
    )
    creative_records_df = add_additional_domain_id_column(
        creative_records_df, ad_domains_df
    )
    creative_records_df = creative_records_df.merge(
        ad_domains_df.rename(columns={"domain_id": "creative_host_domain_id"}),
        left_on="host_ad_network_tld",
        right_on="domain_name",
        how="left",
    )
    creative_records_df = creative_records_df.merge(
        ad_domains_df.rename(columns={"domain_id": "creative_initial_domain_id"}),
        left_on="creative_initial_domain_tld",
        right_on="domain_name",
        how="left",
    )
    creative_records_df = creative_records_df.merge(
        ad_domains_df.rename(columns={"domain_id": "mmp_domain_id"}),
        left_on="mmp_tld",
        right_on="domain_name",
        how="left",
    )
    creative_records_df = creative_records_df[
        [
            "api_call_id",
            "creative_asset_id",
            "creative_host_domain_id",
            "creative_initial_domain_id",
            "advertiser_store_app_id",
            "advertiser_domain_id",
            "mmp_domain_id",
            "mmp_urls",
            "additional_ad_domain_ids",
        ]
    ]
    # Nullable IDs, watch out for Int64
    check_cols = [
        "creative_initial_domain_id",
        "mmp_domain_id",
        "advertiser_store_app_id",
        "advertiser_domain_id",
        "mmp_domain_id",
    ]
    for col in check_cols:
        creative_records_df[col] = np.where(
            creative_records_df[col].isna(), None, creative_records_df[col]
        )
    return creative_records_df


def parse_store_id_mitm_log(
    pub_store_id: str,
    run_id: int,
    database_connection: PostgresCon,
) -> list[dict[str, Any]]:
    """Parses MITM log for a specific store ID and processes creative content."""
    df, error_message = get_mitm_df(pub_store_id, run_id, database_connection)
    if error_message:
        logger.error(error_message)
        error_message_info = {
            "run_id": run_id,
            "pub_store_id": pub_store_id,
            "error_msg": error_message,
        }
        return [error_message_info]
    adv_creatives_df, error_messages = attribute_creatives(
        df, pub_store_id, database_connection
    )
    if adv_creatives_df.empty:
        if len(error_messages) == 0:
            error_msg = "No creatives or errors"
        else:
            error_msg = "No creatives found"
        logger.info(f"{error_msg}")
        row = {
            "run_id": run_id,
            "pub_store_id": pub_store_id,
            "error_msg": error_msg,
        }
        error_messages.append(row)
        return error_messages
    else:
        msg = "Success found creatives!"
        row = {
            "run_id": run_id,
            "pub_store_id": pub_store_id,
            "error_msg": msg,
        }
        error_messages.append(row)
    assets_df = adv_creatives_df[
        ["md5_hash", "file_extension", "phash"]
    ].drop_duplicates()
    assets_df = upsert_df(
        assets_df,
        table_name="creative_assets",
        database_connection=database_connection,
        key_columns=["md5_hash"],
        insert_columns=["md5_hash", "file_extension", "phash"],
        return_rows=True,
    )
    assets_df = assets_df.rename(columns={"id": "creative_asset_id"})
    # Future feature
    adv_creatives_df["advertiser_domain_id"] = None
    creative_records_df = make_creative_records_df(
        adv_creatives_df, assets_df, database_connection
    )
    key_columns = ["api_call_id"]
    creative_records_df["updated_at"] = datetime.datetime.now(tz=datetime.UTC)
    upsert_df(
        creative_records_df,
        table_name="creative_records",
        database_connection=database_connection,
        key_columns=key_columns,
        insert_columns=key_columns
        + [
            "creative_asset_id",
            "creative_initial_domain_id",
            "creative_host_domain_id",
            "advertiser_store_app_id",
            "advertiser_domain_id",
            "mmp_domain_id",
            "additional_ad_domain_ids",
            "mmp_urls",
            "updated_at",
        ],
    )
    logger.info(
        f"Upserted {creative_records_df.shape[0]} creative records for {pub_store_id} {assets_df.shape[0]} creatives"
    )
    upload_creatives_to_s3(assets_df)
    return error_messages


def parse_all_runs_for_store_id(
    pub_store_id: str, database_connection: PostgresCon
) -> None:
    """Parses all MITM runs for a store ID and logs results to database."""
    mitms = get_store_id_mitm_s3_keys(store_id=pub_store_id)
    for _i, mitm in mitms.iterrows():
        run_id = mitm["run_id"]
        try:
            error_messages = parse_store_id_mitm_log(
                pub_store_id, run_id, database_connection
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
        d = [x for x in error_messages if type(x) is dict]
        s = [x for x in error_messages if type(x) is pd.Series]
        error_msg_df = pd.concat([pd.DataFrame(d), pd.DataFrame(s)], ignore_index=True)
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
        log_creative_scan_results(error_msg_df, database_connection)


def _init_worker(use_ssh_tunnel: bool):
    """Initialize worker process with a database connection that will be reused."""
    global _worker_db_connection
    _worker_db_connection = get_db_connection(use_ssh_tunnel=use_ssh_tunnel)
    logger.info(
        f"Worker initialized with database connection (tunnel={use_ssh_tunnel})"
    )


def _process_single_mitm_log(row: pd.Series) -> dict:
    """Worker function to process a single MITM log in parallel."""
    global _worker_db_connection

    pub_store_id = row["store_id"]
    run_id = row["run_id"]

    try:
        error_messages = parse_store_id_mitm_log(
            pub_store_id, run_id, _worker_db_connection
        )
        return {
            "run_id": run_id,
            "pub_store_id": pub_store_id,
            "error_messages": error_messages,
            "success": True,
        }
    except Exception:
        error_msg = "CRITICAL uncaught error"
        logger.exception(f"{error_msg}")
        return {
            "run_id": run_id,
            "pub_store_id": pub_store_id,
            "error_messages": [
                {
                    "run_id": run_id,
                    "pub_store_id": pub_store_id,
                    "error_msg": error_msg,
                }
            ],
            "success": False,
        }


def scan_all_apps(
    database_connection: PostgresCon,
    only_new_apps: bool = False,
    recent_months: bool = False,
    use_ssh_tunnel: bool = False,
    max_workers: int = 1,
) -> None:
    """Scans all apps for creative content and uploads thumbnails to S3."""
    all_api_calls = query_api_calls_to_creative_scan(
        database_connection=database_connection, recent_months=recent_months
    )
    mitm_runs_to_scan = all_api_calls[["store_id", "run_id"]].drop_duplicates()
    logger.info(f"MITM logs to scan: {mitm_runs_to_scan.shape[0]}")

    if only_new_apps:
        creative_records = query_creative_records(
            database_connection=database_connection
        )
        mitm_runs_to_scan = mitm_runs_to_scan[
            ~mitm_runs_to_scan["run_id"]
            .astype(str)
            .isin(creative_records["run_id"].astype(str))
        ]
        logger.info(f"Apps to scan (limited to new apps): {mitm_runs_to_scan.shape[0]}")

    mitms_count = mitm_runs_to_scan.shape[0]
    logger.info(f"Starting parallel processing with {max_workers} workers")

    completed = 0

    with ProcessPoolExecutor(
        max_workers=max_workers, initializer=_init_worker, initargs=(use_ssh_tunnel,)
    ) as executor:
        for result in executor.map(
            _process_single_mitm_log, [row for _, row in mitm_runs_to_scan.iterrows()]
        ):
            completed += 1
            run_id = result["run_id"]
            pub_store_id = result["pub_store_id"]
            error_messages = result["error_messages"]

            logger.info(
                f"{completed}/{mitms_count}: {pub_store_id} {run_id=} completed"
            )

            if len(error_messages) == 0:
                continue

            # Log results to database
            d = [x for x in error_messages if type(x) is dict]
            s = [x for x in error_messages if type(x) is pd.Series]
            error_msg_df = pd.concat(
                [pd.DataFrame(d), pd.DataFrame(s)], ignore_index=True
            )
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
            log_creative_scan_results(error_msg_df, database_connection)

    logger.info("All MITM logs processed, syncing thumbs to S3...")
    subprocess.run(
        [
            "s3cmd",
            "sync",
            str(CREATIVE_THUMBS_DIR) + "/",
            "s3://appgoblin-data/creatives/thumbs/",
            "--acl-public",
        ],
        check=False,
    )
