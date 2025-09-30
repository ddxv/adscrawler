import datetime
import subprocess
import urllib
from typing import Any

import numpy as np
import pandas as pd

from adscrawler.config import CREATIVES_DIR, get_logger
from adscrawler.dbcon.connection import PostgresCon
from adscrawler.dbcon.queries import (
    log_creative_scan_results,
    query_ad_domains,
    query_apps_to_creative_scan,
    query_creative_records,
    query_store_app_by_store_id,
    query_store_apps_no_creatives,
    upsert_df,
)
from adscrawler.mitm_ad_parser.creative_processor import get_phash, store_creatives
from adscrawler.mitm_ad_parser.mitm_logs import get_creatives_df
from adscrawler.mitm_ad_parser.network_parsers import parse_sent_video_df
from adscrawler.mitm_ad_parser.utils import get_tld
from adscrawler.packages.storage import (
    get_app_creatives_s3_keys,
    get_store_id_mitm_s3_keys,
    upload_ad_creative_to_s3,
)

logger = get_logger(__name__, "mitm_scrape_ads")


IGNORE_CREATIVE_IDS = ["privacy", "google_play_icon_grey_2022", "favicon"]


def find_sent_video_df(
    df: pd.DataFrame, row: pd.Series, video_id: str
) -> pd.DataFrame | None:
    """Retrieves DataFrame rows containing the specified video ID from the given DataFrame."""
    sent_video_df = df[
        (df["response_text"].astype(str).str.contains(video_id, regex=False))
        & (df["start_time"] <= row.start_time)
    ].copy()
    if sent_video_df.empty:
        sent_video_df = df[
            (df["response_text"].astype(str).str.contains(video_id, regex=False))
        ].copy()
    if sent_video_df.empty:
        return None
    if sent_video_df.shape[0] > 1:
        logger.info(f"Multiple responses for {video_id=}")
    sent_video_df["tld_url"] = sent_video_df["url"].apply(lambda x: get_tld(x))
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
    creatives_df: pd.DataFrame,
    pub_store_id: str,
    database_connection: PostgresCon,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Attributes creative content to advertisers by analyzing ad network responses."""
    error_messages = []
    i = 0
    adv_creatives = []
    row_count = creatives_df.shape[0]
    for _i, row in creatives_df.iterrows():
        i += 1
        host_ad_network_tld = get_tld(row["tld_url"])
        video_id = get_video_id(row)
        if video_id == "":
            error_msg = "Bad creative parsing video ID is empty"
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        file_extension = row["file_extension"]
        # store_creatives(row, 'cheeck', file_extension)
        if video_id in IGNORE_CREATIVE_IDS:
            logger.info(f"Ignoring video {video_id}.{file_extension}")
            continue
        sent_video_df = find_sent_video_df(df, row, video_id)
        if sent_video_df is None or sent_video_df.empty:
            error_msg = f"No source bidrequest found for {row['tld_url']}"
            logger.error(f"{error_msg} {video_id}")
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        sent_video_df["pub_store_id"] = pub_store_id
        found_ad_infos, found_error_messages = parse_sent_video_df(
            row, pub_store_id, sent_video_df, database_connection, video_id
        )
        for found_error_message in found_error_messages:
            row_copy = row.copy()
            row_copy["error_msg"] = found_error_message["error_msg"]
            error_messages.append(row_copy)
        found_ad_infos = [x for x in found_ad_infos if x["adv_store_id"] != "unknown"]
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
            continue
        elif len(found_advs) == 0:
            error_msg = f"No adv_store_id found for {row['tld_url']} {video_id=}"
            logger.error(error_msg)
            row["error_msg"] = error_msg
            error_messages.append(row)
            continue
        else:
            adv_store_id = found_advs[0]
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
            md5_hash = store_creatives(
                row,
                adv_store_id=adv_store_id,
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
                adv_store_id=adv_store_id,
                file_extension=file_extension,
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
        init_tld = init_tlds[0]
        adv_store_app_ids = [x["adv_store_app_id"] for x in found_ad_infos]
        adv_store_app_id = adv_store_app_ids[0]
        adv_creatives.append(
            {
                "pub_store_id": pub_store_id,
                "adv_store_id": adv_store_id,
                "adv_store_app_id": adv_store_app_id,
                "host_ad_network_tld": host_ad_network_tld,
                "creative_initial_domain_tld": init_tld,
                "mmp_urls": found_mmp_urls,
                "found_ad_network_tlds": found_ad_network_tlds,
                "mmp_tld": mmp_tld,
                "md5_hash": md5_hash,
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


def upload_creatives(adv_creatives_df: pd.DataFrame) -> None:
    """Uploads creative files to S3 storage."""
    upload_creatives_to_s3(adv_creatives_df)


def upload_creatives_to_s3(adv_creatives_df: pd.DataFrame) -> None:
    """Uploads creative files to S3, checking for existing files to avoid duplicates."""
    for adv_id, adv_creatives_df_adv in adv_creatives_df.groupby("adv_store_id"):
        try:
            s3_keys = get_app_creatives_s3_keys(store=1, store_id=adv_id)
            s3_keys_md5_hashes = s3_keys["md5_hash"].to_numpy()
        except FileNotFoundError:
            s3_keys_md5_hashes = []
        for _i, row in adv_creatives_df_adv.iterrows():
            if row["md5_hash"] in s3_keys_md5_hashes:
                logger.info(f"Creative {row['md5_hash']} already in S3")
                continue
            upload_ad_creative_to_s3(
                store=1,
                adv_store_id=adv_id,
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
            ~creative_records_df[col].isin(ad_domains_df["domain"])
        ]
        if not missing_ad_domains.empty:
            new_ad_domains = (
                missing_ad_domains[[col]]
                .drop_duplicates()
                .rename(columns={col: "domain"})
            )
            new_ad_domains = upsert_df(
                table_name="ad_domains",
                df=new_ad_domains,
                insert_columns=["domain"],
                key_columns=["domain"],
                database_connection=database_connection,
                return_rows=True,
            )
            ad_domains_df = pd.concat([new_ad_domains, ad_domains_df])
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
        ad_domains_df[["domain", "id"]],
        left_on="found_ad_network_tlds",
        right_on="domain",
        how="left",
    )

    # Group back the matching ids by the original row index
    grouped = merged.groupby("orig_idx")["id"].apply(
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
        assets_df[["store_app_id", "md5_hash", "creative_asset_id"]],
        left_on=["adv_store_app_id", "md5_hash"],
        right_on=["store_app_id", "md5_hash"],
        how="left",
    )
    ad_domains_df = query_ad_domains(database_connection=database_connection)
    ad_domains_df = append_missing_domains(
        ad_domains_df, creative_records_df, database_connection
    )
    creative_records_df = add_additional_domain_id_column(
        creative_records_df, ad_domains_df
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
        left_on="host_ad_network_tld",
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
            "mmp_urls",
            "additional_ad_domain_ids",
        ]
    ]
    check_cols = ["creative_host_domain_id", "mmp_domain_id"]
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
    df, creatives_df, error_message = get_creatives_df(
        pub_store_id, run_id, database_connection
    )
    if error_message:
        logger.error(error_message)
        error_message_info = {
            "run_id": run_id,
            "pub_store_id": pub_store_id,
            "error_msg": error_message,
        }
        return [error_message_info]
    adv_creatives_df, error_messages = attribute_creatives(
        df, creatives_df, pub_store_id, database_connection
    )
    if adv_creatives_df.empty:
        if len(error_messages) == 0:
            error_msg = "No creatives or errors"
            logger.error(f"{error_msg}")
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
    pub_db_id = query_store_app_by_store_id(
        store_id=pub_store_id, database_connection=database_connection
    )
    adv_creatives_df["store_app_pub_id"] = pub_db_id
    adv_creatives_df["run_id"] = run_id
    assets_df = adv_creatives_df[
        ["adv_store_app_id", "md5_hash", "file_extension", "phash"]
    ].drop_duplicates()
    assets_df = assets_df.rename(columns={"adv_store_app_id": "store_app_id"})
    assets_df = upsert_df(
        assets_df,
        table_name="creative_assets",
        database_connection=database_connection,
        key_columns=["store_app_id", "md5_hash"],
        insert_columns=["store_app_id", "md5_hash", "file_extension", "phash"],
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
    creative_records_df["updated_at"] = datetime.datetime.now()
    upsert_df(
        creative_records_df,
        table_name="creative_records",
        database_connection=database_connection,
        key_columns=key_columns,
        insert_columns=key_columns
        + [
            "updated_at",
            "mmp_domain_id",
            "additional_ad_domain_ids",
            "mmp_urls",
        ],
    )
    upload_creatives(adv_creatives_df)
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


def scan_all_apps(
    database_connection: PostgresCon,
    limit_store_apps_no_creatives: bool = True,
    only_new_apps: bool = False,
) -> None:
    """Scans all apps for creative content and uploads thumbnails to S3."""
    apps_to_scan = query_apps_to_creative_scan(database_connection=database_connection)
    logger.info(f"Apps to scan: {apps_to_scan.shape[0]}")
    if limit_store_apps_no_creatives:
        store_apps_no_creatives = query_store_apps_no_creatives(
            database_connection=database_connection
        )
        store_apps_no_creatives["no_creatives"] = True
        store_apps_no_creatives["run_id"] = store_apps_no_creatives["run_id"].astype(
            int
        )
        filtered_apps = pd.merge(
            apps_to_scan,
            store_apps_no_creatives,
            left_on=["store_id", "run_id"],
            right_on=["pub_store_id", "run_id"],
            how="left",
        )
        apps_to_scan = filtered_apps[filtered_apps["no_creatives"].isna()]
        apps_to_scan = apps_to_scan[["store_id", "api_calls"]].drop_duplicates()
        logger.info(f"Apps to scan (limited to no creatives): {apps_to_scan.shape[0]}")
    if only_new_apps:
        creative_records = query_creative_records(
            database_connection=database_connection
        )
        apps_to_scan = apps_to_scan[
            ~apps_to_scan["store_id"].isin(creative_records["pub_store_id"])
        ]
        logger.info(f"Apps to scan (limited to new apps): {apps_to_scan.shape[0]}")
    apps_count = apps_to_scan.shape[0]
    i = 0
    for _i, app in apps_to_scan.iterrows():
        i += 1
        pub_store_id = app["store_id"]
        logger.info(f"{i}/{apps_count}: {pub_store_id} start")
        try:
            parse_all_runs_for_store_id(pub_store_id, database_connection)
        except Exception as e:
            logger.exception(f"Error parsing {pub_store_id}: {e}")
            continue
    subprocess.run(
        [
            "s3cmd",
            "sync",
            str(CREATIVES_DIR / "thumbs") + "/",
            "s3://appgoblin-data/creatives/thumbs/",
            "--acl-public",
        ],
        check=False,
    )
