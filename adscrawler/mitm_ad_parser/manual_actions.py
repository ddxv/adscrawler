import os
import pathlib

import numpy as np
import pandas as pd

from adscrawler.config import MITM_DIR, get_logger
from adscrawler.dbcon.connection import PostgresCon
from adscrawler.dbcon.queries import (
    get_store_id_mitm_s3_keys,
    query_apps_to_creative_scan,
)
from adscrawler.mitm_ad_parser.mitm_scrape_ads import (
    add_is_creative_content_column,
    parse_log,
)
from adscrawler.packages.storage import (
    download_mitm_log_by_key,
    get_s3_client,
)

logger = get_logger(__name__, "mitm_scrape_ads")


def download_all_mitms(database_connection: PostgresCon) -> None:
    """Downloads all MITM log files from S3 for apps that need creative scanning."""
    apps_to_download = query_apps_to_creative_scan(
        database_connection=database_connection
    )
    for i, app in apps_to_download.iterrows():
        logger.info(f"{i}/{apps_to_download.shape[0]}: {app['store_id']} start")
        pub_store_id = app["store_id"]
        # Check if any log files exist for this pub_store_id
        if list(pathlib.Path(MITM_DIR).glob(f"{pub_store_id}_*.log")):
            logger.info(f"{pub_store_id} a mitm log is already downloaded")
            continue
        try:
            mitms = get_store_id_mitm_s3_keys(store_id=pub_store_id)
        except FileNotFoundError:
            logger.error(f"{pub_store_id} not found in s3")
            continue
        for _i, mitm in mitms.iterrows():
            key = mitm["key"]
            run_id = mitm["run_id"]
            filename = f"{pub_store_id}_{run_id}.log"
            if not pathlib.Path(MITM_DIR, filename).exists():
                _mitm_log_path = download_mitm_log_by_key(key, filename)
            else:
                pass


def open_all_local_mitms(database_connection: PostgresCon) -> pd.DataFrame:
    """Opens and processes all local MITM log files into a combined DataFrame."""
    all_mitms_df = pd.DataFrame()
    i = 0
    num_files = len(list(pathlib.Path(MITM_DIR).glob("*.log")))
    logger.info(f"Opening {num_files} local mitm logs")
    for mitm_log_path in pathlib.Path(MITM_DIR).glob("*.log"):
        i += 1
        logger.info(f"{i}/{num_files}: {mitm_log_path}")
        pub_store_id = mitm_log_path.name.split("_")[0]
        run_id = mitm_log_path.name.split("_")[1].replace(".log", "")
        df = parse_log(pub_store_id, run_id, database_connection)
        if "response_mime_type" in df.columns:
            df = add_is_creative_content_column(df)
            df["response_text"] = np.where(
                df["is_creative_content"], "", df["response_text"]
            )
            df["response_content"] = np.where(
                df["is_creative_content"], "", df["response_content"]
            )
        if "response_size" in df.columns:
            df["response_text"] = np.where(
                df["response_size"].fillna("0").astype(int) > 500000,
                "",
                df["response_text"],
            )
            df["response_content"] = np.where(
                df["response_size"].fillna("0").astype(int) > 500000,
                "",
                df["response_content"],
            )
        df["pub_store_id"] = pub_store_id
        df["run_id"] = run_id
        all_mitms_df = pd.concat([all_mitms_df, df], ignore_index=True)
    return all_mitms_df


def upload_all_mitms_to_s3() -> None:
    """Uploads processed MITM data to S3 storage."""
    bucket_name = "appgoblin-data"
    client = get_s3_client("digi-cloud")
    client.upload_file(
        "all_mitms.tsv.xz", Bucket=bucket_name, Key="mitmcsv/all_mitms.tsv.xz"
    )
    os.system("s3cmd setacl s3://appgoblin-data/mitmcsv/ --acl-public --recursive")
