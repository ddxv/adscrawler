import datetime
import time

import pandas as pd
from sqlalchemy import text

from adscrawler.config import get_logger
from adscrawler.dbcon.connection import get_db_connection
from adscrawler.dbcon.queries import upsert_df
from adscrawler.packages.storage import (
    get_s3_client,
)

logger = get_logger(__name__)


pgdb = get_db_connection()


def get_s3_apk_paths(
    s3_client: str, bucket: str, prefix: str, my_cutoff_date: str
) -> pd.DataFrame:
    s3 = get_s3_client(s3_client)
    continuation_token = None
    rows = []
    while True:
        params = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        response = s3.list_objects_v2(**params)
        if "Contents" in response:
            rows += [
                {
                    "s3_key": f"s3://{bucket}/{obj['Key']}",
                    "size_bytes": obj["Size"],
                    "last_modified": obj["LastModified"],
                }
                for obj in response["Contents"]
            ]
        if "NextContinuationToken" in response:
            continuation_token = response["NextContinuationToken"]
        else:
            break
    sdf = pd.DataFrame(rows, columns=["s3_key", "size_bytes", "last_modified"])
    sdf["file_path"] = sdf["s3_key"].str.split("/").str[-1]
    sdf["versionstr"] = sdf["s3_key"].str.split("/").str[-2]
    sdf["store_id"] = sdf["s3_key"].str.split("/").str[-3]
    init_rows = sdf.shape[0]
    sdf = sdf[sdf["last_modified"] < my_cutoff_date]
    logger.info(f"Filtered {init_rows - sdf.shape[0]} APKs newer than cutoff date")
    logger.info(f"Returning df with {sdf.shape[0]} APKs")
    return sdf


def query_apps_to_check(limit: int = 100) -> pd.DataFrame:
    query = f"""
    SELECT * FROM version_codes 
    LEFT JOIN store_apps
    ON version_codes.store_app = store_apps.id
    WHERE apk_hash IS NOT NULL
    LIMIT {limit}
    """
    return pd.read_sql(query, pgdb.engine)


def query_all_version_codes(store: int, my_cutoff_date: str) -> pd.DataFrame:
    query = f"""
    SELECT vc.*, 
    sa.store_id 
    from version_codes vc
    LEFT JOIN store_apps sa ON vc.store_app = sa.id
    WHERE sa.store = {store}
    AND vc.created_at <= '{my_cutoff_date}' 
    AND vc.updated_at <= '{my_cutoff_date}'
           ;
           """
    return pd.read_sql(query, pgdb.engine)


def query_problematic_apks(limit: int = 100) -> pd.DataFrame:
    # NOTE Tried 500 and had zereo issues
    query = f"""
    WITH mysel AS (SELECT 
    version_code_id, scan_result, count(*) AS scan_count, max(scanned_at) AS last_scanned_at
    FROM version_code_sdk_scan_results vcssr 
        WHERE scanned_at >= '2026-03-21' AND vcssr.scan_result != 1
           GROUP BY version_code_id, scan_result
           ORDER BY scan_count desc
              LIMIT {limit}
    )
    SELECT * FROM mysel ms 
    LEFT JOIN version_codes vc ON ms.version_code_id = vc.id
    LEFT JOIN store_apps sa ON vc.store_app = sa.id
           ;
           """
    return pd.read_sql(query, pgdb.engine)


def version_codes_with_no_scans() -> pd.DataFrame:
    query = """
         SELECT * FROM version_codes vc WHERE id IN (
         SELECT DISTINCT version_code_id
         FROM version_code_api_scan_results
         WHERE version_code_id NOT IN (
             SELECT version_code_id
             FROM version_code_api_scan_results
             WHERE run_result = 1
         )
         AND vc.created_at <= '2026-04-01' AND vc.updated_at <= '2026-04-01'
         );
        """
    return pd.read_sql(query, pgdb.engine)


def delete_s3_apks(bucket: str, apk_keys: list[str]) -> None:
    """Delete all S3 objects with the given prefix."""
    s3 = get_s3_client()
    keys = [k.replace(f"s3://{bucket}/", "") for k in apk_keys]
    chunk_size = 1000
    for i in range(0, len(keys), chunk_size):
        chunk = keys[i : i + chunk_size]
        logger.info(f"Deleting {len(chunk)} objects (batch {i // chunk_size + 1})")
        response = s3.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": k} for k in chunk]}
        )
        if "Errors" in response:
            for err in response["Errors"]:
                logger.error(f"Failed to delete {err['Key']}: {err['Message']}")
        if i + chunk_size < len(keys):
            time.sleep(0.3)


def query_version_codes_with_failed_scans(scan_cutoff_date: str) -> pd.DataFrame:
    query = """WITH failed_api_scans AS (
        SELECT DISTINCT ON (version_code_id)
            version_code_id,
            run_at,
            COUNT(*) OVER (PARTITION BY version_code_id) AS failed_count
        FROM version_code_api_scan_results r
        WHERE NOT EXISTS (
            SELECT 1
            FROM api_calls ac LEFT JOIN version_code_api_scan_results vcasr ON ac.run_id = vcasr.id
            WHERE version_code_id = r.id
        )
        ORDER BY version_code_id, run_at DESC 
    ),
    failed_sdk_scans AS (
        SELECT DISTINCT ON (version_code_id)
            version_code_id,
            scanned_at,
            COUNT(*) OVER (PARTITION BY version_code_id) AS failed_count
        FROM version_code_sdk_scan_results r
        WHERE NOT EXISTS (
            SELECT 1
            FROM version_code_sdk_scan_results
            WHERE version_code_id = r.version_code_id
              AND scan_result = 1
        )
        ORDER BY version_code_id, scanned_at DESC
    )
    SELECT
        vc.*,
        sa.store_id,
        fas.version_code_id IS NOT NULL AS failed_api,
        fas.run_at                       AS last_failed_api,
        fas.failed_count                 AS failed_api_count,
        fss.version_code_id IS NOT NULL AS failed_sdk,
        fss.scanned_at                   AS last_failed_sdk,
        fss.failed_count                 AS failed_sdk_count
    FROM version_codes vc
    LEFT JOIN failed_api_scans fas ON fas.version_code_id = vc.id
    LEFT JOIN failed_sdk_scans fss ON fss.version_code_id = vc.id
    LEFT JOIN store_apps sa ON vc.store_app = sa.id
    WHERE
        vc.crawl_result = 1
        AND vc.created_at  < :scan_cutoff_date
        AND vc.updated_at  < :scan_cutoff_date
        AND (fas.version_code_id IS NOT NULL OR fss.version_code_id IS NOT NULL);
        """
    df = pd.read_sql(
        text(query), pgdb.engine, params={"scan_cutoff_date": scan_cutoff_date}
    )
    return df


def delete_copied_apks(store_name: str, sdf: pd.DataFrame) -> None:
    tdf = get_s3_apk_paths(
        s3_client="s3thirdgate",
        bucket="apks",
        prefix=f"{store_name}",
        my_cutoff_date=today_str,
    )
    sdf["store_id_count"] = sdf.groupby("store_id")["store_id"].transform("count")
    duplicated_sdf = (
        sdf[sdf["store_id_count"] > 1]
        .sort_values(["store_id", "last_modified"], ascending=False)
        .drop_duplicates(subset=["store_id"], keep="first")
    )
    dupes_copied_to_thirdgate = pd.merge(
        duplicated_sdf[["store_id", "versionstr", "s3_key"]],
        tdf[["store_id", "versionstr"]],
        on=["store_id", "versionstr"],
        how="inner",
    )
    dupes_in_thirdgate_keys = dupes_copied_to_thirdgate["s3_key"].unique().tolist()
    logger.info(
        f"{len(dupes_in_thirdgate_keys)} duplicated APKs in original S3 that have been copied to thirdgate, deleting from original S3"
    )
    delete_s3_apks(bucket="adscrawler", apk_keys=dupes_in_thirdgate_keys)


def file_cleanup(sdf: pd.DataFrame, vcdf: pd.DataFrame) -> None:
    # In S3 but never recorded
    in_s3_unrecorded = (
        sdf[~sdf["store_id"].isin(vcdf["store_id"].unique())]["s3_key"]
        .unique()
        .tolist()
    )
    logger.info(f"{len(in_s3_unrecorded)} APKs in S3 but never recorded in DB")
    delete_s3_apks(bucket="adscrawler", apk_keys=in_s3_unrecorded)
    # In S3 but incorrectly recorded
    in_s3_db_incorrect = (
        sdf[
            ~sdf["store_id"].isin(vcdf[vcdf["crawl_result"] == 1]["store_id"].unique())
        ]["s3_key"]
        .unique()
        .tolist()
    )
    logger.info(f"{len(in_s3_db_incorrect)} APKs in S3 but incorrectly recorded in DB")
    delete_s3_apks(bucket="adscrawler", apk_keys=in_s3_db_incorrect)
    df = pd.merge(
        sdf,
        vcdf,
        left_on=["store_id", "versionstr"],
        right_on=["store_id", "version_code"],
        how="outer",
    )
    # Exists in S3 with a versionstr, but no matching version_code in DB
    no_matching_db_version_code = (
        df[(df["versionstr"].notna()) & (df["version_code"].isna())]["s3_key"]
        .unique()
        .tolist()
    )
    logger.info(
        f"{len(no_matching_db_version_code)} APKs in S3 with no matching version_code in DB"
    )
    delete_s3_apks(bucket="adscrawler", apk_keys=no_matching_db_version_code)
    # Exists in S3 with a versionstr, but crawl_result is not 1
    no_matching_sucess_db_id = df[
        (df["versionstr"].notna()) & (df["crawl_result"] != 1)
    ]
    logger.info(
        f"{no_matching_sucess_db_id.shape[0]} APKs in S3 with no matching successful crawl in DB"
    )
    delete_s3_apks(
        bucket="adscrawler",
        apk_keys=no_matching_sucess_db_id["s3_key"].unique().tolist(),
    )
    # Does not exist in S3, but DB success
    files_not_in_s3 = (df["versionstr"].isna()) & (df["crawl_result"] == 1)
    fdf = df[files_not_in_s3][
        ["id", "store_app", "crawl_result", "version_code", "created_at"]
    ]
    logger.info(
        f"{fdf.shape[0]} version_codes with crawl_result 1 but no file in S3, setting crawl_result to -2"
    )
    fdf["crawl_result"] = -2
    upsert_df(
        df=fdf,
        table_name="version_codes",
        key_columns=["id"],
        insert_columns=["crawl_result", "store_app", "version_code"],
        pgdb=pgdb,
    )


def delete_failing_apps() -> None:
    # More serious delete based on failed scans
    scan_cutoff_date = "2026-04-01"
    noscans = query_version_codes_with_failed_scans(scan_cutoff_date=scan_cutoff_date)
    noscans = noscans[
        (noscans["failed_api"]) & (noscans["failed_api_count"] > 2)
        | (noscans["failed_sdk"]) & (noscans["failed_sdk_count"] > 3)
    ]
    ndf = pd.merge(
        noscans,
        android_s3_files,
        left_on=["store_id", "version_code"],
        right_on=["store_id", "versionstr"],
        how="inner",
    )
    failing_unzip = ndf["s3_key"].unique().tolist()
    ndf["size_bytes"].sum() / 100000000
    fdf = ndf[["id", "store_app", "crawl_result", "version_code", "created_at"]]
    fdf["crawl_result"] = -2
    logger.info(
        f"{len(failing_unzip)} APKs with failed SDK scans and files in S3, likely failing to unzip or process manifest"
    )
    delete_s3_apks(bucket="adscrawler", apk_keys=failing_unzip)
    upsert_df(
        df=fdf,
        table_name="version_codes",
        key_columns=["id"],
        insert_columns=["crawl_result", "store_app", "version_code"],
        pgdb=pgdb,
    )


today = datetime.datetime.now(tz=datetime.UTC)

yesterday = today - datetime.timedelta(days=1)
one_week_ago = today - datetime.timedelta(days=7)
two_weeks_ago = today - datetime.timedelta(days=14)

today_str = today.strftime("%Y-%m-%d")
yesterday_str = yesterday.strftime("%Y-%m-%d")
one_week_ago_str = one_week_ago.strftime("%Y-%m-%d")
two_weeks_ago_str = two_weeks_ago.strftime("%Y-%m-%d")

# Files in S3
android_s3_files = get_s3_apk_paths(
    s3_client="s3",
    bucket="adscrawler",
    prefix="apks/android",
    my_cutoff_date=one_week_ago_str,
)

ios_s3_files = get_s3_apk_paths(
    s3_client="s3",
    bucket="adscrawler",
    prefix="apks/ios",
    my_cutoff_date=one_week_ago_str,
)

delete_copied_apks(store_name="android", sdf=android_s3_files)
delete_copied_apks(store_name="ios", sdf=ios_s3_files)

# Rebuild S3 Files list
android_s3_files = get_s3_apk_paths(
    s3_client="s3",
    bucket="adscrawler",
    prefix="apks/android",
    my_cutoff_date=one_week_ago_str,
)


ios_s3_files = get_s3_apk_paths(
    s3_client="s3",
    bucket="adscrawler",
    prefix="apks/ios",
    my_cutoff_date=one_week_ago_str,
)

# Delete / Cleanup files
android_db_files = query_all_version_codes(store=1, my_cutoff_date=one_week_ago_str)
ios_db_files = query_all_version_codes(store=2, my_cutoff_date=one_week_ago_str)

file_cleanup(sdf=android_s3_files, vcdf=android_db_files)
file_cleanup(sdf=ios_s3_files, vcdf=ios_db_files)
