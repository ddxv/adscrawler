import datetime
import time

import pandas as pd
from sqlalchemy import text

from adscrawler.config import get_logger
from adscrawler.dbcon.atomic_swap import atomic_swap_partition
from adscrawler.dbcon.connection import get_db_connection
from adscrawler.dbcon.queries import upsert_df
from adscrawler.process.storage import (
    get_s3_client,
)

logger = get_logger(__name__)


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
    logger.info(f"Filtered {init_rows - sdf.shape[0]} APKs newer than cutoff date")
    logger.info(f"Returning df with {sdf.shape[0]} APKs")
    return sdf


def query_all_version_codes(pgdb, store: int, my_cutoff_date: str) -> pd.DataFrame:
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


def delete_s3_apks(bucket: str, apk_keys: list[str]) -> None:
    """Delete all S3 objects with the given prefix."""
    s3 = get_s3_client()
    keys = [k.replace(f"s3://{bucket}/", "") for k in apk_keys]
    too_short_keys = [k for k in keys if len(k) < 20]
    if too_short_keys:
        raise ValueError(
            f"Some keys are too short to safely delete: {too_short_keys}. Aborting."
        )
    chunk_size = 500
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


def delete_copied_apks(tdf: pd.DataFrame, ldf: pd.DataFrame) -> None:
    ldf["store_id_count"] = ldf.groupby("store_id")["store_id"].transform("count")
    duplicated_ldf = (
        ldf[ldf["store_id_count"] > 1]
        .sort_values(["store_id", "last_modified"], ascending=False)
        .drop_duplicates(subset=["store_id"], keep="first")
    )
    dupes_copied_to_thirdgate = pd.merge(
        duplicated_ldf[["store_id", "versionstr", "s3_key"]],
        tdf[["store_id", "versionstr"]],
        on=["store_id", "versionstr"],
        how="inner",
    )
    dupes_in_thirdgate_keys = dupes_copied_to_thirdgate["s3_key"].unique().tolist()
    logger.info(
        f"{len(dupes_in_thirdgate_keys)} duplicated APKs in original S3 that have been copied to thirdgate, deleting from original S3"
    )
    delete_s3_apks(bucket="adscrawler", apk_keys=dupes_in_thirdgate_keys)
    return dupes_in_thirdgate_keys


def file_cleanup(pgdb, sdf: pd.DataFrame, vcdf: pd.DataFrame) -> None:
    """Aggressive Cleanup"""
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


def run_cleanup(pgdb) -> None:
    """Main entry point: remove old / secondary APKs from S3 and reconcile the DB."""
    today_now = datetime.datetime.now(tz=datetime.UTC)
    aldf = get_s3_apk_paths(
        s3_client="s3",
        bucket="adscrawler",
        prefix="apks/android",
        my_cutoff_date=today_now,
    )
    ildf = get_s3_apk_paths(
        s3_client="s3",
        bucket="adscrawler",
        prefix="apks/ios",
        my_cutoff_date=today_now,
    )
    atdf = get_s3_apk_paths(
        s3_client="s3thirdgate",
        bucket="apks",
        prefix="android",
        my_cutoff_date=today_now,
    )
    itdf = get_s3_apk_paths(
        s3_client="s3thirdgate",
        bucket="apks",
        prefix="ios",
        my_cutoff_date=today_now,
    )

    one_week_ago = today_now - datetime.timedelta(days=7)
    one_week_ago_str = one_week_ago.strftime("%Y-%m-%d")

    aldf_old = aldf[aldf["last_modified"] < one_week_ago_str].copy()
    ildf_old = ildf[ildf["last_modified"] < one_week_ago_str].copy()
    today_str = datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%d")
    atdf_old = atdf[atdf["last_modified"] < today_str].copy()
    itdf_old = itdf[itdf["last_modified"] < today_str].copy()

    # DELETE FROM S3: LOKI
    try:
        al_deleted = delete_copied_apks(ldf=aldf_old, tdf=atdf_old)
        aldf = aldf[~(aldf["s3_key"].isin(al_deleted))]
    except Exception as e:
        logger.error(f"Error deleting copied APKs: {e}")
    try:
        il_deleted = delete_copied_apks(ldf=ildf_old, tdf=itdf_old)
        ildf = ildf[~(ildf["s3_key"].isin(il_deleted))]
    except Exception as e:
        logger.error(f"Error deleting copied APKs: {e}")

    # android_db_files = query_all_version_codes(store=1, my_cutoff_date=one_week_ago_str)
    # ios_db_files = query_all_version_codes(store=2, my_cutoff_date=one_week_ago_str)
    # file_cleanup(sdf=android_s3_files, vcdf=android_db_files)
    # file_cleanup(sdf=ios_s3_files, vcdf=ios_db_files)
    itdf["myregion"] = "thirdgate"
    atdf["myregion"] = "thirdgate"
    ildf["myregion"] = "loki"
    aldf["myregion"] = "loki"
    df = pd.concat([itdf, atdf, ildf, aldf])
    ios_version_codes = query_all_version_codes(
        pgdb=pgdb, store=2, my_cutoff_date=today_now
    )
    android_version_codes = query_all_version_codes(
        pgdb=pgdb, store=1, my_cutoff_date=today_now
    )
    version_codes = pd.concat([ios_version_codes, android_version_codes])
    # mdf = pd.merge(
    #     df,
    #     version_codes,
    #     how="outer",
    #     left_on=["store_id", "versionstr"],
    #     right_on=["store_id", "version_code"],
    # )

    # # All -2 that do not have files
    # mdf[(mdf["id"].notna()) & (mdf["s3_key"].isna()) & (mdf["apk_hash"].str.len() > 2)]
    mdf = pd.merge(
        df,
        version_codes,
        how="left",
        left_on=["store_id", "versionstr"],
        right_on=["store_id", "version_code"],
    )

    mdf = mdf[~(mdf["id"].isna())].rename(
        columns={"id": "version_code_id", "s3_key": "file_key"}
    )

    int_cols = ["store_app", "version_code_id"]
    for col in int_cols:
        mdf[col] = mdf[col].astype("Int64")

    mdf = mdf[
        [
            "store_app",
            "version_code_id",
            "versionstr",
            "myregion",
            "file_key",
            "last_modified",
        ]
    ]
    mdf["batch_date"] = today_now.date()
    atomic_swap_partition(
        df=mdf,
        pgdb=pgdb,
        schema="public",
        table="s3_package_inventory",
        batch_date=today_now.date(),
    )


if __name__ == "__main__":
    run_cleanup()
