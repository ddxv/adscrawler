import datetime
import os
import pathlib
import shutil
import time

import boto3
import duckdb
import pandas as pd
from botocore.exceptions import ClientError

from adscrawler.config import (
    APKS_INCOMING_DIR,
    CONFIG,
    CREATIVE_RAW_DIR,
    IPAS_INCOMING_DIR,
    MITM_DIR,
    XAPKS_INCOMING_DIR,
    get_logger,
)
from adscrawler.dbcon.connection import PostgresEngine, start_ssh_tunnel
from adscrawler.dbcon.queries import query_latest_api_scan_by_store_id
from adscrawler.packages.utils import (
    get_local_file_path,
    move_downloaded_app_to_main_dir,
)

logger = get_logger(__name__)


def get_s3_endpoint(key_name: str) -> str:
    host = CONFIG[key_name]["host"]
    port = None
    if CONFIG[key_name].get("use_ssh_tunnel", False):
        # Use SSH tunnel for remote self hosted S3
        port = start_ssh_tunnel(key_name)
        host = "http://127.0.0.1"
    elif CONFIG[key_name].get("remote_port"):
        # Self hosted S3 on same network
        port = CONFIG[key_name]["remote_port"]
        # NOTE: Current self hosted S3 uses HTTP
        host = f"http://{host}"
    if port:
        endpoint = f"{host}:{port}"
    else:
        # External hosted S3 like DigitalOcean Spaces
        if host.startswith("https"):
            endpoint = host
        else:
            endpoint = f"https://{host}"
    return endpoint


def get_s3_client(key_name: str = "s3") -> boto3.client:
    """Create and return an S3 client.

    This supports both self hosted and regular cloud S3. For the self hosted it also supports SSH port forwarding if the S3 is not public.

    """
    global S3_CLIENTS  # noqa: PLW0602

    if key_name in S3_CLIENTS:
        return S3_CLIENTS[key_name]

    endpoint_url = get_s3_endpoint(key_name)
    session = boto3.session.Session()
    client = session.client(
        "s3",
        region_name=CONFIG[key_name]["region_name"],
        endpoint_url=endpoint_url,
        aws_access_key_id=CONFIG[key_name]["access_key_id"],
        aws_secret_access_key=CONFIG[key_name]["secret_key"],
    )
    S3_CLIENTS[key_name] = client
    return client


def rankings_parquet_exists_in_s3(
    store: int, crawled_date: str, country: str, key_name: str = "s3"
) -> bool:
    """Return True if a rankings.parquet already exists in S3 for the given store/date/country."""
    s3 = get_s3_client(key_name)
    bucket = CONFIG[key_name]["bucket"]
    s3_key = f"raw-data/app_rankings/store={store}/crawled_date={crawled_date}/country={country.upper()}/rankings.parquet"
    try:
        s3.head_object(Bucket=bucket, Key=s3_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def delete_s3_objects_by_prefix(bucket: str, prefix: str, key_name: str = "s3") -> None:
    """Delete all S3 objects with the given prefix."""
    s3 = get_s3_client(key_name)
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )
    time.sleep(0.3)
    if "Contents" in response:
        logger.info("found objects, deleting")
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": obj["Key"]} for obj in response["Contents"]]},
        )
    time.sleep(0.3)


def delete_s3_objects_by_date_range(
    bucket: str,
    start_date_mon: datetime.date,
    end_date: datetime.date,
    prefix: str,
    key_name: str = "s3",
) -> None:
    """
    Delete only the week_start partitions within [start_date_mon, end_date]
    """
    s3 = get_s3_client(key_name)

    # Build one prefix per weekly Monday in range
    weekly_prefixes = [
        f"{prefix}/week_start={ddt.strftime('%Y-%m-%d')}/"
        for ddt in pd.date_range(start_date_mon, end_date, freq="W-MON")
    ]

    # Collect all keys across all prefixes (one list call each, but no delete yet)
    keys_to_delete = []
    for pprefix in weekly_prefixes:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=pprefix):
            for obj in page.get("Contents", []):
                keys_to_delete.append({"Key": obj["Key"]})

    if not keys_to_delete:
        logger.info("No objects to delete in date range.")
        return

    # Batch delete in chunks of 1000 (S3 API limit) — one sleep per chunk
    chunk_size = 1000
    for i in range(0, len(keys_to_delete), chunk_size):
        chunk = keys_to_delete[i : i + chunk_size]
        logger.info(f"Deleting {len(chunk)} objects (batch {i // chunk_size + 1})")
        s3.delete_objects(Bucket=bucket, Delete={"Objects": chunk})
        if i + chunk_size < len(keys_to_delete):
            time.sleep(0.3)  # only sleep between chunks, not after the last one


def upload_mitm_log_to_s3(
    store: int,
    store_id: str,
    version_str: str,
    run_id: int,
) -> None:
    """Upload apk to s3."""
    file_path = pathlib.Path(MITM_DIR, f"traffic_{store_id}.log")
    if not file_path.exists():
        logger.error(f"mitm log file not found at {file_path}")
        raise FileNotFoundError
    app_prefix = f"{store_id}/{version_str}/{run_id}.log"
    if store == 1:
        s3_key = f"mitm/android/{app_prefix}"
    elif store == 2:
        s3_key = f"mitm/ios/{app_prefix}"
    else:
        raise ValueError(f"Invalid store: {store}")
    metadata = {
        "store_id": store_id,
        "version_code": version_str,
        "run_id": str(run_id),
    }
    s3_client = get_s3_client()
    response = s3_client.put_object(
        Bucket=CONFIG["s3"]["bucket"],
        Key=s3_key,
        Body=file_path.read_bytes(),
        Metadata=metadata,
    )
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        logger.info(f"Uploaded {store_id} mitm log to S3")
    else:
        logger.error(f"Failed to upload {store_id} mitm log to S3")
    move_to_processed(store_id, run_id)


def move_to_processed(store_id: str, run_id: int) -> None:
    flows_file = f"traffic_{store_id}.log"
    final_flows_file = f"{store_id}_{run_id}.log"
    mitmlog_file = pathlib.Path(MITM_DIR, flows_file)
    destination_path = pathlib.Path(MITM_DIR, final_flows_file)
    if not mitmlog_file.exists():
        logger.error(f"mitm log file not found at {mitmlog_file}")
        raise FileNotFoundError
    shutil.move(mitmlog_file, destination_path)


def upload_ad_creative_to_s3(
    md5_hash: str,
    extension: str,
) -> None:
    """Upload apk to s3."""
    # app_prefix = f"{adv_store_id}/{md5_hash}.{extension}"
    creative_hash_prefix = md5_hash[0:3]
    file_path = pathlib.Path(CREATIVE_RAW_DIR, f"{md5_hash}.{extension}")
    prefix = f"creatives/raw/{creative_hash_prefix}/{md5_hash}.{extension}"
    metadata = {
        "md5": md5_hash,
    }
    s3_client = get_s3_client()
    response = s3_client.put_object(
        Bucket=CONFIG["s3"]["bucket"],
        Key=prefix,
        Body=file_path.read_bytes(),
        Metadata=metadata,
    )
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        logger.info("S3 creative uploaded")
    else:
        logger.error("S3 creative failed to upload")


def upload_apk_to_s3(
    store: int,
    store_id: str,
    extension: str,
    md5_hash: str,
    version_str: str,
    file_path: pathlib.Path,
) -> None:
    """Upload apk to s3."""
    app_prefix = f"{store_id}/{version_str}/{store_id}_{md5_hash}.{extension}"
    if store == 1:
        prefix = f"apks/android/{app_prefix}"
    elif store == 2:
        prefix = f"apks/ios/{app_prefix}"
    else:
        raise ValueError(f"Invalid store: {store}")
    metadata = {
        "store_id": store_id,
        "version_code": version_str,
        "md5": md5_hash,
    }
    s3_client = get_s3_client()
    response = s3_client.put_object(
        Bucket=CONFIG["s3"]["bucket"],
        Key=prefix,
        Body=file_path.read_bytes(),
        Metadata=metadata,
    )
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        logger.info(f"S3 uploaded apk={store_id}")
    else:
        logger.error(f"S3 upload failed apk={store_id}")


def get_downloaded_mitm_files(pgdb: PostgresEngine) -> pd.DataFrame:
    """Get all downloaded files of a given extension."""

    main_dir = pathlib.Path("/home/adscrawler/adscrawler/mitmlogs/")

    store_ids = []
    for file in pathlib.Path(main_dir).glob("traffic_*.log"):
        file_name = file.stem
        store_id = file_name[8:]
        store_ids.append(store_id)

    missing_files = []
    df = query_latest_api_scan_by_store_id(store_ids, pgdb)
    missing_files = [x for x in store_ids if x not in df["store_id"].to_list()]

    logger.info(f"S3 Uploadable: {df.shape[0]:,} missing: {len(missing_files)}")
    return df


def move_local_mitm_files_to_s3(pgdb: PostgresEngine) -> None:
    """Upload all local mitm files to s3.

    This is for occasional MANUAL PROCESSING.
    """

    df = get_downloaded_mitm_files(pgdb)

    df = df.rename(columns={"version_code": "version_str"})

    s3_client = get_s3_client()
    for _, row in df.iterrows():
        logger.info(f"{_}/{df.shape[0]:,}")
        store_id = row.store_id
        run_id = row.run_id
        version_str = row.version_str
        file_path = pathlib.Path(MITM_DIR, f"traffic_{store_id}.log")
        s3_key = f"mitm/android/{store_id}/{version_str}/{run_id}.log"
        metadata = {
            "store_id": store_id,
            "version_code": version_str,
            "run_id": str(run_id),
        }
        response = s3_client.put_object(
            Bucket=CONFIG["s3"]["bucket"],
            Key=s3_key,
            Body=file_path.read_bytes(),
            Metadata=metadata,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            pass
        else:
            logger.error(f"S3 upload failed for mitm logs {store_id}")
        os.system(f"mv {file_path} /home/adscrawler/completed-mitm-logs/{store_id}.log")


def get_store_id_mitm_s3_keys(store_id: str) -> pd.DataFrame:
    store = 1
    if store == 1:
        prefix = f"mitm/android/{store_id}/"
    elif store == 2:
        prefix = f"mitm/ios/{store_id}/"
    else:
        raise ValueError(f"Invalid store: {store}")
    logger.info(f"S3 download mitm logs {store_id=}")
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket=CONFIG["s3"]["bucket"], Prefix=prefix)
    objects_data = []
    if response["KeyCount"] == 0:
        msg = f"S3 no mitm logs found for {store_id=}"
        logger.error(msg)
        raise FileNotFoundError(msg)
    for obj in response["Contents"]:
        key = obj["Key"]
        key_parts = key.split("/")
        if len(key_parts) >= 4:
            version_code = key_parts[3]
        else:
            version_code = "unknown"
        run_id = key.split("/")[-1].replace(".log", "")
        objects_data.append(
            {
                "key": key,
                "store_id": store_id,
                "run_id": run_id,
                "version_code": version_code,
                "size": obj["Size"],
                "last_modified": obj["LastModified"],
            }
        )
    df = pd.DataFrame(objects_data)
    logger.debug(f"S3 found mitm logs: {store_id=} {df.shape[0]:,}")
    return df


def get_store_id_apk_s3_keys(store: int, store_id: str) -> pd.DataFrame:
    if store == 1:
        prefix = f"apks/android/{store_id}/"
    elif store == 2:
        prefix = f"apks/ios/{store_id}/"
    else:
        raise ValueError(f"Invalid store: {store}")
    logger.debug(f"S3 getting apk keys start {store_id=}")
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket=CONFIG["s3"]["bucket"], Prefix=prefix)
    objects_data = []
    if response["KeyCount"] == 0:
        logger.error(f"S3 no apk found for {store_id=}")
        raise FileNotFoundError(f"S3 no apk found for {store_id=}")
    for obj in response["Contents"]:
        key_parts = obj["Key"].split("/")
        if len(key_parts) >= 4:
            version_code = key_parts[3]
        else:
            version_code = "unknown"

        objects_data.append(
            {
                "key": obj["Key"],
                "store_id": store_id,
                "version_code": version_code,
                "size": obj["Size"],
                "last_modified": obj["LastModified"],
            }
        )
    df = pd.DataFrame(objects_data)
    logger.info(f"S3 keys found {store_id=} {df.shape[0]:,}")
    return df


def download_mitm_log_by_key(key: str, filename: str) -> pathlib.Path:
    downloaded_file_path = pathlib.Path(MITM_DIR, filename)
    s3_client = get_s3_client()
    with open(downloaded_file_path, "wb") as f:
        s3_client.download_fileobj(
            Bucket=CONFIG["s3"]["bucket"],
            Key=key,
            Fileobj=f,
        )
    if not downloaded_file_path.exists():
        raise FileNotFoundError(f"{downloaded_file_path=} after download not found")
    return downloaded_file_path


def download_app_by_store_id(
    store: int, store_id: str, version_str: str | None = None
) -> tuple[pathlib.Path, str]:
    func_info = f"S3 download_app_by_store_id {store_id=} {version_str=}"
    df = get_store_id_apk_s3_keys(store, store_id)
    if df.empty:
        logger.error(f"S3 no apk found for {store_id=}")
        raise FileNotFoundError(f"S3 no apk found for {store_id=}")
    df = df[~(df["version_code"] == "failed")]
    if df.empty:
        logger.error(f"S3 only has failed apk for {store_id=}, no version_code")
    final_version_str: str
    if version_str and version_str != "-1":
        df = df[df["version_code"] == version_str]
        final_version_str = version_str
    else:
        df = df.sort_values(by="version_code", ascending=False)
        final_version_str = str(df["version_code"].to_numpy()[0])
    key = df["key"].to_numpy()[0]
    filename = key.split("/")[-1]
    extension = filename.split(".")[-1]
    if extension == "apk":
        downloaded_file_path = pathlib.Path(APKS_INCOMING_DIR, filename)
    elif extension == "xapk":
        downloaded_file_path = pathlib.Path(XAPKS_INCOMING_DIR, filename)
    elif extension == "ipa":
        downloaded_file_path = pathlib.Path(IPAS_INCOMING_DIR, filename)
    else:
        raise ValueError(f"Invalid extension: {extension}")
    logger.info(f"{func_info} {key=} to local start")
    s3_client = get_s3_client()
    with open(downloaded_file_path, "wb") as f:
        s3_client.download_fileobj(
            Bucket=CONFIG["s3"]["bucket"],
            Key=key,
            Fileobj=f,
        )
    if not downloaded_file_path.exists():
        raise FileNotFoundError(f"{downloaded_file_path=} after download not found")
    final_path = move_downloaded_app_to_main_dir(downloaded_file_path)
    logger.info(f"{func_info} to local finished")
    return final_path, final_version_str


def download_s3_app_by_key(
    s3_key: str,
) -> pathlib.Path:
    func_info = "S3 download_s3_file_by_key "
    filename = s3_key.rsplit("/", maxsplit=1)[-1]
    extension = filename.split(".")[-1]
    if extension == "apk":
        local_path = pathlib.Path(APKS_INCOMING_DIR, filename)
    elif extension == "xapk":
        local_path = pathlib.Path(XAPKS_INCOMING_DIR, filename)
    elif extension == "ipa":
        local_path = pathlib.Path(IPAS_INCOMING_DIR, filename)
    else:
        raise ValueError(f"Invalid extension: {extension}")
    logger.info(f"{func_info} {s3_key=} to local start")
    s3_client = get_s3_client()
    with open(local_path, "wb") as f:
        s3_client.download_fileobj(
            Bucket=CONFIG["s3"]["bucket"],
            Key=s3_key,
            Fileobj=f,
        )
    if extension in ["apk", "xapk", "ipa"]:
        final_path = move_downloaded_app_to_main_dir(local_path)
    else:
        final_path = local_path
    logger.info(f"{func_info} to local finished")
    return final_path


def download_app_to_local(
    store: int, store_id: str, version_str: str | None = None
) -> tuple[pathlib.Path, str | None]:
    """Return a local app package path, downloading from S3 when needed.

    If the app already exists locally, that path is returned immediately.
    Otherwise, the app is downloaded for the requested store/store_id and moved
    into the main local package directory.

    Returns:
        A tuple of (local_file_path, resolved_version_str). The version may be
        None when an existing local file is reused and no version was provided.
    """
    file_path = get_local_file_path(store, store_id)
    if file_path:
        logger.info(f"{store_id=} app already downloaded")
        return file_path, version_str
    file_path, version_str = download_app_by_store_id(
        store=store, store_id=store_id, version_str=version_str
    )
    if file_path is None:
        raise FileNotFoundError(f"{store_id=} no file found: {file_path=}")
    return file_path, version_str


def creative_exists_in_s3(md5_hash: str, extension: str) -> bool:
    """Check if a creative already exists in S3 (fast)."""

    hash_prefix = md5_hash[0:3]
    s3_key = f"creatives/raw/{hash_prefix}/{md5_hash}.{extension}"
    s3_client = get_s3_client()
    try:
        s3_client.head_object(Bucket=CONFIG["s3"]["bucket"], Key=s3_key)
        return True
    except ClientError as e:
        # Check if it's a 404 (not found) error
        if e.response["Error"]["Code"] == "404":
            return False
        # Re-raise other errors (permissions, etc.)
        raise


def get_duckdb_connection(s3_config_key: str) -> duckdb.DuckDBPyConnection:
    s3_region = CONFIG[s3_config_key]["region_name"]
    # DuckDB uses S3 endpoint url
    endpoint = get_s3_endpoint(s3_config_key)
    duckdb_con = duckdb.connect()
    if "http://" in endpoint:
        duckdb_con.execute("SET s3_use_ssl=false;")
        endpoint = endpoint.replace("http://", "")
    elif "https://" in endpoint:
        endpoint = endpoint.replace("https://", "")
    duckdb_con.execute("INSTALL httpfs; LOAD httpfs;")
    duckdb_con.execute(f"SET s3_region='{s3_region}';")
    duckdb_con.execute(f"SET s3_endpoint='{endpoint}';")
    duckdb_con.execute("SET s3_url_style='path';")
    duckdb_con.execute("SET s3_url_compatibility_mode=true;")
    duckdb_con.execute("SET threads = 3;")
    duckdb_con.execute(
        f"SET s3_access_key_id='{CONFIG[s3_config_key]['access_key_id']}';"
    )
    duckdb_con.execute(
        f"SET s3_secret_access_key='{CONFIG[s3_config_key]['secret_key']}';"
    )
    duckdb_con.execute("SET temp_directory = '/tmp/duckdb.tmp/';")
    duckdb_con.execute("SET preserve_insertion_order = false;")
    return duckdb_con


S3_CLIENTS: dict = {}
