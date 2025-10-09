import os
import pathlib

import boto3
import numpy as np
import pandas as pd

from adscrawler.config import (
    APKS_DIR,
    APKS_INCOMING_DIR,
    CONFIG,
    CREATIVES_DIR,
    IPAS_DIR,
    IPAS_INCOMING_DIR,
    MITM_DIR,
    XAPKS_DIR,
    XAPKS_INCOMING_DIR,
    get_logger,
)
from adscrawler.dbcon.connection import PostgresCon, start_ssh_tunnel
from adscrawler.dbcon.queries import query_latest_api_scan_by_store_id
from adscrawler.mitm_ad_parser import mitm_logs
from adscrawler.packages.utils import (
    get_local_file_path,
    get_md5_hash,
    get_version,
    move_downloaded_app_to_main_dir,
    remove_tmp_files,
    unzip_apk,
)

logger = get_logger(__name__)


def get_s3_endpoint(key_name: str) -> str:
    host = CONFIG[key_name]["host"]
    port = None
    if CONFIG[key_name].get("use_ssh", False):
        # Use SSH tunnel for remote self hosted S3
        port = start_ssh_tunnel(key_name)
        host = "http://127.0.0.1"
    elif CONFIG[key_name].get("remote_port"):
        # Self hosted S3 on same network
        port = CONFIG["s3"]["remote_port"]
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
    global S3_CLIENT
    if S3_CLIENT is not None:
        return S3_CLIENT

    endpoint_url = get_s3_endpoint(key_name)
    session = boto3.session.Session()
    S3_CLIENT = session.client(
        "s3",
        region_name=CONFIG[key_name]["region_name"],
        endpoint_url=endpoint_url,
        aws_access_key_id=CONFIG[key_name]["access_key_id"],
        aws_secret_access_key=CONFIG[key_name]["secret_key"],
    )
    return S3_CLIENT


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
    mitm_logs.move_to_processed(store_id, run_id)


def upload_ad_creative_to_s3(
    store: int,
    adv_store_id: str,
    md5_hash: str,
    extension: str,
) -> None:
    """Upload apk to s3."""
    app_prefix = f"{adv_store_id}/{md5_hash}.{extension}"
    file_path = pathlib.Path(CREATIVES_DIR, adv_store_id, f"{md5_hash}.{extension}")
    if store == 1:
        prefix = f"ad-creatives/android/{app_prefix}"
    elif store == 2:
        prefix = f"ad-creatives/ios/{app_prefix}"
    else:
        raise ValueError(f"Invalid store: {store}")
    metadata = {
        "store_id": adv_store_id,
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
        logger.info(f"Uploaded {adv_store_id} creative to S3")
    else:
        logger.error(f"Failed to upload {adv_store_id} creative to S3")


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
        logger.info(f"Uploaded {store_id} to S3")
    else:
        logger.error(f"Failed to upload {store_id} to S3")


def get_downloaded_apk_files(extension: str) -> list[str]:
    """Get all downloaded files of a given extension."""
    if extension == "apk":
        main_dir = APKS_DIR
    elif extension == "xapk":
        main_dir = XAPKS_DIR
    elif extension == "ipa":
        main_dir = IPAS_DIR
    elif extension == "xapk-incoming":
        main_dir = XAPKS_INCOMING_DIR
        extension = "xapk"
    else:
        raise ValueError(f"Invalid extension: {extension}")
    files = []
    for file in pathlib.Path(main_dir).glob(f"*.{extension}"):
        file_name = file.stem
        # Check if the file has a 32-character MD5 hash at the end
        if len(file_name) > 33 and file_name[-33] == "_" and file_name[-32:].isalnum():
            # Remove the hash and any preceding underscore
            file_name = file_name[:-33]  # Remove hash and underscore
        files.append(file_name)
    return files


def get_downloaded_mitm_files(database_connection: PostgresCon) -> pd.DataFrame:
    """Get all downloaded files of a given extension."""

    main_dir = pathlib.Path("/home/adscrawler/adscrawler/mitmlogs/")

    store_ids = []
    for file in pathlib.Path(main_dir).glob("traffic_*.log"):
        file_name = file.stem
        store_id = file_name[8:]
        store_ids.append(store_id)

    missing_files = []
    df = query_latest_api_scan_by_store_id(store_ids, database_connection)
    missing_files = [x for x in store_ids if x not in df["store_id"].to_list()]

    logger.info(f"Uploadable: {df.shape[0]} missing: {len(missing_files)}")
    return df


def move_local_mitm_files_to_s3(database_connection: PostgresCon) -> None:
    """Upload all local mitm files to s3.

    This is for occasional MANUAL PROCESSING.
    """

    df = get_downloaded_mitm_files(database_connection)

    df = df.rename(columns={"version_code": "version_str"})

    s3_client = get_s3_client()
    for _, row in df.iterrows():
        logger.info(f"{_}/{df.shape[0]}")
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
            logger.error(f"Failed to upload {store_id} to S3")
        os.system(f"mv {file_path} /home/adscrawler/completed-mitm-logs/{store_id}.log")


def move_local_apk_files_to_s3() -> None:
    """Upload all local apk/ipa/xapk files to s3.

    This is for occasional MANUAL PROCESSING.
    """

    apks = get_downloaded_apk_files(extension="apk")
    xapks = get_downloaded_apk_files(extension="xapk")
    xapks_incoming = get_downloaded_apk_files(extension="xapk-incoming")
    ipas = get_downloaded_apk_files(extension="ipa")

    files = (
        [{"file_type": "apk", "package_name": apk} for apk in apks]
        + [{"file_type": "xapk", "package_name": xapk} for xapk in xapks]
        + [
            {"file_type": "xapk", "package_name": xapk_incoming}
            for xapk_incoming in xapks_incoming
        ]
        + [{"file_type": "ipa", "package_name": ipa} for ipa in ipas]
    )
    fdf = pd.DataFrame(files)

    fdf["store"] = np.where(fdf["file_type"] == "ipa", 2, 1)

    s3_client = get_s3_client()
    missing_files = pd.DataFrame()
    failed_files = pd.DataFrame()
    for _, row in fdf.iterrows():
        logger.info(f"{_}/{fdf.shape[0]}")
        df = pd.DataFrame()
        try:
            df = get_store_id_apk_s3_keys(
                store=row["store"], store_id=row["package_name"]
            )
        except Exception:
            logger.exception(f"Failed to get {row['package_name']} s3 keys")
            failed_files = pd.concat([failed_files, row])
            continue
        if df.empty:
            logger.info(f"Not in S3: {row['package_name']}")
            missing_files = pd.concat([missing_files, row])
            continue
        elif df[~(df["version_code"] == "failed")].empty:
            logger.info(f"One Failed version_code found for {row['package_name']}")
            failed_files = pd.concat([failed_files, row])
            continue
        else:
            continue

    logger.info(f"Missing files: {missing_files.shape[0]}")
    logger.info(f"Failed files: {failed_files.shape[0]}")

    for _, row in fdf.iterrows():
        logger.info(f"Processing {row['package_name']}")
        store_id = row.package_name
        extension = row.file_type
        if extension == "apk":
            file_path = pathlib.Path(APKS_DIR, f"{row['package_name']}.{extension}")
            store_name = "android"
        elif extension == "xapk":
            file_path = pathlib.Path(XAPKS_DIR, f"{row['package_name']}.{extension}")
            store_name = "android"
        elif extension == "ipa":
            file_path = pathlib.Path(IPAS_DIR, f"{row['package_name']}.{extension}")
            store_name = "ios"
        else:
            raise ValueError(f"Invalid extension: {extension}")
        try:
            apk_tmp_decoded_output_path = unzip_apk(
                store_id=store_id, file_path=file_path
            )
            apktool_info_path = pathlib.Path(apk_tmp_decoded_output_path, "apktool.yml")
            version_str = get_version(apktool_info_path)
            md5_hash = get_md5_hash(file_path)
            s3_key = f"apks/{store_name}/{store_id}/{version_str}/{store_id}_{md5_hash}.{extension}"
            metadata = {
                "store_id": store_id,
                "version_code": version_str,
                "md5": md5_hash,
            }
        except Exception:
            logger.exception(f"Failed to process {store_id}")
            s3_key = f"apks/{store_name}/{store_id}/failed/{store_id}.{extension}"
            metadata = {"store_id": store_id, "version_code": "failed", "md5": "failed"}
        s3_client = get_s3_client()
        response = s3_client.put_object(
            Bucket=CONFIG["s3"]["bucket"],
            Key=s3_key,
            Body=file_path.read_bytes(),
            Metadata=metadata,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            logger.info(f"Uploaded {store_id} to S3")
        else:
            logger.error(f"Failed to upload {store_id} to S3")
        remove_tmp_files(store_id)
        os.system(
            f"mv {file_path} /home/james/apk-files/{extension}s-tmp/{store_id}.{extension}"
        )


def get_app_creatives_s3_keys(store: int, store_id: str) -> pd.DataFrame:
    if store == 1:
        prefix = f"ad-creatives/android/{store_id}/"
    elif store == 2:
        prefix = f"ad-creatives/ios/{store_id}/"
    else:
        raise ValueError(f"Invalid store: {store}")
    logger.info(f"Getting {store_id=} s3 keys start")
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket=CONFIG["s3"]["bucket"], Prefix=prefix)
    objects_data = []
    if response["KeyCount"] == 0:
        logger.error(f"{store_id=} no creatives found in s3")
        raise FileNotFoundError(f"{store_id=} no creatives found in s3")
    for obj in response["Contents"]:
        key_parts = obj["Key"].split("/")[-1].split(".")
        md5_hash = key_parts[0]
        extension = key_parts[1]
        objects_data.append(
            {
                "key": obj["Key"],
                "store_id": store_id,
                "md5_hash": md5_hash,
                "extension": extension,
                "last_modified": obj["LastModified"],
            }
        )
    df = pd.DataFrame(objects_data)
    logger.info(f"Got {store_id=} s3 keys: {df.shape[0]}")
    return df


def get_store_id_mitm_s3_keys(store_id: str) -> pd.DataFrame:
    store = 1
    if store == 1:
        prefix = f"mitm/android/{store_id}/"
    elif store == 2:
        prefix = f"mitm/ios/{store_id}/"
    else:
        raise ValueError(f"Invalid store: {store}")
    logger.info(f"S3 getting {store_id=} mitm logs start")
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket=CONFIG["s3"]["bucket"], Prefix=prefix)
    objects_data = []
    if response["KeyCount"] == 0:
        msg = f"{store_id=} no mitm logs found in s3"
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
    logger.info(f"S3 found {store_id=} mitm logs: {df.shape[0]}")
    return df


def get_store_id_apk_s3_keys(store: int, store_id: str) -> pd.DataFrame:
    if store == 1:
        prefix = f"apks/android/{store_id}/"
    elif store == 2:
        prefix = f"apks/ios/{store_id}/"
    else:
        raise ValueError(f"Invalid store: {store}")
    logger.info(f"Getting {store_id=} s3 keys start")
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket=CONFIG["s3"]["bucket"], Prefix=prefix)
    objects_data = []
    if response["KeyCount"] == 0:
        logger.error(f"{store_id=} no apk found in s3")
        raise FileNotFoundError(f"{store_id=} no apk found in s3")
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
    logger.info(f"Got {store_id=} s3 keys: {df.shape[0]}")
    return df


def download_mitm_log_by_store_id(store_id: str) -> pathlib.Path:
    func_info = "download_mitm_log_by_store_id "
    df = get_store_id_mitm_s3_keys(store_id)
    if df.empty:
        logger.error(f"{store_id=} no apk found in s3")
        raise FileNotFoundError(f"{store_id=} no apk found in s3")
    df["run_id"] = df["run_id"].astype(int)
    df = df.sort_values(by="run_id").tail(1)
    key = df["key"].to_numpy()[0]
    run_id = df["run_id"].to_numpy()[0]
    filename = f"{store_id}_{run_id}.log"
    downloaded_file_path = download_mitm_log_by_key(key, filename)
    logger.info(f"{func_info} to local finished")
    return downloaded_file_path


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
    func_info = f"download_app_by_store_id {store_id=} {version_str=}"
    df = get_store_id_apk_s3_keys(store, store_id)
    if df.empty:
        logger.error(f"{store_id=} no apk found in s3")
        raise FileNotFoundError(f"{store_id=} no apk found in s3")
    df = df[~(df["version_code"] == "failed")]
    if df.empty:
        logger.error(f"{store_id=} S3 only has failed apk, no version_code")
    if version_str:
        df = df[df["version_code"] == version_str]
    else:
        df = df.sort_values(by="version_code", ascending=False)
        version_str: str = df["version_code"].to_numpy()[0]
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
    return final_path, version_str


def download_s3_app_by_key(
    s3_key: str,
) -> pathlib.Path:
    func_info = "download_s3_file_by_key "
    filename = s3_key.split("/")[-1]
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
) -> tuple[pathlib.Path, str]:
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


S3_CLIENT = None
