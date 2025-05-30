import os
import pathlib

import boto3
import pandas as pd

from adscrawler.apks.download_apk import move_apk_to_main_dir
from adscrawler.apks.process_apk import (
    get_downloaded_apks,
    get_downloaded_xapks,
    get_local_apk_path,
    get_md5_hash,
    get_version,
    remove_tmp_files,
    unzip_apk,
)
from adscrawler.config import (
    APKS_DIR,
    APKS_INCOMING_DIR,
    CONFIG,
    XAPKS_DIR,
    XAPKS_INCOMING_DIR,
    get_logger,
)

logger = get_logger(__name__)


def start_tunnel(server_name: str) -> str:
    from sshtunnel import SSHTunnelForwarder

    ssh_port = CONFIG[server_name].get("ssh_port", 22)
    remote_port = CONFIG[server_name].get("remote_port", 5432)
    server = SSHTunnelForwarder(
        (CONFIG[server_name]["host"], ssh_port),
        ssh_username=CONFIG[server_name]["os_user"],
        remote_bind_address=("127.0.0.1", remote_port),
        ssh_pkey=CONFIG[server_name].get("ssh_pkey"),
        ssh_private_key_password=CONFIG[server_name].get("ssh_pkey_password"),
    )
    server.start()
    db_port = str(server.local_bind_port)
    return db_port


def get_s3_client() -> boto3.client:
    server_name = "loki"
    """Create and return an S3 client."""
    if CONFIG["loki"]["host"].startswith("192.168"):
        # On local network connect directly
        host = f"http://{CONFIG['loki']['host']}"
        db_port = CONFIG["loki"]["remote_port"]
    else:
        # SSH port forwarded
        host = "http://127.0.0.1"
        db_port = start_tunnel(server_name)
    session = boto3.session.Session()
    return session.client(
        "s3",
        region_name="garage",
        endpoint_url=f"{host}:{db_port}",
        aws_access_key_id=CONFIG["cloud"]["access_key_id"],
        aws_secret_access_key=CONFIG["cloud"]["secret_key"],
    )


def upload_apk_to_s3(
    store_id: str,
    extension: str,
    md5_hash: str,
    version_str: str,
    file_path: pathlib.Path,
) -> None:
    s3_key = f"apks/android/{store_id}/{version_str}/{store_id}_{md5_hash}.{extension}"
    metadata = {
        "store_id": store_id,
        "version_code": version_str,
        "md5": md5_hash,
    }
    # s3_key = f"apks/android/{store_id}/failed/{store_id}.{extension}"
    # metadata = {"store_id": store_id, "version_code": "failed", "md5": "failed"}
    response = S3_CLIENT.put_object(
        Bucket="adscrawler",
        Key=s3_key,
        Body=file_path.read_bytes(),
        Metadata=metadata,
    )
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        logger.info(f"Uploaded {store_id} to S3")
    else:
        logger.error(f"Failed to upload {store_id} to S3")


def move_local_files_to_s3() -> None:
    apks = get_downloaded_apks()
    xapks = get_downloaded_xapks()

    files = [{"file_type": "apk", "package_name": apk} for apk in apks] + [
        {"file_type": "xapk", "package_name": xapk} for xapk in xapks
    ]
    df = pd.DataFrame(files)

    for _, row in df.iterrows():
        logger.info(f"Processing {row['package_name']}")
        store_id = row.package_name
        extension = row.file_type
        if extension == "apk":
            file_path = pathlib.Path(APKS_DIR, f"{row['package_name']}.{extension}")
        elif extension == "xapk":
            file_path = pathlib.Path(XAPKS_DIR, f"{row['package_name']}.{extension}")
        else:
            raise ValueError(f"Invalid extension: {extension}")
        try:
            apk_tmp_decoded_output_path = unzip_apk(
                store_id=store_id, file_path=file_path
            )
            apktool_info_path = pathlib.Path(apk_tmp_decoded_output_path, "apktool.yml")
            version_str = get_version(apktool_info_path)
            md5_hash = get_md5_hash(file_path)
            s3_key = f"apks/android/{store_id}/{version_str}/{store_id}_{md5_hash}.{extension}"
            metadata = {
                "store_id": store_id,
                "version_code": version_str,
                "md5": md5_hash,
            }
        except Exception:
            logger.exception(f"Failed to process {store_id}")
            s3_key = f"apks/android/{store_id}/failed/{store_id}.{extension}"
            metadata = {"store_id": store_id, "version_code": "failed", "md5": "failed"}
        response = S3_CLIENT.put_object(
            Bucket="adscrawler",
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


def get_all_store_ids() -> list[str]:
    # Get only the common prefixes (folder-like structure)
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    prefixes = set()
    for page in paginator.paginate(
        Bucket="adscrawler",
        Prefix="apks/android/",
        Delimiter="/",  # This is the key parameter that makes it work
    ):
        if "CommonPrefixes" in page:
            prefixes.update(prefix["Prefix"] for prefix in page["CommonPrefixes"])

    store_ids = [x.replace("apks/android/", "").replace("/", "") for x in prefixes]
    return store_ids


def get_keys_with_metadata() -> pd.DataFrame:
    """
    Get all keys and their metadata from S3 bucket and return as a pandas DataFrame.
    Returns a DataFrame with columns: key, store_id, version_code, md5
    """
    paginator = S3_CLIENT.get_paginator("list_objects_v2")

    # List to store all objects with their metadata
    objects_data = []

    for page in paginator.paginate(Bucket="adscrawler", Prefix="apks/android/"):
        if "Contents" in page:
            for obj in page["Contents"]:
                # Get the object's metadata
                response = S3_CLIENT.head_object(Bucket="adscrawler", Key=obj["Key"])
                metadata = response.get("Metadata", {})

                objects_data.append(
                    {
                        "key": obj["Key"],
                        "store_id": metadata.get("store_id", "unknown"),
                        "version_code": metadata.get("version_code", "unknown"),
                        "md5": metadata.get("md5", "unknown"),
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"],
                    }
                )

    # Create DataFrame
    df = pd.DataFrame(objects_data)
    return df


def get_store_id_s3_keys(store_id: str) -> pd.DataFrame:
    logger.info(f"Getting {store_id=} s3 keys start")
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    objects_data = []
    for page in paginator.paginate(
        Bucket="adscrawler", Prefix=f"apks/android/{store_id}/"
    ):
        if "Contents" in page:
            for obj in page["Contents"]:
                # Get the object's metadata
                # response = S3_CLIENT.head_object(Bucket="adscrawler", Key=obj["Key"])
                key_parts = obj["Key"].split("/")
                if len(key_parts) >= 4:
                    version_code = key_parts[3]
                else:
                    version_code = "uknown"

                objects_data.append(
                    {
                        "key": obj["Key"],
                        "store_id": store_id,
                        "version_code": version_code,
                        "md5": key_parts[4].split(".")[0],
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"],
                    }
                )
    df = pd.DataFrame(objects_data)
    logger.info(f"Got {store_id=} s3 keys: {df.shape[0]}")
    return df


def download_s3_apk(
    s3_key: str | None = None, store_id: str | None = None
) -> pathlib.Path | None:
    if s3_key:
        key = s3_key
    elif store_id:
        df = get_store_id_s3_keys(store_id)
        if df.empty:
            logger.error(f"{store_id=} no apk found in s3")
            return None
        df = df.sort_values(by="version_code", ascending=False)
        key = df.iloc[0].key
    else:
        raise ValueError("Either s3_key or store_id must be provided")
    extension = key.split(".")[-1]
    if extension == "apk":
        local_path = pathlib.Path(APKS_INCOMING_DIR, f"{store_id}.{extension}")
    elif extension == "xapk":
        local_path = pathlib.Path(XAPKS_INCOMING_DIR, f"{store_id}.{extension}")
    else:
        raise ValueError(f"Invalid extension: {extension}")
    logger.info(f"Download {store_id}.{extension} to local start")
    S3_CLIENT.download_file(
        Bucket="adscrawler",
        Key=key,
        Filename=local_path,
    )
    move_apk_to_main_dir(local_path)
    logger.info(f"Download {store_id}.{extension} to local finished")
    return local_path


def download_to_local(store_id: str) -> pathlib.Path | None:
    apk_path = get_local_apk_path(store_id)
    if apk_path:
        logger.info(f"{store_id=} already downloaded")
        return apk_path
    apk_path = download_s3_apk(store_id=store_id)
    if apk_path is None:
        raise FileNotFoundError(f"{store_id=} no apk found: {apk_path=}")
    return apk_path


S3_CLIENT = get_s3_client()
