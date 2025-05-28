import boto3
import os
import pathlib
import pandas as pd

from adscrawler.config import CONFIG, APKS_DIR, XAPKS_DIR, get_logger

from adscrawler.apks.process_apk import (
    get_downloaded_apks,
    get_downloaded_xapks,
    unzip_apk,
    get_md5_hash,
    get_version,
    remove_tmp_files,
)

logger = get_logger(__name__)


def start_tunnel():
    from sshtunnel import SSHTunnelForwarder

    server_name = "loki"

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
    """Create and return an S3 client."""
    db_port = start_tunnel()
    session = boto3.session.Session()
    return session.client(
        "s3",
        region_name="garage",
        endpoint_url=f"http://127.0.0.1:{db_port}",
        # endpoint_url=CONFIG["cloud"]["endpoint_url"],
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
    # if extension == ".apk":
    #     file_path = pathlib.Path(APKS_INCOMING_DIR, f"{store_id}.apk")
    #     main_dir = pathlib.Path(APKS_DIR)
    # elif extension == ".xapk":
    #     file_path = pathlib.Path(XAPKS_INCOMING_DIR, f"{store_id}.xapk")
    #     main_dir = pathlib.Path(XAPKS_DIR)
    # else:
    #     raise ValueError(f"Invalid extension: {extension}")
    # if file_path.exists():
    #     shutil.move(file_path, main_dir / file_path.name)


def move_local_files_to_s3():
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


def get_store_ids():
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


def get_keys_with_metadata():
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


def get_store_id_s3_keys(store_id: str):
    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    objects_data = []
    for page in paginator.paginate(
        Bucket="adscrawler", Prefix=f"apks/android/{store_id}/"
    ):
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
    df = pd.DataFrame(objects_data)
    return df


def download_s3_apk(s3_key: str | None = None, store_id: str | None = None):
    if s3_key:
        key = s3_key
    elif store_id:
        df = get_store_id_s3_keys(store_id)
        key = df.iloc[0].key
    else:
        raise ValueError("Either s3_key or store_id must be provided")
    extension = key.split(".")[-1]
    if extension == "apk":
        local_path = pathlib.Path(APKS_DIR, f"{store_id}.{extension}")
    elif extension == "xapk":
        local_path = pathlib.Path(XAPKS_DIR, f"{store_id}.{extension}")
    else:
        raise ValueError(f"Invalid extension: {extension}")
    S3_CLIENT.download_file(
        Bucket="adscrawler",
        Key=key,
        Filename=local_path,
    )
    return local_path


S3_CLIENT = get_s3_client()


if __name__ == "__main__":
    move_local_files_to_s3()
