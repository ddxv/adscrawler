import os
import pathlib

import boto3
import pandas as pd

from adscrawler.config import (
    APKS_DIR,
    APKS_INCOMING_DIR,
    CONFIG,
    IPAS_DIR,
    IPAS_INCOMING_DIR,
    XAPKS_DIR,
    XAPKS_INCOMING_DIR,
    get_logger,
)
from adscrawler.packages.utils import (
    get_local_file_path,
    get_md5_hash,
    get_version,
    move_downloaded_app_to_main_dir,
    remove_tmp_files,
    unzip_apk,
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
        Bucket="adscrawler",
        Key=prefix,
        Body=file_path.read_bytes(),
        Metadata=metadata,
    )
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        logger.info(f"Uploaded {store_id} to S3")
    else:
        logger.error(f"Failed to upload {store_id} to S3")


def get_downloaded_files(extension: str) -> list[str]:
    """Get all downloaded files of a given extension."""
    if extension == "apk":
        main_dir = APKS_DIR
    elif extension == "xapk":
        main_dir = XAPKS_DIR
    elif extension == "ipa":
        main_dir = IPAS_DIR
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


def move_local_files_to_s3() -> None:
    """Upload all local files to s3.

    This is for occasional MANUAL PROCESSING.
    """
    import numpy as np

    apks = get_downloaded_files(extension="apk")
    xapks = get_downloaded_files(extension="xapk")
    ipas = get_downloaded_files(extension="ipa")

    files = (
        [{"file_type": "apk", "package_name": apk} for apk in apks]
        + [{"file_type": "xapk", "package_name": xapk} for xapk in xapks]
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
            df = get_store_id_s3_keys(store=row["store"], store_id=row["package_name"])
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

    for _, row in df.iterrows():
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


def get_store_id_s3_keys(store: int, store_id: str) -> pd.DataFrame:
    if store == 1:
        prefix = f"apks/android/{store_id}/"
    elif store == 2:
        prefix = f"apks/ios/{store_id}/"
    else:
        raise ValueError(f"Invalid store: {store}")
    logger.info(f"Getting {store_id=} s3 keys start")
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket="adscrawler", Prefix=prefix)
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


def download_app_by_store_id(store: int, store_id: str) -> pathlib.Path:
    func_info = "download_app_by_store_id "
    df = get_store_id_s3_keys(store, store_id)
    if df.empty:
        logger.error(f"{store_id=} no apk found in s3")
        raise FileNotFoundError(f"{store_id=} no apk found in s3")
    df = df[~(df["version_code"] == "failed")]
    if df.empty:
        logger.error(f"{store_id=} S3 only has failed apk, no version_code")
    df = df.sort_values(by="version_code", ascending=False)
    key = df["key"].to_numpy()[0]
    version_str = df["version_code"].to_numpy()[0]
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
            Bucket="adscrawler",
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
            Bucket="adscrawler",
            Key=s3_key,
            Fileobj=f,
        )
    if extension in ["apk", "xapk", "ipa"]:
        final_path = move_downloaded_app_to_main_dir(local_path)
    else:
        final_path = local_path
    logger.info(f"{func_info} to local finished")
    return final_path


def download_to_local(store: int, store_id: str) -> pathlib.Path | None:
    version_str = None
    file_path = get_local_file_path(store, store_id)
    if file_path:
        logger.info(f"{store_id=} already downloaded")
        return file_path
    file_path, version_str = download_app_by_store_id(store=store, store_id=store_id)
    if file_path is None:
        raise FileNotFoundError(f"{store_id=} no file found: {file_path=}")
    return file_path, version_str
