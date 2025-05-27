import pandas as pd
import boto3
import os
import pathlib

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


def get_s3_client() -> boto3.client:
    """Create and return an S3 client."""
    session = boto3.session.Session()
    return session.client(
        "s3",
        region_name="garage",
        endpoint_url=CONFIG["cloud"]["endpoint_url"],
        aws_access_key_id=CONFIG["cloud"]["access_key_id"],
        aws_secret_access_key=CONFIG["cloud"]["secret_key"],
    )


def move_local_files_to_s3():
    s3_client = get_s3_client()

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


if __name__ == "__main__":
    move_local_files_to_s3()
