"""Download an APK and extract it's manifest."""

import os
import pathlib
import xml.etree.ElementTree as ET

import pandas as pd
import yaml

from adscrawler.config import MODULE_DIR, get_logger
from adscrawler.connection import PostgresCon
from adscrawler.queries import get_most_recent_top_ranks, upsert_df
from adscrawler.tools.download_apk import download

logger = get_logger(__name__)


APKS_DIR = pathlib.Path(MODULE_DIR, "apks/")
UNZIPPED_DIR = pathlib.Path(MODULE_DIR, "apksunzipped/")


def check_dirs() -> None:
    """Create if not exists for apks directory."""
    dirs = [APKS_DIR, UNZIPPED_DIR]
    for _dir in dirs:
        if not pathlib.Path.exists(_dir):
            logger.info(f"creating {_dir} directory")
            pathlib.Path.mkdir(_dir, exist_ok=True)


def empty_folder(pth):
    for sub in pth.iterdir():
        if sub.is_dir():
            empty_folder(sub)
        else:
            sub.unlink()


def unzip_apk(apk: str):
    if UNZIPPED_DIR.exists():
        empty_folder(UNZIPPED_DIR)
    apk_path = pathlib.Path(APKS_DIR, apk)
    if not apk_path.exists:
        logger.error(f"path: {apk_path.as_posix()} file not found")
        raise FileNotFoundError
    # apk_path = 'apks/com.thirdgate.rts.eg.apk'
    check_dirs()
    # https://apktool.org/docs/the-basics/decoding
    command = f"apktool decode {apk_path.as_posix()} -f -o {UNZIPPED_DIR.as_posix()}"
    # Run the command
    result = os.system(command)
    # Print the standard output of the command
    logger.info(f"Output: {result}")


def get_parsed_manifest() -> tuple[str, pd.DataFrame]:
    manifest_filename = pathlib.Path(MODULE_DIR, "apksunzipped/AndroidManifest.xml")
    # Load the XML file
    with manifest_filename.open("r") as f:
        manifest_str = f.read()
    tree = ET.parse(manifest_filename)
    root = tree.getroot()
    df = xml_to_dataframe(root)
    return manifest_str, df


def get_version() -> int:
    tool_filename = pathlib.Path(MODULE_DIR, "apksunzipped/apktool.yml")
    # Open and read the YAML file
    with tool_filename.open("r") as file:
        data = yaml.safe_load(file)
    version_int = data["versionInfo"]["versionCode"]
    return version_int


def xml_to_dataframe(root):
    """Flawed process of taking xml and making a dataframe.
    This will not have an accurate portrayal of nested XML. For example:

    <application>
        <receiver android:exported="true" android:name="com.appsflyer.MultipleInstallBroadcastReceiver">
            <intent-filter>
                <action android:name="com.android.vending.INSTALL_REFERRER"/>
            </intent-filter>
        </receiver>
    </application>

    would turn into:
                                path                     tag                android_name
    293                       application/receiver       receiver  com.appsflyer.MultipleInstallBroadcastReceiver
    294         application/receiver/intent-filter  intent-filter
    295  application/receiver/intent-filter/action         action            com.android.vending.INSTALL_REFERRER
    296                       application/receiver       receiver    com.appsflyer.SingleInstallBroadcastReceiver

    """

    def extract_data(element, path="", data=None):
        if data is None:
            data = []
        for child in element:
            tag = child.tag
            android_name = child.attrib.get(
                "{http://schemas.android.com/apk/res/android}name", ""
            )
            # Construct the new path for the current element
            new_path = f"{path}/{tag}" if path else tag
            data.append({"path": new_path, "tag": tag, "android_name": android_name})
            # Recursively extract data for child elements, passing the updated path
            extract_data(child, new_path, data)
        return data

    # Then call extract_data with the root element of your XML document
    data = extract_data(root)

    # Extract data starting from the root
    data = extract_data(root)
    # Convert the list to a pandas DataFrame
    df = pd.DataFrame(data)
    return df


def manifest_main(database_connection: PostgresCon):

    store = 1
    collection_id = 1
    apps_category_id = 1
    games_category_id = 36
    top_apps = get_most_recent_top_ranks(
        database_connection=database_connection,
        store=store,
        collection_id=collection_id,
        category_id=apps_category_id,
        limit=10,
    )
    top_games = get_most_recent_top_ranks(
        database_connection=database_connection,
        store=store,
        collection_id=collection_id,
        category_id=games_category_id,
        limit=10,
    )
    apps = pd.concat([top_apps, top_games]).drop_duplicates()
    for _id, row in apps.iterrows():
        store_id = row.store_id
        store_id = "com.einnovation.temu"
        logger.info(f"{store_id=} start")
        download(store_id=store_id, do_redownload=True)
        try:
            unzip_apk(apk=f"{store_id}.apk")
        except FileNotFoundError:
            logger.warning(f"{store_id=} unable to finish processing")
            continue
        logger.info(f"{store_id=} unzipped")
        version_int = get_version()
        manifest_str, df = get_parsed_manifest()
        df["store_app"] = row.store_app
        df["version_code"] = version_int
        version_code_df = df[["store_app", "version_code"]].drop_duplicates()
        logger.info(f"{store_id=} inserts")
        upserted = upsert_df(
            df=version_code_df,
            table_name="version_codes",
            database_connection=database_connection,
            key_columns=["store_app", "version_code"],
            return_rows=True,
            insert_columns=["store_app", "version_code"],
        )
        upserted = upserted.rename(
            columns={"version_code": "original_version_code", "id": "version_code"}
        ).drop("store_app", axis=1)
        df = df.rename(
            columns={"path": "xml_path", "version_code": "original_version_code"}
        )
        df = pd.merge(
            left=df,
            right=upserted,
            how="left",
            on=["original_version_code"],
            validate="m:1",
        )
        key_insert_columns = ["version_code", "xml_path", "tag", "android_name"]
        df = df[key_insert_columns].drop_duplicates()
        upsert_df(
            df=df,
            table_name="version_details",
            database_connection=database_connection,
            key_columns=key_insert_columns,
            insert_columns=key_insert_columns,
        )
        df["manifest_string"] = manifest_str
        manifest_df = df[["version_code", "manifest_string"]].drop_duplicates()
        upsert_df(
            df=manifest_df,
            table_name="version_manifests",
            database_connection=database_connection,
            key_columns=["version_code"],
            insert_columns=["version_code", "manifest_string"],
        )
