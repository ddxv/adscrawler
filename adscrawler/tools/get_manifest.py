"""Download an APK and extract it's manifest."""

import argparse
import os
import pathlib
import subprocess
import time
from xml.etree import ElementTree

import pandas as pd
import requests
import yaml

from adscrawler.config import APKS_DIR, MODULE_DIR, get_logger
from adscrawler.connection import PostgresCon
from adscrawler.queries import get_top_ranks_for_unpacking, upsert_details_df
from adscrawler.tools.download_apk import download

logger = get_logger(__name__, "download_apk")


UNZIPPED_DIR = pathlib.Path(MODULE_DIR, "apksunzipped/")


def check_dirs() -> None:
    """Create if not exists for apks directory."""
    dirs = [APKS_DIR, UNZIPPED_DIR]
    for _dir in dirs:
        if not pathlib.Path.exists(_dir):
            logger.info(f"creating {_dir} directory")
            pathlib.Path.mkdir(_dir, exist_ok=True)


def empty_folder(pth: pathlib.Path) -> None:
    for sub in pth.iterdir():
        if sub.is_dir() and not sub.is_symlink():
            empty_folder(sub)
            os.rmdir(sub)
        else:
            sub.unlink()


def unzip_apk(store_id: str, extension: str) -> None:
    apk_path = pathlib.Path(APKS_DIR, f"{store_id}{extension}")
    if UNZIPPED_DIR.exists():
        empty_folder(UNZIPPED_DIR)
    check_dirs()
    if extension == ".xapk":
        xapk_path = apk_path
        apk_path = pathlib.Path(APKS_DIR, f"{store_id}.apk")
        unzip_command = (
            f"unzip -o {xapk_path.as_posix()} {store_id}.apk -d {APKS_DIR.as_posix()}"
        )
        unzip_result = os.system(unzip_command)
        logger.info(f"Output unzipped from xapk to apk: {unzip_result}")
    if not apk_path.exists():
        logger.error(f"path: {apk_path.as_posix()} file not found")
        raise FileNotFoundError
    try:
        # https://apktool.org/docs/the-basics/decoding
        command = [
            "apktool",
            "decode",
            apk_path.as_posix(),
            "-f",
            "-o",
            UNZIPPED_DIR.as_posix(),
        ]
        # Run the command and capture output
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,  # Don't raise exception on non-zero exit
        )

        if "java.lang.OutOfMemoryError" in result.stderr:
            # Possibly related: https://github.com/iBotPeaches/Apktool/issues/3736
            logger.error("Java heap space error occurred, try with -j 1")
            # Handle the error as needed
            result = subprocess.run(
                command + ["-j", "1"],
                capture_output=True,
                text=True,
                check=False,  # Don't raise exception on non-zero exit
            )

        if result.stderr:
            logger.error(f"Error: {result.stderr}")

        # Check return code
        if result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, command)

    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with return code {e.returncode}")
        raise


def get_parsed_manifest() -> tuple[str, pd.DataFrame]:
    manifest_filename = pathlib.Path(MODULE_DIR, "apksunzipped/AndroidManifest.xml")
    # Load the XML file
    with manifest_filename.open("r") as f:
        manifest_str = f.read()
    tree = ElementTree.parse(manifest_filename)
    root = tree.getroot()
    df = xml_to_dataframe(root)
    smali_df = get_smali_df()
    df = pd.concat([df, smali_df])
    df = df.drop_duplicates()
    return manifest_str, df


def unzipped_apk_paths(mypath: pathlib.Path) -> pd.DataFrame:
    """Collect all unzipped APK paths recursively and return as a DataFrame."""
    unzipped_paths = []

    def collect_paths(directory: pathlib.Path) -> None:
        for path in directory.iterdir():
            if path.is_dir():
                unzipped_paths.append(str(path))
                collect_paths(path)  # Recursive call

    # Start collecting paths
    collect_paths(mypath)

    # Create a DataFrame
    return pd.DataFrame(unzipped_paths, columns=["path"])


def get_smali_df() -> pd.DataFrame:
    mydf = unzipped_apk_paths(UNZIPPED_DIR)
    smali_df = mydf[mydf["path"].str.lower().str.contains("smali")].copy()
    smali_df["path"] = (
        smali_df["path"]
        .str.replace(MODULE_DIR.as_posix() + "/apksunzipped/", "")
        .str.replace("smali/", "")
        .str.replace(r"smali_classes_\d+/", "", regex=True)
        .str.replace(r"smali_classes\d+/", "", regex=True)
        .str.replace(r"smali_classes\d+", "", regex=True)
        .str.replace(r"smali_assets\d+/", "", regex=True)
        .str.replace("smali_assets/", "")
        .str.replace("smali_assets", "")
        .str.replace("/", ".")
    )
    smali_df = smali_df[smali_df["path"].str.len() > 4]
    smali_df = smali_df.rename(columns={"path": "android_name"})
    smali_df["path"] = "smali"
    return smali_df


def get_version() -> str:
    tool_filename = pathlib.Path(MODULE_DIR, "apksunzipped/apktool.yml")
    # Open and read the YAML file
    with tool_filename.open("r") as file:
        data = yaml.safe_load(file)
    version = str(data["versionInfo"]["versionCode"])
    return version


def xml_to_dataframe(root: ElementTree.Element) -> pd.DataFrame:
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

    def extract_data(
        element: ElementTree.Element, path: str = "", data: list | None = None
    ) -> list:
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


def download_and_unpack(store_id: str) -> str:
    extension = download(store_id=store_id, do_redownload=False)
    unzip_apk(store_id=store_id, extension=extension)
    return extension


def manifest_main(
    database_connection: PostgresCon, number_of_apps_to_pull: int = 20
) -> None:
    error_count = 0
    store = 1
    collection_id = 1  # 'Top' Collection
    logger.info("Start APK processing")
    apps = get_top_ranks_for_unpacking(
        database_connection=database_connection,
        store=store,
        collection_id=collection_id,
        limit=number_of_apps_to_pull,
    )
    logger.info(f"Start APK processing: {apps.shape=}")
    for _id, row in apps.iterrows():
        if error_count > 5:
            continue
        if error_count > 0:
            time.sleep(error_count * error_count * 10)
        crawl_result = 4
        store_id = row.store_id
        logger.info(f"{store_id=} start")
        details_df = row.to_frame().T
        version_str = "-1"
        try:
            extension = download_and_unpack(store_id=store_id)
            manifest_str, details_df = get_parsed_manifest()
            version_str = get_version()
            crawl_result = 1
            logger.info(f"{store_id=} unzipped finished")
        except requests.exceptions.HTTPError:
            crawl_result = 3  # 404s etc
        except requests.exceptions.ConnectionError:
            crawl_result = 3  # 404s etc
        except FileNotFoundError:
            logger.exception(f"{store_id=} unable to unpack apk")
            crawl_result = 2
        except subprocess.CalledProcessError as e:
            logger.exception(f"Unexpected error for {store_id=}: {str(e)}")
            crawl_result = 4  # Unexpected error
        except Exception as e:
            logger.exception(f"Unexpected error for {store_id=}: {str(e)}")
            crawl_result = 4  # Unexpected errors
        if crawl_result in [3]:
            error_count += 3
        if crawl_result in [2, 4]:
            error_count += 1
        details_df["store_app"] = row.store_app
        if version_str:
            details_df["version_code"] = version_str
        else:
            details_df["version_code"] = "-1"
        try:
            upsert_details_df(
                details_df=details_df,
                crawl_result=crawl_result,
                database_connection=database_connection,
                store_id=store_id,
                raw_txt_str=manifest_str,
            )
        except Exception as e:
            logger.exception(f"DB INSERT ERROR for {store_id=}: {str(e)}")
        remove_apks(store_id=store_id, extension=extension)


def remove_apks(store_id: str) -> None:
    apk_path = pathlib.Path(APKS_DIR, f"{store_id}.apk")
    try:
        apk_path.unlink(missing_ok=True)
        logger.info(f"{store_id=} deleted apk {apk_path.as_posix()}")
    except FileNotFoundError:
        pass
    xapk_path = pathlib.Path(APKS_DIR, f"{store_id}.xapk")
    try:
        xapk_path.unlink(missing_ok=True)
        logger.info(f"{store_id=} deleted xapk {xapk_path.as_posix()}")
    except FileNotFoundError:
        pass


def parse_args() -> argparse.Namespace:
    """Check passed args.

    will check for command line --store-id in the form of com.example.app
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--store-id",
        help="Store id to download, ie -s 'org.moire.opensudoku'",
    )
    args, leftovers = parser.parse_known_args()
    return args


def main(args: argparse.Namespace) -> None:
    """Download APK to local directory and exit."""
    store_id = args.store_id
    download_and_unpack(store_id=store_id)


if __name__ == "__main__":
    args = parse_args()
    main(args)
