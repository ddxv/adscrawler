"""Download an APK and extract it's manifest."""

import json
import pathlib
import subprocess
from xml.etree import ElementTree

import pandas as pd

from adscrawler.config import (
    APK_TMP_UNZIPPED_DIR,
    get_logger,
)
from adscrawler.packages.storage import download_app_to_local
from adscrawler.packages.utils import (
    get_version,
    unzip_apk,
)

logger = get_logger(__name__, "process_sdks")

FAILED_VERSION_STR = "-1"


def get_parsed_manifest(
    apk_tmp_decoded_output_path: pathlib.Path, store_id: str
) -> tuple[str, pd.DataFrame]:
    manifest_filename = pathlib.Path(apk_tmp_decoded_output_path, "AndroidManifest.xml")
    # Load the XML file
    with manifest_filename.open("r") as f:
        manifest_str = f.read()
    tree = ElementTree.parse(manifest_filename)
    root = tree.getroot()
    df = xml_to_dataframe(root)
    smali_df = get_smali_df(apk_tmp_decoded_output_path, store_id)
    jsons_df = get_json_df(apk_tmp_decoded_output_path)
    df = pd.concat([df, smali_df, jsons_df])
    df = df.drop_duplicates()
    return manifest_str, df


def get_json_df(apk_tmp_decoded_output_path: pathlib.Path) -> pd.DataFrame:
    """Collect jsons in /res/raw directory."""
    json_df = pd.DataFrame()
    res_path = pathlib.Path(apk_tmp_decoded_output_path, "res", "raw")
    if not res_path.exists():
        return json_df
    raw_jsons = []
    for file in res_path.glob("*.json"):
        try:
            with file.open("r") as f:
                file_name = file.name.replace(".json", "")
                data = json.load(f)
                for key in data.keys():
                    store_key = {}
                    my_path = "res.raw." + file_name + "." + key
                    store_key["path"] = my_path
                    str_data = str(data[key])
                    str_data = str_data[:500]
                    store_key["found_json"] = str_data
                    raw_jsons.append(store_key)
        except Exception as e:
            logger.error(f"Error loading json file {file}: {e}")
            pass
    # flatten the list of jsons where the key is a column and the value is the value
    jsons_df = pd.DataFrame(raw_jsons)
    jsons_df = jsons_df.rename(columns={"found_json": "android_name"})
    return jsons_df


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


def get_smali_df(tmp_decoded_output_path: pathlib.Path, store_id: str) -> pd.DataFrame:
    mydf = unzipped_apk_paths(tmp_decoded_output_path)
    smali_df = mydf[mydf["path"].str.lower().str.contains("smali")].copy()
    smali_df["path"] = (
        smali_df["path"]
        .str.replace(APK_TMP_UNZIPPED_DIR.as_posix() + f"/{store_id}/", "")
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


def process_manifest(store_id: str, store: int) -> tuple[pd.DataFrame, int, str, str]:
    """Process an APKs manifest.
    Due to the original implementation, this still downloads, processes manifest and saves the version_details.
    """
    crawl_result = 3
    logger.info(f"process_manifest {store_id=} start")
    details_df = pd.DataFrame()
    version_str = None
    manifest_str = ""

    try:
        apk_path, version_str = download_app_to_local(store=store, store_id=store_id)
        if apk_path is None:
            raise FileNotFoundError(f"APK file not found for {store_id=}")

        apk_tmp_decoded_output_path = unzip_apk(store_id=store_id, file_path=apk_path)

        manifest_str, details_df = get_parsed_manifest(
            apk_tmp_decoded_output_path, store_id
        )
        apktool_info_path = pathlib.Path(apk_tmp_decoded_output_path, "apktool.yml")
        version_str = get_version(apktool_info_path)
        crawl_result = 1

    except FileNotFoundError as e:
        logger.exception(f"FileNotFoundError for {store_id=}: {str(e)}")
        crawl_result = 4  # FileNotFoundError
    except subprocess.CalledProcessError as e:
        logger.exception(f"Unexpected error for {store_id=}: {str(e)}")
        crawl_result = 3  # Unexpected error
    except Exception as e:
        logger.exception(f"Unexpected error for {store_id=}: {str(e)}")
        crawl_result = 3  # Unexpected errors
    return details_df, crawl_result, version_str, manifest_str
