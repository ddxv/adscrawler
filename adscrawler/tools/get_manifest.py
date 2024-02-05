"""Download an APK and extract it's manifest."""

import os
import pathlib
import xml.etree.ElementTree as ET

import pandas as pd

from adscrawler.config import MODULE_DIR, get_logger

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


def extract_manifest(apk: str):
    if UNZIPPED_DIR.exists():
        pathlib.Path.rmdir(UNZIPPED_DIR)
    apk_path = f"apks/{apk}"
    # apk_path = 'apks/com.thirdgate.rts.eg.apk'
    check_dirs()
    # https://apktool.org/docs/the-basics/decoding
    command = f"apktool decode {apk_path} -f -o apksunzipped"
    # Execute the command
    try:
        # Run the command
        result = os.system(command)
        # Print the standard output of the command
        logger.info(f"Output: {result}")
    except FileNotFoundError:
        # Handle case where apktool is not installed or not in PATH
        logger.exception(
            "apktool not found. Please ensure it is installed and in your PATH."
        )


def get_parsed_manifest() -> tuple[str, pd.DataFrame]:
    manifest_filename = pathlib.Path(MODULE_DIR, "apksunzipped/AndroidManifest.xml")
    # Load the XML file
    with manifest_filename.open("r") as f:
        manifest_str = f.read()
    tree = ET.parse(manifest_filename)
    root = tree.getroot()
    df = xml_to_dataframe(root)
    return manifest_str, df


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


def main():
    apk = "com.zhiliaoapp.musically.apk"
    extract_manifest(apk)
    manifest_str, df = get_parsed_manifest()
