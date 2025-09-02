"""Download an IPA and extract it's Info.plist file."""

import json
import pathlib
import plistlib
import subprocess

import pandas as pd

from adscrawler.config import get_logger
from adscrawler.packages.storage import download_app_to_local
from adscrawler.packages.utils import unzip_ipa

logger = get_logger(__name__)

FAILED_VERSION_STR = "-1"


def unpack_and_attach(
    df: pd.DataFrame, column_to_unpack: str, rename_columns_dict: dict | None = None
) -> pd.DataFrame:
    info = f"Unpack column: {column_to_unpack}"
    if column_to_unpack not in df.columns:
        logger.warning(f"{info} df does not contain column")
        return df
    original_shape = df.shape
    unpacked = df[column_to_unpack].apply(pd.Series)
    if rename_columns_dict:
        unpacked = unpacked.rename(columns=rename_columns_dict)
    unpacked_columns = unpacked.columns
    duplicated_columns = [x for x in unpacked_columns if x in df.columns]
    if len(duplicated_columns) > 0:
        logger.debug(f"{info} contains {duplicated_columns=}")
    combined = pd.concat([df.drop(column_to_unpack, axis=1), unpacked], axis=1)
    combined_shape = combined.shape
    rows = combined_shape[0] - original_shape[0]
    cols = combined_shape[1] - original_shape[1]
    if rows > 0 or cols > 0:
        logger.info(f"{info} df shape changed: new rows: {rows}, new cols: {cols}")
    else:
        logger.info(f"{info} df shape not changed")
    return combined


def get_macho_info(app_dir: pathlib.Path) -> pd.DataFrame:
    try:
        result = subprocess.run(
            ["ipsw", "macho", "info", str(app_dir), "--json"],
            capture_output=True,
            text=True,
            check=True,
        )
        macho_info = json.loads(result.stdout)
        logger.info("Successfully captured macho info")
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        logger.error(f"Failed to get macho info: {e}")
        return pd.DataFrame()
    except FileNotFoundError as e:
        logger.warning(f"ipsw not installed. Macho not found. to get macho info: {e}")
        return pd.DataFrame()

    df = pd.json_normalize(macho_info["loads"])
    df = df[["name"]].drop_duplicates()
    df = df[~df["name"].isna()]
    df[["path", "extension"]] = df["name"].str.split(".", expand=True, n=1)
    df["value"] = df["path"].apply(lambda x: x.split("/")[-1])
    df["extension"] = df["extension"].str.replace("\\/.*$", "", regex=True)
    df["path"] = df["path"].apply(lambda x: "/".join(x.split("/")[:-1]))
    df["value"] = df["value"] + "." + df["extension"]
    df = df[~df["value"].isna()][["path", "value"]].drop_duplicates()
    return df


def get_parsed_plist(
    tmp_decoded_output_path: pathlib.Path,
) -> tuple[str, str, pd.DataFrame]:
    payload_dir = pathlib.Path(tmp_decoded_output_path, "Payload")
    ipa_filename = None
    for app_dir in payload_dir.glob("*"):  # '*' will match any directory inside Payload
        plist_info_path = app_dir / "Info.plist"  # Construct the path to plist.Info
        if plist_info_path.exists():
            logger.info(f"Found plist.Info at: {plist_info_path}")
            ipa_filename = pathlib.Path(plist_info_path)
    if ipa_filename is None:
        raise FileNotFoundError(f"No Info.plist found in {tmp_decoded_output_path}")
    # Load the XML file
    with ipa_filename.open("rb") as f:
        plist_bytes = f.read()
    data = plistlib.loads(plist_bytes)
    plist_str = str(data)
    df = (
        pd.json_normalize(data, sep="/")
        .T.explode(0)
        .reset_index()
        .rename(columns={"index": "path", 0: "value"})
    )
    ddf = unpack_and_attach(
        df, column_to_unpack="value", rename_columns_dict={0: "value"}
    )
    ddf["value"] = (
        ddf[[x for x in ddf.columns if x != "path"]]
        .fillna("")
        .apply(lambda row: "".join([str(x) for x in row]), axis=1)
    )
    ddf = ddf[["path", "value"]]
    version_id = data["CFBundleVersion"]
    version_str = data["CFBundleShortVersionString"]
    if version_id == "0" and version_str:
        version_id = version_str
    frameworks_df = ipa_frameworks(tmp_decoded_output_path)
    bundles_df = ipa_bundles(tmp_decoded_output_path)
    special_files_df = special_files(tmp_decoded_output_path)
    macho_df = get_macho_info(app_dir=app_dir)
    paths_df = pd.concat([frameworks_df, bundles_df, special_files_df, macho_df])
    df = pd.concat([ddf, paths_df])
    df["tag"] = ""
    df = df.rename(columns={"value": "value_name"})
    return version_id, plist_str, df


def ipa_frameworks(tmp_decoded_output_path: pathlib.Path) -> pd.DataFrame:
    """Check for frameworks based on directory names."""
    framework_dirs = []
    payload_dir = pathlib.Path(tmp_decoded_output_path, "Payload")
    if payload_dir.exists():
        for (
            app_dir
        ) in payload_dir.iterdir():  # Assuming the next directory is the app directory
            frameworks_path = app_dir / "Frameworks"
            if frameworks_path.exists() and frameworks_path.is_dir():
                for framework_dir in frameworks_path.iterdir():
                    if framework_dir.is_dir():  # Add only directories
                        framework_dirs.append(framework_dir.name)
                    elif framework_dir.is_file() and framework_dir.name.endswith(
                        ".dylib"
                    ):
                        framework_dirs.append(framework_dir.name)
    df = pd.DataFrame({"path": "frameworks", "value": framework_dirs})
    return df


def ipa_bundles(tmp_decoded_output_path: pathlib.Path) -> pd.DataFrame:
    """Check for frameworks based on directory names."""
    bundle_dirs = []
    payload_dir = pathlib.Path(tmp_decoded_output_path, "Payload")
    if payload_dir.exists():
        for (
            app_dir
        ) in payload_dir.iterdir():  # Assuming the next directory is the app directory
            for mydir in app_dir.iterdir():
                if mydir.is_dir() and mydir.name.endswith(
                    ".bundle"
                ):  # Add only directories
                    bundle_dirs.append(mydir.name)
    df = pd.DataFrame({"path": "bundles", "value": bundle_dirs})
    return df


def special_files(tmp_decoded_output_path: pathlib.Path) -> pd.DataFrame:
    """Check for frameworks based on directory names."""
    bundle_dirs = []
    payload_dir = pathlib.Path(tmp_decoded_output_path, "Payload")
    if payload_dir.exists():
        for (
            app_dir
        ) in payload_dir.iterdir():  # Assuming the next directory is the app directory
            for mydir in app_dir.iterdir():
                if mydir.is_dir() and mydir.name.endswith(
                    "cookeddata"
                ):  # Add only directories
                    bundle_dirs.append(mydir.name)
                elif mydir.is_file() and mydir.name.endswith(
                    "ue4commandline.txt"
                ):  # Add only directories
                    bundle_dirs.append(mydir.name)
    df = pd.DataFrame({"path": "Payload", "value": bundle_dirs})
    return df


def process_plist(
    store_id: str,
) -> tuple[pd.DataFrame, int, str, str]:
    store = 2
    downloaded_file_path, version_str = download_app_to_local(
        store=store, store_id=store_id
    )
    tmp_decoded_output_path = unzip_ipa(
        ipa_path=downloaded_file_path, store_id=store_id
    )
    version_str, plist_str, details_df = get_parsed_plist(
        tmp_decoded_output_path=tmp_decoded_output_path
    )
    crawl_result = 1
    return details_df, crawl_result, version_str, plist_str
