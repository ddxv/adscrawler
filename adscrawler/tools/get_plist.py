"""Download an IPA and extract it's Info.plist file."""

import argparse
import json
import os
import pathlib
import plistlib
import subprocess
import time
from typing import Self

import pandas as pd
import requests

from adscrawler.app_stores.apple import lookupby_id
from adscrawler.config import MODULE_DIR, get_logger
from adscrawler.connection import PostgresCon
from adscrawler.queries import query_apps_to_sdk_scan, upsert_details_df
from adscrawler.tools.download_ipa import download, ipatool_auth

logger = get_logger(__name__, "download_ipa")


IPAS_DIR = pathlib.Path(MODULE_DIR, "ipas/")
UNZIPPED_DIR = pathlib.Path(MODULE_DIR, "ipasunzipped/")


class IPANotFoundError(Exception):
    """Exception raised when an IPA file is not found."""

    def __init__(self: Self, message: str = "IPA file not found") -> None:
        self.message = message
        super().__init__(self.message)


def check_dirs() -> None:
    """Create if not exists for ipas directory."""
    dirs = [IPAS_DIR, UNZIPPED_DIR]
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


def unzip_ipa(ipa_path: pathlib.Path) -> None:
    if UNZIPPED_DIR.exists():
        empty_folder(UNZIPPED_DIR)
    if not ipa_path.exists():
        logger.error(f"path: {ipa_path.as_posix()} file not found")
        raise IPANotFoundError
    check_dirs()
    command = f"unzip {ipa_path.as_posix()} -d {UNZIPPED_DIR.as_posix()}"
    # Run the command
    result = os.system(command)
    # Print the standard output of the command
    logger.info(f"Unzip output: {result}")


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
        logger.warning(f"{info} contains {duplicated_columns=}")
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


def get_parsed_plist() -> tuple[str, str, pd.DataFrame]:
    payload_dir = pathlib.Path(MODULE_DIR, "ipasunzipped/Payload")
    ipa_filename = None
    for app_dir in payload_dir.glob("*"):  # '*' will match any directory inside Payload
        plist_info_path = app_dir / "Info.plist"  # Construct the path to plist.Info
        if plist_info_path.exists():
            logger.info(f"Found plist.Info at: {plist_info_path}")
            ipa_filename = pathlib.Path(plist_info_path)
    if ipa_filename is None:
        raise FileNotFoundError
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
    version = data["CFBundleVersion"]
    frameworks_df = ipa_frameworks()
    bundles_df = ipa_bundles()
    special_files_df = special_files()
    macho_df = get_macho_info(app_dir=app_dir)
    paths_df = pd.concat([frameworks_df, bundles_df, special_files_df, macho_df])
    df = pd.concat([ddf, paths_df])
    return version, plist_str, df


def ipa_frameworks() -> pd.DataFrame:
    """Check for frameworks based on directory names."""
    framework_dirs = []
    payload_dir = pathlib.Path(UNZIPPED_DIR, "Payload")
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


def ipa_bundles() -> pd.DataFrame:
    """Check for frameworks based on directory names."""
    bundle_dirs = []
    payload_dir = pathlib.Path(UNZIPPED_DIR, "Payload")
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


def special_files() -> pd.DataFrame:
    """Check for frameworks based on directory names."""
    bundle_dirs = []
    payload_dir = pathlib.Path(UNZIPPED_DIR, "Payload")
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


def download_and_unpack(store_id: str) -> str:
    # store_id = '835599320'
    r = lookupby_id(app_id=store_id)
    bundle_id: str = r["bundleId"]
    ipa_path = pathlib.Path(IPAS_DIR, f"{bundle_id}.ipa")
    download(bundle_id=bundle_id, do_redownload=False)
    unzip_ipa(ipa_path=ipa_path)
    return bundle_id


def plist_main(
    database_connection: PostgresCon, number_of_apps_to_pull: int = 20
) -> None:
    store = 2
    logger.info("Start iTunes Info.plist")
    apps = query_apps_to_sdk_scan(
        database_connection=database_connection,
        store=store,
        limit=number_of_apps_to_pull,
    )
    logger.info(f"Start iTunes Info.plist: {apps.shape=}")
    if not apps.empty:
        ipatool_auth()
    error_count = 0
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
        plist_str = ""
        ipa_path: None | pathlib.Path = None
        try:
            bundle_id = download_and_unpack(store_id=store_id)
            ipa_path = pathlib.Path(IPAS_DIR, f"{bundle_id}.ipa")
            version_str, plist_str, details_df = get_parsed_plist()
            crawl_result = 1
            logger.info(f"{store_id=} plist finished")
        except requests.exceptions.HTTPError:
            crawl_result = 3  # 404s etc
        except requests.exceptions.ConnectionError:
            crawl_result = 3  # 404s etc
        except FileNotFoundError:
            logger.exception(f"{store_id=} unable to unpack IPA or unpack failed")
            crawl_result = 2
        except IPANotFoundError:
            logger.exception(f"{store_id=} seems download failed, ipa file not found")
            crawl_result = 4
        except Exception as e:
            logger.exception(f"Unexpected error for {store_id=}: {str(e)}")
            crawl_result = -1  # Unexpected errors
        if crawl_result in [3, 4]:
            error_count += 3
        if crawl_result in [2]:
            error_count += 1
        if crawl_result == -1:
            break
        details_df["store_app"] = row.store_app
        details_df["version_code"] = version_str

        try:
            upsert_details_df(
                details_df=details_df,
                database_connection=database_connection,
                store_id=store_id,
                raw_txt_str=plist_str,
            )
        except Exception as e:
            logger.exception(f"DB INSERT ERROR for {store_id=}: {str(e)}")

        if ipa_path:
            ipa_path.unlink(missing_ok=True)


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
    """Download IPA to local directory and exit."""
    store_id = args.store_id

    download_and_unpack(store_id=store_id)


if __name__ == "__main__":
    args = parse_args()
    main(args)
