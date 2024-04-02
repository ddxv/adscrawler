"""Download an IPA and extract it's Info.plist file."""

import argparse
import os
import pathlib
from xml.etree import ElementTree

import pandas as pd
import requests
import numpy as np

from adscrawler.config import MODULE_DIR, get_logger
from adscrawler.connection import PostgresCon
from adscrawler.app_stores.apple import lookupby_id
from adscrawler.queries import get_most_recent_top_ranks, upsert_df
from adscrawler.tools.download_ipa import download

logger = get_logger(__name__)


IPAS_DIR = pathlib.Path(MODULE_DIR, "ipas/")
UNZIPPED_DIR = pathlib.Path(MODULE_DIR, "ipasunzipped/")


def check_dirs() -> None:
    """Create if not exists for ipas directory."""
    dirs = [IPAS_DIR, UNZIPPED_DIR]
    for _dir in dirs:
        if not pathlib.Path.exists(_dir):
            logger.info(f"creating {_dir} directory")
            pathlib.Path.mkdir(_dir, exist_ok=True)


def empty_folder(pth: pathlib.Path) -> None:
    for sub in pth.iterdir():
        if sub.is_dir():
            empty_folder(sub)
            os.rmdir(sub)
        else:
            sub.unlink()


def unzip_ipa(ipa_path: pathlib.Path) -> None:
    if UNZIPPED_DIR.exists():
        empty_folder(UNZIPPED_DIR)
    if not ipa_path.exists():
        logger.error(f"path: {ipa_path.as_posix()} file not found")
        raise FileNotFoundError
    check_dirs()
    command = f"unzip {ipa_path.as_posix()} -d {UNZIPPED_DIR.as_posix()}"
    # Run the command
    result = os.system(command)
    # Print the standard output of the command
    logger.info(f"Output: {result}")


def get_parsed_plist() -> tuple[str, pd.DataFrame]:
    payload_dir = pathlib.Path(MODULE_DIR, "ipasunzipped/Payload")
    for app_dir in payload_dir.glob("*"):  # '*' will match any directory inside Payload
        plist_info_path = app_dir / "Info.plist"  # Construct the path to plist.Info
        if plist_info_path.exists():
            logger.info(f"Found plist.Info at: {plist_info_path}")
            # Add your processing logic here
            ipa_filename = pathlib.Path(plist_info_path)
    # Load the XML file
    with ipa_filename.open("r") as f:
        plist_str = f.read()
    tree = ElementTree.parse(ipa_filename)
    root = tree.getroot()
    version_int = get_version_number(root)
    df = ipa_xml_to_dataframe(root)
    frameworks_df = ipa_frameworks()
    df = pd.concat([df,frameworks_df])
    return version_int, plist_str, df

def get_version_number(root:ElementTree) -> str:
    cf_bundle_version = ""
    for dict_element in root.findall('dict'):
        elements = list(dict_element)  # Convert the iterator to a list to use indexes
        for i, element in enumerate(elements):
            if element.tag == 'key' and element.text == 'CFBundleVersion':
                # Assuming the next element after the key contains the value
                cf_bundle_version = elements[i + 1].text
                print('Found CFBundleVersion:', cf_bundle_version)
                break
    logger.info(f'CFBundleVersion: {cf_bundle_version}')
    def version_to_integer(version_str):
            # Split the version string into its major, minor, and patch components
            parts = version_str.split('.')
            if len(parts) != 3:
                raise ValueError("Version string must have three parts separated by dots (e.g., '1.2.3')")

            # Convert each part to an integer
            major, minor, patch = map(int, parts)

            # Calculate the unique integer representation
            # Assuming the maximum is 99 for major, 999 for minor, and 999 for patch as discussed
            # These can be adjusted based on the actual expected range of version numbers
            return major * 1000000 + minor * 1000 + patch
    try:
        version_int = int(cf_bundle_version)
    except Exception:
        version_int = version_to_integer(cf_bundle_version)
    return version_int


def ipa_frameworks()->pd.DataFrame:
    """Check for frameworks based on directory names."""
    framework_dirs = []
    payload_dir = pathlib.Path(UNZIPPED_DIR, 'Payload')
    if payload_dir.exists():
        for app_dir in payload_dir.iterdir():  # Assuming the next directory is the app directory
            frameworks_path = app_dir / 'Frameworks'
            if frameworks_path.exists() and frameworks_path.is_dir():
                for framework_dir in frameworks_path.iterdir():
                    if framework_dir.is_dir():  # Add only directories
                        framework_dirs.append(framework_dir.name)
    df = pd.DataFrame({'path':"frameworks", 'value':framework_dirs})
    return df

def ipa_xml_to_dataframe(root: ElementTree.Element) -> pd.DataFrame:
    """Flawed process of taking xml and making a dataframe.
    This will not have an accurate portrayal of nested XML.

    """

    def extract_data(element: ElementTree.Element, path: str = "") -> list:
        data = []
        if element.text and element.text.strip():
            data.append({"path": path, "value": element.text.strip()})
        for child in element:
            tag = child.tag
            # Construct the new path for the current element
            new_path = f"{path}/{tag}" if path else tag
            # Recursively extract data for child elements, passing the updated path
            data.extend(extract_data(child, new_path))
        return data

    # Then call extract_data with the root element of your XML document
    data = extract_data(root)

    # Convert the list to a pandas DataFrame
    df = pd.DataFrame(data)
    df = df.drop_duplicates()
    return df


def download_and_unpack(store_id: str) -> None:
    r = lookupby_id(app_id=store_id)
    bundle_id = r["bundleId"]
    ipa_path = pathlib.Path(IPAS_DIR, f"{bundle_id}.ipa")
    download(bundle_id=bundle_id, do_redownload=False)
    unzip_ipa(ipa_path=ipa_path)


def plist_main(
    database_connection: PostgresCon, number_of_apps_to_pull: int = 20
) -> None:
    store = 2
    collection_id = 4  # 'Top' Collection
    logger.info("Start iTunes Info.plist")
    apps = get_most_recent_top_ranks(
        database_connection=database_connection,
        store=store,
        collection_id=collection_id,
        limit=number_of_apps_to_pull,
    )
    logger.info(f"Start iTunes Info.plist: {apps.shape=}")
    for _id, row in apps.iterrows():
        crawl_result = 4
        version_int = -1
        store_id = row.store_id
        logger.info(f"{store_id=} start")
        details_df = row.to_frame().T
        try:
            bundle_id = download_and_unpack(store_id=store_id)
            ipa_path = pathlib.Path(IPAS_DIR, f"{bundle_id}.ipa")
            version_int, plist_str, details_df = get_parsed_plist()
            crawl_result = 1
            logger.info(f"{store_id=} unzipped finished")
        except requests.exceptions.HTTPError:
            crawl_result = 3  # 404s etc
        except requests.exceptions.ConnectionError:
            crawl_result = 3  # 404s etc
        except FileNotFoundError:
            logger.exception(f"{store_id=} unable to unpack ipa")
            crawl_result = 2
        except Exception as e:
            logger.exception(f"Unexpected error for {store_id=}: {str(e)}")
            crawl_result = 4  # Unexpected errors
        details_df["store_app"] = row.store_app
        details_df["version_code"] = version_int
        version_code_df = details_df[["store_app", "version_code"]].drop_duplicates()
        version_code_df["crawl_result"] = crawl_result
        logger.info(f"{store_id=} inserts")
        upserted: pd.DataFrame = upsert_df(
            df=version_code_df,
            table_name="version_codes",
            database_connection=database_connection,
            key_columns=["store_app", "version_code"],
            return_rows=True,
            insert_columns=["store_app", "version_code", "crawl_result"],
        )
        ipa_path.unlink(missing_ok=True)
        if crawl_result != 1:
            continue
        upserted = upserted.rename(
            columns={"version_code": "original_version_code", "id": "version_code"}
        ).drop("store_app", axis=1)
        details_df = details_df.rename(
            columns={"path": "xml_path", "version_code": "original_version_code", "value":"value_name"}
        )
        details_df = pd.merge(
            left=details_df,
            right=upserted,
            how="left",
            on=["original_version_code"],
            validate="m:1",
        )
        details_df['tag'] = np.nan
        key_insert_columns = ["version_code", "xml_path", "tag", "value_name"]
        details_df = details_df[key_insert_columns].drop_duplicates()
        upsert_df(
            df=details_df,
            table_name="version_details",
            database_connection=database_connection,
            key_columns=key_insert_columns,
            insert_columns=key_insert_columns,
        )
        details_df["manifest_string"] = plist_str
        manifest_df = details_df[["version_code", "manifest_string"]].drop_duplicates()
        upsert_df(
            df=manifest_df,
            table_name="version_manifests",
            database_connection=database_connection,
            key_columns=["version_code"],
            insert_columns=["version_code", "manifest_string"],
        )


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
