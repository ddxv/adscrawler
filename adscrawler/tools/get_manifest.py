"""Download an APK and extract it's manifest."""

import pathlib
import os

from adscrawler.config import get_logger, MODULE_DIR

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



def extract_manifest():

    if UNZIPPED_DIR.exists():
        pathlib.Path.rmdir(UNZIPPED_DIR)

    apk = 'apks/com.zhiliaoapp.musically.apk'


    check_dirs()

    # https://apktool.org/docs/the-basics/decoding
    command = f"apktool decode {apk} -f -o apksunzipped"

    # Execute the command
    try:

        # Run the command
        result = os.system(command)


        # Print the standard output of the command
        logger.info(f"Output: {result}")




    except FileNotFoundError:
        # Handle case where apktool is not installed or not in PATH
        logger.exception("apktool not found. Please ensure it is installed and in your PATH.")

    manifest_filename = pathlib.Path(MODULE_DIR, 'apksunzipped/AndroidManifest.xml')

    with manifest_filename.open("r") as f:
        mf = f.read()

    mf


    import xml.etree.ElementTree as ET

    # Load the XML file
    tree = ET.parse(manifest_filename)
    root = tree.getroot()

# Iterate through the elements
for elem in root:
    if elem.tag == 'application':
        for app_elem in elem:
            if app_elem.tag.startswith('intent'):
                print(app_elem.tag, app_elem.attrib)  # Prints the tag and attributes of each element
    if elem.tag.startswith('uses-permission'):
        print(elem.tag, elem.attrib)  # Prints the tag and attributes of each element



from androguard.apk import APK

 get_android_manifest_xml()
