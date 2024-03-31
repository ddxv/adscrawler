"""Download IPA files.
This was modified from this GIST:
https://gist.github.com/spawn9275/6303053b0fae58d5b777e2c6d9192a2d

CONFIG will expect ios: email, password, mac_address
"""

import glob

import requests
import wget
import xmltodict

from adscrawler.config import CONFIG, get_logger

logger = get_logger(__name__)


# Replace with your email and password
EMAIL = CONFIG["apple"]["email"]
PASSWORD = CONFIG["apple"]["password"]

# MAC = 'XX:XX:XX:XX:XX:XX' # Replace with MAC address from your iPhone
GUID = CONFIG["apple"]["mac_address"].replace(":", "")

app_id = "922558758"  # You can get this from https://apps.apple.com/us/app/{app-name}/id{app_id}


def download(app_id: str) -> None:
    app_ver = "0"  # You can leave as 0, I used the value I got from the HTTP request

    # We need to create a persisent session to keep cookies across requests
    # https://stackoverflow.com/questions/12737740/python-requests-and-persistent-sessions
    saved_session = requests.Session()

    auth_params = {
        "appleId": CONFIG["apple"]["email"],
        "password": PASSWORD,
        "attempt": "4",
        "createSession": "true",
        "guid": GUID,
        "rmp": "0",
        "why": "signIn",
    }

    auth_headers = {
        "Accept": "*/*",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Configurator/2.0 (Macintosh; OS X 10.12.6; 16G29) AppleWebKit/2603.3.8",
    }

    auth_url = (
        "https://p25-buy.itunes.apple.com/WebObjects/MZFinance.woa/wa/authenticate?guid=%s"
        % GUID
    )

    # Request it twice, the first one will result in a 302 response
    auth = saved_session.post(url=auth_url, headers=auth_headers, data=auth_params)
    auth = saved_session.post(url=auth_url, headers=auth_headers, data=auth_params)

    # Parse as XML
    dict_data = xmltodict.parse(auth.content)

    # For strictly downloading, these 3 values aren't necessary, only DSID is.
    # However, if you want to implement purchase, you need password_token and store_front
    dsid = dict_data["plist"]["dict"]["string"][4]

    # password_token = dict_data['plist']['dict']['string'][1]
    # store_front = auth.headers.get('x-set-apple-store-front')

    dl_url = "https://p25-buy.itunes.apple.com/WebObjects/MZFinance.woa/wa/volumeStoreDownloadProduct"

    dl_body = {
        "creditDisplay": "",
        "guid": GUID,
        "salableAdamId": app_id,
        "appExtVrsId": app_ver,
    }

    dl_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Configurator/2.0 (Macintosh; OS X 10.12.6; 16G29) AppleWebKit/2603.3.8",
        "X-Dsid": dsid,
    }

    dl_params = {"guid": GUID}

    dl = saved_session.post(
        url=dl_url, headers=dl_headers, params=dl_params, data=dl_body
    )

    # https://stackoverflow.com/questions/2148119/how-to-convert-an-xml-string-to-a-dictionary
    dl_dict = xmltodict.parse(dl.content)

    # Retrieve the IPA URL from the XML we parsed
    ipa = dl_dict["plist"]["dict"]["array"][1]["dict"]["string"][0]

    # https://stackoverflow.com/questions/24346872/python-equivalent-of-a-given-wget-command
    # https://stackoverflow.com/questions/3964681/find-all-files-in-a-directory-with-extension-txt-in-python
    # If the .ipa isn't installed yet, this will download it
    if not glob.glob("*.ipa"):
        wget.download(ipa)
