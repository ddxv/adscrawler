import time
from urllib.parse import quote_plus

import cloudscraper
from bs4 import BeautifulSoup

from adscrawler.config import get_logger

logger = get_logger(__name__)

APKMIRROR_BASE_URL = "https://www.apkmirror.com"
APKMIRROR_BASE_SEARCH = f"{APKMIRROR_BASE_URL}/?post_type=app_release&searchtype=apk&s="

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0"
}

SLEEP_PAUSES = 5


def search(scraper: cloudscraper.CloudScraper, query: str) -> list[dict]:
    search_url = APKMIRROR_BASE_SEARCH + quote_plus(query)
    resp = scraper.get(search_url, headers=USER_AGENT)

    logger.info(f"[search] Status: {resp.status_code}")

    soup = BeautifulSoup(resp.text, "html.parser")
    apps = []
    app_row = soup.find_all("div", {"class": "appRow"})

    for app in app_row:
        try:
            app_dict = {
                "name": app.find("h5", {"class": "appRowTitle"}).text.strip(),
                "link": APKMIRROR_BASE_URL
                + app.find("a", {"class": "downloadLink"})["href"],
            }

            apps.append(app_dict)

        except AttributeError:
            pass

    return apps


def get_app_details(scraper: cloudscraper.CloudScraper, app_link: str) -> str:
    time.sleep(SLEEP_PAUSES)

    resp = scraper.get(app_link, headers=USER_AGENT)

    soup = BeautifulSoup(resp.text, "html.parser")

    data = soup.find_all("div", {"class": ["table-row", "headerFont"]})[1]

    download_link = (
        APKMIRROR_BASE_URL + data.find_all("a", {"class": "accent_color"})[0]["href"]
    )

    return download_link


def get_direct_download_link(
    scraper: cloudscraper.CloudScraper, app_download_url: str
) -> str:
    time.sleep(SLEEP_PAUSES)

    resp = scraper.get(app_download_url, headers=USER_AGENT)

    logger.info(f"get download landing page Status: {resp.status_code}")

    soup = BeautifulSoup(resp.text, "html.parser")

    start_download_link = APKMIRROR_BASE_URL + str(
        soup.find_all("a", {"class": "downloadButton"})[0]["href"]
    )

    time.sleep(SLEEP_PAUSES)

    resp = scraper.get(start_download_link, headers=USER_AGENT)

    logger.info(f"get direct download link Status: {resp.status_code}")

    soup = BeautifulSoup(resp.text, "html.parser")
    data = soup.find(
        "a",
        {
            "rel": "nofollow",
            "data-google-interstitial": "false",
            "href": lambda href: href
            and "/wp-content/themes/APKMirror/download.php" in href,
        },
    )["href"]

    return APKMIRROR_BASE_URL + str(data)


def get_apkmirror_download_url(store_id: str) -> str:
    scraper = cloudscraper.create_scraper()

    results = search(scraper, store_id)
    if len(results) > 0:
        app_details_link = results[0]["link"]

    app_download_link = get_app_details(scraper, app_details_link)

    direct_download_link = get_direct_download_link(scraper, app_download_link)

    return direct_download_link
