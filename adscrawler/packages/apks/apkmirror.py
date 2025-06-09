"""Inspiration from:
https://github.com/devlocalhost/ampy

"""

import time
from urllib.parse import quote_plus

import cloudscraper
from bs4 import BeautifulSoup

from adscrawler.config import get_logger

logger = get_logger(__name__)

APKMIRROR_BASE_URL = "https://www.apkmirror.com"
APKMIRROR_BASE_SEARCH = f"{APKMIRROR_BASE_URL}/?post_type=app_release&searchtype=apk&s="

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:139.0) Gecko/20100101 Firefox/139.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

SLEEP_PAUSES = 5


def search(scraper: cloudscraper.CloudScraper, query: str) -> list[dict]:
    search_url = APKMIRROR_BASE_SEARCH + quote_plus(query)
    resp = scraper.get(search_url, headers=USER_AGENT)

    logger.info(f"APKMIRROR search Status: {resp.status_code}")
    if resp.status_code != 200:
        logger.error(f"APKMIRROR search Status: {resp.status_code}")
        raise Exception(f"APKMIRROR search Status: {resp.status_code}")

    soup = BeautifulSoup(resp.text, "html.parser")
    apps = []
    search_content = soup.find("div", {"id": "content"})
    app_row = search_content.find_all("div", {"id": "appRow"})

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

    if "No results found matching your query" in soup.text:
        logger.info(f"No results found matching your query: {query}")
        if len(apps) == 0:
            raise Exception(f"No results found matching your query: {query}")
        else:
            logger.warning(
                f"No results found matching your query: {query}, but appRows not empty! Check"
            )

    return apps


def get_app_details(scraper: cloudscraper.CloudScraper, app_link: str) -> str:
    time.sleep(SLEEP_PAUSES)

    resp = scraper.get(app_link, headers=USER_AGENT)

    soup = BeautifulSoup(resp.text, "html.parser")

    data = soup.find_all("div", {"class": ["table-row", "headerFont"]})[1]

    try:
        download_link = (
            APKMIRROR_BASE_URL
            + data.find_all("a", {"class": "accent_color"})[0]["href"]
        )
    except Exception:
        logger.exception(f"Error getting download link for {app_link}")
        raise

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


def get_download_url(store_id: str) -> str:
    scraper = cloudscraper.create_scraper()

    results = search(scraper, query=store_id)
    if len(results) > 0:
        app_details_link = results[0]["link"]
        logger.info(f"found app details link: {results[0]}")
    if len(results) == 0:
        logger.info("No results returned")
        raise Exception("No results returned")

    app_download_link = get_app_details(scraper, app_details_link)

    direct_download_link = get_direct_download_link(scraper, app_download_link)

    time.sleep(SLEEP_PAUSES)

    return direct_download_link
