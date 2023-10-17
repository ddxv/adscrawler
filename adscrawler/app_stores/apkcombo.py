import re

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service

from adscrawler.config import get_logger

logger = get_logger(__name__)


def scrape_with_firefox() -> list[str]:
    """Open Firefox and navigate to apkcombo.com."""
    logger.info("Pull RSS feed using Selenium Firefox")
    options = Options()
    options.add_argument("--headless")

    service = Service(
        executable_path="/snap/bin/geckodriver"
    )  # specify the path to your geckodriver
    driver = webdriver.Firefox(options=options, service=service)
    driver.get("https://apkcombo.com/new-releases/feed/")
    new_apps = re.findall(r"https://apkcombo\.com/[^/]+/([^/]+)/", driver.page_source)
    driver.get("https://apkcombo.com/latest-updates/feed/")
    updated_apps = re.findall(
        r"https://apkcombo\.com/[^/]+/([^/]+)/", driver.page_source
    )
    scraped_ids = list(set(updated_apps + new_apps))
    return scraped_ids


def get_apkcombo_android_apps() -> list[dict]:
    scraped_ids = scrape_with_firefox()
    dicts = [{"store": 1, "store_id": x} for x in scraped_ids]
    return dicts


if __name__ == "__main__":
    logger.info("Start main")
    scraped_dicts = get_apkcombo_android_apps()
    logger.info(
        f"Scraped dicts length={len(scraped_dicts)} example: {scraped_dicts[0]}"
    )
