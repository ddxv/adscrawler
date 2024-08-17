import re
import time

import requests

from config import get_logger

logger = get_logger(__name__)

APPBRAIN_BASE_URL = "https://www.appbrain.com/apps/"

APPBRAIN_COLLECTIONS = ["hot", "hot-week", "popular", "highest-rated"]

APPBRAIN_CATEGORIES = [
    "action",
    "adventure",
    "arcade",
    "art-and-design",
    "auto-and-vehicles",
    "beauty",
    "board",
    "books-and-reference",
    "business",
    "card",
    "casino",
    "casual",
    "comics",
    "communication",
    "dating",
    "education",
    "educational",
    "entertainment",
    "events",
    "finance",
    "food-and-drink",
    "health-and-fitness",
    "house-and-home",
    "libraries-and-demo",
    "lifestyle",
    "maps-and-navigation",
    "medical",
    "music",
    "music-and-audio",
    "news-and-magazines",
    "parenting",
    "personalization",
    "photography",
    "productivity",
    "puzzle",
    "racing",
    "role-playing",
    "simulation",
    "social",
    "sports",
    "sports-games",
    "strategy",
    "tools",
    "travel-and-local",
    "trivia",
    "video-players-and-editors",
    "weather",
    "word",
]

HTML_PATTERN = r'href="/app/.*?/([^"/]+)"'


HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0",
}


def scrape_for_ids(collection: str, category: str | None = None) -> list[str]:
    if category is None:
        url_part = f"/{collection}/new"
    else:
        url_part = f"/{collection}/{category}/new"
    response = requests.get(APPBRAIN_BASE_URL + url_part, headers=HEADERS, timeout=2)
    if response.status_code == 200:
        packages = re.findall(HTML_PATTERN, response.content.decode("utf-8"))
    else:
        logger.error(
            f"Response code not 200: {response.status_code=} {response.content=}",
        )
        packages = []
        time.sleep(1)
    return packages


def loop_categories() -> list[str]:
    all_packages: list[str] = []
    for collection in APPBRAIN_COLLECTIONS:
        packages = scrape_for_ids(collection=collection)
        all_packages = list(set(packages + all_packages))
        logger.info(
            f"AppBrain {collection=} total:{len(all_packages)} packages:{len(packages)}",
        )
        time.sleep(1)
        for category in APPBRAIN_CATEGORIES:
            try:
                packages = scrape_for_ids(collection=collection, category=category)
                all_packages = list(set(packages + all_packages))
                logger.info(
                    f"AppBrain {collection=} {category=} total:{len(all_packages)} packages:{len(packages)}",
                )
            except Exception:
                logger.exception(f"AppBrain failed for {collection=} {category=}")
            time.sleep(1)
    return all_packages


def get_appbrain_android_apps() -> list[dict]:
    scraped_ids = loop_categories()
    dicts = [{"store": 1, "store_id": x} for x in scraped_ids]
    return dicts
