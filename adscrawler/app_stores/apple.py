import datetime
import random
import re
import time

import langdetect
import pandas as pd
import requests
import tldextract
from bs4 import BeautifulSoup
from itunes_app_scraper.scraper import AppStoreScraper
from itunes_app_scraper.util import AppStoreCategories, AppStoreCollections

from adscrawler.app_stores.utils import truncate_utf8_bytes
from adscrawler.config import DEVLEOPER_IGNORE_TLDS, get_logger

logger = get_logger(__name__, "scrape_apple")

ITUNES_LOOKUP_API = "https://itunes.apple.com/lookup"


def lookupby_id(app_id: str) -> dict:
    response = requests.get(ITUNES_LOOKUP_API, params={"id": app_id}, timeout=10)
    if response.status_code != 200:
        msg = f"Response code not 200: {response.text}"
        logger.error(msg)
        raise requests.HTTPError(msg)
    try:
        resp_dict: dict = response.json()["results"][0]
    except Exception as err:
        logger.exception("Unable to parse response")
        raise requests.HTTPError(err) from err
    return resp_dict


def get_app_ids_with_retry(
    scraper: AppStoreScraper, coll_value: str, cat_value: str, country: str
) -> list[str]:
    max_retries = 3
    base_delay = 2
    max_delay = 30
    retries = 0
    app_ids: list[str] = []
    while len(app_ids) == 0 and retries < max_retries:
        try:
            app_ids = scraper.get_app_ids_for_collection(
                collection=coll_value,
                category=cat_value,
                country=country,
                num=200,
                timeout=10,
            )
        except Exception as e:
            if retries == max_retries:
                raise e
            delay = min(base_delay * (2**retries) + random.uniform(0, 1), max_delay)
            logger.warning(
                f"Attempt {retries} failed. Retrying in {delay:.2f} seconds..."
            )
            time.sleep(delay)
        retries += 1
    if len(app_ids) == 0:
        logger.info(
            f"Collection: {coll_value}, category: {cat_value} failed to load apps!"
        )
    return app_ids


def scrape_ios_ranks(
    category_keyword: str | None = None,
    collection_keyword: str | None = None,
    country: str = "us",
) -> list[dict]:
    logger.info(f"Scrape iOS ranks for {collection_keyword=} {category_keyword=}")
    scraper = AppStoreScraper()
    # Eg: MAGAZINES_MEN, GAMES_ADVENTURE
    if category_keyword:
        category_keyword = category_keyword.upper()
        categories = {
            k: v
            for k, v in AppStoreCategories.__dict__.items()
            if (category_keyword in k.upper()) and (not k.startswith("__"))
        }
    else:
        categories = {
            k: v
            for k, v in AppStoreCategories.__dict__.items()
            if not k.startswith("__")
        }
    # Eg: TOP_PAID / TOP_FREE
    if collection_keyword:
        collection_keyword = collection_keyword.upper()
        collections = {
            k: v
            for k, v in AppStoreCollections.__dict__.items()
            if not k.startswith("__")
            and "_MAC" not in k
            and (collection_keyword in k.upper())
        }
    else:
        collections = {
            k: v
            for k, v in AppStoreCollections.__dict__.items()
            if not k.startswith("__") and "_MAC" not in k
        }
    ranked_dicts: list[dict] = []
    for _coll_key, coll_value in collections.items():
        logger.info(f"Collection: {_coll_key}")
        for cat_key, cat_value in categories.items():
            logger.info(f"Collection: {_coll_key}, category: {cat_key}")
            try:
                scraped_ids = get_app_ids_with_retry(
                    scraper, coll_value=coll_value, cat_value=cat_value, country=country
                )
                ranked_dicts += [
                    {
                        "store": 2,
                        "country": country,
                        "collection": _coll_key,
                        "category": cat_key,
                        "rank": rank + 1,
                        "store_id": app,
                        "crawled_date": datetime.datetime.now(
                            tz=datetime.UTC,
                        ).date(),
                    }
                    for rank, app in enumerate(scraped_ids)
                ]
            except Exception as e:
                logger.error(
                    f"Failed to scrape collection {_coll_key}, category {cat_key} after multiple retries: {str(e)}"
                )
    return ranked_dicts


def crawl_ios_developers(
    developer_db_id: int,
    developer_id: str,
    store_ids: list[str],
) -> pd.DataFrame:
    store = 2
    ios_scraper = AppStoreScraper()
    apps = ios_scraper.get_apps_for_developer(developer_id=developer_id, timeout=10)
    my_devices = ["iphone", "ipad"]
    apps = [
        x
        for x in apps
        if "supportedDevices" in x.keys()
        and any(map("".join(x["supportedDevices"]).lower().__contains__, my_devices))
    ]
    apps_df = pd.DataFrame(apps).rename(columns={"trackId": "store_id"})
    if not apps_df.empty:
        apps_df["store_id"] = apps_df["store_id"].astype(str)
        apps_df = apps_df[~apps_df["store_id"].isin(store_ids)]
        apps_df["crawl_result"] = 1
        apps_df["store"] = store
        apps_df["developer"] = developer_db_id
    return apps_df


def scrape_store_html(store_id: str, country: str) -> dict:
    """
    Scrape the store html for the developer site.
    """
    logger.info(
        f"Scrape store html for {store_id=} {country=} looking for developer site"
    )
    url = f"https://apps.apple.com/{country}/app/-/id{store_id}"
    headers = {"User-Agent": "Mozilla/5.0"}  # Prevent blocking by some websites
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Failed to retrieve the page: {response.status_code}")
        return {}

    soup = BeautifulSoup(response.text, "html.parser")
    urls = {}

    # Find all links and look for relevant ones
    for link in soup.find_all("a", href=True):
        text = link.get_text(strip=True).lower()
        href = link["href"]

        if "app support" in text:
            urls["app_support"] = href
        elif "developer" in text:
            urls["developer_site"] = href
        elif "privacy policy" in text and "apple.com" not in href:
            urls["privacy_policy"] = href
    return urls


def get_developer_url(result: dict, store_id: str, country: str) -> str:
    """
    Decide if we should crawl the store html for the developer url.
    """
    should_crawl_html = False
    if "sellerUrl" not in result.keys():
        should_crawl_html = True
    else:
        dev_tld = tldextract.extract(result["sellerUrl"])
        dev_tld_str = ".".join([dev_tld.domain, dev_tld.suffix])
        if dev_tld_str in DEVLEOPER_IGNORE_TLDS:
            should_crawl_html = True
    if should_crawl_html:
        urls = scrape_store_html(store_id, country)
        found_tlds = []
        for url_type, url in urls.items():
            print(url_type, url)
            tld = tldextract.extract(url)
            tld_str = ".".join([tld.domain, tld.suffix])
            if tld_str not in DEVLEOPER_IGNORE_TLDS and tld_str not in found_tlds:
                found_tlds.append(tld_str)
        if len(found_tlds) == 0:
            if "sellerUrl" not in result.keys():
                raise Exception(f"No developer url found for {store_id=} {country=}")
            final_url: str = result["sellerUrl"]
        elif len(found_tlds) == 1:
            final_url: str = found_tlds[0]
        else:
            logger.warning(
                f"Multiple developer sites found for {store_id=} {country=} {found_tlds=}"
            )
            final_url: str = result["sellerUrl"]
    else:
        final_url: str = result["sellerUrl"]
    return final_url


def scrape_app_ios(store_id: str, country: str, language: str) -> dict:
    # NOTE: averageUserRating, Rating_count, Histogram are country specific
    logger.info(f"Scrape app start {store_id=} {country=} {language=}")
    scraper = AppStoreScraper()
    result: dict = scraper.get_app_details(
        store_id, country=country, add_ratings=True, timeout=10, lang=language
    )
    try:
        result["sellerUrl"] = get_developer_url(result, store_id, country)
    except Exception as e:
        logger.warning(f"Failed to get developer url for {store_id=} {country=} {e}")
    logger.info(f"Scrape app finish {store_id=} {country=}")
    return result


def clean_ios_app_df(df: pd.DataFrame) -> pd.DataFrame:
    if "store_id" not in df.columns:
        df = df.rename(columns={"trackId": "store_id"})
    df = df.rename(
        columns={
            # "trackId": "store_id",
            "trackName": "name",
            "averageUserRating": "rating",
            "sellerUrl": "url",
            "minimum_OsVersion": "minimum_android",
            "primaryGenreName": "category",
            "bundleId": "bundle_id",
            "releaseDate": "release_date",
            "currentVersionReleaseDate": "store_last_updated",
            "artistId": "developer_id",
            "artistName": "developer_name",
            "userRatingCount": "rating_count",
            "artworkUrl512": "icon_url_512",
            "screenshotUrls": "phone_image_urls",
            "ipadScreenshotUrls": "tablet_image_urls",
            "languageCodesISO2A": "store_language_code",
        },
    )
    if "price" not in df.columns:
        df["price"] = 0
    try:
        # Complicated way to get around many games having very random selections like "games + shopping"
        # TODO: Just store categories as list!
        df.loc[df["category"] == "Games", "category"] = "game_" + df.loc[
            df["category"] == "Games",
            "genres",
        ].apply(
            lambda x: [
                cat.lower().replace(" ", "_")
                for cat in x.split(",")
                if cat.lower().replace(" ", "_") in GAME_CATEGORIES
            ][0],
        )
    except Exception as e:
        logger.warning(
            f"store_id={df['store_id'].to_numpy()[0]} split genre IDs failed {e}",
        )
    df = df.assign(
        free=df["price"] == 0,
        developer_id=df["developer_id"].astype(str),
        store_id=df["store_id"].astype(str),
        category=df["category"].str.lower().str.replace(" & ", "_and_"),
        store_last_updated=pd.to_datetime(df["store_last_updated"]),
        release_date=pd.to_datetime(
            df["release_date"],
            format="%Y-%m-%dT%H:%M:%SZ",
        ).dt.date,
    )
    list_cols = ["phone_image_url", "tablet_image_url"]
    for list_col in list_cols:
        urls_empty = ((df[f"{list_col}s"].isna()) | (df[f"{list_col}s"] == "")).all()
        if not urls_empty:
            columns = {x: f"{list_col}_{x + 1}" for x in range(3)}
            df = pd.concat(
                [
                    df,
                    df[f"{list_col}s"]
                    .str.split(",")
                    .apply(pd.Series)
                    .rename(columns=columns),
                ],
                axis=1,
            )
        else:
            for x in range(3):
                df[f"{list_col}_{x}"] = None
    try:
        df["histogram"] = df["user_ratings"].apply(
            lambda x: [int(num) for num in re.findall(r"\d+", x)[1::2]],
        )
    except Exception:
        logger.warning("Unable to parse histogram")
        df["histogram"] = None
    if "description" in df.columns:
        df["description"] = df["description"].apply(truncate_utf8_bytes)
    if (
        "store_language_code" in df.columns
        and df["store_language_code"].str.len().all() == 2
    ):
        df["store_language_code"] = df["store_language_code"].str.lower()
    else:
        try:
            df["store_language_code"] = df["description"].apply(
                lambda x: langdetect.detect(x)
            )
        except langdetect.lang_detect_exception.LangDetectException:
            logger.warning(
                f"Unable to detect language for {df['store_id'].to_numpy()[0]}"
            )
            # This is not a language code, so later join will fail and description keywords ignored
            df["store_language_code"] = "zz"
        df.loc[
            df["store_language_code"].str.startswith("zh-"), "store_language_code"
        ] = "zh"
    return df


def search_app_store_for_ids(
    search_term: str, country: str = "us", language: str = "en"
) -> list[str]:
    """Search store for new apps or keyword rankings."""
    logger.info("adscrawler apple search start")
    # Call the Node.js script that runs google-play-scraper
    scraper = AppStoreScraper()
    ids: list[str] = scraper.get_app_ids_for_query(
        search_term, country=country, lang=language, timeout=5, num=None
    )
    logger.info(f"adscralwer apple search {len(ids)=}")
    return ids


def app_details_for_ids(ids: list[str]) -> list[dict]:
    logger.info(f"get details for apple {len(ids)=}")
    scraper = AppStoreScraper()
    full_results = scraper.get_multiple_app_details(ids[:15])
    results = list(full_results)
    return results


GAME_CATEGORIES = [
    "arcade",
    "simulation",
    "action",
    "adventure",
    "educational",
    "role_playing",
    "racing",
    "trivia",
    "board",
    "strategy",
    "puzzle",
    "casual",
    "word",
    "card",
    "sports",
    "casino",
    "music",
]
