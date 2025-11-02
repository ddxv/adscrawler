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
    max_retries = 2
    base_delay = 1
    max_delay = 10
    retries = 0
    app_ids: list[str] = []
    log_info = f"{coll_value=} {cat_value=} {country=}"
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
                f"{log_info=} attempt {retries} failed. Retrying in {delay:.2f} seconds..."
            )
            time.sleep(delay)
        retries += 1
    if len(app_ids) == 0:
        logger.info(f"{log_info=} returned empty list of apps")
    return app_ids


def get_app_store_collections(
    collection_keyword: str | None = None,
) -> dict:
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
    return collections


def get_app_store_categories(category_keyword: str | None = None) -> dict:
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
    return categories


def scrape_ios_ranks(
    category_keyword: str | None = None,
    collection_keyword: str | None = None,
    country: str = "us",
) -> list[dict]:
    scrape_info = (
        f"Scrape iOS ranks for {collection_keyword=} {category_keyword=} {country=}"
    )
    logger.info(f"{scrape_info} starting")
    # Eg: MAGAZINES_MEN, GAMES_ADVENTURE
    collections = get_app_store_collections(
        collection_keyword=collection_keyword,
    )
    categories = get_app_store_categories(category_keyword=category_keyword)
    ranked_dicts: list[dict] = []
    scraper = AppStoreScraper()
    for _coll_key, coll_value in collections.items():
        for cat_key, cat_value in categories.items():
            logger.info(f"{scrape_info} Coll_key: {_coll_key}, cat_key: {cat_key}")
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
                logger.error(f"{scrape_info} failed after multiple retries: {str(e)}")
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
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)

    html = response.text

    if response.status_code != 200:
        logger.error(f"Failed to retrieve the page: {response.status_code}")
        return {}

    soup = BeautifulSoup(response.text, "html.parser")
    in_app_purchase_element = soup.find(
        "li", class_="inline-list__item--bulleted", string="Offers In-App Purchases"
    )
    has_in_app_purchases = in_app_purchase_element is not None

    try:
        privacy_details = get_privacy_details(html, country, store_id)
    except Exception as e:
        logger.error(f"Failed to get privacy details for {store_id=} {country=} {e}")
        privacy_details = None

    has_third_party_advertising = "THIRD_PARTY_ADVERTISING" in str(privacy_details)

    urls = get_urls_from_html(soup)

    return {
        "in_app_purchases": has_in_app_purchases,
        "ad_supported": has_third_party_advertising,
        "urls": urls,
    }


def get_urls_from_html(soup: BeautifulSoup) -> dict:
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


def get_privacy_details(html: str, country: str, store_id: str) -> bool:
    """
    Get privacy details for an iOS app from the App Store.

    Args:
        html: HTML of the App Store page
        country: Country code (default: 'US')
        store_id: App Store ID of the app

    Returns:
        True if the app has third-party advertising, False otherwise
    """

    # Extract the token using regex
    reg_exp = r"token%22%3A%22([^%]+)%22%7D"
    match = re.search(reg_exp, html)
    if not match:
        raise ValueError("Could not extract token from App Store page")

    token = match.group(1)

    # Make request to the API for privacy details
    api_url = f"https://amp-api-edge.apps.apple.com/v1/catalog/{country}/apps/{store_id}?platform=web&fields=privacyDetails"
    api_headers = {
        "Origin": "https://apps.apple.com",
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    }

    response = requests.get(api_url, headers=api_headers)
    response.raise_for_status()

    data = response.json()

    if not data.get("data") or len(data["data"]) == 0:
        raise ValueError("App not found (404)")
    return data["data"][0]["attributes"]["privacyDetails"]


def find_privacy_policy_id(soup: BeautifulSoup) -> str | None:
    privacy_learn_more = soup.find("p", class_="app-privacy__learn-more")

    if privacy_learn_more:
        # Get the anchor tag inside the paragraph
        link = privacy_learn_more.find("a")

        if link:
            # Extract the full URL
            url = link.get("href")
            print(f"URL: {url}")

            # Extract the ID from the URL
            # The URL format is https://apps.apple.com/story/id1538632801
            if "id" in url:
                # Find the position where "id" starts and extract everything after it
                id_position = url.find("id")
                if id_position != -1:
                    id_value = url[id_position + 2 :]  # +2 to skip the "id" prefix
                    print(f"ID: {id_value}")  # This will print: 1538632801
                    return id_value


def get_developer_url(result: dict, urls: dict) -> str:
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
        found_tlds = []
        for url_type, url in urls.items():
            print(url_type, url)
            tld = tldextract.extract(url)
            tld_str = ".".join([tld.domain, tld.suffix])
            if tld_str not in DEVLEOPER_IGNORE_TLDS and tld_str not in found_tlds:
                found_tlds.append(tld_str)
        if len(found_tlds) == 0:
            if "sellerUrl" not in result.keys():
                raise Exception(f"No developer url found for {urls=}")
            final_url: str = result["sellerUrl"]
        elif len(found_tlds) == 1:
            final_url: str = found_tlds[0]
        else:
            logger.warning(f"Multiple developer sites found for {urls=} {found_tlds=}")
            final_url: str = result["sellerUrl"]
    else:
        final_url: str = result["sellerUrl"]
    return final_url


def scrape_app_ios(store_id: str, country: str, language: str) -> dict:
    """Scrape iOS app details from the App Store.
    yt_us = scrape_app_ios("544007664", "us", language="en")
    yt_de = scrape_app_ios("544007664", "de", language="en")

    # SAME FOR ALL COUNTRIES
    yt_de['sellerName'] == yt_us['sellerName']
    yt_us['currentVersionReleaseDate'] == yt_de['currentVersionReleaseDate']

    ## UNIQUE PER COUNTRY
    yt_us['userRatingCountForCurrentVersion'] != yt_de['userRatingCountForCurrentVersion']
    yt_de['averageUserRating'] != yt_us['averageUserRating']
    yt_us['userRatingCount'] != yt_de['userRatingCount']
    yt_de['user_ratings'] != yt_us['user_ratings']
    yt_de['description'] != yt_us['description']

    # These very by country but are also the same as each other?
    yt_de['userRatingCount'] == yt_de['userRatingCountForCurrentVersion']


    """
    # NOTE: averageUserRating, Rating_count, Histogram are country specific
    scraper = AppStoreScraper()
    # Note add_ratings is pulling reviews, then not storing them!
    result_dict: dict = scraper.get_app_details(
        store_id, country=country, add_ratings=True, timeout=10, lang=language
    )
    logger.info(f"{store_id=}, {country=}, {language=} ios store scraped")
    return result_dict


def scrape_itunes_additional_html(result: dict, store_id: str, country: str) -> dict:
    try:
        # This is slow and returns 401 often, so use sparingly
        html_res = scrape_store_html(store_id=store_id, country=country)
        result["in_app_purchases"] = html_res["in_app_purchases"]
        result["ad_supported"] = html_res["ad_supported"]
        result["sellerUrl"] = get_developer_url(result, html_res["urls"])
    except Exception as e:
        logger.warning(f"Failed to get developer url for {store_id=} {country=} {e}")
    result["additional_html_scraped_at"] = datetime.datetime.now(tz=datetime.UTC)
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
        # Complicated way to get around many games having genre lists
        # Best case "Games,Puzzles" worst case like "Games,Shopping,Puzzles"
        # TODO: Just store categories as list!
        cat_is_games = df["category"] == "Games"
        genre_is_not_games = df["genres"] != "Games"
        rows_to_update = cat_is_games & genre_is_not_games
        df.loc[rows_to_update, "category"] = "game_" + df.loc[
            rows_to_update,
            "genres",
        ].apply(
            lambda x: [
                cat.lower().replace(" ", "_")
                for cat in x.split(",")
                if cat.lower().replace(" ", "_") in GAME_CATEGORIES
            ][0],
        )
    except Exception as e:
        logger.debug(
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
        except Exception:
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
