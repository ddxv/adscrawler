import json
import os
import subprocess

import google_play_scraper
import pandas as pd

from adscrawler.config import CONFIG, MODULE_DIR, PACKAGE_DIR, get_logger

logger = get_logger(__name__, "scrape_google")


def scrape_app_gp(store_id: str, country: str, language: str = "") -> dict:
    # Note language seems not to change the number of reviews, but country does
    # Note country does not change number of installs
    # NOTE: Histogram, Ratings, score are SOMETIMES country specific
    # NOTE: Reviews are always country specific?
    # NOTE: Installs are never country specific
    # Example: 'ratings'
    # dom_nl = scrape_app_gp('com.nexonm.dominations.adk', 'nl')
    # dom_us = scrape_app_gp('com.nexonm.dominations.adk', 'us')
    # dom_us['ratings']==dom_nl['ratings']
    # In the case above NL and US both have the same number of ratings
    # paw_nl = scrape_app_gp('com.originatorkids.paw', 'nl')
    # paw_us = scrape_app_gp('com.originatorkids.paw', 'us')
    # paw_us['ratings']==paw_nl['ratings']
    # In the case above NL and US both have very different number of ratings
    logger.info(f'scrape app start: {store_id=}, {country=}, {language=} scrape app start')
    result: dict = google_play_scraper.app(
        store_id,
        lang=language,
        country=country,  # defaults to 'en'  # defaults to 'us'
        timeout=10
    )
    logger.info(f'scrape app finish: {store_id=}, {country=}, {language=} scrape app start')
    return result


def clean_google_play_app_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "title": "name",
            "installs": "min_installs",
            "realInstalls": "installs",
            # "appId": "store_id",
            "score": "rating",
            "updated": "store_last_updated",
            "reviews": "review_count",
            "ratings": "rating_count",
            "summary": "short_description",
            "released": "release_date",
            "containsAds": "ad_supported",
            "offersIAP": "in_app_purchases",
            "url": "store_url",
            "icon": "icon_url_512",
            "developerWebsite": "url",
            "developerId": "developer_id",
            "developer": "developer_name",
            "genreId": "category",
            "headerImage": "featured_image_url",
            "screenshots": "phone_image_urls",
        },
    )
    df.loc[df["min_installs"].isna(), "min_installs"] = df.loc[
        df["min_installs"].isna(),
        "installs",
    ].astype(str)
    df = df.assign(
        min_installs=df["min_installs"]
        .str.replace(r"[,+]", "", regex=True)
        .fillna(0)
        .astype(int),
        category=df["category"].str.lower(),
        store_last_updated=pd.to_datetime(
            df["store_last_updated"],
            unit="s",
        ).dt.strftime("%Y-%m-%d %H:%M"),
        release_date=pd.to_datetime(df["release_date"], format="%b %d, %Y").dt.date,
    )
    list_cols = ["phone_image_url"]
    for list_col in list_cols:
        urls_empty = ((df[f"{list_col}s"].isna()) | (df[f"{list_col}s"] == "")).all()
        if not urls_empty:
            columns = {x: f"{list_col}_{x+1}" for x in range(3)}
            df = pd.concat(
                [
                    df,
                    df[f"{list_col}s"].apply(pd.Series).rename(columns=columns),
                ],
                axis=1,
            )
        else:
            for x in range(3):
                df[f"{list_col}_{x}"] = None
    return df


def call_js_to_update_file(filepath: str, is_developers: bool = False) -> None:
    if os.path.exists(filepath):
        os.remove(filepath)
    cmd = f"node {PACKAGE_DIR}/pullAppIds.js"
    if is_developers:
        cmd += " --developers"
    logger.info("Js pull start")
    os.system(cmd)
    logger.info("Js pull finished")


def get_js_data(filepath: str, is_json: bool = True) -> list[dict] | list:
    with open(filepath) as file:
        if is_json:
            data = [json.loads(line) for line in file if line.strip()]
        else:
            data = [line.strip() for line in file]
    return data


def scrape_google_ranks() -> list[dict]:
    logger.info("Scrape Google ranks start")
    filepath = "/tmp/googleplay_json.txt"
    try:
        call_js_to_update_file(filepath)
    except Exception as error:
        logger.exception(f"JS pull failed with {error=}")
    ranked_dicts = get_js_data(filepath)
    logger.info(f"Scrape Google ranks finished: {len(ranked_dicts)}")
    return ranked_dicts


def scrape_gp_for_developer_app_ids(developer_ids: list[str]) -> list:
    logger.info("Scrape GP developers for new apps start")
    developers_filepath = "/tmp/googleplay_developers.txt"
    if os.path.exists(developers_filepath):
        os.remove(developers_filepath)

    with open(developers_filepath, "w") as file:
        for dev_id in developer_ids:
            file.write(f"{dev_id}\n")

    app_ids_filepath = "/tmp/googleplay_developers_app_ids.txt"
    try:
        call_js_to_update_file(app_ids_filepath, is_developers=True)
    except Exception as error:
        logger.exception(f"JS pull failed with {error=}")
    try:
        app_ids = get_js_data(app_ids_filepath, is_json=False)
    except Exception:
        logger.exception("Unable to load scraped js developer app file")
        app_ids = []
    logger.info("Scrape Google developers for new apps finished")
    return app_ids


def crawl_google_developers(
    developer_ids: list[str],
    store_ids: list[str],
) -> pd.DataFrame:
    store = 1
    app_ids = scrape_gp_for_developer_app_ids(developer_ids=developer_ids)
    new_app_ids = [x for x in app_ids if x not in store_ids]
    if len(new_app_ids) > 1:
        apps_df = pd.DataFrame({"store": store, "store_id": new_app_ids})
    else:
        apps_df = pd.DataFrame(columns=["store", "store_id"])
    return apps_df

def search_play_store(search_term:str)-> list[dict]:
    """Search store for new apps or keyword rankings.
    """
    logger.info("adscrawler to call playstore search")
    # Call the Node.js script that runs google-play-scraper

    node_path = 'node'
    if 'local-dev' in CONFIG.keys():
        node_path = CONFIG['local-dev'].get('node_env')

    logger.info(f"Will try calling node with {node_path} {MODULE_DIR}/static/searchApps.js")

    process = subprocess.Popen([node_path, f'{MODULE_DIR}/static/searchApps.js', search_term, '20'], stdout=subprocess.PIPE)
    output, error = process.communicate()

    if error:
        logger.error(f'failed to search: {error!r}')

    results:list[dict] = json.loads(output)

    if len(results) > 0 and 'appId' in results[0]:
        for result in results:
            result['store_id'] = result['appId'] #needed by backend
            result['id'] = result.pop('appId') # needed for response to frontend
            result['store_link'] = result.pop('url')
            result['name'] = result.pop('title')
            result['developer_name'] = result.pop('developer')
            result['icon_url_512'] = result.pop('icon')
            result['store'] = 1

    return results

