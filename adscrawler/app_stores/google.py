import json
import os
import subprocess
from typing import Any

import appgoblin_play_scraper
import pandas as pd

from adscrawler.config import CONFIG, MODULE_DIR, PACKAGE_DIR, get_logger

_MODEL_CACHE: dict[bool, Any] = {}



logger = get_logger(__name__, "scrape_google")


def normalize_google_search_results(
    results: list[dict], country: str, language: str
) -> list[dict]:
    normalized_results = []
    for result in results:
        normalized_result = result.copy()
        store_id = normalized_result.get("store_id") or normalized_result.get("appId")
        if not store_id:
            logger.debug(
                "Skip Google search result without store_id "
                f"{country=} {language=} {result.get('title')= }"
            )
            continue

        normalized_result.pop("appId", None)
        store_link = normalized_result.pop("url", None) or normalized_result.get(
            "store_link"
        )
        normalized_result["store_id"] = store_id
        normalized_result["id"] = store_id
        normalized_result["store_link"] = (
            store_link or f"https://play.google.com/store/apps/details?id={store_id}"
        )
        normalized_result["name"] = normalized_result.pop(
            "title", normalized_result.get("name")
        )
        normalized_result["developer_name"] = normalized_result.pop(
            "developer", normalized_result.get("developer_name")
        )
        normalized_result["icon_url_512"] = normalized_result.pop(
            "icon", normalized_result.get("icon_url_512")
        )
        normalized_result["store"] = 1
        normalized_result["country"] = country
        normalized_result["language"] = language
        normalized_results.append(normalized_result)

    return normalized_results


def scrape_app_gp(store_id: str, country: str, language: str = "en") -> dict:
    """
    yt_us = scrape_app_gp("com.google.android.youtube", "us", language="en")
    yt_de = scrape_app_gp("com.google.android.youtube", "mx", language="en")

    ## SAME FOR ALL COUNTRIES
    yt_us["ratings"] == yt_de["ratings"]
    yt_us["realInstalls"] == yt_de["realInstalls"]
    yt_us["updated"] == yt_de["updated"]

    ## MOSTLY SAME FOR ALL COUNTRIES
    ## Almost always lower for smaller countries
    ## looks more like delays and incomplete (0s)
    yt_us["histogram"] == yt_de["histogram"]


    ## UNIQUE PER COUNTRY
    yt_us["reviews"] != yt_de["reviews"]
    yt_us["score"] != yt_de["score"]

    ## UNIQUE PER LANGUAGE
    yt_us["description"] == yt_de["description"]
    yt_us["description"] == yt_de_en["description"]
    """
    result_dict: dict = appgoblin_play_scraper.app(
        store_id,
        lang=language,
        country=country,
        timeout=10,
    )
    logger.debug(f"store=1 {country=} {language=} {store_id=} play store scraped")
    return result_dict

def _safe_batch_predict(
    model: Any, texts: list[str], k: int = 1
) -> tuple[list[str], list[float]]:
    """Predict a batch; if fasttext chokes on a ragged/empty-token input,
    fall back to per-string prediction so one bad row doesn't kill the batch."""
    try:
        labels, scores = model.predict(texts, k=k)
        return (
            [lbl[0].removeprefix("__label__") for lbl in labels],
            [s[0] for s in scores],
        )
    except ValueError:
        # Something in this batch has zero fasttext tokens; isolate it.
        out_labels: list[str] = []
        out_scores: list[float] = []
        for t in texts:
            try:
                lbl, sc = model.predict(t, k=k)
                out_labels.append(lbl[0].removeprefix("__label__"))
                out_scores.append(sc[0])
            except ValueError:
                out_labels.append("zz")
                out_scores.append(0.0)
        return out_labels, out_scores
    


def _get_model(low_memory: bool = False) -> Any:
    """Load and cache the fasttext LID model once per process.
    """
    if low_memory not in _MODEL_CACHE:
        try:
            from ftlangdetect.detect import get_or_load_model
        except Exception as exc:  # pragma: no cover - optional dependency
            raise ImportError(
                "Language-detection requires optional packages. Install with: pip install ftlangdetect[fasttext] or pip install fasttext ftlangdetect"
            ) from exc

        _MODEL_CACHE[low_memory] = get_or_load_model(low_memory=low_memory)
    return _MODEL_CACHE[low_memory]




def _prep_for_detection(series: pd.Series, max_chars: int = 300) -> pd.Series:
    """Vectorized cleanup: fasttext needs single-line input, and we
    only need a short prefix for confident detection."""
    cleaned = (
        series.fillna("")
        .astype(str)
        .str.slice(0, max_chars)
        .str.replace(r"\s+", " ", regex=True)  # collapses newlines/tabs too
        .str.strip()
    )
    return cleaned


def add_language_column(
    apps_df: pd.DataFrame, max_chars: int = 300, low_memory: bool = False
) -> pd.DataFrame:
    """Add a store_language_code column to apps_df, batched for speed."""
    apps_df = apps_df.copy()
    snippets = _prep_for_detection(apps_df["description"], max_chars=max_chars)

    is_valid = snippets.str.len() > 0
    valid_texts = snippets[is_valid].tolist()

    codes = pd.Series("zz", index=apps_df.index, dtype="object")

    if valid_texts:
        model = _get_model(low_memory=low_memory)
        parsed, _scores = _safe_batch_predict(model, valid_texts, k=1)
        codes.loc[is_valid] = parsed

    apps_df["store_language_code"] = codes
    return apps_df

def clean_google_play_app_df(apps_df: pd.DataFrame) -> pd.DataFrame:
    apps_df = apps_df.rename(
        columns={
            "title": "name",
            "installs": "min_installs",
            "realInstalls": "installs",
            # "appId": "store_id",
            "score": "rating",
            "updated": "store_last_updated",
            "reviews": "review_count",
            "ratings": "rating_count",
            "summary": "description_short",
            "released": "release_date",
            "containsAds": "ad_supported",
            "offersIAP": "in_app_purchases",
            "url": "store_url",
            "icon": "icon_url_512",
            "developerWebsite": "url",
            "developerAddress": "developer_address",
            "developerLegalAddress": "developer_legal_address",
            "developerId": "developer_id",
            "developer": "developer_name",
            "developerEmail": "developer_email",
            "genreId": "category",
            "headerImage": "featured_image_url",
            "screenshots": "phone_image_urls",
        },
    )
    can_replace_min_installs = (apps_df["min_installs"].isna()) & (
        apps_df["installs"].notna()
    )
    apps_df.loc[can_replace_min_installs, "min_installs"] = apps_df.loc[
        can_replace_min_installs,
        "installs",
    ].astype(str)

    release_dt = pd.to_datetime(
            apps_df["release_date"], format="%b %d, %Y", errors="coerce"
        )
    still_na = release_dt.isna()
    if still_na.any():
        release_dt.loc[still_na] = pd.to_datetime(
            apps_df.loc[still_na, "release_date"], format="%d %b %Y", errors="coerce"
        )

    apps_df = apps_df.assign(
        category=apps_df["category"].str.lower(),
        release_date=release_dt.dt.date,
        store_last_updated=pd.to_datetime(
            apps_df["store_last_updated"], unit="s", errors="coerce"
        ),
    )
    if "developer_name" in apps_df.columns:
        apps_df["developer_name"] = apps_df["developer_name"].str.replace(
            "\t", " ", regex=False
        )
    list_cols = ["phone_image_url"]
    for list_col in list_cols:
        col = f"{list_col}s"
        urls_empty = (
            (apps_df[col].isna()) | (apps_df[col].fillna("").str.len() == 0)
        ).all()
        if not urls_empty:
            expanded = pd.DataFrame(
                apps_df[col].tolist(), index=apps_df.index
            ).iloc[:, :3]
            expanded.columns = [f"{list_col}_{x + 1}" for x in range(expanded.shape[1])]
            for x in range(3):
                name = f"{list_col}_{x + 1}"
                if name not in expanded.columns:
                    expanded[name] = None
            apps_df = pd.concat([apps_df, expanded], axis=1)
        else:
            for x in range(3):
                apps_df[f"{list_col}_{x}"] = None
    if "description" in apps_df.columns:
        apps_df = add_language_column(apps_df)
        apps_df.loc[
            apps_df["store_language_code"].str.startswith("zh-"), "store_language_code"
        ] = "zh"
    return apps_df


def call_js_to_update_file(
    filepath: str, country: str = "us", is_developers: bool = False
) -> None:
    if os.path.exists(filepath):
        os.remove(filepath)
    cmd = f"node {PACKAGE_DIR}/pullAppIds.js -c {country}"
    if is_developers:
        cmd += " --developers"
    logger.info("Js pull start")
    # subprocess.run(cmd, shell=True, check=True)
    os.system(cmd)
    logger.info("Js pull finished")


def get_js_data(filepath: str, is_json: bool = True) -> list[dict] | list:
    with open(filepath) as file:
        if is_json:
            data = [json.loads(line) for line in file if line.strip()]
        else:
            data = [line.strip() for line in file]
    return data


def scrape_google_ranks(country: str) -> list[dict]:
    logger.info(f"Scrape Google ranks {country=} start")
    filepath = f"/tmp/googleplay_json_{country}.txt"
    try:
        call_js_to_update_file(filepath, country)
    except Exception as error:
        logger.exception(f"JS pull failed with {country=} {error=}")
    ranked_dicts = get_js_data(filepath)
    logger.info(f"Scrape Google ranks {country=} finished: {len(ranked_dicts)}")
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
    if os.path.exists(app_ids_filepath):
        os.remove(app_ids_filepath)
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
    if len(new_app_ids) > 0:
        apps_df = pd.DataFrame({"store": store, "store_id": new_app_ids})
    else:
        apps_df = pd.DataFrame(columns=["store", "store_id"])
    return apps_df


def search_play_store(
    search_term: str, country: str = "us", language: str = "en"
) -> list[dict]:
    """Search store for new apps or keyword rankings."""
    logger.info(f"Run Playstore search {search_term=} {country=} {language=}")
    results = []
    # Returns few results
    try:
        results = appgoblin_play_scraper.search(
            search_term,
            lang=language,
            country=country,
        )
    except Exception:
        node_path = "node"
        if "local-dev" in CONFIG.keys():
            node_path = CONFIG["local-dev"].get("node_env")

        # Call the Node.js script that runs google-play-scraper
        # Depending how node is installed you may need
        # a system link like sudo ln -sf /home/user/.nvm/versions/node/v24.14.0/bin/node /usr/local/bin/node
        process = subprocess.Popen(
            [
                node_path,
                f"{MODULE_DIR}/static/searchApps.js",
                search_term,
                "200",
                country,
                language,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            output, error = process.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.communicate(timeout=5)  # drain pipes safely
            except subprocess.TimeoutExpired:
                pass
            raise Exception("Search process timed out")

        if error:
            logger.error(f"failed to search: {error!r}")

        results: list[dict] = json.loads(output)
    results = normalize_google_search_results(
        results, country=country, language=language
    )

    logger.info(f"Playstore search finished with {len(results)} results")

    return results
