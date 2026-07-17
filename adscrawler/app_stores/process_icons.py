"""Process app icons — resize, upload to S3, and update missing variants."""

import pathlib
import time
from datetime import UTC, datetime
from io import BytesIO

import imagehash
import pandas as pd
import requests
from PIL import Image

from adscrawler.config import APP_ICONS_TMP_DIR, CONFIG, get_logger
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import (
    query_apps_missing_icon_variants,
    update_from_df,
    upsert_df,
)
from adscrawler.process.storage import get_s3_client

logger = get_logger(__name__, "process_icons")

# Retry delays (in seconds) for failed icon processing
_ICON_RETRY_DELAYS = [0.5, 1.0, 1.5, 2.0]


def _ensure_rgb(img: Image.Image) -> Image.Image:
    """Convert image to RGB if it isn't already (e.g. CMYK, RGBA, P)."""
    if img.mode == "RGB":
        return img
    return img.convert("RGB")


def process_app_icon(
    store_id: str, url: str, proxies: dict[str, str] | None = None
) -> tuple[str, str] | None:
    """Download a 512px icon, resize to 128×128 and 64×64, upload both to S3.

    Retries on failure with backoff delays of 0.5, 1, 1.5, and 2 seconds.

    Returns ``(filename_128, filename_64)`` on success, or ``None`` on failure.
    """
    last_exception: Exception | None = None
    for attempt, delay in enumerate(_ICON_RETRY_DELAYS, start=1):
        try:
            if attempt == 1:
                response = requests.get(url, timeout=10)
            else:
                response = requests.get(url, timeout=10, proxies=proxies)
        except Exception as exc:
            logger.warning(
                f"Attempt {attempt}/{len(_ICON_RETRY_DELAYS)} — "
                f"failed to fetch image from {url}: {exc}"
            )
            last_exception = exc
            if attempt < len(_ICON_RETRY_DELAYS):
                time.sleep(delay)
            continue

        try:
            img = Image.open(BytesIO(response.content))
            img = _ensure_rgb(img)
            # Always store as PNG regardless of source format
            img_resized = img.resize((128, 128), Image.LANCZOS)
            img_64_resized = img.resize((64, 64), Image.LANCZOS)
            phash = str(imagehash.phash(img_resized))
            f_128_name = f"{phash}_128.png"
            f_64_name = f"{phash}_64.png"
            file_path = pathlib.Path(APP_ICONS_TMP_DIR, f"{phash}_128.png")
            file_64_path = pathlib.Path(APP_ICONS_TMP_DIR, f"{phash}_64.png")
            img_resized.save(file_path, format="PNG")
            img_64_resized.save(file_64_path, format="PNG")
        except Exception as exc:
            logger.warning(
                f"Attempt {attempt}/{len(_ICON_RETRY_DELAYS)} — "
                f"failed to process icon for {store_id} from {url}: {exc}"
            )
            last_exception = exc
            continue

        # Upload to S3
        image_format = "image/png"
        s3_key = "digi-cloud"
        s3_client = get_s3_client(s3_key)
        response = s3_client.put_object(
            Bucket=CONFIG[s3_key]["bucket"],
            Key=f"app-icons/{store_id}/{f_128_name}",
            ACL="public-read",
            Body=file_path.read_bytes(),
            ContentType=image_format,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            logger.info(f"S3 uploaded {store_id} 128px icon")
        else:
            logger.error(f"S3 failed to upload {store_id} 128px icon")
        response = s3_client.put_object(
            Bucket=CONFIG[s3_key]["bucket"],
            Key=f"app-icons/{store_id}/{f_64_name}",
            ACL="public-read",
            Body=file_64_path.read_bytes(),
            ContentType=image_format,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            logger.info(f"S3 uploaded {store_id} 64px icon")
        else:
            logger.error(f"S3 failed to upload {store_id} 64px icon")
        return f_128_name, f_64_name

    logger.error(
        f"All {len(_ICON_RETRY_DELAYS)} attempts failed for icon {store_id} "
        f"from {url}: {last_exception}"
    )
    return None


def build_icon_update_df(apps_df: pd.DataFrame) -> pd.DataFrame:
    """Build store_apps updates for apps missing 128/64 icon variants.

    Returns a DataFrame with columns ``[id, icon_128, icon_64]``
    containing only the rows where at least one variant was successfully
    generated.
    """

    if apps_df.empty:
        return pd.DataFrame(columns=["id", "icon_128", "icon_64"])

    proxies = CONFIG.get("proxies", None)

    icol_128 = apps_df.get("icon_128")
    icol_64 = apps_df.get("icon_64")
    if icol_128 is None:
        apps_df = apps_df.copy()
        apps_df["icon_128"] = pd.NA
    if icol_64 is None:
        apps_df = apps_df.copy()
        apps_df["icon_64"] = pd.NA

    needs_update = apps_df["icon_url_512"].notna() & (
        apps_df["icon_128"].isna() | apps_df["icon_64"].isna()
    )
    apps_to_update = apps_df.loc[needs_update].copy()
    if apps_to_update.empty:
        return pd.DataFrame(columns=["id", "icon_128", "icon_64"])

    icon_results = apps_to_update.apply(
        lambda row: process_app_icon(row["store_id"], row["icon_url_512"], proxies),
        axis=1,
    )
    icon_update_df = pd.DataFrame(
        {
            "id": apps_to_update["id"].astype(int),
            "icon_128": [
                result[0] if isinstance(result, tuple) else None
                for result in icon_results
            ],
            "icon_64": [
                result[1] if isinstance(result, tuple) else None
                for result in icon_results
            ],
        }
    )
    icon_update_df = icon_update_df[
        icon_update_df["icon_128"].notna() | icon_update_df["icon_64"].notna()
    ]
    return icon_update_df


def refresh_app_icons(
    pgdb: PostgresEngine, limit: int | None = None, store: int | None = None
) -> int:
    """Refresh missing 128/64 icon variants for apps that already have 512px icons.

    Queries the database for apps that need one or both small variants,
    generates them, uploads to S3, and upserts the filenames into
    ``store_apps``.

    Parameters
    ----------
    pgdb : PostgresEngine
        Database connection.
    limit : int, optional
        Maximum number of apps to process per batch.
    store : int, optional
        Filter by store (1 = Google, 2 = Apple). Pass None for all stores.

    Returns the number of apps updated.
    """
    apps_df = query_apps_missing_icon_variants(pgdb=pgdb, limit=limit, store=store)
    if apps_df.empty:
        logger.info("No apps need icon refresh")
        return 0

    icon_update_df = build_icon_update_df(apps_df)
    if icon_update_df.empty:
        logger.info("No missing icon variants could be generated")
        return 0

    update_from_df(
        table_name="store_apps",
        df=icon_update_df,
        update_columns=["icon_128", "icon_64"],
        key_columns=["id"],
        pgdb=pgdb,
    )

    # Log which apps were crawled
    crawl_log = pd.DataFrame(
        {"store_app": icon_update_df["id"], "crawled_at": datetime.now(UTC)}
    )
    upsert_df(
        table_name="app_icons_crawled_at",
        schema="logging",
        insert_columns=["store_app", "crawled_at"],
        df=crawl_log[["store_app", "crawled_at"]],
        key_columns=["store_app"],
        pgdb=pgdb,
    )

    logger.info(f"Updated {len(icon_update_df)} app icon variants")
    return len(icon_update_df)
