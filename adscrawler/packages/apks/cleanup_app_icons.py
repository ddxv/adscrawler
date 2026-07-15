"""Reconcile app icon references between the database and S3 (two directions).

**Direction 1 — Remove orphans**
  ``store_apps`` rows with ``icon_128`` / ``icon_64`` filenames that point to S3
  objects which no longer exist ('disconnected' because of a bucket migration or
  accidental deletion).  We NULL those columns + clear the ``app_icons_crawled_at``
  log so ``--refresh-app-icons`` re-generates them.

**Direction 2 — Restore connections**
  ``store_apps`` rows with NULL ``icon_128`` / ``icon_64`` columns whose icon
  files *are* still present in S3 (the DB reference was lost but the object
  survived).  We fill in the correct filename so the app is marked as complete
  without needing a fresh download+resize.

Because there can be millions of icon files in S3, we make a **single
paginated walk** through the ``app-icons/`` prefix and cross-reference the
result with the DB — far more efficient than per-app ``head_object`` calls.
For each ``store_id`` the most-recently-uploaded ``*_128.png`` and
``*_64.png`` are kept; this handles the common case of multiple icon versions
over time.
"""

import dataclasses
import datetime
from typing import Any

import pandas as pd
from sqlalchemy import text

from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import get_db_connection
from adscrawler.dbcon.queries import update_from_df
from adscrawler.process.storage import get_s3_client

logger = get_logger(__name__)

ICON_S3_KEY = "digi-cloud"
ICON_BUCKET = CONFIG[ICON_S3_KEY]["bucket"]
ICON_PREFIX = "app-icons"

pgdb = get_db_connection()


# ── Data structures ─────────────────────────────────────────────────────


@dataclasses.dataclass(order=True)
class IconFile:
    """Lightweight holder for a single icon file discovered in S3."""

    filename: str
    last_modified: datetime.datetime


# ── Phase 1: walk S3 ────────────────────────────────────────────────────


def list_all_icons_from_s3() -> dict[str, dict[str, IconFile]]:
    """Walk the entire ``app-icons/`` prefix in S3.

    Returns
    -------
    ``{store_id: {"128": IconFile, "64": IconFile}}``
        For each store_id the single most-recent ``*_128.png`` and
        ``*_64.png``.  If a size variant has never been seen, the inner dict
        will simply lack that key.
    """
    s3 = get_s3_client(ICON_S3_KEY)
    paginator = s3.get_paginator("list_objects_v2")

    result: dict[str, dict[str, IconFile]] = {}

    page: dict[str, Any]
    for page in paginator.paginate(Bucket=ICON_BUCKET, Prefix=f"{ICON_PREFIX}/"):
        for obj in page.get("Contents", []):
            key: str = obj["Key"]
            last_mod: datetime.datetime = obj["LastModified"]

            # key = "app-icons/{store_id}/{phash}_{size}.png"
            # strip prefix → "{store_id}/{phash}_{size}.png"
            relative = key.removeprefix(f"{ICON_PREFIX}/")
            parts = relative.split("/", 1)
            if len(parts) != 2:
                continue
            store_id, filename = parts

            if filename.endswith("_128.png"):
                size_key = "128"
            elif filename.endswith("_64.png"):
                size_key = "64"
            else:
                continue  # unexpected file, skip

            by_store = result.setdefault(store_id, {})
            existing = by_store.get(size_key)
            if existing is None or last_mod > existing.last_modified:
                by_store[size_key] = IconFile(filename=filename, last_modified=last_mod)

    logger.info("S3 walk complete — %d store_ids found", len(result))
    return result


# ── Phase 2: query the current DB state ─────────────────────────────────


def query_apps_icon_state() -> pd.DataFrame:
    """Return *all* ``store_apps`` rows with their current icon columns.

    We need the full picture so we can spot discrepancies in both directions.
    """
    query = """
        SELECT
            sa.id,
            sa.store_id,
            sa.icon_128,
            sa.icon_64,
            sa.crawl_result
        FROM public.store_apps AS sa
        WHERE sa.icon_url_512 IS NOT NULL
          AND sa.crawl_result = 1
        ORDER BY sa.id
    """
    return pd.read_sql(text(query), pgdb.engine, dtype={"id": int})


# ── Phase 3: cross-reference & produce updates ──────────────────────────


def cross_reference(
    apps_df: pd.DataFrame,
    s3_map: dict[str, dict[str, IconFile]],
) -> tuple[
    list[dict[str, Any]],  # rows to restore (id, icon_128, icon_64)
    list[int],  # ids to null-out icon_128
    list[int],  # ids to null-out icon_64
]:
    """Compare DB state against the S3 icon map.

    Returns
    -------
    ``(to_restore, to_null_128, to_null_64)``
    """
    to_restore: list[dict[str, Any]] = []
    to_null_128: list[int] = []
    to_null_64: list[int] = []

    for _, row in apps_df.iterrows():
        sid = int(row["id"])
        store_id = row["store_id"]
        s3_entry = s3_map.get(store_id)

        db_128: str | None = row.get("icon_128")
        db_64: str | None = row.get("icon_64")

        # ── Direction 2: S3 has files but DB is NULL → restore ──
        if s3_entry is not None:
            row_updates: dict[str, Any] = {"id": sid}
            needs_update = False

            if (pd.isna(db_128) or not db_128) and "128" in s3_entry:
                row_updates["icon_128"] = s3_entry["128"].filename
                needs_update = True
            if (pd.isna(db_64) or not db_64) and "64" in s3_entry:
                row_updates["icon_64"] = s3_entry["64"].filename
                needs_update = True

            if needs_update:
                to_restore.append(row_updates)

        # ── Direction 1: DB has filenames but S3 doesn't → orphan ──
        if pd.notna(db_128):
            if s3_entry is None or "128" not in s3_entry:
                to_null_128.append(sid)
        if pd.notna(db_64):
            if s3_entry is None or "64" not in s3_entry:
                to_null_64.append(sid)

    return to_restore, to_null_128, to_null_64


# ── Phase 4: apply updates ──────────────────────────────────────────────


def apply_restore(rows: list[dict[str, Any]]) -> None:
    """Update DB rows with icon filenames recovered from S3."""
    if not rows:
        return
    df = pd.DataFrame(rows)
    update_from_df(
        df=df,
        table_name="store_apps",
        update_columns=["icon_128", "icon_64"],
        key_columns=["id"],
        pgdb=pgdb,
    )
    logger.info("Restored icon_128/icon_64 for %d app(s) from S3", len(rows))


def apply_nullify(to_null_128: list[int], to_null_64: list[int]) -> None:
    """NULL out orphaned icon columns and clear their crawl log."""
    affected_ids: set[int] = set()

    if to_null_128:
        df_128 = pd.DataFrame({"id": to_null_128, "icon_128": None})
        update_from_df(
            df=df_128,
            table_name="store_apps",
            update_columns=["icon_128"],
            key_columns=["id"],
            pgdb=pgdb,
        )
        logger.info(
            "Set icon_128 → NULL for %d app(s) (S3 file missing)", len(to_null_128)
        )
        affected_ids.update(to_null_128)

    if to_null_64:
        df_64 = pd.DataFrame({"id": to_null_64, "icon_64": None})
        update_from_df(
            df=df_64,
            table_name="store_apps",
            update_columns=["icon_64"],
            key_columns=["id"],
            pgdb=pgdb,
        )
        logger.info(
            "Set icon_64  → NULL for %d app(s) (S3 file missing)", len(to_null_64)
        )
        affected_ids.update(to_null_64)

    if affected_ids:
        ids_list = list(affected_ids)
        with pgdb.engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM logging.app_icons_crawled_at "
                    "WHERE store_app = ANY(:ids)"
                ),
                {"ids": ids_list},
            )
        logger.info(
            "Cleared logging.app_icons_crawled_at for %d app(s)",
            len(ids_list),
        )


def run_icon_cleanup() -> None:
    """Reconcile ``store_apps`` icon columns with S3 in both directions.

    Workflow
    --------
    1. Walk the entire ``app-icons/`` prefix in S3 (single paginated pass).
    2. Fetch all ``store_apps`` rows that could plausibly have icons.
    3. Cross-reference to find discrepancies in both directions.
    4. Apply restores (S3 → DB) and nullifications (orphans).
    """

    logger.info("Phase 1/3: Walking S3 app-icons/ prefix …")
    s3_map = list_all_icons_from_s3()
    logger.info("Phase 1/3: Walking S3 app-icons/ prefix …")

    apps_df = query_apps_icon_state()

    logger.info("Phase 2/3: Fetching app icon states …")
    logger.info("S3 icons: %d", len(s3_map))
    logger.info("DB apps: %d", len(apps_df))

    to_restore, to_null_128, to_null_64 = cross_reference(apps_df, s3_map)

    logger.info("Apps to restore from S3: %d", len(to_restore))
    logger.info("Apps to nullify (128px): %d", len(to_null_128))
    logger.info("Apps to nullify (64px): %d", len(to_null_64))

    logger.info(
        "Discrepancies — restore: %d, null-128: %d, null-64: %d",
        len(to_restore),
        len(to_null_128),
        len(to_null_64),
    )

    if to_restore:
        apply_restore(to_restore)
    if to_null_128 or to_null_64:
        apply_nullify(to_null_128, to_null_64)

    if not to_restore and not to_null_128 and not to_null_64:
        logger.info("No discrepancies found — everything is consistent.")
    else:
        logger.info("Cleanup complete.")
