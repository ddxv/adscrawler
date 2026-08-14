"""Shared Dramatiq actor definitions — no broker setup.

Four actors are defined, one per ``(store, country_priority_group)``:

+-----------------------+-------+-------+
| Actor / Queue         | Store | Group |
+-----------------------+-------+-------+
| ``scrape_chunk_google_1`` | 1  | 1     |
| ``scrape_chunk_apple_1``  | 2  | 1     |
| ``scrape_chunk_google_2`` | 1  | 2     |
| ``scrape_chunk_apple_2``  | 2  | 2     |
+-----------------------+-------+-------+

This module does **not** call ``dramatiq.set_broker()``.  The broker must be
set by the importing process (worker or dispatcher) **before** this module is
imported, so each decorator captures the correct broker for that process.

Because the worker and dispatcher run in **separate Python processes**, each
has its own module cache — ``actor_defs`` will be imported once per process,
binding the actors to whichever broker that process configured first.

Fork safety + connection hygiene
---------------------------------
Dramatiq can fork worker processes after module import.  The Postgres
connection is **not** shared — ``process_scrape_apps_and_save`` creates a
fresh connection per chunk and disposes it in its ``finally`` block (via the
``pgdb=None`` default).  This prevents dead SSH tunnels from hanging workers.

The Redis lock client is initialised **lazily** inside the actor body (fork-
safe, guarded by ``threading.Lock``), since it doesn't hold long-lived state
across chunks.
"""

import threading
from typing import Any

import dramatiq
import pandas as pd

from adscrawler.app_stores.scrape_stores import process_scrape_apps_and_save
from adscrawler.config import CONFIG, get_logger

logger = get_logger(__name__, "actor_defs")

_redis_url = CONFIG.get("redis", {}).get("url", "redis://127.0.0.1:6379/0")
_redis_lock = threading.Lock()
_worker_redis: Any = None


def _get_lock_client() -> Any:  # noqa: ANN401
    """Return (or create) a per-process Redis client for lock management."""
    global _worker_redis  # noqa: PLW0603
    if _worker_redis is None:
        with _redis_lock:
            if _worker_redis is None:
                import redis as redis_module  # noqa: PLW0415

                _worker_redis = redis_module.from_url(
                    _redis_url,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                )
                logger.info("Lazy lock client established")
    return _worker_redis


def _release_locks(store_app_ids: list[int], store: int, group: int) -> None:
    """Remove distributed locks for a list of ``store_app`` IDs on a given queue."""
    if not store_app_ids:
        return
    prefix = f"{queue_for(store, group)}:lock:"
    try:
        client = _get_lock_client()
        pipe = client.pipeline()
        for app_id in store_app_ids:
            pipe.unlink(f"{prefix}{app_id}")
        pipe.execute()
        logger.info(
            "Released %d locks for store=%s group=%s",
            len(store_app_ids),
            store,
            group,
        )
    except Exception:
        logger.warning("Failed to release locks", exc_info=True)


# ---------------------------------------------------------------------------
# Queue names — one per (store × country_priority_group) combination.
# ---------------------------------------------------------------------------
QUEUE_GOOGLE_1 = "store_crawls_google_1"
QUEUE_APPLE_1 = "store_crawls_apple_1"
QUEUE_GOOGLE_2 = "store_crawls_google_2"
QUEUE_APPLE_2 = "store_crawls_apple_2"


def queue_for(store: int, country_priority_group: int) -> str:
    """Map (store, group) to the canonical Dramatiq queue name."""
    prefix = "google" if store == 1 else "apple"
    return f"store_crawls_{prefix}_{country_priority_group}"


def _actor_body(
    app_data: list[dict[str, Any]],
    store: int,
    *,
    group: int,
) -> None:
    """Shared execution body for all scrape-chunk actors."""
    logger.info(f"Actor received chunk: {store=} {group=} apps={len(app_data)}")

    df_chunk = pd.DataFrame(app_data)
    store_app_ids = df_chunk["store_app"].unique().tolist()

    try:
        process_scrape_apps_and_save(
            df_chunk=df_chunk,
            store=store,
        )
        logger.info(f"Actor finished chunk: {store=} {group=} apps={len(app_data)}")
    except Exception:
        logger.exception(f"Fatal error processing chunk for {store=} {group=}")
        raise
    finally:
        # Group 2 apps are split across 36 chunks, locks expire naturally via TTL (1800s)
        if group == 1:
            _release_locks(store_app_ids, store, group)


@dramatiq.actor(
    queue_name=QUEUE_GOOGLE_1, max_retries=1, min_backoff=15_000, time_limit=1_200_000
)
def scrape_chunk_google_1(
    app_data: list[dict[str, Any]],
    store: int,
) -> None:
    """Scrape Google Play apps (country priority group 1, e.g. US)."""
    _actor_body(app_data, store, group=1)


@dramatiq.actor(
    queue_name=QUEUE_APPLE_1, max_retries=1, min_backoff=15_000, time_limit=1_200_000
)
def scrape_chunk_apple_1(
    app_data: list[dict[str, Any]],
    store: int,
) -> None:
    """Scrape Apple App Store apps (country priority group 1, e.g. US)."""
    _actor_body(app_data, store, group=1)


@dramatiq.actor(
    queue_name=QUEUE_GOOGLE_2, max_retries=1, min_backoff=15_000, time_limit=2_100_000
)
def scrape_chunk_google_2(
    app_data: list[dict[str, Any]],
    store: int,
) -> None:
    """Scrape Google Play apps (country priority group 2, e.g. international)."""
    _actor_body(app_data, store, group=2)


@dramatiq.actor(
    queue_name=QUEUE_APPLE_2, max_retries=1, min_backoff=15_000, time_limit=2_100_000
)
def scrape_chunk_apple_2(
    app_data: list[dict[str, Any]],
    store: int,
) -> None:
    """Scrape Apple App Store apps (country priority group 2, e.g. international)."""
    _actor_body(app_data, store, group=2)
