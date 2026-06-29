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

Fork safety
-----------
Dramatiq can fork worker processes after module import.  To avoid sharing
file descriptors (SSH tunnels, DB sockets, Redis connections) across fork
boundaries, all connection state is initialised **lazily** — inside the actor
function body, not at module level.  ``threading.Lock`` guards against
concurrent first-access within a single process.
"""

import threading
from typing import Any

import dramatiq
import pandas as pd

from adscrawler.app_stores.scrape_stores import process_scrape_apps_and_save
from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import PostgresEngine, get_db_connection

logger = get_logger(__name__, "actor_defs")

# ---------------------------------------------------------------------------
# Fork-safe lazy-per-process connections.
# ---------------------------------------------------------------------------
# Each Dramatiq worker process (whether forked or threaded) will see its own
# copy of these module-level globals.  We initialise them on first use inside
# the actor body so that pre-fork state is never shared.

_pgdb_lock = threading.Lock()
_worker_pgdb: PostgresEngine | None = None

_redis_url = CONFIG.get("redis", {}).get("url", "redis://127.0.0.1:6379/0")
_redis_lock = threading.Lock()
_worker_redis: Any = (
    None  # redis.Redis client — type-erased to avoid import at module level
)


def _get_pgdb() -> PostgresEngine | None:
    """Return (or create) the per-process Postgres connection.

    Returns ``None`` if the initial connection fails — the caller will fall
    back to per-chunk connections (``process_scrape_apps_and_save`` opens its
    own when ``pgdb=None``).
    """
    global _worker_pgdb  # noqa: PLW0603
    if _worker_pgdb is None:
        with _pgdb_lock:
            if _worker_pgdb is None:
                try:
                    _worker_pgdb = get_db_connection()
                    logger.info("Lazy DB connection established")
                except Exception:
                    logger.exception(
                        "Lazy DB connection FAILED, falling back to per-chunk"
                    )
    return _worker_pgdb


def _get_lock_client() -> Any:  # noqa: ANN401
    """Return (or create) a per-process Redis client for lock management."""
    global _worker_redis  # noqa: PLW0603
    if _worker_redis is None:
        with _redis_lock:
            if _worker_redis is None:
                import redis as redis_module  # noqa: PLW0415

                _redis_url_parts = _redis_url.split("redis://", 1)[-1]
                _redis_host = _redis_url_parts.split(":", 1)[0]
                _redis_rest = (
                    _redis_url_parts.split(":", 1)[1]
                    if ":" in _redis_url_parts
                    else "6379/0"
                )
                _redis_port_str, _redis_db_str = (
                    _redis_rest.split("/", 1)
                    if "/" in _redis_rest
                    else (_redis_rest, "0")
                )
                _worker_redis = redis_module.Redis(
                    host=_redis_host,
                    port=int(_redis_port_str),
                    db=int(_redis_db_str) if _redis_db_str else 0,
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
            pipe.delete(f"{prefix}{app_id}")
        pipe.execute()
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
    process_icon: bool,
    thread_workers: int,
    *,
    group: int,
) -> None:
    """Shared execution body for all scrape-chunk actors.

    Extracted so each per-queue actor can delegate to the same logic without
    duplicating it four times.

    Parameters
    ----------
    group:
        Country priority group — used to derive the lock key namespace for
        this queue so lock release targets the correct keys.
    """
    logger.info(
        "Actor received chunk: store=%s group=%s apps=%d thread_workers=%d",
        store,
        group,
        len(app_data),
        thread_workers,
    )

    df_chunk = pd.DataFrame(app_data)
    df_chunk["store_app"] = df_chunk["store_app"].astype(int)
    store_app_ids = df_chunk["store_app"].tolist()

    pgdb = _get_pgdb()

    try:
        process_scrape_apps_and_save(
            df_chunk=df_chunk,
            store=store,
            process_icon=process_icon,
            thread_workers=thread_workers,
            pgdb=pgdb,
        )
        logger.info("Actor finished chunk: store=%s apps=%d", store, len(app_data))
    except Exception:
        logger.exception("Fatal error processing chunk for store=%s", store)
        raise
    finally:
        _release_locks(store_app_ids, store, group)


# ---------------------------------------------------------------------------
# Per-queue actor definitions.
# Each actor shares the same body but lives on a separate Dramatiq queue so
# workers can subscribe to only the queues they care about.
# ---------------------------------------------------------------------------


@dramatiq.actor(queue_name=QUEUE_GOOGLE_1, max_retries=3, min_backoff=15_000)
def scrape_chunk_google_1(
    app_data: list[dict[str, Any]],
    store: int,
    process_icon: bool,
    thread_workers: int,
) -> None:
    """Scrape Google Play apps (country priority group 1, e.g. US)."""
    _actor_body(app_data, store, process_icon, thread_workers, group=1)


@dramatiq.actor(queue_name=QUEUE_APPLE_1, max_retries=3, min_backoff=15_000)
def scrape_chunk_apple_1(
    app_data: list[dict[str, Any]],
    store: int,
    process_icon: bool,
    thread_workers: int,
) -> None:
    """Scrape Apple App Store apps (country priority group 1, e.g. US)."""
    _actor_body(app_data, store, process_icon, thread_workers, group=1)


@dramatiq.actor(queue_name=QUEUE_GOOGLE_2, max_retries=3, min_backoff=15_000)
def scrape_chunk_google_2(
    app_data: list[dict[str, Any]],
    store: int,
    process_icon: bool,
    thread_workers: int,
) -> None:
    """Scrape Google Play apps (country priority group 2, e.g. international)."""
    _actor_body(app_data, store, process_icon, thread_workers, group=2)


@dramatiq.actor(queue_name=QUEUE_APPLE_2, max_retries=3, min_backoff=15_000)
def scrape_chunk_apple_2(
    app_data: list[dict[str, Any]],
    store: int,
    process_icon: bool,
    thread_workers: int,
) -> None:
    """Scrape Apple App Store apps (country priority group 2, e.g. international)."""
    _actor_body(app_data, store, process_icon, thread_workers, group=2)
