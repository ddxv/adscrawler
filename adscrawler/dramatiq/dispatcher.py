"""Dispatcher: lightweight message producer that replaces ``update_app_details``.

Queries Postgres, splits the returned apps into chunks, and sends each chunk
as a Dramatiq message to one of 4 queues (google/apple × group 1/2).  Each
queue has its own lock namespace and throttle counter so one slow queue
doesn't block the others.

See ``adscrawler/dramatiq/README.md`` for usage instructions.
"""

import dramatiq
import pandas as pd
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware.prometheus import Prometheus

from adscrawler.config import get_logger
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import query_store_apps_to_update
from adscrawler.config import CONFIG

logger = get_logger(__name__, "dispatcher")


_redis_url = CONFIG.get("redis", {}).get("url", "redis://127.0.0.1:6379/0")
logger.info("Dispatcher connecting to Redis at %s", _redis_url)
broker = RedisBroker(url=_redis_url)
broker.add_middleware(Prometheus(http_host="0.0.0.0", http_port=9191))
dramatiq.set_broker(broker)

# We import *after* setting the broker so it binds to our local Redis.
import redis as redis_module  # noqa: E402

from adscrawler.dramatiq.app_stores.actor_defs import (  # noqa: E402
    queue_for,
    scrape_chunk_apple_1,
    scrape_chunk_apple_2,
    scrape_chunk_google_1,
    scrape_chunk_google_2,
)

# Time in redis, after this app is removed from queue
_lock_ttl_seconds = 10800

# Extract host/port/db from the configured URL so we always talk to the
# same Redis instance the broker uses.
_redis_url_parts = _redis_url.split("redis://", 1)[-1]
_redis_host = _redis_url_parts.split(":", 1)[0]
_redis_rest = _redis_url_parts.split(":", 1)[1] if ":" in _redis_url_parts else "6379/0"
_redis_port_str, _redis_db_str = (
    _redis_rest.split("/", 1) if "/" in _redis_rest else (_redis_rest, "0")
)
_redis_port = int(_redis_port_str)
_redis_db = int(_redis_db_str) if _redis_db_str else 0

redis_client = redis_module.Redis(
    host=_redis_host,
    port=int(_redis_port),
    db=_redis_db,
    socket_connect_timeout=5,
    socket_timeout=5,
)

_MAX_PENDING_CHUNKS = 1500
MAX_CHUNK_SIZE = 40


def _queue_key(store: int, group: int) -> str:
    """Return the Dramatiq Redis list key for a given queue."""
    return f"dramatiq:{queue_for(store, group)}"


def _lock_prefix(store: int, group: int) -> str:
    """Return the lock key prefix for a given queue."""
    return f"{queue_for(store, group)}:lock:"


def _count_pending_chunks(store: int, group: int) -> int:
    """O(1) check of how many messages are waiting in a specific Dramatiq queue."""
    try:
        return redis_client.llen(_queue_key(store, group)) or 0
    except Exception:
        logger.warning(
            "Failed to check queue length for store=%s group=%s", store, group
        )
        return 0


def _acquire_locks(store_app_ids: list[int], store: int, group: int) -> list[int]:
    """Atomically claim locks for a list of ``store_app`` IDs on a specific queue.

    Returns only the IDs that were *not* already locked.
    """
    prefix = _lock_prefix(store, group)
    pipe = redis_client.pipeline()
    for app_id in store_app_ids:
        pipe.set(
            f"{prefix}{app_id}",
            "in_flight",
            nx=True,
            ex=_lock_ttl_seconds,
        )
    results = pipe.execute()
    acquired = [
        app_id for app_id, success in zip(store_app_ids, results) if success is True
    ]
    return acquired


SERIALIZABLE_COLUMNS = [
    "store_app",
    "store_id",
    "country_code",
    "language",
    "icon_url_100",
    "html_recently_scraped",
]


def _serialize_chunk(df: pd.DataFrame) -> list[dict]:
    """Turn a DataFrame slice into a JSON-safe list of dicts for Redis transport."""
    # Only select columns that exist in the DataFrame; the rest get filled below.
    present = [c for c in SERIALIZABLE_COLUMNS if c in df.columns]
    records = df[present].copy()

    # Cast store_app to native int so JSON serialization works
    records["store_app"] = records["store_app"].astype(int)

    # Fill optional columns with None where missing (e.g. group-2 queries
    # don't return html_recently_scraped).
    for col in ("icon_url_100", "html_recently_scraped"):
        if col not in records.columns:
            records[col] = None

    # html_recently_scraped comes in as nullable bool — convert to native types
    if "html_recently_scraped" in records.columns:
        records["html_recently_scraped"] = records["html_recently_scraped"].apply(
            lambda x: bool(x) if pd.notna(x) else None
        )

    return records.to_dict(orient="records")


def dispatch_app_details_jobs(
    pgdb: PostgresEngine,
    store: int,
    process_icon: bool,
    app_limit: int,
    country_priority_group: int,
) -> None:
    """Query Postgres, chunk the results, and fire each chunk over Redis.

    This function is a drop-in conceptual replacement for
    :func:`~adscrawler.app_stores.scrape_stores.update_app_details` — it keeps
    the same chunking strategy but replaces the ``ProcessPoolExecutor`` with
    distributed Dramatiq workers.

    Messages are routed to the appropriate queue based on ``(store,
    country_priority_group)``.  Each queue has its own lock namespace and
    throttle counter, so one slow queue doesn't block the others.

    Parameters
    ----------
    pgdb:
        Postgres connection (Controller-local).
    store:
        Store identifier (1 = Google Play, 2 = Apple App Store).
    process_icon:
        Whether workers should resize and upload app icons to S3.
    limit:
        Maximum number of apps to fetch from the database.
    country_priority_group:
        Country priority group passed to ``query_store_apps_to_update``.
    """
    log_info = f"{store=} group={country_priority_group} dispatcher"

    # --- Throttle: don't enqueue more if this queue is already full ---
    group = country_priority_group
    pending = _count_pending_chunks(store, group)
    empty_slots = _MAX_PENDING_CHUNKS - pending
    if empty_slots < _MAX_PENDING_CHUNKS / 10:
        logger.info(f"{log_info} {pending=} queue is mostly full, skipping")
        return
    logger.info(f"{log_info} {pending=} {empty_slots=}")

    query_app_limit = min([empty_slots * MAX_CHUNK_SIZE, app_limit])

    # We do need a larger query to handle possibly locked apps still in queue
    query_app_limit *= 2

    df = query_store_apps_to_update(
        store=store,
        pgdb=pgdb,
        limit=query_app_limit,
        country_priority_group=group,
    )

    df = df.sort_values("country_code").reset_index(drop=True)
    if df.empty:
        logger.info(f"{log_info} query returned no apps to update")
        return

    all_app_ids = df["store_app"].astype(int).tolist()
    acquired_ids = _acquire_locks(all_app_ids, store, group)
    df_active = df[df["store_app"].isin(acquired_ids)].copy()
    if df_active.empty:
        logger.info(f"{log_info} No new locks acquired. Skipping dispatch.")
        return

    chunks: list[pd.DataFrame] = []
    for _country, country_df in df_active.groupby("country_code"):
        country_size = len(country_df)
        if country_size <= MAX_CHUNK_SIZE:
            chunks.append(country_df)
        else:
            num_chunks = (country_size + MAX_CHUNK_SIZE - 1) // MAX_CHUNK_SIZE
            chunk_size_local = country_size // num_chunks
            for i in range(0, country_size, chunk_size_local):
                chunks.append(country_df.iloc[i : i + chunk_size_local])

    logger.info(
        f"{log_info} dispatching {len(df_active)} apps across {len(chunks)} chunks to Redis"
    )

    # --- Select the right actor for this queue ---
    actor_map = {
        (1, 1): scrape_chunk_google_1,
        (2, 1): scrape_chunk_apple_1,
        (1, 2): scrape_chunk_google_2,
        (2, 2): scrape_chunk_apple_2,
    }
    actor = actor_map.get((store, group))
    if actor is None:
        logger.error(f"{log_info} no actor registered for store={store} group={group}")
        return

    for i, df_chunk in enumerate(chunks):
        app_data = _serialize_chunk(df_chunk)
        actor.send(
            app_data=app_data,
            store=store,
            process_icon=process_icon,
        )

    logger.info(
        f"{log_info} finished: queried={len(df)} apps={len(df_active)} {len(chunks)} chunks"
    )


def dispatch_all_queues(
    pgdb: PostgresEngine,
    process_icon: bool,
    limit: int = 20_000,
) -> None:
    """Dispatch all 4 store×group combinations in a single call.

    Calls ``dispatch_app_details_jobs`` for each of the four
    ``(store, country_priority_group)`` pairs.  Group 2 queues (international)
    are limited to 5,000 apps per cycle since they have fewer worker resources.

    Each queue has its own throttle and lock namespace, so one being full
    won't skip the others.

    This is the recommended entry point for cron — a single ``* * * * *``
    invocation replaces four separate cron jobs.
    """
    for store, group in ((1, 1), (2, 1), (1, 2), (2, 2)):
        app_limit = 5_000 if group == 2 else limit
        dispatch_app_details_jobs(
            pgdb=pgdb,
            store=store,
            process_icon=process_icon,
            app_limit=app_limit,
            country_priority_group=group,
        )
