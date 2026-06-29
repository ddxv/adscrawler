"""Dispatcher: lightweight message producer that replaces ``update_app_details``.

Instead of using a ``ProcessPoolExecutor`` to scrape apps locally, this module
queries Postgres, splits the returned apps into chunks, and sends each chunk as
a Dramatiq message.  Messages are routed to one of **4 queues** based on
``(store, country_priority_group)``:

+-----------------------+-------+-------+
| Queue                 | Store | Group |
+-----------------------+-------+-------+
| ``store_crawls_google_1`` | 1  | 1     |
| ``store_crawls_apple_1``  | 2  | 1     |
| ``store_crawls_google_2`` | 1  | 2     |
| ``store_crawls_apple_2``  | 2  | 2     |
+-----------------------+-------+-------+

Each queue has its own lock namespace and throttle counter so one slow queue
doesn't block the others.

The Redis broker URL is read from ``config.toml`` (``[redis]`` section).

Queue throttling
----------------
Before querying Postgres, the dispatcher uses ``LLEN`` (O(1)) on the target
queue to see how many messages (chunks) are pending.  If **≥34** chunks
(≈100k apps at 3000 apps/chunk) are already queued, the dispatch cycle for
that queue is skipped entirely.  This lets you run the dispatcher on a
frequent cron interval without overflowing Redis.

Deduplication
-------------
Before enqueuing each chunk, the dispatcher uses a **Redis pipeline with
SET NX** to atomically claim a lock for each ``store_app`` (key:
``<queue_name>:lock:<store_app>``, 24-hour TTL).  Apps already in-flight are
silently skipped.  Workers release the locks in a ``finally`` block.

Usage (Controller VPS)
----------------------
Dispatch all 4 queues in a single invocation (recommended)::

    python main.py -u --dispatch-all

Or target a single queue::

    python main.py -u --dispatch --platform google --country-priority-group=1

Usage (from code)::

    from adscrawler.dramatiq.dispatcher import dispatch_all_queues
    from adscrawler.dbcon.connection import get_db_connection

    pgdb = get_db_connection()
    dispatch_all_queues(pgdb=pgdb, process_icon=False)

    # or a single queue:
    from adscrawler.dramatiq.dispatcher import dispatch_app_details_jobs
    dispatch_app_details_jobs(
        pgdb=pgdb,
        store=1,
        process_icon=False,
        limit=200_000,
        country_priority_group=1,
    )
"""

import dramatiq
import pandas as pd
from dramatiq.brokers.redis import RedisBroker

from adscrawler.config import get_logger
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import query_store_apps_to_update

logger = get_logger(__name__, "dispatcher")

# ---------------------------------------------------------------------------
# Broker — connect to Redis.  The URL is read from config.toml so it works
# both locally (for testing) and across the VPS cluster.
# ---------------------------------------------------------------------------
from adscrawler.config import CONFIG  # noqa: E402

_redis_url = CONFIG.get("redis", {}).get("url", "redis://127.0.0.1:6379/0")
logger.info("Dispatcher connecting to Redis at %s", _redis_url)
dramatiq.set_broker(RedisBroker(url=_redis_url))

# Import the actor so we can call .send() on it.
# The actor is defined in actor_defs.py (no broker setup there).
# We import *after* setting the broker so it binds to our local Redis.
# ---------------------------------------------------------------------------
# Direct Redis client for distributed locks (not the Dramatiq broker).
# ---------------------------------------------------------------------------
import redis as redis_module  # noqa: E402

from adscrawler.dramatiq.app_stores.actor_defs import (  # noqa: E402
    queue_for,
    scrape_chunk_apple_1,
    scrape_chunk_apple_2,
    scrape_chunk_google_1,
    scrape_chunk_google_2,
)

_lock_ttl_seconds = 86400  # 24 hours — safety net for crashed workers

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

# Max pending chunks per queue before we stop dispatching into that queue.
# Each chunk holds up to ``max_chunk_size=3000`` apps, so ~34 chunks ≈ 100k apps.
_MAX_PENDING_CHUNKS = 34


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
    limit: int,
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
    if pending >= _MAX_PENDING_CHUNKS:
        logger.info(
            f"{log_info} {pending} chunks pending (>= {_MAX_PENDING_CHUNKS}), "
            f"skipping this dispatch cycle"
        )
        return
    logger.info(f"{log_info} {pending} chunks currently pending")

    if store == 1:
        thread_workers = 1
    elif store == 2:
        thread_workers = 2
    else:
        thread_workers = 1

    df = query_store_apps_to_update(
        store=store,
        pgdb=pgdb,
        limit=limit,
        country_priority_group=group,
    )

    df = df.sort_values("country_code").reset_index(drop=True)
    if df.empty:
        logger.info(f"{log_info} no apps to update")
        return

    # --- Keep the same highly-optimised chunking as update_app_details ---
    max_chunk_size = 500
    chunks: list[pd.DataFrame] = []
    for _country, country_df in df.groupby("country_code"):
        country_size = len(country_df)
        if country_size <= max_chunk_size:
            chunks.append(country_df)
        else:
            num_chunks = (country_size + max_chunk_size - 1) // max_chunk_size
            chunk_size_local = country_size // num_chunks
            for i in range(0, country_size, chunk_size_local):
                chunks.append(country_df.iloc[i : i + chunk_size_local])

    logger.info(
        f"{log_info} dispatching {len(df)} apps across {len(chunks)} chunks to Redis"
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

    total_dispatched = 0
    total_skipped = 0

    # --- Fire each chunk as a Dramatiq message ---
    for i, df_chunk in enumerate(chunks):
        # 1. Extract store_app IDs to attempt lock acquisition
        store_app_ids = df_chunk["store_app"].astype(int).tolist()

        # 2. Atomically claim locks — only keep IDs that weren't already locked
        acquired_ids = _acquire_locks(store_app_ids, store, group)

        # 3. If every app in this chunk is already in-flight, skip entirely
        if not acquired_ids:
            total_skipped += len(store_app_ids)
            if (i + 1) % 10 == 0 or (i + 1) == len(chunks):
                logger.info(
                    f"{log_info} chunk {i + 1}/{len(chunks)}: all "
                    f"{len(store_app_ids)} apps already in-flight, skipped"
                )
            continue

        # 4. Filter the DataFrame down to only the newly-acquired apps
        df_deduped = df_chunk[df_chunk["store_app"].isin(acquired_ids)]

        # 5. Serialize and send
        app_data = _serialize_chunk(df_deduped)
        actor.send(
            app_data=app_data,
            store=store,
            process_icon=process_icon,
            thread_workers=thread_workers,
        )

        total_dispatched += len(acquired_ids)
        total_skipped += len(store_app_ids) - len(acquired_ids)

        if (i + 1) % 10 == 0 or (i + 1) == len(chunks):
            logger.info(
                f"{log_info} dispatched {i + 1}/{len(chunks)} chunks "
                f"(dispatched={total_dispatched} skipped={total_skipped})"
            )

    logger.info(
        f"{log_info} finished: {total_dispatched} apps enqueued "
        f"({total_skipped} already in-flight)"
    )


def dispatch_all_queues(
    pgdb: PostgresEngine,
    process_icon: bool,
    limit: int = 200_000,
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
        group_limit = 5_000 if group == 2 else limit
        dispatch_app_details_jobs(
            pgdb=pgdb,
            store=store,
            process_icon=process_icon,
            limit=group_limit,
            country_priority_group=group,
        )
