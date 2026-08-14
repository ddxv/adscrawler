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

from adscrawler.config import CONFIG, get_logger
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import query_store_apps_to_update

logger = get_logger(__name__, "dispatcher")


_redis_url = CONFIG.get("redis", {}).get("url", "redis://127.0.0.1:6379/0")
logger.info("Dispatcher connecting to Redis at %s", _redis_url)
broker = RedisBroker(url=_redis_url)
broker.add_middleware(Prometheus())
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

# Seconds in redis, after this app is removed from queue
# Dispatcher Cron 5m < Dispatcher Lock TTL  < actor_defs time_limit
_lock_ttl_seconds = 1500


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

_MAX_PENDING_CHUNKS = 2000
MAX_CHUNK_SIZE = 40


def _queue_key(store: int, group: int) -> str:
    """Return the Dramatiq Redis list key for a given queue."""
    return f"dramatiq:{queue_for(store, group)}"


def insert_apps_into_redis(
    store_app_ids: list[int], store: int, group: int
) -> set[int]:
    """Atomically claim locks for unique store_app IDs on a queue."""
    if not store_app_ids:
        return set()

    # Deduplicate IDs so we only hit Redis ONCE per app
    unique_ids = list(set(store_app_ids))
    prefix = f"{queue_for(store, group)}:lock:"

    pipe = redis_client.pipeline()
    for app_id in unique_ids:
        pipe.set(
            f"{prefix}{app_id}",
            "in_flight",
            nx=True,
            ex=_lock_ttl_seconds,
        )
    results = pipe.execute()

    acquired_ids = {
        app_id for app_id, success in zip(unique_ids, results) if success is True
    }
    return acquired_ids


SERIALIZABLE_COLUMNS = [
    "store_app",
    "store_id",
    "country_code",
    "language",
    "html_recently_scraped",
]


def _serialize_chunk(df: pd.DataFrame) -> list[dict]:
    """Turn a DataFrame slice into a JSON-safe list of dicts for Redis transport."""
    # Only select columns that exist in the DataFrame; the rest get filled below.
    present = [c for c in SERIALIZABLE_COLUMNS if c in df.columns]
    records = df[present].copy()

    # Cast store_app to native int so JSON serialization works
    records["store_app"] = records["store_app"].astype(int)

    # Fill optional columns with None where missing
    for col in ["html_recently_scraped"]:
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
    app_limit: int,
    group: int,
) -> None:
    """Query Postgres, chunk the results, and fire each chunk over Redis.

    This function is a drop-in conceptual replacement for
    :func:`~adscrawler.app_stores.scrape_stores.update_app_details` — it keeps
    the same chunking strategy but replaces the ``ProcessPoolExecutor`` with
    distributed Dramatiq workers.

    Messages are routed to the appropriate queue based on ``(store,
    group)``.  Each queue has its own lock namespace and
    throttle counter, so one slow queue doesn't block the others.

    Parameters
    ----------
    pgdb:
        Postgres connection (Controller-local).
    store:
        Store identifier (1 = Google Play, 2 = Apple App Store).
    app_limit:
        Maximum number of apps to fetch from the database.
    group:
        Country priority group passed to ``query_store_apps_to_update``.
    """
    log_info = f"{store=} group={group} dispatcher"

    max_pending_chunks = _MAX_PENDING_CHUNKS
    pending = redis_client.llen(_queue_key(store, group)) or 0

    empty_slots = max_pending_chunks - pending

    if empty_slots < max_pending_chunks / 10:
        logger.info(f"{log_info} {pending=} queue is mostly full, skipping")
        return
    logger.info(f"{log_info} {pending=} {empty_slots=}")

    df = query_store_apps_to_update(
        store=store,
        pgdb=pgdb,
        limit=app_limit,
        country_priority_group=group,
    )

    df = df.sort_values("country_code").reset_index(drop=True)
    if df.empty:
        logger.info(f"{log_info} query returned no apps to update")
        return

    df["store_app"] = df["store_app"].astype(int)
    unique_apps = df["store_app"].unique().tolist()

    acquired_apps = insert_apps_into_redis(unique_apps, store, group)

    df_active = df[df["store_app"].isin(acquired_apps)].copy()

    if df_active.empty:
        logger.error(f"{log_info} No new locks acquired. Skipping dispatch.")
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
        )

    logger.info(
        f"{log_info} finished: queried={len(df)} inserted_apps={len(df_active)} {len(chunks)} chunks"
    )


def dispatch_all_queues(
    pgdb: PostgresEngine,
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
        app_limit = 100 if group == 2 else 5_000
        dispatch_app_details_jobs(
            pgdb=pgdb,
            store=store,
            app_limit=app_limit,
            group=group,
        )
