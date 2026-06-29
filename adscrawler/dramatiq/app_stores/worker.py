"""Worker entry-point for Dramatiq.

Sets the Redis broker and imports all 4 per-queue actors.  The actors bind
to this broker at import time because we configure the broker *first*.

**You must pass ``-Q`` to select which queue(s) this worker process consumes
from.**  Without it Dramatiq will subscribe to *all* registered queues, which
is almost never what you want in a multi-VPS setup.

Examples::

    # Single queue per process (recommended)
    python -m dramatiq adscrawler.dramatiq.app_stores.worker -Q store_crawls_google_1

    # Two queues in one process (e.g. both Apple queues on the same VPS)
    python -m dramatiq adscrawler.dramatiq.app_stores.worker -Q store_crawls_apple_1 store_crawls_apple_2

Auto-restart via systemd::

    # Enable a worker for a specific queue (runs on boot + auto-restart)
    sudo systemctl enable --now dramatiq-worker@store_crawls_google_1

    # Check status
    systemctl status dramatiq-worker@store_crawls_google_1

    # Tail logs
    journalctl -u dramatiq-worker@store_crawls_google_1 -f

Template unit files are provided in ``deploy/systemd/dramatiq-worker@.service``.
Copy them to ``/etc/systemd/system/`` and adjust paths/``-p`` for your VPS.

Dramatiq auto-discovers actors in modules that call ``dramatiq.set_broker()``.
"""

import dramatiq
from dramatiq.brokers.redis import RedisBroker

from adscrawler.config import CONFIG, get_logger

logger = get_logger(__name__, "worker")

# ---------------------------------------------------------------------------
# 1. Set the broker FIRST — the actor decorator captures it at import time.
# ---------------------------------------------------------------------------
_redis_url = CONFIG.get("redis", {}).get("url", "redis://127.0.0.1:6379/0")
logger.info("Worker broker: %s", _redis_url)
dramatiq.set_broker(RedisBroker(url=_redis_url))

# ---------------------------------------------------------------------------
# 2. Now import the actors — they will bind to the broker we just set.
# ---------------------------------------------------------------------------
# The actors live in actor_defs.py so neither worker.py nor dispatcher.py
# causes the other's broker to leak into the actor.
from adscrawler.dramatiq.app_stores.actor_defs import (  # noqa: E402, F401
    scrape_chunk_apple_1,
    scrape_chunk_apple_2,
    scrape_chunk_google_1,
    scrape_chunk_google_2,
)
