# systemd units for Dramatiq workers

All service files follow the pattern ``crawl-<category>-<store>@.service``
so they sort together and are easy to manage.  ``%i`` is the country-priority
group number (``1`` or ``2``).

## Templates

| Template                              | Store  | `-p` | `-t` | Reasoning                                     |
|---------------------------------------|--------|------|------|-----------------------------------------------|
| ``crawl-store-apps-google@.service``  | Google | 4    | 2    | Sequential scrapers benefit from more processes |
| ``crawl-store-apps-apple@.service``   | Apple  | 2    | 4    | Slower HTTP responses benefit from more threads per process |

## Quick start

### Usage (Controller VPS)

Dispatch all 4 queues in a single invocation (recommended)::

    python main.py -u --dispatch-all

### Setting up workers

```bash
# 1. Copy the templates to systemd directory
sudo cp crawl-store-apps-google@.service /etc/systemd/system/
sudo cp crawl-store-apps-apple@.service  /etc/systemd/system/

# 2. Optionally adjust paths and -p / -t in the templates first:
#    vim crawl-store-apps-google@.service
#    Then copy after editing

# 3. Reload systemd
sudo systemctl daemon-reload

# 4. Enable + start workers
#    ── Google (4 processes × 2 threads = 8 concurrent) ──
sudo systemctl enable --now crawl-store-apps-google@1
sudo systemctl enable --now crawl-store-apps-google@2

#    ── Apple (2 processes × 4 threads = 8 concurrent) ──
sudo systemctl enable --now crawl-store-apps-apple@1
sudo systemctl enable --now crawl-store-apps-apple@2
```

## Managing workers

```bash
# View status
systemctl status crawl-store-apps-google@1

# Tail logs
journalctl -u crawl-store-apps-google@1 -f

# Restart
sudo systemctl restart crawl-store-apps-google@1

# Stop + disable on boot
sudo systemctl disable --now crawl-store-apps-google@1

# Stop all app-crawl workers
sudo systemctl stop 'crawl-store-apps-*'
```

## Adding new worker types

This naming scheme scales cleanly.  Future workers would get their own
template and use the same store × group pattern:

```bash
sudo systemctl enable --now crawl-store-keywords-google@1
sudo systemctl enable --now crawl-ads-txt-sites@1
```

## Tuning per VPS

Each VPS gets its **own copy** of the unit files.  Adjust:

- **`-p`** — number of Dramatiq worker processes (AKA concurrency).
- **`-t`** — number of Dramatiq worker threads per process.
- **`User`** — the system user that owns the ``adscrawler`` installation.
- **`WorkingDirectory` + `ExecStart` path** — match your VPS's venv location.
- **`LimitNOFILE`** — Dramatiq workers can open many file descriptors (HTTP connections).
  ``65536`` is a safe default; raise if you see "Too many open files" errors.

## Cold restart (after code changes)

Use this flow when pushing updated worker or dispatcher code to reset
everything cleanly:

```bash
# 1. Stop all workers (no in-flight state to lose — locks release in finally)
sudo systemctl stop 'crawl-store-apps-*'

# 2. Clear Redis — wipes queues AND distributed locks
redis-cli FLUSHDB

# 3. Re-dispatch all apps from Postgres into Redis
python main.py -u --dispatch-all

# 4. Start workers back up
sudo systemctl start 'crawl-store-apps-*'
```

The stop → flush → dispatch → start order matters: you must drain workers
before flushing Redis to avoid races between a running worker and the
``FLUSHDB``.  If your Redis instance is shared (not dedicated to Dramatiq),
use targeted key deletion instead of ``FLUSHDB``:

```bash
redis-cli KEYS 'dramatiq:*' | xargs redis-cli DEL
redis-cli KEYS '*:lock:*' | xargs redis-cli DEL
```
