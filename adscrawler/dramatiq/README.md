# systemd units for Dramatiq workers

There are two template files — one per store — because Google and Apple
need different ``-p`` / ``-t`` values:

| Template file                         | Store  | `-p` | `-t` | Reasoning                                     |
|---------------------------------------|--------|------|------|-----------------------------------------------|
| ``dramatiq-worker-google@.service``   | Google | 4    | 2    | Sequential scrapers benefit from more processes |
| ``dramatiq-worker-apple@.service``    | Apple  | 2    | 4    | Slower HTTP responses benefit from more threads per process |

## Quick start

### Usage (Controller VPS)
----------------------
Dispatch all 4 queues in a single invocation (recommended)::
```
python main.py -u --dispatch-all
```

```bash
# 1. Copy the templates to systemd directory
sudo cp dramatiq-worker-google@.service /etc/systemd/system/
sudo cp dramatiq-worker-apple@.service  /etc/systemd/system/

# 2. Optionally adjust paths and -p / -t in the templates first:
#    vim dramatiq-worker-google@.service
#    Then copy after editing

# 3. Reload systemd
sudo systemctl daemon-reload

# 4. Enable + start workers for the queues you need
#    ── Google workers (4 processes × 2 threads = 8 concurrent) ──
sudo systemctl enable --now dramatiq-worker-google@store_crawls_google_1
sudo systemctl enable --now dramatiq-worker-google@store_crawls_google_2

#    ── Apple workers (2 processes × 4 threads = 8 concurrent) ──
sudo systemctl enable --now dramatiq-worker-apple@store_crawls_apple_1
sudo systemctl enable --now dramatiq-worker-apple@store_crawls_apple_2
```

The `@` in the filename is a systemd template — `%i` expands to whatever
comes after the `@`.  All four queue names are:

| Instance name               | Queue                   |
|-----------------------------|-------------------------|
| `store_crawls_google_1`    | Google Play, group 1    |
| `store_crawls_apple_1`     | Apple App Store, group 1|
| `store_crawls_google_2`    | Google Play, group 2    |
| `store_crawls_apple_2`     | Apple App Store, group 2 |

## Managing workers

```bash
# View status
systemctl status dramatiq-worker-google@store_crawls_google_1

# Tail logs
journalctl -u dramatiq-worker-google@store_crawls_google_1 -f

# Restart
sudo systemctl restart dramatiq-worker-google@store_crawls_google_1

# Stop + disable on boot
sudo systemctl disable --now dramatiq-worker-google@store_crawls_google_1
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
sudo systemctl stop 'dramatiq-worker-*'

# 2. Clear Redis — wipes queues AND distributed locks
redis-cli FLUSHDB

# 3. Re-dispatch all apps from Postgres into Redis
python main.py -u --dispatch-all

# 4. Start workers back up
sudo systemctl start 'dramatiq-worker-*'
```

The stop → flush → dispatch → start order matters: you must drain workers
before flushing Redis to avoid races between a running worker and the
``FLUSHDB``.  If your Redis instance is shared (not dedicated to Dramatiq),
use targeted key deletion instead of ``FLUSHDB``:

```bash
redis-cli KEYS 'dramatiq:*' | xargs redis-cli DEL
redis-cli KEYS '*:lock:*' | xargs redis-cli DEL
```
