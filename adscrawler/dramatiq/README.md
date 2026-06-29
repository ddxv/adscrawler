# systemd units for Dramatiq workers

## Quick start

```bash
# 1. Copy the template to systemd directory
sudo cp dramatiq-worker@.service /etc/systemd/system/

# 2. Optionally adjust paths and -p count in the template first:
#    vim dramatiq-worker@.service
#    Then copy after editing:
#    sudo cp dramatiq-worker@.service /etc/systemd/system/

# 3. Reload systemd
sudo systemctl daemon-reload

# 4. Enable + start workers for the queues you need
#    ── vps-1 (group 1, 2 processes each) ──
sudo systemctl enable --now dramatiq-worker@store_crawls_apple_1
sudo systemctl enable --now dramatiq-worker@store_crawls_apple_2

#    ── vps-2 (group 2, 2 processes each) ──
sudo systemctl enable --now dramatiq-worker@store_crawls_google_1
sudo systemctl enable --now dramatiq-worker@store_crawls_google_2
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
systemctl status dramatiq-worker@store_crawls_google_1

# Tail logs
journalctl -u dramatiq-worker@store_crawls_google_1 -f

# Restart
sudo systemctl restart dramatiq-worker@store_crawls_google_1

# Stop + disable on boot
sudo systemctl disable --now dramatiq-worker@store_crawls_google_1
```

## Tuning per VPS

Each VPS gets its **own copy** of the unit file.  Adjust:

- **`-p`** — number of Dramatiq worker processes (AKA concurrency).
  - Apple queues: `-p 2` (each process uses 3 threads internally)
  - Google queues: `-p 4` (sequential per process, so more processes = more concurrency)
- **`User`** — the system user that owns the `adscrawler` installation.
- **`WorkingDirectory` + `ExecStart` path** — match your VPS's venv location.
- **`LimitNOFILE`** — Dramatiq workers can open many file descriptors (HTTP connections).
  `65536` is a safe default; raise if you see "Too many open files" errors.
