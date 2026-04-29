# Compliance Cron — Docker Container Guide

## What This Container Does

`compliance-cron` is an isolated Docker container that runs three compliance
jobs on separate schedules without relying on Odoo's built-in cron scheduler.
All jobs call Odoo via XML-RPC — the container needs no Odoo source code,
no database connection, and no Python virtualenv matching Odoo's dependencies.

| Job | Script | Schedule | What it does |
|-----|--------|----------|-------------|
| Transaction Screening | `screen_daemon.py` | Every 5 min | Fetches all `state=new` transactions and runs them through active screening rules + AML detection |
| Escalation Engine | `run_escalation.py` | Every hour | Checks open alerts past their TAT (Turn-Around Time) and sends escalation emails |
| AML Profile Rebuild | `aml_profile_rebuild.py` | Sundays 02:00 | Rebuilds all customer behavioural profiles from full transaction history |

---

## Files

```
cron_folder/
├── Dockerfile             — container image (python:3.11-slim + python-dotenv)
├── run_forever.sh         — entrypoint: daemon with separate loops per job
├── run_all.sh             — manual one-shot trigger (all three jobs in sequence)
├── screen_daemon.py       — transaction screening runner
├── run_escalation.py      — escalation engine runner
└── aml_profile_rebuild.py — AML customer profile rebuild runner
```

---

## How the Scheduling Works

The container runs `run_forever.sh` as its entrypoint. There is no `crond` or
crontab inside the container — scheduling is done with pure bash sleep loops,
following the same pattern as `ETL_script/run_forever.sh`.

```
run_forever.sh
  │
  ├── run_escalation_loop()   [background]
  │     sleep ESCALATION_INTERVAL → run_escalation.py → repeat
  │
  ├── run_rebuild_loop()      [background]
  │     sleep 3600 → check day/hour → run aml_profile_rebuild.py on Sundays 02:xx → repeat
  │
  └── main loop               [foreground — keeps container alive]
        run screen_daemon.py → sleep SCREEN_INTERVAL → repeat
```

The main loop (screening) keeps the container process alive. If Docker stops
the container, `restart: unless-stopped` brings it back up automatically.

### Why background loops instead of crond?

- No extra packages (`cron`) needed in the image — smaller and faster to build
- Logs go directly to stdout/file without cron's output routing quirks
- Consistent with how ETL and other containers in this stack work
- Intervals are controllable via environment variables without rebuilding the image

---

## Environment Variables

Set in `.env` (shared with all services) or overridden in `docker-compose.yml`:

| Variable | Default | Purpose |
|----------|---------|---------|
| `ODOO_URL` | — | Odoo base URL e.g. `http://odoo:8069` |
| `ODOO_DATABASE` | — | Database name e.g. `compliance_db2` |
| `ODOO_USERNAME` | — | Odoo login username |
| `ODOO_PASSWORD` | — | Odoo login password |
| `SCREEN_INTERVAL` | `300` | Seconds between screening runs (default 5 min) |
| `ESCALATION_INTERVAL` | `3600` | Seconds between escalation runs (default 1 hour) |

---

## Logging

All output is written to `/var/log/icomply/ServerLog.log` inside the container,
which is mounted from `./logs` on the host — **the same file Odoo writes to**.
The `elasticsearch-logger` service already monitors this path, so compliance
job output is automatically shipped to Elasticsearch alongside Odoo logs.

`docker logs compliance-cron` shows nothing (`logging: driver: "none"`) — read
the log file instead:

```bash
tail -f ./logs/ServerLog.log | grep -E "Screening|Escalation|Rebuild|✓|✗"
```

### Log format

```
2026-04-23 02:00:01: ========================================
2026-04-23 02:00:01: Compliance Daemon starting
2026-04-23 02:00:01:   Screen interval     : 300s
2026-04-23 02:00:01:   Escalation interval : 3600s
2026-04-23 02:00:01:   AML rebuild         : Sundays 02:00
2026-04-23 02:00:01: ========================================
2026-04-23 02:00:01: --- Transaction Screening ---
2026-04-23 02:00:02: ✓ Screening complete
2026-04-23 03:00:01: --- Escalation Engine ---
2026-04-23 03:00:03: ✓ Escalation complete
2026-04-27 02:00:01: --- AML Profile Rebuild (weekly) ---
2026-04-27 02:00:18: ✓ AML rebuild complete
```

---

## Docker Setup

### docker-compose.yml entry

```yaml
compliance-cron:
  build:
    context: ./cron_folder
    dockerfile: Dockerfile
  container_name: compliance-cron
  logging:
    driver: "none"
  networks:
    - app_network
  user: root
  restart: unless-stopped
  env_file:
    - .env
  environment:
    - SCREEN_INTERVAL=300
    - ESCALATION_INTERVAL=3600
  extra_hosts:
    - "host.docker.internal:host-gateway"
  volumes:
    - ./logs:/var/log/icomply
  depends_on:
    - odoo
```

### Build and start

```bash
docker compose build compliance-cron
docker compose up -d compliance-cron
```

### Check it is running

```bash
docker ps | grep compliance-cron
```

---

## Running Jobs Manually

### From outside the container (host)

```bash
# Run all three jobs once in sequence
docker exec compliance-cron /app/run_all.sh

# Force-escalate all open alerts (skip TAT check)
docker exec -e FORCE=1 compliance-cron python3 /app/run_escalation.py
```

### Run individual scripts directly (no container)

From the repo root, with `.env` in place:

```bash
cd /home/novaji/odoo
python3 cron_folder/screen_daemon.py
python3 cron_folder/run_escalation.py
python3 cron_folder/aml_profile_rebuild.py
FORCE=1 python3 cron_folder/run_escalation.py
```

---

## Script Reference

### `screen_daemon.py` — Transaction Screening

Searches for all transactions in `state=new` and calls `multi_screen()` on
them via XML-RPC. `multi_screen()` runs each transaction through:
1. Active rule-based screening (SQL and Python conditions)
2. AML detection (velocity, structuring, anomaly, dormant)

Exits immediately with code 0 if there are no `new` transactions.

**Odoo method called:** `res.customer.transaction.multi_screen([ids])`

---

### `run_escalation.py` — Escalation Engine

Counts open `alert.history` records that have an escalation matrix assigned
and are not yet complete. If none exist, exits cleanly. Otherwise calls the
escalation engine which evaluates TAT (Turn-Around Time) thresholds and sends
escalation emails to the configured recipients.

**`FORCE=1`:** skips the TAT check and escalates all open alerts immediately.
Useful for testing escalation email templates.

**Odoo method called:** `fsdh.escalation.engine.run_escalation(force=False)`

---

### `aml_profile_rebuild.py` — AML Profile Rebuild

Calls `_cron_rebuild_profiles()` which runs a single SQL aggregate across
all non-cancelled transactions grouped by customer, then upserts the
`res.aml.customer.profile` records with the recalculated mean, M2
(variance accumulator), and transaction count.

This corrects any statistical drift that accumulates from incremental
Welford updates and ensures the anomaly baseline reflects the full
transaction history, not just transactions screened since the last rebuild.

The Odoo built-in cron for this job (`cron_rebuild_aml_profiles`) is set to
`active=False` — the container owns this schedule.

**Odoo method called:** `res.aml.customer.profile._cron_rebuild_profiles([])`

---

## Concurrency and Safety

Each script is a single-pass runner that exits after one execution. The
daemon loops call them sequentially, not in parallel, so there is no risk
of two screening runs overlapping within the same container.

If you scale to multiple `compliance-cron` replicas (e.g. Kubernetes), the
screening script could process the same transactions twice. The guard against
this is that `action_screen()` writes `state=done` on the transaction before
the RPC call returns — a second concurrent run would find the same transaction
already in `state=done` and skip it on the next `search(state=new)` call.

The AML rebuild is idempotent — running it twice produces the same result.
