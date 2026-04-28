# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

**iComply** is a suite of custom Odoo 16.0 addons for financial compliance management (AML/CFT) at Nigerian financial institutions. It is deployed as custom addons inside an existing Odoo 16.0 installation — there is no standalone server in this repo. All modules in this repo are Odoo addon packages.

**References:**
- Online docs: https://www.odoo.com/documentation/16.0/nl/index.html for Odoo 16.0 framework questions (ORM, views, controllers, QWeb, OWL, security).
- Local Odoo 16 source: `/home/jonathan/projects/odoo/16/src/odoo-16.0/` — use this as the primary reference for how core models, views, and addons are implemented. Core addons are in `addons/`, the ORM and HTTP framework are in `odoo/`. When in doubt about how a base model works (e.g. `res.partner`, `mail.thread`, `ir.cron`), read the source directly.

---

## Docker Environment

The local development environment is a Docker Compose stack at `/home/jonathan/projects/odoo/16/odoo-16/`. This repo (`icomply_odoo`) is bind-mounted into the Odoo container as a second addons path.

### Services

| Service | Host Port | Purpose |
|---|---|---|
| `odoo16` | `10016` (HTTP), `20016` (longpolling/websocket), `8073` (custom CSV import websocket) | Odoo application |
| `db` | `5432` | PostgreSQL 15 |
| `pgbouncer` | `5433` | Connection pooler (Odoo connects here, not directly to db) |
| `redis` | `6379` | Session store + ORM cache |
| `fastapi` | `8001` | Fraud detection API + RQ workers/scheduler |

### Key paths inside the container

| Host path | Container path | Purpose |
|---|---|---|
| `/home/jonathan/projects/icomply_odoo` | `/mnt/icomply_odoo` | **This repo** — live-mounted, changes are immediately visible |
| `./addons` | `/mnt/extra-addons` | Other third-party/community addons |
| `./etc/odoo.conf` | `/etc/odoo/odoo.conf` | Odoo configuration |
| `/home/jonathan/storage/odoo16/data` | `/root/.local/share/Odoo` | Odoo filestore |

`odoo.conf` sets `addons_path = /mnt/extra-addons,/mnt/icomply_odoo`, so all modules in this repo are automatically discoverable by Odoo.

### Starting and stopping

```bash
cd /home/jonathan/projects/odoo/16/odoo-16

# Start all services (detached)
docker compose up -d

# Stop all services
docker compose down

# Restart only Odoo (e.g. after a Python model change)
docker compose restart odoo16

# Tail Odoo logs
docker compose logs -f odoo16
# or read the log file directly:
# /home/jonathan/projects/odoo/16/odoo-16/etc/odoo-server.log
```

### Installing or updating a module

Changes to Python models, `__manifest__.py`, XML data/views, or security files require a module update. Run inside the container:

```bash
docker compose exec odoo16 odoo -c /etc/odoo/odoo.conf -d <database> -u <module_name> --stop-after-init
```

Or to install a new module:

```bash
docker compose exec odoo16 odoo -c /etc/odoo/odoo.conf -d <database> -i <module_name> --stop-after-init
```

> **Note:** JavaScript/XML template/CSS changes in `static/` do not need a module update — a browser hard-refresh (Ctrl+Shift+R) is sufficient when `dev_mode = all` is set in `odoo.conf`.

### Python dependencies

On container startup, `entrypoint.sh` runs `pip install -r /etc/odoo/requirements.txt`. To add a new Python dependency:
1. Add it to the relevant module's `requirements.txt`
2. Also add it to `/home/jonathan/projects/odoo/16/odoo-16/etc/requirements.txt` so it is installed on the next container start
3. Or install immediately: `docker compose exec odoo16 pip install <package>`

The main dependency files:
- `compliance_management/requirements.txt` — covers most of the stack
- `etl_manager/requirements.txt` — multi-database connectors (psycopg2, pyodbc, cx_Oracle, snowflake, bigquery, etc.)

### odoo.conf highlights

- `dev_mode = all` — developer mode enabled (auto-reloads assets, shows technical menus)
- `workers = 4` — multiprocessing mode
- `server_wide_modules` includes `queue_job`, `fpg_redis_session`, `ro_cache_redis`
- DB connection goes through **pgbouncer** on port 5433, not directly to PostgreSQL
- Redis is used for both **session storage** (`fpg_redis_session`) and **ORM cache** (`ro_cache_redis`)

### CI/CD

GitHub Actions (`.github/workflows/deploy_dev.yml`) deploys to the remote dev server via SSH on push to `main` or `production` branches — it does a `git pull` into `/home/ubuntu/odoo/custom_addons/icomply_odoo/`. No automated module update step is run in CI; that must be done manually on the server after deploy.

---

## Module Dependency Graph

Understanding which modules depend on which is critical when making changes:

```
base Odoo
  └── compliance_management   ← Central hub; most modules depend on this
        ├── alert_management
        ├── case_management  → alert_management
        ├── nfiu_reporting   → case_management, regulatory_reports
        ├── regulatory_reports
        ├── internal_control → icomply_dashboard, bi_sql_editor,
        │                      psql_query_execute, alert_management,
        │                      transaction_screening
        └── transaction_screening
  └── access_control → session_control
  └── etl_manager            ← standalone, only needs base + queue_job
  └── rule_book / rule_book_api
```

**Key external Odoo dependencies:** `queue_job` (OCA) for async jobs, `bus` for WebSocket push, `mail` for chatter/email, `hr` for employee records.

---

## Code Architecture

### Module Structure

Every module follows the standard Odoo layout:

```
<module>/
  __manifest__.py       # name, version, depends[], data[], assets{}
  __init__.py
  models/               # Python model classes (business logic)
  views/                # XML — form/list/kanban views, menus, actions
  security/
    groups.xml          # res.groups definitions
    ir.model.access.csv # model-level CRUD access per group
  data/
    schedules/          # ir.cron definitions
    demo_data/          # seed records (CSV, XML)
    email_templates/    # mail.template XML
  static/src/
    components/         # OWL components (JS + XML template + SCSS co-located)
    js/                 # standalone JS services
    xml/                # standalone QWeb templates
    lib/                # bundled third-party JS (e.g. chart.umd.min.js)
```

### OWL Frontend Components

Frontend components live in `compliance_management/static/src/components/`. The pattern is:

```
components/<feature>/
  js/<Component>.js      # OWL component class
  xml/<Component>.xml    # QWeb template
  scss/<Component>.scss  # styles (optional)
```

Assets must be explicitly declared in `__manifest__.py` under `assets → web.assets_backend`. **Order matters:** XML templates must be declared before the JS files that reference them.

OWL components use:
- `owl.Component` with `setup()` hooks
- `useService('orm')`, `useService('rpc')`, `useService('action')`, `useService('bus_service')` for service injection
- `useState()` for reactive local state
- `useBusListener()` custom hook for real-time server push notifications (bus service)
- Chart.js loaded as a UMD bundle (`static/lib/chart.umd.min.js`)

### Key Data Models (compliance_management)

| Model | Description |
|---|---|
| `res.partner` (inherited) | Customers — extended with KYC fields, risk score, compliance status |
| `res.partner.account` | Bank accounts linked to customers |
| `res.customer.transaction` | Financial transactions; base for NFIU reporting |
| `res.risk.assessment` | Risk evaluations with weighted scoring lines |
| `res.customer.screening` | KYC/AML screening records |
| `res.pep` | PEP/sanctions list entries |
| `res.adverse_media` | Adverse media monitoring with keyword matching |
| `res.edd` / `res.customer.edd` | Enhanced Due Diligence records |

`nfiu_reporting` models inherit from `compliance_management` ones — `nfiu.transaction` extends `res.customer.transaction`.

### Async Processing

Heavy operations use `queue_job` (OCA) for background processing:
- Large CSV imports (chunked upload + background processing)
- ETL synchronisation jobs
- Report generation (`regulatory_reports`)
- Risk assessment recalculation

Scheduled work uses Odoo `ir.cron` records defined in `data/schedules/`.

### Dashboard / Materialized Views

`compliance_management` maintains PostgreSQL materialized views for dashboard performance. Refresh is triggered via cron (`data/schedules/refresh_charts_materialized_views.xml`). See `models/res_materialized_views.py` and `models/dashboard_chart_view_refresher.py`.

### SQL Safety

Any module that executes raw SQL (especially `regulatory_reports`, `psql_query_execute`, `bi_sql_editor`) must follow the 5-layer injection defence documented in `SQL_INJECTION_PROTECTION_IMPLEMENTATION_GUIDE.md`:
1. Input validation
2. Pattern detection (170+ blocked patterns via `sqlparse`)
3. Header validation
4. Model-level constraints
5. Parameterised queries with timeouts

Never use string formatting or `%` interpolation to build SQL — always use `self.env.cr.execute(sql, params)` with a params tuple.

---

## Common Patterns

**Adding a new field to a model:**
1. Add the field in `models/<file>.py`
2. Add it to the view XML in `views/<file>.xml`
3. If it needs access control, update `security/ir.model.access.csv`
4. Bump the module version in `__manifest__.py` (Odoo uses this to detect upgrades)

**Adding a cron job:**
- Create an XML record of model `ir.cron` in `data/schedules/`
- Reference the method as `model_id.method_name` — method must be on a model class
- Register the file in `__manifest__.py` under `data`

**Adding an OWL component:**
1. Create `static/src/components/<feature>/js/`, `xml/`, `scss/` files
2. Register the template XML **before** the JS file in `__manifest__.py` assets
3. Register the action client tag in `main.js` if it's a top-level action

**Accessing the database directly (ETL / reporting):**
- Use `self.env.cr` for Odoo-managed connections
- For ETL multi-database connections use the connector factories in `etl_manager/models/` (`etl_fast_sync_postgres.py`, `etl_fast_sync_mysql.py`, etc.)

---

## Security Groups

Defined per-module in `security/groups.xml`. The primary groups for compliance access are in `compliance_management/security/groups.xml`. Always scope new model access rules to the appropriate group in `ir.model.access.csv`.
