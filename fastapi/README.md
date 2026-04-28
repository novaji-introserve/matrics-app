# FastAPI Service

## Scheduler Workflow

The FastAPI scheduler workflow is separate from the HTTP app. It uses three containers: the API service, an RQ worker, and an RQ scheduler. The scheduler process keeps Redis schedule entries in sync with the configured alert jobs, `rq-scheduler` enqueues due jobs onto the `alerts` queue, and the worker executes the SQL/email job.

### Startup

[`docker-compose.yml`](../docker-compose.yml) defines the runtime split:

- `fastapi` runs the web app with `uvicorn`.
- `fastapi-rq-worker` runs `python -m jobs.run_worker`.
- `fastapi-rq-scheduler` runs `python -m jobs.run_scheduler`.

[`requirements.txt`](./requirements.txt) shows the scheduler stack explicitly: `rq`, `rq-scheduler`, and `fastapi-mail`.

### Configuration

[`config/settings.py`](./config/settings.py) is the central config loader.

- It reads `.env` through `BaseSettings`.
- It defines Redis, PostgreSQL, mail, and Odoo connection settings.
- Scheduled jobs are now loaded from the Odoo model `alert.rules`, not from a local JSON file.

### File-By-File Workflow

#### `docker-compose.yml`

This file wires the services together.

- `fastapi` serves the API.
- `fastapi-rq-worker` consumes jobs from Redis.
- `fastapi-rq-scheduler` maintains the schedule and runs the `rqscheduler` process.

#### `config/settings.py`

This file defines the runtime settings model.

- It loads database, Redis, mail, and Odoo settings from `.env`.
- It provides the shared mail defaults used when an Odoo alert rule is executed.

#### `repo/alert_jobs.py`

This file is the Odoo-backed alert rule repository.

- It connects to the Odoo model `alert.rules`.
- It filters only active rules with `status = 1`.
- It reads `name`, `alert_query`, `cron_string`, and `recipients`.
- It maps:
  - `job_id` from `name`, lower-cased with spaces replaced by `-`
  - `query` from `alert_query`
  - `cron` from `cron_string`
- It also fills in the email subject from the rule name and recipients from the Odoo rule.

#### `jobs/run_scheduler.py`

This is the scheduler entrypoint.

- `_get_scheduler()` connects `rq-scheduler` to Redis and the configured queue.
- `ensure_scheduled_jobs()` rebuilds the alert schedule in Redis from the current Odoo `alert.rules` data.
- It first cancels previously registered `"database-alert"` jobs, then re-registers all enabled jobs with `scheduler.cron(...)`.
- Each scheduled job points to `jobs.query_alert.run_database_alert_query(job_id)`.
- `sync_scheduled_jobs_forever()` reruns this reconciliation loop every `alert_jobs_sync_interval_seconds`.
- `main()` starts that sync loop in a daemon thread, then starts the `rqscheduler` subprocess itself.

In practice, this file treats Redis as generated scheduler state. Configuration changes are periodically reapplied.

#### `jobs/run_worker.py`

This is the worker entrypoint.

- It loads settings.
- It builds the Redis URL.
- It launches `rq worker <queue>`.

This process does not handle cron expressions. It only processes jobs that have already been enqueued by `rq-scheduler`.

#### `router/utils.py`

This file contains shared connection helpers.

- `get_postgres_dsn()` builds the DSN used by scheduled jobs to connect to PostgreSQL.
- `get_redis_client()` returns the shared Redis client.
- `get_rq_queue()` returns the queue object used elsewhere for background jobs.

#### `jobs/query_alert.py`

This is the actual scheduled task implementation.

- `run_database_alert_query(job_id)` is what the scheduler enqueues.
- It reloads settings and resolves the matching Odoo alert rule by `job_id`.
- `_validate_query()` ensures the SQL is non-empty and starts with `SELECT`.
- It skips execution if recipients are missing or mail settings are incomplete.
- It executes the configured query against PostgreSQL.
- If no rows are returned, it logs and exits without sending mail.
- If rows exist, `_build_email_body()` renders an HTML table and `send_email(...)` sends the alert.

This means alerts are row-driven: no matching rows means no email.

#### `jobs/emailer.py`

This file handles email delivery.

- `_get_mail_config()` builds the SMTP config from settings.
- `send_email_async()` sends an HTML email through `fastapi-mail`.
- `send_email()` is a synchronous wrapper used by the worker job.

#### `config/logger.py`

This file configures shared logging for both API and background processes.

- It logs to stdout.
- It also writes rotating logs to `fastapi/logs/app.log`.

### End-To-End Sequence

1. `docker-compose` starts `fastapi-rq-scheduler`.
2. `jobs/run_scheduler.py` starts the config-sync thread and the `rqscheduler` subprocess.
3. The sync thread reads active `alert.rules` records from Odoo and writes the cron jobs into Redis.
4. When a cron schedule matches, `rq-scheduler` enqueues `run_database_alert_query(job_id)` onto the `alerts` queue.
5. `fastapi-rq-worker` picks up that queued job.
6. `jobs/query_alert.py` runs the SQL query.
7. If rows are returned, `jobs/emailer.py` sends the alert email.
