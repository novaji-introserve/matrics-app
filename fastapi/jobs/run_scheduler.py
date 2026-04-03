import logging
import subprocess
import threading
import time

import odoo_connect
from requests import exceptions as requests_exceptions
from redis import Redis
from rq_scheduler import Scheduler

from config.logger import setup_logging
from config.odoo import get_odoo_client
from config.settings import load_settings
from jobs.query_alert import run_database_alert_query
from repo.alert_jobs import list_alert_jobs


logger = logging.getLogger("app")

ODOO_RETRYABLE_EXCEPTIONS = (
    odoo_connect.OdooConnectionError,
    requests_exceptions.ConnectionError,
)


def _get_scheduler() -> Scheduler:
    settings = load_settings(refresh=True)
    redis_connection = Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        password=settings.redis_password,
    )
    return Scheduler(queue_name=settings.rq_queue_name, connection=redis_connection)


def _build_schedule_key(job_id: str, cron: str) -> str:
    return f"{job_id}|{cron}"


def _scheduled_jobs_by_key(scheduler: Scheduler) -> dict[str, object]:
    scheduled_jobs: dict[str, object] = {}
    for job in scheduler.get_jobs():
        if job.meta.get("scheduler_group") != "database-alert":
            continue
        scheduled_job_id = job.meta.get("scheduler_job_id")
        scheduled_cron = job.meta.get("scheduler_cron")
        if not scheduled_job_id or not scheduled_cron:
            continue
        scheduled_jobs[_build_schedule_key(scheduled_job_id, scheduled_cron)] = job
    return scheduled_jobs


def ensure_scheduled_jobs() -> None:
    settings = load_settings(refresh=True)
    scheduler = _get_scheduler()
    alert_jobs = list_alert_jobs()
    existing_jobs = _scheduled_jobs_by_key(scheduler)
    desired_keys = {
        _build_schedule_key(alert_job.job_id, alert_job.cron): alert_job
        for alert_job in alert_jobs
    }

    for schedule_key, job in existing_jobs.items():
        if schedule_key not in desired_keys:
            scheduler.cancel(job)
            logger.info(
                "Removed rq-scheduler job id=%s cron=%s",
                job.meta.get("scheduler_job_id"),
                job.meta.get("scheduler_cron"),
            )

    for schedule_key, alert_job in desired_keys.items():
        if schedule_key in existing_jobs:
            continue
        scheduler.cron(
            alert_job.cron,
            func=run_database_alert_query,
            args=[alert_job.job_id],
            queue_name=settings.rq_queue_name,
            result_ttl=3600,
            timeout=300,
            meta={
                "scheduler_group": "database-alert",
                "scheduler_job_id": alert_job.job_id,
                "scheduler_cron": alert_job.cron,
            },
        )
        logger.info(
            "Registered rq-scheduler job id=%s on cron=%s queue=%s",
            alert_job.job_id,
            alert_job.cron,
            settings.rq_queue_name,
        )


def wait_for_odoo_ready() -> None:
    settings = load_settings(refresh=True)
    while True:
        try:
            get_odoo_client()
            logger.info("Odoo is reachable at %s", settings.odoo_base_url)
            return
        except ODOO_RETRYABLE_EXCEPTIONS as exc:
            logger.warning(
                "Odoo is not ready at %s; retrying in %ss: %s",
                settings.odoo_base_url,
                settings.alert_jobs_sync_interval_seconds,
                exc,
            )
            time.sleep(settings.alert_jobs_sync_interval_seconds)


def sync_scheduled_jobs_forever() -> None:
    while True:
        settings = load_settings(refresh=True)
        try:
            ensure_scheduled_jobs()
        except ODOO_RETRYABLE_EXCEPTIONS as exc:
            logger.warning(
                "Skipping scheduled alert sync because Odoo is unavailable at %s: %s",
                settings.odoo_base_url,
                exc,
            )
        except Exception:
            logger.exception("Failed to sync scheduled alert jobs")

        time.sleep(settings.alert_jobs_sync_interval_seconds)


def main() -> None:
    setup_logging()
    wait_for_odoo_ready()
    settings = load_settings(refresh=True)
    sync_thread = threading.Thread(target=sync_scheduled_jobs_forever, daemon=True)
    sync_thread.start()
    command = [
        "python",
        "-m",
        "jobs.run_rq_scheduler_process",
        "--host",
        settings.redis_host,
        "--port",
        str(settings.redis_port),
        "--db",
        str(settings.redis_db),
        "--interval",
        str(settings.rq_scheduler_poll_interval_seconds),
    ]
    if settings.redis_password:
        command.extend(["--password", settings.redis_password])

    logger.info("Starting rq-scheduler process")
    raise SystemExit(subprocess.call(command))


if __name__ == "__main__":
    main()
