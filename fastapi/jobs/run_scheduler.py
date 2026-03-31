import logging
import subprocess
import threading
import time

from redis import Redis
from rq_scheduler import Scheduler

from config.logger import setup_logging
from config.settings import load_settings
from jobs.query_alert import run_database_alert_query


logger = logging.getLogger("app")


def _get_scheduler() -> Scheduler:
    settings = load_settings(refresh=True)
    redis_connection = Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        password=settings.redis_password,
    )
    return Scheduler(queue_name=settings.rq_queue_name, connection=redis_connection)


def ensure_scheduled_jobs() -> None:
    settings = load_settings(refresh=True)
    scheduler = _get_scheduler()

    for job in scheduler.get_jobs():
        if (
            job.meta.get("scheduler_group") == "database-alert"
            or job.func_name == "jobs.query_alert.run_database_alert_query"
        ):
            scheduler.cancel(job)

    for alert_job in settings.alert_jobs:
        if not alert_job.enabled:
            logger.info("Skipping disabled alert job id=%s", alert_job.job_id)
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
            },
        )
        logger.info(
            "Registered rq-scheduler job id=%s on cron=%s queue=%s",
            alert_job.job_id,
            alert_job.cron,
            settings.rq_queue_name,
        )


def sync_scheduled_jobs_forever() -> None:
    while True:
        try:
            ensure_scheduled_jobs()
        except Exception:
            logger.exception("Failed to sync scheduled alert jobs")

        settings = load_settings(refresh=True)
        time.sleep(settings.alert_jobs_sync_interval_seconds)


def main() -> None:
    setup_logging()
    settings = load_settings(refresh=True)
    sync_thread = threading.Thread(target=sync_scheduled_jobs_forever, daemon=True)
    sync_thread.start()
    command = [
        "rqscheduler",
        "--host",
        settings.redis_host,
        "--port",
        str(settings.redis_port),
        "--db",
        str(settings.redis_db),
        "--interval",
        "30",
    ]
    if settings.redis_password:
        command.extend(["--password", settings.redis_password])

    logger.info("Starting rq-scheduler process")
    raise SystemExit(subprocess.call(command))


if __name__ == "__main__":
    main()
