import re
from typing import Any

from config import get_odoo_client
from config.settings import AlertJobConfig, load_settings


def _get_env():
    return get_odoo_client()


def _slugify_job_id(name: str) -> str:
    normalized = re.sub(r"\s+", "-", (name or "").strip().lower())
    return normalized.strip("-")


def _normalize_recipients(value: Any) -> list[str]:
    if value in (None, False, ""):
        return []
    if isinstance(value, str):
        return [email.strip() for email in value.split(",") if email.strip()]
    if isinstance(value, (list, tuple, set)):
        recipients: list[str] = []
        for item in value:
            if item in (None, False, ""):
                continue
            if isinstance(item, str):
                recipients.extend(
                    email.strip() for email in item.split(",") if email.strip()
                )
                continue
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                display_value = item[1]
                if isinstance(display_value, str) and "@" in display_value:
                    recipients.append(display_value.strip())
                    continue
            if isinstance(item, dict):
                email = item.get("email") or item.get("name")
                if isinstance(email, str) and "@" in email:
                    recipients.append(email.strip())
                    continue
        return recipients
    return []


def _build_alert_job(record: dict[str, Any]) -> AlertJobConfig:
    settings = load_settings(refresh=True)
    name = str(record.get("name") or "").strip()
    return AlertJobConfig(
        job_id=_slugify_job_id(name),
        cron=str(record.get("cron_string") or "").strip(),
        query=str(record.get("alert_query") or "").strip(),
        subject=name or settings.alert_email_subject,
        recipients=_normalize_recipients(record.get("recipients")),
        mail_from_name=None,
        enabled=True,
    )


def list_alert_jobs() -> list[AlertJobConfig]:
    env = _get_env()
    model = env["alert.rules"]
    record_ids = model.search([("status", "=", 1)])
    if not record_ids:
        return []

    records = model.read(record_ids, ["name", "alert_query", "cron_string", "recipients"])
    jobs: list[AlertJobConfig] = []
    for record in records:
        job = _build_alert_job(record)
        if not job.job_id or not job.cron or not job.query:
            continue
        jobs.append(job)
    return jobs


def get_alert_job(job_id: str) -> AlertJobConfig:
    for job in list_alert_jobs():
        if job.job_id == job_id:
            return job
    raise KeyError(f"Alert job not found: {job_id}")
