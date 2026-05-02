import re
from dataclasses import dataclass
from typing import Any

from config.odoo import get_odoo_client_with_retry
from config.settings import AlertJobConfig, load_settings


@dataclass
class AlertMailTemplate:
    html_header: str
    inline_style: str
    html_body: str
    html_footer: str


def _get_env():
    return get_odoo_client_with_retry()


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


def _normalize_optional_text(value: Any) -> str | None:
    if value in (None, False, ""):
        return None
    return str(value).strip() or None


def _build_alert_job(record: dict[str, Any]) -> AlertJobConfig:
    settings = load_settings(refresh=True)
    name = str(record.get("name") or "").strip()
    return AlertJobConfig(
        job_id=_slugify_job_id(name),
        cron=str(record.get("cron_string") or "").strip(),
        query=str(record.get("alert_query") or "").strip(),
        name=name,
        subject=name or settings.alert_email_subject,
        recipients=_normalize_recipients(record.get("recipients")),
        model_id=_normalize_optional_text(record.get("model_id")),
        risk_rating=record.get("risk_rating"),
        mail_from_name=None,
        enabled=True,
    )


def list_alert_jobs() -> list[AlertJobConfig]:
    env = _get_env()
    model = env["alert.rules"]
    record_ids = model.search([("status", "=", 1)])
    if not record_ids:
        return []

    records = model.read(
        record_ids,
        ["name", "alert_query", "cron_string", "recipients", "model_id", "risk_rating"],
    )
    jobs: list[AlertJobConfig] = []
    for record in records:
        job = _build_alert_job(record)
        if not job.job_id or not job.cron or not job.query:
            continue
        jobs.append(job)
    return jobs


def create_alert_history(
    *,
    alert_id: str,
    ref_id: str | None,
    risk_rating: int | str | None,
    html_body: str,
    email: str,
    source: str,
) -> Any:
    env = _get_env()
    history_model = env["alert.history"]
    return history_model.create(
        {
            "alert_id": alert_id,
            "ref_id": ref_id,
            "risk_rating": risk_rating,
            "html_body": html_body,
            "email": email,
            "source": source,
        }
    )


def get_alert_job(job_id: str) -> AlertJobConfig:
    for job in list_alert_jobs():
        if job.job_id == job_id:
            return job
    raise KeyError(f"Alert job not found: {job_id}")


def get_alert_mail_template(code: str = "alert") -> AlertMailTemplate:
    env = _get_env()
    model = env["alert.mail.template"]
    record_ids = model.search([("code", "=", code)])
    if not record_ids:
        raise KeyError(f"Alert mail template not found: {code}")
    records = model.read(
        record_ids[:1],
        ["html_header", "inline_style", "html_body", "html_footer"],
    )
    if not records:
        raise KeyError(f"Alert mail template not found: {code}")
    rec = records[0]
    return AlertMailTemplate(
        html_header=str(rec.get("html_header") or ""),
        inline_style=str(rec.get("inline_style") or ""),
        html_body=str(rec.get("html_body") or ""),
        html_footer=str(rec.get("html_footer") or ""),
    )
