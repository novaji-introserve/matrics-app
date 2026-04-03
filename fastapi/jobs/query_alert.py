import html
import logging

import psycopg
from psycopg.rows import dict_row

from config.logger import setup_job_logging
from config.settings import load_settings
from jobs.emailer import send_email
from repo.alert_jobs import get_alert_job
from router.utils import get_postgres_dsn


logger = logging.getLogger("app")


def _validate_query(query: str) -> str:
    normalized = query.strip().rstrip(";")
    if not normalized:
        raise ValueError("ALERT_QUERY_SQL is empty")
    if not normalized.lower().startswith("select"):
        raise ValueError("ALERT_QUERY_SQL must start with SELECT")
    return normalized


def _build_email_body(job_id: str,name:str, rows: list[dict], query: str) -> str:
    row_count = len(rows)
    header_cells = "".join(
        f"<th style='text-align:left;padding:5px;border:1px solid #d0d7de'>{html.escape(str(key).replace('_', ' ').title())}</th>"
        for key in rows[0].keys()
    )
    body_rows = []
    for row in rows:
        cells = "".join(
            f"<td style='padding:5px;border:1px solid #d0d7de'>{html.escape(str(value))}</td>"
            for value in row.values()
        )
        body_rows.append(f"<tr>{cells}</tr>")

    table = (
        "<table style='border: 0px solid #ddd !important;font-family:Helvetica,sans-serif;font-size:12px'>"
        f"<thead><tr>{header_cells}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )

    return (
        "<div style='font-family:Helvetica,sans-serif;font-size:12px'>"
        f"<p><strong>Alert Rule</strong>: <code>{html.escape(name)}</code></p>"
        f"<p>Alert manager alert query found <strong>{row_count}</strong> record(s).</p>"
        f"{table}"
        "</div>"
    )


def run_database_alert_query(job_id: str) -> None:
    setup_job_logging()
    settings = load_settings(refresh=True)
    job = get_alert_job(job_id)

    query = _validate_query(job.query)
    if not job.recipients:
        logger.warning("Skipping scheduled database alert job=%s because recipients are empty", job_id)
        return
    if not settings.mail_server or not settings.mail_from:
        logger.warning("Skipping scheduled database alert job=%s because mail settings are incomplete", job_id)
        return

    logger.info("Running scheduled database alert query for job=%s", job_id)
    with psycopg.connect(get_postgres_dsn(), row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    if not rows:
        logger.info("Scheduled database alert query returned no rows for job=%s", job_id)
        return

    final_subject = f"{job.subject} ({len(rows)})"
    body = _build_email_body(job_id, job.subject, rows, query)
    send_email(
        subject=final_subject,
        body=body,
        recipients=job.recipients,
        mail_from_name=job.mail_from_name,
    )
    logger.info(
        "Scheduled database alert email sent for job=%s to %s",
        job_id,
        ", ".join(job.recipients),
    )
