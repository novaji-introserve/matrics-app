import html
import logging

import psycopg
from psycopg.rows import dict_row

from config.logger import setup_job_logging
from config.settings import get_alert_job_config, load_settings
from jobs.emailer import send_email
from router.utils import get_postgres_dsn


logger = logging.getLogger("app")


def _validate_query(query: str) -> str:
    normalized = query.strip().rstrip(";")
    if not normalized:
        raise ValueError("ALERT_QUERY_SQL is empty")
    if not normalized.lower().startswith("select"):
        raise ValueError("ALERT_QUERY_SQL must start with SELECT")
    return normalized


def _build_email_body(job_id: str, rows: list[dict], query: str) -> str:
    row_count = len(rows)
    header_cells = "".join(
        f"<th style='text-align:left;padding:8px;border:1px solid #d0d7de'>{html.escape(str(key))}</th>"
        for key in rows[0].keys()
    )
    body_rows = []
    for row in rows:
        cells = "".join(
            f"<td style='padding:8px;border:1px solid #d0d7de'>{html.escape(str(value))}</td>"
            for value in row.values()
        )
        body_rows.append(f"<tr>{cells}</tr>")

    table = (
        "<table style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:14px'>"
        f"<thead><tr>{header_cells}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )

    return (
        "<div style='font-family:Arial,sans-serif'>"
        f"<p><strong>Alert job</strong>: <code>{html.escape(job_id)}</code></p>"
        f"<p>The scheduled database alert query found <strong>{row_count}</strong> record(s).</p>"
        f"{table}"
        "</div>"
    )


def run_database_alert_query(job_id: str) -> None:
    setup_job_logging()
    settings = load_settings(refresh=True)
    job = get_alert_job_config(job_id, refresh=True)

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
    body = _build_email_body(job_id, rows, query)
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
