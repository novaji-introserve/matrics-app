import html
import logging
import time
from pathlib import Path
from uuid import uuid4

from config.logger import setup_job_logging
from config.settings import load_settings
from jobs.emailer import send_email
from repo.alert_jobs import AlertMailTemplate, create_alert_history, get_alert_job, get_alert_mail_template
from router.utils import get_postgres_dsn


logger = logging.getLogger("app")
DEFAULT_EMAIL_CSS_PATH = Path(__file__).with_name("styles") / "query_alert.css"

_TEMPLATE_TTL = 300  # seconds
_template_cache: dict[str, tuple[float, AlertMailTemplate]] = {}


def _validate_query(query: str) -> str:
    normalized = query.strip().rstrip(";")
    if not normalized:
        raise ValueError("ALERT_QUERY_SQL is empty")
    if not normalized.lower().startswith("select"):
        raise ValueError("ALERT_QUERY_SQL must start with SELECT")
    return normalized


def _load_cached_template(code: str = "alert") -> AlertMailTemplate | None:
    now = time.monotonic()
    cached = _template_cache.get(code)
    if cached:
        cached_at, tmpl = cached
        if now - cached_at < _TEMPLATE_TTL:
            return tmpl
    try:
        tmpl = get_alert_mail_template(code)
        _template_cache[code] = (now, tmpl)
        return tmpl
    except Exception as exc:
        logger.warning("Could not load alert mail template code=%s: %s", code, exc)
        return None


def _load_email_css_rules() -> str:
    settings = load_settings(refresh=True)
    css_path = (settings.alert_email_css_path or "").strip()
    if css_path:
        configured_path = Path(css_path)
        if configured_path.is_file():
            return configured_path.read_text(encoding="utf-8").strip()
        logger.warning("Configured alert email CSS file was not found: %s", configured_path)

    if settings.alert_email_css.strip():
        return settings.alert_email_css.strip()

    return DEFAULT_EMAIL_CSS_PATH.read_text(encoding="utf-8").strip()


def _format_column_label(key: object) -> str:
    return str(key).replace("_", " ").title()


def _build_logo_markup() -> str:
    settings = load_settings(refresh=True)
    logo_url = (settings.alert_email_logo_url or "").strip()
    if not logo_url:
        return ""

    escaped_logo_url = html.escape(logo_url, quote=True)
    return (
        "<div class='query-alert__logo-wrap'>"
        f"<img class='query-alert__logo' src='{escaped_logo_url}' alt='Alert logo'>"
        "</div>"
    )


def _build_table(rows: list[dict]) -> str:
    header_cells = "".join(
        f"<th>{html.escape(_format_column_label(key))}</th>"
        for key in rows[0].keys()
    )
    body_rows = []
    for row in rows:
        cells = "".join(
            f"<td>{html.escape(str(value))}</td>"
            for value in row.values()
        )
        body_rows.append(f"<tr>{cells}</tr>")

    return (
        "<table>"
        f"<thead><tr>{header_cells}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )


def _build_email_body(
    job_id: str,
    alert_id: str,
    name: str,
    rows: list[dict],
    query: str,
) -> str:
    row_count = len(rows)
    logo = _build_logo_markup()
    table = _build_table(rows)

    tmpl = _load_cached_template()
    if tmpl:
        header = tmpl.html_header.replace("{inline_style}", tmpl.inline_style)
        body = tmpl.html_body.format(
            logo=logo,
            alert_name=html.escape(name),
            alert_id=html.escape(alert_id),
            row_count=row_count,
            table=table,
        )
        return header + body + tmpl.html_footer

    # Fallback: build inline if template is unavailable
    css_rules = _load_email_css_rules()
    return (
        "<!DOCTYPE html>"
        "<html>"
        "<head>"
        "<meta charset='utf-8'>"
        "<style>"
        f"{css_rules}"
        "</style>"
        "</head>"
        "<body>"
        "<div class='query-alert'>"
        f"{logo}"
        f"<p class='query-alert__meta'><strong>Alert Rule</strong>: <code>{html.escape(name)}</code></p>"
        f"<p class='query-alert__meta'><strong>Alert ID</strong>: <code>{html.escape(alert_id)}</code></p>"
        f"<p class='query-alert__meta'>Alert manager alert query found <strong>{row_count}</strong> record(s).</p>"
        f"{table}"
        "</div>"
        "</body>"
        "</html>"
    )


def run_database_alert_query(job_id: str) -> None:
    import psycopg
    from psycopg.rows import dict_row

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

    alert_id = uuid4().hex
    final_subject = f"{job.subject} ({len(rows)})"
    body = _build_email_body(job_id, alert_id, job.subject, rows, query)
    send_email(
        subject=final_subject,
        body=body,
        recipients=job.recipients,
        mail_from_name=job.mail_from_name,
    )
    history_email = ", ".join(job.recipients)
    history_source = job.name
    logger.info(
        "Creating alert.history for job=%s alert_id=%s ref_id=%s source=%s email=%s",
        job_id,
        alert_id,
        job.model_id,
        history_source,
        history_email,
    )
    create_alert_history(
        alert_id=alert_id,
        ref_id=job.model_id,
        risk_rating=job.risk_rating,
        html_body=body,
        email=history_email,
        source=history_source,
    )
    logger.info(
        "Created alert.history for job=%s alert_id=%s",
        job_id,
        alert_id,
    )
    logger.info(
        "Scheduled database alert email sent for job=%s alert_id=%s to %s",
        job_id,
        alert_id,
        ", ".join(job.recipients),
    )
