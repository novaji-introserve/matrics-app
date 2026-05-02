import html
import hashlib
import json
import logging
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from config.logger import setup_job_logging
from config.settings import load_settings
from jobs.emailer import send_email
from repo.alert_jobs import AlertMailTemplate, create_alert_history, get_alert_job, get_alert_mail_template
from router.utils import get_postgres_dsn, get_redis_client


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


def _alert_lock_key(job_id: str) -> str:
    return f"alert-job-lock:{job_id}"


def _alert_seen_key(job_id: str) -> str:
    return f"alert-job-seen:{job_id}"


def _row_fingerprint(row: dict) -> str:
    payload = json.dumps(row, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@contextmanager
def _job_execution_lock(job_id: str):
    settings = load_settings(refresh=True)
    redis_client = get_redis_client()
    lock_key = _alert_lock_key(job_id)
    lock_value = uuid4().hex
    acquired = redis_client.set(
        lock_key,
        lock_value,
        ex=max(1, settings.alert_job_lock_ttl_seconds),
        nx=True,
    )
    try:
        yield bool(acquired)
    finally:
        if not acquired:
            return
        current_value = redis_client.get(lock_key)
        if current_value == lock_value:
            redis_client.delete(lock_key)


def _filter_unseen_rows(job_id: str, rows: list[dict]) -> list[dict]:
    settings = load_settings(refresh=True)
    redis_client = get_redis_client()
    seen_key = _alert_seen_key(job_id)
    now = int(time.time())
    min_score = 0
    max_score = now - max(1, settings.alert_job_seen_ttl_seconds)

    pipeline = redis_client.pipeline()
    pipeline.zremrangebyscore(seen_key, min_score, max_score)
    fingerprints = [_row_fingerprint(row) for row in rows]
    if fingerprints:
        for fingerprint in fingerprints:
            pipeline.zscore(seen_key, fingerprint)
    else:
        pipeline.expire(seen_key, settings.alert_job_seen_ttl_seconds)
    results = pipeline.execute()

    existing_scores = results[1:] if fingerprints else []
    unseen_rows = [
        row for row, score in zip(rows, existing_scores)
        if score is None
    ]
    return unseen_rows


def _mark_rows_seen(job_id: str, rows: list[dict]) -> None:
    if not rows:
        return
    settings = load_settings(refresh=True)
    redis_client = get_redis_client()
    seen_key = _alert_seen_key(job_id)
    now = int(time.time())
    mapping = {_row_fingerprint(row): now for row in rows}
    pipeline = redis_client.pipeline()
    pipeline.zadd(seen_key, mapping)
    pipeline.expire(seen_key, max(1, settings.alert_job_seen_ttl_seconds))
    pipeline.execute()


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

    with _job_execution_lock(job_id) as acquired:
        if not acquired:
            logger.info("Skipping scheduled database alert job=%s because another worker already holds the lock", job_id)
            return

        logger.info("Running scheduled database alert query for job=%s", job_id)
        with psycopg.connect(get_postgres_dsn(), row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

        if not rows:
            logger.info("Scheduled database alert query returned no rows for job=%s", job_id)
            return

        unseen_rows = _filter_unseen_rows(job_id, rows)
        if not unseen_rows:
            logger.info("Skipping scheduled database alert job=%s because all %s row(s) were already sent", job_id, len(rows))
            return

        alert_id = uuid4().hex
        final_subject = f"{job.subject} ({len(unseen_rows)})"
        body = _build_email_body(job_id, alert_id, job.subject, unseen_rows, query)
        send_email(
            subject=final_subject,
            body=body,
            recipients=job.recipients,
            mail_from_name=job.mail_from_name,
        )
        _mark_rows_seen(job_id, unseen_rows)
        history_email = ", ".join(job.recipients)
        history_source = job.name
        logger.info(
            "Creating alert.history for job=%s alert_id=%s ref_id=%s source=%s email=%s rows=%s/%s",
            job_id,
            alert_id,
            job.model_id,
            history_source,
            history_email,
            len(unseen_rows),
            len(rows),
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
            "Scheduled database alert email sent for job=%s alert_id=%s unseen_rows=%s total_rows=%s to %s",
            job_id,
            alert_id,
            len(unseen_rows),
            len(rows),
            ", ".join(job.recipients),
        )
