import json
from functools import lru_cache
from pathlib import Path
from typing import NamedTuple

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class OdooConfig(NamedTuple):
    base_url: str
    database: str
    username: str
    password: str


class AlertJobConfig(BaseModel):
    job_id: str
    cron: str
    query: str
    subject: str = "Database alert records found"
    recipients: list[str]
    mail_from_name: str | None = None
    enabled: bool = True


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    postgres_host: str = "db"
    postgres_port: int = 5432
    postgres_db: str = "postgres"
    postgres_user: str = "odoo"
    postgres_password: str = ""

    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str | None = None

    rq_queue_name: str = "alerts"
    rq_scheduler_cron: str = "*/5 * * * *"
    rq_scheduler_job_id: str = "database-alert-email"
    alert_jobs_sync_interval_seconds: int = 10
    alert_jobs_file: str = "config/alert_jobs.json"
    alert_jobs_json: str = ""

    alert_query_enabled: bool = False
    alert_query_sql: str = ""
    alert_email_subject: str = "Database alert records found"

    mail_server: str = ""
    mail_port: int = 587
    mail_username: str = ""
    mail_password: str = ""
    mail_from: str = ""
    mail_from_name: str = "Odoo FastAPI Bridge"
    mail_starttls: bool = True
    mail_ssl_tls: bool = False
    mail_validate_certs: bool = True
    alert_email_to: str = ""

    odoo_base_url: str = "http://odoo16:8069"
    odoo_db: str | None = None
    odoo_username: str = "admin"
    odoo_password: str = Field(default="")

    @property
    def resolved_odoo_db(self) -> str:
        return self.odoo_db or self.postgres_db

    @property
    def odoo_config(self) -> OdooConfig:
        if not self.odoo_password:
            raise RuntimeError("ODOO_PASSWORD is required to run partner actions")

        return OdooConfig(
            base_url=self.odoo_base_url.rstrip("/"),
            database=self.resolved_odoo_db,
            username=self.odoo_username,
            password=self.odoo_password,
        )

    @property
    def alert_email_recipients(self) -> list[str]:
        return [email.strip() for email in self.alert_email_to.split(",") if email.strip()]

    @property
    def alert_jobs(self) -> list[AlertJobConfig]:
        if self.alert_jobs_json.strip():
            raw_jobs = json.loads(self.alert_jobs_json)
            return [AlertJobConfig.model_validate(job) for job in raw_jobs]

        alert_jobs_path = Path(self.alert_jobs_file)
        if not alert_jobs_path.is_absolute():
            alert_jobs_path = Path(__file__).resolve().parent.parent / alert_jobs_path

        if alert_jobs_path.exists():
            raw_jobs = json.loads(alert_jobs_path.read_text(encoding="utf-8"))
            return [AlertJobConfig.model_validate(job) for job in raw_jobs]

        if not self.alert_query_enabled:
            return []

        return [
            AlertJobConfig(
                job_id=self.rq_scheduler_job_id,
                cron=self.rq_scheduler_cron,
                query=self.alert_query_sql,
                subject=self.alert_email_subject,
                recipients=self.alert_email_recipients,
                mail_from_name=self.mail_from_name,
                enabled=True,
            )
        ]


@lru_cache
def get_settings() -> Settings:
    return Settings()


def load_settings(refresh: bool = False) -> Settings:
    if refresh:
        get_settings.cache_clear()
    return get_settings()


def get_alert_job_config(job_id: str, refresh: bool = False) -> AlertJobConfig:
    settings = load_settings(refresh=refresh)
    for job in settings.alert_jobs:
        if job.job_id == job_id:
            return job
    raise KeyError(f"Alert job not found: {job_id}")
