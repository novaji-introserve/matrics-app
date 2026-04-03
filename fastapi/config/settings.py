from functools import lru_cache
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
    subject: str = "Alert records found"
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
    rq_scheduler_poll_interval_seconds: int = 5
    alert_jobs_sync_interval_seconds: int = 10
    alert_email_subject: str = "Database alert records found"

    mail_server: str = ""
    mail_port: int = 587
    mail_username: str = ""
    mail_password: str = ""
    mail_from: str = ""
    mail_from_name: str | None = None
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


@lru_cache
def get_settings() -> Settings:
    return Settings()


def load_settings(refresh: bool = False) -> Settings:
    if refresh:
        get_settings.cache_clear()
    return get_settings()
