from redis import Redis
from rq import Queue

from config.settings import get_settings


def get_postgres_dsn() -> str:
    settings = get_settings()
    return (
        f"host={settings.postgres_host} "
        f"port={settings.postgres_port} "
        f"dbname={settings.postgres_db} "
        f"user={settings.postgres_user} "
        f"password={settings.postgres_password}"
    )


def get_redis_client() -> Redis:
    settings = get_settings()
    return Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        password=settings.redis_password,
        decode_responses=True,
    )


def get_rq_queue(name: str | None = None) -> Queue:
    settings = get_settings()
    return Queue(name or settings.rq_queue_name, connection=get_redis_client())
