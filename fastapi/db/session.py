import socket

from sqlalchemy import URL
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config.settings import get_settings


def get_database_url() -> URL:
    settings = get_settings()
    return URL.create(
        drivername="postgresql+psycopg",
        username=settings.postgres_user,
        password=settings.postgres_password,
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_db,
    )


def get_connect_args() -> dict[str, str]:
    settings = get_settings()
    return {
        "hostaddr": socket.gethostbyname(settings.postgres_host),
    }


engine = create_async_engine(
    get_database_url(),
    connect_args=get_connect_args(),
    pool_pre_ping=True,
)

SessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    autoflush=False,
    expire_on_commit=False,
)
