import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text

from config.settings import Settings, get_settings
from db import SessionLocal
from router.utils import get_redis_client


router = APIRouter()


@router.get("")
def health(settings: Settings = Depends(get_settings)) -> dict[str, object]:
    return {
        "status": "ok",
        "service": "fastapi",
        "postgres_host": settings.postgres_host,
        "redis_host": settings.redis_host,
        "odoo_base_url": settings.odoo_base_url,
    }


@router.get("/db")
async def health_db(settings: Settings = Depends(get_settings)) -> dict[str, object]:
    try:
        async with SessionLocal() as session:
            row = (
                await session.execute(
                    text("SELECT current_database() AS database, current_user AS username")
                )
            ).mappings().one()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database check failed: {exc}") from exc

    return {
        "status": "ok",
        "database": row["database"],
        "user": row["username"],
        "postgres_host": settings.postgres_host,
        "postgres_port": settings.postgres_port,
    }

@router.get("/redis")
def health_redis(settings: Settings = Depends(get_settings)) -> dict[str, object]:
    try:
        redis_client = get_redis_client()
        pong = redis_client.ping()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Redis check failed: {exc}") from exc

    return {
        "status": "ok",
        "redis_host": settings.redis_host,
        "redis_port": settings.redis_port,
        "ping": pong,
    }


@router.get("/odoo")
def health_odoo(settings: Settings = Depends(get_settings)) -> dict[str, object]:
    base_url = settings.odoo_base_url.rstrip("/")
    candidates = ("/web/login", "/")
    last_error = None

    with httpx.Client(timeout=10.0, follow_redirects=True) as client:
        for path in candidates:
            url = f"{base_url}{path}"
            try:
                response = client.get(url)
                if response.status_code < 500:
                    return {
                        "status": "ok",
                        "url": url,
                        "status_code": response.status_code,
                    }
            except Exception as exc:
                last_error = str(exc)

    detail = last_error or f"Odoo check failed for {base_url}"
    raise HTTPException(status_code=503, detail=detail)
