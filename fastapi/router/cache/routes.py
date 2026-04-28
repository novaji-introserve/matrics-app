from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException

from router.utils import get_redis_client


router = APIRouter()


@router.get("/ping")
def cache_ping() -> dict[str, object]:
    try:
        redis_client = get_redis_client()
        cache_key = "fastapi:cache:ping"
        cache_value = datetime.now(UTC).isoformat()
        redis_client.setex(cache_key, 60, cache_value)
        cached = redis_client.get(cache_key)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Redis cache write failed: {exc}") from exc

    return {"status": "ok", "key": cache_key, "value": cached, "ttl_seconds": 60}
