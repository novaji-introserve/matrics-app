from fastapi import APIRouter


router = APIRouter()


@router.get("/")
def root() -> dict[str, object]:
    return {
        "message": "FastAPI service is running",
        "docs_url": "/docs",
        "health_endpoints": [
            "/health",
            "/health/db",
            "/health/redis",
            "/health/odoo",
        ],
        "utility_endpoints": ["/cache/ping"],
        "sync_endpoints": ["/sync/contacts"],
    }
