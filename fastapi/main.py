from fastapi import FastAPI

from config.logger import setup_logging
from router.cache import router as cache_router
from router.fraud import router as fraud_router
from router.health import router as health_router
from router.root import router as root_router
from router.sync import router as sync_router


app = FastAPI(title="Alert Manager", version="0.1.0")
logger = setup_logging()


@app.on_event("startup")
def on_startup() -> None:
    logger.info("FastAPI application startup complete")


app.include_router(root_router, tags=["Root"])
app.include_router(health_router, prefix="/health", tags=["Health Checks"])
app.include_router(cache_router, prefix="/cache", tags=["Cache"])
app.include_router(fraud_router, prefix="/fraud", tags=["Fraud"])
app.include_router(sync_router, prefix="/sync", tags=["Sync"])
