import importlib
import sys
from pathlib import Path

from fastapi import FastAPI

from config.logger import setup_logging
from router.cache import router as cache_router
from router.health import router as health_router
from router.root import router as root_router


app = FastAPI(title="Alert Manager", version="0.1.0")
logger = setup_logging()


def _load_custom_routers() -> None:
    app_root = Path(__file__).parent
    custom_root = app_root / "custom"
    custom_routes = custom_root / "routes"
    if not custom_routes.exists():
        return
    # prepend custom root so repo.*, models.*, routes.* resolve from custom first
    custom_root_str = str(custom_root)
    if custom_root_str not in sys.path:
        sys.path.insert(0, custom_root_str)
    for pkg in sorted(custom_routes.iterdir()):
        if pkg.is_dir() and (pkg / "__init__.py").exists():
            mod = importlib.import_module(f"routes.{pkg.name}")
            if hasattr(mod, "router"):
                prefix = getattr(mod, "PREFIX", f"/{pkg.name}")
                tags = getattr(mod, "TAGS", [pkg.name.capitalize()])
                app.include_router(mod.router, prefix=prefix, tags=tags)
                logger.info("Loaded custom router: %s at %s", pkg.name, prefix)


@app.on_event("startup")
def on_startup() -> None:
    logger.info("FastAPI application startup complete")


_load_custom_routers()

app.include_router(root_router, tags=["Root"])
app.include_router(health_router, prefix="/health", tags=["Health Checks"])
app.include_router(cache_router, prefix="/cache", tags=["Cache"])
