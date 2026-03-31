import logging
import subprocess

from config.logger import setup_logging
from config.settings import get_settings


logger = logging.getLogger("app")


def main() -> None:
    setup_logging()
    settings = get_settings()
    redis_auth = f":{settings.redis_password}@" if settings.redis_password else ""
    command = [
        "rq",
        "worker",
        settings.rq_queue_name,
        "--url",
        f"redis://{redis_auth}{settings.redis_host}:{settings.redis_port}/{settings.redis_db}",
    ]
    logger.info("Starting rq worker for queue=%s", settings.rq_queue_name)
    raise SystemExit(subprocess.call(command))


if __name__ == "__main__":
    main()
