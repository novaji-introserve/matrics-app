import logging
import subprocess

from config.logger import setup_logging
from config.settings import get_settings

logger = logging.getLogger("app")


def main() -> None:
    setup_logging()
    settings = get_settings()
    redis_auth = f":{settings.redis_password}@" if settings.redis_password else ""
    queues = ["risk-high", "sync", "risk-low", settings.rq_queue_name]
    command = [
        "rq",
        "worker",
        *queues,
        "--url",
        f"redis://{redis_auth}{settings.redis_host}:{settings.redis_port}/{settings.redis_db}",
    ]
    logger.info("Starting rq worker queues=%s", queues)
    raise SystemExit(subprocess.call(command))


if __name__ == "__main__":
    main()
