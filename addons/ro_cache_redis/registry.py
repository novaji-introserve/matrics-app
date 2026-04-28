import logging

from redis import Redis
from redis.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import TimeoutError, ConnectionError as RedisConnectionError

from odoo.tools import config
from odoo.modules.registry import Registry

from .tools.redis_lru import RedisLRU

_redis_client = None
_old_init = Registry.init
_logger = logging.getLogger(__name__)


def init(self, db_name):
    _old_init(self, db_name)

    global _redis_client

    if not (redis_url := config.get('redis_cache_url', None)):
        _logger.info("Redis cache URL not configured; using default in-memory caches.")
        return

    try:
        retry_strategy = Retry(ExponentialBackoff(), 3)
        _redis_client = Redis.from_url(
            url=redis_url,
            socket_timeout=5,
            retry=retry_strategy,
            decode_responses=False,
            health_check_interval=30,
            socket_connect_timeout=5,
            retry_on_error=[RedisConnectionError, TimeoutError],
        )
        _redis_client.ping()
        _logger.info("Connected to Redis cache server at %s", redis_url)
    except Exception as e:
        _logger.warning("Could not connect to Redis cache server; using default in-memory caches: \n%s", e)
        return

    try:
        expiration = int(config.get('redis_cache_expiration', 3600))
    except ValueError:
        expiration = 3600
    prefix = config.get('redis_cache_prefix', 'odoo_orm_cache')
    self._Registry__cache = RedisLRU(
        count=8192,
        expiration=expiration,
        redis_client=_redis_client,
        prefix=f"{prefix}:{db_name}:",
    )


if not config.get('test_enable', False):
    Registry.init = init
