import logging
import time

import odoo_connect
from requests import exceptions as requests_exceptions

from config.settings import get_settings

logger = logging.getLogger("app")
ODOO_RETRYABLE_EXCEPTIONS = (
    odoo_connect.OdooConnectionError,
    requests_exceptions.ConnectionError,
)


def get_odoo_config():
    return get_settings().odoo_config


def get_odoo_client():
    odoo_config = get_odoo_config()
    return odoo_connect.connect(
        url=odoo_config.base_url,
        database=odoo_config.database,
        username=odoo_config.username,
        password=odoo_config.password,
    )


def get_odoo_client_with_retry():
    settings = get_settings()
    attempts = max(1, settings.odoo_connect_max_attempts)
    delay_seconds = max(0, settings.odoo_connect_retry_delay_seconds)

    last_exception: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return get_odoo_client()
        except ODOO_RETRYABLE_EXCEPTIONS as exc:
            last_exception = exc
            if attempt >= attempts:
                break
            logger.warning(
                "Odoo connection attempt %s/%s failed for %s; retrying in %ss: %s",
                attempt,
                attempts,
                settings.odoo_base_url,
                delay_seconds,
                exc,
            )
            time.sleep(delay_seconds)

    if last_exception is not None:
        raise last_exception
    return get_odoo_client()
