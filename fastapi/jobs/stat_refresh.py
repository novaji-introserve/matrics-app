import logging

from config.odoo import get_odoo_client_with_retry
from config.logger import setup_job_logging


logger = logging.getLogger("app")


def run_compliance_stat_refresh() -> None:
    setup_job_logging()
    env = get_odoo_client_with_retry()
    stat_model = env["res.compliance.stat"]
    logger.info("Triggering compliance stat refresh via odoo_connect")
    result = stat_model.update_stat()
    logger.info("Compliance stat refresh completed with result=%s", result)
