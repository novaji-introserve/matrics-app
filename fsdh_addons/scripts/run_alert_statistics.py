#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run Alert Statistics - One-shot script to compute and store all alert statistics.

Same idea as Rabba's run_queries_and_store_results: when this script runs,
it executes all configured alert statistics SQL queries and saves the results
to the database. When users open the Alert UI dashboard, those values load
already (no need to click Compute in the UI).

Usage:
  # From alert_management/scripts with env vars or defaults:
  python3 run_alert_statistics.py

  # With options:
  python3 run_alert_statistics.py --odoo-url http://localhost:8069 --odoo-db Novaji_Test

  # Environment variables (same as alert_scheduler):
  ODOO_URL, DB_NAME or PGDATABASE, ADMIN_USER, ADMIN_PASSWORD
"""
import os
import sys
import logging
import argparse
from pathlib import Path

# Setup logging
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers = []
fh = logging.FileHandler(LOG_DIR / "run_alert_statistics.log")
fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(fh)
logger.addHandler(logging.StreamHandler())


def get_odoo_connection(odoo_url=None, odoo_db=None):
    """Get Odoo XML-RPC connection (models proxy and uid)."""
    import xmlrpc.client

    url = odoo_url or os.getenv("ODOO_URL", "http://localhost:8069")
    db = odoo_db or os.getenv("DB_NAME") or os.getenv("PGDATABASE", "Novaji_Test")
    user = os.getenv("ADMIN_USER", os.getenv("ODOO_ADMIN_USER", "admin"))
    password = os.getenv("ADMIN_PASSWORD", os.getenv("ODOO_ADMIN_PASSWORD", "admin"))

    common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common", allow_none=True)
    uid = common.authenticate(db, user, password, {})
    if not uid:
        raise Exception("Authentication failed - check credentials")

    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object", allow_none=True)
    return models, uid, db, password


def main():
    parser = argparse.ArgumentParser(
        description="Run all alert statistics queries and store results for the Alert dashboard."
    )
    parser.add_argument("--odoo-url", default=None, help="Odoo URL (default: http://localhost:8069)")
    parser.add_argument("--odoo-db", default=None, help="Odoo database name")
    args = parser.parse_args()

    try:
        logger.info("Connecting to Odoo...")
        models, uid, db, password = get_odoo_connection(args.odoo_url, args.odoo_db)
        logger.info("Calling alert.stat run_queries_and_store_results()...")
        result = models.execute_kw(
            db, uid, password,
            "alert.stat",
            "run_queries_and_store_results",
            [],
        )
        logger.info("Done: computed=%s, errors=%s", result.get("computed", 0), result.get("errors", 0))
        return 0 if (result.get("errors") or 0) == 0 else 1
    except Exception as e:
        logger.exception("Error: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
