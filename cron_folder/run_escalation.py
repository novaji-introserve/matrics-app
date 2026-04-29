#!/usr/bin/env python3
"""
Escalation Engine — single-pass runner
=======================================
Calls the escalation engine to check open alerts and send escalation emails
based on the configured TAT (Turn-Around Time) matrix.

Schedule this via cron / Docker CronJob instead of piping into odoo shell.

Environment variables (set in .env or Docker env):
    ODOO_URL              http://odoo:8069
    ODOO_DATABASE         compliance_db2
    ODOO_USERNAME         admin
    ODOO_PASSWORD         <password>
    ESCALATION_INTERVAL   seconds between runs (informational only — scheduling
                          is done externally by cron/Docker, not this script)

Flags:
    FORCE=1   Skip TAT check and escalate all open alerts immediately.
              Useful for testing. Default: 0 (normal TAT-based escalation).

Usage:
    python3 run_escalation.py           # normal run
    FORCE=1 python3 run_escalation.py   # force-escalate all open alerts
"""

import os
import sys
import xmlrpc.client
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

_here = Path(__file__).resolve().parent
for _candidate in [_here / '.env', _here.parent / '.env', _here.parent.parent / '.env']:
    if _candidate.exists():
        load_dotenv(_candidate, override=False)
        if os.getenv('ODOO_URL'):
            break

ODOO_URL = os.environ['ODOO_URL']
DATABASE = os.environ['ODOO_DATABASE']
USERNAME = os.environ['ODOO_USERNAME']
PASSWORD = os.environ['ODOO_PASSWORD']
FORCE    = os.getenv('FORCE', '0') == '1'


def ts():
    return datetime.now().strftime('%H:%M:%S')


def connect():
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common', allow_none=True)
    uid = common.authenticate(DATABASE, USERNAME, PASSWORD, {})
    if not uid:
        print(f"[{ts()}] ERROR: Authentication failed")
        sys.exit(1)
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object', allow_none=True)
    return uid, models


def call(models, uid, model, method, args, kwargs=None):
    return models.execute_kw(DATABASE, uid, PASSWORD, model, method, args, kwargs or {})


def main():
    print(f"[{ts()}] {'=' * 50}")
    print(f"[{ts()}] Escalation Engine — START")
    print(f"[{ts()}]   force_mode : {FORCE}")
    print(f"[{ts()}]   timestamp  : {datetime.now()}")
    print(f"[{ts()}] {'=' * 50}")

    uid, models = connect()

    try:
        open_count = call(models, uid, 'alert.history', 'search_count', [[
            ['status', '=', 'pending review'],
            ['escalation_matrix_id', '!=', False],
            ['escalation_complete', '=', False],
        ]])
        print(f"[{ts()}] Open alerts pending escalation: {open_count}")

        if open_count == 0:
            print(f"[{ts()}] Nothing to escalate — exiting cleanly")
            sys.exit(0)

        call(models, uid, 'fsdh.escalation.engine', 'run_escalation', [[]], {'force': FORCE})

        print(f"[{ts()}] {'=' * 50}")
        print(f"[{ts()}] Escalation Engine — DONE")
        print(f"[{ts()}] {'=' * 50}")
        sys.exit(0)

    except xmlrpc.client.Fault as e:
        print(f"[{ts()}] RPC fault: {e.faultString}")
        sys.exit(1)
    except Exception as e:
        print(f"[{ts()}] Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
