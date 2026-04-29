#!/usr/bin/env python3
"""
AML Customer Profile Rebuild — single-pass runner
==================================================
Rebuilds all customer behavioural profiles from the full transaction history
using a single SQL aggregate query.  Run this weekly (or after a bulk import)
via cron / Docker CronJob instead of the Odoo built-in scheduler.

Typical Docker CronJob schedule:
    0 2 * * 0   # every Sunday at 02:00

Environment variables (set in .env or Docker env):
    ODOO_URL          http://odoo:8069
    ODOO_DATABASE     compliance_db2
    ODOO_USERNAME     admin
    ODOO_PASSWORD     <password>
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

ODOO_URL  = os.environ['ODOO_URL']
DATABASE  = os.environ['ODOO_DATABASE']
USERNAME  = os.environ['ODOO_USERNAME']
PASSWORD  = os.environ['ODOO_PASSWORD']


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
    print(f"[{ts()}] AML profile rebuild starting")
    uid, models = connect()

    try:
        call(models, uid, 'res.aml.customer.profile', '_cron_rebuild_profiles', [[]])
        print(f"[{ts()}] Profile rebuild complete")
        sys.exit(0)
    except xmlrpc.client.Fault as e:
        print(f"[{ts()}] RPC fault: {e.faultString}")
        sys.exit(1)
    except Exception as e:
        print(f"[{ts()}] Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
