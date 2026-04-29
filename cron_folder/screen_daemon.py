#!/usr/bin/env python3
"""
Transaction Screening — single-pass runner
==========================================
Fetches up to SCREEN_BATCH_SIZE transactions in 'new' state and screens them.
Schedule this via cron / Docker / K8s CronJob instead of looping internally.
"""

import os
import xmlrpc.client
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Walk up from cron_folder → icomply_odoo → odoo to find the root .env
_here = Path(__file__).resolve().parent
for _candidate in [_here / '.env', _here.parent / '.env', _here.parent.parent / '.env']:
    if _candidate.exists():
        load_dotenv(_candidate, override=False)
        if os.getenv('ODOO_URL'):
            break

ODOO_URL   = os.environ['ODOO_URL']
DATABASE   = os.environ['ODOO_DATABASE']
USERNAME   = os.environ['ODOO_USERNAME']
PASSWORD   = os.environ['ODOO_PASSWORD']
BATCH_SIZE = int(os.getenv('SCREEN_BATCH_SIZE', 100))



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
    uid, models = connect()

    try:
        ids = call(models, uid, 'res.customer.transaction', 'search',
                   [[['state', '=', 'new']]], {'order': 'id asc'})
        

        if not ids:
            print(f"[{ts()}] No new transactions — exiting cleanly")
            sys.exit(0)

        call(models, uid, 'res.customer.transaction', 'multi_screen', [ids])
        print(f"[{ts()}] Screened {len(ids)} transaction(s)")
        sys.exit(0)

    except xmlrpc.client.Fault as e:
        print(f"[{ts()}] RPC fault: {e.faultString}")
        sys.exit(1)
    except Exception as e:
        print(f"[{ts()}] Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()