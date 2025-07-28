

# https://github.com/kmagusiak/odoo-connect
import odoo_connect
from odoo_connect.explore import explore
import pandas as pd
import numpy as np
import random
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
# importing necessary functions from dotenv library
from dotenv import load_dotenv, dotenv_values
load_dotenv()
chunk_size = 1000
branch_ids = []

odoo = env = odoo_connect.connect(url=os.getenv("HOST_URL"), database=os.getenv(
    "DB"), username=os.getenv("USERNAME"), password=os.getenv("PASSWORD"))


def get_currency_ids():
    currencies = explore(env['res.currency'])
    # only NGN
    return currencies.search([('name', 'in', ['USD', 'NGN', 'CAD', 'EUR', 'CNY', 'GBP']), ('active', 'in', [True, False])], limit=10)


def get_currency_thresholds():
    threshold = explore(env['nfiu.currency.threshold'])
    return threshold.search([])

def report_transactions():
    thresholds = get_currency_thresholds()
    for e in thresholds:
        tran = explore(env['res.customer.transaction'])
        trans = tran.search(['|',('amount', '<=',(e.threshold * -1)), ('amount', '>=',e.threshold), ('currency_id', '=', e.currency_id.id)])
        for t in trans:
            try:
                t.write({
                    'report_nfiu': True,
                    'transaction_number': t.name,
                    'internal_ref_number': t.name,
                    'transaction_location': t.branch_id.name,
                    'teller': 'SYSTEM',
                    'authorized': 'SYSTEM',
                    'transmode_code': 'E',
                    'transmode_comment': t.narration,
                    'amount_local': abs(t.amount),
                    'from_funds_comment': t.narration,
                    'to_funds_comment': t.narration,
                    'comments': t.narration

                })
                print(t)
            except Exception as e:
                # pass
                print(f"Error creating record for {t.name}: {e}")


report_transactions()
# print(f"Created {cnt} reports transactions records" if cnt else "Nothing done")
