

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

odoo = env = odoo_connect.connect(url=os.getenv("HOST_URL"), database=os.getenv("DB"),username=os.getenv("USERNAME"), password=os.getenv("PASSWORD"))


def get_currency_ids():
    currencies = explore(env['res.currency'])
    return currencies.search([('name', 'in', ['USD','NGN','CAD','EUR','CNY','GBP']),('active','in',[True,False])],limit=10)  # only NGN


def create_currency_thresholds():
    currency_ids = get_currency_ids()
    threshold = env['nfiu.currency.threshold']
    num = threshold.search_count([])
    print(f"Total existing records: {num}")
    if num >= 1:
        print("There are existing records, skipping creation.")
        return None
    print("No existing records found, proceeding...")
    tot_created = 0
    for i,rec in enumerate(currency_ids):
        try:
            new_rec = threshold.create({
                'currency_id': rec.id,
                'threshold': 25.00 if rec.name == 'NGN' else 1.00
<<<<<<< HEAD
                'shortname': rec.name[:2].upper(),
=======
>>>>>>> 816be76 (XML Schema Validator)
            })
            print(f"Created account: {rec.name} with ID: {new_rec}")
            tot_created+=1
        except Exception as e:
                    print(f"Error creating record for {rec.name}: {e}")

cnt = create_currency_thresholds()
print(f"Created {cnt} account records" if cnt else "No accounts created")