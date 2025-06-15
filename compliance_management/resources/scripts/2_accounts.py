

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

def get_branch_ids():
    branches = explore(env['res.branch'])
    branch_ids = []
    for b in branches.search([]):
        branch_ids.append(b.id)
    return branch_ids

def get_education_level_ids():
    education_levels = explore(env['res.education.level'])
    education_level_ids = []
    for e in education_levels.search([]):
        education_level_ids.append(e.id)
    return education_level_ids

def get_sector_ids():
    sectors = explore(env['res.partner.sector'])
    sector_ids = []
    for s in sectors.search([]):
        sector_ids.append(s.id)
    return sector_ids

def get_account_officer_ids():
    account_officers = explore(env['account.officers'])
    account_officer_ids = []
    for officer in account_officers.search([]):
        account_officer_ids.append(officer.id)
    return account_officer_ids

def get_country_ids():
    countries = explore(env['res.country'])
    country_ids = []
    for c in countries.search([('code', '=', 'NG')]):  # only NG
        country_ids.append(c.id)
    return country_ids

def get_random_date(start, end):
    """Generate a random dates."""
    today = datetime.now()
    start_years_ago = today - relativedelta(years=start)
    end_years_ago = today - relativedelta(years=end)# Ensure age is at least 18
    start_date = pd.Timestamp(start_years_ago.strftime('%Y-%m-%d'))
    end_date = pd.Timestamp(end_years_ago.strftime('%Y-%m-%d'))
    return start_date + (end_date - start_date) * np.random.rand()

def get_customers():
    customers = explore(env['res.partner'])
    return customers.search([('customer_id', '!=', None),('origin', 'in', ['prod'])], limit=1000)

def get_currency_ids():
    currencies = explore(env['res.currency'])
    return currencies.search([('name', 'in', ['USD','NGN']),('active','in',[True,False])],limit=10)  # only NGN

def get_account_type(name):
    """Get account type by name."""
    account_type = explore(env['res.partner.account.type'])
    return account_type.search([('name', '=', name)], limit=1)

def get_account_product(name):
    """Get account product by name."""
    account_product = explore(env['res.partner.account.product'])
    return account_product.search([('code', '=', name)], limit=1)

def get_customer_tier(name):
    """Get customer tier by name."""
    customer_tier = explore(env['res.partner.tier'])
    return customer_tier.search([('code', '=', name)], limit=1)

def create_accounts():
    account = env['res.partner.account']
    currency_ids = get_currency_ids()
    account_officer_ids = get_account_officer_ids()
    account_type = get_account_type('Savings Accounts')
    account_tier = get_customer_tier('tier_1')
    num = account.search_count([('customer_id', '!=',None)])
    print(f"Total existing partners: {num}")
    if num > 1:
        print("There are existing accounts, skipping account creation.")
        return None
    print("No existing accounts found, proceeding with account creation.")
    #return None
    customers = get_customers()
    tot_created = 0
    print(f"Found {len(customers)} customers to process")
    for customer in customers:
        if customer.customer_id:
            # Generate random data for the account
            name = customer.name or f"{customer.id}"
            print(f"Creating account for {customer.name} with ID: {customer.id}")
            for curr in currency_ids:
                name = f"{customer.name} - {curr.name}"
                cur_id = str(curr.id).zfill(3)
                account_name = f"{cur_id}{customer.customer_id}"
                account_date = get_random_date(1,3).strftime('%Y-%m-%d')
                bal = round(random.uniform(10.0, 1000.0), 2) if curr.name == 'USD' else round(random.uniform(1000.0, 10000000.0), 2)
                account_product = get_account_product('regular_individual_account') if curr.name == 'NGN' else get_account_product('dom_saving')
                account_data = {
                    'customer_id': customer.id,
                    'account_officer_id':random.choice(account_officer_ids) if account_officer_ids else None,
                    'currency_id': curr.id,
                    'currency': curr.name,
                    'opening_date': account_date,
                    'date_created': account_date,
                    'name': str(account_name),
                    'account_name': name,
                    'branch_id': customer.branch_id.id,
                    'customer':customer.name,
                    'account_type_id': account_type.id if account_type else None,
                    'account_type': account_type.name if account_type else None,
                    'account_status': 'active',
                    'balance': bal,
                    'product_id': account_product.id if account_product else None,
                }
                try:
                    tot_created += 1
                    new_account = account.create(account_data)
                    print(f"Created account: {account_name} with ID: {new_account}")
                except Exception as e:
                    print(f"Error creating account for {customer.name}: {e}")
    return tot_created
   
        

cnt = create_accounts()
print(f"Created {cnt} account records" if cnt else "No accounts created")