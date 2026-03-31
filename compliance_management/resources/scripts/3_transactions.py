

# https://github.com/kmagusiak/odoo-connect
import odoo_connect
from odoo_connect.explore import explore
import pandas as pd
import numpy as np
import random
from datetime import datetime
from dateutil.relativedelta import relativedelta
import uuid
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

def get_account_officer(id):
    return explore(env['account.officers']).search([('id', '=', id)], limit=1)

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

def get_accounts():
    """Get accounts."""
    accounts = explore(env['res.partner.account'])
    return accounts.search([], limit=1000)
    #return accounts.search([('active', '=', True)], limit=1000)

def generate_custom_id(prefix="TRN"):
    return f"{prefix}_{str(uuid.uuid4()).replace('-', '')[:10]}"

def get_transaction_type(trancode):
    """Get transaction type by name."""
    transaction_type = explore(env['res.transaction.type'])
    return transaction_type.search([('trancode', '=', trancode)], limit=1)

def get_authorizer_id():
    """Get authorizer."""
    return lambda: random.choice(get_account_officer_ids()) if get_account_officer_ids() else None

def create_transactions():
    tran = env['res.customer.transaction']
    currency_ids = get_currency_ids()
    account_officer_ids = get_account_officer_ids()
    account_type = get_account_type('Savings Accounts')
    account_tier = get_customer_tier('tier_1')
    num = tran.search_count([])
    print(f"Total existing transactions: {num}")
    #if num > 1:
    #    print("There are existing transactions, skipping transactions creation.")
    #    return None
    #print("No existing transactions found, proceeding with transaction creation.")
    accnts = get_accounts()
    tot_created = 0
    print(f"Found {len(accnts)} accounts to process")
    for ac in accnts:
        if ac.customer_id:
            # Generate random data for the account
            name = generate_custom_id(prefix="TRN")
            amount = round(random.uniform(-1.5, 2.0), 2) if ac.currency == 'USD' else round(random.uniform(-50.0, 50.0), 2)
            narration = f"Transaction for {ac.name} with Tran ID: {name}"
            date_created = get_random_date(1, 2).strftime('%Y-%m-%d')
            transaction_type = get_transaction_type('DEP') if amount > 0 else get_transaction_type('WDR')
            inputter ='SYSTEM'
            officer = get_account_officer(ac.account_officer_id.id) if ac.account_officer_id else get_authorizer_id()
            authorizer = officer.code
            tran_code = f"{transaction_type.trancode}{date_created.replace('-','')}" if transaction_type else None
            print(f"Account ID: {ac.id}, Amount: {amount}, Currency: {ac.currency}")
            tran_data = {
                'customer_id': ac.customer_id.id,
                'account_id': ac.id,
                'currency_id': ac.currency_id.id if ac.currency_id else None,
                'currency': ac.currency,
                'amount': (amount * 1025),
                'narration': narration,
                'date_created': date_created,
                'name': name,
                'account_officer_id': ac.account_officer_id.id if ac.account_officer_id else None,
                'tran_type': transaction_type.id if transaction_type else None,
                'inputter': inputter,
                'authorizer': authorizer,
                'branch_id': ac.branch_id.id if ac.branch_id else None,
                'trans_code': tran_code,
                'transaction_type': transaction_type.trantype if transaction_type else None,
                'batch_code': f"{tran_code}_{name}",
            }
            try:
             
                new_id =  tran.create(tran_data)
                print(f"Created transaction: {name} with ID: {new_id}")
                tot_created += 1
            except Exception as e:
                print(f"Error creating transaction for {ac.name}: {e}")
    return tot_created
   
        

cnt = create_transactions()
print(f"Created {cnt} transactions records" if cnt else "No transactions created")