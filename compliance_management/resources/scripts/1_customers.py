

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

df = pd.read_csv("customers.csv", header=0, delimiter=',', encoding='utf-8',
                 index_col='ID', usecols=['ID', 'name', 'customer_id', 'phone'])
df = df.where(pd.notnull(df), None)
df['customer_id'] = df['customer_id'].fillna(0).astype('string')
#df['branch_id'] = df['branch_id'].fillna(0).astype('int64')
df['phone'] = df['phone'].fillna(0).astype('Int64')
total_rows = len(df)
odoo = env = odoo_connect.connect(url=os.getenv("HOST_URL"), database=os.getenv("DB"),username=os.getenv("USERNAME"), password=os.getenv('PASSWORD'))

def get_branch_ids():
    branches = explore(env['res.branch'])
    branch_ids = []
    for b in branches.search([]):
        branch_ids.append(b.id)
    return branch_ids

def get_branch(id):
    return explore(env['res.branch']).search([('id', '=', id)], limit=1)

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

def create_customers():
    branch_ids = get_branch_ids()
    sector_ids = get_sector_ids()
    country_ids = get_country_ids()
    education_level_ids = get_education_level_ids()
    partner = env['res.partner']
    num_partners = partner.search_count([('customer_id', '!=',None)])
    print(f"Total existing partners: {num_partners}")
    if num_partners > 10:
        print("There are existing customers, skipping customer creation.")
        return None
    for i in range(0, total_rows, chunk_size):
        chunk = df.iloc[i:i + chunk_size]
        print(f"Processing rows {i} to {i + len(chunk) - 1}")
        records = []
        for index, row in chunk.iterrows():
            firstname = ' '.join(row['name'].split()[1:]).capitalize() if pd.notnull(row['name']) else ''
            lastname = row['name'].capitalize().split()[0] if pd.notnull(row['name']) else ''
            customer_id = int(float(row['customer_id'])) if pd.notnull(row['customer_id']) else ''
            email =  f"{firstname.lower()}.{lastname.lower()}@example.com" if firstname and lastname else ''
            branch_id = random.choice(branch_ids) if branch_ids else None
            branch = get_branch(branch_id) if branch_id else None
           
            record = {
                'name': f"{firstname} {lastname}",
                'customer_id': customer_id,
                'firstname': firstname,
                'lastname': lastname,
                'customer_id': int(float(row['customer_id'])) if pd.notnull(row['customer_id']) else '',
                'branch_id': branch_id,  # Randomly select a branch ID,
                'region_id': branch.region_id.id,  # Get region ID from the branch,
                'phone': str(row['phone']),
                'mobile': str(row['phone']),
                'origin':"prod",
                'bvn':customer_id,
                'email': email,
                'country_id': random.choice(country_ids) if country_ids else None,  # Randomly select a country ID
                'sector_id': random.choice(sector_ids) if sector_ids else None,  # Randomly select a sector ID,
                'education_level_id': random.choice(education_level_ids) if education_level_ids else None,  # Randomly select an education level ID
                'dob': get_random_date(18, 75).strftime('%Y-%m-%d'),
                'registration_date': get_random_date(0,3).strftime('%Y-%m-%d')
                
            }
            records.append(record)
        #print(records)
        created_records = partner.create(records)
        return created_records
        

customers = create_customers()
print(f"Created {len(customers)} customer records" if customers else "No customers created")