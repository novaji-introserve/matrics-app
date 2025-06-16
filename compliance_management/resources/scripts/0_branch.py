

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

df = pd.read_csv("branches.csv", header=0, delimiter=',', encoding='utf-8', usecols=['branch_code', 'name','region_id'])
df = df.where(pd.notnull(df), None)
df['region_id'] = df['region_id'].fillna(0).astype('string')
df['branch_code'] = df['branch_code'].fillna(0).astype('string')
total_rows = len(df)
odoo = env = odoo_connect.connect(url=os.getenv("HOST_URL"), database=os.getenv("DB"),username=os.getenv("USERNAME"), password=os.getenv('PASSWORD'))


def create_branches():
    branch = env['res.branch']
    cnt = branch.search_count([])
    print(f"Total existing branches: {cnt}")
    if cnt >= 1:
        print("There are existing branches, skipping creation.")
        return None
    for i in range(0, total_rows, chunk_size):
        chunk = df.iloc[i:i + chunk_size]
        print(f"Processing rows {i} to {i + len(chunk) - 1}")
        records = []
        for index, row in chunk.iterrows():
            branch_code = row['branch_code']
            name = row['name']
            record = {
                'code': branch_code,
                'name': name
            }
            records.append(record)
        #print(records)
        created_records = branch.create(records)
        return created_records
        

records = create_branches()
print(f"Created {len(records)} records" if records else "No records created")