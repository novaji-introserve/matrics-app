

# https://github.com/kmagusiak/odoo-connect
import odoo_connect
import pandas as pd
import numpy as np

chunk_size = 1000
df = pd.read_csv("customers.csv", header=0, delimiter=',', encoding='utf-8',
                 index_col='ID', usecols=['ID', 'name', 'customer_id', 'branch_id', 'phone'])
df = df.where(pd.notnull(df), None)
df['customer_id'] = df['customer_id'].fillna(0).astype('string')
df['branch_id'] = df['branch_id'].fillna(0).astype('int64')
df['phone'] = df['phone'].fillna(0).astype('float64').astype('Int64')
total_rows = len(df)
#  dtype={'name':np.strings, 'customer_id':np.strings, 'branch_id':np.strings, 'phone':np.strings}
# First 5 rows
print(total_rows)
rows = df.head()
print(len(rows))

for i in range(0, total_rows, chunk_size):
    chunk = df.iloc[i:i + chunk_size]
    print(f"Processing rows {i} to {i + len(chunk) - 1}")
    records = []
    for index, row in chunk.iterrows():
        record = {
            'name': row['name'],
            'customer_id': row['customer_id'],
            'firstname': row['name'].capitalize().split()[0] if pd.notnull(row['name']) else '',
            'lastname': ' '.join(row['name'].split()[1:]).capitalize() if pd.notnull(row['name']) else '',
             'customer_id': int(float(row['customer_id'])) if pd.notnull(row['customer_id']) else '',
            'branch_id': row['branch_id'],
            'phone': row['phone'],
            'origin':"demo"
        }
        records.append(record)
    print(records)
    # Connect to Odoo and create records
    odoo = env = odoo_connect.connect(url='http://localhost:8069', database='icomply_dev',username='admin', password='admin')
    partner = env['res.partner']
    created_records = partner.create(records)
    print(f"Created {len(created_records)} records")
     
"""
odoo = env = odoo_connect.connect(url='http://localhost:8069', database='icomply_dev',username='admin', password='admin')
partner = env['res.partner']
record = {'name': 'Test Partner2','branch_id': 1,'origin':"demo","firtname":"Test","lastname":"Partner2","email":""}
ret = partner.create(record)
print(ret)
"""
# partners = partner.search_read([('create_uid', '=', 1)], [])
# print(partners[1]['name'])  # Example to print the name of the first partner


"""



"""
"""
from pyodoo_connect import connect_odoo,connect_model

URL = "http://0.0.0.0:8069/"
# Get session ID from Odoo
session_id = connect_odoo(
    url="http://0.0.0.0:8069/",
    db="icomply_dev",
    username="admin",
    password="admin"
)

partner_model = connect_model(
    session_id=session_id,
    url="http://0.0.0.0:8069/",
    model="res.partner"
)
partners = partner_model.search([])
for partner in partners:
    print(f"Partner ID: {partner['id']}, Name: {partner['name']}")

"""
