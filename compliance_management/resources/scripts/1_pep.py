#!/usr/bin/env python
# pip install click-odoo pandas
# Run from Odoo directory
#  cd /home/jonathan/odoo-16.0 && python -m click_odoo -d icomply_dev -c /home/jonathan/etc/odoo.conf  /home/jonathan/Projects/icomply_odoo/compliance_management/resources/scripts/task-1-load-pep.py
import odoo_connect
from odoo_connect.explore import explore
import pandas as pd

# Change path to list as required
PEP_LIST='/home/jonathan/data_pro_pep.csv'
chunk_size = 500
# Read the CSV file into a DataFrame
data_types={"Unique_Identifier": str,
            "Surname": str,
            "First_Name": str,
            "Middle_Name": str,
            "Title": str,
            "AKA": str,
            "Date_of_Birth": str,"Present_Position": str,
            "Previous_Position": str,
            "PEP_Classification": str,
            "Official_Address": str,
            "Profession": str,
            "Residential_Address": str,
            "State_Of_Origin": str,
            "Spouse": str,
            "Children": str,
            "Sibling": str,
            "Parents": str,
            "MOTHERS_MADEN_NAME": str,
            "Associates__Business_Political_Social_": str,
            "Bankers": str,
            "Account_Details": str,
            "Place_Of_Birth": str,
            "Press_Report": str,
            "Date_Report": str,
            "Additional_Info": str,
            "Email": str,
            "Remarks": str,
            "Status": str,
            "Business_Interest": str,
            "Age": str,
            "ASSOCIATE_BUSINESS_POLITICS": str,
            "POB": str,
            "CreatedBy": str,
            "CreatedOn": str,
            "CreatedByEmail": str,
            "LastModifiedBy": str,
            "LastModifiedOn": str,
            "LastModifiedByEmail": str} 
df = pd.read_csv(PEP_LIST,delimiter=',',header=0, dtype=data_types, encoding='utf-8')
# Fill NaN values with empty strings
df = df.where(pd.notnull(df), None)
# Replace NaN and NULL with empty strings
df = df.fillna('')
df = df.replace('NaN', '')
df = df.replace('NULL', '')
total_rows = len(df)
odoo = env = odoo_connect.connect(url='http://localhost:8069', database='icomply_dev',username='admin', password='admin')
pep = env['res.pep']
#env['res.pep'].search([]).unlink()
for i in range(0, total_rows, chunk_size):
    chunk = df.iloc[i:i + chunk_size]
    print(f"Processing rows {i} to {i + len(chunk) - 1} of {total_rows}")
    # Here you can process the chunk as needed, e.g., save to database
    # For example, you can call a function to save the chunk to the database
    # save_to_database(chunk)
    
    for index, row in chunk.iterrows():
        d = row.to_dict()
        try:
            #print(d)
            doc = {
                'unique_identifier': d['Unique_Identifier'],
                'surname': d['Surname'],
                'first_name': d['First_Name'],
                'middle_name': d['Middle_Name'],
                'title': d['Title'],
                'aka': d['AKA'],
                'sex': d['Sex'],
                'date_of_birth': d['Date_of_Birth'],
                'present_position': d['Present_Position'],
                'previous_position': d['Previous_Position'],
                'pep_classification': d['PEP_Classification'],
                'official_address': d['Official_Address'],
                'profession': d['Profession'],
                'residential_address': d['Residential_Address'],
                'state_of_origin': d['State_Of_Origin'],
                'spouse': d['Spouse'],
                'children': d['Children'],
                'sibling': d['Sibling'],
                'parents': d['Parents'],
                'mothers_maden_name': d['MOTHERS_MADEN_NAME'],
                'associates__business_political_social_': d['Associates__Business_Political_Social_'],
                'bankers': d['Bankers'],
                'account_details': d['Account_Details'],
                'place_of_birth': d['Place_Of_Birth'],
                'press_report': d['Press_Report'],
                'date_report': d['Date_Report'],
                'additional_info': d['Additional_Info'],
                'email': d['Email'],
                'remarks': d['Remarks'],
                'status': d['Status'],
                'business_interest': d['Business_Interest'],
                'age': d['Age'],
                'associate_business_politics': d['ASSOCIATE_BUSINESS_POLITICS'],
                'pob': d['POB'],
                'createdby': d['CreatedBy'],
                'createdon': d['CreatedOn'],
                'createdbyemail': d['CreatedByEmail'],
                'lastmodifiedby': d['LastModifiedBy'],
                'lastmodifiedon': d['LastModifiedOn'],
                'lastmodifiedbyemail': d['LastModifiedByEmail']
            }
            pep.create(doc)
        except Exception as e:
            pass
            #print(e)