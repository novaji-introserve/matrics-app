#!/usr/bin/env python
# pip install click-odoo pandas
# Run from Odoo directory
#  cd /home/jonathan/odoo-16.0 && python -m click_odoo -d icomply_dev -c /home/jonathan/etc/odoo.conf  /home/jonathan/Projects/icomply_odoo/compliance_management/resources/scripts/task-1-load-pep.py
import pandas as pd

# Change path to list as required
PEP_LIST='/home/jonathan/data_pro_pep.csv'

# Read the CSV file into a DataFrame
df = pd.read_csv(PEP_LIST)
df = df.fillna('')
df = df.replace('NaN', '')
df = df.replace('NULL', '')
env['res.pep'].search([]).unlink()

for index, row in df.iterrows():
    d = row.to_dict()
    try:
        env['res.pep'].create({
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
        })
    except Exception as e:
        print(e)