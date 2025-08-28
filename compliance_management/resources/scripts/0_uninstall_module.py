import erppeek
import os
from dotenv import load_dotenv, dotenv_values 
load_dotenv()

# Connect to Odoo
client = erppeek.Client(os.getenv("HOST_URL"), db=os.getenv("DB"), user=os.getenv("USERNAME"), password=os.getenv("PASSWORD"))

# Uninstall a single module
module_name = 'access_control'
module = client.model('ir.module.module')

# Find the module
module_record = module.search([('name', '=', module_name), ('state', '=', 'installed')])

if module_record:
    # Uninstall the module
    module.browse(module_record).button_immediate_uninstall()
    print(f"Module '{module_name}' uninstalled successfully")
else:
    print(f"Module '{module_name}' not found or not installed")