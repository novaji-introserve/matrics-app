import logging
import csv
import os
from odoo import models, api, tools, fields

_logger = logging.getLogger(__name__)

# class ExceptionDataLoader(models.AbstractModel):
#     _name = 'exception.data.loader'
#     _description = 'Loads Exception Data from CSV'

#     def _get_csv_path(self, filename):
#         """Get the correct path to the CSV file, handling WSL paths if needed"""
#         # Define the direct Linux paths
#         linux_path = f'/home/novaji/odoo16/custom_addons/icomply_odoo/case_management/data/{filename}'
        
#         if os.path.exists(linux_path):
#             _logger.info(f"Found file at Linux path: {linux_path}")
#             return linux_path
            
#         # Try the module's data directory
#         module_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', filename)
#         if os.path.exists(module_path):
#             _logger.info(f"Found file at module path: {module_path}")
#             return module_path
        
#         # Check addons path as fallback
#         for path in tools.config['addons_path'].split(','):
#             potential_path = os.path.join(path.strip(), 'icomply_odoo', 'case_management', 'data', filename)
#             if os.path.exists(potential_path):
#                 _logger.info(f"Found file at addons path: {potential_path}")
#                 return potential_path
        
#         _logger.error(f"Could not find file {filename} in any location")
#         raise FileNotFoundError(f"File {filename} not found in any expected location")

#     @api.model
#     def load_exception_process_types(self):
#         _logger.info("Loading Exception Process Types...")
        
#         try:
#             file_path = self._get_csv_path('exception.process.type.csv')
#             success_count = 0
#             error_count = 0
            
#             with open(file_path, 'r', newline='', encoding='utf-8-sig') as csvfile:
#                 _logger.info(f"Successfully opened file: {file_path}")
#                 reader = csv.DictReader(csvfile)
                
#                 # Print headers to debug
#                 _logger.info(f"CSV headers: {reader.fieldnames}")
                
#                 for row in reader:
#                     try:
#                         # Make sure the required fields exist
#                         if 'NUM_ID' not in row or not row['NUM_ID']:
#                             _logger.warning(f"Skipping row missing NUM_ID: {row}")
#                             continue
                            
#                         if 'NAME' not in row or not row['NAME']:
#                             _logger.warning(f"Skipping row missing NAME: {row}")
#                             continue
                        
#                         # Debug the row content
#                         _logger.info(f"Processing row: {row}")
                        
#                         # Try to convert NUM_ID to integer
#                         num_id = 0
#                         try:
#                             num_id = int(float(row['NUM_ID']))
#                         except (ValueError, TypeError) as e:
#                             _logger.warning(f"Invalid NUM_ID value '{row['NUM_ID']}': {e}")
#                             num_id = 0
                        
#                         # Try to convert CATEGORY_ID to integer or False
#                         category_id = False
#                         if 'CATEGORY_ID' in row and row['CATEGORY_ID']:
#                             try:
#                                 category_id = int(float(row['CATEGORY_ID']))
#                             except (ValueError, TypeError):
#                                 category_id = False
                        
#                         # Check if record already exists
#                         existing = self.env['exception.process.type'].search([
#                             ('num_id', '=', num_id)
#                         ], limit=1)
                        
#                         if existing:
#                             # Update existing record
#                             existing.write({
#                                 'name': row['NAME'],
#                                 'category_id': category_id,
#                             })
#                             _logger.info(f"Updated type: {row['NAME']} (ID: {num_id})")
#                         else:
#                             # Create new record
#                             self.env['exception.process.type'].create({
#                                 'num_id': num_id,
#                                 'name': row['NAME'],
#                                 'category_id': category_id,
#                             })
#                             _logger.info(f"Created type: {row['NAME']} (ID: {num_id})")
                        
#                         success_count += 1
#                     except Exception as e:
#                         _logger.error(f"Error processing exception.process.type for row {row}: {str(e)}")
#                         error_count += 1
                        
#             _logger.info(f"Exception Process Types loading completed. Success: {success_count}, Errors: {error_count}")
#             return True
            
#         except Exception as e:
#             _logger.error(f"Failed to load Exception Process Types: {str(e)}")
#             return False

#     @api.model
#     def load_exception_processes(self):
#         _logger.info("Loading Exception Processes...")
        
#         try:
#             file_path = self._get_csv_path('exception.process.csv')
#             success_count = 0
#             error_count = 0
            
#             with open(file_path, 'r', newline='', encoding='utf-8-sig') as csvfile:
#                 _logger.info(f"Successfully opened file: {file_path}")
#                 reader = csv.DictReader(csvfile)
                
#                 # Print headers to debug
#                 _logger.info(f"CSV headers: {reader.fieldnames}")
                
#                 for row in reader:
#                     try:
#                         # Debug the row content
#                         _logger.info(f"Processing row: {row}")
                        
#                         # Safely convert values
#                         def safe_int(value):
#                             try:
#                                 return int(float(value)) if value and str(value).strip() else False
#                             except (ValueError, TypeError):
#                                 return False
                        
#                         # Check for name to avoid creating blank records
#                         if 'NAME' not in row or not row['NAME']:
#                             _logger.warning(f"Skipping row with missing NAME: {row}")
#                             continue
                        
#                         # Get type_id from num_id
#                         type_id = False
#                         if 'TYPE_ID' in row and row['TYPE_ID']:
#                             type_num_id = safe_int(row['TYPE_ID'])
#                             if type_num_id:
#                                 type_record = self.env['exception.process.type'].search([
#                                     ('num_id', '=', type_num_id)
#                                 ], limit=1)
#                                 if type_record:
#                                     type_id = type_record.id
                        
#                         # Create values dictionary with safe conversions
#                         values = {
#                             'name': row.get('NAME', ''),
#                             'sql_text': row.get('SQL_TEXT', ''),
#                             'frequency': row.get('FREQUENCY', ''),
#                             'category_id': safe_int(row.get('CATEGORY_ID')),
#                             'email_to': row.get('EMAIL_TO', ''),
#                             'state': row.get('STATE', ''),
#                             'alert_group_id': safe_int(row.get('ALERT_GROUP_ID')),
#                             'branch_code': safe_int(row.get('BRANCH_CODE')),
#                             'type_id': type_id,
#                             'risk_rating_id': safe_int(row.get('RISK_RATING_ID')),
#                             'first_line_owner': safe_int(row.get('FIRST_LINE_OWNER')),
#                             'second_line_owner': safe_int(row.get('SECOND_LINE_OWNER')),
#                             'first_line_owner_id': safe_int(row.get('FIRST_OWNER_ID')),
#                             'second_owner_id': safe_int(row.get('SECOND_OWNER_ID')),
#                             'user_id': safe_int(row.get('USER_ID')),
#                         }
                        
#                         # Handle date fields separately
#                         if row.get('APPROVED_AT'):
#                             try:
#                                 values['approved_at'] = row['APPROVED_AT']
#                             except Exception as e:
#                                 _logger.warning(f"Could not parse date {row['APPROVED_AT']}: {e}")
                        
#                         # Check if record already exists
#                         existing = self.env['exception.process'].search([
#                             ('name', '=', row['NAME'])
#                         ], limit=1)
                        
#                         if existing:
#                             existing.write(values)
#                             _logger.info(f"Updated process: {row['NAME']}")
#                         else:
#                             self.env['exception.process'].create(values)
#                             _logger.info(f"Created process: {row['NAME']}")
                            
#                         success_count += 1
#                     except Exception as e:
#                         _logger.error(f"Error processing exception.process for row {row}: {str(e)}")
#                         error_count += 1
                        
#             _logger.info(f"Exception Processes loading completed. Success: {success_count}, Errors: {error_count}")
#             return True
            
#         except Exception as e:
#             _logger.error(f"Failed to load Exception Processes: {str(e)}")
#             return False

#     @api.model
#     def load_all_exception_data(self):
#         """Load all exception data in the correct order"""
#         _logger.info("Starting to load all exception data...")
#         success_types = self.load_exception_process_types()
#         success_processes = self.load_exception_processes()
#         _logger.info(f"Completed loading all exception data. Types: {'Success' if success_types else 'Failed'}, Processes: {'Success' if success_processes else 'Failed'}")
#         return success_types and success_processes

class ExceptionCategory(models.Model):
    _name = 'exception.category'
    _description = 'Exception Category'

    name = fields.Char(string='Name', size=50, required=False)
    description = fields.Char(string='Description', size=50, required=False)
    code = fields.Char(string='Code', size=20, required=False)
    created_at = fields.Datetime(string='Created At', required=False)



# models/exception_models.py
from odoo import models, fields, api

class ExceptionProcessType(models.Model):
    _name = 'exception.process.type'
    _description = 'Exception Process Type'

    num_id = fields.Float(string='ID', required=True)
    name = fields.Char(string='Name', required=True)
    
    def name_get(self):
        return [(record.id, f"{record.name}") for record in self]

class ExceptionProcess(models.Model):
    _name = 'exception.process'
    _description = 'Exception Process'

    name = fields.Char(string='Name', required=True)
    # type_id = fields.Float(string='Type ID', required=True)
    type_id = fields.Many2one('exception.process.type', string='Process Type', required=True)
    
    # Add this related field for easier domain filtering
    process_type_id = fields.Many2one('exception.process.type', string='Process Type',
                                      compute='_compute_process_type_id', store=True)
    
    
    @api.depends('type_id')
    def _compute_process_type_id(self):
        """Map the type_id to an actual process_type record"""
        for record in self:
            process_type = self.env['exception.process.type'].search([('num_id', '=', record.type_id.num_id)], limit=1)

           # process_type = self.env['exception.process.type'].search([('num_id', '=', record.type_id)], limit=1)
            record.process_type_id = process_type.id if process_type else False
    
    def name_get(self):
        return [(record.id, f"{record.name}") for record in self]



# class ExceptionProcessType(models.Model):
#     _name = 'exception.process.type'
#     _description = 'Exception Process Type'

#     name = fields.Char(string='Name', required=True)
#     num_id = fields.Integer(string='NUM ID', index=True)
#     active = fields.Boolean(string='Active', default=True)
#     category_id = fields.Many2one('exception.category', string='Category')


# class ExceptionProcessType(models.Model):
#     _name = 'exception.process.type'
#     _description = 'Exception Process Type'

#     active = fields.Boolean(string='Active', default=True)

#     name = fields.Char(string='Name', required=True)
#     num_id = fields.Integer(string='NUM ID', required=True, index=True, unique=True)
#     category_id = fields.Many2one('exception.category', string='Category', required=False)




class ComplianceRiskRating(models.Model):
    _name = 'compliance.risk.rating'
    _description = 'Compliance Risk Rating'

    name = fields.Selection([
        ('1', 'Low'),
        ('2', 'Medium'),
        ('3', 'High')
    ], string='Risk Level', required=True)
    
    description = fields.Char(string='Description', compute='_compute_description', store=True)
    
    @api.depends('name')
    def _compute_description(self):
        for record in self:
            risk_mapping = {'1': 'Low', '2': 'Medium', '3': 'High'}
            record.description = risk_mapping.get(record.name, '')




# class ExceptionProcess(models.Model):
#     _name = 'exception.process'
#     _description = 'Exception Process'

#     name = fields.Char(string='Name', required=True)
#     active = fields.Boolean(string='Active', default=True)

#     type_id = fields.Many2one('exception.process.type', string='Process Type')  # FIXED
#     sql_text = fields.Char(string='SQL Text')
#     frequency = fields.Char(string='Frequency')
#     email_to = fields.Char(string='Email To')
#     state = fields.Char(string='State')

#     category_id = fields.Many2one('exception.category', string='Category')
#     alert_group_id = fields.Many2one('alert.group', string='Alert Group')
#     branch_code = fields.Many2one('branch', string='Branch')
#     risk_rating_id = fields.Many2one('compliance.risk.rating', string='Risk Rating')
#     policy_id = fields.Many2one('policy', string='Policy')
#     user_id = fields.Many2one('res.users', string='User')

#     first_line_owner = fields.Integer(string='First Line Owner')
#     second_line_owner = fields.Integer(string='Second Line Owner')
#     first_line_owner_id = fields.Integer(string='First Line Owner ID')
#     second_owner_id = fields.Integer(string='Second Owner ID')

#     approved_at = fields.Datetime(string='Approved At')
