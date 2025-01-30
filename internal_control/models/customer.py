from odoo import models, fields, api, _
# import logging
# import traceback
# _logger = logging.getLogger(__name__)

class Customer(models.Model): 
    _inherit = "res.partner" 
      
    customertype = fields.Many2one(
        comodel_name='res.customer.type', string='Customer Type', tracking=True, index=True)
    nationality = fields.Many2one(
        comodel_name='res.country', string='Country', tracking=True, index=True)
    state_id = fields.Many2one(
        comodel_name='res.country.state', string='State', tracking=True, index=True)
    occupation = fields.Char(string='Occupation', index=True, tracking=True)
    date_opened = fields.Char(string='Date Opened', index=True, tracking=True)
    address = fields.Char(string='Address', index=True, tracking=True)
    nin = fields.Char(string='NIN', index=True, tracking=True)
    status = fields.Many2one(
        comodel_name='res.user.status', string='Status', tracking=True, index=True)
    phone1 = fields.Char(string='Phone', index=True, tracking=True)
    identification_issue_date = fields.Char(string='identification Issue Date', index=True, tracking=True)
    town_id = fields.Many2one(
    comodel_name='res.partner.town', string='Town', index=True)


    @api.model
    def open_all_customers(self):
        return {
            'name': _('Customers'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': [('create_uid','=',False)],
            'context': {'search_default_group_branch': 1}
        }
            # 'domain': [('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),('create_uid','=',False)],


    # Script to check field data integrity
    def check_record_fields(record):
        for field_name in record._fields:
            try:
                value = record[field_name]
                # Check if value is a recordset or has unexpected type
                print(f"Field {field_name}: {type(value)}")
            except Exception as e:
                print(f"Issue with field: {field_name}, Error: {e}")

    # @api.model
    # def read(self, fields=None, load='_classic_read'):
    #     try:
    #         # Validate fields before reading
    #         valid_fields = [
    #             field for field in fields 
    #             if isinstance(field, str)  # Ensure only string field names
    #         ]
            
    #         # Log problematic inputs
    #         if len(fields) != len(valid_fields):
    #             _logger.error(f"Invalid fields detected: {fields}")
            
    #         return super().read(fields=valid_fields, load=load)
    #     except Exception as e:
    #         _logger.error(f"Read error: {str(e)}")
    #         _logger.error(f"Fields causing error: {fields}")
    #         raise

    # def check_record_integrity(self, record_id):
    #     record = self.browse(record_id)
    #     if not record.exists():
    #         _logger.error(f"Record {record_id} does not exist")
    #         return False
    #     return True
        
    # def check_record_integrity(self, record_id):
    #     record = self.browse(record_id)
    #     if not record.exists():
    #         _logger.error(f"Record {record_id} does not exist")
    #         return False
    #     return True