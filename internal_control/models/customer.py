from odoo import models, fields, api, _
import logging
# import traceback
_logger = logging.getLogger(__name__)

class Customer(models.Model): 
    _inherit = "res.partner" 
      
    customertype = fields.Many2one(
        comodel_name='res.customer.type', string='Customer Type', tracking=True, index=True)
    nationality = fields.Many2one(
        comodel_name='res.country', string='Country', tracking=True, index=True)
    state_id = fields.Many2one(
        comodel_name='res.country.state', string='State', tracking=True, index=True)
    occupation = fields.Char(string='Occupation', index=True, tracking=True)
    date_opened = fields.Date(string='Date Opened', index=True, tracking=True)
    address = fields.Char(string='Address', index=True, tracking=True)
    nin = fields.Char(string='NIN', index=True, tracking=True)
    status = fields.Many2one(
        comodel_name='res.user.status', string='Status', tracking=True, index=True)
    phone1 = fields.Char(string='Phone', index=True, tracking=True)
    identification_issue_date = fields.Date(string='identification Issue Date', index=True, tracking=True)
    town_id = fields.Many2one(
    comodel_name='res.partner.town', string='Town', index=True)
    
    def read(self, fields=None, load='_classic_read'):
        # Call the super method to get the default behavior
        records = super(Customer, self).read(fields, load)

        # Process the records to convert datetime to date format
        for record in records:
            if 'date_opened' in record:
                record['date_opened'] = self._convert_to_date(record['date_opened'])
            
        return records

    def _convert_to_date(self, datetime_value):
        """Convert a datetime string to a date string in YYYY-MM-DD format."""
        if datetime_value:
            datetime_obj = datetime.fromisoformat(datetime_value)
            return datetime_obj.date().isoformat()  # Return as YYYY-MM-DD
        return None  #
   


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



    def _sync_branch_id_from_accounts_sql(self):
        """
        Sync branch_id using direct SQL query
        """
        try:
            query = """
                UPDATE res_partner rp
                SET branch_id = rpa.branch_id
                FROM res_partner_account rpa
                WHERE rp.customer_id::numeric = rpa.customer_id
                AND rp.branch_id IS NULL
                AND rp.customer_id IS NOT NULL
                AND rpa.branch_id IS NOT NULL
                RETURNING rp.id, rp.customer_id, rpa.branch_id;
            """
            
            self.env.cr.execute(query)
            updated_records = self.env.cr.fetchall()
            self.env.cr.commit()
            
            _logger.info(f"Branch ID sync completed. Updated {len(updated_records)} records")
            
            # Log detailed updates
            for record in updated_records:
                _logger.info(f"Updated partner ID {record[0]}, customer_id {record[1]} with branch_id {record[2]}")
                
        except Exception as e:
            _logger.error(f"Error in branch ID sync: {str(e)}")
            raise e

    # Updated cron method that uses SQL approach
    def sync_branch_id_from_accounts(self):
        """
        Main cron job method - uses SQL approach by default
        """
        return self._sync_branch_id_from_accounts_sql()

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