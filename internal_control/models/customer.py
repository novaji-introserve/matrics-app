from odoo import models, fields, api, _
import logging
from odoo.exceptions import UserError
from datetime import date,timedelta,datetime,time
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
    # account_officer_id = fields.Many2one(
    #     comodel_name='res.account.officer', string='Account Officer', index=True, tracking=True)
    officer_code = fields.Many2one(
        comodel_name='res.account.officer', string='Account Officer', index=True, tracking=True)


    @api.model
    def open_all_customers_today(self):
        
        today = fields.Date().today()
    

        domain = [("create_uid", "=", False), ("create_date", ">=", datetime.combine(today, time.min)), ("create_date", "<=", datetime.combine(today, time.max))]
        

         # Check if the current user is a Chief Compliance Officer
        is_cco = self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')

        if not is_cco:  # Only apply branch filtering if not a CCO
            domain.append(('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]))
            
        return {
            'name': _('Customers - Today'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }
            # 'domain': [('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),('create_uid','=',False)],
    @api.model
    def open_all_customers_last_7days(self):
        today = fields.Date().today()

        # Calculate the START of the 7-day period (inclusive of the 7th day)
        last_7_days_start = today - timedelta(days=7)

        # Calculate the END of the 7-day period (inclusive of today)
        today_end = today
        


        domain = [
            ("create_uid", "=", False),
            ("create_date", ">=", datetime.combine(last_7_days_start, time.min)), # Start of the 7 day period
            ("create_date", "<=", datetime.combine(today_end, time.max)), # End of today
        ]

        is_cco = self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')

        if not is_cco:
            branch_ids = self.env.user.branches_id.ids
            if branch_ids:
                domain.append(('branch_id', 'in', branch_ids))

        return {
            'name': _('Customers - Last 7Days'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}  # Test if still needed
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
                WHERE rp.customer_id = rpa.customer_id::text
                AND rp.branch_id IS NULL
                AND rp.customer_id IS NOT NULL
                AND rpa.branch_id IS NOT NULL
                RETURNING rp.id, rp.customer_id, rpa.branch_id;
            """

            self.env.cr.execute(query)
            updated_records = self.env.cr.fetchall()

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
