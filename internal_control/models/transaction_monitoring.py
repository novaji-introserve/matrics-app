from odoo import models, fields, api, _
import logging
from odoo.exceptions import ValidationError
from datetime import datetime, timedelta,date


_logger = logging.getLogger(__name__)

class TransactionMonitoring(models.Model):
    _inherit = 'res.customer.transaction'
    _sql_constraints = [
        ('uniq_trans_id', 'unique(trans_id)',
         "Transaction already exists. Value must be unique!"),
    ] 

    def init(self):
        """Automatically setup transaction triggers when model initializes"""
        super().init()
        try:
            # Check if triggers already exist to avoid recreating them
            if not self._verify_transaction_triggers():
                _logger.info("Transaction triggers not found, setting up production system...")
                self._setup_transaction_indexes()
                self._setup_transaction_triggers()
                _logger.info("Transaction production system initialized automatically")
        except Exception as e:
            _logger.warning(f"Auto-setup failed, manual setup may be required: {str(e)}")
            # Don't raise the exception to prevent module loading failure
            pass

    # id = fields.Integer(string="id", readonly=True)
    refno = fields.Char(string="Ref Number", readonly=True, index=True)
    valuedate = fields.Char(string="Value Date", readonly=True, index=True)
    actualdate = fields.Char(string="Actual Date", readonly=True, index=True)
    userid = fields.Char(string="User ID", readonly=True, index=True)
    reversal = fields.Char(string="Reversal Flag", readonly=True, index=True)
    accountmodule = fields.Char(string="Account Module", readonly=True, index=True)
    tellerno = fields.Char(string="Teller Number", readonly=True, index=True)
    deptcode = fields.Many2one(comodel_name='hr.department',
                              string='Dept. Code', index=True)
    status = fields.Many2one(comodel_name='res.transaction.status',
                              string='Trans. Status', index=True)
    tran_channel = fields.Many2one(comodel_name='res_transaction_channel', string="Transaction Channel", readonly=True, index=True)
    request_id = fields.Char(string="Request ID", readonly=True, index=True)
    trans_id = fields.Char(string="Transaction ID", readonly=True, index=True)
    account_group = fields.Char(string="Account Group", index=True)
    transaction_mode = fields.Char(string="Transaction Mode", index=True)
    parent_ledger_id = fields.Char(string="Parent Ledger ID", index=True)
    sub_general_ledger_code = fields.Char(string="Sub General Ledger Code", index=True)
    source_branch_code = fields.Char(string="Source Branch Code", index=True)
    posted_by = fields.Char(string="Posted By", index=True)
    initiated_by = fields.Char(string="Initiated By", index=True)
    account_name = fields.Char(string="Account Name", index=True)

    
    def _sync_transaction_branch_id_sql(self):
        """
        Sync branch_id in res_customer_transaction table using direct SQL query
        """
        try:
            # Query to handle both regular users and special cases (channels/E-Fincore)
            query = """
                -- First handle regular cases - match with hr_employee
                UPDATE res_customer_transaction rct
                SET branch_id = he.branch_id
                FROM hr_employee he
                WHERE rct.userid = he.userid
                AND rct.branch_id IS NULL
                AND rct.userid IS NOT NULL
                AND rct.userid NOT IN ('channels', 'E-Fincore')
                AND he.branch_id IS NOT NULL
                RETURNING rct.id, rct.userid, he.branch_id;
            """
            
            self.env.cr.execute(query)
            updated_records_regular = self.env.cr.fetchall()
            
            # Query to handle special cases - channels and E-Fincore cases
            special_query = """
                UPDATE res_customer_transaction rct
                SET branch_id = rb.id
                FROM res_branch rb
                WHERE rb.code = '001'
                AND rct.branch_id IS NULL
                AND rct.userid IN ('channels', 'E-Fincore')
                RETURNING rct.id, rct.userid, rb.id;
            """
            
            self.env.cr.execute(special_query)
            updated_records_special = self.env.cr.fetchall()
            
            self.env.cr.commit()
            
            total_updated = len(updated_records_regular) + len(updated_records_special)
            _logger.info(f"Transaction Branch ID sync completed. Updated {total_updated} records")
            
            # Log detailed updates for regular cases
            for record in updated_records_regular:
                _logger.info(f"Updated transaction ID {record[0]}, userid {record[1]} with branch_id {record[2]} (via employee)")
                
            # Log detailed updates for special cases
            for record in updated_records_special:
                _logger.info(f"Updated transaction ID {record[0]}, userid {record[1]} with branch_id {record[2]} (special case)")
                
        except Exception as e:
            _logger.error(f"Error in transaction branch ID sync: {str(e)}")
            raise e

    # Main cron method
    def sync_transaction_branch_id(self):
        """
        Main cron job method for transaction branch_id sync
        """
        return self._sync_transaction_branch_id_sql()
    

    def _check_cco_and_get_branch_domain(self):
        """Helper method to check CCO access and return branch domain if needed"""
        is_cco = self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')
        if not is_cco:
            return [('branch_id.id', 'in', [e.id for e in self.env.user.branches_id])]
        return []

    def _filter_customer_accounts(self, domain):
        """
        Flexible method to filter customer accounts based on bank structure
        This method can be configured for different bank requirements
        """
        records = self.env['res.customer.transaction'].search(domain)
        
        # Method 1: Length-based filtering
        # return records.filtered(lambda r: len(str(r.account_id.name)) == 10)
        
        # Method 2: Length + customer_id based filtering
        return records.filtered(lambda r: 
            len(str(r.account_id.name)) == 10 and 
            r.customer_id is not None
        )
    
    def _filter_internal_accounts(self, domain):
        """
        Flexible method to filter internal accounts based on bank structure
        This method can be configured for different bank requirements
        """
        records = self.env['res.customer.transaction'].search(domain)
        
        # Method 1: Length-based filtering
        # return records.filtered(lambda r: len(str(r.account_id.name)) == 14)
        
        # Method 2: Length + customer_id based filtering
        return records.filtered(lambda r: 
            len(str(r.account_id.name)) > 10 and 
            r.customer_id is None
        )
    
    # Keep old methods for backward compatibility

    # def _filter_account_length_10(self, domain):
    #     """Helper method to filter records with account number length of 10 (legacy)"""
    #     return self._filter_account_length_10(domain)
    
     # def _filter_account_length_14(self, domain):
     #     """Helper method to filter records with account number length of 14 (legacy)"""
     #     return self._filter_internal_accounts(domain)
        

    def open_customers_all_transactions_today_ngn(self):
        today = fields.Date.today()
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [
            ('currency_id', '=', ngn_currency.id),
            ('date_created', '=', today)
        ]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_customer_accounts(domain)

   
        
        return {
            'name': _('All NGN Transactions - Today '),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }

    def open_customers_all_transactions_today_other(self):
        today = fields.Date.today()
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('currency_id', '!=', ngn_currency.id),('date_created', '=', today)]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_customer_accounts(domain)

        return {
            'name': _('All Foreign Transactions - Today '),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
        
    def open_customers_all_transactions_7days_ngn(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)

        
        
        domain = [('currency_id', '=', ngn_currency.id),('date_created', '>=', last_7_days),
                ('date_created', '<', today)]
        
        domain.extend(self._check_cco_and_get_branch_domain())

        
        filtered_records = self._filter_customer_accounts(domain)
        
        return {
            'name': _('All NGN Transactions - Last 7 Days '),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
    def open_customers_all_transactions_7days_other(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
                    
        domain = [('currency_id', '!=', ngn_currency.id),('date_created', '>=', last_7_days),
                ('date_created', '<', today)]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_customer_accounts(domain)

        return {
            'name': _('All Foreign Transactions - Last 7 Days '),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
    def open_customers_awaiting_review_today_ngn(self):
        today = fields.Date.today()
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)

        
        domain = [('state', '=', 'new'),('date_created', '=', today), ('currency_id', '=', ngn_currency.id)]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_customer_accounts(domain)
        
        return {
            'name': _('NGN Transactions Awaiting Review - Today'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
        
    def open_customers_awaiting_review_today_other(self):
        today = fields.Date.today()
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)

        
        domain = [('state', '=', 'new'),('date_created', '=', today), ('currency_id', '!=', ngn_currency.id)]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_customer_accounts(domain)
        
        return {
            'name': _('Foreign Transactions Awaiting Review - Today'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
        
    def open_customers_awaiting_review_7days_ngn(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('state', '=', 'new'),('date_created', '>=', last_7_days),
                ('date_created', '<', today), ('currency_id', '=', ngn_currency.id)]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_customer_accounts(domain)
        
        return {
            'name': _('NGN Transactions Awaiting Review - Last 7 Day'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
    def open_customers_awaiting_review_7days_other(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('state', '=', 'new'),('date_created', '>=', last_7_days),
                ('date_created', '<', today), ('currency_id', '!=', ngn_currency.id)]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_customer_accounts(domain)
        
        return {
            'name': _('Foreign Transactions Awaiting Review - Last 7 Day'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
        
    def open_customers_reviewed_today_ngn(self):
        today = fields.Date.today()

        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('state', '=', 'done'),('date_created', '=', today), ('currency_id', '=', ngn_currency.id)]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_customer_accounts(domain)
        
        return {
            'name': _('NGN Reviewed Transactions - Today'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
        
    def open_customers_reviewed_today_other(self):
        today = fields.Date.today()
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)

        
        domain = [('state', '=', 'done'),('date_created', '=', today), ('currency_id', '!=', ngn_currency.id)]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_customer_accounts(domain)
        
        return {
            'name': _('Foreign Reviewed Transactions- Today'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
    
    def open_customers_reviewed_7days_ngn(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('state', '=', 'done'),('date_created', '>=', last_7_days),
                ('date_created', '<', today), ('currency_id', '=', ngn_currency.id)]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_customer_accounts(domain)
        
        return {
            'name': _('NGN Reviewed Transactions - Last 7 Days'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
    def open_customers_reviewed_7days_other(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('state', '=', 'done'),('date_created', '>=', last_7_days),
                ('date_created', '<', today), ('currency_id', '!=', ngn_currency.id)]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_customer_accounts(domain)
        
        return {
            'name': _('Foreign Reviewed Transactions - Last 7 Days'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
        
    def open_internal_all_transactions_today_ngn(self):
        today = fields.Date.today()

        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('date_created', '=', today), ('currency_id', '=', ngn_currency.id)]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_internal_accounts(domain)

        
        
        return {
            'name': _('All NGN Internal Transactions - Today'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
    def open_internal_all_transactions_today_other(self):
        today = fields.Date.today()

        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('date_created', '=', today), ('currency_id', '!=', ngn_currency.id)]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_internal_accounts(domain)
        
        return {
            'name': _('All Foreign Internal Transactions - Today '),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
        
    def open_internal_all_transactions_7days_ngn(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('date_created', '>=', last_7_days),
                ('date_created', '<', today), ('currency_id', '=', ngn_currency.id)]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_internal_accounts(domain)
        
        return {
            'name': _('All NGN Internal Transactions - Last 7 Days'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
    def open_internal_all_transactions_7days_other(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('date_created', '>=', last_7_days),
                ('date_created', '<', today), ('currency_id', '!=', ngn_currency.id)]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_internal_accounts(domain)
        
        return {
            'name': _('All Foreign Internal Transactions - Last 7 Days'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
    def open_internal_awaiting_review_today_ngn(self):
        today = fields.Date.today()
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('date_created', '=', today),('currency_id', '=', ngn_currency.id), ('state', '=', 'new')]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_internal_accounts(domain)
        
        return {
            'name': _('Internal NGN Transactions Awaiting Review - Today'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
    def open_internal_awaiting_review_today_other(self):
        today = fields.Date.today()
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('date_created', '=', today),('currency_id', '!=', ngn_currency.id), ('state', '=', 'new')]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_internal_accounts(domain)
        
        return {
            'name': _('Internal Foreign Transactions Awaiting Review - Today'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
        
    def open_internal_awaiting_review_7days_ngn(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('date_created', '>=', last_7_days),
                ('date_created', '<', today),('currency_id', '=', ngn_currency.id), ('state', '=', 'new')]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_internal_accounts(domain)
        
        return {
            'name': _('Internal NGN Transactions Awaiting Review - Last 7 Days'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
        
    def open_internal_awaiting_review_7days_other(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('date_created', '>=', last_7_days),
                ('date_created', '<', today),('currency_id', '!=', ngn_currency.id), ('state', '=', 'new')]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_internal_accounts(domain)
        
        return {
            'name': _('Internal Foreign Transactions Awaiting Review - Last 7 Days'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
        
    def open_internal_reviewed_today_ngn(self):
        today = fields.Date.today()
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('date_created', '=', today),('currency_id', '=', ngn_currency.id), ('state', '=', 'done')]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_internal_accounts(domain)
        
        return {
            'name': _('Internal NGN Transactions Reviewed - Today'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
    def open_internal_reviewed_today_other(self):
        today = fields.Date.today()
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [('date_created', '=', today),('currency_id', '!=', ngn_currency.id), ('state', '=', 'done')]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_internal_accounts(domain)
        
        return {
            'name': _('Internal Foreign Transactions Reviewed - Today'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
        
    def open_internal_reviewed_7days_ngn(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        
        domain = [('date_created', '>=', last_7_days),
                ('date_created', '<', today),('currency_id', '=', ngn_currency.id), ('state', '=', 'done')]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_internal_accounts(domain)
        
        return {
            'name': _('Internal NGN Transactions Reviewed - Last 7 Days'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
        
    def open_internal_reviewed_7days_other(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        
        domain = [('date_created', '>=', last_7_days),
                ('date_created', '<', today),('currency_id', '!=', ngn_currency.id), ('state', '=', 'done')]
        
        domain.extend(self._check_cco_and_get_branch_domain())
        
        filtered_records = self._filter_internal_accounts(domain)
        
        return {
            'name': _('Internal Foreign Transactions Reviewed - Last 7 Days'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': [('id', 'in', filtered_records.ids)],
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }
        

    # In Odoo shell
    def diagnose_branch_references(self):
        transactions = self.search([('branch_id', '!=', False)])
        print(f"Total transactions with branch: {len(transactions)}")
        for t in transactions[:10]:
            print(f"Transaction ID: {t.id}, Branch: {t.branch_id.id if t.branch_id else 'None'}")
    

    @api.model
    def read_group(self, domain, fields, groupby, offset=0, limit=None, orderby=False, lazy=True):
        try:
            # Detailed logging for branch_id
            branch_records = self.env['res.branch'].search([])
            _logger.error(f"Total Branch Records: {len(branch_records)}")
            
            # Find transactions with problematic branch references
            invalid_branch_records = self.search([
                ('branch_id', '!=', False),
                ('branch_id.id', '=', False)
            ])
            
            if invalid_branch_records:
                _logger.error(f"Transactions with Invalid Branch: {invalid_branch_records.ids}")
                for record in invalid_branch_records:
                    _logger.error(f"Record ID: {record.id}, Branch Value: {record.branch_id}")
            
            return super().read_group(domain, fields, groupby, offset=offset, limit=limit, orderby=orderby, lazy=lazy)
        
        except Exception as e:
            _logger.error(f"Detailed Error: {str(e)}")
            raise


    @api.model
    def action_screen(self, rules=None):
        '''
        Apply additional logic here if needed
        This method is called when the screen action is triggered.
        '''
        # Only call super if there are records to process (avoids ensure_one() on empty set)
        records = self.search([])
        for record in records:
            try:
                super(TransactionMonitoring, record).action_screen(rules=rules)
            except Exception as e:
                _logger.error(f"Error screening transaction {record.id}: {str(e)}")
                continue