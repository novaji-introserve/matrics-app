from odoo import models, fields, api, _
import logging
from odoo.exceptions import ValidationError
from datetime import datetime, timedelta


_logger = logging.getLogger(__name__)

class TransactionMonitoring(models.Model):
    _inherit = 'res.customer.transaction'
    _sql_constraints = [
        ('uniq_refno', 'unique(refno)',
         "Transaction already exists. Value must be unique!"),
    ] 

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
    tran_channel = fields.Char(string="Transaction Channel", readonly=True, index=True)
    request_id = fields.Char(string="Request ID", readonly=True, index=True)
    trans_id = fields.Char(string="Transaction ID", readonly=True, index=True)




  # Methods for All Transactions
    @api.model
    def open_all_transactions_today(self):
        today = fields.Date.today()
        
       
        
        # Get NGN currency ID dynamically
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('date_created', '=', today),
                ('currency_id', '=', ngn_currency.id)
            ]
        
        return {
            'name': _('All Transactions - Today'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'limit': 50,
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new',
                'group_by': 'branch_id',
            }
        }

    @api.model
    def open_all_transactions_last_7_days(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        # Get NGN currency ID dynamically
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('date_created', '>=', last_7_days),
                ('date_created', '<=', today),
                ('currency_id', '=', ngn_currency.id)
            ]

        return {
            'name': _('All Transactions - Last 7 Days'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'limit': 50,
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }

    # Methods for Awaiting Review
    @api.model
    def open_awaiting_review_today(self):
        today = fields.Date.today()
        
        # Get NGN currency ID dynamically
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('state', '=', 'new'),
                ('date_created', '=', today),
                ('currency_id', '=', ngn_currency.id)
            ]
        
        return {
            'name': _('Transactions To Review - Today'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'limit': 50,
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }

    @api.model
    def open_awaiting_review_last_7_days(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        # Get NGN currency ID dynamically
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('state', '=', 'new'),
                ('date_created', '>=', last_7_days),
                ('date_created', '<=', today),
                ('currency_id', '=', ngn_currency.id)
            ]
        
        return {
            'name': _('Transactions To Review - Last 7 Days'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'limit': 50,
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }

    # Methods for Reviewed
    @api.model
    def open_reviewed_today(self):
        today = fields.Date.today()
        
        # Get NGN currency ID dynamically
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('state', '=', 'done'),
                ('date_created', '=', today),
                ('currency_id', '=', ngn_currency.id)
            ]
        
        return {
            'name': _('Transactions Reviewed - Today'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'limit': 50,
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {
                'search_default_group_branch': 1,
                'search_default_group_currency': 1,
                'default_state': 'new'
            }
        }

    @api.model 
    def open_reviewed_last_7_days(self):
        today = fields.Date.today()
        last_7_days = today - timedelta(days=7)
        
        # Get NGN currency ID dynamically
        ngn_currency = self.env['res.currency'].search([('code', '=', '001')], limit=1)
        if not ngn_currency:
            ngn_currency = self.env['res.currency'].search([('name', '=', 'NGN')], limit=1)
        
        domain = [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('state', '=', 'done'),
                ('date_created', '>=', last_7_days),
                ('date_created', '<=', today),
                ('currency_id', '=', ngn_currency.id)
            ]

        return {
            'name': _('Transactions Reviewed - Last 7 Days'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'limit': 50,
            'view_mode': 'tree,form',
            'domain': domain,
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


    def action_screen(self):
     
        rules = self.env['res.transaction.screening.rule'].search(
            [('state', '=', 'active')], order='priority')

        if rules:
            for rule in rules:
                # try:
                    query = rule.sql_query
                    char_to_replace = {'#AMOUNT#': f"{self.amount}",
                                    '#ACCOUNT_ID#': f"{self.account_id.id}",
                                    "#CUSTOMER_ID#": f"{self.customer_id.id}",
                                    "#TRAN_DATE#": f"{self.date_created}",
                                    "#BRANCH_ID#": f"{self.branch_id.id}",
                                    "#CURRENCY_ID#": f"{self.currency_id.id}"}
                    # Iterate over all key-value pairs in dictionary
                    for key, value in char_to_replace.items():
                        # Replace key character with value character in string
                        query = query.replace(key, value)
                        
                    self.env.cr.execute(query)
                   
                    records = self.env.cr.fetchall()
                    
                    
                    for rec in records: 

                        record = self.env['res.customer.transaction'].browse(rec[0])  # rec[0] contains the ID of the record
    
                        # Make sure the record exists and then update it
                        if record.exists() and not record.rule_id:
                            record.write({
                                'rule_id': rule.id,  # Assuming 'rule' is a record
                                'risk_level': rule.risk_level,
                            })
                            print(f"Record {record.id}: rule_id updated to {rule.id}, risk_level updated to {rule.risk_level}, rule name: {rule.name}")
                     