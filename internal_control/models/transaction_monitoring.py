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



    # def _get_customer_account_domain(self):
    #     """Return domain for customer accounts (start with 0, length 10)"""
    #     return [('account_id.name', '=like', '0[1-9]________')]  # Start with 0, exactly 10 digits

    # Methods for Customer Transactions - All Transactions
    # @api.model
    # def open_customers_all_transactions_today_ngn(self):
    #     today = datetime.now().strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '=', 121),
    #         ('status', '=', 1)  
    #     ]
        
    #     return {
    #         'name': _('Customer Transactions NGN - Today'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new',
    #         }
    #     }


    # def _get_customer_account_domain(self):
    #     """Return domain for customer accounts (length 10)"""
    #     return [
    #         ('account_id.name', '!=', False),  # Ensure it's not empty
    #         ('account_id.name', '!=', ''),     # Ensure it's not an empty string
    #         ('account_id.name', 'ilike', '__________')  # 10 characters (10 underscores)
    #     ]

    # @api.model
    # def open_customers_all_transactions_today_ngn(self):
    #     today = datetime.now().strftime('%Y-%m-%d')  # Get today's date in YYYY-MM-DD format

    #     # Create the domain
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('currency_id', '=', 121)                   # Filter by NGN currency
    #     ] # Include account domain

    #     # Print the domain for debugging
    #     print("Domain:", domain)

    #     # Fetch the records based on the domain
    #     transactions = self.env['res.customer.transaction'].search(domain)

    #     # Print the results
    #     print(f"Number of transactions found: {len(transactions)}")
    #     print(f"Transactions: {[transaction.name for transaction in transactions]}")  # Adjust 'name' to the field you want to show

    #     return {
    #         'name': _('Customer Transactions NGN - Today'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new',
    #         }
    #     }

    # @api.model
    # def open_customers_all_transactions_today_ngn(self):
    #     today = datetime.now().strftime('%Y-%m-%d')

    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '=', 121)  # NGN currency
    #     ]

    #     # Fetch the records based on the domain
    #     transactions = self.env['res.customer.transaction'].search(domain)

    #     # Print the results
    #     print(f"Number of transactions found: {len(transactions)}")
    #     print(f"Transactions: {[transaction.name for transaction in transactions]}")  # Adjust 'name' to the field you want to show

    #     return {
    #         'name': _('Customer Transactions NGN - Today'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new',
    #         }
    #     }

    # @api.model
    # def open_customers_all_transactions_today_other(self):
    #     today = datetime.now().strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '!=', 121)
    #     ]
        
    #     return {
    #         'name': _('Customer Transactions Other Currencies - Today'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new',
    #         }
    #     }

    # @api.model
    # def open_customers_all_transactions_7days_ngn(self):
    #     today = datetime.now().strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '=', 121)
    #     ]
        
    #     return {
    #         'name': _('Customer Transactions NGN - Last 7 Days'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new',
    #         }
    #     }

    # @api.model
    # def open_customers_all_transactions_7days_other(self):
    #     today = datetime.now().strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '!=', 121)
    #     ]
        
    #     return {
    #         'name': _('Customer Transactions Other Currencies - Last 7 Days'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new',
    #         }
    #     }
    
    #     # Methods for Customer Transactions - Awaiting Review
    # @api.model
    # def open_customers_awaiting_review_today_ngn(self):
    #     today = datetime.now().strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'new'),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '=', 121)
    #     ]
        
    #     return {
    #         'name': _('Customer Transactions To Review NGN - Today'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # @api.model
    # def open_customers_awaiting_review_today_other(self):
    #     today = datetime.now().strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'new'),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '!=', 121)
    #     ]
        
    #     return {
    #         'name': _('Customer Transactions To Review Other Currencies - Today'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # @api.model
    # def open_customers_awaiting_review_7days_ngn(self):
    #     today = datetime.now()
    #     last_7_days = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    #     today_str = today.strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'new'),
    #         ('valuedate', '>=', f"{last_7_days} 00:00:00"),
    #         ('valuedate', '<=', today_str + ' 23:59:59'),
    #         ('currency_id', '=', 121)
    #     ]
        
    #     return {
    #         'name': _('Customer Transactions To Review NGN - Last 7 Days'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # @api.model
    # def open_customers_awaiting_review_7days_other(self):
    #     today = datetime.now()
    #     last_7_days = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    #     today_str = today.strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'new'),
    #         ('valuedate', '>=', f"{last_7_days} 00:00:00"),
    #         ('valuedate', '<=', today_str + ' 23:59:59'),
    #         ('currency_id', '!=', 121)
    #     ]
        
    #     return {
    #         'name': _('Customer Transactions To Review Other Currencies - Last 7 Days'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # # Methods for Customer Transactions - Reviewed
    # @api.model
    # def open_customers_reviewed_today_ngn(self):
    #     today = datetime.now().strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'done'),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '=', 121)
    #     ]
        
    #     return {
    #         'name': _('Customer Transactions Reviewed NGN - Today'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # @api.model
    # def open_customers_reviewed_today_other(self):
    #     today = datetime.now().strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'done'),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '!=', 121)
    #     ]
        
    #     return {
    #         'name': _('Customer Transactions Reviewed Other Currencies - Today'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # @api.model
    # def open_customers_reviewed_7days_ngn(self):
    #     today = datetime.now()
    #     last_7_days = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    #     today_str = today.strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'done'),
    #         ('valuedate', '>=', f"{last_7_days} 00:00:00"),
    #         ('valuedate', '<=', today_str + ' 23:59:59'),
    #         ('currency_id', '=', 121)
    #     ]
        
    #     return {
    #         'name': _('Customer Transactions Reviewed NGN - Last 7 Days'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # @api.model
    # def open_customers_reviewed_7days_other(self):
    #     today = datetime.now()
    #     last_7_days = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    #     today_str = today.strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'done'),
    #         ('valuedate', '>=', f"{last_7_days} 00:00:00"),
    #         ('valuedate', '<=', today_str + ' 23:59:59'),
    #         ('currency_id', '!=', 121)
    #     ]
        
    #     return {
    #         'name': _('Customer Transactions Reviewed Other Currencies - Last 7 Days'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # # Methods for Internal Transactions - All Transactions
    # @api.model
    # def open_internal_all_transactions_today_ngn(self):
    #     today = datetime.now().strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '=', 121),
    #         ('tran_type', '=', 15)
    #     ]
        
    #     return {
    #         'name': _('Internal Transactions NGN - Today'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new',
    #         }
    #     }

    # @api.model
    # def open_internal_all_transactions_today_other(self):
    #     today = datetime.now().strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '!=', 121),
    #         ('tran_type', '=', 15)
    #     ]
        
    #     return {
    #         'name': _('Internal Transactions Other Currencies - Today'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new',
    #         }
    #     }

    # @api.model
    # def open_internal_all_transactions_7days_ngn(self):
    #     today = datetime.now()
    #     last_7_days = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    #     today_str = today.strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('valuedate', '>=', f"{last_7_days} 00:00:00"),
    #         ('valuedate', '<=', today_str + ' 23:59:59'),
    #         ('currency_id', '=', 121),
    #         ('tran_type', '=', 15)
    #     ]
        
    #     return {
    #         'name': _('Internal Transactions NGN - Last 7 Days'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new',
    #         }
    #     }

    # @api.model
    # def open_internal_all_transactions_7days_other(self):
    #     today = datetime.now()
    #     last_7_days = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    #     today_str = today.strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('valuedate', '>=', f"{last_7_days} 00:00:00"),
    #         ('valuedate', '<=', today_str + ' 23:59:59'),
    #         ('currency_id', '!=', 121),
    #         ('tran_type', '=', 15)
    #     ]
        
    #     return {
    #         'name': _('Internal Transactions Other Currencies - Last 7 Days'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new',
    #         }
    #     }

    # # Methods for Internal Transactions - Awaiting Review
    # @api.model
    # def open_internal_awaiting_review_today_ngn(self):
    #     today = datetime.now().strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'new'),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '=', 121),
    #         ('tran_type', '=', 15)
    #     ]
        
    #     return {
    #         'name': _('Internal Transactions To Review NGN - Today'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # @api.model
    # def open_internal_awaiting_review_today_other(self):
    #     today = datetime.now().strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'new'),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '!=', 121),
    #         ('tran_type', '=', 15)
    #     ]
        
    #     return {
    #         'name': _('Internal Transactions To Review Other Currencies - Today'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # @api.model
    # def open_internal_awaiting_review_7days_ngn(self):
    #     today = datetime.now()
    #     last_7_days = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    #     today_str = today.strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'new'),
    #         ('valuedate', '>=', f"{last_7_days} 00:00:00"),
    #         ('valuedate', '<=', today_str + ' 23:59:59'),
    #         ('currency_id', '=', 121),
    #         ('tran_type', '=', 15)
    #     ]
        
    #     return {
    #         'name': _('Internal Transactions To Review NGN - Last 7 Days'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # @api.model
    # def open_internal_awaiting_review_7days_other(self):
    #     today = datetime.now()
    #     last_7_days = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    #     today_str = today.strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'new'),
    #         ('valuedate', '>=', f"{last_7_days} 00:00:00"),
    #         ('valuedate', '<=', today_str + ' 23:59:59'),
    #         ('currency_id', '!=', 121),
    #         ('tran_type', '=', 15)
    #     ]
        
    #     return {
    #         'name': _('Internal Transactions To Review Other Currencies - Last 7 Days'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # # Methods for Internal Transactions - Reviewed
    # @api.model
    # def open_internal_reviewed_today_ngn(self):
    #     today = datetime.now().strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'done'),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '=', 121),
    #         ('tran_type', '=', 15)
    #     ]
        
    #     return {
    #         'name': _('Internal Transactions Reviewed NGN - Today'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # @api.model
    # def open_internal_reviewed_today_other(self):
    #     today = datetime.now().strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'done'),
    #         ('valuedate', '=', f"{today} 00:00:00"),
    #         ('currency_id', '!=', 121),
    #         ('tran_type', '=', 15)
    #     ]
        
    #     return {
    #         'name': _('Internal Transactions Reviewed Other Currencies - Today'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # @api.model
    # def open_internal_reviewed_7days_ngn(self):
    #     today = datetime.now()
    #     last_7_days = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    #     today_str = today.strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'done'),
    #         ('valuedate', '>=', f"{last_7_days} 00:00:00"),
    #         ('valuedate', '<=', today_str + ' 23:59:59'),
    #         ('currency_id', '=', 121),
    #         ('tran_type', '=', 15)
    #     ]
        
    #     return {
    #         'name': _('Internal Transactions Reviewed NGN - Last 7 Days'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }

    # @api.model
    # def open_internal_reviewed_7days_other(self):
    #     today = datetime.now()
    #     last_7_days = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    #     today_str = today.strftime('%Y-%m-%d')
        
    #     domain = [
    #         ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
    #         ('state', '=', 'done'),
    #         ('valuedate', '>=', f"{last_7_days} 00:00:00"),
    #         ('valuedate', '<=', today_str + ' 23:59:59'),
    #         ('currency_id', '!=', 121),
    #         ('tran_type', '=', 15)
    #     ]
        
    #     return {
    #         'name': _('Internal Transactions Reviewed Other Currencies - Last 7 Days'),
    #         'type': 'ir.actions.act_window',
    #         'res_model': 'res.customer.transaction',
    #         'limit': 50,
    #         'view_mode': 'tree,form',
    #         'domain': domain,
    #         'context': {
    #             'search_default_group_branch': 1,
    #             'search_default_group_currency': 1,
    #             'default_state': 'new'
    #         }
    #     }



    # Methods for All Transactions
    @api.model
    def open_all_transactions_today(self):
        today = datetime.now().strftime('%Y-%m-%d')
        
        domain = [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('valuedate', '=', f"{today} 00:00:00"),
                ('currency_id', '=', 121)  # NGN currency
            ]
        
        # chk_if_empty = self.env['res.customer.transaction'].search(domain)
        

    

        # if len(chk_if_empty) == 0:
        #     raise ValidationError("No record found")
        # else:
        return {
        'name': _('All Transactions - Today'),
        'type': 'ir.actions.act_window',
        'res_model': 'res.customer.transaction',
        'limit': 50,
        'view_mode': 'tree,form',
        'domain': domain,
        'context': {
            'search_default_group_branch': 1,  # Automatically apply "Group by Branch"
            'search_default_group_currency': 1,  # Automatically apply "Group by Currency"
            'default_state': 'new',
            'group_by': 'branch_id',
        }
    }

    @api.model
    def open_all_transactions_last_7_days(self):
        today = datetime.now()
        last_7_days = (today - timedelta(days=7)).strftime('%Y-%m-%d')
        today_str = today.strftime('%Y-%m-%d')
        
        domain = [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('valuedate', '>=', f"{last_7_days} 00:00:00"),
                ('valuedate', '<=', today_str + ' 23:59:59'),
                ('currency_id', '=', 121)  # NGN currency
            ]

        # chk_if_empty = self.env['res.customer.transaction'].search(domain)
        
        # if len(chk_if_empty) == 0:
        #     raise ValidationError("No record found")
        # else:
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
        today = datetime.now().strftime('%Y-%m-%d')
        
        domain = [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('state', '=', 'new'),
                ('valuedate', '=', f"{today} 00:00:000"),
                ('currency_id', '=', 121)  # NGN currency
            ]
        
        # chk_if_empty = self.env['res.customer.transaction'].search(domain)
        
        # if len(chk_if_empty) == 0:
        #     raise ValidationError("No record found")
        # else:
        
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
        today = datetime.now()
        last_7_days = (today - timedelta(days=7)).strftime('%Y-%m-%d')
        today_str = today.strftime('%Y-%m-%d')
        
        domain = [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('state', '=', 'new'),
                ('valuedate', '>=', f"{last_7_days} 00:00:00"),
                ('valuedate', '<=', today_str + ' 23:59:59'),
                ('currency_id', '=', 121)  # NGN currency
            ]
        
        # chk_if_empty = self.env['res.customer.transaction'].search(domain)
        
        # if len(chk_if_empty) == 0:
        #     raise ValidationError("No record found")
        # else:
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
        today = datetime.now().strftime('%Y-%m-%d')
        domain = [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('state', '=', 'done'),
                ('valuedate', '=', f"{today} 00:00:00"),
                ('currency_id', '=', 121)  # NGN currency
            ]
        
        # chk_if_empty = self.env['res.customer.transaction'].search(domain)
        
        # if len(chk_if_empty) == 0:
        #     raise ValidationError("No record found")
        # else:
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
        today = datetime.now()
        last_7_days = (today - timedelta(days=7)).strftime('%Y-%m-%d')
        today_str = today.strftime('%Y-%m-%d')
        
        domain = [
                ('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]),
                ('state', '=', 'done'),
                ('valuedate', '>=', f"{last_7_days} 00:00:00"),
                ('valuedate', '<=', today_str + ' 23:59:59'),
                ('currency_id', '=', 121)  # NGN currency
            ]

        # chk_if_empty = self.env['res.customer.transaction'].search(domain)
        
        # if len(chk_if_empty) == 0:
        #     raise ValidationError("No record found")
        # else:
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
                     