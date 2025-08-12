# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
from odoo.exceptions import UserError, ValidationError
from odoo.tools.safe_eval import safe_eval


class TransactionScreeningRule(models.Model):
    _name = 'res.transaction.screening.rule'
    _description = 'Transaction Screening Rule'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _sql_constraints = [
        ('uniq_tran_screening_code', 'unique(code)',
         "Code already exists. Value must be unique!"),
        ('uniq_tran_screening_name', 'unique(name)',
         "Name already exists. Value must be unique!")
    ]
    _order = 'priority asc'

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    condition_select = fields.Selection([
        ('sql', 'SQL Query Returning Single Value'),
        ('python', 'Python Expression')
    ], string="Condition Based on", default='sql', required=True,help="Select the type of condition to apply for this rule. SQL query will return a single value, while Python expression will evaluate a condition based on the transaction object.")
    condition_python = fields.Text(string='Python Expression',
        default='''
                    # Available variables:
                    #----------------------
                    # transaction: object containing the transaction
                    # rule: object containing the rule
                    # env: environment  object
                    
                    # Note: returned value have to be set in the variable 'result'

                    result = transaction.amount > 1000''',
                    help='Applied this rule for calculation if condition is true. You can specify condition like transaction.amount > 1000.')
    sql_query = fields.Text(string='SQL Query',
                            help="SQL query returning single value. If query returns a value an exception will be raised on the transaction",tracking=True)
    priority = fields.Integer(
        string='Sequence', help="Order of priority in which screening will be evaluated", required=True, default=1)
    state = fields.Selection(string='State', selection=[('draft', 'Draft'), (
        'active', 'Active'), ('inactive', 'Inactive')], default='draft', index=True,tracking=True)
    narration = fields.Text(string='Narration')
    likely_fraud = fields.Boolean(string='Likely Fraud',tracking=True,default=False)
    risk_level = fields.Selection(string='Risk Level', selection=[('low', 'Low'), ('medium', 'Medium'),('high','High')],default='high',tracking=True)

    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    
    
    @api.model
    def create(self, vals):
        condition_select  = vals.get('condition_select')
        if condition_select == 'sql':
            sql_query = vals.get('sql_query')
            if not sql_query:
                raise ValidationError(_("SQL Query is required for SQL condition type."))
        elif condition_select == 'python':
            python_code = vals.get('condition_python')
            if not python_code:
                raise ValidationError(_("Python Expression is required for Python condition type."))
        return super(TransactionScreeningRule, self).create(vals)
    
    def write(self, vals):
        condition_select = vals.get('condition_select', self.condition_select)
        if condition_select == 'sql':
            sql_query = vals.get('sql_query', self.sql_query)
            if not sql_query:
                raise ValidationError(_("SQL Query is required for SQL condition type."))
        elif condition_select == 'python':
            python_code = vals.get('condition_python', self.condition_python)
            if not python_code:
                raise ValidationError(_("Python Expression is required for Python condition type."))
        return super(TransactionScreeningRule, self).write(vals)

    def action_activate(self):
        for e in self:
            e.write({'state': 'active'})

    def action_deactivate(self):
        for e in self:
            e.write({'state': 'inactive'})
            
    def _satisfy_condition(self, localdict):
        """
        This method is used to compute the rule based on the local dictionary.
        It evaluates the condition_python or executes the sql_query based on the condition_select.
        
        # Available variables in localdict:
        #----------------------
        # transaction: object containing the transaction<br />
        # customer: object containing the customer
        # branch: object containing the branch
        # account: object containing the account
        # currency: object containing the currency
        # env: environment  object
        #----------------------
        # Note: returned value have to be set in the variable 'result'
        """
        self.ensure_one()
        try:
            safe_eval(self.condition_python, localdict, mode='exec', nocopy=True)
            print(localdict)
            if 'result' in localdict and localdict['result'] is not None:
                return localdict['result']
            return False
        except:
            raise UserError(_('Wrong python code defined for transaction screening rule %s (%s).') % (self.name, self.code))
            