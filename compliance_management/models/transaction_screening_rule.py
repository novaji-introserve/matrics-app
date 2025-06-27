# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


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
    sql_query = fields.Text(string='SQL Query', required=True,
                            help="SQL query returning single value. If query returns a value an exception will be raised on the transaction",tracking=True)
    priority = fields.Integer(
        string='Sequence', help="Order of priority in which screening will be evaluated", required=True, default=1)
    state = fields.Selection(string='State', selection=[('draft', 'Draft'), (
        'active', 'Active'), ('inactive', 'Inactive')], default='draft', index=True,tracking=True)
    narration = fields.Text(string='Narration')
    likely_fraud = fields.Boolean(string='Likely Fraud',tracking=True,default=False)
    risk_level = fields.Selection(string='Risk Level', selection=[('low', 'Low'), ('medium', 'Medium'),('high','High')],default='high',tracking=True)

    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    
    def action_activate(self):
        for e in self:
            e.write({'state': 'active'})

    def action_deactivate(self):
        for e in self:
            e.write({'state': 'inactive'})
            