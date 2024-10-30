# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class CustomerAccount(models.Model):
    _name = 'res.partner.account'
    _description = 'Account'
    _sql_constraints = [
        ('uniq_account_name', 'unique(name)',
         "Account Name already exists. Value must be unique!"),
    ]

    name = fields.Char(string="Name", required=True)
    account_number = fields.Char(string='Account Number', index=True)
    currency_id = fields.Many2one(
        comodel_name='res.currency', string='Currency', index=True)
    product_id = fields.Many2one(
        comodel_name='res.partner.account.product', string='Product',index=True)
    date_created = fields.Date(string='Date Created', index=True)
    ledger_id = fields.Many2one(comodel_name='res.partner.account.ledger', string='Ledger',index=True)
    closure_status = fields.Selection(string='Closure Status', selection=[('N', 'No'), ('Y', 'Yes')])
    customer_id = fields.Many2one(comodel_name='res.partner', string='Customer',index=True)
    branch_id = fields.Many2one(comodel_name='res.branch', string='Branch',index=True)
    balance = fields.Float(string='Balance', digits=(15,4))
    account_type_id = fields.Many2one(comodel_name='res.partner.account.type', string='Account Type',required=True,index=True)
    risk_assessment = fields.Many2one(comodel_name='res.risk.assessment', string='Risk Assessment',index=True)
