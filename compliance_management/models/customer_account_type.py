# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class CustomerAccountType(models.Model):
    _name = 'res.partner.account.type'
    _description = 'Account Type'
    _sql_constraints = [
        ('uniq_partner_account_type_code', 'unique(code)',
         "Account Type code already exists. Value must be unique!"),
        ('uniq_partner_account_type_name', 'unique(name)',
         "Account Type Name already exists. Value must be unique!")
    ]
    _order = "name"

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string='Code', required=True)
    # risk_assessment = fields.Char(string="Risk Assessment")
    risk_assessment = fields.Many2one('res.risk.assessment', string='Risk Assessment',index=True)

    status = fields.Selection(string='Status', selection=[(
        'active', 'Active'), ('inactive', 'Inactive')], default='active',index=True)
    
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
     