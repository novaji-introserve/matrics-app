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

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string='Code', required=True)
    risk_assessment = fields.Char(string="Risk Assessment")
    status = fields.Selection(string='Status', selection=[(
        'active', 'Active'), ('inactive', 'Inactive')], default='active',index=True)
