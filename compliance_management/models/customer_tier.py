# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class CustomerTier(models.Model):
    _name = 'res.partner.tier'
    _description = 'Customer Tier'
    _sql_constraints = [
        ('uniq_customer_tier_code', 'unique(code)',
         "Tier code already exists. Code must be unique!"),
    ]
    
    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    risk_assessment = fields.Char(string="Risk Assessment")
    status = fields.Selection(string='Status', selection=[(
        'active', 'Active'), ('inactive', 'Inactive')], default='active',index=True)
