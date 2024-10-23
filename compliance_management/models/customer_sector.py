# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class CustomerSector(models.Model):
    _name = 'res.partner.sector'
    _description = 'Customer Sector'
    _sql_constraints = [
        ('uniq_sector_code', 'unique(code)',
         "Sector code already exists. Code must be unique!"),
    ]
    
    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    risk_assessment = fields.Char(string="Risk Assessment")
    status = fields.Selection(string='Status', selection=[(
        'active', 'Active'), ('inactive', 'Inactive')], default='active',index=True)
