# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class CustomerSector(models.Model):
    _name = 'res.partner.sector'
    _description = 'Customer Sector'
    _sql_constraints = [
        ('uniq_sector_code', 'unique(code)',
         "Sector code already exists. Code must be unique!"),
    ]
    _order = "name"
    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    # risk_assessment = fields.Char(string="Risk Assessment")
    risk_assessment = fields.Many2one(comodel_name='res.risk.assessment', string='Risk Assessment',index=True)
    status = fields.Selection(string='Status', selection=[(
        'active', 'Active'), ('inactive', 'Inactive')], default='active',index=True)

class CustomerIndustry(models.Model):
    _name = 'customer.industry'
    _description = 'Customer Industry'
    _sql_constraints = [
        ('uniq_industry_code', 'unique(code)',
         "Customer industry already exists. Code must be unique!"),
    ]
    _order = "name"
    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    



   
