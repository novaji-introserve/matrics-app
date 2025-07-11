# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class CustomerRegion(models.Model):
    _name = 'res.partner.region'
    _description = 'Customer Region'
    _sql_constraints = [
        ('uniq_customer_region_code', 'unique(name)',
         "Customer region code already exists. Value must be unique!"),
    ]
    _order = "name"
    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    risk_assessment = fields.Many2one('res.risk.assessment', string='Risk Assessment')
    status = fields.Selection(string='Status', selection=[(
        'active', 'Active'), ('inactive', 'Inactive')], default='active',index=True)
    risk_rating = fields.Float(string='Risk Score', digits=(10,2),related='risk_assessment.risk_rating')
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    