# -*- coding: utf-8 -*-

from odoo import models, fields, api


class CustomerTier(models.Model):
    _name = 'res.partner.tier'
    _description = 'Customer Tier'
    _sql_constraints = [
        ('uniq_customer_tier_code', 'unique(code)',
         "Tier code already exists. Code must be unique!"),
    ]
    _order = "name"
    
    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True, index=True)
    tier_level = fields.Selection([
        ('1', 'Tier 1'),
        ('2', 'Tier 2'),
        ('3', 'Tier 3')
    ], string="Tier Level", required=True, index=True)
    risk_assessment = fields.Many2one(
        comodel_name='res.risk.assessment', string='Risk Assessment', index=True)
    status = fields.Selection(string='Status', selection=[
        ('active', 'Active'),
        ('inactive', 'Inactive')
    ], default='active', index=True)
    
    @api.model_create_multi
    def create(self, vals_list):
        records = super(CustomerTier, self).create(vals_list)
        return records

    def write(self, vals):
        result = super(CustomerTier, self).write(vals)
        return result
    
