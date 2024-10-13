# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class RiskType(models.Model):
    _name = 'res.risk.type'
    _description = 'Risk Type'
    _sql_constraints = [
        ('uniq_risk_type_code', 'unique(code)',
         "Risk Type code already exists. Value must be unique!"),
        ('uniq_risk_type_name', 'unique(name)',
         "Risk Type Name already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string='Code', required=True)
    user_id = fields.Many2one(comodel_name='res.users', string='User',
                              required=True, index=True, default=lambda self: self.env.user.id)
