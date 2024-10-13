# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class RiskUniverse(models.Model):
    _name = 'res.risk.universe'
    _description = 'Risk Universe'
    _sql_constraints = [
        ('uniq_risk_universe_code', 'unique(code)',
         "Risk Universe code already exists. Value must be unique!"),
        ('uniq_risk_universe_name', 'unique(name)',
         "Risk Universe Name already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string='Code', required=True)
    user_id = fields.Many2one(comodel_name='res.users', string='User',
                              required=True, index=True, default=lambda self: self.env.user.id)
