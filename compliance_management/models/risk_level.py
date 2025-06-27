# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class RiskLevel(models.Model):
    _name = 'res.risk.level'
    _description = 'Risk Level'
    _sql_constraints = [
        ('uniq_risk_level_code', 'unique(code)',
         "Risk level code already exists. Value must be unique!"),
    ]

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string='Code', required=True)
    user_id = fields.Many2one(comodel_name='res.users', string='User',
                              required=True, index=True, default=lambda self: self.env.user.id)
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    