# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class RiskCategory(models.Model):
    _name = 'res.risk.category'
    _description = 'Risk Category'
    _sql_constraints = [
        ('uniq_risk_category_code', 'unique(code)',
         "Risk Category code already exists. Value must be unique!"),
        ('uniq_risk_category_name', 'unique(name)',
         "Risk Category Name already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string='Code', required=True)
    user_id = fields.Many2one(comodel_name='res.users', string='User',
                              required=True, index=True, default=lambda self: self.env.user.id)
