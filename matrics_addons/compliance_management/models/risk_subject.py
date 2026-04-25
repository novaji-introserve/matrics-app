# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class RiskSubject(models.Model):
    _name = 'res.risk.subject'
    _description = 'Risk Subject'
    _sql_constraints = [
        ('uniq_risk_subject_code', 'unique(code)',
         "Risk Subject code already exists. Value must be unique!"),
        ('uniq_risk_subject_name', 'unique(name)',
         "Risk Subject Name already exists. Value must be unique!")
    ]
    _order = "name"
    name = fields.Char(string="Name", required=True)
    code = fields.Char(string='Code', required=True)
    universe_id = fields.Many2one(comodel_name='res.risk.universe', string='Risk Universe')
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    