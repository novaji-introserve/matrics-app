# -*- coding: utf-8 -*-

from odoo import models, fields, api


class EducationLevel(models.Model):
    _name = 'res.education.level'
    _description = 'Education Level'
    _sql_constraints = [
        ('uniq_edu_level_code', 'unique(code)',
         "Education level code already exists. Code must be unique!"),
    ]
    _order = "name"
    name = fields.Char(string="Education Level", required=True)
    code = fields.Char(string="Code", required=True)
    risk_assessment = fields.Many2one(comodel_name='res.risk.assessment', string='Risk Assessment',index=True)
    status = fields.Selection(string='Status', selection=[(
        'active', 'Active'), ('inactive', 'Inactive')], default='active',index=True)
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    