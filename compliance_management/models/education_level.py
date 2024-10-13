# -*- coding: utf-8 -*-

from odoo import models, fields, api


class EducationLevel(models.Model):
    _name = 'res.education.level'
    _description = 'Education Level'
    _sql_constraints = [
        ('uniq_edu_level_code', 'unique(code)',
         "Education level code already exists. Code must be unique!"),
    ]

    name = fields.Char(string="Education Level", required=True)
    code = fields.Char(string="Code", required=True)
