# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class Gender(models.Model):
    _name = 'res.partner.gender'
    _description = 'Gender'
    _sql_constraints = [
        ('uniq_gender_name', 'unique(name)',
         "Gender already exists. Value must be unique!"),
    ]

    name = fields.Char(string="Name", required=True)
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    