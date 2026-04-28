# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class MaritalStatus(models.Model):
    _name = 'res.marital.status'
    _description = 'Marital Status'
    _sql_constraints = [
        ('uniq_marital_status_name', 'unique(name)',
         "Marital Status already exists. Value must be unique!"),
    ]

    name = fields.Char(string="Name", required=True)
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    