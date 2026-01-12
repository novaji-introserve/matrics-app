# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class AccountStatus(models.Model):
    _name = 'res.partner.account.status'
    _description = 'Account Status'
    _sql_constraints = [
        ('uniq_code', 'unique(code)',
         "Status already exists. Value must be unique!"),
    ]

    name = fields.Char(string="Account Status", readonly=True, index=True)
    code = fields.Char(string="Status Code", readonly=True, index=True, unique=True)
