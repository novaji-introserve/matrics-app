# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class AccountProduct(models.Model):
    _name = 'res.partner.account.product'
    _description = 'Account Product'
    _sql_constraints = [
        ('uniq_account_product_name', 'unique(name)',
         "Account Product already exists. Value must be unique!"),

        ('uniq_account_product_code', 'unique(code)',
         "Account Product code already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string='Code', required=True)
