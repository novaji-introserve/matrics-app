# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class BankProduct(models.Model):
    _name = 'res.bank.product'
    _description = 'Bank Product'
    _sql_constraints = [
        ('uniq_code', 'unique(code)',
         "Product already exists. Value must be unique!"),
    ]

    
    name = fields.Char(string="name", index=True)
    code = fields.Char(string="Code", index=True, unique=True)
    shortname = fields.Char(string="Short Name", index=True)
    productclass = fields.Char(string="Product Class", index=True)
    producttype_id = fields.Many2one(comodel_name='res.partner.account.product',
                              string='Product Type', index=True)
