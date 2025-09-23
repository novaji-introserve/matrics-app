# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class AccountProduct(models.Model):
    _name = 'res.partner.account.product'
    _description = 'Account Product'
    _sql_constraints = [
        # ('uniq_account_product_name', 'unique(name)',
        #  "Account Product already exists. Value must be unique!"),

        ('uniq_product_id', 'unique(product_id)',
         "Account Product id already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string='Code')
    risk_assessment = fields.Many2one(comodel_name='res.risk.assessment', string='Risk Assessment',index=True)
    product_id = fields.Char(string='Product')
    product_category = fields.Char(string='Product Category')
    description = fields.Char(string='Product Description')
    product_type = fields.Char(string='Product Type')
    customer_product_id = fields.Char(string='Account Product')

    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    