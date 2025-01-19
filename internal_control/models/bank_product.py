# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class State(models.Model):
    _name = 'res.bank.product'
    _description = 'Bank Product'

    
    name = fields.Char(string="name", index=True)
    code = fields.Char(string="Code", index=True, unique=True)
    shortname = fields.Char(string="Short Name", index=True)
    producttype_id = fields.Many2one(comodel_name='res.partner.account.product',
                              string='Product Type', index=True)
