# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class State(models.Model):
    _inherit = 'res.partner.account.product'
    
    productclass = fields.Char(string="Product Class", index=True)
