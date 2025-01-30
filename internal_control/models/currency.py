# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class Currency(models.Model):
    _inherit = 'res.currency'
    code = fields.Char(string="code", index=True)
