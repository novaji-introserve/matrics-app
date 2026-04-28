# -*- coding: utf-8 -*-

from odoo import fields, models


class ResLedgerType(models.Model):
    _name = 'res.ledger.type'
    _description = 'Ledger Type'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Name', required=True, index=True, tracking=True)
