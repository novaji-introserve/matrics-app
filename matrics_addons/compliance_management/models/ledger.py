# -*- coding: utf-8 -*-

from odoo import fields, models


class ResLedger(models.Model):
    _name = 'res.ledger'
    _description = 'Ledger'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Name', required=True)
    ledger_code = fields.Char(string='Ledger Code', required=True, index=True, tracking=True)
    ledger_type_id = fields.Many2one(
        'res.ledger.type',
        string='Ledger Type',
        required=True,
        index=True,
        tracking=True,
    )
