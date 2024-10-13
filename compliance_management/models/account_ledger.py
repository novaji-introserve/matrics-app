# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class AccountLedger(models.Model):
    _name = 'res.partner.account.ledger'
    _description = 'Account Ledger'
    _sql_constraints = [
        ('uniq_account_ledger_name', 'unique(name)',
         "Account Ledger already exists. Value must be unique!"),

        ('uniq_account_ledger_code', 'unique(code)',
         "Account Ledger code already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string='Code', required=True)
    user_id = fields.Many2one(comodel_name='res.users', string='User',
                              required=True, index=True, default=lambda self: self.env.user.id)
