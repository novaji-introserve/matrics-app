# -*- coding: utf-8 -*-
from odoo import _, api, fields, models


class TransactionScreeningRule(models.Model):
    _inherit = 'res.transaction.screening.rule'
    blocked = fields.Boolean(string='Blocked', default=False,
                             help="Indicates if the transaction should be blocked by this rule.", tracking=True, index=True)
