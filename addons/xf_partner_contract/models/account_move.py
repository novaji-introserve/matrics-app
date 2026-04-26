# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
from odoo.exceptions import ValidationError


class AccountMove(models.Model):
    _inherit = 'account.move'

    # Fields
    use_contract = fields.Selection(
        string='Use Contract',
        related='company_id.use_contract',
        readonly=True,
    )
    contract_id = fields.Many2one(
        string='Contract',
        comodel_name='xf.partner.contract',
        ondelete='restrict',
        domain=[('state', '=', 'running')],
        states={'posted': [('readonly', True)], 'cancel': [('readonly', True)]},
    )

    # Compute and search fields, in the same order of fields declaration
    # Constraints and onchanges
    # Built-in methods overrides
    # Action methods

    def apply_contract(self):
        for move in self:
            if not move.contract_id:
                continue
            invoice_vals = move.contract_id._prepare_invoice(move.move_type)
            move.write(invoice_vals)
            move.apply_contract_lines()

    def apply_contract_lines(self):
        for move in self:
            if not move.contract_id:
                continue
            invoice_lines = []
            for line in move.contract_id.line_ids:
                invoice_lines.append((0, 0, line._prepare_account_move_line(move)))
            move.write({'invoice_line_ids': invoice_lines})

    # Business methods
