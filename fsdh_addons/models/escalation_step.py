# -*- coding: utf-8 -*-
from odoo import models, fields, api


class EscalationStep(models.Model):
    _name = 'fsdh.escalation.step'
    _description = 'Escalation Step'
    _order = 'sequence asc'

    matrix_id = fields.Many2one(
        'fsdh.escalation.matrix', string='Matrix',
        required=True, ondelete='cascade',
    )
    sequence = fields.Integer(string='Level', default=1)
    level_label = fields.Char(
        string='Level Label',
        compute='_compute_level_label', store=True,
    )
    # User picker — filtered to members of the matrix's subsidiary group in the view
    recipient_user_id = fields.Many2one(
        'res.users',
        string='Recipient',
        help="Select a user from the subsidiary. Their email will be used for escalation notifications.",
    )
    # Stored email, auto-filled from the selected user
    recipient_email = fields.Char(
        string='Recipient Email',
        compute='_compute_recipient_email',
        store=True,
        readonly=False,
    )
    tat_id = fields.Many2one(
        'fsdh.escalation.period', string='TAT (Time to Escalate)',
        required=True,
        help="How long before this level escalates to the next one.",
    )

    @api.depends('sequence')
    def _compute_level_label(self):
        for step in self:
            step.level_label = f"Level {step.sequence}"

    @api.depends('recipient_user_id')
    def _compute_recipient_email(self):
        for step in self:
            step.recipient_email = step.recipient_user_id.email or step.recipient_email or ''
