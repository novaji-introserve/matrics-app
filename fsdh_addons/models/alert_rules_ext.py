# -*- coding: utf-8 -*-
from odoo import models, fields, api


class AlertRulesEscalation(models.Model):
    _inherit = 'alert.rules'

    # Computed from the alert group — read-only, no manual editing needed
    escalation_matrix_id = fields.Many2one(
        'fsdh.escalation.matrix',
        string='Escalation Matrix',
        compute='_compute_escalation_matrix',
        readonly=True,
        help="Automatically pulled from the Alert Group's default escalation matrix.",
    )

    @api.depends('alert_id', 'alert_id.escalation_matrix_id')
    def _compute_escalation_matrix(self):
        for rule in self:
            rule.escalation_matrix_id = rule.alert_id.escalation_matrix_id if rule.alert_id else False

    def action_test_escalation_matrix(self):
        """
        TEST BUTTON — forces immediate escalation for all pending alerts
        belonging to this rule's escalation matrix, bypassing the TAT check.
        Comment out the button in the view to disable once testing is done.
        """
        self.ensure_one()
        engine = self.env['fsdh.escalation.engine']

        # Count open alerts before so we can report how many were escalated
        domain = [
            ('status', '=', 'pending review'),
            ('escalation_matrix_id', '=', self.escalation_matrix_id.id),
            ('escalation_complete', '=', False),
        ]
        alerts_before = self.env['alert.history'].search_count(domain)

        engine.run_escalation(force=True)

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Escalation Test Complete',
                'message': (
                    f"Force-escalated {alerts_before} pending alert(s) "
                    f"linked to matrix: {self.escalation_matrix_id.display_name or 'N/A'}."
                ),
                'type': 'success',
                'sticky': False,
            },
        }

