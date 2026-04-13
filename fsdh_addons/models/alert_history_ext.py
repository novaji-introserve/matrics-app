from odoo import models, fields, api


class AlertHistoryEscalation(models.Model):
    _inherit = 'alert.history'

    # Patch: override from Char → Datetime so the escalation engine
    # can do arithmetic without string parsing.
    # Odoo will ALTER the DB column from VARCHAR to TIMESTAMP on upgrade.
    date_created = fields.Datetime(string='Date Created')

    escalation_matrix_id = fields.Many2one(
        'fsdh.escalation.matrix',
        string='Escalation Matrix',
        help="Matrix governing escalation for this alert.",
    )
    current_escalation_step = fields.Integer(
        string='Current Escalation Level',
        default=0,
        help="0 = initial state, 1 = Level 1 escalated, etc.",
    )
    last_escalation_dt = fields.Datetime(
        string='Last Escalation Time',
        help="Timestamp of the last escalation action.",
    )
    escalation_complete = fields.Boolean(
        string='Escalation Complete',
        default=False,
        help="True when all escalation levels have been exhausted.",
    )

    @api.model_create_multi
    def create(self, vals_list):
        """Auto-copy escalation_matrix_id from the triggering alert.rules record."""
        for vals in vals_list:
            if vals.get('escalation_matrix_id'):
                continue  # already explicitly set — respect it

            ref = vals.get('ref_id', '')
            if isinstance(ref, str) and ref.startswith('alert.rules,'):
                try:
                    rule_id = int(ref.split(',')[1])
                    rule = self.env['alert.rules'].browse(rule_id)
                    if rule.exists() and rule.escalation_matrix_id:
                        vals['escalation_matrix_id'] = rule.escalation_matrix_id.id
                        if not vals.get('last_escalation_dt'):
                            vals['last_escalation_dt'] = fields.Datetime.now()
                except (ValueError, IndexError):
                    pass

        return super().create(vals_list)
