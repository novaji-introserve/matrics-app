from odoo import models, fields, api


class CaseManagerEscalation(models.Model):
    _inherit = 'case.manager'

    escalation_matrix_id = fields.Many2one(
        'fsdh.escalation.matrix',
        string='Escalation Matrix',
        tracking=True,
        help="Escalation matrix governing how unresolved cases are escalated.",
    )

    def _save_alert_to_history(self, email_result, rendered_html, title, mail_values):
        """
        Extend the base method to inject escalation_matrix_id into every
        alert.history record created by this case, so the escalation engine
        can pick it up automatically.
        """
        if not email_result:
            return super()._save_alert_to_history(email_result, rendered_html, title, mail_values)

        mail = self.env['mail.mail'].browse(email_result)
        if mail.state != 'sent':
            return super()._save_alert_to_history(email_result, rendered_html, title, mail_values)

        # Call base — it creates the alert.history record
        super()._save_alert_to_history(email_result, rendered_html, title, mail_values)

        # If this case has a matrix, stamp it on the just-created history record
        if self.escalation_matrix_id:
            history = self.env['alert.history'].search([
                ('ref_id', '=', f'{self._name},{self.id}'),
            ], order='id desc', limit=1)
            if history and not history.escalation_matrix_id:
                history.write({
                    'escalation_matrix_id': self.escalation_matrix_id.id,
                    'last_escalation_dt': fields.Datetime.now(),
                })
