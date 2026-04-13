import logging
from odoo import models, api, fields
from datetime import timedelta

_logger = logging.getLogger(__name__)

class EscalationEngine(models.Model):
    _name = 'fsdh.escalation.engine'
    _description = 'Escalation Engine — Cron Runner'

    @api.model
    def run_escalation(self, force=False):
        """
        Called hourly by cron, or manually from Odoo shell / external script.

        Args:
            force (bool): If True, skip the TAT time check and escalate ALL
                          pending alerts immediately. Use this for testing.
        """
        open_alerts = self.env['alert.history'].search([
            ('status', '=', 'pending review'),
            ('escalation_matrix_id', '!=', False),
            ('escalation_complete', '=', False),
        ])
        _logger.info(f"Escalation Engine: checking {len(open_alerts)} open alerts. force={force}")

        for alert in open_alerts:
            matrix = alert.escalation_matrix_id
            next_level = alert.current_escalation_step + 1

            # Get the next step
            next_step = matrix.step_ids.filtered(
                lambda s: s.sequence == next_level
            )
            if not next_step:
                # No more steps — escalation exhausted
                alert.escalation_complete = True
                _logger.info(f"Alert {alert.alert_id}: escalation complete.")
                continue

            now = fields.Datetime.now()

            if not force:
                # Normal mode: check TAT before escalating
                reference_dt = alert.last_escalation_dt or alert.date_created
                if not reference_dt:
                    _logger.warning(f"Alert {alert.alert_id}: no reference datetime, skipping.")
                    continue

                tat = next_step.tat_id
                if tat.name == 'hours':
                    delta = timedelta(hours=tat.escalation_cycle)
                elif tat.name == 'days':
                    delta = timedelta(days=tat.escalation_cycle)
                elif tat.name == 'weeks':
                    delta = timedelta(weeks=tat.escalation_cycle)
                elif tat.name == 'months':
                    delta = timedelta(days=tat.escalation_cycle * 30)
                elif tat.name == 'years':
                    delta = timedelta(days=tat.escalation_cycle * 365)
                else:
                    continue

                if now < reference_dt + delta:
                    _logger.info(
                        f"Alert {alert.alert_id}: TAT not exceeded yet, skipping."
                    )
                    continue

            # TAT exceeded (or force=True) — escalate!
            self._send_escalation_email(alert, next_step)
            alert.write({
                'current_escalation_step': next_level,
                'last_escalation_dt': now,
            })
            _logger.info(
                f"Alert {alert.alert_id}: escalated to Level {next_level} "
                f"→ {next_step.recipient_email}"
            )

    def _send_escalation_email(self, alert, step):
        """Send escalation notification email."""
        template = self.env.ref(
            'fsdh_addons.email_template_escalation', raise_if_not_found=False
        )
        if template:
            template.with_context(step=step).send_mail(alert.id, force_send=True)
        else:
            # Fallback: plain email via mail.mail
            self.env['mail.mail'].create({
                'subject': f"[ESCALATION Level {step.sequence}] Alert {alert.alert_id}",
                'email_to': step.recipient_email,
                'body_html': (
                    f"<p>Alert <strong>{alert.alert_id}</strong> has not been resolved.<br/>"
                    f"This is a <strong>Level {step.sequence}</strong> escalation notification.<br/>"
                    f"Please review the alert immediately.</p>"
                ),
            }).send()