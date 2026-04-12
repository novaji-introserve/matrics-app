# models/internal_control_screening_rule.py
from odoo import models

class InternalControlScreeningRule(models.Model):
    _inherit = 'res.transaction.screening.rule'  # Inherit the original model

    def screen_transactions(self):
        """Delegate cron-driven screening to the shared transaction flow."""
        transactions = self.env['res.customer.transaction'].search([
            ('state', '=', 'new'),
            ('rule_id', '=', False),
        ])
        for transaction in transactions:
            transaction.action_screen()
        return True
