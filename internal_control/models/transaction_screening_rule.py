# internal_control/models/transaction_screening_rule.py
from odoo import models, fields, api

class TransactionScreeningRuleInternalControl(models.Model):
    _name = 'transaction.screening.rule.internal.control'
    _inherit = 'res.transaction.screening.rule'  # Inherit from compliance_management
    _description = 'Transaction Screening Rule for Internal Control'

    # Add additional fields or customizations specific to internal_control

    @api.model
    def create(self, vals):
        # Optionally customize the behavior for internal_control
        return super(TransactionScreeningRuleInternalControl, self).create(vals)
