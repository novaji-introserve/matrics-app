from datetime import timedelta

from odoo import _, fields, models


class TransactionScreeningHistory(models.Model):
    _inherit = 'res.transaction.screening.history'

    transaction_date = fields.Datetime(
        string='Transaction Date',
        related='transaction_id.date_created',
        store=True,
        readonly=True,
    )
    currency_id = fields.Many2one(
        'res.currency',
        string='Currency',
        related='transaction_id.currency_id',
        store=True,
        readonly=True,
    )

    def open_transaction_screening_history(self):
        action = super().open_transaction_screening_history()
        today = fields.Date.today()
        tomorrow = today + timedelta(days=1)
        action['domain'] = list(action.get('domain', [])) + [
            ('transaction_date', '>=', fields.Datetime.to_string(today)),
            ('transaction_date', '<', fields.Datetime.to_string(tomorrow)),
        ]
        context = dict(action.get('context', {}))
        context.update({
            'search_default_filter_today': 1,
            'search_default_group_branch_id': 1,
            'search_default_group_currency_id': 1,
        })
        action['context'] = context
        return action
