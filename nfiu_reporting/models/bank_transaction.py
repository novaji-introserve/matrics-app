from odoo import models, fields, api

TRANSACTION_TYPE_SELECTION = [
    ('debit', 'Debit'),
    ('credit', 'Credit'),
]


class BankTransaction(models.Model):
    _name = 'bank.transaction'
    _description = 'Bank Transaction'
    _rec_name = 'transaction_number'

    # Transaction Details
    transaction_number = fields.Char(
        string='Transaction Number', required=True, index=True)
    name = fields.Char(string='Transaction Name',
                       required=True, related='transaction_number')
    internal_ref_number = fields.Char(
        string='Internal Reference Number', index=True)
    transaction_location = fields.Char(string='Transaction Location')
    transaction_description = fields.Text(string='Transaction Description')
    date_transaction = fields.Datetime(
        string='Transaction Date', required=True)
    value_date = fields.Datetime(string='Value Date')

    # System Information
    teller = fields.Char(string='Teller')
    authorized = fields.Char(string='Authorized By')
    late_deposit = fields.Boolean(string='Late Deposit', default=False)
    transmode_code = fields.Char(string='Transaction Mode Code', index=True)
    tran_type = fields.Selection(
        TRANSACTION_TYPE_SELECTION,
        string='Transaction Type',
        index=True,
    )

    # Amount
    amount_local = fields.Float(
        string='Amount (Local Currency)', digits=(16, 2))
    currency_code = fields.Char(
        string='Currency Code', default='NGN', index=True)

    # Related Accounts
    from_account_id = fields.Many2one('bank.account', string='From Account')
    to_account_id = fields.Many2one('bank.account', string='To Account')

    # Additional Fields
    from_funds_code = fields.Char(string='From Funds Code')
    to_funds_code = fields.Char(string='To Funds Code')
    from_country = fields.Char(string='From Country')
    to_country = fields.Char(string='To Country')


# Security and Access Rules
class BankTransactionSecurity(models.Model):
    _inherit = 'bank.transaction'

    @api.model
    def create(self, vals):
        # Add any custom validation logic here
        return super().create(vals)

    def write(self, vals):
        # Add any custom validation logic here
        return super().write(vals)


# Add views configuration (optional)
class BankTransactionViews(models.Model):
    _inherit = 'bank.transaction'

    def action_view_from_account(self):
        return {
            'type': 'ir.actions.act_window',
            'name': 'From Account',
            'res_model': 'bank.account',
            'res_id': self.from_account_id.id,
            'view_mode': 'form',
            'target': 'current',
        }

    def action_view_to_account(self):
        return {
            'type': 'ir.actions.act_window',
            'name': 'To Account',
            'res_model': 'bank.account',
            'res_id': self.to_account_id.id,
            'view_mode': 'form',
            'target': 'current',
        }
