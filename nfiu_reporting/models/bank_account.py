from odoo import models, fields, api


class BankAccount(models.Model):
    _name = 'bank.account'
    _description = 'Bank Account'
    _rec_name = 'account_name'

    # Account Information
    account_number = fields.Char(string='Account Number', required=True)
    account_name = fields.Char(string='Account Name', required=True)
    name = fields.Char(string='Account Name', related='account_number')
    currency_code = fields.Char(string='Currency Code', default='NGN')
    balance = fields.Float(string='Balance', digits=(16, 2))

    # Bank Information
    institution_name = fields.Char(string='Institution Name', required=True)
    institution_code = fields.Char(string='Institution Code')
    swift_code = fields.Char(string='SWIFT Code')
    branch = fields.Char(string='Branch')
    non_bank_institution = fields.Boolean(
        string='Non-Bank Institution', default=False)

    # Account Details
    client_number = fields.Char(string='Client Number')
    personal_account_type = fields.Char(string='Personal Account Type')
    opened = fields.Datetime(string='Date Opened')
    status_code = fields.Char(string='Status Code',index=True)
    beneficiary = fields.Char(string='Beneficiary')

    # Related Records
    signatory_ids = fields.One2many(
        'account.signatory', 'account_id', string='Signatories')

    # Transactions
    outgoing_transactions = fields.One2many(
        'bank.transaction', 'from_account_id', string='Outgoing Transactions')
    incoming_transactions = fields.One2many(
        'bank.transaction', 'to_account_id', string='Incoming Transactions')

class AccountSignatory(models.Model):
    _name = 'account.signatory'
    _description = 'Account Signatory'

    account_id = fields.Many2one('bank.account', string='Account', required=True, ondelete='cascade')
    person_id = fields.Many2one('bank.person', string='Person', required=True)
    is_primary = fields.Boolean(string='Is Primary Signatory', default=False)
