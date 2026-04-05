from odoo import models, fields, api

DEFAULT_CURRENCY = 'NGN'
ACCOUNT_STATUS_SELECTION = [
    ('active', 'Active'),
    ('inactive', 'Inactive'),
    ('frozen', 'Frozen'),
]


class BankInstitution(models.Model):
    _name = 'bank.institution'
    _description = 'Bank Institution'
    _rec_name = 'name'

    name = fields.Char(string='Institution Name', required=True)
    inst_code = fields.Char(string='Institution Code')
    swift_code = fields.Char(string='SWIFT Code')
    non_bank_institution = fields.Boolean(
        string='Non-Bank Institution', default=False
    )


class BankAccountType(models.Model):
    _name = 'bank.account.type'
    _description = 'Bank Account Type'
    _rec_name = 'name'

    name = fields.Char(string='Account Type', required=True)


class BankAccount(models.Model):
    _name = 'bank.account'
    _description = 'Bank Account'
    _rec_name = 'account_name'

    # Account Information
    account_number = fields.Char(string='Account Number', required=True)
    account_name = fields.Char(string='Account Name', required=True)
    name = fields.Char(string='Account Name', related='account_number')
    currency_id = fields.Many2one(
        'res.currency',
        string='Currency',
        default=lambda self: self.env['res.currency'].search([('name', '=', DEFAULT_CURRENCY)], limit=1),
        required=True,
    )
    currency_code = fields.Char(string='Currency Code', default=DEFAULT_CURRENCY, index=True)
    balance = fields.Float(string='Balance', digits=(16, 2))

    # Bank Information
    institution_id = fields.Many2one(
        'bank.institution', string='Institution', required=True
    )
    institution_name = fields.Char(string='Institution Name', required=True)
    institution_code = fields.Char(string='Institution Code')
    swift_code = fields.Char(string='SWIFT Code')
    branch = fields.Char(string='Branch')
    non_bank_institution = fields.Boolean(string='Non-Bank Institution', default=False)

    # Account Details
    client_number = fields.Char(string='Client Number')
    personal_account_type = fields.Many2one(
        'bank.account.type', string='Personal Account Type'
    )
    opened = fields.Datetime(string='Date Opened')
    status_code = fields.Selection(
        ACCOUNT_STATUS_SELECTION,
        string='Status Code',
        default='active',
        index=True,
    )
    beneficiary = fields.Char(string='Beneficiary')

    # Related Records
    signatory_ids = fields.One2many(
        'account.signatory', 'account_id', string='Signatories')

    # Transactions
    outgoing_transactions = fields.One2many(
        'bank.transaction', 'from_account_id', string='Outgoing Transactions')
    incoming_transactions = fields.One2many(
        'bank.transaction', 'to_account_id', string='Incoming Transactions')

    @api.onchange('institution_id')
    def _onchange_institution_id(self):
        for record in self:
            record._sync_institution_fields()

    @api.onchange('currency_id')
    def _onchange_currency_id(self):
        for record in self:
            record._sync_currency_fields()

    def _sync_institution_fields(self):
        for record in self:
            if not record.institution_id:
                continue
            record.institution_name = record.institution_id.name
            record.institution_code = record.institution_id.inst_code
            record.swift_code = record.institution_id.swift_code
            record.non_bank_institution = record.institution_id.non_bank_institution

    def _sync_currency_fields(self):
        for record in self:
            if not record.currency_id:
                continue
            record.currency_code = record.currency_id.name

    @api.model
    def _prepare_synced_vals(self, vals):
        prepared_vals = dict(vals)

        institution_id = prepared_vals.get('institution_id')
        if institution_id:
            institution = self.env['bank.institution'].browse(institution_id)
            if institution.exists():
                prepared_vals.update({
                    'institution_name': institution.name,
                    'institution_code': institution.inst_code,
                    'swift_code': institution.swift_code,
                    'non_bank_institution': institution.non_bank_institution,
                })

        currency_id = prepared_vals.get('currency_id')
        if currency_id:
            currency = self.env['res.currency'].browse(currency_id)
            if currency.exists():
                prepared_vals['currency_code'] = currency.name

        return prepared_vals

    @api.model_create_multi
    def create(self, vals_list):
        prepared_vals_list = [self._prepare_synced_vals(vals) for vals in vals_list]
        records = super().create(prepared_vals_list)
        records._sync_institution_fields()
        records._sync_currency_fields()
        return records

    def write(self, vals):
        result = super().write(self._prepare_synced_vals(vals))
        if 'institution_id' in vals:
            self._sync_institution_fields()
        if 'currency_id' in vals:
            self._sync_currency_fields()
        return result

class AccountSignatory(models.Model):
    _name = 'account.signatory'
    _description = 'Account Signatory'

    account_id = fields.Many2one('bank.account', string='Account', required=True, ondelete='cascade')
    person_id = fields.Many2one('bank.person', string='Person', required=True)
    is_primary = fields.Boolean(string='Is Primary Signatory', default=False)
