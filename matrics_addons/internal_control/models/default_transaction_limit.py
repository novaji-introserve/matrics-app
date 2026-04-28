from odoo import fields, models, _


class DefaultTransactionLimit(models.Model):
    _name = 'res.default.transaction.limit'
    _description = _('Default Transaction Limits')

    single_transaction = fields.Float(string='Single Transaction', digits=(20,2),  index=True)
    daily_transaction = fields.Float(string='Daily Transaction', digits=(20,2),  index=True)
    single_bills_transaction = fields.Float(string='Single Bills Transaction', digits=(20,2),  index=True)
    daily_bills_transaction = fields.Float(string='Daily Bills Transaction', digits=(20,2),  index=True)
    single_airtime_transaction = fields.Float(string='Single Airtime Transaction', digits=(20,2),  index=True)
    daily_airtime_transaction = fields.Float(string='Daily Airtime Transaction', digits=(20,2),  index=True)
    account_tier = fields.Many2one(comodel_name='res.partner.tier', string='Account Tier', digits=(20,2),  index=True, unique=True)
