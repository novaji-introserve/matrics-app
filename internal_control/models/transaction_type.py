from odoo import fields, models, api, _


class TransactionType(models.Model):
    _name = 'res.transaction.type'
    _description = _('Transaction Type')
    _sql_constraints = [
        ('uniq_trancode', 'unique(trancode)',
         "Transaction type already exists. Value must be unique!"),
    ]
    trancode = fields.Char(string='Transaction Code',  readonly=True, index=True, unique=True)
    tranname = fields.Char(string='Transaction Name',  readonly=True, index=True)
    trantype = fields.Char(string='Transaction Type',  readonly=True, index=True)
    transhortname = fields.Char(string='Transaction Shortname',  readonly=True, index=True)
