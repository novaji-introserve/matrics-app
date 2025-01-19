from odoo import fields, models, api, _


class CustomerType(models.Model):
    _name = 'res.customer.type'
    _description = _('Customer Type')
    _inherit = ['mail.thread', 'mail.activity.mixin']

    code = fields.Char(string='Transaction Code',  readonly=True, index=True, unique=True)
    name = fields.Char(string='Transaction Name',  readonly=True, index=True)
