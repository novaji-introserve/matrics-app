from odoo import fields, models, api, _


class MaritalStatus(models.Model):
    _inherit = 'res.marital.status'

    code= fields.Char(string='Status Code',  readonly=True, index=True, unique=True)