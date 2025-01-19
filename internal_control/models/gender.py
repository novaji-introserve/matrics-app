from odoo import fields, models, api, _


class Gender(models.Model):
    _inherit = 'res.partner.gender'

    code= fields.Char(string='Gender Code',  readonly=True, index=True, unique=True)