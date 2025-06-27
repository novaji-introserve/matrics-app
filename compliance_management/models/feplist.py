from odoo import _, api, fields, models


class FEPlist(models.Model):
    _name = 'res.partner.fep'
    _description = 'FEPlist'

    name = fields.Char(string='')
    customer_id = fields.Many2one(
        comodel_name='res.partner', string='Customer', required=True, index=True)
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    