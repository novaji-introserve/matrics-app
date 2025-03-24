from odoo import _, api, fields, models


class Greylist(models.Model):
    _name = 'res.partner.greylist'
    _description = 'Greylist'

    name = fields.Char(string='')
    customer_id = fields.Many2one(
        comodel_name='res.partner', string='Customer', required=True, index=True)
