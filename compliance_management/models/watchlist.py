from odoo import _, api, fields, models


class Watchlist(models.Model):
    _name = 'res.partner.watchlist'
    _description = 'Watchlist'

    name = fields.Char(string='')
    customer_id = fields.Many2one(
        comodel_name='res.partner', string='Customer', required=True, index=True)
