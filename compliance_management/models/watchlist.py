from odoo import _, api, fields, models


class Watchlist(models.Model):
    _name = 'res.partner.watchlist'
    _description = 'Watchlist'



    name = fields.Char(string="Name")
    watchlist_id = fields.Char(string="Watchlist ID")
    nationality = fields.Char(string="Nationality")
    surname = fields.Char(string="Surname",tracking=True,required=True,index=True)
    first_name = fields.Char(string="First Name",tracking=True,required=True,index=True)
    middle_name = fields.Char(string="Middle Name")
    customer_id = fields.Many2one(
        comodel_name='res.partner', string='Customer', required=False, index=True)
    bvn = fields.Char(string='BVN', tracking=True, index = True)