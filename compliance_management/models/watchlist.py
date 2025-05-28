from odoo import _, api, fields, models


class Watchlist(models.Model):
    _name = 'res.partner.watchlist'
    _description = 'Watchlist'
    _sql_constraints = [

        ('watchlist_id', 'unique(watchlist_id)',
         "Watch List ID already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name")
    watchlist_id = fields.Char(string="Watchlist ID")
    nationality = fields.Char(string="Nationality")
    surname = fields.Char(string="Surname", tracking=True, index=True)
    first_name = fields.Char(
        string="First Name", tracking=True, required=False, index=True)
    middle_name = fields.Char(string="Middle Name")
    customer_id = fields.Many2one(
        comodel_name='res.partner', string='Customer', required=False, index=True)
    bvn = fields.Char(string='BVN', index=True)
    source = fields.Char(string='Source', tracking=True, index=True)


    def init(self):
        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS res_partner_watchlist_id_idx ON res_partner_watchlist (id)")