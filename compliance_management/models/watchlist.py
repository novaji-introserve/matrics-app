from odoo import _, api, fields, models


class Watchlist(models.Model):
    _name = 'res.partner.watchlist'
    _description = 'Watchlist'
    _sql_constraints = [

        # ('watchlist_id', 'unique(watchlist_id)',
        #  "Watch List ID already exists. Value must be unique!"),
        ('bvn', 'unique(bvn)',
         "bvn already exists. Value must be unique!")
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
        """Initialize database index when module is installed/updated"""
        
        # 1. First, check if the index exists
        self.env.cr.execute("""
            SELECT 1 
            FROM pg_indexes 
            WHERE indexname = 'res_partner_watchlist_id_idx'
        """)
        
        # 2. fetchone() will be None if the index doesn't exist
        index_exists = self.env.cr.fetchone()

        # 3. Only create the index if it doesn't exist
        if not index_exists:
            self.env.cr.execute(
                "CREATE INDEX res_partner_watchlist_id_idx ON res_partner_watchlist (id)"
            )
        
        