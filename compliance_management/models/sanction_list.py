from odoo import _, api, fields, models


class Watchlist(models.Model):
    _name = 'sanction.list'
    _description = 'Sanction List'
    _sql_constraints = [

        ('sanction_id', 'unique(sanction_id)',
         "Sanction List ID already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name")
    sanction_id = fields.Char(string="Sanction ID")
    nationality = fields.Char(string="Nationality")
    surname = fields.Char(string="Surname", tracking=True, index=True)
    first_name = fields.Char(
        string="First Name", tracking=True, required=False, index=True)
    middle_name = fields.Char(string="Middle Name")
    
    source = fields.Char(string='Source', tracking=True, index=True)

    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    
    def init(self):
        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS sanction_list_id_idx ON sanction_list (id)")
        