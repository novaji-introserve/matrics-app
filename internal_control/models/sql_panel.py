from odoo import _, api, fields, models


class SQLPanel(models.Model):
    _inherit = 'psql.query'
    _description = 'SQL Panel'
