from odoo import models, fields, api

class Process_category(models.Model):
    _name = 'process.category'
    _description = 'process Category for exception management'

    name = fields.Char(string="Name", required=True)
    description = fields.Char(string="description", required=True)

  