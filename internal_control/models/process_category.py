from odoo import models, fields, api

class Process_category(models.Model):
    _name = 'process.category'
    _description = 'process Category for exception management'

    name = fields.Char(string="Name", required=True)
    description = fields.Char(string="description", required=True)
<<<<<<< HEAD
    date_created = fields.Datetime(string="created_at", default=fields.Datetime.now())
    process_category_id = fields.Many2one('process.category', string="Process Category")
=======
>>>>>>> main

  