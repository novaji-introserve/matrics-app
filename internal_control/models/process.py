from odoo import models, fields, api

class Process(models.Model):
    _name = 'process'
    _description = "process for exception management"

   
    name = fields.Char(string="Name", required=True)
    description = fields.Char(string="Description", required=True)
<<<<<<< HEAD

=======
    process_category_id = fields.Many2one('process.category', string="Process Category")
    
>>>>>>> main

   