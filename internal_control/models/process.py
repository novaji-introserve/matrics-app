from odoo import models, fields, api

class Process(models.Model):
    _name = 'process'
    _description = "process for exception management"

   
    name = fields.Char(string="Name", required=True)
    description = fields.Char(string="Description", required=True)


   