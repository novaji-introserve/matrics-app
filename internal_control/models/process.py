from odoo import models, fields, api

class Process(models.Model):
    _name = 'process'
    _description = "process for exception management"
    _inherit = ['mail.thread', 'mail.activity.mixin']
   
    name = fields.Char(string="Name", required=True)
    description = fields.Char(string="Description", required=True)
    process_category_id = fields.Many2one('process.category', string="Process Category")
   

   