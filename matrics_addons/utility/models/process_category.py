# models/process_category.py
from odoo import models, fields, api

class ProcessCategory(models.Model):
    _name = 'process.category'
    _description = 'Process Category'

    name = fields.Char(string="Category Name", required=True)
    description = fields.Text(string="Description")
