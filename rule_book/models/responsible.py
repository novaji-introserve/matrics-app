# Responsible Model
from odoo import models, fields, api

class Responsible(models.Model):
    _name = 'rulebook.responsible'
    _description = 'Responsible'
    _rec_name = 'name'
    
    name = fields.Char(string='Name', required=True, tracking=True)
    email = fields.Char(string='Email', required=True, tracking=True)
    cc = fields.Char(string='CC (Email List)', tracking=True)
    bc = fields.Char(string='BC (Email List)', tracking=True)