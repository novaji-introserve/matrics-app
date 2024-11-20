from odoo import models, fields, api

class InternalControl(models.Model):
    _name = 'res.internal_control'
    _description = 'Internal Control'

    name = fields.Char(string="Name", required=True)
    description = fields.Text(string="Description")