from odoo import models, fields

class AlertGroup(models.Model):
    _name = 'alert.group'
    _description = 'Alert Group'

    name = fields.Char(string='Name', required=False)
    created_at = fields.Datetime(string='Created At', required=False)
    email = fields.Char(string='Email', required=False)
    email_cc = fields.Char(string='Email CC', required=False)
    tag = fields.Char(string='Tag', required=False)
    
    state = fields.Selection([
        ('active', 'Active'),
        ('inactive', 'Inactive')
    ], string='State', required=False)
