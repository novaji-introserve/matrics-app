from odoo import models, fields

class AlertFrequency(models.Model):
    _name = 'alert.frequency'
    _description = 'Alert Frequency'

    name = fields.Char(string='Name', required=False)
    status = fields.Char(string='Status', required=False)
    sending_priority = fields.Char(string='Sending Priority', required=False)
    review_life_cycle = fields.Char(string='Review Life Cycle', required=False)
    
    created_at = fields.Datetime(string='Created At', required=False)
    updated_at = fields.Datetime(string='Updated At', required=False)
    
    user_id = fields.Many2one('res.users', string='User', required=False)
