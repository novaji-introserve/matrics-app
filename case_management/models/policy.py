from odoo import models, fields

class Policy(models.Model):
    _name = 'policy'
    _description = 'Policy'

    name = fields.Char(string='Name', required=False)
    first_owner_id = fields.Char(string='First Owner', required=False)
    second_owner_id = fields.Char(string='Second Owner', required=False)
    risk_rating_id = fields.Char(string='Risk Rating', required=False)
    notification_required = fields.Char(string='Notification Required', required=False)
    review_frequency = fields.Char(string='Review Frequency', required=False)
    document_id = fields.Char(string='Document ID', required=False)
    status = fields.Char(string='Status', required=False)
    description = fields.Char(string='Description', required=False)
    created_at = fields.Datetime(string='Created At', required=False)

    updated_at = fields.Datetime(string='Updated At', required=False)
    
    user_id = fields.Many2one('res.users', string='User', required=False)
