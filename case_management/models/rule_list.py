from odoo import models, fields

class RuleList(models.Model):
    _name = 'rule.list'
    _description = 'Rule List'

    name = fields.Char(string='Name', required=False)
    description = fields.Char(string='Description', required=False)
    status = fields.Char(string='Status', required=False)
    source_id = fields.Char(string='Source ID', required=False)
    
    created_at = fields.Datetime(string='Created At', required=False)
    updated_at = fields.Datetime(string='Updated At', required=False)
    
    user_id = fields.Many2one('res.users', string='User', required=False)
