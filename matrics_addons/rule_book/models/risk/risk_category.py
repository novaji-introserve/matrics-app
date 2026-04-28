from odoo import models, fields, api

# Risk Category Model

class RiskCategory(models.Model):
    _name = 'rulebook.risk_category'
    _description = 'Risk Category'
    _rec_name = 'name'
    
    name = fields.Char(string='Name', required=True, tracking=True)
    risk_priority = fields.Selection([
        ('low', 'Low'),
        ('medium', 'Medium'),
        ('critical', 'Critical')
    ], string='Risk Priority', required=True, tracking=True)
    risk_score = fields.Integer(string='Risk Score', required=True, tracking=True)