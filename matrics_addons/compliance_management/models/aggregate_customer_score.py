from odoo import models, fields, api

class AggregateCustomerRiskScore(models.Model):
    _name = 'customer.agg.risk.score'
    _description = 'Customer Aggregate Risk Score'

    branch_id = fields.Many2one(
        comodel_name='res.branch', string='Branch', index=True
    )
    weighted_avg_risk_score = fields.Float(
        string='Weighted Average Risk Score', digits=(10, 2), store=True
    )
    total_customers = fields.Integer(
        string='Total Customers', store=True
    )
    formatted_name = fields.Char(
        string='Formatted Name', store=True
    )
    
    