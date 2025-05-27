# models/account_agg_risk_score.py

from odoo import models, fields

class AccountAggRiskScore(models.Model):
    _name = "account.agg.risk.score"
    _description = "Account Aggregate Risk Score"

    branch_id = fields.Many2one("res.branch", string="Branch")
    product_id = fields.Many2one("res.partner.account.product", string="Product")
    currency_id = fields.Many2one("res.currency", string="Currency")
    account_type_id = fields.Many2one("res.partner.account.type", string="Account Type")
    state = fields.Selection([
        ('Active', 'Active'),
        ('Inactive', 'Inactive'),
        ('Dormant', 'Dormant'),
        ('Flagged', 'Flagged'),
        ('Closed', 'Closed')
    ], string="Status")

    weighted_avg_risk_score = fields.Float(string="Weighted Avg Risk Score")
    total_accounts = fields.Integer(string="Total Accounts")
    high_count = fields.Integer(string="High Risk Count")
    medium_count = fields.Integer(string="Medium Risk Count")
    low_count = fields.Integer(string="Low Risk Count")