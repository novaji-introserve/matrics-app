# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class RiskAssessmentPlan(models.Model):
    _name = 'res.compliance.risk.assessment.plan'
    _description = 'Risk Analysis'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _sql_constraints = [
        ('uniq_stats_code', 'unique(code)',
         "Plan code already exists. Value must be unique!"),
        ('uniq_stats_name', 'unique(name)',
         "Plan Name already exists. Value must be unique!")
    ]
    _order = 'priority asc'

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    sql_query = fields.Text(string='SQL Query', required=True,
                            help="SQL query returning single value")
    priority = fields.Integer(
        string='Sequence', help="Order of priority in which plan will be evaluated", required=True, default=1)
    state = fields.Selection(string='State', selection=[('draft', 'Draft'), (
        'active', 'Active'), ('inactive', 'Inactive')], default='draft', index=True)
    narration = fields.Text(string='Narration')
    risk_score = fields.Integer(string='Risk Score', default=1)
    compute_score_from = fields.Selection(string='Compute Risk Score From', selection=[(
        'dynamic', 'Dynamically From Return Value'), ('static', 'Static From Risk Rating')], default='static', index=True,required=True)

    def action_activate(self):
        for e in self:
            e.write({'state': 'active'})

    def action_deactivate(self):
        for e in self:
            e.write({'state': 'inactive'})
    
