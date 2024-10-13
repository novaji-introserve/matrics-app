# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class RiskAssessmentLine(models.Model):
    _name = 'res.risk.assessment.line'
    _description = 'Risk Assessment Line'
    _sql_constraints = [
        ('uniq_risk_assessment_line_name', 'unique(name)',
         "Risk Assessment Name already exists. Value must be unique!")
    ]
    
    name = fields.Text(string="description", required=True)
    user_id = fields.Many2one(comodel_name='res.users', string='User',required=True,index=True,default=lambda self: self.env.user.id)
    category_id = fields.Many2one(comodel_name='res.risk.category', string='Category')
    risk_assessment_id = fields.Many2one(comodel_name='res.risk.assessment', string='Risk Assessment',ondelete="cascade")
    implication = fields.Text(string='Implication',required=True)
    inherent_risk_score = fields.Float(string='', digits=(10,2),required=True)
    existing_controls = fields.Text(string='Existing Controls',required=True)
    control_effectiveness_score = fields.Float(string='Control Effectiveness Score', digits=(10,2),required=True)
    residual_risk_probability = fields.Float(string='Residual Risk Probability', digits=(10,2))
    residual_risk_impact = fields.Float(string='Residual Risk Impact', digits=(10,2))
    planned_mitigation = fields.Text(string='Planned Mitigation',required=True)
    department_id = fields.Many2one(comodel_name='hr.department', string='Department Responsible')
    implementation_date = fields.Date(string='Implementation Deadline')
    residual_risk_score = fields.Float(string='Residual Risk Score', digits=(10,2))
    
