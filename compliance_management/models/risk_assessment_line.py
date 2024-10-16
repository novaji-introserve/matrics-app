# -*- coding: utf-8 -*-

from odoo import models, fields, api, _

CONTROL_EFFECTIVENESS_MAX_SCORE = 25


class RiskAssessmentLine(models.Model):
    _name = 'res.risk.assessment.line'
    _description = 'Risk Assessment Line'
    _sql_constraints = [
        ('uniq_risk_assessment_line_name', 'unique(name)',
         "Risk Assessment Name already exists. Value must be unique!")
    ]
    name = fields.Text(string="Description", required=True)
    category_id = fields.Many2one(
        comodel_name='res.risk.category', string='Category', required=True)
    risk_assessment_id = fields.Many2one(
        comodel_name='res.risk.assessment', string='Risk Assessment', ondelete="cascade")
    implication = fields.Text(string='Implication', required=True)
    inherent_risk_score = fields.Integer(
        string='Inherent Risk Score', required=True, tracking=True)
    existing_controls = fields.Text(string='Existing Controls', required=True)
    control_effectiveness_score = fields.Integer(
        string='Control Effectiveness Score', required=True, tracking=True)
    residual_risk_probability = fields.Float(
        string='Residual Risk Probability', digits=(10, 2), required=True, tracking=True)
    residual_risk_impact = fields.Integer(
        string='Residual Risk Impact', required=True, tracking=True)
    planned_mitigation = fields.Text(
        string='Planned Mitigation', required=True)
    department_id = fields.Many2one(
        comodel_name='hr.department', string='Department', required=True, help="Department Responsible")
    implementation_date = fields.Date(
        string='Implementation Deadline', help="Recurring deadline for implementation")
    residual_risk_score = fields.Float(
        string='Residual Risk Score', digits=(10, 2), required=True)

    @api.onchange('inherent_risk_score')
    def _onchange_inherent_risk_score(self):
        # self.compute_residual_risk_probability()
        self.residual_risk_probability = (
            1 - (self.control_effectiveness_score /25)) * 100
    '''
    @api.depends('inherent_risk_score','control_effectiveness_score','residual_risk_impact')
    def _compute_risk_scores(self):
        self.residual_risk_probability = (1 - (self.control_effectiveness_score / CONTROL_EFFECTIVENESS_MAX_SCORE)) * 100

    def compute_residual_risk_probability(self):
        self.residual_risk_probability = (
            1 - (self.control_effectiveness_score / CONTROL_EFFECTIVENESS_MAX_SCORE)) * 100
    '''
