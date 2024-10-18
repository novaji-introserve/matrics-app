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
    inherent_risk_score = fields.Float(
        string='Inherent Risk Score', required=True, tracking=True)
    existing_controls = fields.Text(string='Existing Controls', required=True)
    control_effectiveness_score = fields.Float(
        string='Control Effectiveness Score', required=True, tracking=True)
    residual_risk_probability = fields.Float(
        string='Residual Risk Probability', required=True, compute='_compute_risk_probability', tracking=True, store=True)
    residual_risk_impact = fields.Float(
        string='Residual Risk Impact', required=True, tracking=True)
    planned_mitigation = fields.Text(
        string='Planned Mitigation', required=True)
    department_id = fields.Many2one(
        comodel_name='hr.department', string='Department', required=True, help="Department Responsible")
    implementation_date = fields.Date(
        string='Implementation Deadline', help="Recurring deadline for implementation")
    residual_risk_score = fields.Float(
        string='Residual Risk Score', required=True, compute='_compute_risk_score', store=True, tracking=True)

    @api.depends('inherent_risk_score', 'control_effectiveness_score', 'residual_risk_impact')
    def _compute_risk_score(self):
        for record in self:
            probability = self._compute_risk_probability(
                record.control_effectiveness_score)
            score = self._compute_residual_risk_score(
                probability, record.residual_risk_impact)
            record.residual_risk_probability = probability
            record.residual_risk_score = score

    @api.depends('inherent_risk_score', 'control_effectiveness_score', 'residual_risk_impact')
    def _compute_risk_probability(self):
        for record in self:
            probability = self._compute_risk_probability(
                record.control_effectiveness_score)
            score = self._compute_residual_risk_score(
                probability, record.residual_risk_impact)
            record.residual_risk_probability = probability
            record.residual_risk_score = score

    def _compute_risk_probability(self, control_effectiveness_score):
        return (
            1 - (control_effectiveness_score / CONTROL_EFFECTIVENESS_MAX_SCORE)) * 100

    def _compute_residual_risk_score(self, probability, impact):
        return (probability/100) * impact
