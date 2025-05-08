# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
import logging
# CONTROL_EFFECTIVENESS_MAX_SCORE = 25

_logger = logging.getLogger(__name__)


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
    implication = fields.Many2many("risk.assessment.implication","res_risk_assessment_line_implication_rel", tracking=True)
    inherent_risk_score = fields.Float(
        string='Inherent Risk Score', required=True, tracking=True)
    existing_controls = fields.Many2many("risk.assessment.control", "res_risk_assessment_line_risk_assessment_control_rel", tracking=True)
    control_effectiveness_score = fields.Float(
        string='Control Effectiveness Score', tracking=True)
    residual_risk_probability = fields.Float(
        string='Residual Risk Probability', compute='_compute_risk_probability', store=True)
    residual_risk_impact = fields.Float(
        string='Residual Risk Impact', required=True, tracking=True)
    planned_mitigation = fields.Many2many("risk.assessment.mitigation", "res_risk_assessment_line_risk_assessment_mitigation_rel", tracking=True)
    department_id = fields.Many2one(
        comodel_name='hr.department', string='Department', required=True, help="Department Responsible")
    implementation_date = fields.Selection([
        ('0', 'Immediate'),
        ('7', '7 Days'),
        ('14', '14 Days'),
        ('21', '21 Days'),
        ('30', 'A month'),
        ('60', '2 month'),
        ('90', '3 month'),
    ],string='Implementation Deadline',default="0", help="Recurring deadline for implementation")
    
    residual_risk_score = fields.Float(
        string='Residual Risk Score', compute='_compute_risk_score', store=True, tracking=True)
    
    # inherent_max_val = fields.Float(
    # string='Inherent Max', 
    # default=lambda self: self.env['res.fcra.score'].search([], limit=1).inherent_risk_score_max or 0.0
    # )

    # inherent_min_val = fields.Float(
    #     string='Inherent Min', 
    #     default=lambda self: self.env['res.fcra.score'].search([], limit=1).inherent_risk_score_min or 0.0
    # )

    # control_max_val = fields.Float(
    #     string='Control Max', 
    #     default=lambda self: self.env['res.fcra.score'].search([], limit=1).control_effectiveness_score_max or 0.0
    # )

    # control_min_val = fields.Float(
    #     string='Control Min', 
    #     default=lambda self: self.env['res.fcra.score'].search([], limit=1).control_effectiveness_score_min or 0.0
    # )

    # residual_max_val = fields.Float(
    #     string='Residual Max', 
    #     default=lambda self: self.env['res.fcra.score'].search([], limit=1).residual_risk_score_max or 0.0
    # )

    # residual_min_val = fields.Float(
    #     string='Residual Min', 
    #     default=lambda self: self.env['res.fcra.score'].search([], limit=1).residual_risk_score_min or 0.0
    # )





    @api.model
    def create(self, vals):
        record = super(RiskAssessmentLine, self).create(vals)
        record.update_aggregate_risk_score()
        return record

    def write(self, vals):
        record = super(RiskAssessmentLine, self).write(vals)
        self.update_aggregate_risk_score()
        return record

    @api.depends('inherent_risk_score', 'control_effectiveness_score', 'residual_risk_impact','residual_risk_score','residual_risk_probability','residual_risk_score')
    def _compute_risk_score(self):
        for record in self:
            probability = self._compute_risk_probability(
                record.control_effectiveness_score)
            score = self._compute_residual_risk_score(
                probability, record.residual_risk_impact)
            record.residual_risk_probability = probability
            record.residual_risk_score = score

    @api.depends('inherent_risk_score', 'control_effectiveness_score', 'residual_risk_impact','residual_risk_score','residual_risk_probability','residual_risk_score')
    def _compute_risk_probability(self):
        for record in self:
            probability = self._compute_risk_probability(
                record.control_effectiveness_score)
            score = self._compute_residual_risk_score(
                probability, record.residual_risk_impact)
            record.residual_risk_probability = probability
            record.residual_risk_score = score
    
    def get_control_effectiveness_max_score(self):
        """Retrieves the maximum control effectiveness score from system parameters."""
        return int(self.env['ir.config_parameter'].sudo().get_param('risk_management.control_effectiveness_max_score') or 25)

    def _compute_risk_probability(self, control_effectiveness_score):
        max_score = self.get_control_effectiveness_max_score()
        if max_score == 0:
            return 100.0  # Avoid division by zero
        
        return (
            1 - (control_effectiveness_score / max_score)) * 100

    def _compute_residual_risk_score(self, probability, impact):
        return (probability/100) * impact

    def update_aggregate_risk_score(self):
        risk_assessment_id = self.risk_assessment_id.id
        self.env.cr.execute('update res_risk_assessment set risk_rating = (SELECT avg(residual_risk_score) FROM res_risk_assessment_line WHERE risk_assessment_id = %s) where id =%s',
                            (risk_assessment_id, risk_assessment_id))
