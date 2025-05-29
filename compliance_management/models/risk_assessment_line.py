# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
import logging
# CONTROL_EFFECTIVENESS_MAX_SCORE = 25

_logger = logging.getLogger(__name__)


class RiskAssessmentLine(models.Model):
    _name = 'res.risk.assessment.line'
    _description = 'Risk Assessment Line'
    # _sql_constraints = [
    #     ('uniq_risk_assessment_line_name', 'unique(name)',
    #      "Risk Assessment Name already exists. Value must be unique!")
    # ]
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
        string='Residual Risk Impact', compute="_compute_residual_risk_impact", tracking=True, store=False, readonly="True")
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
    
    def init(self):
        """
        Override init() to drop constraint if exists.
        """
        self._cr.execute("""
            SELECT conname FROM pg_constraint 
            WHERE conname = 'res_risk_assessment_line_uniq_risk_assessment_line_name'
        """)
        if self._cr.fetchone():
            self._cr.execute("""
                ALTER TABLE res_risk_assessment_line 
                DROP CONSTRAINT res_risk_assessment_line_uniq_risk_assessment_line_name
            """)
        super().init()

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

    @api.depends('control_effectiveness_score', 'inherent_risk_score')
    def _compute_residual_risk_impact(self):
        for record in self:
            control_effectiveness_score = record.control_effectiveness_score or 0  # Handle None/False values
            inherent_score = record.inherent_risk_score or 0  # Handle None/False values
            max_score = self.env['res.fcra.score'].max_score
            max_score = float(max_score)
            
            _logger.info(f"The control score is {control_effectiveness_score}")
            _logger.info(f"Max score is {max_score}")
            
            # Ensure we don't have negative values if control score > max
            record.residual_risk_impact = max(0, inherent_score - control_effectiveness_score)
            _logger.info(f"Residual risk impact score is {record.residual_risk_impact}")


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
        """Get the maximum score for control effectiveness from model"""
        max_score = self.env['res.fcra.score'].search([], limit=1).max_score or 9

        return float(max_score)
    

    def _compute_risk_probability(self, control_effectiveness_score):
        max_score = self.get_control_effectiveness_max_score()
        print(f"max score is {max_score}")
        if max_score == 0:
            return 100.0  # Avoid division by zero
        
        score =  (1 - (control_effectiveness_score / max_score))
        print(f"the score is {score}")
        return score  * 100

    def _compute_residual_risk_score(self, probability, impact):
        print(f"the probability is {probability}")
        print(f"the impact is {impact}")
        return (probability/100) * impact

    def update_aggregate_risk_score(self):
        risk_assessment_id = self.risk_assessment_id.id
        self.env.cr.execute('update res_risk_assessment set risk_rating = (SELECT avg(residual_risk_score) FROM res_risk_assessment_line WHERE risk_assessment_id = %s) where id =%s',
                            (risk_assessment_id, risk_assessment_id))
