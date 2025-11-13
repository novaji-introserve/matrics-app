# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
import logging
# CONTROL_EFFECTIVENESS_MAX_SCORE = 25
from odoo.exceptions import ValidationError
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
        string='Control Effectiveness Score', readonly=True,  tracking=True)
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
    
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    
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

    
    @api.onchange('existing_controls')
    def _check_accumulated_score(self):
        for record in self:
            if not record.existing_controls:
                record.update({'control_effectiveness_score': 0}) 
                continue

            # Search for the specific setting record by code
            max_threshold_setting = self.env['res.compliance.settings'].sudo().search(
                [('code', '=', 'maximum_risk_threshold')], 
                limit=1
            )
            
            if not max_threshold_setting:
                return {
                    'warning': {
                        'title': _("Compliance Settings"),
                        'message': "High risk threshold setting is not configured"
                    }
                }

            max_threshold = float(max_threshold_setting.val)
    
            total_score = sum(control.effectiveness_score_numeric or 0 for control in record.existing_controls)
            
            if total_score > max_threshold:
                # Remove last added control (approximate)
                record.existing_controls = [(3, record.existing_controls[-1].id)] if record.existing_controls else []
                return {
                    'warning': {
                        'title': _("Total Effectiveness Score Exceeded"),
                        'message': _(
                            "The current selection of controls has a total effectiveness score of %.1f, "
                            "which exceeds the allowed maximum of %.1f.\n\n"
                            "This will prevent saving until corrected. "
                            "Consider removing or replacing some controls."
                        ) % (total_score, max_threshold)
                    }
                }

            # Use .update() to set readonly field in onchange context
            record.update({'control_effectiveness_score': total_score})

    @api.constrains('existing_controls')
    def _check_controls_total_score(self):
        """Ensure total effectiveness score of selected controls doesn't exceed threshold"""
        score_config = self.env['res.fcra.score'].sudo().search([], limit=1)
        
        if not score_config:
            raise ValidationError(_("FCRA score configuration is missing. Please contact administrator."))
        
        max_threshold = float(score_config.max_score)
        
        for record in self:
            if record.existing_controls:
                total_score = sum(
                    control.effectiveness_score_numeric or 0 
                    for control in record.existing_controls
                )
                
                if total_score > max_threshold:
                    raise ValidationError(_(
                        f"Total Effectiveness Score Exceeded!\n\n"
                        f"The sum of effectiveness scores from selected controls ({total_score}) "
                        f"exceeds the maximum threshold of {max_threshold}.\n\n"
                        f"Please remove some controls or select different ones to stay within the limit."
                    ))
                
                # Update control_effectiveness_score with the calculated total
                record.control_effectiveness_score = total_score

    @api.depends('inherent_risk_score', 'control_effectiveness_score', 'residual_risk_impact','residual_risk_score','residual_risk_probability','residual_risk_score')
    def _compute_risk_score(self):
        max_score = self.get_control_effectiveness_max_score()
        for record in self:
            probability = self._compute_risk_probability(
                record.control_effectiveness_score)
            
            record.residual_risk_probability = probability
            record.residual_risk_score = record.inherent_risk_score * (1 - (record.control_effectiveness_score / max_score))

 
    @api.depends('control_effectiveness_score', 'inherent_risk_score')
    def _compute_residual_risk_impact(self):
        max_score = self.get_control_effectiveness_max_score()
        for record in self:
            control_effectiveness_score = record.control_effectiveness_score or 0  # Handle None/False values
            inherent_score = record.inherent_risk_score or 0  # Handle None/False values
            
            # Ensure we don't have negative values if control score > max
            record.residual_risk_impact = inherent_score * (1 - (control_effectiveness_score / max_score))
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
        if max_score == 0:
            return 100.0  # Avoid division by zero
        
        score =  (1 - (control_effectiveness_score / max_score))
        return score  * 100

    def _compute_residual_risk_score(self, probability, impact):
        return (probability/100) * impact

    def update_aggregate_risk_score(self):
        risk_assessment_id = self.risk_assessment_id.id
        self.env.cr.execute('update res_risk_assessment set risk_rating = (SELECT avg(residual_risk_score) FROM res_risk_assessment_line WHERE risk_assessment_id = %s) where id =%s',
                            (risk_assessment_id, risk_assessment_id))
        