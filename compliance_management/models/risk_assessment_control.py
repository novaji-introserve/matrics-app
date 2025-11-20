# -*- coding: utf-8 -*-

from odoo import models, fields, api
from datetime import datetime
from odoo.exceptions import ValidationError
class RiskAssessmentControl(models.Model):
    _name = 'risk.assessment.control'
    _description = 'Risk Assessment Control'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _order = 'priority desc, id desc'

    name = fields.Char('Control Name', required=True, tracking=True)
    code = fields.Char('Control Code', tracking=True)
    description = fields.Text('Description', tracking=True)
    control_type = fields.Selection([
        ('preventive', 'Preventive'),
        ('detective', 'Detective'),
        ('corrective', 'Corrective'),
        ('directive', 'Directive')
    ], string='Control Type', required=True, tracking=True)
    control_nature = fields.Selection([
        ('manual', 'Manual'),
        ('automated', 'Automated'),
        ('semi_automated', 'Semi-Automated')
    ], string='Control Nature', required=True, tracking=True)
    
    # FCRA specific fields
    is_fcra_relevant = fields.Boolean('FCRA Relevant', default=False, tracking=True)
    # fcra_section = fields.Char('FCRA Section', tracking=True)
    
    # Risk related fields
    # risk_ids = fields.Many2many('risk.assessment', string='Related Risks')
    priority = fields.Selection([
        ('0', 'Low'),
        ('1', 'Medium'),
        ('2', 'High'),
        ('3', 'Critical')
    ], string='Priority', default='1', tracking=True)
    
    # Effectiveness
    effectiveness_score_numeric = fields.Float(
        string='Effectiveness Score Numeric',
        tracking=True,
        default=1,
        digits=(10,1),
        help="Score from 1 to the maximum risk score."
    )

    effectiveness_score = fields.Selection([
        ('1', 'Not Effective'),
        ('2', 'Partially Effective'),
        ('3', 'Mostly Effective'),
        ('4', 'Effective'),
        ('5', 'Highly Effective')
    ], string='Effectiveness Score', tracking=True)

    effectiveness_score_help = fields.Char(
        string='Score Help',
        compute='_compute_effectiveness_score_help',
        # store=False (default) — no DB storage, computed on-the-fly
    )
    last_assessment_date = fields.Date('Last Assessment Date', tracking=True)
    
   
    state = fields.Selection([
        ('active', 'Active'),
        ('inactive', 'Inactive')
    ], string='Status', default='active', tracking=True)
    
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    
    max_risk_score = fields.Float(string='Maximum Risk Score', compute='_compute_max_risk_score', store=False)
    
   

    def action_set_active(self):
        self.write({'state': 'active'})

    def action_set_under_review(self):
        self.write({'state': 'under_review'})

    def action_set_inactive(self):
        self.write({'state': 'inactive'})

    
    @api.depends('name')
    def _compute_max_risk_score(self):
        """Compute the maximum risk score to use for slider widget"""
        for record in self:
            if record.name:
                record.max_risk_score = float(self.env['res.compliance.settings'].get_setting('maximum_risk_threshold'))
            else:
                record.max_risk_score = 10  # Default maximum value
                
    @api.depends('effectiveness_score_numeric') 
    def _compute_effectiveness_score_help(self):
        # Fetch config once (cached per request)
        score_config = self.env['res.fcra.score'].sudo().search([], limit=1)
        if score_config:
            min_s = int(score_config.min_score)
            max_s = int(score_config.max_score)
            help_text = f"Valid range: {min_s} – {max_s} (configured in FCRA settings)"
        else:
            help_text = "FCRA score range not configured — contact administrator."

        for rec in self:
            rec.effectiveness_score_help = help_text
            

    @api.onchange('effectiveness_score_numeric')
    def _onchange_effectiveness_score(self):
        # Skip if not set (None/False) or if 0 is likely accidental *and* below min
        score = self.effectiveness_score_numeric
        if score is False:
            return  # truly unset

        score_config = self.env['res.fcra.score'].sudo().search([], limit=1)
        if not score_config:
            return {'warning': {
                'title': "Missing Configuration",
                'message': "FCRA score range is not configured. Please set min/max scores in FCRA settings."
            }}

        min_score = int(score_config.min_score)
        max_score = int(score_config.max_score)


        if score == 0 and min_score > 0:
            return

        # Now validate intentional values
        if not (min_score <= score <= max_score):
            return {
                'warning': {
                    'title': "Invalid Effectiveness Score",
                    'message': (
                        f"The entered score ({score}) is outside the allowed range.\n"
                        f"Valid range: {min_score} – {max_score}."
                    )
                }
            }

    
    @api.constrains('effectiveness_score_numeric')
    def _check_effectiveness_score_range(self):
        score_config = self.env['res.fcra.score'].sudo().search([], limit=1)
        if not score_config:
            # Only raise if any record has a score set
            if any(rec.effectiveness_score_numeric is not False and rec.effectiveness_score_numeric != 0 for rec in self):
                raise ValidationError("FCRA score configuration is missing...")
            return  # allow saving if no meaningful score set yet

        min_score = int(score_config.min_score)
        max_score = int(score_config.max_score)

        for rec in self:
            score = rec.effectiveness_score_numeric
            # Treat 0 as "not set" ONLY if 0 is below the minimum (i.e., invalid by config)
            # i.e., if min_score > 0, then 0 is likely accidental blank → skip validation
            if score == 0 and min_score > 0:
                # Assume user left it blank → skip
                continue
            if score is False:
                continue
            if not (min_score <= score <= max_score):
                raise ValidationError(
                    f"Effectiveness Score ({score}) is outside allowed range [{min_score}, {max_score}]."
                )