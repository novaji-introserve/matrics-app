# -*- coding: utf-8 -*-

from odoo import models, fields, api
from datetime import datetime

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
    effectiveness_score = fields.Selection([
        ('1', 'Not Effective'),
        ('2', 'Partially Effective'),
        ('3', 'Mostly Effective'),
        ('4', 'Effective'),
        ('5', 'Highly Effective')
    ], string='Effectiveness Score', tracking=True)
    last_assessment_date = fields.Date('Last Assessment Date', tracking=True)
    
   
    state = fields.Selection([
        ('draft', 'Draft'),
        ('active', 'Active'),
        ('under_review', 'Under Review'),
        ('inactive', 'Inactive')
    ], string='Status', default='draft', tracking=True)
    
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')    
   