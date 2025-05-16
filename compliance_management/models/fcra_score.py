# models/fcra_risk.py
from odoo import models, fields, api
from datetime import datetime, timedelta

class FcraScore(models.Model):
    _name = 'res.fcra.score'
    _description = 'FCRA Risk Score'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    name = fields.Char(required=True)
    inherent_risk_score_max = fields.Float(default=15, tracking=True)
    inherent_risk_score_min = fields.Integer(default=1)  # Add this field
    control_effectiveness_score_max = fields.Float(default=15, tracking=True)
    control_effectiveness_score_min = fields.Integer(default=1)  # Add this field
    residual_risk_score_max = fields.Float(default=15, tracking=True)
    residual_risk_score_min = fields.Integer(default=1)  # Add this field
    
   