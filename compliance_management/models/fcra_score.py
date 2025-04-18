# models/fcra_risk.py
from odoo import models, fields, api
from datetime import datetime, timedelta

class FcraScore(models.Model):
    _name = 'res.fcra.score'
    _description = 'FCRA Risk Score'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    name = fields.Char('Implication Name', required=True, tracking=True)
    max_score = fields.Float(
        string='Maximum Score', required=True, tracking=True)
    min_score = fields.Integer(string='Minimum Score', default=1)  # Add this field
    
   