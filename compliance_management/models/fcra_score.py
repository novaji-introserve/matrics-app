# models/fcra_risk.py
from odoo import models, fields, api
from datetime import datetime, timedelta

class FcraScore(models.Model):
    _name = 'res.fcra.score'
    _description = 'Risk Score'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    name = fields.Text(string='Name')
    max_score = fields.Float(default=9, tracking=True)
    min_score = fields.Float(default=0, tracking=True)
    