# models/fcra_risk.py
from odoo import models, fields, api
from datetime import datetime, timedelta

class FcraMitigation(models.Model):
    _name = 'risk.assessment.mitigation'
    _description = 'FCRA Risk Mitigation'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    name = fields.Char('Mitigation Name', required=True, tracking=True)
    description = fields.Text('Mitigation Description', tracking=True)
   