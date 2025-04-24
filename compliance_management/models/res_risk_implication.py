# models/fcra_risk.py
from odoo import models, fields, api
from datetime import datetime, timedelta

class FcraImplication(models.Model):
    _name = 'risk.assessment.implication'
    _description = 'FCRA Risk Implication'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    name = fields.Char('Implication Name', required=True, tracking=True)
    code = fields.Char('Code', required=True, tracking=True)
    description = fields.Text('Implication Description', tracking=True)
    state = fields.Selection([
        ('active', 'Active'),
        ('inactive', 'InActive')
    ], string="State", default="active")
    severity = fields.Selection([
        ('low', 'Low'),
        ('medium', 'Medium'),
        ('high', 'High'),
    ], string='Severity',default="high", tracking=True)
   