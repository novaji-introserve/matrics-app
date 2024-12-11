# models/internal_control_screening_rule.py
from odoo import models, fields, api, _

class InternalControlScreeningRule(models.Model):
    _inherit = 'res.transaction.screening.rule'  # Inherit the original model
    
    
    