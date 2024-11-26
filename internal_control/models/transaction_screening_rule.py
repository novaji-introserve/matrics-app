# models/internal_control_screening_rule.py
from odoo import models, fields, api, _

class InternalControlScreeningRule(models.Model):
    _inherit = 'res.transaction.screening.rule'  # Inherit the original model
    _name = 'internal_control.screening.rule'  # New model name
    _description = 'Internal Control Screening Rule'
    _table = 'internal_control_screening_rule'  # Separate table
    
    # Add your custom fields
    control_reference = fields.Char('Control Reference')
    department_id = fields.Many2one('hr.department', string='Department')
    
    # Override activate method
    def action_activate(self):
        for record in self:
            super(InternalControlScreeningRule, self).action_activate()
            # Add your custom activation logic here
            
    # Override deactivate method
    def action_deactivate(self):
        for record in self:
            super(InternalControlScreeningRule, self).action_deactivate()
            # Add your custom deactivation logic here