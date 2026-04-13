from odoo import models, fields, api

class AlertGroupEscalation(models.Model):
    _inherit = 'alert.group'

    escalation_matrix_id = fields.Many2one(
        'fsdh.escalation.matrix',
        string='Default Escalation Matrix',
        help="Automatically fills the Escalation Matrix on alert rules for this group.",
        tracking=True,
    )
