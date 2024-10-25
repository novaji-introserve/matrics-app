from odoo import models, fields, api, _


class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'
    # mail send configurator
    risk_assessment_plan = fields.Selection(
        string='Compute Risk Assessment', selection=[('avg', 'Using Average Score'), ('max', 'Using Maximum Score')], default='avg')
