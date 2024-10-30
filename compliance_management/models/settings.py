from odoo import models, fields, api, _

    
class ComplianceSettings(models.Model):
    _name = 'res.compliance.settings'
    _description = 'Compliance Settings'
    _sql_constraints = [
        ('uniq_compl_settings_code', 'unique(code)',
         "Code already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string='Code', required=True,index=True)
    val = fields.Char(string='Value')
    narration = fields.Text(string='Narration')
