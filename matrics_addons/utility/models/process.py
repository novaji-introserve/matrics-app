# models/process.py
from odoo import models, fields

class Process(models.Model):
    _name = 'process'
    _description = 'Process'

    name = fields.Char(string="Process Name", required=True)
    category_id = fields.Many2one('process.category', string="Process Category", required=True)
    sla_severity_id = fields.Many2one('sla.severity', string="SLA Severity", required=True)
    description = fields.Text(string="Description")
