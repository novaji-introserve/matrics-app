# models/process.py
from odoo import models, fields

class Process(models.Model):
    _name = 'process'
    _description = 'Process'

    name = fields.Char(string="Process Name", required=True)
    category_id = fields.Many2one('process.category', string="Process Category", required=True)
    sla_severity_id = fields.Many2one('sla.severity', string="SLA Severity", required=True)
    description = fields.Text(string="Description")

    severity_level = fields.Char(compute='_compute_severity_level', store=False)

    def _compute_severity_level(self):
        for record in self:
            if record.sla_severity_id:
                record.severity_level = record.sla_severity_id.name
            else:
                record.severity_level = 'None'