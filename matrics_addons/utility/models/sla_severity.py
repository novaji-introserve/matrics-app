# models/sla_severity.py
from odoo import models, fields

class SLASeverity(models.Model):
    _name = 'sla.severity'
    _description = 'SLA Severity'

    name = fields.Char(string="Severity Level", required=True)
    noOfSupervisor = fields.Integer(string="No of Supervisor",required=True)
    description = fields.Text(string="Description")
   