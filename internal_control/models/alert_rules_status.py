from odoo import models, fields, api

class alert_rules_status(models.Model):
    _name = 'alert.rules.status'
    _description = "alert rules status for exception management"

    name = fields.Char(string="Name", required=True)
    code = fields.Boolean(string="Code", required=True)

   