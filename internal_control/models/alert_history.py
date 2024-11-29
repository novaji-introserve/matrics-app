from odoo import models, fields, api

class alert_history(models.Model):
    _name = 'alert.history'
    _description = "alert history"

    alert_id = fields.Char(string="alert_id", required=True)
    attachment = fields.Many2one("ir.attachment")
    html_body = fields.Html(string="html body")
    alert_rule_id = fields.Many2one("alert.rules")

   