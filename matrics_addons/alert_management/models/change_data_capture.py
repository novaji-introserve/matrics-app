from odoo import fields, models


class ChangeDataCapture(models.Model):
    _inherit = "change.data.capture"

    alert_history_id = fields.Many2one(
        "alert.history",
        string="Alert History",
    )
