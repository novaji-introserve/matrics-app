from odoo import fields, models


class InterbankStatisticScope(models.Model):
    _inherit = "res.compliance.stat"

    scope = fields.Selection(
        selection_add=[("interbank", "Inter-Bank")],
        ondelete={"interbank": "set default"},
    )


class InterbankChartScope(models.Model):
    _inherit = "res.dashboard.charts"

    scope = fields.Selection(
        selection_add=[("interbank", "Inter-Bank")],
        ondelete={"interbank": "set default"},
    )
