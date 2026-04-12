# -*- coding: utf-8 -*-

from odoo import fields, models


class DashboardCharts(models.Model):
    _inherit = "res.dashboard.charts"

    scope = fields.Selection(
        selection_add=[("interbank", "Transaction Monitoring")],
        ondelete={"interbank": "set default"},
    )


class ComplianceStat(models.Model):
    _inherit = "res.compliance.stat"

    scope = fields.Selection(
        selection_add=[("interbank", "Transaction Monitoring")],
        ondelete={"interbank": "set default"},
    )
