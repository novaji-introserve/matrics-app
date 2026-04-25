from odoo import SUPERUSER_ID, api, models

from ..hooks import _seed_dashboard_charts


class DashboardChartSeed(models.Model):
    _inherit = "res.dashboard.charts"

    def init(self):
        super().init()
        env = api.Environment(self._cr, SUPERUSER_ID, {})
        _seed_dashboard_charts(env)
