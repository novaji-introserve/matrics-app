from datetime import timedelta

from odoo import api, fields, models
from odoo.addons.compliance_management.services.chart_data_service import ChartDataService


class AlertHistoryDashboard(models.Model):
    _inherit = "alert.history"

    @api.model
    def get_alert_dashboard_data(self, period=7):
        period = self._normalize_period(period)

        return {
            "period": period,
            "cards": self._get_alert_cards(),
            "charts": self._get_alert_charts(period),
        }

    @api.model
    def _get_alert_cards(self):
        stat_records = self.env["res.compliance.stat"].sudo().search(
            [("state", "=", "active"), ("is_visible", "=", True), ("scope", "=", "alert")],
            order="display_order asc, id asc",
        )
        return [
            {
                "id": stat.code,
                "title": stat.name,
                "value": self._get_alert_stat_value(stat),
                "display_summary": stat.display_summary,
                **stat._get_dashboard_action_metadata(),
            }
            for stat in stat_records
        ]

    @api.model
    def _get_alert_stat_value(self, stat):
        try:
            return stat._compute_current_value(stat)
        except Exception:
            return stat.val or "0"

    def _normalize_period(self, period):
        try:
            period = int(period)
        except (TypeError, ValueError):
            period = 7
        return period if period in (0, 1, 7, 30) else 7

    def _get_period_bounds(self, period):
        now = fields.Datetime.context_timestamp(self, fields.Datetime.now())
        end_date = now.date()
        if period == 0:
            start_date = end_date
            range_end = end_date
        elif period == 1:
            start_date = end_date - timedelta(days=1)
            range_end = start_date
        elif period == 7:
            start_date = end_date - timedelta(days=6)
            range_end = end_date
        else:
            start_date = end_date - timedelta(days=29)
            range_end = end_date
        return start_date, range_end

    @api.model
    def _get_alert_charts(self, period):
        start_date, end_date = self._get_period_bounds(period)
        start_at = f"{start_date.isoformat()} 00:00:00"
        end_at = f"{(end_date + timedelta(days=1)).isoformat()} 00:00:00"
        chart_service = ChartDataService(self.env)
        chart_records = self.env["res.dashboard.charts"].sudo().search(
            [
                ("state", "=", "active"),
                ("active", "=", True),
                ("is_visible", "=", True),
                ("scope", "=", "alert"),
            ],
            order="display_order asc, id asc",
        )
        return [
            chart._get_dashboard_chart_payload(
                chart_service,
                cco=True,
                branches_id=[],
                datepicked=period,
                start_at=start_at,
                end_at=end_at,
            )
            for chart in chart_records
        ]
