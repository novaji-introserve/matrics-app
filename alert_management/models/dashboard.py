from datetime import datetime, timedelta

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
        payloads = []
        for chart in chart_records:
            if chart.code == "alert_alerts_per_day":
                payloads.append(
                    self._get_alerts_per_day_payload(
                        chart, period, start_date, end_date, start_at, end_at
                    )
                )
                continue
            payloads.append(
                chart._get_dashboard_chart_payload(
                    chart_service,
                    cco=True,
                    branches_id=[],
                    datepicked=period,
                    start_at=start_at,
                    end_at=end_at,
                )
            )
        return payloads

    def _get_alerts_per_day_payload(
        self, chart, period, start_date, end_date, start_at, end_at
    ):
        if chart.refresh_mode == "live":
            return self._build_alerts_per_day_payload(
                chart, period, start_date, end_date, start_at, end_at
            )

        cached_payload = chart._read_cached_payload(
            cco=True,
            branches_id=[],
            datepicked=period,
        )
        if cached_payload:
            return cached_payload

        payload = self._build_alerts_per_day_payload(
            chart, period, start_date, end_date, start_at, end_at
        )
        chart._store_cached_payload(
            payload,
            cco=True,
            branches_id=[],
            datepicked=period,
        )
        return payload

    def _build_alerts_per_day_payload(self, chart, period, start_date, end_date, start_at, end_at):
        domain = [
            ("create_date", ">=", start_at),
            ("create_date", "<", end_at),
        ]
        grouped = self.env["alert.history"].sudo().read_group(
            domain,
            ["create_date"],
            ["create_date:day"],
            lazy=False,
        )
        counts_by_day = {}
        domains_by_day = {}
        for row in grouped:
            day_key = self._extract_grouped_day_key(row, "create_date:day")
            if not day_key:
                continue
            counts_by_day[day_key] = row.get("create_date_count", row.get("__count", 0))
            domains_by_day[day_key] = self._build_grouped_day_domain(row, "create_date:day")

        labels = []
        values = []
        point_domains = []
        current_day = start_date
        while current_day <= end_date:
            label = current_day.isoformat()
            labels.append(label)
            values.append(counts_by_day.get(label, 0))
            point_domains.append(domains_by_day.get(label, self._build_fallback_day_domain(label)))
            current_day += timedelta(days=1)

        colors = ChartDataService(self.env).color_generator._generate_colors(
            chart.color_scheme, max(len(labels), 1)
        )
        additional_domain = chart._build_dashboard_navigation_domain(
            cco=True,
            branches_id=[],
            datepicked=period,
            start_at=start_at,
            end_at=end_at,
        )
        return {
            "id": chart.code,
            "record_id": chart.id,
            "title": chart.name,
            "display_summary": chart.display_summary or chart.description or "",
            "type": chart.chart_type,
            "model_name": chart.target_model,
            "filter": chart.navigation_filter_field or chart.domain_field,
            "column": chart.column,
            "labels": labels,
            "ids": labels,
            "point_domains": point_domains,
            "datefield": chart.date_field,
            "additional_domain": additional_domain,
            "datasets": [
                {
                    "label": chart.name,
                    "data": values,
                    "backgroundColor": colors,
                    "borderColor": colors,
                    "borderWidth": 1,
                }
            ],
        }

    def _coerce_grouped_day(self, value):
        value = (value or "").strip()
        for fmt in ("%Y-%m-%d", "%d %b %Y", "%d %B %Y", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value, fmt).date().isoformat()
            except ValueError:
                continue
        return None

    def _extract_grouped_day_key(self, row, field_key):
        range_values = row.get("__range", {}).get(field_key, {})
        range_start = range_values.get("from")
        if range_start:
            day_key = self._coerce_grouped_day(range_start)
            if day_key:
                return day_key

        grouped_day = row.get(field_key)
        if not grouped_day:
            return None
        if isinstance(grouped_day, str):
            return self._coerce_grouped_day(grouped_day)
        if hasattr(grouped_day, "date"):
            return grouped_day.date().isoformat()
        if hasattr(grouped_day, "isoformat"):
            return grouped_day.isoformat()
        return None

    def _build_grouped_day_domain(self, row, field_key):
        range_values = row.get("__range", {}).get(field_key, {})
        range_start = range_values.get("from")
        range_end = range_values.get("to")
        if range_start and range_end:
            return [
                ["create_date", ">=", range_start],
                ["create_date", "<", range_end],
            ]
        day_key = self._extract_grouped_day_key(row, field_key)
        return self._build_fallback_day_domain(day_key) if day_key else []

    def _build_fallback_day_domain(self, day_key):
        if not day_key:
            return []
        day_start = f"{day_key} 00:00:00"
        day_end = f"{(fields.Date.to_date(day_key) + timedelta(days=1)).isoformat()} 00:00:00"
        return [
            ["create_date", ">=", day_start],
            ["create_date", "<", day_end],
        ]
