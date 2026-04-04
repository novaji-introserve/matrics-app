from datetime import timedelta

from odoo import api, fields, models


class AlertHistoryDashboard(models.Model):
    _inherit = "alert.history"

    @api.model
    def get_alert_dashboard_data(self, period=7):
        period = self._normalize_period(period)
        today_start, tomorrow_start = self._today_range()

        return {
            "period": period,
            "cards": [
                {
                    "id": "alerts_today",
                    "title": "Total Alerts Today",
                    "value": self.search_count(
                        [
                            ("date_created", ">=", fields.Datetime.to_string(today_start)),
                            ("date_created", "<", fields.Datetime.to_string(tomorrow_start)),
                        ]
                    ),
                },
                {
                    "id": "alert_groups",
                    "title": "Number of Alert Groups",
                    "value": self.env["alert.group"].search_count([]),
                },
                {
                    "id": "alert_rules",
                    "title": "Number of Alert Rules",
                    "value": self.env["alert.rules"].search_count([]),
                },
                {
                    "id": "sql_queries",
                    "title": "Number of SQL Queries",
                    "value": self.env["process.sql"].search_count([]),
                },
            ],
            "trend": self._get_alert_trend(period),
        }

    def _normalize_period(self, period):
        try:
            period = int(period)
        except (TypeError, ValueError):
            period = 7
        return period if period in (0, 7, 30) else 7

    def _today_range(self):
        now = fields.Datetime.to_datetime(fields.Datetime.now())
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_start = today_start + timedelta(days=1)
        return today_start, tomorrow_start

    @api.model
    def _get_alert_trend(self, period):
        now = fields.Datetime.context_timestamp(self, fields.Datetime.now())
        end_date = now.date()
        start_date = end_date if period == 0 else end_date - timedelta(days=period - 1)

        self.env.cr.execute(
            """
            SELECT DATE(date_created::timestamp) AS day, COUNT(*)
            FROM alert_history
            WHERE date_created IS NOT NULL
              AND DATE(date_created::timestamp) BETWEEN %s AND %s
            GROUP BY DATE(date_created::timestamp)
            ORDER BY DATE(date_created::timestamp)
            """,
            [start_date, end_date],
        )
        counts_by_day = {row[0]: row[1] for row in self.env.cr.fetchall()}

        labels = []
        dates = []
        values = []
        cursor = start_date
        while cursor <= end_date:
            labels.append(cursor.strftime("%b %d"))
            dates.append(cursor.isoformat())
            values.append(counts_by_day.get(cursor, 0))
            cursor += timedelta(days=1)

        return {
            "dates": dates,
            "labels": labels,
            "values": values,
        }
