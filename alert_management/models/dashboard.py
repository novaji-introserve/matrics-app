from datetime import timedelta

from odoo import api, fields, models

ALERT_DASHBOARD_ACTIONS = {
    "alert_total_today": "alert_management.action_alert_history_status",
    "alert_groups_total": "alert_management.action_alert_group",
    "alert_rules_total": "alert_management.action_alert_rules",
    "alert_sql_queries_total": "alert_management.action_process_sql",
}


class AlertHistoryDashboard(models.Model):
    _inherit = "alert.history"

    @api.model
    def get_alert_dashboard_data(self, period=7):
        period = self._normalize_period(period)

        return {
            "period": period,
            "cards": self._get_alert_cards(),
            "trend": self._get_alert_trend(period),
        }

    @api.model
    def _get_alert_cards(self):
        stat_records = self.env["res.compliance.stat"].sudo().search(
            [("state", "=", "active"), ("scope", "=", "alert")],
            order="id",
        )
        self._refresh_alert_stat_values(stat_records)
        return [
            {
                "id": stat.code,
                "title": stat.name,
                "value": stat.val or 0,
                "action_xmlid": ALERT_DASHBOARD_ACTIONS.get(stat.code),
            }
            for stat in stat_records
        ]

    @api.model
    def _refresh_alert_stat_values(self, stat_records):
        for stat in stat_records:
            try:
                original_query, query = stat._prepare_and_validate_query(
                    stat.sql_query, code=stat.code
                )
                current_value = stat._execute_query_and_get_value(original_query, query) if original_query else "0"
            except Exception:
                current_value = "0"

            if (stat.val or "0") != current_value:
                self.env.cr.execute(
                    """
                    UPDATE res_compliance_stat
                    SET val = %s,
                        write_date = NOW(),
                        write_uid = %s
                    WHERE id = %s
                    """,
                    (current_value, self.env.user.id, stat.id),
                )

        if stat_records:
            stat_records.invalidate_recordset(["val", "write_date", "write_uid"])

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
    def _get_alert_trend(self, period):
        start_date, end_date = self._get_period_bounds(period)

        self.env.cr.execute(
            """
            SELECT DATE(create_date) AS day, COUNT(*)
            FROM alert_history
            WHERE create_date IS NOT NULL
              AND DATE(create_date) BETWEEN %s AND %s
            GROUP BY DATE(create_date)
            ORDER BY DATE(create_date)
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
