# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

from odoo import fields, http
from odoo.http import request


class InterbankDashboardController(http.Controller):
    def _normalize_dashboard_period(self, datepicked):
        try:
            datepicked = int(datepicked)
        except (TypeError, ValueError):
            datepicked = 7
        return datepicked if datepicked in (0, 1, 7, 30) else 7

    def _dashboard_date_range(self, datepicked):
        today = datetime.now().date()
        if datepicked == 0:
            start_date = today
            end_date = today
        elif datepicked == 1:
            start_date = today - timedelta(days=1)
            end_date = start_date
        elif datepicked == 7:
            start_date = today - timedelta(days=6)
            end_date = today
        else:
            start_date = today - timedelta(days=29)
            end_date = today

        start_at = fields.Datetime.to_string(
            datetime.combine(start_date, datetime.min.time())
        )
        end_at = fields.Datetime.to_string(
            datetime.combine(end_date, datetime.max.time())
        )
        return start_at, end_at

    def _query_rows(self, sql, params):
        request.env.cr.execute(sql, params)
        return request.env.cr.fetchall()

    def _build_bar_chart(self, chart_id, title, subtitle, rows, color):
        labels = [row[0] for row in rows]
        data = [int(row[1]) for row in rows]
        return {
            "id": chart_id,
            "title": title,
            "display_summary": subtitle,
            "type": "bar",
            "labels": labels,
            "datasets": [
                {
                    "label": "Transactions",
                    "data": data,
                    "backgroundColor": color,
                    "borderRadius": 8,
                    "maxBarThickness": 48,
                }
            ],
        }

    @http.route("/interbank/dashboard/data", auth="user", type="json")
    def interbank_dashboard_data(self, datepicked=7, **kw):
        datepicked = self._normalize_dashboard_period(datepicked)
        start_at, end_at = self._dashboard_date_range(datepicked)

        stat_rows = self._query_rows(
            """
            SELECT
                COUNT(*) AS total_transactions,
                COALESCE(SUM(amount_local), 0) AS total_amount,
                COUNT(DISTINCT currency_code) AS total_currencies
            FROM bank_transaction
            WHERE date_transaction >= %s
              AND date_transaction <= %s
            """,
            (start_at, end_at),
        )
        total_transactions, total_amount, total_currencies = stat_rows[0]

        source_account_rows = self._query_rows(
            """
            SELECT COUNT(DISTINCT from_account_id)
            FROM bank_transaction
            WHERE date_transaction >= %s
              AND date_transaction <= %s
              AND from_account_id IS NOT NULL
            """,
            (start_at, end_at),
        )
        source_accounts = source_account_rows[0][0] if source_account_rows else 0

        institution_rows = self._query_rows(
            """
            SELECT COUNT(DISTINCT a.institution_id)
            FROM bank_transaction t
            JOIN bank_account a ON a.id = t.from_account_id
            WHERE t.date_transaction >= %s
              AND t.date_transaction <= %s
              AND a.institution_id IS NOT NULL
            """,
            (start_at, end_at),
        )
        institutions = institution_rows[0][0] if institution_rows else 0

        account_type_rows = self._query_rows(
            """
            SELECT
                COALESCE(NULLIF(atype.name, ''), 'Unspecified') AS account_type,
                COUNT(*) AS transaction_count
            FROM bank_transaction t
            LEFT JOIN bank_account a ON a.id = t.from_account_id
            LEFT JOIN bank_account_type atype ON atype.id = a.personal_account_type
            WHERE t.date_transaction >= %s
              AND t.date_transaction <= %s
            GROUP BY account_type
            ORDER BY transaction_count DESC, account_type ASC
            """,
            (start_at, end_at),
        )

        currency_rows = self._query_rows(
            """
            SELECT
                COALESCE(NULLIF(currency_code, ''), 'Unspecified') AS currency_code,
                COUNT(*) AS transaction_count
            FROM bank_transaction
            WHERE date_transaction >= %s
              AND date_transaction <= %s
            GROUP BY currency_code
            ORDER BY transaction_count DESC, currency_code ASC
            """,
            (start_at, end_at),
        )

        charts = [
            self._build_bar_chart(
                "transactions_by_account_type",
                "Transactions by Account Type",
                "Grouped by originating account type for the selected period.",
                account_type_rows,
                [
                    "#0f766e",
                    "#14b8a6",
                    "#2dd4bf",
                    "#5eead4",
                    "#99f6e4",
                    "#134e4a",
                ],
            ),
            self._build_bar_chart(
                "transactions_by_currency",
                "Transactions by Currency",
                "Volume of inter-bank transactions by transaction currency.",
                currency_rows,
                [
                    "#1d4ed8",
                    "#3b82f6",
                    "#60a5fa",
                    "#93c5fd",
                    "#bfdbfe",
                    "#1e3a8a",
                ],
            ),
        ]

        return {
            "period": datepicked,
            "range": {"start_at": start_at, "end_at": end_at},
            "stats": [
                {
                    "id": "total_transactions",
                    "label": "Transactions",
                    "value": int(total_transactions or 0),
                    "accent": "#0f766e",
                },
                {
                    "id": "total_amount",
                    "label": "Total Value",
                    "value": float(total_amount or 0.0),
                    "accent": "#1d4ed8",
                    "is_currency": True,
                },
                {
                    "id": "source_accounts",
                    "label": "Source Accounts",
                    "value": int(source_accounts or 0),
                    "accent": "#c2410c",
                },
                {
                    "id": "institutions",
                    "label": "Institutions",
                    "value": int(institutions or 0),
                    "accent": "#7c3aed",
                },
                {
                    "id": "currencies",
                    "label": "Currencies",
                    "value": int(total_currencies or 0),
                    "accent": "#be123c",
                },
            ],
            "charts": charts,
            "generated_at": fields.Datetime.now(),
        }
