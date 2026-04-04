# -*- coding: utf-8 -*-

import logging
import re
from datetime import datetime, timedelta

from odoo import fields, http
from odoo.http import request

_logger = logging.getLogger(__name__)


CASE_DASHBOARD_ACTIONS = {
    "case_all_cases": "case_management.action_case_manager",
    "case_draft_cases": "case_management.action_draft_cases",
    "case_open_cases": "case_management.action_open_cases",
    "case_overdue_cases": "case_management.action_overdue_cases",
    "case_closed_cases": "case_management.action_closed_cases",
    "case_archived_cases": "case_management.action_archived_cases",
}


class CaseDashboardController(http.Controller):
    def _normalize_period(self, period):
        try:
            period = int(period)
        except (TypeError, ValueError):
            period = 7
        return period if period in (0, 1, 7, 30) else 7

    def _dashboard_date_range(self, period):
        today = datetime.now().date()
        if period == 0:
            current_start_date = today
            current_end_date = today
            previous_start_date = today - timedelta(days=1)
            previous_end_date = today - timedelta(days=1)
        elif period == 1:
            current_start_date = today - timedelta(days=1)
            current_end_date = current_start_date
            previous_start_date = today - timedelta(days=2)
            previous_end_date = previous_start_date
        elif period == 7:
            current_start_date = today - timedelta(days=6)
            current_end_date = today
            previous_start_date = today - timedelta(days=13)
            previous_end_date = today - timedelta(days=7)
        else:
            current_start_date = today - timedelta(days=29)
            current_end_date = today
            previous_start_date = today - timedelta(days=59)
            previous_end_date = today - timedelta(days=30)

        current_start = fields.Datetime.to_string(
            datetime.combine(current_start_date, datetime.min.time())
        )
        current_end = fields.Datetime.to_string(
            datetime.combine(current_end_date, datetime.max.time())
        )
        previous_start = fields.Datetime.to_string(
            datetime.combine(previous_start_date, datetime.min.time())
        )
        previous_end = fields.Datetime.to_string(
            datetime.combine(previous_end_date, datetime.max.time())
        )
        return current_start, current_end, previous_start, previous_end

    def _append_runtime_filters(self, sql_query, runtime_clause):
        normalized_query = (sql_query or "").strip().rstrip(";")
        if not normalized_query:
            return normalized_query
        if re.search(r"\bwhere\b", normalized_query, flags=re.IGNORECASE):
            return f"{normalized_query} AND {runtime_clause}"
        return f"{normalized_query} WHERE {runtime_clause}"

    def _build_runtime_clause(self, include_period=False, include_previous=False, period=0):
        user_id = request.env.user.id
        current_start, current_end, previous_start, previous_end = self._dashboard_date_range(period)
        clauses = [
            "("
            "case_manager.create_uid = %s "
            "OR case_manager.officer_responsible = %s "
            "OR EXISTS ("
            "SELECT 1 "
            "FROM case_manager_res_users_rel supervisor_rel "
            "WHERE supervisor_rel.case_manager_id = case_manager.id "
            "AND supervisor_rel.res_users_id = %s"
            ")"
            ")"
        ]
        params = [user_id, user_id, user_id]

        if include_period:
            if include_previous:
                clauses.append("case_manager.create_date >= %s")
                clauses.append("case_manager.create_date <= %s")
                params.extend([previous_start, previous_end])
            else:
                clauses.append("case_manager.create_date >= %s")
                clauses.append("case_manager.create_date <= %s")
                params.extend([current_start, current_end])

        return " AND ".join(clauses), params

    def _execute_count_query(self, stat_record, include_previous=False, period=0):
        base_query, _prepared_query = stat_record._prepare_and_validate_query(
            stat_record.sql_query, code=stat_record.code
        )
        runtime_clause, params = self._build_runtime_clause(
            include_period=True,
            include_previous=include_previous,
            period=period,
        )
        final_query = self._append_runtime_filters(base_query, runtime_clause)
        request.env.cr.execute(final_query, tuple(params))
        row = request.env.cr.fetchone()
        return int(row[0] or 0) if row else 0

    @http.route("/case_dashboard/stats", auth="user", type="json")
    def case_dashboard_stats(self, period=0, **kw):
        period = self._normalize_period(period)

        stat_records = request.env["res.compliance.stat"].sudo().search(
            [("state", "=", "active"), ("scope", "=", "case")],
            order="id",
        )

        results = []
        for stat in stat_records:
            try:
                current_value = self._execute_count_query(stat, include_previous=False, period=period)
                previous_value = self._execute_count_query(stat, include_previous=True, period=period)
                percentage = ((current_value - previous_value) / previous_value) * 100 if previous_value else 0
                results.append(
                    {
                        "id": stat.id,
                        "code": stat.code,
                        "name": stat.name,
                        "scope": stat.scope,
                        "scope_color": stat.scope_color,
                        "sql_query": stat.sql_query,
                        "val": current_value,
                        "percentage": f"{percentage:.2f}" if percentage == percentage else "0.00",
                        "action_xmlid": CASE_DASHBOARD_ACTIONS.get(stat.code),
                    }
                )
            except Exception:
                _logger.exception("Failed to compute case dashboard stat %s", stat.code)
                results.append(
                    {
                        "id": stat.id,
                        "code": stat.code,
                        "name": stat.name,
                        "scope": stat.scope,
                        "scope_color": stat.scope_color,
                        "sql_query": stat.sql_query,
                        "val": 0,
                        "percentage": "0.00",
                        "action_xmlid": CASE_DASHBOARD_ACTIONS.get(stat.code),
                    }
                )

        return {"data": results, "total": len(results)}
