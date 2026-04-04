# -*- coding: utf-8 -*-

import logging
from odoo import http
from odoo.http import request

_logger = logging.getLogger(__name__)


class CaseDashboardController(http.Controller):
    @http.route("/case_dashboard/stats", auth="user", type="json")
    def case_dashboard_stats(self, period=0, **kw):
        stat_records = request.env["res.compliance.stat"].sudo().search(
            [("state", "=", "active"), ("is_visible", "=", True), ("scope", "=", "case")],
            order="display_order asc, id asc",
        )

        results = [
            {
                "id": stat.id,
                "code": stat.code,
                "name": stat.name,
                "scope": stat.scope,
                "scope_color": stat.scope_color,
                "sql_query": stat.sql_query,
                "display_summary": stat.display_summary,
                **stat._get_dashboard_action_metadata(),
                "val": stat.val or 0,
                "percentage": "0.00",
            }
            for stat in stat_records
        ]

        return {"data": results, "total": len(results)}
