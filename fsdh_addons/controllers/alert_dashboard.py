# -*- coding: utf-8 -*-

from odoo import http
from odoo.http import request


class AlertDashboard(http.Controller):
    """Controller for Alert dashboard: returns stored alert statistics (no computation)."""

    @http.route("/alert/dashboard/stats", type="json", auth="user")
    def get_stats(self):
        """
        Return all alert statistics for the dashboard. Dynamic: one card per statistic,
        like Compliance. Values come from DB (updated by Compute or run_alert_statistics
        script). When users open the dashboard they see the full set of cards automatically.
        """
        try:
            records = request.env["alert.stat"].search([
                ("active", "=", True),
            ], order="id")
            data = []
            for r in records:
                data.append({
                    "id": r.id,
                    "name": r.name or "",
                    "code": r.code or "",
                    "scope": r.scope or "alert",
                    "val": r.val or "0",
                    "query": (r.sql_query or "").strip() or None,
                })
            return {"data": data, "total": len(data)}
        except Exception:
            return {"data": [], "total": 0}
