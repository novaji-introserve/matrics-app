# -*- coding: utf-8 -*-

import logging
from odoo import models, fields, api

_logger = logging.getLogger(__name__)


class AlertStat(models.Model):
    """Model for Alert Statistics (no materialized view)."""

    _name = "alert.stat"
    _description = "Alert Statistics"
    _order = "id"

    _sql_constraints = [
        (
            "uniq_alert_stat_code",
            "unique(code)",
            "Statistics code already exists. Value must be unique!",
        ),
    ]

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    scope = fields.Selection(
        string="Scope",
        selection=[
            ("bank", "Bank Wide"),
            ("branch", "Branch"),
            ("alert", "Alert"),
        ],
        default="branch",
    )
    sql_query = fields.Text(
        string="SQL Query",
        help="Only SELECT queries are allowed. Example: SELECT COUNT(*) FROM alert_history",
    )
    val = fields.Char(
        string="Value",
        readonly=True,
        help="Computed value - updated when you click the Compute button",
    )
    narration = fields.Text(string="Internal Notes")
    active = fields.Boolean(default=True)
    last_execution_time = fields.Float(
        string="Last Execution Time (ms)",
        readonly=True,
    )
    last_execution_status = fields.Selection(
        selection=[("success", "Success"), ("error", "Error")],
        string="Last Execution Status",
        readonly=True,
    )
    last_error_message = fields.Text(string="Last Error Message", readonly=True)

    @api.onchange("sql_query")
    def _onchange_sql_query(self):
        """
        Update only the Value field when the user types or changes the SQL query,
        like Compliance Statistics. Does not touch last execution status.
        Value is not persisted until you click Save (saves the form) then Compute (adds value to the list).
        """
        for record in self:
            if not record.sql_query or not record.sql_query.strip():
                record.val = ""
                continue
            try:
                val_str, _status, _error_msg, _ = record._compute_one(record)
                record.val = val_str
            except Exception:
                record.val = "0"

    def _format_value(self, value):
        """Format numeric value for display (e.g. with thousands separator)."""
        if value is None:
            return "0"
        if isinstance(value, (int, float)):
            if isinstance(value, float) and value == int(value):
                return "{:,.0f}".format(value)
            if isinstance(value, int):
                return "{:,}".format(value)
            return "{:,}".format(value)
        return str(value)

    def _compute_one(self, record):
        """
        Run the SQL for one record, return (value_str, status, error_msg, exec_time_ms).
        Uses compliance SecurityService for safe execution when available.
        """
        import time
        cr = self.env.cr
        if not record.sql_query or not record.sql_query.strip():
            return ("0", "error", "No SQL query defined", 0.0)
        query = record.sql_query.strip()
        if query.endswith(";"):
            query = query[:-1].strip()
        start = time.time()
        success = False
        results = None
        error_msg = None
        try:
            from odoo.addons.compliance_management.services.security_service import SecurityService
            success, results, error_msg = SecurityService.secure_execute_query(cr, query, timeout=30000)
        except Exception as e:
            _logger.warning("Alert stat SecurityService not available: %s", e)
            try:
                cr.rollback()
            except Exception:
                pass
            if not query.upper().strip().startswith("SELECT"):
                return ("0", "error", "Only SELECT queries are allowed", 0.0)
            try:
                cr.execute("SET LOCAL statement_timeout = 30000")
                cr.execute(query)
                results = cr.fetchall()
                success, error_msg = True, None
            except Exception as ex:
                try:
                    cr.rollback()
                except Exception:
                    pass
                return ("0", "error", str(ex)[:500], (time.time() - start) * 1000)
        elapsed_ms = (time.time() - start) * 1000
        if not success:
            try:
                cr.rollback()
            except Exception:
                pass
            return ("0", "error", error_msg or "Execution failed", elapsed_ms)
        if not results:
            return ("0", "success", None, elapsed_ms)
        cell = results[0][0] if results[0] else 0
        return (self._format_value(cell), "success", None, elapsed_ms)

    def action_compute(self):
        """Alias for compute_statistics (for backward compatibility)."""
        return self.compute_statistics()

    def compute_statistics(self):
        """
        Compute values for selected (or all) statistics: run SQL, save to val.
        Persists to DB like the Save button so the Alert dashboard can show values.
        """
        records = self if self else self.search([("active", "=", True)])
        # Only compute records that are already saved (have an id); skip new form records
        records = records.filtered(lambda r: r.id)
        if not records and self:
            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": "Save first",
                    "message": "Please save the record first, then click Compute.",
                    "type": "warning",
                    "sticky": False,
                },
            }
        success_count = 0
        error_count = 0
        for record in records:
            val_str, status, error_msg, exec_time = self._compute_one(record)
            record.write({
                "val": val_str,
                "last_execution_status": status,
                "last_error_message": error_msg or "",
                "last_execution_time": exec_time,
            })
            if status == "success":
                success_count += 1
            else:
                error_count += 1

        message = "Computed %s statistic(s)." % success_count
        if error_count:
            message += " %s failed." % error_count
        notification = {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": "Compute finished",
                "message": message,
                "type": "success" if error_count == 0 else "warning",
                "sticky": False,
            },
        }
        # When Compute was clicked from the form (single record), reopen the form so
        # Value and status fields refresh immediately without leaving or reloading the page.
        if len(records) == 1:
            return {
                "type": "ir.actions.act_window",
                "res_model": "alert.stat",
                "res_id": records.id,
                "view_mode": "form",
                "views": [(False, "form")],
                "target": "current",
                "context": {"form_view_refresh": True},
            }
        return notification

    @api.model
    def run_queries_and_store_results(self):
        """
        Run all active alert statistics queries and store results (script entry point).
        Same idea as Rabba's statistics_sql_settings.run_queries_and_store_results:
        when this runs, all values are updated so the Alert UI dashboard can load them.
        Call via XML-RPC or Odoo shell for cron/script use.
        """
        records = self.search([
            ("active", "=", True),
            ("sql_query", "!=", False),
            ("sql_query", "!=", ""),
        ])
        success_count = 0
        error_count = 0
        for record in records:
            try:
                _logger.info("Executing alert stat: %s (%s)", record.name, record.code)
                val_str, status, error_msg, exec_time = self._compute_one(record)
                record.write({
                    "val": val_str,
                    "last_execution_status": status,
                    "last_error_message": error_msg or "",
                    "last_execution_time": exec_time,
                })
                if status == "success":
                    success_count += 1
                else:
                    error_count += 1
                    if self.env.get("ir.logging"):
                        try:
                            self.env["ir.logging"].create({
                                "name": "Alert Statistics SQL Error",
                                "type": "server",
                                "dbname": self.env.cr.dbname,
                                "level": "ERROR",
                                "message": error_msg or "Unknown error",
                                "path": "alert.stat",
                                "line": "0",
                                "func": "run_queries_and_store_results",
                            })
                        except Exception:
                            pass
            except Exception as e:
                error_count += 1
                _logger.exception("Alert stat %s (%s) failed: %s", record.name, record.code, e)
                if self.env.get("ir.logging"):
                    try:
                        self.env["ir.logging"].create({
                            "name": "Alert Statistics SQL Error",
                            "type": "server",
                            "dbname": self.env.cr.dbname,
                            "level": "ERROR",
                            "message": str(e),
                            "path": "alert.stat",
                            "line": "0",
                            "func": "run_queries_and_store_results",
                        })
                    except Exception:
                        pass
        _logger.info("run_queries_and_store_results: computed=%s, errors=%s", success_count, error_count)
        return {"computed": success_count, "errors": error_count}
