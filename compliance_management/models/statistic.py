# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import re
from datetime import datetime, timedelta
import logging

_logger = logging.getLogger(__name__)

class Statistic(models.Model):
    """Model for managing compliance statistics."""

    _name = "res.compliance.stat"
    _description = "Compliance Statistics"
    _sql_constraints = [
        (
            "uniq_stats_code",
            "unique(code)",
            "Stats code already exists. Value must be unique!",
        ),
        (
            "uniq_stats_name",
            "unique(name)",
            "Name already exists. Value must be unique!",
        ),
    ]
    _inherit = ["mail.thread", "mail.activity.mixin"]
    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    sql_query = fields.Text(string="SQL Query", required=True)
    scope = fields.Selection(
        string="Scope",
        selection=[
            ("bank", "Bank Wide"),
            ("branch", "Branch"),
            ("compliance", "Compliance"),
            ("regulatory", "Regulatory"),
            ("risk", "Risk Assessment"),
        ],
        default="bank",
    )
    state = fields.Selection(
        string="State",
        selection=[("active", "Active"), ("inactive", "Inactive")],
        default="active",
    )
    val = fields.Char(string="Value", compute="_compute_val", store=True, readonly=True)
    narration = fields.Text(string="Narration")
    scope_color = fields.Char()
    use_materialized_view = fields.Boolean(
        string="Use Materialized View",
        default=False,
        help="If enabled, a materialized view will be created for this statistic to optimize performance",
        tracking=True,
    )
    materialized_view_refresh_interval = fields.Integer(
        string="Refresh Interval (minutes)",
        default=60,
        help="How often the materialized view should be refreshed (in minutes)",
    )
    materialized_view_last_refresh = fields.Datetime(
        string="Last View Refresh", readonly=True
    )
    last_execution_time = fields.Float(
        string="Last Execution Time (ms)",
        readonly=True,
        help="Time taken to execute this query the last time it ran",
    )
    last_execution_status = fields.Selection(
        [("success", "Success"), ("error", "Error")],
        string="Last Execution Status",
        readonly=True,
    )
    last_error_message = fields.Text(string="Last Error Message", readonly=True)

    def _prepare_and_validate_query(self, sql_query):
        """Prepare and validate SQL query, ensuring it meets safety requirements.
        Args:
            sql_query (str): The SQL query to validate.
        Returns:
            tuple: The original and processed query.
        Raises:
            ValidationError: If the query is invalid or contains unsafe operations.
        """
        if not sql_query:
            return None
        pattern = r"\bres_partner\b"
        try:
            original_query = sql_query.strip()
            query = original_query.lower()
            if not query.startswith("select"):
                raise ValidationError("Query not supported.\nHint: Start with SELECT")
            if re.search(pattern, query, re.IGNORECASE):
                if query.endswith(";"):
                    query = query[:-1]
                    original_query = original_query[:-1]
                has_where = bool(re.search(r"\bwhere\b", query))
                condition = (
                    " AND origin IN ('demo','test','prod')"
                    if has_where
                    else " WHERE origin IN ('demo','test','prod')"
                )
                for clause in ["group by", "order by", "limit", "offset", "having"]:
                    clause_pos = query.find(" " + clause + " ")
                    if clause_pos > -1:
                        original_query = (
                            original_query[:clause_pos]
                            + condition
                            + original_query[clause_pos:]
                        )
                        break
                else:
                    original_query += condition
            return original_query, query
        except Exception as e:
            self.env.cr.rollback()
            raise ValidationError(f"Invalid SQL query:\n{str(e)}")

    def _execute_query_and_get_value(self, original_query, query):
        """Execute the SQL query and return the calculated value.
        Args:
            original_query (str): The original SQL query to execute.
            query (str): The processed SQL query.
        Returns:
            str: The calculated value from the query execution.
        """
        self.env.cr.execute(original_query)
        aggregate_functions = ["count", "sum", "avg", "max", "min", "round"]
        pattern = r"\b(" + "|".join(aggregate_functions) + r")\s*\("
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            result = self.env.cr.fetchone()
            return str(result[0]) if result and result[0] is not None else "0"
        else:
            records = self.env.cr.fetchall()
            return str(len(records)) if records else "0"

    @api.model
    def create(self, vals):
        """Create a new statistic record, validating the SQL query.
        Args:
            vals (dict): The values for the new statistic record.
        Returns:
            record: The created statistic record.
        Raises:
            ValidationError: If the SQL query is invalid.
        """
        sql_query = vals.get("sql_query")
        if sql_query:
            try:
                original_query, query = self._prepare_and_validate_query(sql_query)
                self._execute_query_and_get_value(original_query, query)
            except Exception as e:
                raise ValidationError(f"Invalid SQL query:\n{str(e)}")
        return super(Statistic, self).create(vals)

    def write(self, vals):
        """Update an existing statistic record, validating the SQL query if changed.
        Args:
            vals (dict): The values to update.
        Returns:
            bool: True if the update was successful, False otherwise.
        Raises:
            ValidationError: If the SQL query is invalid.
        """
        if "sql_query" in vals:
            sql_query = vals["sql_query"]
            try:
                original_query, query = self._prepare_and_validate_query(sql_query)
                self._execute_query_and_get_value(original_query, query)
            except Exception as e:
                raise ValidationError(f"Invalid SQL query:\n{str(e)}")
        return super(Statistic, self).write(vals)

    @api.depends("sql_query", "use_materialized_view")
    def _compute_val(self):
        """Compute the value based on the SQL query or materialized view.
        This method evaluates the current SQL query or retrieves the value from
        the materialized view if enabled.
        """
        for record in self:
            if not record.sql_query:
                record.val = "0"
                continue
            if record.use_materialized_view:
                view_value = record.get_value_from_materialized_view()
                if view_value is not None:
                    record.val = view_value
                    continue
            try:
                original_query, query = record._prepare_and_validate_query(
                    record.sql_query
                )
                if original_query:
                    record.val = record._execute_query_and_get_value(
                        original_query, query
                    )
            except Exception as e:
                record.val = "Error"
                _logger.error(f"Error computing value for stat {record.name}: {str(e)}")

    @api.onchange("sql_query")
    def _onchange_sql_query(self):
        """Trigger value computation when the SQL query is changed."""
        self._compute_val()

    def get_value_from_materialized_view(self):
        """Retrieve the value from the materialized view if it exists and is enabled.
        Returns:
            str: The value from the materialized view or None if not available.
        """
        self.ensure_one()
        if not self.use_materialized_view:
            return None
        view_name = f"stat_view_{self.id}"
        self.env.cr.execute(
            """
            SELECT EXISTS (
                SELECT FROM pg_catalog.pg_class c
                WHERE c.relname = %s AND c.relkind = 'm'
            )
        """,
            (view_name,),
        )
        view_exists = self.env.cr.fetchone()[0]
        if not view_exists:
            refresher = self.env["dashboard.stats.view.refresher"].sudo()
            if not refresher.create_materialized_view_for_stat(self.id):
                return None
        try:
            with self.env.registry.cursor() as cr:
                cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                cr.execute(f"SELECT * FROM {view_name} LIMIT 1")
                result = cr.fetchone()
                if result is None:
                    return "0"
                if len(result) == 1:
                    value = result[0]
                else:
                    value = 1
                if isinstance(value, (int, float)):
                    return "{:,}".format(value)
                else:
                    return str(value) if value is not None else "0"
        except Exception as e:
            _logger.error(
                f"Error querying materialized view for statistic {self.id}: {e}"
            )
            return None

    def refresh_materialized_view(self):
        """Manually refresh the materialized view for this statistic.
        Returns:
            dict: Action dictionary to display the result notification.
        """
        self.ensure_one()
        if not self.use_materialized_view:
            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": "Error",
                    "message": "Materialized view is not enabled for this statistic.",
                    "type": "danger",
                    "sticky": False,
                },
            }
        refresher = self.env["dashboard.stats.view.refresher"].sudo()
        success = refresher.refresh_stat_view(self.id)
        if success:
            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": "Success",
                    "message": "Materialized view refreshed successfully.",
                    "type": "success",
                    "sticky": False,
                },
            }
        else:
            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": "Error",
                    "message": "Failed to refresh materialized view.",
                    "type": "danger",
                    "sticky": False,
                },
            }
