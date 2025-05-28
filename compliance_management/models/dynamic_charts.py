# -*- coding: utf-8 -*-

from odoo import models, fields, api
from odoo import exceptions
import psycopg2
import re
import logging
import time

_logger = logging.getLogger(__name__)

class ResCharts(models.Model):
    """Model for managing dashboard charts."""

    _name = "res.dashboard.charts"
    _description = "Dashboard Charts"
    _sql_constraints = [
        (
            "uniq_chart_title",
            "unique(name)",
            "Chart name already exists. It must be unique!",
        ),
    ]
    _inherit = ["mail.thread", "mail.activity.mixin"]
    name = fields.Char("Chart Name", required=True, tracking=True)
    description = fields.Text("Description")
    chart_type = fields.Selection(
        [
            ("bar", "Bar Chart"),
            ("line", "Line Chart"),
            ("pie", "Pie Chart"),
            ("doughnut", "Doughnut Chart"),
            ("radar", "Radar Chart"),
            ("polarArea", "Polar Area Chart"),
        ],
        string="Chart Type",
        required=True,
        tracking=True,
    )
    query = fields.Text(
        "SQL Query",
        required=True,
        help="SQL query must return at least two columns for label and value",
        tracking=True,
    )
    color_scheme = fields.Selection(
        [
            ("default", "Default"),
            ("cool", "Cool Colors"),
            ("brown", "Brown Colors"),
            ("warm", "Warm Colors"),
            ("rainbow", "Rainbow"),
        ],
        string="Color Scheme",
        default="brown",
        tracking=True,
    )
    x_axis_field = fields.Char(
        "X-Axis Field", help="Column name to use for X-axis labels", required=True
    )
    y_axis_field = fields.Char(
        "Y-Axis Field", help="Column name to use for Y-axis values", required=True
    )
    branch_filter = fields.Boolean("Enable Branch Filter", default=True, tracking=True)
    date_field = fields.Char(
        "Date Field Name",
        help="Name of date field in query to filter by",
        required=True,
    )
    branch_field = fields.Char(
        "Branch Field Name", help="Name of branch field in query to filter by"
    )
    column = fields.Selection(
        [
            ("1", "One"),
            ("2", "Two"),
            ("3", "Three"),
            ("4", "Four"),
            ("5", "Five"),
            ("6", "Six"),
            ("7", "Seven"),
            ("8", "Eight"),
            ("9", "Nine"),
            ("10", "Ten"),
            ("11", "Eleven"),
            ("12", "Twelve"),
        ],
        string="Columns",
        default="4",
        tracking=True,
    )
    state = fields.Selection(
        [("active", "Active"), ("inactive", "Inactive")],
        default="active",
        string="State",
        tracking=True,
    )
    target_model_id = fields.Many2one(
        "ir.model",
        string="Target Model for Action",
        required=True,
        ondelete="cascade",
        help="Select the model to use for this chart's action",
    )
    target_model = fields.Char(
        related="target_model_id.model", string="Model Technical Name", store=True
    )
    domain_field_id = fields.Many2one(
        "ir.model.fields",
        string="Domain Fields",
        help="Select fields from the target model to use as domain filters",
        domain="[('model_id', '=', target_model_id)]",
    )
    domain_field = fields.Char(
        related="domain_field_id.name", string="Domain Field Name", store=True
    )
    domain_filter = fields.Char(
        string="Domain Filter", help="Domain filter for the action window"
    )
    use_materialized_view = fields.Boolean(
        string="Use Materialized View",
        default=False,
        help="If enabled, a materialized view will be created for this chart to optimize performance",
        tracking=True,
    )
    materialized_view_refresh_interval = fields.Integer(
        string="Refresh Interval (minutes)",
        default=60,
        help="How often the materialized view should be refreshed (in minutes)",
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
    materialized_view_last_refresh = fields.Datetime(
        string="Last View Refresh", readonly=True
    )

    @api.constrains("query")
    def _check_query_safety(self):
        """Validate the query for safety using an isolated transaction.

        Raises:
            ValidationError: If the query contains unsafe operations or invalid fields.
        """
        for chart in self:
            if not chart.query:
                continue
            dangerous_patterns = [
                r"\b(CREATE|DROP|ALTER|TRUNCATE)\s+(TABLE|DATABASE|INDEX|VIEW)\b",
                r"\bINSERT\s+INTO\b",
                r"\bUPDATE\s+\w+\s+SET\b",
                r"\bDELETE\s+FROM\b",
                r"pg_\w+",
                r"information_schema\.\w+",
                r"/\*.*\*/",
                r"--.*$",
            ]
            for pattern in dangerous_patterns:
                if re.search(pattern, chart.query, re.IGNORECASE):
                    raise exceptions.ValidationError(
                        "Query contains potentially unsafe operations. Pattern detected: %s"
                        % pattern
                    )
            registry = self.env.registry
            with registry.cursor() as cr:
                try:
                    original_query = chart.query.strip()
                    if original_query.endswith(";"):
                        original_query = original_query[:-1]
                    cr.execute("SET LOCAL statement_timeout = 120000;")
                    _logger.info(
                        f"Validating query for chart {chart.id}: {original_query}"
                    )
                    cr.execute(original_query)
                    results = cr.dictfetchall()
                    if results and chart.x_axis_field and chart.y_axis_field:
                        column_names = list(results[0].keys())
                        if (
                            chart.x_axis_field not in column_names
                            and "." in chart.x_axis_field
                        ):
                            _, field_name = chart.x_axis_field.split(".", 1)
                            if field_name not in column_names:
                                raise exceptions.ValidationError(
                                    f"X-Axis field '{chart.x_axis_field}' not found in query results. "
                                    f"Available fields: {', '.join(column_names)}"
                                )
                        elif chart.x_axis_field not in column_names:
                            raise exceptions.ValidationError(
                                f"X-Axis field '{chart.x_axis_field}' not found in query results. "
                                f"Available fields: {', '.join(column_names)}"
                            )
                        if (
                            chart.y_axis_field not in column_names
                            and "." in chart.y_axis_field
                        ):
                            _, field_name = chart.y_axis_field.split(".", 1)
                            if field_name not in column_names:
                                raise exceptions.ValidationError(
                                    f"Y-Axis field '{chart.y_axis_field}' not found in query results. "
                                    f"Available fields: {', '.join(column_names)}"
                                )
                        elif chart.y_axis_field not in column_names:
                            raise exceptions.ValidationError(
                                f"Y-Axis field '{chart.y_axis_field}' not found in query results. "
                                f"Available fields: {', '.join(column_names)}"
                            )
                    cr.execute("RESET statement_timeout")
                except psycopg2.errors.SyntaxError as e:
                    cr.rollback()
                    raise exceptions.ValidationError(f"SQL syntax error: {e}")
                except psycopg2.errors.UndefinedColumn as e:
                    cr.rollback()
                    raise exceptions.ValidationError(f"Undefined column error: {e}")
                except psycopg2.errors.QueryCanceled as e:
                    cr.rollback()
                    raise exceptions.ValidationError(
                        f"Query execution timed out after 120 seconds. Please optimize your query or "
                        f"use a materialized view for better performance."
                    )
                except psycopg2.Error as e:
                    cr.rollback()
                    raise exceptions.ValidationError(f"Database error: {e}")
                except Exception as e:
                    cr.rollback()
                    raise exceptions.ValidationError(f"Error validating query: {e}")

    def action_test_query(self):
        """Test the SQL query and display results with robust error handling.

        Returns:
            dict: Action dictionary to display the result notification.
        """
        self.ensure_one()
        registry = self.env.registry
        with registry.cursor() as cr:
            try:
                original_query = self.query.strip()
                if not original_query:
                    raise exceptions.ValidationError("Invalid query")
                if original_query.endswith(";"):
                    original_query = original_query[:-1]
                start_time = time.time()
                cr.execute("SET LOCAL statement_timeout = 120000;")
                _logger.info(
                    f"Executing test query for chart {self.id}: {original_query}"
                )
                cr.execute(original_query)
                results = cr.dictfetchall()
                execution_time = (time.time() - start_time) * 1000
                with registry.cursor() as write_cr:
                    env = api.Environment(write_cr, self.env.uid, self.env.context)
                    chart = env["res.dashboard.charts"].browse(self.id)
                    if chart.exists():
                        chart.write(
                            {
                                "last_execution_time": execution_time,
                                "last_execution_status": "success",
                                "last_error_message": False,
                            }
                        )
                        write_cr.commit()
                preview_results = results[:10] if results else []
                fields = list(preview_results[0].keys()) if preview_results else []
                if preview_results:
                    message = (
                        f"Query executed successfully in {execution_time:.2f} ms.<br/>"
                    )
                    message += f"Total rows: {len(results)}<br/>"
                    message += f"Fields: {', '.join(fields)}<br/><br/>"
                    message += "Preview (first 10 rows):<br/>"
                    message += "<table class='table table-sm'><thead><tr>"
                    for field in fields:
                        message += f"<th>{field}</th>"
                    message += "</tr></thead><tbody>"
                    for row in preview_results:
                        message += "<tr>"
                        for field in fields:
                            message += f"<td>{row[field]}</td>"
                        message += "</tr>"
                    message += "</tbody></table>"
                else:
                    message = "Query executed successfully but returned no results."
                return {
                    "type": "ir.actions.client",
                    "tag": "display_notification",
                    "params": {
                        "title": "Query Result",
                        "message": message,
                        "type": "success",
                        "sticky": True,
                    },
                }
            except Exception as e:
                try:
                    cr.execute("RESET statement_timeout")
                    cr.rollback()
                except:
                    pass
                _logger.error(
                    f"Error executing test query for chart {self.id}: {str(e)}"
                )
                try:
                    with registry.cursor() as write_cr:
                        env = api.Environment(write_cr, self.env.uid, self.env.context)
                        chart = env["res.dashboard.charts"].browse(self.id)
                        if chart.exists():
                            chart.write(
                                {
                                    "last_execution_status": "error",
                                    "last_error_message": str(e),
                                }
                            )
                            write_cr.commit()
                except Exception as write_err:
                    _logger.error(
                        f"Failed to update chart error status: {str(write_err)}"
                    )
                return {
                    "type": "ir.actions.client",
                    "tag": "display_notification",
                    "params": {
                        "title": "Error",
                        "message": str(e),
                        "type": "danger",
                        "sticky": True,
                    },
                }

    @api.model
    def create(self, vals):
        """Create a new chart record and optionally create its materialized view.

        Args:
            vals (dict): The values for the new chart record.

        Returns:
            record: The created chart record.
        """
        record = super(ResCharts, self).create(vals)
        if record.use_materialized_view:
            self.env["dashboard.chart.view.refresher"].with_context(
                bypass_validation=True
            ).create_materialized_view_for_chart(record.id)
        return record

    def write(self, vals):
        """Update an existing chart record and manage materialized view creation.

        Args:
            vals (dict): The values to update.

        Returns:
            bool: True if the update was successful, False otherwise.
        """
        result = super(ResCharts, self).write(vals)
        if "query" in vals or "use_materialized_view" in vals:
            for record in self:
                if record.use_materialized_view:
                    self.env["dashboard.chart.view.refresher"].with_context(
                        bypass_validation=True
                    ).create_materialized_view_for_chart(record.id)
                elif (
                    "use_materialized_view" in vals and not record.use_materialized_view
                ):
                    self.env[
                        "dashboard.chart.view.refresher"
                    ].drop_materialized_view_for_chart(record.id)
        return result

    def unlink(self):
        """Delete the chart record and drop its associated materialized view if it exists.

        Returns:
            bool: True if the deletion was successful, False otherwise.
        """
        for record in self:
            if record.use_materialized_view:
                self.env[
                    "dashboard.chart.view.refresher"
                ].drop_materialized_view_for_chart(record.id)
        return super(ResCharts, self).unlink()

    def action_refresh_materialized_view(self):
        """Manually refresh the materialized view for this chart.

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
                    "message": "Materialized view is not enabled for this chart.",
                    "type": "danger",
                    "sticky": False,
                },
            }
        success = self.env["dashboard.chart.view.refresher"].refresh_chart_view(self.id)
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
