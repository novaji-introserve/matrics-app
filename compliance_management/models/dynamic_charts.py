# -*- coding: utf-8 -*-

from odoo import models, fields, api
from odoo import exceptions
import psycopg2
import re
import logging
import time
import sqlparse
from ..services.security_service import SecurityService

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
        readonly=True
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
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')

    def _validate_sql_query_structure(self, parsed_query):
        """Validate the structure of a parsed SQL query to prevent injection attacks.
        
        Args:
            parsed_query: A sqlparse.sql.Statement object
            
        Raises:
            ValidationError: If the query contains dangerous constructs
        """
        dangerous_functions = [
            'pg_sleep', 'sleep', 'waitfor', 'delay', 'benchmark',
            'current_database', 'version', 'user', 'current_user',
            'session_user', 'system_user', 'pg_backend_pid',
            'inet_server_addr', 'inet_server_port', 'pg_postmaster_start_time',
            'extractvalue', 'updatexml', 'load_file', 'into_outfile',
            'xp_cmdshell', 'sp_executesql', 'openrowset', 'opendatasource'
        ]
        
        dangerous_keywords = [
            'drop', 'delete', 'insert', 'update', 'alter', 'create',
            'truncate', 'grant', 'revoke', 'execute', 'exec', 'xp_',
            'sp_', 'declare', 'cursor', 'procedure', 'function',
            'backup', 'restore', 'dump'
        ]
        
        # First check for CASE WHEN constructs in the entire query
        query_text = str(parsed_query).lower()
        if re.search(r'\bcase\b.*\bwhen\b', query_text, re.DOTALL):
            raise exceptions.ValidationError(
                "CASE WHEN statements are not allowed for security reasons. "
                "Please use simpler SELECT queries."
            )
        
        def check_token_recursively(token):
            """Recursively check all tokens in the SQL statement."""
            if hasattr(token, 'tokens'):
                for sub_token in token.tokens:
                    check_token_recursively(sub_token)
            else:
                token_value = str(token).lower().strip()
                if not token_value:
                    return
                    
                # Check for dangerous functions (with word boundaries to avoid false positives)
                for func in dangerous_functions:
                    # Use word boundary matching to avoid blocking legitimate table names
                    pattern = r'\b' + re.escape(func) + r'\b'
                    if re.search(pattern, token_value):
                        raise exceptions.ValidationError(
                            f"Dangerous function '{func}' detected in SQL query. "
                            f"This function is not allowed for security reasons."
                        )
                
                # Check for dangerous keywords
                for keyword in dangerous_keywords:
                    if token_value.startswith(keyword) and (
                        len(token_value) == len(keyword) or 
                        not token_value[len(keyword)].isalnum()
                    ):
                        raise exceptions.ValidationError(
                            f"Dangerous SQL keyword '{keyword}' detected. "
                            f"Only SELECT statements are allowed."
                        )
                
                # Check for time-based functions and constructs
                time_patterns = [
                    'pg_sleep', 'sleep', 'waitfor', 'delay', 'benchmark',
                    'extract(epoch', 'now()', 'current_timestamp'
                ]
                for pattern in time_patterns:
                    if pattern in token_value:
                        raise exceptions.ValidationError(
                            f"Time-based function '{pattern}' detected. "
                            f"These functions are not allowed to prevent timing attacks."
                        )
        
        check_token_recursively(parsed_query)

    def _prepare_and_validate_query(self, sql_query):
        """Prepare and validate SQL query using sqlparse for comprehensive security.
        
        Args:
            sql_query (str): The SQL query to validate.
            
        Returns:
            tuple: The original and processed query.
            
        Raises:
            ValidationError: If the query is invalid or contains unsafe operations.
        """
        if not sql_query:
            return None
            
        # Get current user info for security logging
        current_user = self.env.user
        user_info = f"User: {current_user.name} (ID: {current_user.id}, Login: {current_user.login})"
        remote_addr = self.env.context.get('remote_addr', 'Unknown IP')
        
        # Log all SQL query attempts for security auditing
        _logger.info(f"Chart SQL Query Attempt - {user_info} from {remote_addr}")
        _logger.info(f"Query Content: {sql_query[:200]}{'...' if len(sql_query) > 200 else ''}")
            
        try:
            # Clean and normalize the query
            original_query = sql_query.strip()
            
            # Remove trailing semicolons
            if original_query.endswith(";"):
                original_query = original_query[:-1].strip()
            
            # Parse the SQL query using sqlparse
            try:
                parsed_statements = sqlparse.parse(original_query)
            except Exception as parse_error:
                # Log parsing errors as potential obfuscation attempts
                _logger.warning(f"Chart SQL Parse Error - {user_info} from {remote_addr}")
                _logger.warning(f"Parse Error: {str(parse_error)}")
                _logger.warning(f"Malformed Query: {sql_query}")
                raise exceptions.ValidationError(
                    f"Unable to parse SQL statement: {str(parse_error)}\n"
                    f"Please check your SQL syntax."
                )
            
            if not parsed_statements:
                _logger.warning(f"Empty Chart SQL Statement - {user_info} from {remote_addr}")
                raise exceptions.ValidationError("Empty SQL statement provided.")
            
            if len(parsed_statements) > 1:
                # Log multiple statement injection attempts
                _logger.warning(f"CHART SQL INJECTION ATTEMPT - Multiple Statements - {user_info} from {remote_addr}")
                _logger.warning(f"SECURITY ALERT: Found {len(parsed_statements)} statements in query")
                _logger.warning(f"Suspicious Query: {sql_query}")
                raise exceptions.ValidationError(
                    "Multiple SQL statements detected. "
                    "Only single SELECT statements are allowed."
                )
            
            parsed_query = parsed_statements[0]
            
            # Validate that it's a SELECT statement
            first_token = None
            for token in parsed_query.tokens:
                if not token.is_whitespace:
                    first_token = token
                    break
            
            if not first_token or str(first_token).upper().strip() != 'SELECT':
                # Log non-SELECT statement attempts
                _logger.warning(f"CHART SQL INJECTION ATTEMPT - Non-SELECT Statement - {user_info} from {remote_addr}")
                _logger.warning(f"SECURITY ALERT: Attempted {str(first_token).upper()} statement")
                _logger.warning(f"Malicious Query: {sql_query}")
                raise exceptions.ValidationError(
                    "Only SELECT statements are allowed. "
                    f"Found: {str(first_token)}"
                )
            
            # Perform deep security validation
            try:
                self._validate_sql_query_structure(parsed_query)
            except exceptions.ValidationError as ve:
                # Log specific security violations
                _logger.warning(f"CHART SQL INJECTION ATTEMPT - Security Violation - {user_info} from {remote_addr}")
                _logger.warning(f"SECURITY ALERT: {str(ve)}")
                _logger.warning(f"Dangerous Query: {sql_query}")
                raise
            
            # Log successful validation
            _logger.info(f"Chart SQL Query Validated Successfully - {user_info}")
            _logger.info(f"Final Query: {original_query}")
            
            return original_query, original_query.lower()
            
        except exceptions.ValidationError as ve:
            # Security violations are already logged above, just re-raise
            raise
        except Exception as e:
            # Log unexpected errors as potential attack attempts
            _logger.error(f"UNEXPECTED CHART SQL VALIDATION ERROR - {user_info} from {remote_addr}")
            _logger.error(f"Error: {str(e)}")
            _logger.error(f"Query: {sql_query}")
            self.env.cr.rollback()
            raise exceptions.ValidationError(
                f"SQL validation failed: {str(e)}\n"
                f"Please contact your system administrator."
            )

    @api.constrains("query")
    def _check_query_safety(self):
        """Validate the query for safety using comprehensive validation including sqlparse.

        Raises:
            ValidationError: If the query contains unsafe operations or invalid fields.
        """
        for chart in self:
            if not chart.query:
                continue
            
            # First use comprehensive SecurityService validation
            security_service = SecurityService()
            is_safe, error_msg = security_service.validate_sql_query(chart.query)
            if not is_safe:
                security_service.log_security_event(
                    "CHART_MODEL_SQL_VALIDATION_FAILED",
                    f"Chart {chart.id} query validation failed: {error_msg} - Query: {chart.query[:200]}..."
                )
                raise exceptions.ValidationError("Query validation failed. Please check your SQL syntax and ensure it contains only safe operations.")
            
            # Then use sqlparse for deeper validation
            try:
                validated_query, query_lower = chart._prepare_and_validate_query(chart.query)
                if not validated_query:
                    raise exceptions.ValidationError("Query validation failed.")
            except exceptions.ValidationError:
                raise
            except Exception as e:
                _logger.error(f"Chart sqlparse validation error: {str(e)}")
                raise exceptions.ValidationError("Query validation failed. Please check your SQL syntax.")
            registry = self.env.registry
            with registry.cursor() as cr:
                try:
                    original_query = chart.query.strip()
                    if original_query.endswith(";"):
                        original_query = original_query[:-1]
                    
                    # Use secure query execution
                    security_service = SecurityService()
                    success, results, error_msg = security_service.secure_execute_query(
                        cr, original_query, timeout=120000
                    )
                    
                    if not success:
                        raise exceptions.ValidationError(f"Query execution failed: {error_msg}")
                        
                    # Convert results to dict format for validation
                    if results and cr.description:
                        column_names = [desc[0] for desc in cr.description]
                        dict_results = [dict(zip(column_names, row)) for row in results] if results else []
                    else:
                        dict_results = []
                    if dict_results and chart.x_axis_field and chart.y_axis_field:
                        column_names = list(dict_results[0].keys())
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
                
                # Use secure query execution for test queries
                security_service = SecurityService()
                success, raw_results, error_msg = security_service.secure_execute_query(
                    cr, original_query, timeout=120000
                )
                
                execution_time = (time.time() - start_time) * 1000
                
                if not success:
                    raise exceptions.ValidationError(f"Query execution failed: {error_msg}")
                
                # Convert results to dict format
                if raw_results and cr.description:
                    column_names = [desc[0] for desc in cr.description]
                    results = [dict(zip(column_names, row)) for row in raw_results]
                else:
                    results = []
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
            