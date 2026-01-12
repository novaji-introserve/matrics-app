# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import re
import logging
import sqlparse
from sqlparse import sql, tokens as T, keywords as K

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
        readonly=True
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
            'inet_server_addr', 'inet_server_port', 'pg_postmaster_start_time'
        ]
        
        dangerous_keywords = [
            'drop', 'delete', 'insert', 'update', 'alter', 'create',
            'truncate', 'grant', 'revoke', 'execute', 'exec', 'xp_',
            'sp_', 'declare', 'cursor', 'procedure', 'function'
        ]
        
        # First check for CASE WHEN constructs in the entire query
        query_text = str(parsed_query).lower()
        if re.search(r'\bcase\b.*\bwhen\b', query_text, re.DOTALL):
            raise ValidationError(
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
                    import re
                    pattern = r'\b' + re.escape(func) + r'\b'
                    if re.search(pattern, token_value):
                        raise ValidationError(
                            f"Dangerous function '{func}' detected in SQL query. "
                            f"This function is not allowed for security reasons."
                        )
                
                # Check for dangerous keywords
                for keyword in dangerous_keywords:
                    if token_value.startswith(keyword) and (
                        len(token_value) == len(keyword) or 
                        not token_value[len(keyword)].isalnum()
                    ):
                        raise ValidationError(
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
                        raise ValidationError(
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
        # _logger.info(f"SQL Query Attempt - {user_info} from {remote_addr}")
        # _logger.info(f"Query Content: {sql_query[:200]}{'...' if len(sql_query) > 200 else ''}")
            
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
                _logger.warning(f"SQL Parse Error - {user_info} from {remote_addr}")
                _logger.warning(f"Parse Error: {str(parse_error)}")
                _logger.warning(f"Malformed Query: {sql_query}")
                raise ValidationError(
                    f"Unable to parse SQL statement: {str(parse_error)}\n"
                    f"Please check your SQL syntax."
                )
            
            if not parsed_statements:
                _logger.warning(f"Empty SQL Statement - {user_info} from {remote_addr}")
                raise ValidationError("Empty SQL statement provided.")
            
            if len(parsed_statements) > 1:
                # Log multiple statement injection attempts
                _logger.warning(f"SQL INJECTION ATTEMPT - Multiple Statements - {user_info} from {remote_addr}")
                _logger.warning(f"SECURITY ALERT: Found {len(parsed_statements)} statements in query")
                _logger.warning(f"Suspicious Query: {sql_query}")
                raise ValidationError(
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
                _logger.warning(f"SQL INJECTION ATTEMPT - Non-SELECT Statement - {user_info} from {remote_addr}")
                _logger.warning(f"SECURITY ALERT: Attempted {str(first_token).upper()} statement")
                _logger.warning(f"Malicious Query: {sql_query}")
                raise ValidationError(
                    "Only SELECT statements are allowed. "
                    f"Found: {str(first_token)}"
                )
            
            # Perform deep security validation
            try:
                self._validate_sql_query_structure(parsed_query)
            except ValidationError as ve:
                # Log specific security violations
                _logger.warning(f"SQL INJECTION ATTEMPT - Security Violation - {user_info} from {remote_addr}")
                _logger.warning(f"SECURITY ALERT: {str(ve)}")
                _logger.warning(f"Dangerous Query: {sql_query}")
                raise
            
            # Additional validation for res_partner table access
            query_lower = original_query.lower()
            pattern = r"\bres_partner\b"
            
            if re.search(pattern, query_lower, re.IGNORECASE):
                # Apply origin filtering for res_partner queries
                has_where = bool(re.search(r"\bwhere\b", query_lower))
                condition = (
                    " AND origin IN ('demo','test','prod');"
                    if has_where
                    else " WHERE origin IN ('demo','test','prod');"
                )
                
                # Find the right place to insert the condition
                for clause in ["group by", "order by", "limit", "offset", "having"]:
                    clause_pos = query_lower.find(" " + clause + " ")
                    if clause_pos > -1:
                        original_query = (
                            original_query[:clause_pos]
                            + condition
                            + original_query[clause_pos:]
                        )
                        break
                else:
                    original_query += condition
                
                # _logger.info(f"Applied security filter for res_partner query - {user_info}")
            
            # Log successful validation
            # _logger.info(f"SQL Query Validated Successfully - {user_info}")
            # _logger.info(f"Final Query: {original_query}")
            
            return original_query, query_lower
            
        except ValidationError as ve:
            # Security violations are already logged above, just re-raise
            raise
        except Exception as e:
            # Log unexpected errors as potential attack attempts
            _logger.error(f"UNEXPECTED SQL VALIDATION ERROR - {user_info} from {remote_addr}")
            _logger.error(f"Error: {str(e)}")
            _logger.error(f"Query: {sql_query}")
            self.env.cr.rollback()
            raise ValidationError(
                f"SQL validation failed: {str(e)}\n"
                f"Please contact your system administrator."
            )

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

        result = super(Statistic, self).write(vals)

        # Fields that should trigger materialized view refresh when changed
        refresh_trigger_fields = ['sql_query', 'use_materialized_view', 'state']

        # Check if any of the trigger fields were updated
        if any(field in vals for field in refresh_trigger_fields):
            for record in self:
                # Only refresh if materialized view is enabled and stat is active
                if record.use_materialized_view and record.state == 'active':
                    try:
                        refresher = self.env["dashboard.stats.view.refresher"].sudo()
                        # If materialized view doesn't exist yet, create it
                        refresher.create_materialized_view_for_stat(record.id)
                        # Then refresh it to get latest data
                        refresh_success = refresher.refresh_stat_view(record.id)

                        if refresh_success:
                            # Invalidate cache to ensure UI shows updated value on next access (using new API)
                            record.invalidate_recordset(['val'])
                            _logger.info(f"Automatically refreshed materialized view for stat {record.name} (ID: {record.id})")
                        else:
                            _logger.warning(f"Materialized view refresh returned False for stat {record.name}")
                    except Exception as e:
                        _logger.warning(f"Failed to auto-refresh materialized view for stat {record.name}: {e}")

        return result

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
        from ..services.materialized_view import MaterializedViewService

        self.ensure_one()
        if not self.use_materialized_view:
            return None
        sanitized_code = MaterializedViewService.sanitize_view_name(self.code)
        view_name = f"stat_view_{sanitized_code}"
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
        from ..services.materialized_view import MaterializedViewService

        refresher = self.env["dashboard.stats.view.refresher"].sudo()
        success = refresher.refresh_stat_view(self.id)
        if success:
            # Query the materialized view directly to get the fresh value
            sanitized_code = MaterializedViewService.sanitize_view_name(self.code)
            view_name = f"stat_view_{sanitized_code}"

            try:
                # Fetch value directly from materialized view
                self.env.cr.execute(f"SELECT * FROM {view_name} LIMIT 1")
                result = self.env.cr.fetchone()

                if result is not None:
                    # Format the value
                    if len(result) == 1:
                        raw_value = result[0]
                    else:
                        raw_value = 1

                    if isinstance(raw_value, (int, float)):
                        new_value = "{:,}".format(raw_value)
                    else:
                        new_value = str(raw_value) if raw_value is not None else "0"
                else:
                    new_value = "0"

                # Update the val field directly in the database
                self.env.cr.execute(
                    """
                    UPDATE res_compliance_stat
                    SET val = %s
                    WHERE id = %s
                    """,
                    (new_value, self.id)
                )

                # Invalidate cache to ensure fresh data is loaded
                self.invalidate_recordset(['val'])

                # Return action with reload to refresh the form view
                return {
                    "type": "ir.actions.client",
                    "tag": "display_notification",
                    "params": {
                        "title": "Success",
                        "message": "Materialized view refreshed.",
                        "type": "success",
                        "sticky": False,
                        "next": {
                            "type": "ir.actions.act_window_close",
                        }
                    },
                }
            except Exception as e:
                _logger.error(f"Error fetching value from materialized view: {e}")
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
            