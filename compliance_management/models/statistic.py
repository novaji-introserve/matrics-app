# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import ast
import re
import logging
import sqlparse
import time
from sqlparse import sql, tokens as T, keywords as K

_logger = logging.getLogger(__name__)
STAT_REFRESH_ADVISORY_LOCK = 972451
DEFAULT_STAT_REFRESH_TIMEOUT_MS = 5000

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

    @api.model
    def _default_resource_id(self):
        resource = self.env.ref(
            "compliance_management.res_resource_uri_customer",
            raise_if_not_found=False,
        )
        return resource.id if resource else False

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string="Code", required=True)
    display_order = fields.Integer(string="Display Order", default=1, tracking=True)
    is_visible = fields.Boolean(string="Is Visible", default=True, tracking=True)
    resource_id = fields.Many2one(
        "res.resource.uri",
        string="Resource URI",
        default=_default_resource_id,
        ondelete="set null",
        tracking=True,
    )
    domain = fields.Text(string="Domain", tracking=True)
    display_summary = fields.Text(string="Display Summary")
    sql_query = fields.Text(string="SQL Query", required=True)
    scope = fields.Selection(
        string="Scope",
        selection=[
            ("alert", "Alert Management"),
            ("bank", "Bank Wide"),
            ("branch", "Branch"),
            ("case", "Case Management"),
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
    val = fields.Char(string="Value", readonly=True, copy=False)
    narration = fields.Text(string="Narration")
    scope_color = fields.Char()
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

    def init(self):
        super().init()
        stat_resource_map = {
            "avg_cust_risk_score": "res_resource_uri_customer",
            "total_cust": "res_resource_uri_customer",
            "customers_no_bvn": "res_resource_uri_customer",
            "pep_customers": "res_resource_uri_customer",
            "global_pep": "res_resource_uri_global_pep_list",
            "watch_list": "res_resource_uri_watchlist",
            "alert_total_today": "res_resource_uri_alert_history",
            "alert_groups_total": "res_resource_uri_alert_group",
            "alert_rules_total": "res_resource_uri_alert_rules",
            "alert_sql_queries_total": "res_resource_uri_alert_sql_query",
            "case_all_cases": "res_resource_uri_case_manager",
            "case_draft_cases": "res_resource_uri_case_manager",
            "case_open_cases": "res_resource_uri_case_manager",
            "case_overdue_cases": "res_resource_uri_case_manager",
            "case_closed_cases": "res_resource_uri_case_manager",
            "case_archived_cases": "res_resource_uri_case_manager",
        }
        stat_domain_map = {
            "avg_cust_risk_score": "[('internal_category', '=', 'customer'), ('active', '=', True)]",
            "total_cust": "[('internal_category', '=', 'customer'), ('active', '=', True)]",
            "customers_no_bvn": "[('internal_category', '=', 'customer'), ('active', '=', True), '|', ('bvn', '=', False), ('bvn', 'ilike', 'NOBVN%')]",
            "pep_customers": "[('internal_category', '=', 'customer'), ('active', '=', True), ('is_pep', '=', True)]",
            "branch_tot": "[]",
            "risk_univ_cnt": "[]",
            "global_pep": "[]",
            "watch_list": "[]",
            "alert_total_today": "[]",
            "alert_groups_total": "[]",
            "alert_rules_total": "[]",
            "alert_sql_queries_total": "[]",
            "case_all_cases": "[]",
            "case_draft_cases": "[('case_status', '=', 'draft')]",
            "case_open_cases": "[('case_status', '=', 'open')]",
            "case_overdue_cases": "[('case_status', '=', 'overdue')]",
            "case_closed_cases": "[('case_status', '=', 'closed')]",
            "case_archived_cases": "[('case_status', '=', 'archived'), ('active', '=', False)]",
        }
        for stat_code, xml_id in stat_resource_map.items():
            self.env.cr.execute(
                """
                UPDATE res_compliance_stat stat
                SET resource_id = imd.res_id
                FROM ir_model_data imd
                WHERE imd.module = 'compliance_management'
                  AND imd.name = %s
                  AND stat.code = %s
                """,
                (xml_id, stat_code),
            )
        for stat_code, domain_value in stat_domain_map.items():
            self.env.cr.execute(
                """
                UPDATE res_compliance_stat
                SET domain = %s
                WHERE code = %s
                """,
                (domain_value, stat_code),
            )


    def _get_stat_refresh_config(self):
        params = self.env["ir.config_parameter"].sudo()
        timeout_ms = int(
            params.get_param(
                "compliance_management.stat_refresh_timeout_ms",
                DEFAULT_STAT_REFRESH_TIMEOUT_MS,
            )
        )
        return max(timeout_ms, 1000)

    def _acquire_stat_refresh_lock(self):
        self.env.cr.execute("SELECT pg_try_advisory_lock(%s)", (STAT_REFRESH_ADVISORY_LOCK,))
        row = self.env.cr.fetchone()
        return bool(row and row[0])

    def _release_stat_refresh_lock(self):
        try:
            self.env.cr.execute("SELECT pg_advisory_unlock(%s)", (STAT_REFRESH_ADVISORY_LOCK,))
        except Exception:
            _logger.exception("Failed to release statistic refresh advisory lock")

    def _update_stat_refresh_metadata(
        self, stat_id, *, value=None, execution_time_ms=0.0, status="success", error_message=None
    ):
        value_to_store = "0" if value is None else str(value)
        truncated_error = error_message[:1000] if error_message else None
        self.env.cr.execute(
            """
            UPDATE res_compliance_stat
            SET val = CASE WHEN COALESCE(val, '') <> %s THEN %s ELSE val END,
                last_execution_time = %s,
                last_execution_status = %s,
                last_error_message = %s,
                write_date = NOW(),
                write_uid = %s
            WHERE id = %s
            """,
            (
                value_to_store,
                value_to_store,
                execution_time_ms,
                status,
                truncated_error,
                self.env.user.id,
                stat_id,
            ),
        )

    def _compute_stat_value_for_refresh(self, record, timeout_ms):
        start = time.time()
        self.env.cr.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
        original_query, query = record._prepare_and_validate_query(record.sql_query)
        value = record._execute_query_and_get_value(original_query, query) if original_query else "0"
        execution_time_ms = (time.time() - start) * 1000
        return value, execution_time_ms

    def _compute_current_value(self, record):
        original_query, query = record._prepare_and_validate_query(record.sql_query)
        return record._execute_query_and_get_value(original_query, query) if original_query else "0"

    def _get_dashboard_action_metadata(self):
        self.ensure_one()
        model_uri = self.resource_id.model_uri if self.resource_id else False
        search_view_xmlids = {
            "res.transaction.screening.rule": "compliance_management.compliance_transaction_screening_rule_search",
            "res.compliance.risk.assessment.plan": "compliance_management.compliance_risk_assessment_plan_search",
            "res.partner.risk.plan.line": "compliance_management.view_partner_risk_plan_line_search",
        }
        search_view_id = False
        if model_uri in search_view_xmlids:
            view = self.env.ref(search_view_xmlids[model_uri], raise_if_not_found=False)
            search_view_id = view.id if view else False
        return {
            "resource_model_uri": model_uri,
            "search_view_id": search_view_id,
            "domain": self._parse_domain(),
        }

    def _parse_domain(self):
        self.ensure_one()
        if not self.domain:
            return False
        try:
            parsed_domain = ast.literal_eval(self.domain)
        except (ValueError, SyntaxError):
            _logger.warning("Invalid domain for statistic %s (%s): %s", self.name, self.code, self.domain)
            return False
        return parsed_domain if isinstance(parsed_domain, list) else False

    @api.model
    def update_stat(self, limit=None):
        if not self._acquire_stat_refresh_lock():
            _logger.info("Statistic refresh skipped because another run is already active")
            return False

        timeout_ms = self._get_stat_refresh_config()
        search_kwargs = {"order": "id"}
        if limit:
            search_kwargs["limit"] = int(limit)
        stats = self.sudo().search(
            [("state", "=", "active"), ("active", "=", True), ("is_visible", "=", True)],
            **search_kwargs,
        )

        _logger.info(
            "Starting statistic refresh run for %s records with timeout=%sms",
            len(stats),
            timeout_ms,
        )

        try:
            for record in stats:
                with self.env.cr.savepoint():
                    try:
                        value, execution_time_ms = self._compute_stat_value_for_refresh(
                            record, timeout_ms
                        )
                        self._update_stat_refresh_metadata(
                            record.id,
                            value=value,
                            execution_time_ms=execution_time_ms,
                            status="success",
                            error_message=None,
                        )
                    except Exception as exc:
                        _logger.warning(
                            "Statistic refresh failed for %s (%s): %s",
                            record.name,
                            record.code,
                            exc,
                        )
                        self._update_stat_refresh_metadata(
                            record.id,
                            value=record.val or "0",
                            execution_time_ms=0.0,
                            status="error",
                            error_message=str(exc),
                        )

            stats.invalidate_recordset(
                ["val", "last_execution_time", "last_execution_status", "last_error_message"]
            )
            return True
        finally:
            self._release_stat_refresh_lock()

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
        _logger.info(f"SQL Query Attempt - {user_info} from {remote_addr}")
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
            
            query_lower = original_query.lower()
            
            # Log successful validation
            _logger.info(f"SQL Query Validated Successfully - {user_info}")
            _logger.info(f"Final Query: {original_query}")
            
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
        computed_value = "0"
        if sql_query:
            try:
                original_query, query = self._prepare_and_validate_query(sql_query)
                computed_value = self._execute_query_and_get_value(original_query, query)
            except Exception as e:
                raise ValidationError(f"Invalid SQL query:\n{str(e)}")
        vals = dict(vals, val=computed_value)
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
                vals = dict(vals, val=self._execute_query_and_get_value(original_query, query))
            except Exception as e:
                raise ValidationError(f"Invalid SQL query:\n{str(e)}")
        return super(Statistic, self).write(vals)

    def action_run_statistic(self):
        """Execute the statistic query on demand and persist the latest value."""
        self.ensure_one()

        start = time.time()
        original_query, query = self._prepare_and_validate_query(self.sql_query)
        value = self._execute_query_and_get_value(original_query, query)
        execution_time_ms = (time.time() - start) * 1000

        super(Statistic, self).write(
            {
                "val": value,
                "last_execution_time": execution_time_ms,
                "last_execution_status": "success",
                "last_error_message": False,
            }
        )

        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": "Success",
                "message": "Statistic updated successfull",
                "type": "success",
                "sticky": False,
                "next": {"type": "ir.actions.client", "tag": "reload"},
            },
        }
            
