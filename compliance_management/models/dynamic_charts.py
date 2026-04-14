# -*- coding: utf-8 -*-

from threading import Thread

from odoo import models, fields, api, _
from odoo import exceptions
import ast
import json
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
        (
            "uniq_chart_code",
            "unique(code)",
            "Chart code already exists. It must be unique!",
        ),
    ]
    _inherit = ["mail.thread", "mail.activity.mixin"]
    name = fields.Char("Chart Name", required=True, tracking=True)
    code = fields.Char("Code", tracking=True)
    description = fields.Text("Description")
    display_summary = fields.Text("Display Summary")
    display_order = fields.Integer("Display Order", default=1, tracking=True)
    is_visible = fields.Boolean("Is Visible", default=False, tracking=True)
    refresh_mode = fields.Selection(
        [
            ("live", "Live"),
            ("scheduled", "Scheduled Cache"),
            ("manual", "Manual Cache"),
        ],
        string="Refresh Mode",
        default="live",
        tracking=True,
    )
    cache_ttl_minutes = fields.Integer(
        string="Cache TTL (Minutes)",
        default=60,
        tracking=True,
        help="How long the cached chart payload is considered fresh.",
    )
    cached_payload = fields.Text(string="Cached Payload", readonly=True, copy=False)
    cache_computed_at = fields.Datetime(string="Cache Computed At", readonly=True, copy=False)
    cache_expires_at = fields.Datetime(string="Cache Expires At", readonly=True, copy=False)
    scope = fields.Selection(
        [
            ("alert", "Alert Management"),
            ("bank", "Bank Wide"),
            ("branch", "Branch"),
            ("case", "Case Management"),
            ("compliance", "Compliance"),
            ("regulatory", "Regulatory"),
            ("risk", "Risk Assessment"),
        ],
        string="Scope",
        default="compliance",
        tracking=True,
    )
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
    navigation_filter_field = fields.Char(
        string="Navigation Filter Field",
        help="Target model field used when a chart point is clicked.",
        tracking=True,
    )
    navigation_value_field = fields.Char(
        string="Navigation Value Field",
        help="Query result column used as the clicked filter value.",
        tracking=True,
    )
    navigation_domain = fields.Text(
        string="Navigation Domain",
        help="Static Odoo domain appended during chart drill-down.",
        tracking=True,
    )
    apply_dashboard_date_filter = fields.Boolean(
        string="Apply Dashboard Date Filter",
        default=False,
        tracking=True,
    )
    navigation_date_field = fields.Char(
        string="Navigation Date Field",
        help="Target model field used for dashboard period drill-down.",
        tracking=True,
    )
    apply_dashboard_branch_filter = fields.Boolean(
        string="Apply Dashboard Branch Filter",
        default=False,
        tracking=True,
    )
    navigation_branch_field = fields.Char(
        string="Navigation Branch Field",
        help="Target model field used for dashboard branch drill-down.",
        tracking=True,
    )
    date_filter = fields.Boolean(
        string="Apply Dashboard Date Filter To Query",
        default=False,
        tracking=True,
    )
    domain_filter = fields.Char(
        string="Domain Filter", help="Domain filter for the action window"
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

    def init(self):
        super().init()
        # Do not rewrite dashboard chart records on registry init or module upgrade.
        # Chart configuration should remain user-editable after installation.

    def _parse_navigation_domain(self):
        self.ensure_one()
        if not self.navigation_domain:
            return []
        try:
            parsed_domain = ast.literal_eval(self.navigation_domain)
        except (ValueError, SyntaxError):
            _logger.warning(
                "Invalid navigation domain for chart %s (%s): %s",
                self.name,
                self.code,
                self.navigation_domain,
            )
            return []
        return parsed_domain if isinstance(parsed_domain, list) else []

    def _build_dashboard_navigation_domain(
        self, *, cco=False, branches_id=None, datepicked=7, start_at=None, end_at=None
    ):
        self.ensure_one()
        domain = list(self._parse_navigation_domain())
        if (
            self.apply_dashboard_date_filter
            and self.navigation_date_field
            and start_at
            and end_at
            and datepicked in (0, 1, 7, 30)
        ):
            domain.extend(
                [
                    [self.navigation_date_field, ">=", start_at],
                    [self.navigation_date_field, "<=", end_at],
                ]
            )
        if (
            self.apply_dashboard_branch_filter
            and self.navigation_branch_field
            and not cco
            and branches_id
        ):
            domain.append([self.navigation_branch_field, "in", branches_id])
        return domain

    def _cache_payload_key(self, *, cco=False, branches_id=None, datepicked=7):
        normalized_branches = ",".join(
            str(branch_id) for branch_id in sorted(set(branches_id or []))
        )
        return f"cco={int(bool(cco))}|period={int(datepicked)}|branches={normalized_branches}"

    def _read_cached_payload(self, *, cco=False, branches_id=None, datepicked=7):
        self.ensure_one()
        if not self.cached_payload:
            return None
        try:
            cached_payloads = json.loads(self.cached_payload)
        except (TypeError, ValueError):
            _logger.warning("Invalid cached payload for chart %s (%s)", self.name, self.code)
            return None
        if not isinstance(cached_payloads, dict):
            return None
        cache_key = self._cache_payload_key(
            cco=cco, branches_id=branches_id, datepicked=datepicked
        )
        cache_entry = cached_payloads.get(cache_key)
        if not isinstance(cache_entry, dict):
            return None
        expires_at = cache_entry.get("expires_at")
        if expires_at:
            try:
                expires_at_dt = fields.Datetime.to_datetime(expires_at)
            except Exception:
                expires_at_dt = None
            if expires_at_dt and fields.Datetime.now() > expires_at_dt:
                return None
        return cache_entry.get("payload")

    def _store_cached_payload(self, payload, *, cco=False, branches_id=None, datepicked=7):
        self.ensure_one()
        now = fields.Datetime.now()
        ttl_minutes = max(int(self.cache_ttl_minutes or 60), 1)
        try:
            cached_payloads = json.loads(self.cached_payload) if self.cached_payload else {}
        except (TypeError, ValueError):
            cached_payloads = {}
        if not isinstance(cached_payloads, dict):
            cached_payloads = {}
        cache_key = self._cache_payload_key(
            cco=cco, branches_id=branches_id, datepicked=datepicked
        )
        cached_payloads[cache_key] = {
            "payload": payload,
            "computed_at": fields.Datetime.to_string(now),
            "expires_at": fields.Datetime.to_string(
                fields.Datetime.add(now, minutes=ttl_minutes)
            ),
        }
        try:
            registry = self.env.registry
            with registry.cursor() as cr:
                cr.execute(
                    "SELECT id FROM res_dashboard_charts WHERE id = %s FOR UPDATE NOWAIT",
                    (self.id,),
                )
                cr.execute(
                    """
                    UPDATE res_dashboard_charts
                    SET cached_payload = %s,
                        cache_computed_at = %s,
                        cache_expires_at = %s,
                        write_uid = %s,
                        write_date = %s
                    WHERE id = %s
                    """,
                    (
                        json.dumps(cached_payloads),
                        now,
                        fields.Datetime.add(now, minutes=ttl_minutes),
                        self.env.uid,
                        now,
                        self.id,
                    ),
                )
                cr.commit()
        except (psycopg2.errors.LockNotAvailable, psycopg2.errors.SerializationFailure):
            _logger.info(
                "Skipping dashboard cache write for chart %s (%s) because the record is being edited.",
                self.name,
                self.code,
            )
        except psycopg2.Error as exc:
            _logger.warning(
                "Skipping dashboard cache write for chart %s (%s) due to database contention: %s",
                self.name,
                self.code,
                exc,
            )

    def _clear_cached_payload(self):
        self.ensure_one()
        now = fields.Datetime.now()
        try:
            registry = self.env.registry
            with registry.cursor() as cr:
                cr.execute(
                    "SELECT id FROM res_dashboard_charts WHERE id = %s FOR UPDATE NOWAIT",
                    (self.id,),
                )
                cr.execute(
                    """
                    UPDATE res_dashboard_charts
                    SET cached_payload = NULL,
                        cache_computed_at = NULL,
                        cache_expires_at = NULL,
                        write_uid = %s,
                        write_date = %s
                    WHERE id = %s
                    """,
                    (self.env.uid, now, self.id),
                )
                cr.commit()
        except (psycopg2.errors.LockNotAvailable, psycopg2.errors.SerializationFailure):
            _logger.info(
                "Skipping dashboard cache clear for chart %s (%s) because the record is being edited.",
                self.name,
                self.code,
            )
        except psycopg2.Error as exc:
            _logger.warning(
                "Skipping dashboard cache clear for chart %s (%s) due to database contention: %s",
                self.name,
                self.code,
                exc,
            )

    def _get_dashboard_chart_payload(
        self, chart_service, *, cco=False, branches_id=None, datepicked=7, start_at=None, end_at=None
    ):
        self.ensure_one()
        branches_id = branches_id or []
        if self.refresh_mode == "live":
            return chart_service.get_dashboard_chart_data(
                self,
                cco,
                branches_id,
                datepicked=datepicked,
                start_at=start_at,
                end_at=end_at,
            )

        cached_payload = self._read_cached_payload(
            cco=cco, branches_id=branches_id, datepicked=datepicked
        )
        if cached_payload:
            return cached_payload

        if self.refresh_mode == "manual":
            return self._build_empty_dashboard_payload(
                cco=cco,
                branches_id=branches_id,
                datepicked=datepicked,
                start_at=start_at,
                end_at=end_at,
                error=_("No cached payload is available yet. Use Run Query to generate it."),
            )

        payload = chart_service.get_dashboard_chart_data(
            self,
            cco,
            branches_id,
            datepicked=datepicked,
            start_at=start_at,
            end_at=end_at,
        )
        self._store_cached_payload(
            payload, cco=cco, branches_id=branches_id, datepicked=datepicked
        )
        return payload

    def _build_empty_dashboard_payload(
        self,
        *,
        cco=False,
        branches_id=None,
        datepicked=7,
        start_at=None,
        end_at=None,
        error=None,
    ):
        self.ensure_one()
        return {
            "id": self.code or self.id,
            "record_id": self.id,
            "title": self.name,
            "display_summary": self.display_summary or self.description or "",
            "type": self.chart_type,
            "model_name": self.target_model,
            "filter": self.navigation_filter_field or self.domain_field,
            "column": self.column,
            "labels": [],
            "ids": [],
            "datefield": self.date_field,
            "additional_domain": self._build_dashboard_navigation_domain(
                cco=cco,
                branches_id=branches_id or [],
                datepicked=datepicked,
                start_at=start_at,
                end_at=end_at,
            ),
            "datasets": [
                {
                    "label": self.name,
                    "data": [],
                    "backgroundColor": [],
                    "borderColor": [],
                    "borderWidth": 1,
                }
            ],
            "error": error or False,
        }

    @api.model
    def create(self, vals):
        return super().create(vals)

    def write(self, vals):
        cache_sensitive_fields = {
            "query",
            "x_axis_field",
            "y_axis_field",
            "chart_type",
            "color_scheme",
            "branch_filter",
            "branch_field",
            "date_filter",
            "date_field",
            "refresh_mode",
            "cache_ttl_minutes",
            "target_model_id",
            "navigation_filter_field",
            "navigation_value_field",
            "navigation_domain",
            "apply_dashboard_date_filter",
            "navigation_date_field",
            "apply_dashboard_branch_filter",
            "navigation_branch_field",
            "domain_field_id",
            "domain_filter",
            "display_summary",
            "name",
            "code",
            "column",
            "state",
            "active",
            "is_visible",
            "scope",
        }
        if cache_sensitive_fields.intersection(vals):
            vals = dict(vals)
            vals.update(
                {
                    "cached_payload": False,
                    "cache_computed_at": False,
                    "cache_expires_at": False,
                }
            )
        return super().write(vals)

    def _get_paginated_dashboard_chart_payload(
        self,
        chart_service,
        *,
        cco=False,
        branches_id=None,
        datepicked=7,
        start_at=None,
        end_at=None,
        page=0,
        page_size=50,
    ):
        self.ensure_one()
        payload = self._get_dashboard_chart_payload(
            chart_service,
            cco=cco,
            branches_id=branches_id,
            datepicked=datepicked,
            start_at=start_at,
            end_at=end_at,
        )
        labels = list(payload.get("labels", []) or [])
        ids = list(payload.get("ids", []) or [])
        datasets = list(payload.get("datasets", []) or [])
        total = len(labels)
        page = max(int(page or 0), 0)
        page_size = max(int(page_size or 50), 1)
        start_idx = page * page_size
        end_idx = start_idx + page_size

        paginated_payload = dict(payload)
        paginated_payload["labels"] = labels[start_idx:end_idx]
        if ids:
            paginated_payload["ids"] = ids[start_idx:end_idx]
        paginated_datasets = []
        for dataset in datasets:
            paginated_dataset = dict(dataset)
            data = list(dataset.get("data", []) or [])
            colors = list(dataset.get("backgroundColor", []) or [])
            border_colors = list(dataset.get("borderColor", []) or [])
            paginated_dataset["data"] = data[start_idx:end_idx]
            if colors:
                paginated_dataset["backgroundColor"] = colors[start_idx:end_idx]
            if border_colors and len(border_colors) == len(data):
                paginated_dataset["borderColor"] = border_colors[start_idx:end_idx]
            paginated_datasets.append(paginated_dataset)
        paginated_payload["datasets"] = paginated_datasets
        paginated_payload["pagination"] = {
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": ((total + page_size - 1) // page_size) if page_size > 0 else 0,
        }
        return paginated_payload

    @api.model
    def refresh_dashboard_chart_payloads(self, limit=None):
        search_kwargs = {"order": "display_order asc, id asc"}
        if limit:
            search_kwargs["limit"] = int(limit)
        charts = self.sudo().search(
            [
                ("state", "=", "active"),
                ("active", "=", True),
                ("is_visible", "=", True),
                ("refresh_mode", "in", ["scheduled", "manual"]),
            ],
            **search_kwargs,
        )
        if not charts:
            return True

        from ..services.chart_data_service import ChartDataService

        payload_service = ChartDataService(self.env)
        for chart in charts:
            for period in (0, 1, 7, 30):
                try:
                    payload = payload_service.get_dashboard_chart_data(
                        chart,
                        cco=True,
                        branches_id=[],
                        datepicked=period,
                        start_at=None,
                        end_at=None,
                    )
                    chart._store_cached_payload(
                        payload, cco=True, branches_id=[], datepicked=period
                    )
                except Exception as exc:
                    _logger.warning(
                        "Failed to refresh cached payload for chart %s (%s) period=%s: %s",
                        chart.name,
                        chart.code,
                        period,
                        exc,
                    )
        return True

    def action_refresh_dashboard_cache(self):
        from ..services.chart_data_service import ChartDataService

        payload_service = ChartDataService(self.env)
        for chart in self:
            payload = payload_service.get_dashboard_chart_data(
                chart,
                cco=True,
                branches_id=[],
                datepicked=30,
                start_at=None,
                end_at=None,
            )
            chart._store_cached_payload(
                payload, cco=True, branches_id=[], datepicked=30
            )
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": "Success",
                "message": "Chart cache refreshed successfully.",
                "type": "success",
                "sticky": False,
            },
        }

    @api.model
    def job_run_query_refresh(self, chart_ids):
        charts = self.browse(chart_ids).exists()
        if not charts:
            return False

        from ..services.chart_data_service import ChartDataService

        payload_service = ChartDataService(self.env)
        for chart in charts:
            for period in (0, 1, 7, 30):
                try:
                    payload = payload_service.get_dashboard_chart_data(
                        chart,
                        cco=True,
                        branches_id=[],
                        datepicked=period,
                        start_at=None,
                        end_at=None,
                    )
                    chart._store_cached_payload(
                        payload, cco=True, branches_id=[], datepicked=period
                    )
                except Exception as exc:
                    _logger.warning(
                        "Manual query run failed for chart %s (%s) period=%s: %s",
                        chart.name,
                        chart.code,
                        period,
                        exc,
                    )
        return True

    @api.model
    def _run_query_refresh_thread(self, chart_ids, uid, context=None):
        context = context or {}
        try:
            with api.Environment.manage():
                new_cr = self.pool.cursor()
                try:
                    env = api.Environment(new_cr, uid, context)
                    env["res.dashboard.charts"].job_run_query_refresh(chart_ids)
                    new_cr.commit()
                finally:
                    new_cr.close()
        except Exception as exc:
            _logger.error("Background chart query refresh failed: %s", exc)

    def action_run_query(self):
        self.ensure_one()
        queue_job = self.env["ir.module.module"].sudo().search(
            [("name", "=", "queue_job"), ("state", "=", "installed")], limit=1
        )

        if queue_job and hasattr(self, "with_delay"):
            self.with_delay(
                priority=30,
                description=_("Run chart query and refresh cached payload for %s")
                % (self.display_name,),
            ).job_run_query_refresh(self.ids)
            title = _("Query Queued")
            message = _("The chart query has been queued and the cached payload will be refreshed.")
        else:
            worker = Thread(
                target=self._run_query_refresh_thread,
                args=(self.ids, self.env.uid, dict(self.env.context)),
                daemon=True,
            )
            worker.start()
            title = _("Query Started")
            message = _("The chart query is running in the background and will refresh the cached payload.")

        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": title,
                "message": message,
                "type": "success",
                "sticky": False,
            },
        }

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
                        "Query execution timed out after 120 seconds. Please optimize your query."
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
                    preview_lines = []
                    for row in preview_results:
                        row_preview = ", ".join(
                            f"{field}={row.get(field)}" for field in fields
                        )
                        preview_lines.append(row_preview)
                    message = "\n".join(
                        [
                            f"Query executed successfully in {execution_time:.2f} ms.",
                            f"Total rows: {len(results)}",
                            f"Fields: {', '.join(fields)}",
                            "",
                            "Preview (first 10 rows):",
                            *preview_lines,
                        ]
                    )
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
