# -*- coding: utf-8 -*-

from odoo import http
from odoo.http import request
from datetime import datetime, timedelta
from odoo import fields
import re
import logging

from ..services.security_service import SecurityService
from ..services.database_service import DatabaseService
from ..services.cache_service import CacheService
from ..services.chart_data_service import ChartDataService
from ..services.materialized_view import MaterializedViewService
from ..utils.cache_key_unique_identifier import get_unique_client_identifier, normalize_cache_key_components
from ..services.query_service import QueryService
from ..decorators.security_decorators import validate_sql_input, log_access

_logger = logging.getLogger(__name__)

class Compliance(http.Controller):
    """
    A controller to manage compliance-related operations for dashboards.

    This class handles user permissions, dynamic SQL extraction, caching, and 
    statistics retrieval based on user roles and branches.
    """
    
    def __init__(self):
        """
        Initialize the Compliance controller and its security service.

        This sets up the services for security, database operations, and caching.
        """
        super(Compliance, self).__init__()
        self.security_service = SecurityService()
        self.database_service = DatabaseService()
        self.cache_service = CacheService()
        self.query_service = QueryService()
        self.chart_data_service = ChartDataService()
        self.materialized_view_service = MaterializedViewService()
        self.get_unique_client_identifier = get_unique_client_identifier
        self.normalize_cache_key_components = normalize_cache_key_components

    @http.route("/dashboard/user", auth="public", type="json")
    def index(self, **kw):
        """
        Retrieve user information for dashboard display.

        This method checks user roles (superuser, CCO, CO) and returns
        relevant user data including branches and a unique client identifier.

        Returns:
            dict: A dictionary containing user role information and unique client ID.
        """
        user = request.env.user
        is_superuser = user.has_group("base.group_system")
        is_cco = self.security_service.is_cco_user()
        is_co = self.security_service.is_co_user()
        branch = [branch.id for branch in user.branches_id]
        unique_id = get_unique_client_identifier()
        
        result = {
            "group": is_cco,
            "is_cco": is_cco,
            "is_co": is_co,
            "branch": branch,
            "unique_id": unique_id,
        }
        
        return result

    def check_branches_id(self, branches_id):
        """
        Ensure branches_id is always a list.

        This method checks the type of branches_id and converts it to a list if necessary.

        Args:
            branches_id (list or any): The branches ID to check.

        Returns:
            list: A list of branches IDs.
        """
        return self.security_service.check_branches_id(branches_id)

    def _normalize_dashboard_period(self, datepicked):
        try:
            datepicked = int(datepicked)
        except (TypeError, ValueError):
            datepicked = 7
        return datepicked if datepicked in (0, 1, 7, 30) else 7

    def _normalize_dashboard_scope(self, dashboard_scope, default=None):
        allowed_scopes = {
            "alert",
            "bank",
            "branch",
            "case",
            "compliance",
            "interbank",
            "regulatory",
            "risk",
        }
        if dashboard_scope in allowed_scopes:
            return dashboard_scope
        return default

    def _dashboard_date_range(self, datepicked):
        today = datetime.now().date()
        if datepicked == 0:
            start_date = today
            end_date = today
        elif datepicked == 1:
            start_date = today - timedelta(days=1)
            end_date = start_date
        elif datepicked == 7:
            start_date = today - timedelta(days=6)
            end_date = today
        else:
            start_date = today - timedelta(days=29)
            end_date = today

        start_at = fields.Datetime.to_string(
            datetime.combine(start_date, datetime.min.time())
        )
        end_at = fields.Datetime.to_string(
            datetime.combine(end_date, datetime.max.time())
        )
        return start_at, end_at

    def _execute_dashboard_query(self, sql, params=None):
        request.env.cr.execute(sql, params or ())
        return request.env.cr.fetchall()

    def _build_case_domain(self, cco, branches_array, start_at, end_at, datepicked):
        case_domain = []
        if datepicked in (0, 1, 7, 30):
            case_domain.extend([
                ["create_date", ">=", start_at],
                ["create_date", "<=", end_at],
            ])
        return case_domain

    def _get_branch_codes(self, branch_ids):
        if not branch_ids:
            return []
        rows = self._execute_dashboard_query(
            """
                SELECT COALESCE(NULLIF(TRIM(co_code), ''), NULLIF(TRIM(code), ''))
                FROM res_branch
                WHERE id = ANY(%s)
            """,
            (branch_ids,),
        )
        return [row[0] for row in rows if row and row[0]]

    def _get_default_compliance_chart_records(self):
        chart_xmlids = [
            "compliance_management.demo_chart_top10_branch_by_customer",
            "compliance_management.demo_chart_top10_high_risk_branch",
            "compliance_management.demo_chart_top_customer_risk_rules",
            "compliance_management.demo_chart_top10_screened_transaction",
            "compliance_management.demo_chart_cases_by_status",
            "compliance_management.demo_chart_cases_by_exceptions",
        ]
        charts = request.env["res.dashboard.charts"].sudo().browse()
        for xmlid in chart_xmlids:
            chart = request.env.ref(xmlid, raise_if_not_found=False)
            if chart:
                charts |= chart
        return charts.sorted(lambda chart: (chart.display_order, chart.id))

    @http.route("/dashboard/focused_charts", auth="user", type="json")
    def focused_charts(self, cco=False, branches_id=None, datepicked=7, dashboard_scope=None, **kw):
        try:
            sanitized_data = self.security_service.validate_and_sanitize_request_data({
                'cco': cco,
                'branches_id': branches_id,
                'datepicked': datepicked,
                'dashboard_scope': dashboard_scope,
                **kw
            })
            cco = sanitized_data.get('cco', cco)
            branches_id = sanitized_data.get('branches_id', branches_id)
            datepicked = self._normalize_dashboard_period(
                sanitized_data.get('datepicked', datepicked)
            )
            dashboard_scope = self._normalize_dashboard_scope(
                sanitized_data.get('dashboard_scope', dashboard_scope),
                default="compliance",
            )
        except Exception as e:
            self.security_service.log_security_event(
                "FOCUSED_CHARTS_INPUT_VALIDATION_FAILED",
                f"Focused charts endpoint validation failed: {str(e)}"
            )
            return []

        is_cco = self.security_service.is_cco_user()
        is_co = self.security_service.is_co_user()
        if is_cco or is_co:
            cco = True

        branches_array = self.check_branches_id(branches_id) if branches_id else []
        start_at, end_at = self._dashboard_date_range(datepicked)
        chart_service = ChartDataService(request.env)
        chart_records = request.env["res.dashboard.charts"].sudo().search(
            [
                ("state", "=", "active"),
                ("active", "=", True),
                ("is_visible", "=", True),
                ("scope", "=", dashboard_scope),
            ],
            order="display_order asc, id asc",
        )
        if not chart_records and dashboard_scope == "compliance":
            chart_records = request.env["res.dashboard.charts"].sudo().search(
                [("state", "=", "active")],
                order="display_order asc, id asc",
            )
        if not chart_records and dashboard_scope == "compliance":
            chart_records = self._get_default_compliance_chart_records()
        results = [
            chart._get_dashboard_chart_payload(
                chart_service,
                cco=cco,
                branches_id=branches_array,
                datepicked=datepicked,
                start_at=start_at,
                end_at=end_at,
            )
            for chart in chart_records
        ]
        if results:
            return results

        from .charts import DynamicChartController

        return DynamicChartController().get_chart_data(
            cco=cco,
            branches_id=branches_array,
            datepicked=datepicked,
            **kw,
        )

    @validate_sql_input
    @log_access
    @http.route("/dashboard/dynamic_sql", auth="public", type="json")
    def extract_table_and_domain(self, sql_query: str, branches_id, cco):
        """
        Extract table names and WHERE conditions from SQL queries using regex.

        This method ignores COUNT aggregation and validates SQL structure for security.

        Args:
            sql_query (str): The SQL query string to analyze.
            branches_id (list): The IDs of branches to filter on.
            cco (bool): Indicates if the user is a CCO.

        Returns:
            dict: A dictionary containing the extracted table name and domain conditions.
        """
        # Validate and sanitize the SQL query first
        try:
            is_safe, error_msg = self.security_service.validate_sql_query(sql_query)
            if not is_safe:
                self.security_service.log_security_event(
                    "SQL_INJECTION_ATTEMPT",
                    f"Blocked dangerous query: {error_msg} - Query: {sql_query[:200]}..."
                )
                return {"error": "Request validation failed"}
        except Exception as e:
            _logger.error(f"Security validation error: {str(e)}")
            return {"error": "Security validation failed"}
            
        is_cco = self.security_service.is_cco_user()
        is_co = self.security_service.is_co_user()
        
        if is_co:
            cco = True
            _logger.info(
                f"CO user {request.env.user.id} accessing dynamic SQL with CCO privileges"
            )
            
        lower_query = sql_query.lower()
        table = None
        domain = []
        
        if re.search(r"\b(?:sum|avg|min|max)\s*\(", lower_query):
            return None
            
        table = self.query_service.extract_main_table(sql_query)
        if not table:
            join_match = re.search(
                r"\b(?:inner|left|right|full outer)?\s+join\s+([\w.]+)", lower_query
            )
            if join_match:
                return None
                
        where_match = re.search(
            r"\bwhere\s+(.+?)(?:\s+(?:group\s+by|order\s+by|limit|having)\s+|\s*$)",
            lower_query,
            re.DOTALL,
        )
        
        if where_match:
            condition_string = where_match.group(1).strip()
            domain = self.query_service.parse_condition_string(condition_string)
            
        additional_filters = []
        if table == "res_partner":
            additional_filters.append(("origin", "in", ["demo", "test", "prod"]))
            
        db_service = DatabaseService(request.env)
        has_branch_id = db_service.check_table_for_branch_column(table) is not None
        
        if not cco and has_branch_id:
            branch_ids = self.check_branches_id(branches_id)
            additional_filters.append(("branch_id", "in", branch_ids))
            
        if additional_filters:
            if domain:
                is_complex = any(op == "|" for op in domain if isinstance(op, str))
                if is_complex:
                    domain = ["&"] + domain + [additional_filters[0]]
                    for filter_item in additional_filters[1:]:
                        domain = ["&"] + domain + [filter_item]
                else:
                    for filter_item in additional_filters:
                        domain = ["&"] + domain + [filter_item]
            else:
                domain = additional_filters
                
        _logger.info(f"Final domain: {domain}")
        return {"table": table, "domain": domain}

    def format_number(self, result_value):
        """
        Format a number with commas for better readability.

        Args:Z 
            result_value (int or float): The number to format.

        Returns:
            str: The formatted number as a string.
        """
        if isinstance(result_value, (int, float)):
            result_value = "{:,}".format(result_value)
            return result_value
        return result_value

    @log_access
    @http.route("/dashboard/stats", auth="public", type="json")
    def getAllstats(self, cco, branches_id, datepicked, dashboard_scope=None, **kw):
        # Validate all input parameters
        try:
            # Sanitize and validate input parameters
            sanitized_data = self.security_service.validate_and_sanitize_request_data({
                'cco': cco,
                'branches_id': branches_id,
                'datepicked': datepicked,
                'dashboard_scope': dashboard_scope,
                **kw
            })
            cco = sanitized_data.get('cco', cco)
            branches_id = sanitized_data.get('branches_id', branches_id)
            datepicked = self._normalize_dashboard_period(
                sanitized_data.get('datepicked', datepicked)
            )
            dashboard_scope = self._normalize_dashboard_scope(
                sanitized_data.get('dashboard_scope', dashboard_scope)
            )
        except Exception as e:
            self.security_service.log_security_event(
                "STATS_INPUT_VALIDATION_FAILED",
                f"Stats endpoint input validation failed: {str(e)}"
            )
            return {"error": "Request validation failed"}
            
        """
        Retrieve all statistics for the dashboard.

        This method checks user permissions and retrieves statistics based on
        the given date and branches, caching results where applicable.

        Args:
            cco (bool): Indicates if the user is a CCO.
            branches_id (list): The IDs of branches to filter on.
            datepicked (int): The number of days to consider for the statistics.

        Returns:
            dict: A dictionary containing computed statistics and total count.
        """
        user_id = request.env.user.id
        is_cco = self.security_service.is_cco_user()
        is_co = self.security_service.is_co_user()
        
        if is_co or is_cco:
            cco = True
            if is_co:
                _logger.info(f"CO user {user_id} accessing stats with CCO privileges")

        unique_id = get_unique_client_identifier()
        cco_str, branches_str, datepicked_str, unique_id = normalize_cache_key_components(
            cco, branches_id, datepicked, unique_id
        )
        cache_key = f"all_stats_v2_{cco_str}_{branches_str}_{datepicked_str}_{unique_id}"
        _logger.info(f"This is the stats cache key: {cache_key}")

        stat_domain = [("state", "=", "active"), ("is_visible", "=", True)]
        if dashboard_scope:
            stat_domain.append(("scope", "=", dashboard_scope))
        else:
            stat_domain.append(("scope", "in", self.DASHBOARD_SCOPES))
        stat_records = request.env["res.compliance.stat"].sudo().search(
            stat_domain,
            order="display_order asc, id asc",
        )

        computed_results = [
            {
                "name": stat.name,
                "scope": stat.scope,
                "val": (
                    stat.val
                    if stat.val not in (False, None, "")
                    else stat._compute_current_value(stat)
                ) or 0,
                "id": stat.id,
                "scope_color": stat.scope_color,
                "query": stat.sql_query,
                "display_summary": stat.display_summary,
                **stat._get_dashboard_action_metadata(),
            }
            for stat in stat_records
        ]
                    
        return {"data": computed_results, "total": len(computed_results)}

    @log_access
    @http.route("/dashboard/statsbycategory", auth="public", type="json")
    def getAllstatsByCategory(self, cco, branches_id, category, datepicked, **kw):
        """
        Retrieve statistics grouped by a specified category.

        This method handles user permissions, retrieves statistics based on
        the given category and date range, and returns formatted results.

        Args:
            cco (bool): Indicates if the user is a CCO.
            branches_id (list): The IDs of branches to filter on.
            category (str): The category to filter statistics by.
            datepicked (int): The number of days to consider for the statistics.

        Returns:
            dict: A dictionary containing computed statistics and total count.
        """
        # Validate all input parameters
        try:
            # Sanitize and validate input parameters
            sanitized_data = self.security_service.validate_and_sanitize_request_data({
                'cco': cco,
                'branches_id': branches_id,
                'category': category,
                'datepicked': datepicked,
                **kw
            })
            cco = sanitized_data.get('cco', cco)
            branches_id = sanitized_data.get('branches_id', branches_id)
            category = sanitized_data.get('category', category)
            datepicked = self._normalize_dashboard_period(
                sanitized_data.get('datepicked', datepicked)
            )
        except Exception as e:
            self.security_service.log_security_event(
                "STATS_CATEGORY_INPUT_VALIDATION_FAILED",
                f"Stats by category endpoint input validation failed: {str(e)}"
            )
            return {"error": "Request validation failed"}
            
        is_cco = self.security_service.is_cco_user()
        is_co = self.security_service.is_co_user()
        
        if is_co or is_cco:
            cco = True
            if is_co:
                _logger.info(
                    f"CO user {request.env.user.id} accessing stats by category with CCO privileges"
                )
                
        results = request.env["res.compliance.stat"].sudo().search(
            [
                ("state", "=", "active"),
                ("is_visible", "=", True),
                ("scope", "in", self.DASHBOARD_SCOPES),
                ("scope", "=", category),
            ],
            order="display_order asc, id asc",
        )

        computed_results = [
            {
                "name": result.name,
                "scope": result.scope,
                "val": result.val or 0,
                "id": result.id,
                "scope_color": result.scope_color,
                "query": result.sql_query,
                "display_summary": result.display_summary,
                **result._get_dashboard_action_metadata(),
            }
            for result in results
        ]

        return {"data": computed_results, "total": len(computed_results)}
        
    DASHBOARD_SCOPES = ("bank", "regulatory", "risk", "branch")
