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

    @http.route("/dashboard/focused_charts", auth="user", type="json")
    def focused_charts(self, cco=False, branches_id=None, datepicked=7, **kw):
        try:
            sanitized_data = self.security_service.validate_and_sanitize_request_data({
                'cco': cco,
                'branches_id': branches_id,
                'datepicked': datepicked,
                **kw
            })
            cco = sanitized_data.get('cco', cco)
            branches_id = sanitized_data.get('branches_id', branches_id)
            datepicked = self._normalize_dashboard_period(
                sanitized_data.get('datepicked', datepicked)
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

        account_filters = []
        account_params = []
        tx_filters = []
        tx_params = []
        risk_rule_filters = []
        risk_rule_params = []

        if datepicked in (0, 1, 7, 30):
            account_filters.append(
                "((COALESCE(rpa.opening_date, rpa.date_created) BETWEEN %s AND %s) OR (rpa.create_date BETWEEN %s AND %s))"
            )
            account_params.extend([
                start_at,
                end_at,
                start_at,
                end_at,
            ])
            tx_filters.append("rct.date_created BETWEEN %s AND %s")
            tx_params.extend([start_at, end_at])
            risk_rule_filters.append("rppl.create_date BETWEEN %s AND %s")
            risk_rule_params.extend([start_at, end_at])

        if not cco:
            if not branches_array:
                return []
            account_filters.append("rb.id = ANY(%s)")
            account_params.append(branches_array)
            tx_filters.append("rct.branch_id = ANY(%s)")
            tx_params.append(branches_array)
            risk_rule_filters.append("rp.branch_id = ANY(%s)")
            risk_rule_params.append(branches_array)

        account_where = f"WHERE {' AND '.join(account_filters)}" if account_filters else ""
        tx_where = f"WHERE {' AND '.join(tx_filters)}" if tx_filters else ""
        risk_rule_where = f"WHERE {' AND '.join(risk_rule_filters)}" if risk_rule_filters else ""

        account_filters_fallback = []
        account_params_fallback = []
        if not cco:
            account_filters_fallback.append("rb.id = ANY(%s)")
            account_params_fallback.append(branches_array)
        account_where_fallback = (
            f"WHERE {' AND '.join(account_filters_fallback)}"
            if account_filters_fallback else ""
        )

        top_accounts_sql = f"""
            SELECT rb.id, rb.name, COUNT(rpa.id) AS account_count
            FROM res_branch rb
            JOIN res_partner_account rpa ON rb.id = rpa.branch_id
            {account_where}
            GROUP BY rb.id, rb.name
            ORDER BY account_count DESC
            LIMIT 10
        """

        high_risk_filters = list(account_filters)
        high_risk_params = list(account_params)
        high_risk_filters.append("rp.risk_level = 'high'")
        high_risk_where = f"WHERE {' AND '.join(high_risk_filters)}" if high_risk_filters else ""

        top_high_risk_sql = f"""
            SELECT rb.id, rb.name, COUNT(rpa.id) AS high_risk_accounts
            FROM res_branch rb
            JOIN res_partner_account rpa ON rb.id = rpa.branch_id
            JOIN res_partner rp ON rpa.customer_id = rp.id
            {high_risk_where}
            GROUP BY rb.id, rb.name
            ORDER BY high_risk_accounts DESC
            LIMIT 10
        """

        top_exceptions_sql = f"""
            SELECT rtsr.id, rtsr.name, COUNT(rct.id) AS hit_count
            FROM res_transaction_screening_rule rtsr
            JOIN res_customer_transaction rct ON rtsr.id = rct.rule_id
            {tx_where}
            GROUP BY rtsr.id, rtsr.name
            ORDER BY hit_count DESC
            LIMIT 10
        """

        top_risk_rules_sql = f"""
            SELECT rcap.id, rcap.name, COUNT(DISTINCT rppl.partner_id) AS partner_count
            FROM res_compliance_risk_assessment_plan rcap
            JOIN res_partner_risk_plan_line rppl ON rcap.id = rppl.plan_line_id
            JOIN res_partner rp ON rp.id = rppl.partner_id
            {risk_rule_where}
            GROUP BY rcap.id, rcap.name
            ORDER BY partner_count DESC, rcap.name ASC
            LIMIT 10
        """

        top_accounts = self._execute_dashboard_query(top_accounts_sql, tuple(account_params))
        top_high_risk = self._execute_dashboard_query(top_high_risk_sql, tuple(high_risk_params))
        top_exceptions = self._execute_dashboard_query(top_exceptions_sql, tuple(tx_params))
        top_risk_rules = self._execute_dashboard_query(top_risk_rules_sql, tuple(risk_rule_params))

        account_domain = []
        account_domain_fallback = []
        if datepicked in (0, 1, 7, 30):
            account_domain.extend([
                ["create_date", ">=", start_at],
                ["create_date", "<=", end_at],
            ])
        if not cco and branches_array:
            account_domain.append(["branch_id", "in", branches_array])
            account_domain_fallback.append(["branch_id", "in", branches_array])

        if datepicked in (0, 1, 7, 30) and not top_accounts:
            top_accounts_sql = f"""
                SELECT rb.id, rb.name, COUNT(rpa.id) AS account_count
                FROM res_branch rb
                JOIN res_partner_account rpa ON rb.id = rpa.branch_id
                {account_where_fallback}
                GROUP BY rb.id, rb.name
                ORDER BY account_count DESC
                LIMIT 10
            """
            top_accounts = self._execute_dashboard_query(
                top_accounts_sql, tuple(account_params_fallback)
            )
            account_domain = list(account_domain_fallback)

        if datepicked in (0, 1, 7, 30) and not top_high_risk:
            top_high_risk_sql = f"""
                SELECT rb.id, rb.name, COUNT(rpa.id) AS high_risk_accounts
                FROM res_branch rb
                JOIN res_partner_account rpa ON rb.id = rpa.branch_id
                JOIN res_partner rp ON rpa.customer_id = rp.id
                {f"{account_where_fallback} {'AND' if account_where_fallback else 'WHERE'} rp.risk_level = 'high'"}
                GROUP BY rb.id, rb.name
                ORDER BY high_risk_accounts DESC
                LIMIT 10
            """
            top_high_risk = self._execute_dashboard_query(
                top_high_risk_sql, tuple(account_params_fallback)
            )
            account_domain = list(account_domain_fallback)

        case_statuses = []
        case_exceptions = []

        if request.env.registry.get("case.manager"):
            case_filters = []
            case_params = []

            if datepicked in (0, 1, 7, 30):
                case_filters.append("cm.create_date BETWEEN %s AND %s")
                case_params.extend([start_at, end_at])

            if not cco:
                if not branches_array:
                    return []

            case_where = f"WHERE {' AND '.join(case_filters)}" if case_filters else ""

            case_status_rows = self._execute_dashboard_query(
                f"""
                    SELECT cm.case_status, COUNT(cm.id) AS case_count
                    FROM case_manager cm
                    {case_where}
                    GROUP BY cm.case_status
                    ORDER BY case_count DESC, cm.case_status ASC
                """,
                tuple(case_params),
            )
            status_labels = {
                "draft": "Draft",
                "open": "Open",
                "closed": "Closed",
                "overdue": "Overdue",
                "archived": "Archived",
            }
            case_statuses = [
                (row[0], status_labels.get(row[0], str(row[0]).title()), row[1])
                for row in case_status_rows if row and row[0]
            ]

            case_exception_filters = list(case_filters)
            case_exception_params = list(case_params)
            case_exception_filters.append("cm.process IS NOT NULL")
            case_exception_where = (
                f"WHERE {' AND '.join(case_exception_filters)}"
                if case_exception_filters else ""
            )

            case_exception_sql = f"""
                SELECT
                    ep.id,
                    ep.name,
                    COUNT(cm.id) AS case_count
                FROM case_manager cm
                JOIN exception_process_ ep ON ep.id = cm.process
                {case_exception_where}
                GROUP BY ep.id, ep.name
                ORDER BY case_count DESC, ep.name ASC
                LIMIT 10
            """

            case_exceptions = self._execute_dashboard_query(
                case_exception_sql, tuple(case_exception_params)
            )

        tx_domain = []
        if datepicked in (0, 1, 7, 30):
            tx_domain.extend([
                ["date_created", ">=", start_at],
                ["date_created", "<=", end_at],
            ])
        if not cco and branches_array:
            tx_domain.append(["branch_id", "in", branches_array])

        risk_rule_domain = []
        if datepicked in (0, 1, 7, 30):
            risk_rule_domain.extend([
                ["create_date", ">=", start_at],
                ["create_date", "<=", end_at],
            ])
        if not cco and branches_array:
            risk_rule_domain.append(["partner_id.branch_id", "in", branches_array])

        case_domain = self._build_case_domain(
            cco, branches_array, start_at, end_at, datepicked
        )

        return [
            {
                "id": "top_customer_risk_rules",
                "title": "Top 10 Customer Risk Rules",
                "type": "line",
                "labels": [row[1] for row in top_risk_rules],
                "ids": [row[0] for row in top_risk_rules],
                "filter": "plan_line_id",
                "model_name": "res.partner.risk.plan.line",
                "additional_domain": risk_rule_domain,
                "datasets": [{
                    "label": "Partners",
                    "data": [row[2] for row in top_risk_rules],
                }],
            },
            {
                "id": "top_accounts",
                "title": "Top 10 Branch by Accounts Opened",
                "type": "bar",
                "labels": [row[1] for row in top_accounts],
                "ids": [row[0] for row in top_accounts],
                "filter": "branch_id",
                "model_name": "res.partner.account",
                "additional_domain": account_domain,
                "datasets": [{
                    "label": "Accounts",
                    "data": [row[2] for row in top_accounts],
                }],
            },
            {
                "id": "top_high_risk_accounts",
                "title": "Top 10 High Risk Branch by Accounts",
                "type": "bar",
                "labels": [row[1] for row in top_high_risk],
                "ids": [row[0] for row in top_high_risk],
                "filter": "branch_id",
                "model_name": "res.partner.account",
                "additional_domain": account_domain + [["risk_level", "=", "high"]],
                "datasets": [{
                    "label": "High Risk Accounts",
                    "data": [row[2] for row in top_high_risk],
                }],
            },
            {
                "id": "top_transaction_exceptions",
                "title": "Top Transaction Exception",
                "type": "line",
                "labels": [row[1] for row in top_exceptions],
                "ids": [row[0] for row in top_exceptions],
                "filter": "rule_id",
                "model_name": "res.customer.transaction",
                "additional_domain": tx_domain,
                "datasets": [{
                    "label": "Exceptions",
                    "data": [row[2] for row in top_exceptions],
                }],
            },
            {
                "id": "cases_by_status",
                "title": "Cases by Status",
                "type": "pie",
                "labels": [row[1] for row in case_statuses],
                "ids": [row[0] for row in case_statuses],
                "filter": "case_status",
                "model_name": "case.manager",
                "additional_domain": case_domain,
                "datasets": [{
                    "label": "Cases",
                    "data": [row[2] for row in case_statuses],
                }],
            },
            {
                "id": "cases_by_exceptions",
                "title": "Cases by Exceptions",
                "type": "pie",
                "labels": [row[1] for row in case_exceptions],
                "ids": [row[0] for row in case_exceptions],
                "filter": "process",
                "model_name": "case.manager",
                "additional_domain": case_domain,
                "datasets": [{
                    "label": "Cases",
                    "data": [row[2] for row in case_exceptions],
                }],
            },
        ]

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
    def getAllstats(self, cco, branches_id, datepicked, **kw):
        # Validate all input parameters
        try:
            # Sanitize and validate input parameters
            sanitized_data = self.security_service.validate_and_sanitize_request_data({
                'cco': cco,
                'branches_id': branches_id,
                'datepicked': datepicked,
                **kw
            })
            cco = sanitized_data.get('cco', cco)
            branches_id = sanitized_data.get('branches_id', branches_id)
            datepicked = self._normalize_dashboard_period(
                sanitized_data.get('datepicked', datepicked)
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

        stat_records = request.env["res.compliance.stat"].sudo().search(
            [("state", "=", "active"), ("scope", "in", self.DASHBOARD_SCOPES)],
            order="id",
        )

        computed_results = [
            {
                "name": stat.name,
                "scope": stat.scope,
                "val": stat.val or 0,
                "id": stat.id,
                "scope_color": stat.scope_color,
                "query": stat.sql_query,
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
                ("scope", "in", self.DASHBOARD_SCOPES),
                ("scope", "=", category),
            ],
            order="id",
        )

        computed_results = [
            {
                "name": result.name,
                "scope": result.scope,
                "val": result.val or 0,
                "id": result.id,
                "scope_color": result.scope_color,
                "query": result.sql_query,
            }
            for result in results
        ]

        return {"data": computed_results, "total": len(computed_results)}
        
    DASHBOARD_SCOPES = ("bank", "regulatory", "risk", "branch")
