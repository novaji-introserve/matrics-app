import psycopg2
from ..services.chart_data_service import ChartDataService
from odoo import api, http
from odoo.http import request
import json
import logging
import re
import time

_logger = logging.getLogger(__name__)


class DynamicChartController(http.Controller):
    """Controller for handling dynamic chart requests with security and performance"""

    def __init__(self):
        """
        Initialize the DynamicChartController.

        This setup includes initializing the ChartSecurityService and 
        utility functions for generating unique identifiers and normalizing cache keys.
        """
        super(DynamicChartController, self).__init__()
        from ..services.branch_security import ChartSecurityService
        from ..utils.cache_key_unique_identifier import (
            get_unique_client_identifier,
            normalize_cache_key_components,
        )

        self.security_service = ChartSecurityService()
        self.get_unique_client_identifier = get_unique_client_identifier
        self.normalize_cache_key_components = normalize_cache_key_components
        self.debug_mode = True

    @http.route("/web/dynamic_charts/preview", type="json", auth="user")
    def preview_chart(
        self,
        chart_type,
        query,
        x_axis_field=None,
        y_axis_field=None,
        color_scheme="default",
    ):
        """Preview chart without saving - with query safety checks and security.

        Args:
            chart_type (str): The type of the chart to preview.
            query (str): The SQL query to be executed for the chart data.
            x_axis_field (str, optional): The field for the x-axis.
            y_axis_field (str, optional): The field for the y-axis.
            color_scheme (str, optional): The color scheme for the chart.

        Returns:
            dict: A dictionary containing chart data or an error message.
        """
        try:
            chart_data_service = ChartDataService()
            if not chart_data_service._is_safe_query(query):
                return {"error": "Query contains unsafe operations"}
            chart = request.env["res.dashboard.charts"].new(
                {
                    "chart_type": chart_type,
                    "query": query,
                    "x_axis_field": x_axis_field,
                    "y_axis_field": y_axis_field,
                    "color_scheme": color_scheme,
                    "branch_field": "branch_id",
                    "branch_filter": True,
                    "date_field": "create_date",
                }
            )
            if hasattr(chart, "_check_query_safety"):
                chart._check_query_safety()
            cco = (
                self.security_service.is_cco_user()
                or self.security_service.is_co_user()
            )
            secured_query = self.security_service.secure_chart_query(chart, cco, [])
            with request.env.registry.cursor() as new_cr:
                new_cr.execute("SET LOCAL statement_timeout = 10000;")
                start_time = time.time()
                new_cr.execute(secured_query)
                execution_time = (time.time() - start_time) * 1000
                results = new_cr.dictfetchall()
                chart_data_service = ChartDataService()
                chart_data = chart_data_service._extract_chart_data(
                    chart, results, secured_query
                )
                chart_data["execution_time_ms"] = round(execution_time, 2)
                return chart_data
        except Exception as e:
            _logger.error(f"Error in preview_chart: {e}")
            return {"error": str(e)}

    @http.route("/dashboard/dynamic_chart_page/", type="json", auth="user")
    def get_chart_page(
        self, chart_id, page=0, page_size=50, cco=False, branches_id=None, **kw
    ):
        """Get paginated chart data for a single chart with robust error handling.

        Args:
            chart_id (int): The ID of the chart to retrieve.
            page (int, optional): The page number for pagination.
            page_size (int, optional): The number of items per page.
            cco (bool, optional): Indicates if the user is a CCO.
            branches_id (list, optional): List of branch IDs to filter data.

        Returns:
            dict: A dictionary containing chart data or an error message.
        """
        if branches_id is None:
            branches_id = []
        try:
            chart_id = int(chart_id)
            page = max(0, int(page))
            page_size = min(100, max(1, int(page_size)))
        except (ValueError, TypeError):
            return {"error": "Invalid parameters"}
        if not isinstance(cco, bool):
            cco = str(cco).lower() == "true"
        if self.security_service.is_co_user():
            cco = True
        if not isinstance(branches_id, list):
            try:
                branches_id = json.loads(branches_id) if branches_id else []
            except (ValueError, TypeError):
                branches_id = []
        user_id = request.env.user.id
        datepicked = kw.get("datepicked", 20000)
        cache_params = {
            "chart_id": chart_id,
            "page": page,
            "page_size": page_size,
            "cco": cco,
            "branches_id": branches_id,
            "datepicked": datepicked,
        }
        cache_key = self.generate_cache_key("chart_page", cache_params)
        cache_data = request.env["res.dashboard.cache"].get_cache(cache_key, user_id)
        if cache_data:
            return cache_data
        return self.get_chart_with_retries(
            chart_id, page, page_size, cco, branches_id, cache_key, user_id
        )

    def get_chart_with_retries(
        self, chart_id, page, page_size, cco, branches_id, cache_key, user_id
    ):
        """Get chart data with robust retry handling for serialization failures.

        Args:
            chart_id (int): The ID of the chart to retrieve.
            page (int): The page number for pagination.
            page_size (int): The number of items per page.
            cco (bool): Indicates if the user is a CCO.
            branches_id (list): List of branch IDs to filter data.
            cache_key (str): Cache key for storing results.
            user_id (int): The ID of the user requesting the data.

        Returns:
            dict: A dictionary containing chart data or an error message.
        """
        max_retries = 5
        retry_count = 0
        while retry_count < max_retries:
            try:
                chart = request.env["res.dashboard.charts"].browse(chart_id)
                if not chart.exists():
                    return {"error": "Chart not found"}
                return self._get_chart_from_materialized_view(
                    chart, page, page_size, cco, branches_id, cache_key, user_id
                )
            except psycopg2.errors.SerializationFailure as e:
                retry_count += 1
                if retry_count >= max_retries:
                    _logger.error(f"Maximum retries reached for chart {chart_id}: {e}")
                    return {"error": "Database serialization failure, please try again"}
                wait_time = 2 ** (retry_count - 1)
                _logger.info(
                    f"Serialization failure for chart {chart_id}, retry {retry_count} in {wait_time}s"
                )
                time.sleep(wait_time)
            except Exception as e:
                _logger.error(f"Error in get_chart_with_retries: {e}")
                return {"error": str(e)}
        return {"error": "Failed after multiple retries"}

    def _get_chart_from_materialized_view(
        self, chart, page, page_size, cco, branches_id, cache_key, user_id
    ):
        """Get chart data from materialized view with robust retry mechanism.

        Args:
            chart (record): The chart record to retrieve data from.
            page (int): The page number for pagination.
            page_size (int): The number of items per page.
            cco (bool): Indicates if the user is a CCO.
            branches_id (list): List of branch IDs to filter data.
            cache_key (str): Cache key for storing results.
            user_id (int): The ID of the user requesting the data.

        Returns:
            dict: A dictionary containing chart data or an error message.
        """
        view_name = f"dashboard_chart_view_{chart.id}"
        retry_params = request.env["ir.config_parameter"].sudo()
        max_retries = int(retry_params.get_param("chart.view.max_retries", default=3))
        base_delay = float(retry_params.get_param("chart.view.base_delay", default=0.5))
        for retry_attempt in range(max_retries):
            try:
                with request.env.registry.cursor() as cr:
                    isolation_level = retry_params.get_param(
                        "chart.view.isolation_level", default="READ COMMITTED"
                    )
                    cr.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                    cr.execute(
                        f"""
                        SELECT COUNT(*) 
                        FROM pg_catalog.pg_class 
                        WHERE relname = %s AND relkind = 'm'
                    """,
                        (view_name,),
                    )
                    if cr.fetchone()[0] == 0:
                        _logger.warning(
                            f"Materialized view {view_name} does not exist (attempt {retry_attempt+1}/{max_retries})"
                        )
                        refresher = request.env["dashboard.chart.view.refresher"].sudo()
                        created = refresher.create_materialized_view_for_chart(chart.id)
                        if not created:
                            _logger.error(
                                f"Failed to create materialized view {view_name}"
                            )
                            if retry_attempt == max_retries - 1:
                                return self._get_chart_from_direct_query(
                                    chart,
                                    page,
                                    page_size,
                                    cco,
                                    branches_id,
                                    cache_key,
                                    user_id,
                                )
                            delay = base_delay * (retry_attempt + 1)
                            time.sleep(delay)
                            continue
                        delay = base_delay * (retry_attempt + 1)
                        time.sleep(delay)
                        continue
                    try:
                        _logger.info(
                            f"Querying materialized view {view_name} directly (attempt {retry_attempt+1})"
                        )
                        cr.execute(f"SELECT * FROM {view_name} LIMIT 1")
                        columns = [desc[0] for desc in cr.description]
                        if not columns:
                            _logger.warning(
                                f"View {view_name} exists but returned no columns (attempt {retry_attempt+1}/{max_retries})"
                            )
                            if retry_attempt == max_retries - 1:
                                _logger.info(f"Last attempt - forcing view recreation")
                                cr.execute(
                                    f"DROP MATERIALIZED VIEW IF EXISTS {view_name}"
                                )
                                cr.commit()
                                refresher = request.env[
                                    "dashboard.chart.view.refresher"
                                ].sudo()
                                refresher.create_materialized_view_for_chart(chart.id)
                                delay = base_delay * 2 * (retry_attempt + 1)
                                time.sleep(delay)
                                continue
                            delay = base_delay * (retry_attempt + 1)
                            time.sleep(delay)
                            continue
                        _logger.info(
                            f"Successfully found {len(columns)} columns in {view_name}: {columns}"
                        )
                        branch_col = None
                        if chart.branch_field:
                            field = (
                                chart.branch_field.split(".")[-1]
                                if "." in chart.branch_field
                                else chart.branch_field
                            )
                            if field in columns:
                                branch_col = field
                            else:
                                for col in columns:
                                    if (
                                        col.lower().endswith("_id")
                                        or "branch" in col.lower()
                                    ):
                                        branch_col = col
                                        break
                        sort_col = None
                        if chart.y_axis_field:
                            field = (
                                chart.y_axis_field.split(".")[-1]
                                if "." in chart.y_axis_field
                                else chart.y_axis_field
                            )
                            if field in columns:
                                sort_col = field
                            else:
                                for col in columns:
                                    if any(
                                        term in col.lower()
                                        for term in [
                                            "count",
                                            "total",
                                            "sum",
                                            "amount",
                                            "value",
                                            "risk",
                                        ]
                                    ):
                                        sort_col = col
                                        break
                        query = f"SELECT * FROM {view_name}"
                        if (
                            chart.branch_field
                            and not cco
                            and not self.security_service.is_cco_user()
                        ):
                            user_branches = self.security_service.get_user_branch_ids()
                            effective_branches = []
                            if branches_id:
                                if user_branches:
                                    effective_branches = [
                                        b for b in branches_id if b in user_branches
                                    ]
                                else:
                                    effective_branches = branches_id
                            elif user_branches:
                                effective_branches = user_branches
                            if effective_branches and branch_col:
                                if len(effective_branches) == 1:
                                    query += (
                                        f" WHERE {branch_col} = {effective_branches[0]}"
                                    )
                                else:
                                    query += f" WHERE {branch_col} IN {tuple(effective_branches)}"
                            elif branch_col:
                                query += " WHERE 1=0"
                        cr.execute(f"SELECT COUNT(*) FROM ({query}) AS count_query")
                        total_count = cr.fetchone()[0]
                        if sort_col:
                            query += f" ORDER BY {sort_col} DESC"
                        query += f" LIMIT {page_size} OFFSET {page * page_size}"
                        timeout = int(
                            retry_params.get_param(
                                "chart.view.query_timeout", default=30000
                            )
                        )
                        cr.execute(f"SET LOCAL statement_timeout = {timeout}")
                        cr.execute(query)
                        results = cr.dictfetchall()
                        chart_data_service = ChartDataService()
                        chart_data = chart_data_service._extract_chart_data(
                            chart, results, query
                        )
                        chart_data["pagination"] = {
                            "total": total_count,
                            "page": page,
                            "page_size": page_size,
                            "pages": (
                                (total_count + page_size - 1) // page_size
                                if page_size > 0
                                else 0
                            ),
                        }
                        request.env["res.dashboard.cache"].set_cache(
                            cache_key, chart_data, user_id
                        )
                        return chart_data
                    except Exception as query_error:
                        _logger.warning(
                            f"Error querying view {view_name}: {query_error}, attempt {retry_attempt+1}/{max_retries}"
                        )
                        if retry_attempt == max_retries - 1:
                            try:
                                _logger.info(
                                    f"Attempting to drop and recreate view {view_name}"
                                )
                                cr.execute(
                                    f"DROP MATERIALIZED VIEW IF EXISTS {view_name}"
                                )
                                cr.commit()
                                refresher = request.env[
                                    "dashboard.chart.view.refresher"
                                ].sudo()
                                created = refresher.create_materialized_view_for_chart(
                                    chart.id
                                )
                                if not created:
                                    _logger.error(
                                        f"Failed to recreate view {view_name}"
                                    )
                                    return self._get_chart_from_direct_query(
                                        chart,
                                        page,
                                        page_size,
                                        cco,
                                        branches_id,
                                        cache_key,
                                        user_id,
                                    )
                                delay = base_delay * 2 * (retry_attempt + 1)
                                time.sleep(delay)
                            except Exception as drop_error:
                                _logger.error(
                                    f"Error dropping view {view_name}: {drop_error}"
                                )
                                return self._get_chart_from_direct_query(
                                    chart,
                                    page,
                                    page_size,
                                    cco,
                                    branches_id,
                                    cache_key,
                                    user_id,
                                )
            except Exception as e:
                _logger.error(
                    f"Transaction error in attempt {retry_attempt+1}/{max_retries}: {e}"
                )
                delay = base_delay * (retry_attempt + 1)
                time.sleep(delay)
        _logger.error(
            f"All {max_retries} attempts failed for chart {chart.id}, falling back to direct query"
        )
        return self._get_chart_from_direct_query(
            chart, page, page_size, cco, branches_id, cache_key, user_id
        )

    def get_chart_data_from_direct_query(self, chart, cco, branches_id):
        """Get chart data directly from the database with thorough error prevention.

        Args:
            chart (record): The chart record to retrieve data from.
            cco (bool): Indicates if the user is a CCO.
            branches_id (list): List of branch IDs to filter data.

        Returns:
            dict: A dictionary containing chart data or an error message.
        """
        try:
            secured_query = self.security_service.secure_chart_query(
                chart, cco, branches_id
            )
            is_valid, validation_message = self.validate_query_syntax(secured_query)
            if not is_valid:
                _logger.error(
                    f"Invalid query for chart {chart.id}: {validation_message}"
                )
                _logger.error(f"Original query: {chart.query}")
                _logger.error(f"Secured query: {secured_query}")
                return {
                    "id": chart.id,
                    "title": chart.name,
                    "type": chart.chart_type,
                    "error": f"Query syntax error: {validation_message}",
                    "labels": [],
                    "datasets": [{"data": [], "backgroundColor": []}],
                }
            with request.env.registry.cursor() as cr:
                try:
                    cr.execute("SET LOCAL statement_timeout = 15000;")
                    start_time = time.time()
                    cr.execute(secured_query)
                    execution_time = (time.time() - start_time) * 1000
                    results = cr.dictfetchall()
                    chart_data_service = ChartDataService()
                    chart_data = chart_data_service._extract_chart_data(
                        chart, results, secured_query
                    )
                    chart_data["execution_time_ms"] = round(execution_time, 2)
                    self._record_execution_stats(chart.id, execution_time, "success")
                    return chart_data
                except psycopg2.Error as sql_error:
                    cr.rollback()
                    error_msg = str(sql_error)
                    _logger.error(f"SQL error for chart {chart.id}: {error_msg}")
                    self._record_execution_stats(chart.id, 0, "error", error_msg)
                    friendly_msg = self._get_friendly_error_message(error_msg)
                    return {
                        "id": chart.id,
                        "title": chart.name,
                        "type": chart.chart_type,
                        "error": friendly_msg,
                        "labels": [],
                        "datasets": [{"data": [], "backgroundColor": []}],
                    }
                except Exception as e:
                    cr.rollback()
                    _logger.error(f"Error executing chart {chart.id}: {str(e)}")
                    self._record_execution_stats(chart.id, 0, "error", str(e))
                    return {
                        "id": chart.id,
                        "title": chart.name,
                        "type": chart.chart_type,
                        "error": str(e),
                        "labels": [],
                        "datasets": [{"data": [], "backgroundColor": []}],
                    }
        except Exception as e:
            _logger.error(f"Error preparing chart query: {str(e)}")
            return {
                "id": chart.id,
                "title": chart.name,
                "type": chart.chart_type,
                "error": f"Error preparing query: {str(e)}",
                "labels": [],
                "datasets": [{"data": [], "backgroundColor": []}],
            }

    def _get_friendly_error_message(self, error_msg):
        """Convert technical SQL errors to user-friendly messages.

        Args:
            error_msg (str): The raw error message from SQL execution.

        Returns:
            str: A user-friendly error message.
        """
        if "syntax error" in error_msg.lower():
            return "SQL syntax error. Please check your query format."
        elif "timeout" in error_msg.lower():
            return "Query timed out. Please simplify your query or enable the materialized view option."
        elif "does not exist" in error_msg.lower():
            if "column" in error_msg.lower():
                column = re.search(r'column\s+"([^"]+)"', error_msg)
                if column:
                    return f"Column '{column.group(1)}' does not exist. Please check field names."
            elif "relation" in error_msg.lower():
                table = re.search(r'relation\s+"([^"]+)"', error_msg)
                if table:
                    return f"Table '{table.group(1)}' does not exist. Please check table names."
            return "Referenced column or table does not exist. Please check your query."
        else:
            return f"Database error: {error_msg}"

    def validate_query_syntax(self, query):
        """Thoroughly validate SQL query syntax before execution.

        Args:
            query (str): The SQL query to validate.

        Returns:
            tuple: A tuple containing a boolean indicating validity and an error message if invalid.
        """
        try:
            query = re.sub(r"--.*?$", "", query, flags=re.MULTILINE)
            query = re.sub(r"/\*.*?\*/", "", query, flags=re.DOTALL)
            error_patterns = [
                (r"WHERE\s+WHERE", "Duplicate WHERE clause"),
                (r"AND\s+WHERE", "Invalid AND WHERE sequence"),
                (r"\(\s*WHERE", "WHERE inside parentheses without SELECT/FROM"),
                (r"WHERE\s*\)", "WHERE followed directly by closing parenthesis"),
                (r"WHERE\s+OR\b", "WHERE followed directly by OR"),
                (r"WHERE\s+ORDER", "WHERE followed directly by ORDER"),
                (r"WHERE\s+GROUP", "WHERE followed directly by GROUP"),
                (r"WHERE\s+HAVING", "WHERE followed directly by HAVING"),
                (r"AND\s+OR\b", "Mixed AND OR without parentheses"),
                (r"OR\s+AND\b", "Mixed OR AND without parentheses"),
                (r"WHERE\s*$", "WHERE at end of query without conditions"),
                (r"WHERE\s+SELECT", "WHERE followed by SELECT without comparison"),
                (r"SELECT\s+FROM\s+WHERE", "FROM followed directly by WHERE"),
                (r"FROM\s+WHERE\s+\w+", "FROM WHERE sequence (missing table)"),
                (r"\.\s*IN\s*\(", "Potential syntax error with IN clause"),
                (r"\.\s*WHERE", "Table.WHERE syntax error"),
            ]
            for pattern, error in error_patterns:
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    return False, f"SQL syntax error: {error} at '{match.group(0)}'"
            if query.count("(") != query.count(")"):
                return False, "Unbalanced parentheses in query"
            subquery_pattern = r"\(\s*SELECT.*?FROM.*?\)"
            subqueries = re.finditer(subquery_pattern, query, re.IGNORECASE | re.DOTALL)
            for match in subqueries:
                subquery = match.group(0)
                for pattern, error in error_patterns:
                    submatch = re.search(pattern, subquery, re.IGNORECASE)
                    if submatch:
                        return (
                            False,
                            f"Subquery syntax error: {error} at '{submatch.group(0)}'",
                        )
            return True, "Query syntax appears valid"
        except Exception as e:
            return False, f"Query validation error: {str(e)}"

    def _record_execution_stats(
        self, chart_id, execution_time, status, error_message=None
    ):
        """Record execution statistics for a chart with error isolation.

        Args:
            chart_id (int): The ID of the chart being executed.
            execution_time (float): The time taken to execute the query.
            status (str): The execution status (success/error).
            error_message (str, optional): An error message if applicable.
        """
        try:
            registry = request.env.registry
            with registry.cursor() as cr:
                env = api.Environment(cr, request.env.uid, request.env.context.copy())
                chart = env["res.dashboard.charts"].browse(chart_id)
                if chart.exists():
                    values = {
                        "last_execution_time": execution_time,
                        "last_execution_status": status,
                    }
                    if error_message:
                        values["last_error_message"] = error_message
                    else:
                        values["last_error_message"] = False
                    chart.write(values)
                    cr.commit()
        except Exception as e:
            _logger.error(f"Failed to record execution statistics: {e}")

    @http.route("/dashboard/dynamic_charts/", type="json", auth="user")
    def get_chart_data(self, cco=False, branches_id=None, **kw):
        """Get chart data in JSON format for all charts with strict branch security.

        Args:
            cco (bool, optional): Indicates if the user is a CCO.
            branches_id (list, optional): List of branch IDs to filter data.

        Returns:
            list: A list of dictionaries containing chart data or an error message.
        """
        try:
            if not isinstance(cco, bool):
                cco = str(cco).lower() == "true"
            if not isinstance(branches_id, list):
                try:
                    branches_id = json.loads(branches_id) if branches_id else []
                except (ValueError, TypeError):
                    branches_id = []
            if branches_id:
                branches_id = [int(b) for b in branches_id if str(b).isdigit()]
            user_id = request.env.user.id
            datepicked = kw.get("datepicked", 20000)
            actual_is_cco = self.security_service.is_cco_user()
            actual_is_co = self.security_service.is_co_user()
            if actual_is_co:
                _logger.info(f"CO user {user_id} accessing charts")
                cco = False
            elif not actual_is_cco and cco:
                _logger.warning(
                    f"Non-CCO/CO user {user_id} attempted to use CCO parameter"
                )
                cco = False
            _logger.info(
                f"Chart data requested - cco={cco}, branches_id={branches_id}, user_id={user_id}, actual_is_cco={actual_is_cco}, actual_is_co={actual_is_co}"
            )
            unique_id = self.get_unique_client_identifier()
            cco_str, branches_str, datepicked_str, unique_id = (
                self.normalize_cache_key_components(
                    cco, branches_id, datepicked, unique_id
                )
            )
            cache_key = (
                f"charts_data_{cco_str}_{branches_str}_{datepicked_str}_{unique_id}"
            )
            _logger.info(f"This is the charts cache key: {cache_key}")
            cache_data = request.env["res.dashboard.cache"].get_cache(
                cache_key, user_id
            )
            if cache_data:
                return cache_data
            charts = request.env["res.dashboard.charts"].search(
                [("state", "=", "active")]
            )
            results = []
            for chart in charts:
                try:
                    _logger.info(f"Processing chart {chart.id}: {chart.name}")
                    if chart.use_materialized_view:
                        chart_data = self._get_chart_data_from_materialized_view(
                            chart, cco, branches_id
                        )
                    else:
                        chart_data = self._get_chart_data_from_direct_query(
                            chart, cco, branches_id
                        )
                    if chart_data:
                        results.append(chart_data)
                        _logger.info(
                            f"Chart {chart.id} returned {len(chart_data.get('labels', []))} results"
                        )
                except Exception as e:
                    _logger.error(f"Error processing chart {chart.id}: {e}")
                    results.append(
                        {
                            "id": chart.id,
                            "title": chart.name,
                            "type": chart.chart_type,
                            "error": str(e),
                            "labels": [],
                            "datasets": [{"data": [], "backgroundColor": []}],
                        }
                    )
            request.env["res.dashboard.cache"].set_cache(cache_key, results, user_id)
            _logger.info(f"Returning {len(results)} charts for user {user_id}")
            return results
        except Exception as e:
            _logger.error(f"Error in get_chart_data: {e}")
            return []

    def _get_chart_data_from_materialized_view(self, chart, cco, branches_id):
        """Get chart data from materialized view with strict branch security enforcement.

        Args:
            chart (record): The chart record to retrieve data from.
            cco (bool): Indicates if the user is a CCO.
            branches_id (list): List of branch IDs to filter data.

        Returns:
            dict: A dictionary containing chart data or an error message.
        """
        try:
            view_name = f"dashboard_chart_view_{chart.id}"
            with request.env.registry.cursor() as cr:
                cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                cr.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM pg_catalog.pg_class c
                        WHERE c.relname = %s AND c.relkind = 'm'
                    )
                """,
                    (view_name,),
                )
                view_exists = cr.fetchone()[0]
                if not view_exists:
                    _logger.warning(f"Materialized view {view_name} does not exist!")
                    success = (
                        request.env["dashboard.chart.view.refresher"]
                        .sudo()
                        .create_materialized_view_for_chart(chart.id)
                    )
                    if not success:
                        return self._get_chart_data_from_direct_query(
                            chart, cco, branches_id
                        )
                cr.execute(f"SELECT * FROM {view_name} LIMIT 0")
                columns = [desc[0] for desc in cr.description]
                if not columns:
                    cr.execute(f"SELECT * FROM {view_name} LIMIT 1")
                    columns = [desc[0] for desc in cr.description]
                _logger.info(f"View {view_name} columns: {columns}")
                if not columns:
                    return self._get_chart_data_from_direct_query(
                        chart, cco, branches_id
                    )
                if (
                    not cco
                    and not self.security_service.is_cco_user()
                    and not self.security_service.is_co_user()
                ):
                    user_branches = self.security_service.get_user_branch_ids()
                    effective_branches = []
                    if (
                        branches_id
                        and isinstance(branches_id, list)
                        and len(branches_id) > 0
                    ):
                        if user_branches:
                            effective_branches = [
                                b for b in branches_id if b in user_branches
                            ]
                            _logger.info(
                                f"UI branches {branches_id} intersected with user branches {user_branches} = {effective_branches}"
                            )
                        else:
                            effective_branches = branches_id
                    elif user_branches:
                        effective_branches = user_branches
                        _logger.info(
                            f"No UI filter, using user branches: {effective_branches}"
                        )
                    else:
                        _logger.warning(
                            f"Non-CCO/CO user with no branch access attempting to view chart {chart.id}"
                        )
                        return {
                            "id": chart.id,
                            "title": chart.name,
                            "type": chart.chart_type,
                            "labels": [],
                            "datasets": [{"data": [], "backgroundColor": []}],
                            "error": "No branch access",
                        }
                    if not effective_branches:
                        _logger.warning(
                            f"No effective branches for user on chart {chart.id}"
                        )
                        return {
                            "id": chart.id,
                            "title": chart.name,
                            "type": chart.chart_type,
                            "labels": [],
                            "datasets": [{"data": [], "backgroundColor": []}],
                        }
                    branch_col = None
                    if chart.branch_field and chart.branch_filter:
                        branch_col = self._find_branch_column_in_view(
                            columns, chart.branch_field
                        )
                        _logger.info(
                            f"Chart {chart.id} branch field: {chart.branch_field}, found column: {branch_col}"
                        )
                    if chart.branch_filter and not branch_col:
                        _logger.warning(
                            f"Branch filtering enabled but no branch column found in view for chart {chart.id}"
                        )
                        return self._get_chart_data_from_direct_query(
                            chart, cco, branches_id
                        )
                    query = f"SELECT * FROM {view_name}"
                    if branch_col:
                        if len(effective_branches) == 1:
                            query += f" WHERE {branch_col} = {effective_branches[0]}"
                        else:
                            query += (
                                f" WHERE {branch_col} IN {tuple(effective_branches)}"
                            )
                        _logger.info(
                            f"Applying branch security filter: {effective_branches}"
                        )
                else:
                    query = f"SELECT * FROM {view_name}"
                    if (
                        branches_id
                        and isinstance(branches_id, list)
                        and len(branches_id) > 0
                    ):
                        branch_col = (
                            self._find_branch_column_in_view(
                                columns, chart.branch_field
                            )
                            if chart.branch_field
                            else None
                        )
                        if branch_col:
                            if len(branches_id) == 1:
                                query += f" WHERE {branch_col} = {branches_id[0]}"
                            else:
                                query += f" WHERE {branch_col} IN {tuple(branches_id)}"
                            _logger.info(f"CCO/CO with UI filter: {branches_id}")
                sort_col = self._find_sort_column_in_view(columns, chart.y_axis_field)
                if sort_col:
                    query += f" ORDER BY {sort_col} DESC"
                query += " LIMIT 100"
                _logger.info(
                    f"Executing secure materialized view query for chart {chart.id}: {query}"
                )
                cr.execute("SET LOCAL statement_timeout = 30000")
                cr.execute(query)
                results = cr.dictfetchall()
                _logger.info(
                    f"Materialized view query returned {len(results)} rows for chart {chart.id}"
                )
                if (
                    not cco
                    and not self.security_service.is_cco_user()
                    and not self.security_service.is_co_user()
                    and results
                ):
                    if branch_col and effective_branches:
                        filtered_results = []
                        for row in results:
                            if (
                                branch_col in row
                                and row[branch_col] in effective_branches
                            ):
                                filtered_results.append(row)
                            else:
                                _logger.error(
                                    f"Security violation: Row with branch {row.get(branch_col)} found for user with branches {effective_branches}"
                                )
                        if len(filtered_results) != len(results):
                            _logger.error(
                                f"Security filter removed {len(results) - len(filtered_results)} unauthorized rows"
                            )
                        results = filtered_results
                chart_data_service = ChartDataService()
                return chart_data_service._extract_chart_data(chart, results, query)
        except Exception as e:
            _logger.error(f"Error getting chart from materialized view: {e}")
            return self._get_chart_data_from_direct_query(chart, cco, branches_id)

    def _find_branch_column_in_view(self, columns, preferred_field=None):
        """Dynamically find branch column from view columns using the chart's configured field.

        Args:
            columns (list): The list of columns in the view.
            preferred_field (str, optional): The preferred field to look for.

        Returns:
            str: The name of the branch column if found, otherwise None.
        """
        _logger.info(
            f"Looking for branch column in {columns} with preferred_field: {preferred_field}"
        )
        if preferred_field:
            field_name = (
                preferred_field.split(".")[-1]
                if "." in preferred_field
                else preferred_field
            )
            if field_name in columns:
                _logger.info(f"Found exact match: {field_name}")
                return field_name
            for col in columns:
                if col.lower() == field_name.lower():
                    _logger.info(f"Found case-insensitive match: {col}")
                    return col
        branch_patterns = ["branch_id", "id", "branch"]
        for pattern in branch_patterns:
            if pattern in columns:
                _logger.info(f"Found fallback pattern '{pattern}': {pattern}")
                return pattern
        for col in columns:
            if "branch" in col.lower():
                _logger.info(f"Found branch-related column: {col}")
                return col
        _logger.warning(f"No branch column found in {columns}")
        return None

    def _find_sort_column_in_view(self, columns, preferred_field=None):
        """Dynamically find sort column from view columns using the chart's configured field.

        Args:
            columns (list): The list of columns in the view.
            preferred_field (str, optional): The preferred field to look for.

        Returns:
            str: The name of the sort column if found, otherwise None.
        """
        if preferred_field:
            field_name = (
                preferred_field.split(".")[-1]
                if "." in preferred_field
                else preferred_field
            )
            if field_name in columns:
                return field_name
            for col in columns:
                if col.lower() == field_name.lower():
                    return col
        value_patterns = ["count", "amount", "total", "sum", "value", "risk", "hit"]
        for col in columns:
            col_lower = col.lower()
            for pattern in value_patterns:
                if pattern in col_lower:
                    return col
        return None

    def _get_chart_data_from_direct_query(self, chart, cco, branches_id):
        """Get chart data directly from the database with proper branch handling.

        Args:
            chart (record): The chart record to retrieve data from.
            cco (bool): Indicates if the user is a CCO.
            branches_id (list): List of branch IDs to filter data.

        Returns:
            dict: A dictionary containing chart data or an error message.
        """
        try:
            if self.security_service.is_co_user():
                cco = True
            secured_query = self.security_service.secure_chart_query(
                chart, cco, branches_id
            )
            _logger.info(
                f"Direct query for chart {chart.id} - cco: {cco}, branches_id: {branches_id}"
            )
            _logger.info(f"Secured query: {secured_query[:200]}...")
            with request.env.registry.cursor() as cr:
                try:
                    cr.execute("SET LOCAL statement_timeout = 15000;")
                    cr.execute(secured_query)
                    results = cr.dictfetchall()
                    _logger.info(
                        f"Direct query returned {len(results)} rows for chart {chart.id}"
                    )
                    chart_data_service = ChartDataService()
                    return chart_data_service._extract_chart_data(
                        chart, results, secured_query
                    )
                except Exception as query_error:
                    cr.rollback()
                    _logger.error(f"Query error for chart {chart.id}: {query_error}")
                    return {
                        "id": chart.id,
                        "title": chart.name,
                        "type": chart.chart_type,
                        "error": str(query_error),
                        "labels": [],
                        "datasets": [{"data": [], "backgroundColor": []}],
                    }
        except Exception as e:
            _logger.error(f"Error executing chart query: {e}")
            return {
                "id": chart.id,
                "title": chart.name,
                "type": chart.chart_type,
                "error": str(e),
                "labels": [],
                "datasets": [{"data": [], "backgroundColor": []}],
            }

    @http.route("/dashboard/chart_view_refresh/", type="json", auth="user")
    def refresh_chart_views(self, **kw):
        """Manually refresh chart materialized views - admin only.

        Returns:
            dict: A success message or an error message if the user is not authorized.
        """
        if not request.env.user.has_group("base.group_system"):
            return {"error": "Only administrators can refresh chart views"}
        try:
            charts = request.env["res.dashboard.charts"].search(
                [("state", "=", "active"), ("use_materialized_view", "=", True)]
            )
            refreshed_count = 0
            for chart in charts:
                if request.env["dashboard.chart.view.refresher"].refresh_chart_view(
                    chart.id
                ):
                    refreshed_count += 1
            return {
                "success": True,
                "message": f"Successfully refreshed {refreshed_count} chart views",
            }
        except Exception as e:
            _logger.error(f"Error refreshing chart views: {e}")
            return {"error": str(e)}
