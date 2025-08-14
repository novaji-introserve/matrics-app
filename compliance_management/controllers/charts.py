# -*- coding: utf-8 -*-

import psycopg2
from odoo import http
from odoo.http import request
import json
import logging
import time

from ..services.chart_data_service import ChartDataService
from ..services.security_service import SecurityService
from ..services.database_service import DatabaseService
from ..services.cache_service import CacheService
from ..services.materialized_view import MaterializedViewService
from ..utils.cache_key_unique_identifier import generate_cache_key
from ..utils.cache_key_unique_identifier import get_unique_client_identifier, normalize_cache_key_components
from ..services.query_service import QueryService

_logger = logging.getLogger(__name__)

class DynamicChartController(http.Controller):
    """Controller for handling dynamic chart requests with security and performance"""

    def __init__(self):
        """
        Initialize the DynamicChartController.

        This setup includes initializing services and utility functions.
        """
        super(DynamicChartController, self).__init__()
        self.security_service = SecurityService()
        self.chart_data_service = ChartDataService(request.env if request else None)
        self.database_service = DatabaseService(request.env if request else None)
        self.cache_service = CacheService(request.env if request else None)
        self.materialized_view_service = MaterializedViewService(request.env if request else None)
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
            # Validate all input parameters
            try:
                # Sanitize and validate input parameters
                sanitized_data = self.security_service.validate_and_sanitize_request_data({
                    'chart_type': chart_type,
                    'query': query,
                    'x_axis_field': x_axis_field,
                    'y_axis_field': y_axis_field,
                    'color_scheme': color_scheme
                })
                chart_type = sanitized_data.get('chart_type', chart_type)
                query = sanitized_data.get('query', query)
                x_axis_field = sanitized_data.get('x_axis_field', x_axis_field)
                y_axis_field = sanitized_data.get('y_axis_field', y_axis_field)
                color_scheme = sanitized_data.get('color_scheme', color_scheme)
            except Exception as e:
                self.security_service.log_security_event(
                    "CHART_PREVIEW_INPUT_VALIDATION_FAILED",
                    f"Chart preview endpoint input validation failed: {str(e)}"
                )
                return {"error": "Request validation failed"}
            # Use SecurityService for comprehensive SQL validation
            security_service = SecurityService()
            is_safe, error_msg = security_service.validate_sql_query(query)
            if not is_safe:
                security_service.log_security_event(
                    "CHART_SQL_INJECTION_ATTEMPT",
                    f"Blocked dangerous chart query: {error_msg} - Query: {query[:200]}..."
                )
                return {"error": "Request validation failed"}
                
            chart_data_service = ChartDataService(request.env)
                
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
            
            # Use secure query execution
            security_service = SecurityService()
            db_service = DatabaseService(request.env)
            
            # Additional validation for the secured query
            is_query_safe, query_error = security_service.validate_sql_query(secured_query)
            if not is_query_safe:
                return {"error": "Request validation failed"}
            
            success, results, execution_time = db_service.execute_query_with_timeout(
                secured_query, timeout=10000
            )
            
            if success and results:
                chart_data = chart_data_service._extract_chart_data(
                    chart, results, secured_query
                )
                chart_data["execution_time_ms"] = round(execution_time, 2)
                return chart_data
            else:
                _logger.error(f"Error in preview_chart: {results}")
                return {"error": "Request validation failed"}
        except Exception as e:
            _logger.error(f"Error in preview_chart: {e}")
            return {"error": "Request validation failed"}

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
            
        # Validate all input parameters
        try:
            # Sanitize and validate input parameters
            sanitized_data = self.security_service.validate_and_sanitize_request_data({
                'chart_id': chart_id,
                'page': page,
                'page_size': page_size,
                'cco': cco,
                'branches_id': branches_id,
                **kw
            })
            chart_id = sanitized_data.get('chart_id', chart_id)
            page = sanitized_data.get('page', page)
            page_size = sanitized_data.get('page_size', page_size)
            cco = sanitized_data.get('cco', cco)
            branches_id = sanitized_data.get('branches_id', branches_id)
        except Exception as e:
            self.security_service.log_security_event(
                "CHART_PAGE_INPUT_VALIDATION_FAILED",
                f"Chart page endpoint input validation failed: {str(e)}"
            )
            return {"error": "Request validation failed"}
            
        try:
            chart_id = int(chart_id)
            page = max(0, int(page))
            page_size = min(100, max(1, int(page_size)))
        except (ValueError, TypeError):
            return {"error": "Request validation failed"}
            
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
        
        cache_service = CacheService(request.env)
        cache_data = cache_service.get_cache(cache_key, user_id)
        
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
                return {"error": "Request validation failed"}
                
        return {"error": "Request validation failed"}

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
        
        db_service = DatabaseService(request.env)
        query_service = QueryService()
        chart_data_service = ChartDataService(request.env)
        cache_service = CacheService(request.env)
        
        for retry_attempt in range(max_retries):
            try:
                with request.env.registry.cursor() as cr:
                    isolation_level = retry_params.get_param(
                        "chart.view.isolation_level", default="READ COMMITTED"
                    )
                    cr.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                    
                    if not db_service.check_view_exists(view_name):
                        _logger.warning(
                            f"Materialized view {view_name} does not exist (attempt {retry_attempt+1}/{max_retries})"
                        )
                        refresher = request.env["dashboard.chart.view.refresher"].sudo()
                        created = refresher.create_materialized_view_for_chart(chart.id)
                        if not created:
                            _logger.error(f"Failed to create materialized view {view_name}")
                            if retry_attempt == max_retries - 1:
                                return self._get_chart_from_direct_query(
                                    chart, page, page_size, cco, branches_id, cache_key, user_id
                                )
                            delay = base_delay * (retry_attempt + 1)
                            time.sleep(delay)
                            continue
                        delay = base_delay * (retry_attempt + 1)
                        time.sleep(delay)
                        continue
                        
                    try:
                        _logger.info(f"Querying materialized view {view_name} directly (attempt {retry_attempt+1})")
                        
                        columns = db_service.get_table_columns(view_name)
                        if not columns:
                            _logger.warning(
                                f"View {view_name} exists but returned no columns (attempt {retry_attempt+1}/{max_retries})"
                            )
                            if retry_attempt == max_retries - 1:
                                _logger.info(f"Last attempt - forcing view recreation")
                                cr.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
                                cr.commit()
                                refresher = request.env["dashboard.chart.view.refresher"].sudo()
                                refresher.create_materialized_view_for_chart(chart.id)
                                delay = base_delay * 2 * (retry_attempt + 1)
                                time.sleep(delay)
                                continue
                            delay = base_delay * (retry_attempt + 1)
                            time.sleep(delay)
                            continue
                            
                        _logger.info(f"Successfully found {len(columns)} columns in {view_name}: {columns}")
                        
                        branch_col = query_service.find_branch_column_in_view(columns, chart.branch_field)
                            
                        sort_col = query_service.find_sort_column_in_view(columns, chart.y_axis_field)
                        
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
                                    effective_branches = [b for b in branches_id if b in user_branches]
                                else:
                                    effective_branches = branches_id
                            elif user_branches:
                                effective_branches = user_branches
                                
                            if effective_branches and branch_col:
                                if len(effective_branches) == 1:
                                    query += f" WHERE {branch_col} = {effective_branches[0]}"
                                else:
                                    query += f" WHERE {branch_col} IN {tuple(effective_branches)}"
                            elif branch_col:
                                query += " WHERE 1=0"
                        
                        # Validate the dynamically built query before execution
                        security_service = SecurityService()
                        is_safe, error_msg = security_service.validate_sql_query(query)
                        if not is_safe:
                            security_service.log_security_event(
                                "CHART_DYNAMIC_QUERY_BLOCKED",
                                f"Dynamically built query blocked: {error_msg} - Query: {query[:200]}..."
                            )
                            _logger.error(f"Unsafe dynamically built query blocked: {error_msg}")
                            return {"error": "Request validation failed"}
                                
                        count_query = f"SELECT COUNT(*) FROM ({query}) AS count_query"
                        # Validate count query as well
                        is_count_safe, count_error = security_service.validate_sql_query(count_query)
                        if not is_count_safe:
                            security_service.log_security_event(
                                "CHART_COUNT_QUERY_BLOCKED",
                                f"Count query blocked: {count_error}"
                            )
                            return {"error": "Request validation failed"}
                            
                        cr.execute(count_query)
                        total_count = cr.fetchone()[0]
                        
                        if sort_col:
                            query += f" ORDER BY {sort_col} DESC"
                        query += f" LIMIT {page_size} OFFSET {page * page_size}"
                        
                        # Validate final query with pagination
                        is_final_safe, final_error = security_service.validate_sql_query(query)
                        if not is_final_safe:
                            security_service.log_security_event(
                                "CHART_PAGINATED_QUERY_BLOCKED",
                                f"Paginated query blocked: {final_error}"
                            )
                            return {"error": "Request validation failed"}
                        
                        timeout = int(retry_params.get_param("chart.view.query_timeout", default=30000))
                        cr.execute(f"SET LOCAL statement_timeout = {timeout}")
                        cr.execute(query)
                        results = cr.dictfetchall()
                        
                        chart_data = chart_data_service._extract_chart_data(chart, results, query)
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
                        
                        cache_service.set_cache(cache_key, chart_data, user_id)
                        return chart_data
                    except Exception as query_error:
                        _logger.warning(
                            f"Error querying view {view_name}: {query_error}, attempt {retry_attempt+1}/{max_retries}"
                        )
                        if retry_attempt == max_retries - 1:
                            try:
                                _logger.info(f"Attempting to drop and recreate view {view_name}")
                                cr.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
                                cr.commit()
                                refresher = request.env["dashboard.chart.view.refresher"].sudo()
                                created = refresher.create_materialized_view_for_chart(chart.id)
                                if not created:
                                    _logger.error(f"Failed to recreate view {view_name}")
                                    return self._get_chart_from_direct_query(
                                        chart, page, page_size, cco, branches_id, cache_key, user_id
                                    )
                                delay = base_delay * 2 * (retry_attempt + 1)
                                time.sleep(delay)
                            except Exception as drop_error:
                                _logger.error(f"Error dropping view {view_name}: {drop_error}")
                                return self._get_chart_from_direct_query(
                                    chart, page, page_size, cco, branches_id, cache_key, user_id
                                )
            except Exception as e:
                _logger.error(f"Transaction error in attempt {retry_attempt+1}/{max_retries}: {e}")
                delay = base_delay * (retry_attempt + 1)
                time.sleep(delay)
                
        _logger.error(f"All {max_retries} attempts failed for chart {chart.id}, falling back to direct query")
        return self._get_chart_from_direct_query(
            chart, page, page_size, cco, branches_id, cache_key, user_id
        )

    def _get_chart_from_direct_query(
        self, chart, page, page_size, cco, branches_id, cache_key, user_id
    ):
        """Get chart data directly from the database.

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
        try:
            chart_data_service = ChartDataService(request.env)
            result = chart_data_service.get_chart_data_from_direct_query(chart, cco, branches_id)
            
            result["pagination"] = {
                "total": len(result.get("labels", [])),
                "page": page,
                "page_size": page_size,
                "pages": 1,
            }
            
            cache_service = CacheService(request.env)
            cache_service.set_cache(cache_key, result, user_id)
            
            return result
        except Exception as e:
            _logger.error(f"Error in _get_chart_from_direct_query: {e}")
            return {
                "id": chart.id,
                "title": chart.name,
                "type": chart.chart_type,
                "error": "Request validation failed",
                "labels": [],
                "datasets": [{"data": [], "backgroundColor": []}],
                "pagination": {
                    "total": 0,
                    "page": page,
                    "page_size": page_size,
                    "pages": 0,
                },
            }

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
            # Validate all input parameters
            try:
                # Sanitize and validate input parameters
                sanitized_data = self.security_service.validate_and_sanitize_request_data({
                    'cco': cco,
                    'branches_id': branches_id,
                    **kw
                })
                cco = sanitized_data.get('cco', cco)
                branches_id = sanitized_data.get('branches_id', branches_id)
            except Exception as e:
                self.security_service.log_security_event(
                    "CHARTS_DATA_INPUT_VALIDATION_FAILED",
                    f"Charts data endpoint input validation failed: {str(e)}"
                )
                return []
                
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
                cco = True
            elif actual_is_cco and cco:
                _logger.warning(f"CCO user {user_id} accessing charts")
                cco = True
            elif not actual_is_cco and cco:
                _logger.warning(f"Non-CCO/CO user {user_id} attempted to use CCO parameter")
                cco = False
                
            _logger.info(
                f"Chart data requested - cco={cco}, branches_id={branches_id}, user_id={user_id}, "
                f"actual_is_cco={actual_is_cco}, actual_is_co={actual_is_co}"
            )
            
            unique_id = self.get_unique_client_identifier()
            cco_str, branches_str, datepicked_str, unique_id = self.normalize_cache_key_components(
                cco, branches_id, datepicked, unique_id
            )
            cache_key = f"charts_data_{cco_str}_{branches_str}_{datepicked_str}_{unique_id}"
            _logger.info(f"This is the charts cache key: {cache_key}")
            
            cache_service = CacheService(request.env)
            cache_data = cache_service.get_cache(cache_key, user_id)
            
            if cache_data:
                return cache_data
                
            charts = request.env["res.dashboard.charts"].search([("state", "=", "active")])
            results = []
            
            chart_data_service = ChartDataService(request.env)
            for chart in charts:
                try:
                    _logger.info(f"Processing chart {chart.id}: {chart.name}")
                    
                    if chart.use_materialized_view:
                        chart_data = chart_data_service.get_chart_data_from_materialized_view(
                            chart, cco, branches_id
                        )
                    else:
                        chart_data = chart_data_service.get_chart_data_from_direct_query(
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
                            "error": "Request validation failed",
                            "labels": [],
                            "datasets": [{"data": [], "backgroundColor": []}],
                        }
                    )
                    
            cache_service.set_cache(cache_key, results, user_id)
            _logger.info(f"Returning {len(results)} charts for user {user_id}")
            
            return results
        except Exception as e:
            _logger.error(f"Error in get_chart_data: {e}")
            return []

    @http.route("/dashboard/chart_view_refresh/", type="json", auth="user")
    def refresh_chart_views(self, **kw):
        """Manually refresh chart materialized views - admin only.

        Returns:
            dict: A success message or an error message if the user is not authorized.
        """
        if not request.env.user.has_group("base.group_system"):
            return {"error": "Only administrators can refresh chart views"}
            
        try:
            mv_service = MaterializedViewService(request.env)
            result = mv_service.refresh_all_chart_views()
            
            return {
                "success": True,
                "message": f"Successfully refreshed {result.get('refreshed', 0)} chart views",
                "errors": result.get('errors', 0),
            }
        except Exception as e:
            _logger.error(f"Error refreshing chart views: {e}")
            return {"error": str(e)}

    def generate_cache_key(self, prefix, params):
        """Generate a cache key for the given prefix and parameters.
        
        Args:
            prefix (str): The prefix for the cache key.
            params (dict): The parameters to include in the key.
            
        Returns:
            str: The generated cache key.
        """
        return generate_cache_key(prefix, params)
    