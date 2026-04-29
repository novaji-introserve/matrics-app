# -*- coding: utf-8 -*-

import psycopg2
from odoo import http
from odoo.http import request
import json
import logging
import time
from datetime import datetime, timedelta
from odoo import fields

from ..services.chart_data_service import ChartDataService
from ..services.security_service import SecurityService
from ..services.database_service import DatabaseService
from ..services.cache_service import CacheService
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
        self.get_unique_client_identifier = get_unique_client_identifier
        self.normalize_cache_key_components = normalize_cache_key_components
        self.debug_mode = True

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
        datepicked = self._normalize_dashboard_period(kw.get("datepicked", 7))

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

                return self._get_chart_from_direct_query(
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

    def _get_chart_from_direct_query(
        self, chart, page, page_size, cco, branches_id, cache_key, user_id
    ):
        """Get chart data from the chart payload JSON cache or live SQL.

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
            datepicked = self._normalize_dashboard_period(
                request.params.get("datepicked", 7)
            )
            start_at, end_at = self._dashboard_date_range(datepicked)
            result = chart._get_paginated_dashboard_chart_payload(
                chart_data_service,
                cco=cco,
                branches_id=branches_id,
                datepicked=datepicked,
                start_at=start_at,
                end_at=end_at,
                page=page,
                page_size=page_size,
            )

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
            datepicked = self._normalize_dashboard_period(kw.get("datepicked", 7))
            start_at, end_at = self._dashboard_date_range(datepicked)

            actual_is_cco = self.security_service.is_cco_user()
            actual_is_co = self.security_service.is_co_user()

            if actual_is_co:
                # _logger.info(f"CO user {user_id} accessing charts")
                cco = True
            elif actual_is_cco and cco:
                # _logger.warning(f"CCO user {user_id} accessing charts")
                cco = True
            elif not actual_is_cco and cco:
                # _logger.warning(f"Non-CCO/CO user {user_id} attempted to use CCO parameter")
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
            if not charts:
                charts = self._get_default_compliance_chart_records()
            results = []


            chart_data_service = ChartDataService(request.env)
            snapshot_model = request.env['dashboard.snapshot']

            for chart in charts:
                try:
                    _logger.info(f"Processing chart {chart.id}: {chart.name}")

                    chart_data = chart._get_dashboard_chart_payload(
                        chart_data_service,
                        cco=cco,
                        branches_id=branches_id,
                        datepicked=datepicked,
                        start_at=start_at,
                        end_at=end_at,
                    )

                    if chart_data:
                        results.append(chart_data)
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

    def generate_cache_key(self, prefix, params):
        """Generate a cache key for the given prefix and parameters.

        Args:
            prefix (str): The prefix for the cache key.
            params (dict): The parameters to include in the key.

        Returns:
            str: The generated cache key.
        """
        return generate_cache_key(prefix, params)

