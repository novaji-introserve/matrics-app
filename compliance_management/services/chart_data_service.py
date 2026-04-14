# -*- coding: utf-8 -*-

import logging

from ..services.security_service import SecurityService
from ..services.database_service import DatabaseService
from ..services.query_service import QueryService

_logger = logging.getLogger(__name__)

class ChartDataService:
    """Service for generating and processing chart data with improved security and performance"""

    def __init__(self, env=None):
        """Initialize the ChartDataService.

        Args:
            env (Environment, optional): The Odoo environment. Defaults to None.
        """
        self.env = env
        from ..utils.color_generator import ColorGenerator
        self.color_generator = ColorGenerator()

    @staticmethod
    def _default_chart_title(chart):
        code = getattr(chart, "code", False)
        title_map = {
            "top_accounts": "Top 10 Branch by Accounts Opened",
            "top_high_risk_accounts": "Top 10 High Risk Branch by Accounts",
            "top_customer_risk_rules": "Top 10 Customer Risk Rules",
            "top_transaction_exceptions": "Top Transaction Exception",
            "cases_by_status": "Cases by Status",
            "cases_by_exceptions": "Cases by Exceptions",
        }
        return getattr(chart, "name", False) or title_map.get(code) or "Dashboard Chart"

    def _extract_chart_data(
        self,
        chart,
        results,
        query,
        *,
        cco=False,
        branches_id=None,
        datepicked=7,
        start_at=None,
        end_at=None,
    ):
        """Extract chart data from query results"""
        chart_identifier = chart.code or chart.id
        chart_title = self._default_chart_title(chart)
        if not results:
            return {
                "id": chart_identifier,
                "title": chart_title,
                "display_summary": chart.display_summary or chart.description or "",
                "type": chart.chart_type,
                "labels": [],
                "datasets": [{"data": [], "backgroundColor": []}],
            }
            
        x_field = chart.x_axis_field or "name"
        if x_field not in results[0]:
            x_field = next(iter(results[0]))
            
        y_field = chart.y_axis_field
        if not y_field or y_field not in results[0]:
            for field in results[0].keys():
                if (
                    field != x_field
                    and field != "id"
                    and isinstance(results[0][field], (int, float))
                ):
                    y_field = field
                    break
            if not y_field:
                y_field = next(
                    (k for k in results[0].keys() if k != x_field and k != "id"), None
                )

        id_field = chart.navigation_value_field or "id"
        if id_field not in results[0]:
            id_field = next((k for k in results[0].keys() if k.endswith("_id")), None)
            
        ids = (
            [r.get(id_field) for r in results]
            if id_field and id_field in results[0]
            else []
        )

        if not y_field:
            _logger.error(f"Cannot determine Y-axis field for chart {chart.id}")
            return {
                "id": chart_identifier,
                "title": chart_title,
                "display_summary": chart.display_summary or chart.description or "",
                "type": chart.chart_type,
                "error": "Cannot determine Y-axis field from query results",
                "labels": [],
                "datasets": [{"data": [], "backgroundColor": []}],
            }
            
        labels = [str(r.get(x_field, "")) for r in results]
        values = []
        for r in results:
            val = r.get(y_field)
            try:
                values.append(float(val) if val is not None else 0)
            except (ValueError, TypeError):
                values.append(0)

        additional_domain = chart._build_dashboard_navigation_domain(
            cco=cco,
            branches_id=branches_id or [],
            datepicked=datepicked,
            start_at=start_at,
            end_at=end_at,
        )

        colors = self.color_generator._generate_colors(chart.color_scheme, len(results))
        
        return {
            "id": chart_identifier,
            "record_id": chart.id,
            "title": chart_title,
            "display_summary": chart.display_summary or chart.description or "",
            "type": chart.chart_type,
            "model_name": chart.target_model,
            "filter": chart.navigation_filter_field or chart.domain_field,
            "column": chart.column,
            "labels": labels,
            "ids": ids,
            "datefield": chart.date_field,
            "additional_domain": additional_domain,
            "datasets": [
                {
                    "label": chart_title,
                    "data": values,
                    "backgroundColor": colors,
                    "borderColor": (
                        colors if chart.chart_type in ["line", "radar"] else []
                    ),
                    "borderWidth": 1,
                }
            ],
        }

    def _apply_dashboard_date_filter_to_query(self, chart, query, datepicked, start_at, end_at):
        if (
            not chart.date_filter
            or not chart.date_field
            or datepicked not in (0, 1, 7, 30)
            or not start_at
            or not end_at
        ):
            return query
        sanitized_query = (query or "").strip()
        had_semicolon = sanitized_query.endswith(";")
        if had_semicolon:
            sanitized_query = sanitized_query[:-1]
        date_condition = f"{chart.date_field} BETWEEN '{start_at}' AND '{end_at}'"
        filtered_query = QueryService.add_condition_to_query(sanitized_query, date_condition)
        if had_semicolon and not filtered_query.endswith(";"):
            filtered_query += ";"
        return filtered_query

    def get_dashboard_chart_data(
        self, chart, cco, branches_id, datepicked=7, start_at=None, end_at=None
    ):
        if not self.env:
            return {
                "id": chart.code or chart.id,
                "title": self._default_chart_title(chart),
                "display_summary": chart.display_summary or chart.description or "",
                "type": chart.chart_type,
                "error": "No environment provided",
                "labels": [],
                "datasets": [{"data": [], "backgroundColor": []}],
            }

        try:
            security_service = SecurityService()
            db_service = DatabaseService(self.env)

            if security_service.is_co_user():
                cco = True

            secured_query = security_service.secure_chart_query(chart, cco, branches_id)
            secured_query = self._apply_dashboard_date_filter_to_query(
                chart, secured_query, datepicked, start_at, end_at
            )

            is_safe, error_msg = security_service.validate_sql_query(secured_query)
            if not is_safe:
                security_service.log_security_event(
                    "DASHBOARD_CHART_SQL_INJECTION",
                    f"Unsafe dashboard chart query: {error_msg} - Query: {secured_query[:200]}..."
                )
                return {
                    "id": chart.code or chart.id,
                    "title": self._default_chart_title(chart),
                    "display_summary": chart.display_summary or chart.description or "",
                    "type": chart.chart_type,
                    "error": "Request validation failed",
                    "labels": [],
                    "datasets": [{"data": [], "backgroundColor": []}],
                }

            success, results, execution_time = db_service.execute_query_with_timeout(
                secured_query, timeout=15000
            )
            if success and results:
                chart_data = self._extract_chart_data(
                    chart,
                    results,
                    secured_query,
                    cco=cco,
                    branches_id=branches_id,
                    datepicked=datepicked,
                    start_at=start_at,
                    end_at=end_at,
                )
                chart_data["execution_time_ms"] = round(execution_time, 2)
                db_service.record_execution_stats(chart.id, execution_time, "success")
                return chart_data

            db_service.record_execution_stats(chart.id, 0, "error", results)
            return {
                "id": chart.code or chart.id,
                "title": self._default_chart_title(chart),
                "display_summary": chart.display_summary or chart.description or "",
                "type": chart.chart_type,
                "error": "Request validation failed",
                "labels": [],
                "datasets": [{"data": [], "backgroundColor": []}],
            }
        except Exception as e:
            _logger.error(f"Error executing dashboard chart query: {e}")
            return {
                "id": chart.code or chart.id,
                "title": self._default_chart_title(chart),
                "display_summary": chart.display_summary or chart.description or "",
                "type": chart.chart_type,
                "error": "Request validation failed",
                "labels": [],
                "datasets": [{"data": [], "backgroundColor": []}],
            }

    def _is_safe_query(self, query):
        """Check if a query is safe to execute"""
        return QueryService.is_safe_query(query)

    def get_chart_data_from_direct_query(self, chart, cco, branches_id):
        """Get chart data directly from the database with proper branch handling.

        Args:
            chart (record): The chart record to retrieve data from.
            cco (bool): Indicates if the user is a CCO.
            branches_id (list): List of branch IDs to filter by.

        Returns:
            dict: A dictionary containing chart data or an error message.
        """
        if not self.env:
            return {
                "id": chart.id,
                "title": chart.name,
                "type": chart.chart_type,
                "error": "No environment provided",
                "labels": [],
                "datasets": [{"data": [], "backgroundColor": []}],
            }
            
        try:
            security_service = SecurityService()
            db_service = DatabaseService(self.env)
            
            if security_service.is_co_user():
                cco = True
                
            secured_query = security_service.secure_chart_query(chart, cco, branches_id)
            _logger.info(f"Direct query for chart {chart.id} - cco: {cco}, branches_id: {branches_id}")
            _logger.info(f"Secured query: {secured_query[:200]}...")
            
            # Additional validation of the secured query
            is_safe, error_msg = security_service.validate_sql_query(secured_query)
            if not is_safe:
                security_service.log_security_event(
                    "CHART_DIRECT_QUERY_SQL_INJECTION",
                    f"Unsafe secured query: {error_msg} - Query: {secured_query[:200]}..."
                )
                return {
                    "id": chart.id,
                    "title": chart.name,
                    "type": chart.chart_type,
                    "error": "Request validation failed",
                    "labels": [],
                    "datasets": [{"data": [], "backgroundColor": []}],
                }
            
            success, results, execution_time = db_service.execute_query_with_timeout(secured_query, timeout=15000)
            
            if success and results:
                _logger.info(f"Direct query returned {len(results)} rows for chart {chart.id}")
                chart_data = self._extract_chart_data(chart, results, secured_query)
                chart_data["execution_time_ms"] = round(execution_time, 2)
                db_service.record_execution_stats(chart.id, execution_time, "success")
                return chart_data
            else:
                _logger.error(f"Query error for chart {chart.id}: {results}")
                db_service.record_execution_stats(chart.id, 0, "error", results)
                return {
                    "id": chart.id,
                    "title": chart.name,
                    "type": chart.chart_type,
                    "error": "Request validation failed",
                    "labels": [],
                    "datasets": [{"data": [], "backgroundColor": []}],
                }
        except Exception as e:
            _logger.error(f"Error executing chart query: {e}")
            return {
                "id": chart.id,
                "title": chart.name,
                "type": chart.chart_type,
                "error": "Request validation failed",
                "labels": [],
                "datasets": [{"data": [], "backgroundColor": []}],
            }
            
