# -*- coding: utf-8 -*-

import logging
import re
from odoo import tools

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
    def _get_view_name_for_chart(chart_id):
        """Generate a consistent view name for a chart - accepts either chart object or ID"""
        if isinstance(chart_id, int):
            return f"dashboard_chart_view_{chart_id}"
        else:
            return f"dashboard_chart_view_{chart_id.id}"

    def _extract_chart_data(self, chart, results, query):
        """Extract chart data from query results"""
        if not results:
            return {
                "id": chart.id,
                "title": chart.name,
                "type": chart.chart_type,
                "labels": [],
                "datasets": [{"data": [], "backgroundColor": []}],
            }
            
        # Find X and Y fields in results
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
                
        # Find ID field
        id_field = "id"
        if id_field not in results[0]:
            id_field = next((k for k in results[0].keys() if k.endswith("_id")), None)
            
        ids = (
            [r.get(id_field) for r in results]
            if id_field and id_field in results[0]
            else []
        )
        
        # Check if Y field was found
        if not y_field:
            _logger.error(f"Cannot determine Y-axis field for chart {chart.id}")
            return {
                "id": chart.id,
                "title": chart.name,
                "type": chart.chart_type,
                "error": "Cannot determine Y-axis field from query results",
                "labels": [],
                "datasets": [{"data": [], "backgroundColor": []}],
            }
            
        # Extract labels and values
        labels = [str(r.get(x_field, "")) for r in results]
        values = []
        for r in results:
            val = r.get(y_field)
            try:
                values.append(float(val) if val is not None else 0)
            except (ValueError, TypeError):
                values.append(0)
                
        # Extract additional domain if needed
        additional_domain = []
        if chart.query:
            original_query = chart.query.upper()
            where_clause = ""
            if "WHERE" in original_query:
                where_start = original_query.find("WHERE") + 5
                where_end = -1
                for clause in ["GROUP BY", "ORDER BY", "LIMIT"]:
                    clause_pos = original_query.find(clause, where_start)
                    if clause_pos > -1 and (where_end == -1 or clause_pos < where_end):
                        where_end = clause_pos
                where_clause = original_query[
                    where_start : where_end if where_end > -1 else None
                ].strip()
                if where_clause:
                    conditions = where_clause.split("AND")
                    for condition in conditions:
                        condition = condition.strip()
                        if "=" in condition and "." in condition:
                            parts = condition.split("=")
                            field_part = parts[0].strip()
                            value_part = parts[1].strip()
                            if "." in field_part:
                                table, field = field_part.split(".")
                                if "'" in value_part:
                                    value = value_part.replace("'", "").lower()
                                    additional_domain.append(
                                        (field.lower(), "=", value)
                                    )
                                elif value_part.isdigit():
                                    value = int(value_part)
                                    additional_domain.append(
                                        (field.lower(), "=", value)
                                    )
                                    
        # Generate colors
        colors = self.color_generator._generate_colors(chart.color_scheme, len(results))
        
        return {
            "id": chart.id,
            "title": chart.name,
            "type": chart.chart_type,
            "model_name": chart.target_model,
            "filter": chart.domain_field,
            "column": chart.column,
            "labels": labels,
            "ids": ids,
            "datefield": chart.date_field,
            "additional_domain": additional_domain,
            "datasets": [
                {
                    "data": values,
                    "backgroundColor": colors,
                    "borderColor": (
                        colors if chart.chart_type in ["line", "radar"] else []
                    ),
                    "borderWidth": 1,
                }
            ],
        }

    def _is_safe_query(self, query):
        """Check if a query is safe to execute"""
        from ..services.query_service import QueryService
        return QueryService.is_safe_query(query)

    def get_chart_data_from_materialized_view(self, chart, cco, branches_id):
        """Get chart data from the materialized view with improved column detection.
        
        Args:
            chart (record): The chart record to retrieve data from.
            cco (bool): Indicates if the user is a CCO.
            branches_id (list): List of branch IDs to filter data.
            
        Returns:
            dict: The chart data extracted from the materialized view.
        """
        from ..services.query_service import QueryService
        from ..services.database_service import DatabaseService
        from ..services.security_service import SecurityService
        
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
            view_name = f"dashboard_chart_view_{chart.id}"
            db_service = DatabaseService(self.env)
            security_service = SecurityService()
            
            # Check if view exists
            view_exists = db_service.check_view_exists(view_name)
            if not view_exists:
                _logger.warning(f"Materialized view {view_name} does not exist - creating it")
                success = self.env["dashboard.chart.view.refresher"].sudo().create_materialized_view_for_chart(chart.id)
                if not success:
                    _logger.error(f"Failed to create materialized view for chart {chart.id}")
                    return self.get_chart_data_from_direct_query(chart, cco, branches_id)
                    
            # Get columns from view
            columns = db_service.get_table_columns(view_name)
            if not columns:
                _logger.error(f"No columns found in materialized view {view_name}")
                return self.get_chart_data_from_direct_query(chart, cco, branches_id)
                
            # Find branch column
            branch_col = QueryService.find_branch_column_in_view(columns, chart.branch_field)
            
            # Build query
            query = f"SELECT * FROM {view_name}"
            
            # Apply branch filtering if needed
            if (
                chart.branch_field
                and not cco
                and not security_service.is_cco_user()
            ):
                user_branches = security_service.get_user_branch_ids()
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
                    
            # Apply sorting
            sort_col = QueryService.find_sort_column_in_view(columns, chart.y_axis_field)
            if sort_col:
                query += f" ORDER BY {sort_col} DESC"
                
            # Apply limit
            query += " LIMIT 100"
            
            # Execute query
            success, results, _ = db_service.execute_query_with_timeout(query, timeout=30000)
            if not success or not results:
                _logger.error(f"Error executing materialized view query for chart {chart.id}")
                return self.get_chart_data_from_direct_query(chart, cco, branches_id)
                
            # Post-process results for additional security
            if (
                not cco
                and not security_service.is_cco_user()
                and not security_service.is_co_user()
                and results
            ):
                if branch_col and 'effective_branches' in locals():
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
                    
            # Extract chart data
            return self._extract_chart_data(chart, results, query)
        except Exception as e:
            _logger.error(f"Error getting chart from materialized view: {e}")
            return self.get_chart_data_from_direct_query(chart, cco, branches_id)

    def get_chart_data_from_direct_query(self, chart, cco, branches_id):
        """Get chart data directly from the database with proper branch handling.

        Args:
            chart (record): The chart record to retrieve data from.
            cco (bool): Indicates if the user is a CCO.
            branches_id (list): List of branch IDs to filter by.

        Returns:
            dict: A dictionary containing chart data or an error message.
        """
        from ..services.security_service import SecurityService
        from ..services.database_service import DatabaseService
        
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
                    "error": results,
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