import logging
import re
from odoo import tools
from odoo import api
from odoo.http import request
from ..utils.color_generator import ColorGenerator

_logger = logging.getLogger(__name__)

class ChartDataService:
    """Service for generating and processing chart data with improved security and performance"""
    
    def __init__(self):
        # Import dependencies here to avoid circular imports
        from ..utils.color_generator import ColorGenerator
        from ..utils.sql_parser import SQLParser
        from ..services.branch_security import ChartSecurityService
        from ..services.materialized_view import MaterializedViewService
        from ..services.dynamic_query_builder import DynamicQueryBuilder
        
        self.color_generator = ColorGenerator()
        self.sql_parser = SQLParser()
        self.security_service = ChartSecurityService()
        self.query_builder = DynamicQueryBuilder()
        
        # Initialize materialized view service with a new environment
        self.materialized_view_service = None
        # Will be initialized on first use with proper environment
    
    def _ensure_view_service(self):
        """Ensure materialized view service is initialized with proper environment"""
        if not self.materialized_view_service and request and request.env:
            from ..services.materialized_view import MaterializedViewService
            self.materialized_view_service = MaterializedViewService(request.env)
    
    def process_multiple_charts(self, charts, cco, branches_id):
        """Process multiple charts in batch with security and optimization"""
        # Pre-filter to only active charts
        active_charts = charts.filtered(lambda c: c.state == 'active')
        
        if not active_charts:
            return []
        
        results = []
        
        # Process each chart with its own transaction
        for chart in active_charts:
            try:
                # Use a new cursor for each chart to isolate transactions
                with request.env.registry.cursor() as new_cr:
                    new_env = api.Environment(new_cr, request.env.uid, request.env.context.copy())
                    # Get chart from new environment
                    isolated_chart = new_env['res.dashboard.charts'].browse(chart.id)
                    
                    if not isolated_chart.exists():
                        continue
                    
                    # Check if this is a good candidate for materialized view
                    if self._is_view_candidate(isolated_chart):
                        # Process using materialized view
                        chart_result = self._process_chart_with_view(new_cr, isolated_chart, cco, branches_id)
                    else:
                        # Process using regular query with security
                        chart_result = self._process_chart_with_security(new_cr, isolated_chart, cco, branches_id)
                    
                    if chart_result:
                        results.append(chart_result)
                    
            except Exception as e:
                _logger.error(f"Error processing chart {chart.id}: {e}")
                # Continue with next chart since each has its own transaction
                continue
            
        return results
    
    def _is_view_candidate(self, chart):
        """Determine if a chart is a good candidate for materialized view"""
        # Check if the chart's query is complex or likely to be expensive
        complex_indicators = ['INNER JOIN', 'LEFT JOIN', 'GROUP BY', 'SUM(', 'AVG(', 'COUNT(']
        query = chart.query or ""
        
        # Charts with frequent updates should not use materialized views
        frequency_indicators = ['NOW()', 'CURRENT_DATE', 'CURRENT_TIMESTAMP']
        
        # Check if any indicators are present in the query
        has_complex = any(indicator in query.upper() for indicator in complex_indicators)
        has_frequent_updates = any(indicator in query.upper() for indicator in frequency_indicators)
        
        # Use materialized view if complex but not frequently updated
        return has_complex and not has_frequent_updates
    
    @staticmethod
    def _get_view_name_for_chart(chart_id):
        """Generate a consistent view name for a chart - accepts either chart object or ID"""
        if isinstance(chart_id, int):
            return f"dashboard_chart_view_{chart_id}"
        else:
            # If passed a chart object instead of ID
            return f"dashboard_chart_view_{chart_id.id}"
    
    def _process_chart_with_view(self, cr, chart, cco, branches_id):
        """Process chart using materialized views for better performance"""
        self._ensure_view_service()
        if not self.materialized_view_service:
            # Fall back to regular processing if view service not available
            return self._process_chart_with_security(cr, chart, cco, branches_id)
        
        try:
            view_name = self._get_view_name_for_chart(chart)
            
            # Get original query with security applied
            original_query = chart.query or ""
            if chart.branch_field:
                # Apply security to the base query that populates the view
                # We'll filter again when querying the view to ensure proper security
                original_query = self.security_service.apply_branch_security_filter(
                    original_query, chart.branch_field, True, []  # No filtering at view creation time
                )
            
            # Ensure the view exists
            self.materialized_view_service.ensure_view_exists(view_name, original_query, [
                {'columns': 'id', 'unique': True},
                # Add other indexes based on chart's needs
                {'columns': chart.x_axis_field} if chart.x_axis_field else None,
                {'columns': chart.y_axis_field} if chart.y_axis_field else None
            ])
            
            # Refresh the view (will only refresh if needed based on interval)
            self.materialized_view_service.refresh_view(view_name)
            
            # Build query against the view with proper security
            where_clause = None
            if chart.branch_field and not cco:
                # Get user's branch IDs
                user_branches = self.security_service.get_user_branch_ids()
                
                # Determine effective branches
                effective_branches = []
                if branches_id and len(branches_id) > 0:
                    # If user has branch restrictions, intersect with branches_id
                    if user_branches:
                        effective_branches = [b for b in branches_id if b in user_branches]
                    else:
                        effective_branches = branches_id
                elif user_branches:
                    effective_branches = user_branches
                
                # Build where clause if we have effective branches
                if effective_branches:
                    if len(effective_branches) == 1:
                        where_clause = f"{chart.branch_field} = {effective_branches[0]}"
                    else:
                        where_clause = f"{chart.branch_field} IN {tuple(effective_branches)}"
            
            # Query the view
            order_by = "aggregate_value DESC" if "aggregate_value" in original_query else None
            results = self.materialized_view_service.query_view(
                view_name, 
                where_clause=where_clause,
                order_by=order_by,
                limit=100  # Reasonable default
            )
            
            # Extract chart data from results
            return self._extract_chart_data(chart, results, original_query)
            
        except Exception as e:
            _logger.error(f"Error in materialized view chart: {e}")
            # Fall back to regular processing
            return self._process_chart_with_security(cr, chart, cco, branches_id)
    
    def _process_chart_with_security(self, cr, chart, cco, branches_id):
        """Process chart data with proper security filters"""
        # Get original query from chart
        query = chart.query
        if not query:
            return {
                'id': chart.id,
                'title': chart.name,
                'type': chart.chart_type,
                'labels': [],
                'datasets': [{'data': [], 'backgroundColor': []}]
            }
        
        # Clean up query and add timeout hint
        query = query.replace(';', '').strip()
        
        # Apply branch security filter
        if chart.branch_field:
            query = self.security_service.apply_branch_security_filter(
                query, chart.branch_field, cco, branches_id
            )
        
        query = "SET LOCAL statement_timeout = 10000; " + query  # Set 10 second timeout
        
        try:
            # Execute with timeout
            cr.execute(query)
            
            # Fetch results
            results = cr.dictfetchall()
            
            # Extract chart data
            return self._extract_chart_data(chart, results, query)
        except Exception as e:
            _logger.error(f"Error in chart processing: {e}")
            return {
                'id': chart.id,
                'title': chart.name,
                'type': chart.chart_type,
                'error': str(e),
                'labels': [],
                'datasets': [{'data': [], 'backgroundColor': []}]
            }
    
    def process_paginated_results(self, chart, query, total_count, page, page_size):
        """Process paginated query results with pagination metadata and security"""
        # Execute in isolated transaction
        with request.env.registry.cursor() as new_cr:
            new_env = api.Environment(new_cr, request.env.uid, request.env.context.copy())
            isolated_chart = new_env['res.dashboard.charts'].browse(chart.id)
            
            # Apply security filter
            if isolated_chart.branch_field:
                query = self.security_service.apply_branch_security_filter(
                    query, isolated_chart.branch_field, False, []
                )
            
            # Use process_chart_with_security with pagination
            result = self._process_chart_with_security(new_cr, isolated_chart, False, [])
            
            # Add pagination information
            result['pagination'] = {
                'total': total_count,
                'page': page,
                'page_size': page_size,
                'pages': (total_count + page_size - 1) // page_size if page_size > 0 else 0
            }
            
            return result
    def _extract_chart_data(self, chart, results, query):
        """Extract chart data from query results"""
        if not results:
            return {
                'id': chart.id,
                'title': chart.name,
                'type': chart.chart_type,
                'labels': [],
                'datasets': [{'data': [], 'backgroundColor': []}]
            }
            
        # Extract labels and values
        x_field = chart.x_axis_field or 'name'
        if x_field not in results[0]:
            x_field = next(iter(results[0]))
            
        y_field = chart.y_axis_field
        if not y_field or y_field not in results[0]:
            # Try finding a numeric field that might be a good y-axis
            for field in results[0].keys():
                if field != x_field and field != 'id' and isinstance(results[0][field], (int, float)):
                    y_field = field
                    break
            
            # If still not found, use the next field that's not x_field or id
            if not y_field:
                y_field = next((k for k in results[0].keys() if k != x_field and k != 'id'), None)
        
        # Try to find an ID field
        id_field = 'id'
        if id_field not in results[0]:
            id_field = next((k for k in results[0].keys() if k.endswith('_id')), None)

        # Extract the IDs if found
        ids = [r.get(id_field) for r in results] if id_field and id_field in results[0] else []
        
        if not y_field:
            _logger.error(f"Cannot determine Y-axis field for chart {chart.id}")
            return {
                'id': chart.id,
                'title': chart.name,
                'type': chart.chart_type,
                'error': 'Cannot determine Y-axis field from query results',
                'labels': [],
                'datasets': [{'data': [], 'backgroundColor': []}]
            }
        
        # Extract labels and values with safety checks
        labels = [str(r.get(x_field, '')) for r in results]
        
        # Convert values to float with safety checks
        values = []
        for r in results:
            val = r.get(y_field)
            try:
                values.append(float(val) if val is not None else 0)
            except (ValueError, TypeError):
                values.append(0)
                
        # Extract domain filters from the query automatically
        additional_domain = []
        
        # Parse conditions from the original query to build the domain
        if chart.query:
            # Get original query conditions
            original_query = chart.query.upper()
            where_clause = ""
            
            # Extract the WHERE clause
            if "WHERE" in original_query:
                where_start = original_query.find("WHERE") + 5
                where_end = -1
                
                for clause in ["GROUP BY", "ORDER BY", "LIMIT"]:
                    clause_pos = original_query.find(clause, where_start)
                    if clause_pos > -1 and (where_end == -1 or clause_pos < where_end):
                        where_end = clause_pos
                
                where_clause = original_query[where_start:where_end if where_end > -1 else None].strip()
                
                # Extract conditions from WHERE clause
                if where_clause:
                    conditions = where_clause.split("AND")
                    for condition in conditions:
                        condition = condition.strip()
                        
                        # Look for field = value patterns
                        if "=" in condition and "." in condition:
                            parts = condition.split("=")
                            field_part = parts[0].strip()
                            value_part = parts[1].strip()
                            
                            # Extract field name without table prefix
                            if "." in field_part:
                                table, field = field_part.split(".")
                                
                                # Handle different value types
                                if "'" in value_part:  # String value
                                    value = value_part.replace("'", "").lower()
                                    additional_domain.append((field.lower(), '=', value))
                                elif value_part.isdigit():  # Numeric value
                                    value = int(value_part)
                                    additional_domain.append((field.lower(), '=', value))
        
        # Generate colors based on selected scheme
        color_generator = ColorGenerator()
        colors = color_generator._generate_colors(chart.color_scheme, len(results))
        
        return {
            'id': chart.id,
            'title': chart.name,
            'type': chart.chart_type,
            'model_name': chart.target_model,
            'filter': chart.domain_field,
            'column': chart.column,
            'labels': labels,
            'ids': ids,
            'datefield': chart.date_field,
            'additional_domain': additional_domain, 
            'datasets': [{
                'data': values,
                'backgroundColor': colors,
                'borderColor': colors if chart.chart_type in ['line', 'radar'] else [],
                'borderWidth': 1
            }]
        }
    
    def _is_safe_query(self, query):
        """Check if a query is safe to execute"""
        if not query:
            return False
            
        # Prevent multiple statements
        if ';' in query and not query.strip().endswith(';'):
            return False
            
        # Block dangerous SQL commands
        unsafe_commands = [
            'UPDATE', 'DELETE', 'INSERT', 'ALTER', 'DROP', 'TRUNCATE', 
            'CREATE', 'GRANT', 'REVOKE', 'SET ROLE'
        ]
        
        for cmd in unsafe_commands:
            if re.search(r'\b' + cmd + r'\b', query, re.IGNORECASE):
                return False
                
        return True
        
    # def _extract_chart_data(self, chart, results, query):
    #     """Extract chart data from query results"""
    #     if not results:
    #         return {
    #             'id': chart.id,
    #             'title': chart.name,
    #             'type': chart.chart_type,
    #             'labels': [],
    #             'datasets': [{'data': [], 'backgroundColor': []}]
    #         }
            
    #     # Extract labels and values
    #     x_field = chart.x_axis_field or 'name'
    #     if x_field not in results[0]:
    #         x_field = next(iter(results[0]))
            
    #     y_field = chart.y_axis_field
    #     if not y_field or y_field not in results[0]:
    #         # Try finding a numeric field that might be a good y-axis
    #         for field in results[0].keys():
    #             if field != x_field and field != 'id' and isinstance(results[0][field], (int, float)):
    #                 y_field = field
    #                 break
            
    #         # If still not found, use the next field that's not x_field or id
    #         if not y_field:
    #             y_field = next((k for k in results[0].keys() if k != x_field and k != 'id'), None)
        
    #     # Try to find an ID field
    #     id_field = 'id'
    #     if id_field not in results[0]:
    #         id_field = next((k for k in results[0].keys() if k.endswith('_id')), None)

    #     # Extract the IDs if found
    #     ids = [r.get(id_field) for r in results] if id_field and id_field in results[0] else []
        
    #     if not y_field:
    #         _logger.error(f"Cannot determine Y-axis field for chart {chart.id}")
    #         return {
    #             'id': chart.id,
    #             'title': chart.name,
    #             'type': chart.chart_type,
    #             'error': 'Cannot determine Y-axis field from query results',
    #             'labels': [],
    #             'datasets': [{'data': [], 'backgroundColor': []}]
    #         }
        
    #     # Extract labels and values with safety checks
    #     labels = [str(r.get(x_field, '')) for r in results]
        
    #     # Convert values to float with safety checks
    #     values = []
    #     for r in results:
    #         val = r.get(y_field)
    #         try:
    #             values.append(float(val) if val is not None else 0)
    #         except (ValueError, TypeError):
    #             values.append(0)
        
    #     # Generate colors based on selected scheme
    #     colors = self.color_generator.generate_colors(chart.color_scheme, len(results))

    #     # Get domain filter from the query
    #     domain_filter = self.sql_parser.sql_where_to_odoo_domain(query) if query else []
        
    #     return {
    #         'id': chart.id,
    #         'title': chart.name,
    #         'type': chart.chart_type,
    #         'model_name': chart.target_model,
    #         'filter': chart.domain_field,
    #         'column': chart.column,
    #         'labels': labels,
    #         'ids': ids,
    #         'datefield': chart.date_field,
    #         'datasets': [{
    #             'data': values,
    #             'backgroundColor': colors,
    #             'borderColor': colors if chart.chart_type in ['line', 'radar'] else [],
    #             'borderWidth': 1
    #         }],
    #         'domain_filter': domain_filter
    #     }


