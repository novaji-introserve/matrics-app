# import logging
# from odoo import tools
# from odoo import api
# from odoo.http import request

# _logger = logging.getLogger(__name__)

# class ChartDataService:
#     """Service for generating and processing chart data with temporary tables"""
    
#     def __init__(self):
#         # Import dependencies here to avoid circular imports
#         from ..utils.color_generator import ColorGenerator
#         from ..utils.sql_parser import SQLParser
        
#         self.color_generator = ColorGenerator()
#         self.sql_parser = SQLParser()
    
#     def process_multiple_charts(self, charts, cco, branches_id):
#         """Process multiple charts in batch with temporary table optimization"""
#         # Pre-filter to only active charts
#         active_charts = charts.filtered(lambda c: c.state == 'active')
        
#         if not active_charts:
#             return []
        
#         results = []
        
#         # Process each chart with its own transaction
#         for chart in active_charts:
#             try:
#                 # Use a new cursor for each chart to isolate transactions
#                 with request.env.registry.cursor() as new_cr:
#                     new_env = api.Environment(new_cr, request.env.uid, request.env.context.copy())
#                     # Get chart from new environment
#                     isolated_chart = new_env['res.dashboard.charts'].browse(chart.id)
                    
#                     if not isolated_chart.exists():
#                         continue
                    
#                     # Process using optimized method based on chart name/type
#                     chart_result = self._process_chart_with_optimization(new_cr, isolated_chart, cco, branches_id)
                    
#                     if chart_result:
#                         results.append(chart_result)
                    
#             except Exception as e:
#                 _logger.error(f"Error processing chart {chart.id}: {e}")
#                 # Continue with next chart since each has its own transaction
#                 continue
            
#         return results
    
#     def _process_chart_with_optimization(self, cr, chart, cco, branches_id):
#         """Process chart data with appropriate optimization strategy"""
#         # Check chart name and apply a specific optimization
#         chart_name = chart.name.lower() if chart.name else ""
        
#         try:
#             if "branch by customer" in chart_name:
#                 return self._process_branch_customer_chart(cr, chart, cco, branches_id)
#             elif "high risk branch" in chart_name:
#                 return self._process_high_risk_branch_chart(cr, chart, cco, branches_id)
#             elif "transaction by rules" in chart_name:
#                 return self._process_transaction_rules_chart(cr, chart, cco, branches_id)
#             else:
#                 # Fallback to original query with timeout
#                 return self._process_default_chart(cr, chart, cco, branches_id)
#         except Exception as e:
#             _logger.error(f"Error processing chart {chart.id} ({chart.name}): {e}")
#             return {
#                 'id': chart.id,
#                 'title': chart.name,
#                 'type': chart.chart_type,
#                 'error': str(e),
#                 'labels': [],
#                 'datasets': [{'data': [], 'backgroundColor': []}]
#             }
    
#     def _process_branch_customer_chart(self, cr, chart, cco, branches_id):
#         """Process branch customer chart using temporary table optimization"""
#         try:
#             # Create a temporary table with pre-aggregated data
#             cr.execute("""
#                 CREATE TEMP TABLE temp_branch_customers AS
#                 SELECT 
#                     branch_id, 
#                     COUNT(id) AS customer_count
#                 FROM 
#                     res_partner
#                 WHERE 
#                     branch_id IS NOT NULL
#                 GROUP BY 
#                     branch_id;
                
#                 -- Create index on the temp table
#                 CREATE INDEX ON temp_branch_customers (customer_count DESC);
#             """)
            
#             # Query the temporary table to get results
#             # Join with branch table to get names
#             cr.execute("""
#                 SELECT 
#                     rb.id, 
#                     rb.name, 
#                     COALESCE(tbc.customer_count, 0) AS customer_count
#                 FROM 
#                     res_branch rb
#                 LEFT JOIN 
#                     temp_branch_customers tbc ON rb.id = tbc.branch_id
#                 ORDER BY 
#                     customer_count DESC
#                 LIMIT 10;
#             """)
            
#             # Get results
#             results = cr.dictfetchall()
            
#             # Drop the temporary table
#             cr.execute("DROP TABLE IF EXISTS temp_branch_customers;")
            
#             # Extract chart data
#             return self._extract_chart_data(chart, results, "")
#         except Exception as e:
#             _logger.error(f"Error in branch customer chart: {e}")
#             # Clean up temp table if it exists
#             try:
#                 cr.execute("DROP TABLE IF EXISTS temp_branch_customers;")
#             except:
#                 pass
#             raise
    
#     def _process_high_risk_branch_chart(self, cr, chart, cco, branches_id):
#         """Process high risk branch chart using temporary table optimization"""
#         try:
#             # Create a temporary table with pre-aggregated data
#             cr.execute("""
#                 CREATE TEMP TABLE temp_high_risk_customers AS
#                 SELECT 
#                     branch_id, 
#                     COUNT(id) AS high_risk_customers
#                 FROM 
#                     res_partner
#                 WHERE 
#                     branch_id IS NOT NULL
#                     AND LOWER(risk_level) = 'high'
#                 GROUP BY 
#                     branch_id;
                
#                 -- Create index on the temp table
#                 CREATE INDEX ON temp_high_risk_customers (high_risk_customers DESC);
#             """)
            
#             # Query the temporary table to get results
#             cr.execute("""
#                 SELECT 
#                     rb.id, 
#                     rb.name, 
#                     COALESCE(thrc.high_risk_customers, 0) AS high_risk_customers
#                 FROM 
#                     res_branch rb
#                 LEFT JOIN 
#                     temp_high_risk_customers thrc ON rb.id = thrc.branch_id
#                 ORDER BY 
#                     high_risk_customers DESC
#                 LIMIT 10;
#             """)
            
#             # Get results
#             results = cr.dictfetchall()
            
#             # Drop the temporary table
#             cr.execute("DROP TABLE IF EXISTS temp_high_risk_customers;")
            
#             # Extract chart data
#             return self._extract_chart_data(chart, results, "")
#         except Exception as e:
#             _logger.error(f"Error in high risk branch chart: {e}")
#             # Clean up temp table if it exists
#             try:
#                 cr.execute("DROP TABLE IF EXISTS temp_high_risk_customers;")
#             except:
#                 pass
#             raise
    
#     def _process_transaction_rules_chart(self, cr, chart, cco, branches_id):
#         """Process transaction rules chart using temporary table optimization"""
#         try:
#             # Create a temporary table with pre-aggregated data
#             cr.execute("""
#                 CREATE TEMP TABLE temp_rule_hits AS
#                 SELECT 
#                     rule_id, 
#                     COUNT(id) AS hit_count
#                 FROM 
#                     res_customer_transaction
#                 WHERE 
#                     rule_id IS NOT NULL
#                 GROUP BY 
#                     rule_id;
                
#                 -- Create index on the temp table
#                 CREATE INDEX ON temp_rule_hits (hit_count DESC);
#             """)
            
#             # Query the temporary table to get results
#             cr.execute("""
#                 SELECT 
#                     rtsr.id, 
#                     rtsr.name, 
#                     COALESCE(trh.hit_count, 0) AS hit_count
#                 FROM 
#                     res_transaction_screening_rule rtsr
#                 LEFT JOIN 
#                     temp_rule_hits trh ON rtsr.id = trh.rule_id
#                 ORDER BY 
#                     hit_count DESC
#                 LIMIT 10;
#             """)
            
#             # Get results
#             results = cr.dictfetchall()
            
#             # Drop the temporary table
#             cr.execute("DROP TABLE IF EXISTS temp_rule_hits;")
            
#             # Extract chart data
#             return self._extract_chart_data(chart, results, "")
#         except Exception as e:
#             _logger.error(f"Error in transaction rules chart: {e}")
#             # Clean up temp table if it exists
#             try:
#                 cr.execute("DROP TABLE IF EXISTS temp_rule_hits;")
#             except:
#                 pass
#             raise
    
#     def _process_default_chart(self, cr, chart, cco, branches_id):
#         """Process chart using original query with timeout"""
#         # Get original query from chart
#         query = chart.query
#         if not query:
#             return {
#                 'id': chart.id,
#                 'title': chart.name,
#                 'type': chart.chart_type,
#                 'labels': [],
#                 'datasets': [{'data': [], 'backgroundColor': []}]
#             }
        
#         # Clean up query and add timeout hint
#         query = query.replace(';', '').strip()
#         query = "SET LOCAL statement_timeout = 10000; " + query  # Set 10 second timeout
        
#         try:
#             # Execute with timeout
#             cr.execute(query)
            
#             # Fetch results
#             results = cr.dictfetchall()
            
#             # Extract chart data
#             return self._extract_chart_data(chart, results, query)
#         except Exception as e:
#             _logger.error(f"Error in default chart processing: {e}")
#             return {
#                 'id': chart.id,
#                 'title': chart.name,
#                 'type': chart.chart_type,
#                 'error': str(e),
#                 'labels': [],
#                 'datasets': [{'data': [], 'backgroundColor': []}]
#             }
    
#     def _extract_chart_data(self, chart, results, query):
#         """Extract chart data from query results"""
#         if not results:
#             return {
#                 'id': chart.id,
#                 'title': chart.name,
#                 'type': chart.chart_type,
#                 'labels': [],
#                 'datasets': [{'data': [], 'backgroundColor': []}]
#             }
        
#         # Log the results for debugging
#         _logger.debug(f"Retrieved {len(results)} results for chart {chart.id}")
#         _logger.debug(f"First result row: {results[0]}")
            
#         # Extract labels and values
#         x_field = chart.x_axis_field or 'name'
#         if x_field not in results[0]:
#             x_field = next(iter(results[0]))
            
#         y_field = chart.y_axis_field
#         if not y_field or y_field not in results[0]:
#             y_field = next((k for k in results[0].keys() if k != x_field and k != 'id'), None)
        
#         # Debug log the fields we're using
#         _logger.debug(f"Using x_field: {x_field}, y_field: {y_field} for chart {chart.id}")
        
#         # Try to find an ID field
#         id_field = 'id'
#         if id_field not in results[0]:
#             id_field = next((k for k in results[0].keys() if k.endswith('_id')), None)

#         # Extract the IDs if found
#         ids = [r.get(id_field) for r in results] if id_field and id_field in results[0] else []
        
#         if not y_field:
#             _logger.error(f"Cannot determine Y-axis field for chart {chart.id}")
#             return {
#                 'id': chart.id,
#                 'title': chart.name,
#                 'type': chart.chart_type,
#                 'error': 'Cannot determine Y-axis field from query results',
#                 'labels': [],
#                 'datasets': [{'data': [], 'backgroundColor': []}]
#             }
        
#         # Extract labels and values with safety checks
#         labels = [str(r.get(x_field, '')) for r in results]
        
#         # Convert values to float with safety checks
#         values = []
#         for r in results:
#             val = r.get(y_field)
#             try:
#                 values.append(float(val) if val is not None else 0)
#             except (ValueError, TypeError) as e:
#                 _logger.warning(f"Error converting value '{val}' to float: {e}")
#                 values.append(0)
        
#         # Log the values
#         _logger.debug(f"Values for chart {chart.id}: {values}")
        
#         # Generate colors based on selected scheme
#         colors = self.color_generator.generate_colors(chart.color_scheme, len(results))

#         # Get domain filter from the query
#         domain_filter = self.sql_parser.sql_where_to_odoo_domain(query) if query else []
        
#         return {
#             'id': chart.id,
#             'title': chart.name,
#             'type': chart.chart_type,
#             'model_name': chart.target_model,
#             'filter': chart.domain_field,
#             'column': chart.column,
#             'labels': labels,
#             'ids': ids,
#             'datefield': chart.date_field,
#             'datasets': [{
#                 'data': values,
#                 'backgroundColor': colors,
#                 'borderColor': colors if chart.chart_type in ['line', 'radar'] else [],
#                 'borderWidth': 1
#             }],
#             'domain_filter': domain_filter
#         }
    
#     def process_paginated_results(self, chart, query, total_count, page, page_size):
#         """Process paginated query results with pagination metadata"""
#         # Execute in isolated transaction
#         with request.env.registry.cursor() as new_cr:
#             new_env = api.Environment(new_cr, request.env.uid, request.env.context.copy())
#             isolated_chart = new_env['res.dashboard.charts'].browse(chart.id)
            
#             # Use default method with limited timeout
#             result = self._process_default_chart(new_cr, isolated_chart, False, [])
            
#             # Add pagination information
#             result['pagination'] = {
#                 'total': total_count,
#                 'page': page,
#                 'page_size': page_size,
#                 'pages': (total_count + page_size - 1) // page_size if page_size > 0 else 0
#             }
            
#             return result







import logging
from odoo import tools
from odoo import api
from odoo.http import request

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
        
        # Generate colors based on selected scheme
        colors = self.color_generator.generate_colors(chart.color_scheme, len(results))

        # Get domain filter from the query
        domain_filter = self.sql_parser.sql_where_to_odoo_domain(query) if query else []
        
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
            'datasets': [{
                'data': values,
                'backgroundColor': colors,
                'borderColor': colors if chart.chart_type in ['line', 'radar'] else [],
                'borderWidth': 1
            }],
            'domain_filter': domain_filter
        }




# import re
# import logging
# from ..utils.sql_parser import SQLParser
# from ..utils.color_generator import ColorGenerator
# from odoo.http import request

# _logger = logging.getLogger(__name__)

# class ChartDataService:
#     """Service for generating and processing chart data"""
    
#     def __init__(self):
#         self.sql_parser = SQLParser()
#         self.color_generator = ColorGenerator()
    
#     def build_filtered_query(self, chart, cco, branches_id, page=None, page_size=None):
#         """Build a filtered SQL query based on parameters"""
#         # Get original query and prepare for modification
#         original_query = chart.query.replace(';', '').strip()
#         modified_query = original_query
        
#         # Add branch filtering if needed
#         if not cco and chart.branch_filter and branches_id:
#             where_clause = ""
#             if len(branches_id) == 1:
#                 where_clause = f"{chart.branch_field} = {branches_id[0]}"
#             elif len(branches_id) > 1:
#                 where_clause = f"{chart.branch_field} IN {tuple(branches_id)}"
            
#             if where_clause:
#                 modified_query = self.sql_parser.add_where_to_query(modified_query, where_clause)
#         elif not cco and chart.branch_filter and not branches_id:
#             modified_query = self.sql_parser.add_where_to_query(modified_query, "1 = 0")  # No branches selected, return nothing
        
#         # Add pagination if requested
#         if page is not None and page_size is not None:
#             modified_query = self._add_pagination(modified_query, page, page_size)
        
#         return modified_query
    
#     def _add_pagination(self, query, page, page_size):
#         """Add pagination to a query, handling existing LIMIT clauses"""
#         # Check if the query already has a LIMIT clause
#         limit_match = re.search(r'\bLIMIT\b\s+\d+', query, re.IGNORECASE)
        
#         if limit_match:
#             # Create a subquery with the original query including its LIMIT
#             # Then apply our pagination to that subquery
#             return f"WITH original_query AS ({query}) SELECT * FROM original_query OFFSET {page * page_size} LIMIT {page_size}"
#         else:
#             # No LIMIT in the original query, add it directly
#             return f"{query} LIMIT {page_size} OFFSET {page * page_size}"
    
#     def build_count_query(self, chart, query):
#         """Build a query to count total results"""
#         # Remove any LIMIT/OFFSET clauses for counting
#         base_query = re.sub(r'\bLIMIT\b\s+\d+(?:\s+OFFSET\s+\d+)?', '', query, flags=re.IGNORECASE)
#         base_query = re.sub(r'\bOFFSET\b\s+\d+', '', base_query, flags=re.IGNORECASE)
        
#         # Wrap in a COUNT query
#         return f"SELECT COUNT(*) as total FROM ({base_query}) AS count_table"
    
#     def process_chart_data(self, chart, query):
#         """Process chart data from a query"""
#         try:
#             # Execute the query without a timeout parameter
#             request.env.cr.execute(query)
#             results = request.env.cr.dictfetchall()
#         except Exception as e:
#             _logger.error(f"Error executing query: {e}")
#             return {
#                 'title': '',
#                 'type': '',
#                 'labels': [],
#                 'datasets': [{'data': [], 'backgroundColor': []}]
#             }
        
#         if not results:
#             return {
#                 'title': '',
#                 'type': '',
#                 'labels': [],
#                 'datasets': [{'data': [], 'backgroundColor': []}]
#             }
        
#         # Extract chart data from results
#         return self._extract_chart_data(chart, results, query)
    
#     def process_paginated_results(self, chart, query, total_count, page, page_size):
#         """Process paginated query results with pagination metadata"""
#         result = self.process_chart_data(chart, query)
        
#         # Add pagination information
#         result['pagination'] = {
#             'total': total_count,
#             'page': page,
#             'page_size': page_size,
#             'pages': (total_count + page_size - 1) // page_size if page_size > 0 else 0
#         }
        
#         return result
    
#     def process_multiple_charts(self, charts, cco, branches_id):
#         """Process multiple charts in batch"""
#         results = []
        
#         for chart in charts:
#             try:
#                 # Build the query with filters
#                 query = self.build_filtered_query(chart, cco, branches_id)
                
#                 # Process the query
#                 result = self.process_chart_data(chart, query)
                
#                 # Add the result if it's not empty
#                 if result['title'] != '' or result['type'] != '' or result['labels']:
#                     results.append(result)
#             except Exception as e:
#                 _logger.error(f"Error processing chart {chart.id}: {e}")
#                 # Continue processing other charts even if one fails
        
#         return results
    
#     def _extract_chart_data(self, chart, results, query):
#         """Extract chart data from query results"""
#         # Extract labels and values
#         x_field = chart.x_axis_field or next(iter(results[0]))
#         y_field = chart.y_axis_field or next((k for k in results[0].keys() if k != x_field), None)
        
#         # Try to find an ID field
#         id_field = next((k for k in results[0].keys() if k.endswith('_id') or k == 'id'), None)
#         if not id_field:
#             id_field = next((k for k in results[0].keys() if k != x_field and k != y_field), None)

#         # Extract the IDs if found
#         ids = [r[id_field] if id_field else None for r in results]
        
#         if not y_field:
#             return {'error': 'Cannot determine Y-axis field from query results'}
        
#         # Extract labels and values
#         labels = [str(r[x_field]) for r in results]
#         values = [float(r[y_field]) if r[y_field] is not None else 0 for r in results]
        
#         # Generate colors based on selected scheme
#         colors = self.color_generator.generate_colors(chart.color_scheme, len(results))

#         # Convert SQL WHERE to Odoo domain
#         domain_filter = self.sql_parser.sql_where_to_odoo_domain(query)
        
#         return {
#             'id': chart.id,
#             'title': chart.name,
#             'type': chart.chart_type,
#             'model_name': chart.target_model,
#             'filter': chart.domain_field,
#             'column': chart.column,
#             'labels': labels,
#             'ids': ids,
#             'datefield': chart.date_field,
#             'datasets': [{
#                 'data': values,
#                 'backgroundColor': colors,
#                 'borderColor': colors if chart.chart_type in ['line', 'radar'] else [],
#                 'borderWidth': 1
#             }],
#             'domain_filter': domain_filter
#         }
