# from odoo import http
# from odoo.http import request
# import json
# import logging
# import re
# from datetime import datetime, timedelta

# _logger = logging.getLogger(__name__)

# class DynamicChartController(http.Controller):
#     """Controller for handling dynamic chart requests with improved performance"""
    
#     def __init__(self):
#         super(DynamicChartController, self).__init__()
#         # Import here to avoid circular imports
#         from ..services.chart_data_service import ChartDataService
#         from ..utils.cache_key_unique_identifier import get_unique_client_identifier
        
#         self.chart_data_service = ChartDataService()
#         self.get_unique_client_identifier = get_unique_client_identifier
    
#     @http.route('/web/dynamic_charts/preview', type='json', auth='user')
#     def preview_chart(self, chart_type, query, x_axis_field=None, y_axis_field=None, color_scheme='default'):
#         """Preview chart without saving - with query safety checks"""
#         try:
#             # Apply query limit for safety
#             if not self._is_safe_query(query):
#                 return {'error': 'Query contains unsafe operations'}
                
#             # Create a temporary chart for preview
#             chart = request.env['res.dashboard.charts'].new({
#                 'chart_type': chart_type,
#                 'query': query,
#                 'x_axis_field': x_axis_field,
#                 'y_axis_field': y_axis_field,
#                 'color_scheme': color_scheme
#             })
            
#             # Run validation manually since it's a new record
#             chart._check_query_safety()
            
#             # Process query in isolated transaction
#             with request.env.registry.cursor() as new_cr:
#                 # Execute with timeout
#                 new_cr.execute("SET LOCAL statement_timeout = 10000;")  # 10 seconds
#                 new_cr.execute(query)
#                 results = new_cr.dictfetchall()
                
#                 return self.chart_data_service._extract_chart_data(chart, results, query)
#         except Exception as e:
#             _logger.error(f"Error in preview_chart: {e}")
#             return {'error': str(e)}
        
#     @http.route('/dashboard/dynamic_chart_page/', type='json', auth='user')
#     def get_chart_page(self, chart_id, page=0, page_size=50, cco=False, branches_id=None, **kw):
#         """Get paginated chart data for a single chart with improved performance"""
#         if branches_id is None:
#             branches_id = []
        
#         # Validate inputs
#         try:
#             chart_id = int(chart_id)
#             page = max(0, int(page))
#             page_size = min(100, max(1, int(page_size)))
#         except (ValueError, TypeError):
#             return {'error': 'Invalid parameters'}
        
#         # Get current user ID
#         user_id = request.env.user.id
#         datepicked = 20000
        
#         # Generate cache key for this specific page
#         unique_id = self.get_unique_client_identifier()
#         cache_key = f"chart_page_{chart_id}_{page}_{page_size}_{cco}_{branches_id}_{datepicked}_{unique_id}"
        
#         # Check cache first
#         cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)
#         if cache_data:
#             return cache_data
        
#         try:
#             # Get the chart
#             chart = request.env['res.dashboard.charts'].browse(chart_id)
#             if not chart.exists():
#                 return {'error': 'Chart not found'}
            
#             # Execute count query in isolated transaction
#             with request.env.registry.cursor() as cr:
#                 # Simple count query with timeout
#                 cr.execute("SET LOCAL statement_timeout = 5000;")  # 5 seconds
#                 cr.execute(f"""
#                     SELECT COUNT(*) as total 
#                     FROM ({chart.query.replace(';', '')}) AS count_query
#                 """)
#                 count_result = cr.dictfetchone()
#                 total_count = count_result['total'] if count_result else 0
            
#             # Process the results with temporary table optimization
#             result = self.chart_data_service.process_paginated_results(
#                 chart, 
#                 chart.query, 
#                 total_count, 
#                 page, 
#                 page_size
#             )
            
#             # Store in cache before returning
#             request.env['res.dashboard.cache'].set_cache(cache_key, result, user_id, ttl=300)
#             return result
        
#         except Exception as e:
#             _logger.error(f"Error in get_chart_page: {e}")
#             return {'error': str(e)}

#     @http.route('/dashboard/dynamic_charts/', type='json', auth='user')
#     def get_chart_data(self, cco, branches_id, **kw):
#         """Get chart data in JSON format for all charts with improved performance"""
#         try:
#             # Input validation and sanitization
#             if not isinstance(cco, bool):
#                 cco = str(cco).lower() == 'true'
                
#             if not isinstance(branches_id, list):
#                 try:
#                     branches_id = json.loads(branches_id) if branches_id else []
#                 except (ValueError, TypeError):
#                     branches_id = []
            
#             # Get current user ID
#             user_id = request.env.user.id
            
#             datepicked = 20000
    
#             # Generate cache key
#             unique_id = self.get_unique_client_identifier()
#             branches_str = json.dumps(branches_id) if branches_id else '[]'
#             # cache_key = f"charts_data_{cco}_{branches_str}_{unique_id}"
#             cache_key = f"charts_data_{cco}_{branches_str}_{datepicked}_{unique_id}"
            
#             # Check if we have valid cache for this user
#             cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)
#             if cache_data:
#                 return cache_data
            
#             # If no cache, generate the data
#             result = self.get_chart_data_internal(cco, branches_id)
            
#             # Return even if empty to avoid repeating slow queries
#             if not result:
#                 result = []
                
#             # Store in cache before returning
#             request.env['res.dashboard.cache'].set_cache(cache_key, result, user_id)
            
#             return result
#         except Exception as e:
#             _logger.error(f"Error in get_chart_data: {e}")
#             # Return empty result in case of errors to prevent further requests
#             return []
    
#     def get_chart_data_internal(self, cco, branches_id):
#         """Internal method for getting chart data with temporary table optimization"""
#         # Get all active charts
#         charts = request.env['res.dashboard.charts'].search([('state', '=', 'active')])
        
#         # No charts, return empty list
#         if not charts:
#             return []
        
#         # Process all charts in a batch with temporary table optimization
#         return self.chart_data_service.process_multiple_charts(charts, cco, branches_id)
    
#     def _is_safe_query(self, query):
#         """Check if a query is safe to execute"""
#         if not query:
#             return False
            
#         # Prevent multiple statements
#         if ';' in query and not query.strip().endswith(';'):
#             return False
            
#         # Block dangerous SQL commands
#         unsafe_commands = [
#             'UPDATE', 'DELETE', 'INSERT', 'ALTER', 'DROP', 'TRUNCATE', 
#             'CREATE', 'GRANT', 'REVOKE', 'SET ROLE'
#         ]
        
#         for cmd in unsafe_commands:
#             if re.search(r'\b' + cmd + r'\b', query, re.IGNORECASE):
#                 return False
                
#         return True



import psycopg2
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
        super(DynamicChartController, self).__init__()
        # Import here to avoid circular imports
        from ..services.branch_security import ChartSecurityService
        from ..utils.cache_key_unique_identifier import generate_cache_key
        
        self.security_service = ChartSecurityService()
        self.generate_cache_key = generate_cache_key
        
        # Enable debug logging for chart issues
        self.debug_mode = True
        
    def _debug_log(self, message):
        """Log debug messages if debug mode is enabled"""
        if self.debug_mode:
            _logger.info(f"CHART DEBUG: {message}")
    
    @http.route('/web/dynamic_charts/preview', type='json', auth='user')
    def preview_chart(self, chart_type, query, x_axis_field=None, y_axis_field=None, color_scheme='default'):
        """Preview chart without saving - with query safety checks and security"""
        try:
            # Apply query limit for safety
            if not self._is_safe_query(query):
                return {'error': 'Query contains unsafe operations'}
                
            # Create a temporary chart for preview
            chart = request.env['res.dashboard.charts'].new({
                'chart_type': chart_type,
                'query': query,
                'x_axis_field': x_axis_field,
                'y_axis_field': y_axis_field,
                'color_scheme': color_scheme,
                # Add default branch field for security
                'branch_field': 'branch_id',
                'branch_filter': True,
                'date_field': 'create_date'  # Default date field
            })
            
            # Run validation manually since it's a new record
            if hasattr(chart, '_check_query_safety'):
                chart._check_query_safety()
            
            # Apply security filters to the query
            secured_query = self.security_service.secure_chart_query(chart, False, [])
            
            # Process query in isolated transaction
            with request.env.registry.cursor() as new_cr:
                # Execute with timeout
                new_cr.execute("SET LOCAL statement_timeout = 10000;")  # 10 seconds
                
                start_time = time.time()
                new_cr.execute(secured_query)
                execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
                
                results = new_cr.dictfetchall()
                
                # Extract chart data
                chart_data = self._extract_chart_data(chart, results, secured_query)
                
                # Add execution time info
                chart_data['execution_time_ms'] = round(execution_time, 2)
                
                return chart_data
                
        except Exception as e:
            _logger.error(f"Error in preview_chart: {e}")
            return {'error': str(e)}
    
    @http.route('/dashboard/dynamic_chart_page/', type='json', auth='user')
    def get_chart_page(self, chart_id, page=0, page_size=50, cco=False, branches_id=None, **kw):
        """Get paginated chart data for a single chart with robust error handling"""
        if branches_id is None:
            branches_id = []
        
        # Validate inputs
        try:
            chart_id = int(chart_id)
            page = max(0, int(page))
            page_size = min(100, max(1, int(page_size)))
        except (ValueError, TypeError):
            return {'error': 'Invalid parameters'}
        
        # Normalize cco parameter
        if not isinstance(cco, bool):
            cco = str(cco).lower() == 'true'
        
        # Normalize branches_id parameter
        if not isinstance(branches_id, list):
            try:
                branches_id = json.loads(branches_id) if branches_id else []
            except (ValueError, TypeError):
                branches_id = []
        
        # Get current user ID
        user_id = request.env.user.id
        datepicked = kw.get('datepicked', 20000)
        
        # Generate cache key
        cache_params = {
            'chart_id': chart_id,
            'page': page,
            'page_size': page_size,
            'cco': cco,
            'branches_id': branches_id,
            'datepicked': datepicked,
            'user_branches': self.security_service.get_user_branch_ids()
        }
        
        cache_key = self.generate_cache_key('chart_page', cache_params)
        
        # Check cache first
        cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)
        if cache_data:
            return cache_data
        
        # Use the robust retry handler
        return self.get_chart_with_retries(chart_id, page, page_size, cco, branches_id, cache_key, user_id)


    # @http.route('/dashboard/dynamic_chart_page/', type='json', auth='user')
    # def get_chart_page(self, chart_id, page=0, page_size=50, cco=False, branches_id=None, **kw):
    #     """Get paginated chart data for a single chart with security"""
    #     if branches_id is None:
    #         branches_id = []
        
    #     # Validate inputs
    #     try:
    #         chart_id = int(chart_id)
    #         page = max(0, int(page))
    #         page_size = min(100, max(1, int(page_size)))
    #     except (ValueError, TypeError):
    #         return {'error': 'Invalid parameters'}
        
    #     # Normalize cco parameter
    #     if not isinstance(cco, bool):
    #         cco = str(cco).lower() == 'true'
        
    #     # Normalize branches_id parameter
    #     if not isinstance(branches_id, list):
    #         try:
    #             branches_id = json.loads(branches_id) if branches_id else []
    #         except (ValueError, TypeError):
    #             branches_id = []
        
    #     # Get current user ID
    #     user_id = request.env.user.id
    #     datepicked = kw.get('datepicked', 20000)
        
    #     # Generate cache key for this specific page with user branch context
    #     cache_params = {
    #         'chart_id': chart_id,
    #         'page': page,
    #         'page_size': page_size,
    #         'cco': cco,
    #         'branches_id': branches_id,
    #         'datepicked': datepicked,
    #         'user_branches': self.security_service.get_user_branch_ids()
    #     }
        
    #     cache_key = self.generate_cache_key('chart_page', cache_params)
        
    #     # Check cache first
    #     cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)
    #     if cache_data:
    #         return cache_data
        
    #     try:
    #         # Get the chart
    #         chart = request.env['res.dashboard.charts'].browse(chart_id)
    #         if not chart.exists():
    #             return {'error': 'Chart not found'}
            
    #         # Check if chart uses materialized view
    #         return self._get_chart_from_materialized_view(chart, page, page_size, cco, branches_id, cache_key, user_id)
    #         # if chart.use_materialized_view:
    #         #     return self._get_chart_from_materialized_view(chart, page, page_size, cco, branches_id, cache_key, user_id)
    #         # else:
    #         #     return self._get_chart_from_direct_query(chart, page, page_size, cco, branches_id, cache_key, user_id)
            
    #     except Exception as e:
    #         _logger.error(f"Error in get_chart_page: {e}")
    #         return {'error': str(e)}
        
    def _safe_execute_query(self, cr, query, params=None):
        """Execute a query with proper error handling to avoid transaction blocks"""
        try:
            # Set a timeout for safety
            cr.execute("SET LOCAL statement_timeout = 15000;")  # 15 seconds
            
            # Execute the actual query
            if params:
                cr.execute(query, params)
            else:
                cr.execute(query)
                
            return True, cr.fetchall()
            
        except Exception as e:
            # Make sure to roll back on error
            cr.rollback()
            _logger.error(f"Query execution error: {e}")
            return False, str(e)
        
    def get_chart_with_retries(self, chart_id, page, page_size, cco, branches_id, cache_key, user_id):
        """Get chart data with robust retry handling for serialization failures"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Get the chart
                chart = request.env['res.dashboard.charts'].browse(chart_id)
                if not chart.exists():
                    return {'error': 'Chart not found'}
                
                # Always use materialized view
                return self._get_chart_from_materialized_view(chart, page, page_size, cco, branches_id, cache_key, user_id)
                
            except psycopg2.errors.SerializationFailure as e:
                retry_count += 1
                if retry_count >= max_retries:
                    _logger.error(f"Maximum retries reached for chart {chart_id}: {e}")
                    return {'error': 'Database serialization failure, please try again'}
                
                # Exponential backoff
                wait_time = 2 ** (retry_count - 1)
                _logger.info(f"Serialization failure for chart {chart_id}, retry {retry_count} in {wait_time}s")
                time.sleep(wait_time)
                
            except Exception as e:
                _logger.error(f"Error in get_chart_with_retries: {e}")
                return {'error': str(e)}
                
        return {'error': 'Failed after multiple retries'}
    
    def _get_chart_from_materialized_view(self, chart, page, page_size, cco, branches_id, cache_key, user_id):
        """Get chart data from materialized view with robust retry mechanism"""
        view_name = f"dashboard_chart_view_{chart.id}"
        
        # Get retry settings from system parameters or fallback to defaults
        retry_params = request.env['ir.config_parameter'].sudo()
        max_retries = int(retry_params.get_param('chart.view.max_retries', default=3))
        base_delay = float(retry_params.get_param('chart.view.base_delay', default=0.5))
        
        for retry_attempt in range(max_retries):
            try:
                # Create a new cursor for this attempt
                with request.env.registry.cursor() as cr:
                    # Set transaction isolation level from parameter
                    isolation_level = retry_params.get_param('chart.view.isolation_level', default='READ COMMITTED')
                    cr.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                    
                    # 1. Check if view exists and is accessible
                    cr.execute(f"""
                        SELECT COUNT(*) 
                        FROM pg_catalog.pg_class 
                        WHERE relname = %s AND relkind = 'm'
                    """, (view_name,))
                    
                    if cr.fetchone()[0] == 0:
                        _logger.warning(f"Materialized view {view_name} does not exist (attempt {retry_attempt+1}/{max_retries})")
                        
                        # Create the view if it doesn't exist
                        refresher = request.env['dashboard.chart.view.refresher'].sudo()
                        created = refresher.create_materialized_view_for_chart(chart.id)
                        
                        if not created:
                            _logger.error(f"Failed to create materialized view {view_name}")
                            if retry_attempt == max_retries - 1:
                                return self._get_chart_from_direct_query(chart, page, page_size, cco, branches_id, cache_key, user_id)
                            
                            # Wait based on retry attempt number (increasing delay)
                            delay = base_delay * (retry_attempt + 1)
                            time.sleep(delay)
                            continue  # Try again
                        
                        # Wait a moment for the database to settle - dynamic based on retry count
                        delay = base_delay * (retry_attempt + 1)
                        time.sleep(delay)
                        continue  # Go to next retry attempt with new transaction
                    
                    # 2. Try to directly query the view - this will get columns if it exists
                    try:
                        _logger.info(f"Querying materialized view {view_name} directly (attempt {retry_attempt+1})")
                        
                        # Direct SQL call with error handling
                        cr.execute(f"SELECT * FROM {view_name} LIMIT 1")
                        
                        # Get columns from description
                        columns = [desc[0] for desc in cr.description]
                        
                        if not columns:
                            _logger.warning(f"View {view_name} exists but returned no columns (attempt {retry_attempt+1}/{max_retries})")
                            
                            # If last retry, force recreate the view
                            if retry_attempt == max_retries - 1:
                                _logger.info(f"Last attempt - forcing view recreation")
                                # Drop the view
                                cr.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
                                cr.commit()
                                
                                # Force recreate in a new transaction
                                refresher = request.env['dashboard.chart.view.refresher'].sudo()
                                refresher.create_materialized_view_for_chart(chart.id)
                                
                                # Wait for PostgreSQL to update its internal state
                                delay = base_delay * 2 * (retry_attempt + 1)
                                time.sleep(delay)
                                continue  # Try one more time in new transaction
                            
                            # Not last retry, just try again with delay
                            delay = base_delay * (retry_attempt + 1)
                            time.sleep(delay)
                            continue
                        
                        _logger.info(f"Successfully found {len(columns)} columns in {view_name}: {columns}")
                        
                        # 3. Build the query with the correct columns
                        # Find the branch column - crucial for security filtering
                        branch_col = None
                        if chart.branch_field:
                            # Try direct match first (without table alias)
                            field = chart.branch_field.split('.')[-1] if '.' in chart.branch_field else chart.branch_field
                            
                            if field in columns:
                                branch_col = field
                            else:
                                # Try to find branch-related columns dynamically
                                for col in columns:
                                    if col.lower().endswith('_id') or 'branch' in col.lower():
                                        branch_col = col
                                        break
                        
                        # Find sort column
                        sort_col = None
                        if chart.y_axis_field:
                            # Try direct match first
                            field = chart.y_axis_field.split('.')[-1] if '.' in chart.y_axis_field else chart.y_axis_field
                            
                            if field in columns:
                                sort_col = field
                            else:
                                # Try to find value-related columns dynamically
                                for col in columns:
                                    # Look for common patterns in column names that suggest value fields
                                    if any(term in col.lower() for term in ['count', 'total', 'sum', 'amount', 'value', 'risk']):
                                        sort_col = col
                                        break
                        
                        # 4. Build and execute the query
                        query = f"SELECT * FROM {view_name}"
                        
                        # Apply branch filter if needed
                        if chart.branch_field and not cco and not self.security_service.is_cco_user():
                            user_branches = self.security_service.get_user_branch_ids()
                            effective_branches = []
                            
                            if branches_id:
                                # Filter by specified branches intersected with user's branches
                                if user_branches:
                                    effective_branches = [b for b in branches_id if b in user_branches]
                                else:
                                    effective_branches = branches_id
                            elif user_branches:
                                effective_branches = user_branches
                            
                            # Add WHERE clause if we have branches and a column
                            if effective_branches and branch_col:
                                if len(effective_branches) == 1:
                                    query += f" WHERE {branch_col} = {effective_branches[0]}"
                                else:
                                    query += f" WHERE {branch_col} IN {tuple(effective_branches)}"
                            elif branch_col:
                                # No branches but we have the column - return empty
                                query += " WHERE 1=0"
                        
                        # 5. Get count for pagination
                        cr.execute(f"SELECT COUNT(*) FROM ({query}) AS count_query")
                        total_count = cr.fetchone()[0]
                        
                        # 6. Add sorting and pagination
                        if sort_col:
                            query += f" ORDER BY {sort_col} DESC"
                        
                        query += f" LIMIT {page_size} OFFSET {page * page_size}"
                        
                        # 7. Execute the query with configured timeout
                        timeout = int(retry_params.get_param('chart.view.query_timeout', default=30000))
                        cr.execute(f"SET LOCAL statement_timeout = {timeout}")
                        cr.execute(query)
                        results = cr.dictfetchall()
                        
                        # 8. Prepare the result
                        chart_data = self._extract_chart_data(chart, results, query)
                        chart_data['pagination'] = {
                            'total': total_count,
                            'page': page,
                            'page_size': page_size,
                            'pages': (total_count + page_size - 1) // page_size if page_size > 0 else 0
                        }
                        
                        # 9. Cache the result
                        request.env['res.dashboard.cache'].set_cache(cache_key, chart_data, user_id)
                        
                        # Return the result directly - success!
                        return chart_data
                    
                    except Exception as query_error:
                        _logger.warning(f"Error querying view {view_name}: {query_error}, attempt {retry_attempt+1}/{max_retries}")
                        
                        # If it's the last retry, try forcing view recreation
                        if retry_attempt == max_retries - 1:
                            try:
                                # Drop and recreate the view as a last resort
                                _logger.info(f"Attempting to drop and recreate view {view_name}")
                                cr.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
                                cr.commit()
                                
                                # Recreate in a new transaction
                                refresher = request.env['dashboard.chart.view.refresher'].sudo()
                                created = refresher.create_materialized_view_for_chart(chart.id)
                                
                                if not created:
                                    _logger.error(f"Failed to recreate view {view_name}")
                                    return self._get_chart_from_direct_query(chart, page, page_size, cco, branches_id, cache_key, user_id)
                                
                                # Wait for the database to update - dynamic delay
                                delay = base_delay * 2 * (retry_attempt + 1)
                                time.sleep(delay)
                            except Exception as drop_error:
                                _logger.error(f"Error dropping view {view_name}: {drop_error}")
                                return self._get_chart_from_direct_query(chart, page, page_size, cco, branches_id, cache_key, user_id)
            
            except Exception as e:
                _logger.error(f"Transaction error in attempt {retry_attempt+1}/{max_retries}: {e}")
                # Wait before retrying - dynamic delay
                delay = base_delay * (retry_attempt + 1)
                time.sleep(delay)
        
        # If we get here, all retries failed
        _logger.error(f"All {max_retries} attempts failed for chart {chart.id}, falling back to direct query")
        return self._get_chart_from_direct_query(chart, page, page_size, cco, branches_id, cache_key, user_id)
    
    # def _get_chart_from_materialized_view(self, chart, page, page_size, cco, branches_id, cache_key, user_id):
    #     """Get chart data from materialized view with pagination and detailed debugging"""
    #     try:
    #         view_name = f"dashboard_chart_view_{chart.id}"
            
    #         # First, check if the view actually exists
    #         with request.env.registry.cursor() as check_cr:
    #             check_cr.execute("""
    #                 SELECT EXISTS (
    #                     SELECT FROM pg_catalog.pg_class c
    #                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    #                     WHERE c.relname = %s AND c.relkind = 'm'
    #                 )
    #             """, (view_name,))
                
    #             view_exists = check_cr.fetchone()[0]
                
    #             if not view_exists:
    #                 _logger.warning(f"Materialized view {view_name} does not exist!")
    #                 # Try to create it on-demand
    #                 request.env['dashboard.chart.view.refresher'].create_materialized_view_for_chart(chart.id)
                    
    #                 # Recheck if it exists now
    #                 check_cr.execute("""
    #                     SELECT EXISTS (
    #                         SELECT FROM pg_catalog.pg_class c
    #                         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    #                         WHERE c.relname = %s AND c.relkind = 'm'
    #                     )
    #                 """, (view_name,))
                    
    #                 view_exists = check_cr.fetchone()[0]
                    
    #                 if not view_exists:
    #                     _logger.error(f"Failed to create materialized view {view_name}")
    #                     # return self._get_chart_from_direct_query(chart, page, page_size, cco, branches_id, cache_key, user_id)
            
    #         # Now get the columns from the materialized view
    #         with request.env.registry.cursor() as cr:
    #             cr.execute(f"""
    #                 SELECT column_name 
    #                 FROM information_schema.columns 
    #                 WHERE table_name = %s
    #             """, (view_name,))
                
    #             columns = [row[0] for row in cr.fetchall()]
                
    #             if not columns:
    #                 _logger.warning(f"No columns found in materialized view {view_name}")
                    
    #                 # Try to diagnose the issue
    #                 refresher = request.env['dashboard.chart.view.refresher']
    #                 diagnosis = refresher.diagnose_materialized_view(chart.id)
    #                 _logger.warning(f"Materialized view diagnosis: {diagnosis}")
                    
    #                 # Attempt to recreate the view
    #                 _logger.info(f"Attempting to recreate materialized view for chart {chart.id}")
    #                 refresher.create_materialized_view_for_chart(chart.id)
                    
    #                 # Check again for columns
    #                 cr.execute(f"""
    #                     SELECT column_name 
    #                     FROM information_schema.columns 
    #                     WHERE table_name = %s
    #                 """, (view_name,))
                    
    #                 columns = [row[0] for row in cr.fetchall()]
                    
    #                 if not columns:
    #                     _logger.error(f"Still no columns found after recreation - falling back to direct query")
    #                     return self._get_chart_from_direct_query(chart, page, page_size, cco, branches_id, cache_key, user_id)
                
    #             # Log all columns for debugging
    #             _logger.info(f"Columns in view {view_name}: {columns}")
                
    #             # Find the proper column for branch filtering
    #             branch_col = None
    #             if chart.branch_field:
    #                 branch_field = chart.branch_field.split('.')[-1] if '.' in chart.branch_field else chart.branch_field
                    
    #                 # First, try exact match
    #                 if branch_field in columns:
    #                     branch_col = branch_field
    #                     _logger.debug(f"Found exact branch column match: {branch_col}")
    #                 else:
    #                     # Try all column detection methods
    #                     branch_candidates = ['branch_id', 'id', 'branch', 'partner_branch_id']
    #                     for candidate in branch_candidates:
    #                         if candidate in columns:
    #                             branch_col = candidate
    #                             _logger.debug(f"Found branch column from candidates: {branch_col}")
    #                             break
                        
    #                     # If still not found, try columns containing 'branch'
    #                     if not branch_col:
    #                         for col in columns:
    #                             if 'branch' in col.lower():
    #                                 branch_col = col
    #                                 _logger.debug(f"Found branch column by partial match: {branch_col}")
    #                                 break
                
    #             # Build query against the materialized view
    #             query = f"SELECT * FROM {view_name}"
                
    #             # Apply security filters with proper column name
    #             where_clause_added = False
    #             if chart.branch_field and not cco and not self.security_service.is_cco_user():
    #                 user_branches = self.security_service.get_user_branch_ids()
    #                 effective_branches = []
                    
    #                 if branches_id:
    #                     # If branches specified in UI, intersect with user's branches
    #                     if user_branches:
    #                         effective_branches = [b for b in branches_id if b in user_branches]
    #                     else:
    #                         effective_branches = branches_id
    #                 elif user_branches:
    #                     # Otherwise use user's branches
    #                     effective_branches = user_branches
                    
    #                 # Build WHERE clause using the correct column name (not table alias)
    #                 if effective_branches and branch_col:
    #                     if len(effective_branches) == 1:
    #                         query += f" WHERE {branch_col} = {effective_branches[0]}"
    #                     else:
    #                         query += f" WHERE {branch_col} IN {tuple(effective_branches)}"
    #                     where_clause_added = True
    #                 elif branch_col:
    #                     # No branches specified, but we have a branch column - return no results
    #                     query += " WHERE 1=0"
    #                     where_clause_added = True
                
    #             # Add high-risk filter if needed (branch_id should be part of the effective_branches list)
    #             high_risk_filter = request.httprequest.cookies.get('high_risk_filter')
    #             if high_risk_filter == 'on':
    #                 _logger.info("High risk filter is enabled")
    #                 # Look for risk_level column
    #                 risk_column = None
    #                 risk_candidates = ['risk_level', 'partner_risk_level', 'customer_risk_level']
                    
    #                 for candidate in risk_candidates:
    #                     if candidate in columns:
    #                         risk_column = candidate
    #                         break
                    
    #                 if risk_column:
    #                     if where_clause_added:
    #                         query += f" AND {risk_column} = 'high'"
    #                     else:
    #                         query += f" WHERE {risk_column} = 'high'"
    #                         where_clause_added = True
                
    #             # Log the query for debugging
    #             _logger.info(f"Built materialized view query: {query}")
                
    #             # Get total count for pagination
    #             count_query = f"SELECT COUNT(*) as total FROM ({query}) AS count_query"
    #             try:
    #                 cr.execute(count_query)
    #                 count_result = cr.fetchone()
    #                 total_count = count_result[0] if count_result else 0
    #             except Exception as e:
    #                 _logger.error(f"Error running count query: {e}")
    #                 total_count = 0
                
    #             # Find a column for sorting
    #             sort_col = None
    #             if chart.y_axis_field:
    #                 y_field = chart.y_axis_field.split('.')[-1] if '.' in chart.y_axis_field else chart.y_axis_field
    #                 if y_field in columns:
    #                     sort_col = y_field
    #                 else:
    #                     # Look for numeric column names
    #                     candidates = ['count', 'customer_count', 'high_risk_customers', 'value', 'amount', 'total']
    #                     for candidate in candidates:
    #                         if candidate in columns:
    #                             sort_col = candidate
    #                             break
                
    #             # Add ORDER BY if we found a suitable column
    #             if sort_col:
    #                 query += f" ORDER BY {sort_col} DESC"
                
    #             # Add pagination
    #             query += f" LIMIT {page_size} OFFSET {page * page_size}"
                
    #             # Execute query with a timeout
    #             cr.execute("SET LOCAL statement_timeout = 30000;")  # 30 seconds
    #             try:
    #                 cr.execute(query)
    #                 results = cr.dictfetchall()
                    
    #                 # Log result count for debugging
    #                 _logger.info(f"Query returned {len(results)} rows")
                    
    #                 # Extract chart data
    #                 chart_data = self._extract_chart_data(chart, results, query)
                    
    #                 # Add pagination information
    #                 chart_data['pagination'] = {
    #                     'total': total_count,
    #                     'page': page,
    #                     'page_size': page_size,
    #                     'pages': (total_count + page_size - 1) // page_size if page_size > 0 else 0
    #                 }
                    
    #                 # Store in cache
    #                 request.env['res.dashboard.cache'].set_cache(cache_key, chart_data, user_id)
                    
    #                 return chart_data
    #             except Exception as query_err:
    #                 _logger.error(f"Error executing materialized view query: {query_err}")
    #                 # Fall back to direct query
    #                 return self._get_chart_from_direct_query(chart, page, page_size, cco, branches_id, cache_key, user_id)
    #     except Exception as e:
    #         _logger.error(f"Error getting chart from materialized view: {e}")
    #         # Fall back to direct query
    #         return self._get_chart_from_direct_query(chart, page, page_size, cco, branches_id, cache_key, user_id)


        
    def get_chart_data_from_direct_query(self, chart, cco, branches_id):
        """Get chart data directly from the database with thorough error prevention"""
        # Apply security to the query
        try:
            secured_query = self.security_service.secure_chart_query(chart, cco, branches_id)
            
            # Validate query syntax before execution
            is_valid, validation_message = self.validate_query_syntax(secured_query)
            if not is_valid:
                _logger.error(f"Invalid query for chart {chart.id}: {validation_message}")
                
                # For diagnosis, log the problematic parts
                _logger.error(f"Original query: {chart.query}")
                _logger.error(f"Secured query: {secured_query}")
                
                return {
                    'id': chart.id,
                    'title': chart.name,
                    'type': chart.chart_type,
                    'error': f"Query syntax error: {validation_message}",
                    'labels': [],
                    'datasets': [{'data': [], 'backgroundColor': []}]
                }
            
            # Create a new cursor for isolation
            with request.env.registry.cursor() as cr:
                try:
                    # Execute with timeout
                    cr.execute("SET LOCAL statement_timeout = 15000;")  # 15 seconds
                    
                    # Start timing
                    start_time = time.time()
                    
                    # Execute query with proper error handling
                    cr.execute(secured_query)
                    execution_time = (time.time() - start_time) * 1000  # ms
                    
                    # Fetch results
                    results = cr.dictfetchall()
                    
                    # Extract chart data
                    chart_data = self._extract_chart_data(chart, results, secured_query)
                    
                    # Add execution time for monitoring
                    chart_data['execution_time_ms'] = round(execution_time, 2)
                    
                    # Record statistics
                    self._record_execution_stats(chart.id, execution_time, 'success')
                    
                    return chart_data
                    
                except psycopg2.Error as sql_error:
                    # Explicit rollback
                    cr.rollback()
                    error_msg = str(sql_error)
                    _logger.error(f"SQL error for chart {chart.id}: {error_msg}")
                    
                    # Record the error
                    self._record_execution_stats(chart.id, 0, 'error', error_msg)
                    
                    # Provide user-friendly error
                    friendly_msg = self._get_friendly_error_message(error_msg)
                    return {
                        'id': chart.id,
                        'title': chart.name,
                        'type': chart.chart_type,
                        'error': friendly_msg,
                        'labels': [],
                        'datasets': [{'data': [], 'backgroundColor': []}]
                    }
                    
                except Exception as e:
                    # Handle any other errors
                    cr.rollback()
                    _logger.error(f"Error executing chart {chart.id}: {str(e)}")
                    
                    # Record the error
                    self._record_execution_stats(chart.id, 0, 'error', str(e))
                    
                    return {
                        'id': chart.id,
                        'title': chart.name,
                        'type': chart.chart_type,
                        'error': str(e),
                        'labels': [],
                        'datasets': [{'data': [], 'backgroundColor': []}]
                    }
                    
        except Exception as e:
            _logger.error(f"Error preparing chart query: {str(e)}")
            return {
                'id': chart.id,
                'title': chart.name,
                'type': chart.chart_type,
                'error': f"Error preparing query: {str(e)}",
                'labels': [],
                'datasets': [{'data': [], 'backgroundColor': []}]
            }

    def _get_friendly_error_message(self, error_msg):
        """Convert technical SQL errors to user-friendly messages"""
        if 'syntax error' in error_msg.lower():
            return "SQL syntax error. Please check your query format."
        elif 'timeout' in error_msg.lower():
            return "Query timed out. Please simplify your query or enable the materialized view option."
        elif 'does not exist' in error_msg.lower():
            if 'column' in error_msg.lower():
                column = re.search(r'column\s+"([^"]+)"', error_msg)
                if column:
                    return f"Column '{column.group(1)}' does not exist. Please check field names."
            elif 'relation' in error_msg.lower():
                table = re.search(r'relation\s+"([^"]+)"', error_msg)
                if table:
                    return f"Table '{table.group(1)}' does not exist. Please check table names."
            return "Referenced column or table does not exist. Please check your query."
        else:
            return f"Database error: {error_msg}"

    def validate_query_syntax(self, query):
        """Thoroughly validate SQL query syntax before execution"""
        try:
            # Remove comments
            query = re.sub(r'--.*?$', '', query, flags=re.MULTILINE)
            query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
            
            # Check for common SQL syntax errors
            error_patterns = [
                (r'WHERE\s+WHERE', 'Duplicate WHERE clause'),
                (r'AND\s+WHERE', 'Invalid AND WHERE sequence'),
                (r'\(\s*WHERE', 'WHERE inside parentheses without SELECT/FROM'),
                (r'WHERE\s*\)', 'WHERE followed directly by closing parenthesis'),
                (r'WHERE\s+OR\b', 'WHERE followed directly by OR'),
                (r'WHERE\s+ORDER', 'WHERE followed directly by ORDER'),
                (r'WHERE\s+GROUP', 'WHERE followed directly by GROUP'),
                (r'WHERE\s+HAVING', 'WHERE followed directly by HAVING'),
                (r'AND\s+OR\b', 'Mixed AND OR without parentheses'),
                (r'OR\s+AND\b', 'Mixed OR AND without parentheses'),
                (r'WHERE\s*$', 'WHERE at end of query without conditions'),
                (r'WHERE\s+SELECT', 'WHERE followed by SELECT without comparison'),
                (r'SELECT\s+FROM\s+WHERE', 'FROM followed directly by WHERE'),
                (r'FROM\s+WHERE\s+\w+', 'FROM WHERE sequence (missing table)'),
                (r'\.\s*IN\s*\(', 'Potential syntax error with IN clause'),
                (r'\.\s*WHERE', 'Table.WHERE syntax error')
            ]
            
            for pattern, error in error_patterns:
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    return False, f"SQL syntax error: {error} at '{match.group(0)}'"
            
            # Check for balanced parentheses
            if query.count('(') != query.count(')'):
                return False, "Unbalanced parentheses in query"
            
            # Check for common subquery errors
            subquery_pattern = r'\(\s*SELECT.*?FROM.*?\)'
            subqueries = re.finditer(subquery_pattern, query, re.IGNORECASE | re.DOTALL)
            
            for match in subqueries:
                subquery = match.group(0)
                # Validate subquery
                for pattern, error in error_patterns:
                    submatch = re.search(pattern, subquery, re.IGNORECASE)
                    if submatch:
                        return False, f"Subquery syntax error: {error} at '{submatch.group(0)}'"
            
            return True, "Query syntax appears valid"
            
        except Exception as e:
            return False, f"Query validation error: {str(e)}"
            
    def _handle_sql_error(self, sql_error, chart, query):
        """Handle specific SQL errors with better context and recovery options"""
        error_msg = str(sql_error)
        error_data = {
            'id': chart.id,
            'title': chart.name,
            'type': chart.chart_type,
            'error': error_msg,
            'labels': [],
            'datasets': [{'data': [], 'backgroundColor': []}]
        }
        
        # Record the error
        self._record_execution_stats(chart.id, 0, 'error', error_msg)
        
        if 'statement timeout' in error_msg.lower():
            # Query timeout error - offer solution
            _logger.error(f"Query timeout for chart {chart.id} - suggesting materialized view")
            
            error_data['error'] = (
                "Query timed out. This chart query is too complex for direct execution. "
                "Enable the 'Use Materialized View' option in chart settings for better performance."
            )
            
            # Try to automatically enable materialized view if it's not already enabled
            if not chart.use_materialized_view:
                try:
                    registry = request.env.registry
                    with registry.cursor() as cr:
                        env = api.Environment(cr, request.env.uid, request.env.context.copy())
                        isolated_chart = env['res.dashboard.charts'].browse(chart.id)
                        
                        if isolated_chart.exists():
                            isolated_chart.write({
                                'use_materialized_view': True,
                                'materialized_view_refresh_interval': 60,  # Default to hourly refresh
                                'last_error_message': 'Auto-enabling materialized view due to timeout'
                            })
                            cr.commit()
                            
                            # Try to create the view
                            env['dashboard.chart.view.refresher'].create_materialized_view_for_chart(chart.id)
                            
                            error_data['materialized_view_enabled'] = True
                            error_data['error'] += " (Materialized view has been automatically enabled)"
                except Exception as e:
                    _logger.error(f"Failed to auto-enable materialized view: {e}")
        
        elif 'column' in error_msg.lower() and 'does not exist' in error_msg.lower():
            # Column not found error
            column_match = re.search(r'column\s+"([^"]+)"\s+does not exist', error_msg, re.IGNORECASE)
            if column_match:
                missing_column = column_match.group(1)
                error_data['error'] = f"Column '{missing_column}' does not exist. Please check the query."
        
        elif 'relation' in error_msg.lower() and 'does not exist' in error_msg.lower():
            # Table not found error
            table_match = re.search(r'relation\s+"([^"]+)"\s+does not exist', error_msg, re.IGNORECASE)
            if table_match:
                missing_table = table_match.group(1)
                error_data['error'] = f"Table '{missing_table}' does not exist. Please check the query."
        
        return error_data
            
    def _record_execution_stats(self, chart_id, execution_time, status, error_message=None):
        """Record execution statistics for a chart with error isolation"""
        try:
            # Use a separate cursor to prevent transaction issues
            registry = request.env.registry
            with registry.cursor() as cr:
                env = api.Environment(cr, request.env.uid, request.env.context.copy())
                chart = env['res.dashboard.charts'].browse(chart_id)
                
                if chart.exists():
                    values = {
                        'last_execution_time': execution_time,
                        'last_execution_status': status,
                    }
                    
                    if error_message:
                        values['last_error_message'] = error_message
                    else:
                        values['last_error_message'] = False
                    
                    chart.write(values)
                    cr.commit()
        except Exception as e:
            _logger.error(f"Failed to record execution statistics: {e}")
    
    def _check_view_exists(self, view_name):
        """Check if a materialized view exists with proper error handling"""
        try:
            with self.env.registry.cursor() as cr:
                cr.execute("""
                    SELECT EXISTS (
                        SELECT FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = %s AND c.relkind = 'm'
                    )
                """, (view_name,))
                return cr.fetchone()[0]
        except Exception as e:
            _logger.error(f"Error checking if view exists: {e}")
            return False
            
    def _add_pagination_to_query(self, query, page, page_size):
        """Add pagination to a query, handling existing LIMIT clauses"""
        # Check if the query already has a LIMIT clause
        limit_match = re.search(r'\bLIMIT\b\s+\d+', query, re.IGNORECASE)
        
        if limit_match:
            # Create a subquery with the original query including its LIMIT
            # Then apply our pagination to that subquery
            return f"""
                WITH original_query AS ({query}) 
                SELECT * FROM original_query 
                OFFSET {page * page_size} 
                LIMIT {page_size}
            """
        else:
            # No LIMIT in the original query, add pagination directly
            if not query.strip().endswith(';'):
                query = query.strip() + " LIMIT " + str(page_size) + " OFFSET " + str(page * page_size)
            else:
                query = query.strip()[:-1] + " LIMIT " + str(page_size) + " OFFSET " + str(page * page_size) + ";"
            return query
    
    @http.route('/dashboard/dynamic_charts/', type='json', auth='user')
    def get_chart_data(self, cco=False, branches_id=None, **kw):
        """Get chart data in JSON format for all charts with improved branch handling"""
        try:
            # Input validation and sanitization
            if not isinstance(cco, bool):
                cco = str(cco).lower() == 'true'
                    
            if not isinstance(branches_id, list):
                try:
                    branches_id = json.loads(branches_id) if branches_id else []
                except (ValueError, TypeError):
                    branches_id = []
                
            # # Get current user ID
            # user_id = request.env.user.id
            # Get current user ID - support both regular request and background operation
            try:
                # Regular request
                user_id = request.env.user.id
            except (NameError, AttributeError):
                # Background operation - the request context manager would have set this
                user_id = self.env.user.id if hasattr(self, 'env') else None
                if not user_id:
                    # Last resort fallback
                    _logger.warning("Could not determine user_id in get_chart_data")
                    return []
            
            datepicked = kw.get('datepicked', 20000)
            
            # Log parameters for debugging
            _logger.debug(f"Chart data requested with cco={cco}, branches_id={branches_id}")
        
            # Generate cache key that includes user branch context for security
            cache_params = {
                'cco': cco,
                'branches_id': branches_id,
                'datepicked': datepicked,
                'user_branches': self.security_service.get_user_branch_ids()
            }
            
            cache_key = self.generate_cache_key('charts_data', cache_params)
            
            # Check if we have valid cache for this user
            cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)
            if cache_data:
                return cache_data
            
            # If no cache, generate the data
            charts = request.env['res.dashboard.charts'].search([('state', '=', 'active')])
            
            # Process charts
            results = []
            for chart in charts:
                try:
                    # Get user branch restriction status
                    user_has_branch_access = True
                    if not cco and chart.branch_filter:
                        # Check if user has access to any branches
                        user_branches = self.security_service.get_user_branch_ids()
                        
                        # Determine effective branches
                        effective_branches = []
                        if branches_id:
                            # If branches specified in UI, intersect with user's branches
                            if user_branches:
                                effective_branches = [b for b in branches_id if b in user_branches]
                            else:
                                effective_branches = branches_id
                        elif user_branches:
                            effective_branches = user_branches
                        
                        # Skip chart if user has no access to any branches
                        if not effective_branches:
                            user_has_branch_access = False
                    
                    if user_has_branch_access:
                        # Process chart with materialized view or direct query
                        if chart.use_materialized_view:
                            chart_data = self._get_chart_data_from_materialized_view(chart, cco, branches_id)
                        else:
                            chart_data = self._get_chart_data_from_direct_query(chart, cco, branches_id)
                        
                        if chart_data:
                            results.append(chart_data)
                except Exception as e:
                    _logger.error(f"Error processing chart {chart.id}: {e}")
                    # Skip this chart and continue with others
            
            # Store in cache before returning
            request.env['res.dashboard.cache'].set_cache(cache_key, results, user_id)
            
            return results
        except Exception as e:
            _logger.error(f"Error in get_chart_data: {e}")
            # Return empty result in case of errors
            return []
        
    # def _get_chart_data_from_materialized_view(self, chart, cco, branches_id):
    #     """Get chart data from materialized view"""
    #     try:
    #         # Ensure the view exists and is refreshed if needed
    #         refresher = request.env['dashboard.chart.view.refresher'].search([('chart_id', '=', chart.id)], limit=1)
    #         if not refresher:
    #             # If no refresher record, create the view first
    #             success = request.env['dashboard.chart.view.refresher'].create_materialized_view_for_chart(chart.id)
    #             if not success:
    #                 # Fall back to direct query if view creation fails
    #                 return self._get_chart_data_from_direct_query(chart, cco, branches_id)
    #             refresher = request.env['dashboard.chart.view.refresher'].search([('chart_id', '=', chart.id)], limit=1)
            
    #         view_name = refresher.view_name
            
    #         # Build query against the materialized view
    #         query = f"SELECT * FROM {view_name}"
            
    #         # Apply security filters
    #         if chart.branch_field and not cco and not self.security_service.is_cco_user():
    #             user_branches = self.security_service.get_user_branch_ids()
    #             effective_branches = []
                
    #             if branches_id:
    #                 # If branches specified in UI, intersect with user's branches
    #                 if user_branches:
    #                     effective_branches = [b for b in branches_id if b in user_branches]
    #                 else:
    #                     effective_branches = branches_id
    #             elif user_branches:
    #                 # Otherwise use user's branches
    #                 effective_branches = user_branches
                
    #             # Build WHERE clause
    #             if effective_branches:
    #                 if len(effective_branches) == 1:
    #                     query += f" WHERE {chart.branch_field} = {effective_branches[0]}"
    #                 else:
    #                     query += f" WHERE {chart.branch_field} IN {tuple(effective_branches)}"
    #             else:
    #                 query += " WHERE 1=0"  # No branches, return empty result
            
    #         # Add ORDER BY if available
    #         if chart.y_axis_field:
    #             query += f" ORDER BY {chart.y_axis_field} DESC"
            
    #         # Add LIMIT
    #         query += " LIMIT 100"  # Reasonable default limit
            
    #         # Execute query
    #         request.env.cr.execute(query)
    #         results = request.env.cr.dictfetchall()
            
    #         # Extract chart data
    #         return self._extract_chart_data(chart, results, query)
            
    #     except Exception as e:
    #         _logger.error(f"Error getting chart from materialized view: {e}")
    #         # Fall back to direct query
    #         return self._get_chart_data_from_direct_query(chart, cco, branches_id)
    
    def _get_chart_data_from_materialized_view(self, chart, cco, branches_id):
        """Get chart data from materialized view with robust column detection"""
        try:
            view_name = f"dashboard_chart_view_{chart.id}"
            
            # Create a dedicated cursor with appropriate transaction isolation
            with request.env.registry.cursor() as cr:
                # Set appropriate transaction isolation level
                cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                
                # Check if view exists first
                cr.execute("""
                    SELECT EXISTS (
                        SELECT FROM pg_catalog.pg_class c
                        WHERE c.relname = %s AND c.relkind = 'm'
                    )
                """, (view_name,))
                
                view_exists = cr.fetchone()[0]
                
                if not view_exists:
                    _logger.warning(f"Materialized view {view_name} does not exist!")
                    # Try to create it on-demand
                    success = request.env['dashboard.chart.view.refresher'].sudo().create_materialized_view_for_chart(chart.id)
                    if not success:
                        _logger.error(f"Failed to create materialized view for chart {chart.id}")
                        return self._get_chart_data_from_direct_query(chart, cco, branches_id)
                        
                    # Wait briefly for DB to update its internal state
                    time.sleep(0.5)
                
                # DIRECT QUERY APPROACH: Get columns directly from the view
                # This bypasses information_schema completely which can be stale
                try:
                    # Execute a query directly on the view to get columns
                    cr.execute(f"SELECT * FROM {view_name} LIMIT 0")
                    columns = [desc[0] for desc in cr.description]
                    
                    if not columns:
                        # Try with an actual row - sometimes that works when LIMIT 0 doesn't
                        cr.execute(f"SELECT * FROM {view_name} LIMIT 1")
                        columns = [desc[0] for desc in cr.description]
                    
                    _logger.info(f"Detected columns for view {view_name}: {columns}")
                except Exception as e:
                    _logger.error(f"Error getting columns directly from view: {e}")
                    columns = []
                    
                # If we still have no columns, try the system catalogs
                if not columns:
                    try:
                        # Query PostgreSQL system catalogs directly
                        cr.execute("""
                            SELECT a.attname
                            FROM pg_attribute a
                            JOIN pg_class c ON c.oid = a.attrelid
                            WHERE c.relname = %s
                            AND a.attnum > 0 AND NOT a.attisdropped
                            ORDER BY a.attnum
                        """, (view_name,))
                        
                        columns = [row[0] for row in cr.fetchall()]
                        _logger.info(f"Retrieved columns via system catalog: {columns}")
                    except Exception as e:
                        _logger.error(f"Error querying system catalog: {e}")
                
                # If still no columns, try to recreate the view
                if not columns:
                    try:
                        _logger.warning(f"No columns detected for {view_name}, attempting to recreate")
                        cr.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
                        cr.commit()
                        
                        # Recreate in a new transaction
                        request.env['dashboard.chart.view.refresher'].sudo().create_materialized_view_for_chart(chart.id)
                        
                        # Wait for database to update and try again
                        time.sleep(1.0)
                        
                        # Open a new cursor to avoid transaction visibility issues
                        with request.env.registry.cursor() as fresh_cr:
                            try:
                                fresh_cr.execute(f"SELECT * FROM {view_name} LIMIT 1")
                                columns = [desc[0] for desc in fresh_cr.description]
                                _logger.info(f"After recreation, detected columns: {columns}")
                            except Exception as e:
                                _logger.error(f"Failed to get columns after recreation: {e}")
                                return self._get_chart_data_from_direct_query(chart, cco, branches_id)
                    except Exception as e:
                        _logger.error(f"Failed to recreate view: {e}")
                        return self._get_chart_data_from_direct_query(chart, cco, branches_id)
                
                # If we still have no columns after all attempts, fall back to direct query
                if not columns:
                    _logger.warning(f"No columns found in materialized view {view_name}")
                    return self._get_chart_data_from_direct_query(chart, cco, branches_id)
                
                # We have columns! Now build and execute the query
                
                # Find the proper column for branch filtering
                branch_col = None
                if chart.branch_field:
                    branch_field = chart.branch_field.split('.')[-1] if '.' in chart.branch_field else chart.branch_field
                    
                    # Try direct match
                    if branch_field in columns:
                        branch_col = branch_field
                    else:
                        # Try finding a suitable column
                        for col in columns:
                            if col == 'id' or 'branch' in col.lower():
                                branch_col = col
                                break
                
                # Build query against the materialized view
                query = f"SELECT * FROM {view_name}"
                
                # Apply security filters with proper column name
                if chart.branch_field and not cco and not self.security_service.is_cco_user():
                    user_branches = self.security_service.get_user_branch_ids()
                    effective_branches = []
                    
                    if branches_id:
                        # If branches specified in UI, intersect with user's branches
                        if user_branches:
                            effective_branches = [b for b in branches_id if b in user_branches]
                        else:
                            effective_branches = branches_id
                    elif user_branches:
                        effective_branches = user_branches
                    
                    # Build WHERE clause using the correct column name
                    if effective_branches and branch_col:
                        if len(effective_branches) == 1:
                            query += f" WHERE {branch_col} = {effective_branches[0]}"
                        else:
                            query += f" WHERE {branch_col} IN {tuple(effective_branches)}"
                    elif branch_col:
                        # No branches specified, return no results
                        query += " WHERE 1=0"
                
                # Find column for sorting
                sort_col = None
                if chart.y_axis_field:
                    y_field = chart.y_axis_field.split('.')[-1] if '.' in chart.y_axis_field else chart.y_axis_field
                    if y_field in columns:
                        sort_col = y_field
                    else:
                        # Try to find a suitable numeric column
                        for col in columns:
                            if any(term in col.lower() for term in ['count', 'sum', 'amount', 'value', 'total']):
                                sort_col = col
                                break
                
                # Add ORDER BY if found a suitable column
                if sort_col:
                    query += f" ORDER BY {sort_col} DESC"
                
                # Add LIMIT
                query += " LIMIT 100"  # Default reasonable limit
                
                # Execute query with timeout protection
                cr.execute("SET LOCAL statement_timeout = 30000")  # 30 seconds
                cr.execute(query)
                results = cr.dictfetchall()
                
                # Process and return results
                return self._extract_chart_data(chart, results, query)
                
        except Exception as e:
            _logger.error(f"Error getting chart from materialized view: {e}")
            # Fall back to direct query
            return self._get_chart_data_from_direct_query(chart, cco, branches_id)


    
    def _get_chart_data_from_direct_query(self, chart, cco, branches_id):
        """Get chart data directly from the database with robust error handling and query optimization"""
        # Apply security to the query
        secured_query = self.security_service.secure_chart_query(chart, cco, branches_id)
        
        # Optimize query - add statement timeout and index hints
        if "SELECT" in secured_query:
            optimized_query = secured_query.replace('SELECT', 'SELECT /*+ PARALLEL(2) ROWS(1000) */', 1)
        else:
            optimized_query = secured_query
        
        try:
            # Execute with isolated cursor and longer timeout for complex queries
            with request.env.registry.cursor() as cr:
                try:
                    # Set a longer timeout for expensive queries
                    if 'JOIN' in optimized_query and 'GROUP BY' in optimized_query:
                        # Complex query - set higher timeout
                        cr.execute("SET LOCAL statement_timeout = 30000;")  # 30 seconds
                    else:
                        # Simple query - set normal timeout
                        cr.execute("SET LOCAL statement_timeout = 15000;")  # 15 seconds
                    
                    # Log query for debugging
                    _logger.debug(f"Executing query for chart {chart.id}: {optimized_query}")
                    
                    # Execute the optimized query
                    cr.execute(optimized_query)
                    
                    # Fetch results
                    results = cr.dictfetchall()
                    
                    # Extract chart data
                    return self._extract_chart_data(chart, results, optimized_query)
                    
                except Exception as query_error:
                    # Rollback to avoid transaction block
                    cr.rollback()
                    
                    # Handle statement timeout errors
                    if "statement timeout" in str(query_error):
                        _logger.warning(f"Query timeout for chart {chart.id} - enabling materialized view")
                        
                        # Try to auto-enable materialized view for this chart
                        self._auto_enable_materialized_view(chart.id)
                    
                    # Return error data
                    return {
                        'id': chart.id,
                        'title': chart.name,
                        'type': chart.chart_type,
                        'error': str(query_error),
                        'labels': [],
                        'datasets': [{'data': [], 'backgroundColor': []}]
                    }
                    
        except Exception as e:
            _logger.error(f"Error executing chart query: {e}")
            
            # Return basic error information
            return {
                'id': chart.id,
                'title': chart.name,
                'type': chart.chart_type,
                'error': str(e),
                'labels': [],
                'datasets': [{'data': [], 'backgroundColor': []}]
            }
            
    def _auto_enable_materialized_view(self, chart_id):
        """Auto-enable materialized view for queries that timeout"""
        try:
            # Use a separate transaction
            with request.env.registry.cursor() as cr:
                # Check if chart exists and isn't already using materialized view
                cr.execute("""
                    SELECT id, use_materialized_view 
                    FROM res_dashboard_charts 
                    WHERE id = %s
                """, (chart_id,))
                chart_data = cr.fetchone()
                
                if not chart_data or chart_data[1]:
                    # Chart doesn't exist or already using materialized view
                    return False
                
                # Enable materialized view
                cr.execute("""
                    UPDATE res_dashboard_charts
                    SET use_materialized_view = TRUE,
                        materialized_view_refresh_interval = 60,
                        last_error_message = 'Auto-enabled materialized view due to query timeout'
                    WHERE id = %s
                """, (chart_id,))
                
                # Create the materialized view
                cr.commit()
                
                # Start view creation in background
                self.env['dashboard.chart.view.refresher'].create_materialized_view_for_chart(chart_id)
                
                return True
                
        except Exception as e:
            _logger.error(f"Error auto-enabling materialized view: {e}")
            return False
    
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
        colors = self._generate_colors(chart.color_scheme, len(results))
        
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
    
    def _generate_colors(self, color_scheme, count):
        """Generate colors based on the selected color scheme"""
        color_schemes = {
            'cool': ['#3366cc', '#66ccff', '#6666ff', '#3333cc', '#000099'],
            'warm': ['#ff6600', '#ff9933', '#ffcc66', '#ff0000', '#cc0000'],
            'rainbow': ['#ff0000', '#ff9900', '#ffff00', '#00ff00', '#0099ff', '#6633ff'],
            'brown': [
                '#483E1D', '#F2D473', '#564B2B', '#ECDFA4', '#83733F',
                '#ECE1A2', '#5F5330', '#B78C00', '#6A5D36', '#C4AA55'
            ],
            'default': [
                '#483E1D', '#F2D473', '#564B2B', '#ECDFA4', '#83733F',
                '#ECE1A2', '#5F5330', '#B78C00', '#6A5D36', '#C4AA55'
            ]
        }
        
        # Get the color scheme or use default if not found
        base_colors = color_schemes.get(color_scheme, color_schemes['default'])
        
        # For small counts, return immediately without list comprehension
        if count <= len(base_colors):
            return base_colors[:count]
        
        # For larger counts, use list comprehension but with modulo for cycling
        return [base_colors[i % len(base_colors)] for i in range(count)]
    
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

    @http.route('/dashboard/chart_view_refresh/', type='json', auth='user')
    def refresh_chart_views(self, **kw):
        """Manually refresh chart materialized views - admin only"""
        if not request.env.user.has_group('base.group_system'):
            return {'error': 'Only administrators can refresh chart views'}
            
        try:
            # Get all active charts with materialized views enabled
            charts = request.env['res.dashboard.charts'].search([
                ('state', '=', 'active'),
                ('use_materialized_view', '=', True)
            ])
            
            refreshed_count = 0
            for chart in charts:
                if request.env['dashboard.chart.view.refresher'].refresh_chart_view(chart.id):
                    refreshed_count += 1
                    
            return {
                'success': True,
                'message': f'Successfully refreshed {refreshed_count} chart views'
            }
        except Exception as e:
            _logger.error(f"Error refreshing chart views: {e}")
            return {'error': str(e)}

























# from odoo import http
# from odoo.http import request
# import json
# from datetime import datetime, timedelta
# import logging

# # Import the services we'll create
# from ..utils.sql_parser import SQLParser
# from ..services.chart_data_service import ChartDataService
# from ..utils.cache_key_unique_identifier import get_unique_client_identifier

# _logger = logging.getLogger(__name__)

# class DynamicChartController(http.Controller):
#     """Controller for handling dynamic chart requests"""
    
#     def __init__(self):
#         super(DynamicChartController, self).__init__()
#         self.sql_parser = SQLParser()
#         self.chart_data_service = ChartDataService()
    
#     @http.route('/web/dynamic_charts/preview', type='json', auth='user')
#     def preview_chart(self, chart_type, query, x_axis_field=None, y_axis_field=None, color_scheme='default'):
#         """Preview chart without saving"""
#         try:
#             # Create a temporary chart for preview
#             chart = request.env['res.dashboard.charts'].new({
#                 'chart_type': chart_type,
#                 'query': query,
#                 'x_axis_field': x_axis_field,
#                 'y_axis_field': y_axis_field,
#                 'color_scheme': color_scheme
#             })
            
#             # Run validation manually since it's a new record
#             chart._check_query_safety()
            
#             # Process the query directly - no caching for previews
#             return self.chart_data_service.process_chart_data(chart, query)
#         except Exception as e:
#             _logger.error(f"Error in preview_chart: {e}")
#             return {'error': str(e)}
        
#     @http.route('/dashboard/dynamic_chart_page/', type='json', auth='user')
#     def get_chart_page(self, chart_id, page=0, page_size=50, cco=False, branches_id=None, **kw):
#         """Get paginated chart data for a single chart"""
#         if branches_id is None:
#             branches_id = []
        
#         # Get current user ID
#         user_id = request.env.user.id
        
#         # Fixed datepicked value for cache consistency
#         datepicked = 20000
        
#         # Generate cache key for this specific page
#         unique_id = get_unique_client_identifier()
#         cache_key = f"charts_data_{cco}_{branches_id}_{datepicked}_{unique_id}"
#         # cache_key = f"chart_page_{chart_id}_{page}_{page_size}_{cco}_{branches_id}_{unique_id}"
        
#         # Check cache first
#         cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)
#         if cache_data:
#             return cache_data
        
#         # Get the chart
#         chart = request.env['res.dashboard.charts'].browse(int(chart_id))
#         if not chart.exists():
#             return {'error': 'Chart not found'}
        
#         try:
#             # Build the modified query with filters (no date filtering)
#             query = self.chart_data_service.build_filtered_query(
#                 chart, 
#                 cco, 
#                 branches_id, 
#                 page, 
#                 page_size
#             )
            
#             # Get total count for pagination
#             count_query = self.chart_data_service.build_count_query(chart, query)
#             request.env.cr.execute(count_query)
#             total_count = request.env.cr.dictfetchone().get('total', 0)
            
#             # Process the results
#             result = self.chart_data_service.process_paginated_results(
#                 chart, 
#                 query, 
#                 total_count, 
#                 page, 
#                 page_size
#             )
            
#             # Store in cache before returning
#             request.env['res.dashboard.cache'].set_cache(cache_key, result, user_id)
#             return result
        
#         except Exception as e:
#             _logger.error(f"Error in get_chart_page: {e}")
#             return {'error': str(e)}

#     @http.route('/dashboard/dynamic_charts/', type='json', auth='user')
#     def get_chart_data(self, cco, branches_id, **kw):
#         """Get chart data in JSON format for all charts"""
        
#         # Get current user ID
#         user_id = request.env.user.id

#         # Get unique identifier 
#         unique_id = get_unique_client_identifier()
        
#         # Fixed datepicked value for cache consistency
#         datepicked = 20000
        
#         # Generate cache key
#         cache_key = f"charts_data_{cco}_{branches_id}_{unique_id}"
#         _logger.info(f"Charts cache key: {cache_key}")
        
#         # Check if we have valid cache for this user
#         cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)
#         if cache_data:
#             return cache_data
        
#         # Get all active charts
#         charts = request.env['res.dashboard.charts'].search([('state', '=', 'active')])
        
#         # Process all charts in a batch
#         results = self.chart_data_service.process_multiple_charts(charts, cco, branches_id)
        
#         # Store in cache before returning
#         request.env['res.dashboard.cache'].set_cache(cache_key, results, user_id)
        
#         return results
























# from odoo import http
# from odoo.http import request
# import json
# from datetime import datetime, timedelta
# import re
# import logging
# from ..utils.cache_key_unique_identifier import get_unique_client_identifier

# _logger = logging.getLogger(__name__)
# class DynamicChartController(http.Controller):


#     def extract_where_clauses(self, sql_query):
#         """Extract all clauses attached to the WHERE statement in an SQL query, handling BETWEEN clauses properly."""
#         # Normalize the query
#         sql_query = ' '.join(sql_query.strip().split())
        
#         # Find the WHERE clause (if any)
#         where_pattern = re.compile(r'\bWHERE\b(.*?)(?:\bGROUP BY\b|\bHAVING\b|\bORDER BY\b|\bLIMIT\b|\bOFFSET\b|$)', 
#                                 re.IGNORECASE | re.DOTALL)
#         where_match = where_pattern.search(sql_query)
        
#         if not where_match:
#             return []
        
#         where_content = where_match.group(1).strip()
        
#         if not where_content:
#             return []
        
#         # Use a more sophisticated approach to parse the WHERE clause
#         clauses = []
#         i = 0
#         current_clause = ""
#         paren_level = 0
#         in_between = False
#         in_quotes = False
#         quote_char = None
        
#         while i < len(where_content):
#             char = where_content[i]
#             current_clause += char
            
#             # Handle quotes - track when we're inside a string literal
#             if char in ["'", '"'] and (i == 0 or where_content[i-1] != '\\'):
#                 if not in_quotes:
#                     in_quotes = True
#                     quote_char = char
#                 elif char == quote_char:
#                     in_quotes = False
#                     quote_char = None
            
#             # Only process special characters if we're not inside quotes
#             if not in_quotes:
#                 if char == '(':
#                     paren_level += 1
#                 elif char == ')':
#                     paren_level -= 1
                
#                 # Check if we're starting a BETWEEN clause
#                 if paren_level == 0 and re.search(r'\bBETWEEN\b\s*$', current_clause, re.IGNORECASE):
#                     in_between = True
                
#                 # Check if we're ending a BETWEEN clause (after consuming the value after AND)
#                 if in_between and paren_level == 0:
#                     # Look for the AND within the BETWEEN clause
#                     if i >= 3 and where_content[i-3:i+1].upper() == ' AND':
#                         # We found the AND in BETWEEN x AND y, now look for the end of the value
#                         j = i + 1
#                         # Skip whitespace
#                         while j < len(where_content) and where_content[j].isspace():
#                             j += 1
                        
#                         # If it's a quoted value, find the closing quote
#                         if j < len(where_content) and where_content[j] in ["'", '"']:
#                             quote = where_content[j]
#                             j += 1
#                             while j < len(where_content) and where_content[j] != quote:
#                                 j += 1
#                             if j < len(where_content):  # Found closing quote
#                                 j += 1
#                         else:
#                             # If it's not quoted, find the end of the value
#                             while j < len(where_content) and where_content[j] not in [' ', '\t', '\n']:
#                                 j += 1
                        
#                         # Move past any whitespace after the value
#                         while j < len(where_content) and where_content[j].isspace():
#                             j += 1
                        
#                         # If the next token is AND/OR, we've completed the BETWEEN clause
#                         if j < len(where_content) and (where_content[j:j+5].upper() == 'AND (' or 
#                                                     where_content[j:j+4].upper() == 'AND ' or
#                                                     where_content[j:j+4].upper() == 'OR (' or
#                                                     where_content[j:j+3].upper() == 'OR '):
#                             in_between = False
                
#                 # Only split at top-level AND/OR operators that are not part of BETWEEN
#                 if paren_level == 0 and not in_between:
#                     and_match = re.search(r'\bAND\b\s*$', current_clause, re.IGNORECASE)
#                     or_match = re.search(r'\bOR\b\s*$', current_clause, re.IGNORECASE)
                    
#                     if and_match:
#                         clauses.append(current_clause[:-len(and_match.group(0))].strip())
#                         current_clause = ""
#                     elif or_match:
#                         clauses.append(current_clause[:-len(or_match.group(0))].strip())
#                         current_clause = ""
            
#             i += 1
        
#         if current_clause.strip():
#             clauses.append(current_clause.strip())
        
#         return clauses
    
#     def _clean_sql_value(self, value):
#         """Clean SQL values by removing quotes and converting special values."""
#         value = value.strip()
#         # Remove quotes if present
#         if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
#             value = value[1:-1]
#         # Convert SQL NULL to Python None/False
#         elif value.upper() == 'NULL':
#             value = False
#         # Convert numbers
#         elif value.isdigit():
#             value = int(value)
#         elif re.match(r'^-?\d+(\.\d+)?$', value):
#             value = float(value)
#         return value

#     def sql_where_to_odoo_domain_no_dates(self, sql_query):
#         """Convert SQL WHERE conditions to Odoo domain format, excluding date-related fields.
#         Maps table-prefixed fields to appropriate Odoo field names."""
#         clauses = self.extract_where_clauses(sql_query)
#         if not clauses:
#             return []
        
#         domain = []
#         date_related_keywords = ['date', 'date_created', 'date_create', 'create_date', 'write_date', 'time', 'datetime']
        
#         # Extract table aliases from the SQL query
#         table_aliases = self._extract_table_aliases(sql_query)
        
#         # Define field mappings based on table and field patterns
#         field_patterns = {
#             'branch': {  # This covers any alias for res_branch table
#                 'id': 'branch_id'  # Map any_branch_alias.id to branch_id
#             },
#             # Add other tables as needed
#         }
        
#         for clause in clauses:
#             # Skip date-related fields - more thorough check
#             if any(date_keyword in clause.lower() for date_keyword in date_related_keywords):
#                 continue
            
#             # Handle different operators
#             if ' = ' in clause:
#                 field, value = clause.split(' = ', 1)
#                 field = field.strip()
#                 field_name = self._map_field_name(field, table_aliases, field_patterns)
#                 value = self._clean_sql_value(value)
#                 domain.append((field_name, '=', value))
            
#             elif ' >= ' in clause:
#                 field, value = clause.split(' >= ', 1)
#                 field = field.strip()
#                 field_name = self._map_field_name(field, table_aliases, field_patterns)
#                 value = self._clean_sql_value(value)
#                 domain.append((field_name, '>=', value))
            
#             elif ' <= ' in clause:
#                 field, value = clause.split(' <= ', 1)
#                 field = field.strip()
#                 field_name = self._map_field_name(field, table_aliases, field_patterns)
#                 value = self._clean_sql_value(value)
#                 domain.append((field_name, '<=', value))
            
#             elif ' > ' in clause:
#                 field, value = clause.split(' > ', 1)
#                 field = field.strip()
#                 field_name = self._map_field_name(field, table_aliases, field_patterns)
#                 value = self._clean_sql_value(value)
#                 domain.append((field_name, '>', value))
            
#             elif ' < ' in clause:
#                 field, value = clause.split(' < ', 1)
#                 field = field.strip()
#                 field_name = self._map_field_name(field, table_aliases, field_patterns)
#                 value = self._clean_sql_value(value)
#                 domain.append((field_name, '<', value))
            
#             elif ' LIKE ' in clause.upper():
#                 field, value = clause.upper().split(' LIKE ', 1)
#                 field = field.strip()
#                 field_name = self._map_field_name(field.lower(), table_aliases, field_patterns)
#                 value = self._clean_sql_value(value)
#                 # Convert SQL LIKE pattern to Odoo pattern
#                 value = value.replace('%', '*')
#                 domain.append((field_name, 'ilike', value))
            
#             elif ' IN ' in clause.upper():
#                 field, value_list = clause.upper().split(' IN ', 1)
#                 field = field.strip()
#                 field_name = self._map_field_name(field.lower(), table_aliases, field_patterns)
#                 # Extract values from IN clause: (val1, val2, ...)
#                 if value_list.strip().startswith('(') and value_list.strip().endswith(')'):
#                     value_list = value_list.strip()[1:-1]
#                     values = [self._clean_sql_value(v.strip()) for v in value_list.split(',')]
#                     domain.append((field_name, 'in', values))
            
#             elif 'BETWEEN' in clause.upper() and ' AND ' in clause.upper():
#                 # Parse BETWEEN clause (already filtered date-related fields)
#                 parts = re.split(r'\bBETWEEN\b', clause, flags=re.IGNORECASE)
#                 if len(parts) == 2:
#                     field = parts[0].strip()
#                     field_name = self._map_field_name(field, table_aliases, field_patterns)
#                     between_parts = re.split(r'\bAND\b', parts[1], flags=re.IGNORECASE, maxsplit=1)
#                     if len(between_parts) == 2:
#                         start_val = self._clean_sql_value(between_parts[0])
#                         end_val = self._clean_sql_value(between_parts[1])
#                         domain.append('&')
#                         domain.append((field_name, '>=', start_val))
#                         domain.append((field_name, '<=', end_val))
            
#             elif ' IS NULL' in clause.upper():
#                 field = clause.upper().split(' IS NULL')[0].strip()
#                 field_name = self._map_field_name(field.lower(), table_aliases, field_patterns)
#                 domain.append((field_name, '=', False))
            
#             elif ' IS NOT NULL' in clause.upper():
#                 field = clause.upper().split(' IS NOT NULL')[0].strip()
#                 field_name = self._map_field_name(field.lower(), table_aliases, field_patterns)
#                 domain.append((field_name, '!=', False))
        
#         return domain

#     def _extract_table_aliases(self, sql_query):
#         """Extract table aliases from SQL query.
#         Returns a dictionary mapping aliases to table names."""
#         aliases = {}
        
#         # Normalize query and convert to lowercase for easier parsing
#         sql_query = ' '.join(sql_query.strip().split()).lower()
        
#         # Extract FROM clause
#         from_match = re.search(r'\bfrom\b(.*?)(?:\bwhere\b|\bjoin\b|\bgroup by\b|\bhaving\b|\border by\b|\blimit\b|\boffset\b|$)', 
#                             sql_query, re.IGNORECASE | re.DOTALL)
#         if from_match:
#             from_clause = from_match.group(1).strip()
#             # Extract table name and alias
#             table_match = re.search(r'(\w+)(?:\s+as)?\s+(\w+)', from_clause, re.IGNORECASE)
#             if table_match:
#                 table_name, alias = table_match.group(1), table_match.group(2)
#                 aliases[alias] = table_name
        
#         # Extract JOIN clauses
#         join_pattern = re.compile(r'\bjoin\b\s+(\w+)(?:\s+as)?\s+(\w+)', re.IGNORECASE)
#         for match in join_pattern.finditer(sql_query):
#             table_name, alias = match.group(1), match.group(2)
#             aliases[alias] = table_name
        
#         return aliases
    
#     def _map_field_name(self, field, table_aliases, field_patterns):
#         """Map a table-prefixed field to the appropriate Odoo field name."""
#         if '.' not in field:
#             return field  # No table prefix, return as is
        
#         alias, field_name = field.split('.', 1)
        
#         # If we have an alias match
#         if alias in table_aliases:
#             table_name = table_aliases[alias]
            
#             # Look for table patterns (e.g., if table contains 'branch')
#             for pattern, field_mappings in field_patterns.items():
#                 if pattern in table_name and field_name in field_mappings:
#                     return field_mappings[field_name]
        
#         # Default fallback: return just the field part
#         return field_name
    
#     def _add_where_to_query(self, query, where_clause):
        
#         if 'WHERE' in query.upper():
#             # Find the position of WHERE
#             where_pos = query.upper().find('WHERE')
#             # Get everything after WHERE (the original conditions)
#             where_content = query[where_pos + 5:].strip()
            
#             # Determine whether we need to add an AND
#             if where_content:
#                 # There are existing conditions, so add AND
#                 new_where = f"WHERE {where_clause} AND {where_content}"
#             else:
#                 # No existing conditions after WHERE
#                 new_where = f"WHERE {where_clause}"
                
#             return query[:where_pos] + new_where
        
#         # If no WHERE clause exists, add it before GROUP BY, ORDER BY, etc.
#         for clause in ['GROUP BY', 'ORDER BY', 'LIMIT']:
#             if clause in query.upper():
#                 position = query.upper().find(clause)
#                 return query[:position] + f" WHERE {where_clause} " + query[position:]
        
#         # If no clauses found, add WHERE at the end
#         return query + f" WHERE {where_clause}"


#     def _process_query_results(self, chart, query):
       
#         try:
           
#             request.env.cr.execute(query)
#             results = request.env.cr.dictfetchall()
#         except Exception as e:
#             print(e)

#         if len(results) == 0:
#             return {
#                 'title': '',
#                 'type': '',
#                 'labels': [],
#                 'datasets': [{'data': [], 'backgroundColor': []}]
#             }
        
#         # Extract labels and values
#         x_field = chart.x_axis_field or next(iter(results[0]))
#         y_field = chart.y_axis_field or next((k for k in results[0].keys() if k != x_field), None)
#         # Try to find an ID field - common patterns might be 'id', '{table}_id', etc.
#         id_field = next((k for k in results[0].keys() if k.endswith('_id') or k == 'id'), None)

#         # If no obvious ID field is found, use the first field that's not x_field or y_field
#         if not id_field:
#             id_field = next((k for k in results[0].keys() if k != x_field and k != y_field), None)

#         # Extract the IDs if we found a suitable field
#         ids = [r[id_field] if id_field else None for r in results]

    
        
#         if not y_field:
#             return {'error': 'Cannot determine Y-axis field from query results'}
        
#         labels = [str(r[x_field]) for r in results]
#         values = [float(r[y_field]) if r[y_field] is not None else 0 for r in results]
        
#         # Generate colors based on selected scheme
#         colors = self._generate_colors(chart.color_scheme, len(results))

#         domain_filter = self.sql_where_to_odoo_domain_no_dates(query)
        
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
    
#     def _generate_colors(self, color_scheme, count):
#         """Generate colors based on the selected color scheme"""
#         if color_scheme == 'cool':
#             base_colors = ['#3366cc', '#66ccff', '#6666ff', '#3333cc', '#000099']
#         elif color_scheme == 'warm':
#             base_colors = ['#ff6600', '#ff9933', '#ffcc66', '#ff0000', '#cc0000']
#         elif color_scheme == 'rainbow':
#             base_colors = ['#ff0000', '#ff9900', '#ffff00', '#00ff00', '#0099ff', '#6633ff']
#         elif color_scheme == 'brown':
#             base_colors = [
#                 '#483E1D',  # Dark earthy brown
#                 '#F2D473',  # Light golden brown
#                 '#564B2B',  # Dark earthy brown
#                 '#ECDFA4',  # Light cream brown
#                 '#83733F',  # Medium-dark olive brown
#                 '#ECE1A2',  # Light beige
#                 '#5F5330',  # Medium earthy brown
#                 '#B78C00',  # Golden amber brown
#                 '#6A5D36',  # Medium-light brown
#                 '#C4AA55',  # Medium-light golden brown
#             ]
#         else:  # default
#             base_colors = [
#                 '#483E1D',  # Dark earthy brown
#                 '#F2D473',  # Light golden brown
#                 '#564B2B',  # Dark earthy brown
#                 '#ECDFA4',  # Light cream brown
#                 '#83733F',  # Medium-dark olive brown
#                 '#ECE1A2',  # Light beige
#                 '#5F5330',  # Medium earthy brown
#                 '#B78C00',  # Golden amber brown
#                 '#6A5D36',  # Medium-light brown
#                 '#C4AA55',  # Medium-light golden brown
#             ]
#             # base_colors = ['#3366cc', '#dc3912', '#ff9900', '#109618', '#990099', '#0099c6']
        
#         colors = []
#         for i in range(count):
#             colors.append(base_colors[i % len(base_colors)])
#         return colors
    
#     @http.route('/web/dynamic_charts/preview', type='json', auth='user')
#     def preview_chart(self, chart_type, query, x_axis_field=None, y_axis_field=None, color_scheme='default'):
#         """Preview chart without saving"""
#         # if not request.env.user.has_group('dynamic_charts.group_dynamic_chart_manager'):
#         #     return {'error': 'Access denied'}
        
#         # Create a temporary chart for preview
#         try:
#             chart = request.env['res.dashboard.charts'].new({
#                 'chart_type': chart_type,
#                 'query': query,
#                 'x_axis_field': x_axis_field,
#                 'y_axis_field': y_axis_field,
#                 'color_scheme': color_scheme
#             })
            
#             # Run validation manually since it's a new record
#             chart._check_query_safety()
            
#             return self.get_chart_data(chart.id)
#         except Exception as e:
#             return {'error': str(e)}
        
#     @http.route('/dashboard/dynamic_chart_page/', type='json', auth='user')
#     def get_chart_page(self, chart_id, page=0, page_size=50, cco=False, branches_id=None, datepicked=20000, **kw):
#         """Get paginated chart data for a single chart"""
#         if branches_id is None:
#             branches_id = []
        
#         # Get current user ID
#         user_id = request.env.user.id
        
#         # Generate cache key for this specific page
#         unique_id = get_unique_client_identifier()
#         cache_key = f"chart_page_{chart_id}_{page}_{page_size}_{cco}_{branches_id}_{datepicked}_{unique_id}"
        
#         # Check cache first
#         cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)
#         if cache_data:
#             return cache_data
        
#         # Get the chart
#         chart = request.env['res.dashboard.charts'].browse(int(chart_id))
#         if not chart.exists():
#             return {'error': 'Chart not found'}
        
#         try:
#             # Build where clause based on conditions
#             if datepicked != 20000 and chart.date_field:
#                 today = datetime.now().date()
#                 prevDate = today - timedelta(days=datepicked)
#                 TIME_00_00_00 = "00:00:00"
#                 TIME_23_59_59 = "23:59:59"
#                 odooCurrentDate = f"{today} {TIME_23_59_59}"
#                 odooPrevDate = f"{prevDate} {TIME_00_00_00}"
#                 where_clause = f"{chart.date_field} BETWEEN '{odooPrevDate}' AND '{odooCurrentDate}'"
#             else:
#                 where_clause = f"{chart.date_field} >= '{odooPrevDate}'" if datepicked == 20000 else f"{chart.date_field} BETWEEN '{odooPrevDate}' AND '{odooCurrentDate}'"
            
#             # Add branch filtering if needed
#             if not cco and chart.branch_filter and branches_id and len(branches_id) > 0:
#                 if len(branches_id) == 1:
#                     where_clause += f" AND {chart.branch_field} = {branches_id[0]}"
#                 else:
#                     where_clause += f" AND {chart.branch_field} IN {tuple(branches_id)}"
#             elif not cco and chart.branch_filter and len(branches_id) == 0:
#                 where_clause += " AND 1 = 0"
            
#             # Get original query and prepare for modification
#             original_query = chart.query.replace(';', '')  # Remove semicolons
            
#             # Check if the query already has a LIMIT clause
#             limit_match = re.search(r'\bLIMIT\b\s+\d+', original_query, re.IGNORECASE)
            
#             if limit_match:
#                 # Extract the parts before and after the LIMIT clause
#                 limit_pos = limit_match.start()
#                 query_before_limit = original_query[:limit_pos].strip()
                
#                 # Create a subquery with the original query including its LIMIT
#                 # Then apply our pagination to that subquery
#                 modified_query = f"WITH original_query AS ({original_query}) SELECT * FROM original_query"
#                 count_query = f"WITH original_query AS ({original_query}) SELECT COUNT(*) as total FROM original_query"
#                 paginated_query = f"{modified_query} OFFSET {page * page_size} LIMIT {page_size}"
#             else:
#                 # No LIMIT in the original query, we can modify it directly
#                 modified_query = self._add_where_to_query(original_query, where_clause)
#                 count_query = f"SELECT COUNT(*) as total FROM ({modified_query}) AS count_table"
#                 paginated_query = f"{modified_query} LIMIT {page_size} OFFSET {page * page_size}"
            
#             # Get total count for pagination
#             request.env.cr.execute(count_query)
#             total_count = request.env.cr.dictfetchone().get('total', 0)
            
#             # Execute the paginated query
#             result = self._process_paginated_query_results(chart, paginated_query, total_count, page, page_size)
            
#             # Store in cache before returning
#             request.env['res.dashboard.cache'].set_cache(cache_key, result, user_id)
#             return result
        
#         except Exception as e:
#             _logger.error(f"Error in get_chart_page: {e}")
#             return {'error': str(e)}

#     def _process_paginated_query_results(self, chart, query, total_count, page, page_size):
#         """Process paginated query results"""
#         try:
#             request.env.cr.execute(query)
#             results = request.env.cr.dictfetchall()
#         except Exception as e:
#             _logger.error(f"Query execution error: {e}")
#             return {
#                 'error': str(e),
#                 'pagination': {
#                     'total': 0,
#                     'page': page,
#                     'page_size': page_size,
#                     'pages': 0
#                 }
#             }

#         if len(results) == 0:
#             return {
#                 'id': chart.id,
#                 'title': chart.name,
#                 'type': chart.chart_type,
#                 'model_name': chart.target_model,
#                 'filter': chart.domain_field,
#                 'column': chart.column,
#                 'labels': [],
#                 'datasets': [{'data': [], 'backgroundColor': []}],
#                 'pagination': {
#                     'total': total_count,
#                     'page': page,
#                     'page_size': page_size,
#                     'pages': (total_count + page_size - 1) // page_size if page_size > 0 else 0
#                 }
#             }
        
#         # Extract labels and values
#         x_field = chart.x_axis_field or next(iter(results[0]))
#         y_field = chart.y_axis_field or next((k for k in results[0].keys() if k != x_field), None)
        
#         # Try to find an ID field - common patterns might be 'id', '{table}_id', etc.
#         id_field = next((k for k in results[0].keys() if k.endswith('_id') or k == 'id'), None)

#         # If no obvious ID field is found, use the first field that's not x_field or y_field
#         if not id_field:
#             id_field = next((k for k in results[0].keys() if k != x_field and k != y_field), None)

#         # Extract the IDs if we found a suitable field
#         ids = [r[id_field] if id_field else None for r in results]
        
#         if not y_field:
#             return {
#                 'error': 'Cannot determine Y-axis field from query results',
#                 'pagination': {
#                     'total': total_count,
#                     'page': page,
#                     'page_size': page_size,
#                     'pages': (total_count + page_size - 1) // page_size if page_size > 0 else 0
#                 }
#             }
        
#         labels = [str(r[x_field]) for r in results]
#         values = [float(r[y_field]) if r[y_field] is not None else 0 for r in results]
        
#         # Generate colors based on selected scheme
#         colors = self._generate_colors(chart.color_scheme, len(results))

#         domain_filter = self.sql_where_to_odoo_domain_no_dates(query)
        
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
#             'domain_filter': domain_filter,
#             'pagination': {
#                 'total': total_count,
#                 'page': page,
#                 'page_size': page_size,
#                 'pages': (total_count + page_size - 1) // page_size if page_size > 0 else 0
#             }
#         }

#     @http.route('/dashboard/dynamic_charts/', type='json', auth='user')
#     def get_chart_data(self, cco, branches_id, datepicked, **kw):
#         """Get chart data in JSON format"""
        
#         # Get current user ID
#         user_id = request.env.user.id

#         # Get unique identifier 
#         unique_id = get_unique_client_identifier()
        
#         # Generate cache key - maintaining your existing cache key structure
#         cache_key = f"charts_data_{cco}_{branches_id}_{datepicked}_{unique_id}"

#         _logger.info(f"This is the charts cache key: {cache_key}")
        
#         # Check if we have valid cache for this user
#         cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)
#         if cache_data:
#             return cache_data
        
#         # Continue with existing functionality
#         charts = request.env['res.dashboard.charts'].search([('state', '=', 'active')])

#         today = datetime.now().date()  # Get today's date
#         prevDate = today - timedelta(days=datepicked)  # Get previous date

#         TIME_00_00_00 = "00:00:00"
#         TIME_23_59_59 = "23:59:59"

#         odooCurrentDate = f"{today} {TIME_23_59_59}"
#         odooPrevDate = f"{prevDate} {TIME_00_00_00}"

#         chartsData = []
        
#         for chart in charts:
#             chart = request.env['res.dashboard.charts'].browse(chart.id)
#             if not chart.exists():
#                 return {'error': 'Chart not found'}
            
#             try:
#                 # Build where clause based on conditions
#                 where_clause = f"{chart.date_field} >= '{odooPrevDate}'" if datepicked == 20000 else f"{chart.date_field} BETWEEN '{odooPrevDate}' AND '{odooCurrentDate}'"
                
#                 # Add branch filtering if needed
#                 if not cco and chart.branch_filter and branches_id and len(branches_id) > 0:
#                     if len(branches_id) == 1: 
#                         where_clause += f" AND {chart.branch_field} = {branches_id[0]}"
#                     else:
#                         where_clause += f" AND {chart.branch_field} IN {tuple(branches_id)}"
#                 elif not cco and chart.branch_filter and len(branches_id) == 0:
#                     where_clause += " AND 1 = 0"
                
#                 # Modify query to include WHERE clause
#                 query = self._add_where_to_query(chart.query, where_clause)
                
#                 # Execute query and process results
#                 result = self._process_query_results(chart, query)
                
#                 if result['title'] == '' and result['type'] == '' and result['labels'] == []:
#                     pass
#                 else:
#                     chartsData.append(result)
#             except Exception as e:
#                 return {'error': str(e)}
        
#         # Store in cache before returning
#         request.env['res.dashboard.cache'].set_cache(cache_key, chartsData, user_id)
        
#         return chartsData
