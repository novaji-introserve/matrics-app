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

        super(DynamicChartController, self).__init__()

        # Import here to avoid circular imports

        from ..services.branch_security import ChartSecurityService

        from ..utils.cache_key_unique_identifier import generate_cache_key

        

        self.security_service = ChartSecurityService()

        self.generate_cache_key = generate_cache_key

        

        # Enable debug logging for chart issues

        self.debug_mode = True

        

    @http.route('/web/dynamic_charts/preview', type='json', auth='user')

    def preview_chart(self, chart_type, query, x_axis_field=None, y_axis_field=None, color_scheme='default'):

        """Preview chart without saving - with query safety checks and security"""

        try:

            chart_data_service = ChartDataService()

            # Apply query limit for safety

            if not chart_data_service._is_safe_query(query):

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

                

                chart_data_service = ChartDataService()

                # Extract chart data

                chart_data = chart_data_service._extract_chart_data(chart, results, secured_query)

                

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

                        chart_data_service = ChartDataService()

                        chart_data = chart_data_service._extract_chart_data(chart, results, query)

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

                    chart_data_service = ChartDataService()

                    chart_data = chart_data_service._extract_chart_data(chart, results, secured_query)

                    

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

            # Try to get cached data instead of returning empty list

            try:

                cached_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)

                if cached_data:

                    _logger.info(f"Retrieved cached data for key {cache_key} after error")

                    return cached_data

            except Exception as cache_err:

                _logger.error(f"Error retrieving cache after failure: {cache_err}")

            

            # Return error objects instead of empty array

            return [

                {

                    'id': 'error',

                    'title': 'Chart Data Error',

                    'type': 'bar',

                    'error': str(e),

                    'labels': [],

                    'datasets': [{'data': [], 'backgroundColor': []}]

                }

            ]

        # except Exception as e:

        #     _logger.error(f"Error in get_chart_data: {e}")

        #     # Return empty result in case of errors

        #     return []

    

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

                

                # DIRECT QUERY APPROACH: Get columns directly from the view

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

                

                # If still no columns, fall back to direct query

                if not columns:

                    _logger.warning(f"No columns found in materialized view {view_name}")

                    return self._get_chart_data_from_direct_query(chart, cco, branches_id)

                

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

                

                # CRITICAL FIX: Apply security filters for non-CCO users properly

                # Apply security filters with proper column name

                where_clause_added = False

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

                    

                    # If non-CCO user has no branches (effective_branches is empty),

                    # and the table has branch filtering, they should see nothing

                    if not effective_branches and branch_col:

                        query += " WHERE 1=0"  # Return no results

                        where_clause_added = True

                    # Build WHERE clause using the correct column name

                    elif effective_branches and branch_col:

                        if len(effective_branches) == 1:

                            query += f" WHERE {branch_col} = {effective_branches[0]}"

                        else:

                            query += f" WHERE {branch_col} IN {tuple(effective_branches)}"

                        where_clause_added = True

                

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

                chart_data_service = ChartDataService()

                return chart_data_service._extract_chart_data(chart, results, query)

                # return self._extract_chart_data(chart, results, query)

                

        except Exception as e:

            _logger.error(f"Error getting chart from materialized view: {e}")

            # Fall back to direct query

            return self._get_chart_data_from_direct_query(chart, cco, branches_id)

    

    # def _get_chart_data_from_materialized_view(self, chart, cco, branches_id):

    #     """Get chart data from materialized view with robust column detection"""

    #     try:

    #         view_name = f"dashboard_chart_view_{chart.id}"

            

    #         # Create a dedicated cursor with appropriate transaction isolation

    #         with request.env.registry.cursor() as cr:

    #             # Set appropriate transaction isolation level

    #             cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")

                

    #             # Check if view exists first

    #             cr.execute("""

    #                 SELECT EXISTS (

    #                     SELECT FROM pg_catalog.pg_class c

    #                     WHERE c.relname = %s AND c.relkind = 'm'

    #                 )

    #             """, (view_name,))

                

    #             view_exists = cr.fetchone()[0]

                

    #             if not view_exists:

    #                 _logger.warning(f"Materialized view {view_name} does not exist!")

    #                 # Try to create it on-demand

    #                 success = request.env['dashboard.chart.view.refresher'].sudo().create_materialized_view_for_chart(chart.id)

    #                 if not success:

    #                     _logger.error(f"Failed to create materialized view for chart {chart.id}")

    #                     return self._get_chart_data_from_direct_query(chart, cco, branches_id)

                        

    #                 # Wait briefly for DB to update its internal state

    #                 time.sleep(0.5)

                

    #             # DIRECT QUERY APPROACH: Get columns directly from the view

    #             # This bypasses information_schema completely which can be stale

    #             try:

    #                 # Execute a query directly on the view to get columns

    #                 cr.execute(f"SELECT * FROM {view_name} LIMIT 0")

    #                 columns = [desc[0] for desc in cr.description]

                    

    #                 if not columns:

    #                     # Try with an actual row - sometimes that works when LIMIT 0 doesn't

    #                     cr.execute(f"SELECT * FROM {view_name} LIMIT 1")

    #                     columns = [desc[0] for desc in cr.description]

                    

    #                 _logger.info(f"Detected columns for view {view_name}: {columns}")

    #             except Exception as e:

    #                 _logger.error(f"Error getting columns directly from view: {e}")

    #                 columns = []

                    

    #             # If we still have no columns, try the system catalogs

    #             if not columns:

    #                 try:

    #                     # Query PostgreSQL system catalogs directly

    #                     cr.execute("""

    #                         SELECT a.attname

    #                         FROM pg_attribute a

    #                         JOIN pg_class c ON c.oid = a.attrelid

    #                         WHERE c.relname = %s

    #                         AND a.attnum > 0 AND NOT a.attisdropped

    #                         ORDER BY a.attnum

    #                     """, (view_name,))

                        

    #                     columns = [row[0] for row in cr.fetchall()]

    #                     _logger.info(f"Retrieved columns via system catalog: {columns}")

    #                 except Exception as e:

    #                     _logger.error(f"Error querying system catalog: {e}")

                

    #             # If still no columns, try to recreate the view

    #             if not columns:

    #                 try:

    #                     _logger.warning(f"No columns detected for {view_name}, attempting to recreate")

    #                     cr.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")

    #                     cr.commit()

                        

    #                     # Recreate in a new transaction

    #                     request.env['dashboard.chart.view.refresher'].sudo().create_materialized_view_for_chart(chart.id)

                        

    #                     # Wait for database to update and try again

    #                     time.sleep(1.0)

                        

    #                     # Open a new cursor to avoid transaction visibility issues

    #                     with request.env.registry.cursor() as fresh_cr:

    #                         try:

    #                             fresh_cr.execute(f"SELECT * FROM {view_name} LIMIT 1")

    #                             columns = [desc[0] for desc in fresh_cr.description]

    #                             _logger.info(f"After recreation, detected columns: {columns}")

    #                         except Exception as e:

    #                             _logger.error(f"Failed to get columns after recreation: {e}")

    #                             return self._get_chart_data_from_direct_query(chart, cco, branches_id)

    #                 except Exception as e:

    #                     _logger.error(f"Failed to recreate view: {e}")

    #                     return self._get_chart_data_from_direct_query(chart, cco, branches_id)

                

    #             # If we still have no columns after all attempts, fall back to direct query

    #             if not columns:

    #                 _logger.warning(f"No columns found in materialized view {view_name}")

    #                 return self._get_chart_data_from_direct_query(chart, cco, branches_id)

                

    #             # We have columns! Now build and execute the query

                

    #             # Find the proper column for branch filtering

    #             branch_col = None

    #             if chart.branch_field:

    #                 branch_field = chart.branch_field.split('.')[-1] if '.' in chart.branch_field else chart.branch_field

                    

    #                 # Try direct match

    #                 if branch_field in columns:

    #                     branch_col = branch_field

    #                 else:

    #                     # Try finding a suitable column

    #                     for col in columns:

    #                         if col == 'id' or 'branch' in col.lower():

    #                             branch_col = col

    #                             break

                

    #             # Build query against the materialized view

    #             query = f"SELECT * FROM {view_name}"

                

    #             # Apply security filters with proper column name

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

    #                     effective_branches = user_branches

                    

    #                 # Build WHERE clause using the correct column name

    #                 if effective_branches and branch_col:

    #                     if len(effective_branches) == 1:

    #                         query += f" WHERE {branch_col} = {effective_branches[0]}"

    #                     else:

    #                         query += f" WHERE {branch_col} IN {tuple(effective_branches)}"

    #                 elif branch_col:

    #                     # No branches specified, return no results

    #                     query += " WHERE 1=0"

                

    #             # Find column for sorting

    #             sort_col = None

    #             if chart.y_axis_field:

    #                 y_field = chart.y_axis_field.split('.')[-1] if '.' in chart.y_axis_field else chart.y_axis_field

    #                 if y_field in columns:

    #                     sort_col = y_field

    #                 else:

    #                     # Try to find a suitable numeric column

    #                     for col in columns:

    #                         if any(term in col.lower() for term in ['count', 'sum', 'amount', 'value', 'total']):

    #                             sort_col = col

    #                             break

                

    #             # Add ORDER BY if found a suitable column

    #             if sort_col:

    #                 query += f" ORDER BY {sort_col} DESC"

                

    #             # Add LIMIT

    #             query += " LIMIT 100"  # Default reasonable limit

                

    #             # Execute query with timeout protection

    #             cr.execute("SET LOCAL statement_timeout = 30000")  # 30 seconds

    #             cr.execute(query)

    #             results = cr.dictfetchall()

                

    #             # Process and return results

    #             chart_data_service = ChartDataService()

    #             return chart_data_service._extract_chart_data(chart, results, query)

                

    #     except Exception as e:

    #         _logger.error(f"Error getting chart from materialized view: {e}")

    #         # Fall back to direct query

    #         return self._get_chart_data_from_direct_query(chart, cco, branches_id)





    

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

                    chart_data_service = ChartDataService()

                    return chart_data_service._extract_chart_data(chart, results, optimized_query)

                    

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


