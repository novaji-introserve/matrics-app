from odoo import http
from odoo.http import request
import json
import logging
import re
from datetime import datetime, timedelta

_logger = logging.getLogger(__name__)

class DynamicChartController(http.Controller):
    """Controller for handling dynamic chart requests with improved performance"""
    
    def __init__(self):
        super(DynamicChartController, self).__init__()
        # Import here to avoid circular imports
        from ..services.chart_data_service import ChartDataService
        from ..utils.cache_key_unique_identifier import get_unique_client_identifier
        
        self.chart_data_service = ChartDataService()
        self.get_unique_client_identifier = get_unique_client_identifier
    
    @http.route('/web/dynamic_charts/preview', type='json', auth='user')
    def preview_chart(self, chart_type, query, x_axis_field=None, y_axis_field=None, color_scheme='default'):
        """Preview chart without saving - with query safety checks"""
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
                'color_scheme': color_scheme
            })
            
            # Run validation manually since it's a new record
            chart._check_query_safety()
            
            # Process query in isolated transaction
            with request.env.registry.cursor() as new_cr:
                # Execute with timeout
                new_cr.execute("SET LOCAL statement_timeout = 10000;")  # 10 seconds
                new_cr.execute(query)
                results = new_cr.dictfetchall()
                
                return self.chart_data_service._extract_chart_data(chart, results, query)
        except Exception as e:
            _logger.error(f"Error in preview_chart: {e}")
            return {'error': str(e)}
        
    @http.route('/dashboard/dynamic_chart_page/', type='json', auth='user')
    def get_chart_page(self, chart_id, page=0, page_size=50, cco=False, branches_id=None, **kw):
        """Get paginated chart data for a single chart with improved performance"""
        if branches_id is None:
            branches_id = []
        
        # Validate inputs
        try:
            chart_id = int(chart_id)
            page = max(0, int(page))
            page_size = min(100, max(1, int(page_size)))
        except (ValueError, TypeError):
            return {'error': 'Invalid parameters'}
        
        # Get current user ID
        user_id = request.env.user.id
        datepicked = 20000
        
        # Generate cache key for this specific page
        unique_id = self.get_unique_client_identifier()
        cache_key = f"chart_page_{chart_id}_{page}_{page_size}_{cco}_{branches_id}_{datepicked}_{unique_id}"
        
        # Check cache first
        cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)
        if cache_data:
            return cache_data
        
        try:
            # Get the chart
            chart = request.env['res.dashboard.charts'].browse(chart_id)
            if not chart.exists():
                return {'error': 'Chart not found'}
            
            # Execute count query in isolated transaction
            with request.env.registry.cursor() as cr:
                # Simple count query with timeout
                cr.execute("SET LOCAL statement_timeout = 5000;")  # 5 seconds
                cr.execute(f"""
                    SELECT COUNT(*) as total 
                    FROM ({chart.query.replace(';', '')}) AS count_query
                """)
                count_result = cr.dictfetchone()
                total_count = count_result['total'] if count_result else 0
            
            # Process the results with temporary table optimization
            result = self.chart_data_service.process_paginated_results(
                chart, 
                chart.query, 
                total_count, 
                page, 
                page_size
            )
            
            # Store in cache before returning
            request.env['res.dashboard.cache'].set_cache(cache_key, result, user_id, ttl=300)
            return result
        
        except Exception as e:
            _logger.error(f"Error in get_chart_page: {e}")
            return {'error': str(e)}

    @http.route('/dashboard/dynamic_charts/', type='json', auth='user')
    def get_chart_data(self, cco, branches_id, **kw):
        """Get chart data in JSON format for all charts with improved performance"""
        try:
            # Input validation and sanitization
            if not isinstance(cco, bool):
                cco = str(cco).lower() == 'true'
                
            if not isinstance(branches_id, list):
                try:
                    branches_id = json.loads(branches_id) if branches_id else []
                except (ValueError, TypeError):
                    branches_id = []
            
            # Get current user ID
            user_id = request.env.user.id
            
            datepicked = 20000
    
            # Generate cache key
            unique_id = self.get_unique_client_identifier()
            branches_str = json.dumps(branches_id) if branches_id else '[]'
            # cache_key = f"charts_data_{cco}_{branches_str}_{unique_id}"
            cache_key = f"charts_data_{cco}_{branches_str}_{datepicked}_{unique_id}"
            
            # Check if we have valid cache for this user
            cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)
            if cache_data:
                return cache_data
            
            # If no cache, generate the data
            result = self.get_chart_data_internal(cco, branches_id)
            
            # Return even if empty to avoid repeating slow queries
            if not result:
                result = []
                
            # Store in cache before returning
            request.env['res.dashboard.cache'].set_cache(cache_key, result, user_id)
            
            return result
        except Exception as e:
            _logger.error(f"Error in get_chart_data: {e}")
            # Return empty result in case of errors to prevent further requests
            return []
    
    def get_chart_data_internal(self, cco, branches_id):
        """Internal method for getting chart data with temporary table optimization"""
        # Get all active charts
        charts = request.env['res.dashboard.charts'].search([('state', '=', 'active')])
        
        # No charts, return empty list
        if not charts:
            return []
        
        # Process all charts in a batch with temporary table optimization
        return self.chart_data_service.process_multiple_charts(charts, cco, branches_id)
    
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
