# -*- coding: utf-8 -*-

import json
from odoo import http
from odoo.http import request
from datetime import datetime, timedelta
from odoo import fields
import re
from ..utils.cache_key_unique_identifier import get_unique_client_identifier, normalize_cache_key_components
import logging
from ..services.branch_security import ChartSecurityService

_logger = logging.getLogger(__name__)


class Compliance(http.Controller):
    def __init__(self):
        super(Compliance, self).__init__()
        self.security_service = ChartSecurityService()

    @http.route('/dashboard/user', auth='public', type='json')
    def index(self, **kw):
        user = request.env.user
        is_superuser = user.has_group('base.group_system')
        
        # Use the security service for CCO and CO checks
        is_cco = self.security_service.is_cco_user()
        is_co = self.security_service.is_co_user()
        
        branch = [branch.id for branch in user.branches_id]
        unique_id = get_unique_client_identifier() 

        result = {
            "group": is_cco,  # Keep the original field name for backward compatibility
            "is_cco": is_cco,  # Add explicit field for CCO status
            "is_co": is_co,    # Add field for CO status
            "branch": branch,
            "unique_id": unique_id,
        }
        return result
    
    def check_branches_id(self, branches_id):
        # Ensure branches_id is a list
        if not isinstance(branches_id, list):
            branches_id = [branches_id]  # Convert to list if it's a single integer
            return branches_id
        else:
            return branches_id

    @http.route('/dashboard/dynamic_sql', auth='public', type='json')
    def extract_table_and_domain(self, sql_query: str, branches_id, cco):
        """
        Extract table names and WHERE conditions from SQL queries using regex patterns.
        Ignores COUNT aggregation function while still blocking other aggregation functions.
        """
        # Check if user is CCO or CO using the security service
        is_cco = self.security_service.is_cco_user()
        is_co = self.security_service.is_co_user()
        
        # If user is CCO or CO, override the cco parameter to true
        if is_co:
            cco = True
            _logger.info(f"CO user {request.env.user.id} accessing dynamic SQL with CCO privileges")
        
        lower_query = sql_query.lower()
        table = None
        domain = []

        # Check for aggregation functions (sum, avg, min, max) in the SELECT clause
        # Specifically exclude COUNT from the check
        if re.search(r"\b(?:sum|avg|min|max)\s*\(", lower_query):
            return None

        # Extract table name (improved to handle more cases)
        from_match = re.search(r"\bfrom\s+([\w.]+)", lower_query)
        if from_match:
            table = from_match.group(1)
        else:
            join_match = re.search(r"\b(?:inner|left|right|full outer)?\s+join\s+([\w.]+)", lower_query)
            if join_match:
                # We're not handling complex joins in this version
                return None
        
        # Extract WHERE clause conditions and convert to Odoo domain format
        where_match = re.search(r"\bwhere\s+(.+?)(?:\s+(?:group\s+by|order\s+by|limit|having)\s+|\s*$)", lower_query, re.DOTALL)
        if where_match:
            condition_string = where_match.group(1).strip()
            domain = self.parse_condition_string(condition_string) 
        
        # Collect additional filters
        additional_filters = []

        if table == "res_partner":
            additional_filters.append(("origin", "in", ["demo", "test", "prod"]))

        check_query = """SELECT 1 FROM information_schema.columns 
                        WHERE table_name = %s AND column_name = 'branch_id'
                     """
        request.env.cr.execute(check_query, (table,))
        has_branch_id = request.env.cr.fetchone() is not None

        # Only add branch filter for non-CCO/CO users
        if not cco and has_branch_id:
            branch_ids = self.check_branches_id(branches_id)
            additional_filters.append(("branch_id", "in", branch_ids))
            
        # Combine domain with additional filters
        if additional_filters:
            if domain:
                # Check if domain is complex (contains '|')
                is_complex = any(op == '|' for op in domain if isinstance(op, str))
                if is_complex:
                    # Wrap complex domain to preserve precedence
                    domain = ['&'] + domain + [additional_filters[0]]
                    # Add remaining additional filters with '&' operators
                    for filter_item in additional_filters[1:]:
                        domain = ['&'] + domain + [filter_item]
                else:
                    # Simple domain: append filters with '&' operators if needed
                    for filter_item in additional_filters:
                        domain = ['&'] + domain + [filter_item]
            else:
                # No parsed domain, use only additional filters
                domain = additional_filters
        else:
            # No additional filters, keep parsed domain
            pass

        _logger.info(f"Final domain: {domain}")
       
        return {'table': table, 'domain': domain}
    
    def parse_condition_string(self, condition_string):
        """
        Parse SQL WHERE conditions into Odoo domain format
        with proper handling of AND, OR operators and parentheses
        """
        if not condition_string:
            return []
            
        # Clean up the condition string
        condition_string = condition_string.strip()
        
        # Track nesting of parentheses and operators
        def parse_expression(expr, depth=0):
            _logger.info(f"Parsing expression (depth {depth}): {expr}")
            
            # Handle empty expression
            if not expr.strip():
                return []
                
            # Split by OR at the current nesting level
            or_parts = self._split_by_operator(expr, "OR")
            
            if len(or_parts) > 1:
                # This is an OR expression
                result = []
                for i in range(len(or_parts) - 1):
                    result.append('|')
                for part in or_parts:
                    result.extend(parse_expression(part, depth + 1))
                return result
                
            # Split by AND at the current nesting level
            and_parts = self._split_by_operator(expr, "AND")
            
            if len(and_parts) > 1:
                # This is an AND expression
                result = []
                for part in and_parts[:-1]:
                    result.append('&')
                    result.extend(parse_expression(part, depth + 1))
                result.extend(parse_expression(and_parts[-1], depth + 1))
                return result
                
            # Handle parenthesized expression
            if expr.strip().startswith('(') and expr.strip().endswith(')'):
                inner_expr = expr.strip()[1:-1].strip()
                return parse_expression(inner_expr, depth + 1)
                
            # Base case: single condition
            return [self._parse_single_condition(expr)]
            
        # Parse the entire condition string
        try:
            domain = parse_expression(condition_string)
            _logger.info(f"Parsed domain: {domain}")
            return domain
        except Exception as e:
            _logger.error(f"Error parsing condition: {e}")
            return []
            
    def _split_by_operator(self, expr, operator):
        """
        Split a SQL expression by an operator (AND/OR) while respecting parentheses
        """
        operator = f" {operator} "  # Add spaces to match only the operator keyword
        parts = []
        current_part = ""
        paren_level = 0
        quote_char = None
        
        i = 0
        while i < len(expr):
            char = expr[i]
            
            # Handle quotes
            if char in ["'", '"'] and (i == 0 or expr[i-1] != '\\'):
                if quote_char is None:
                    quote_char = char
                elif quote_char == char:
                    quote_char = None
                    
            # Skip everything inside quotes
            if quote_char is not None:
                current_part += char
                i += 1
                continue
                
            # Handle parentheses
            if char == '(':
                paren_level += 1
            elif char == ')':
                paren_level -= 1
                
            # Check for operator at the current position
            if (paren_level == 0 and 
                i + len(operator) <= len(expr) and 
                expr[i:i+len(operator)].upper() == operator):
                # Found the operator at current level
                parts.append(current_part.strip())
                current_part = ""
                i += len(operator)
            else:
                current_part += char
                i += 1
                
        # Add the last part
        if current_part:
            parts.append(current_part.strip())
            
        return parts
        
    def _parse_single_condition(self, condition):
        """
        Parse a single SQL condition into an Odoo domain tuple
        """
        condition = condition.strip()
        
        # Handle IS TRUE/FALSE conditions
        is_true_match = re.search(r"(\w+)\s+is\s+true", condition.lower())
        if is_true_match:
            field = is_true_match.group(1).strip()
            return (field, "=", True)
        
        is_false_match = re.search(r"(\w+)\s+is\s+false", condition.lower())
        if is_false_match:
            field = is_false_match.group(1).strip()
            return (field, "=", False)
        
        # Handle IS NULL/NOT NULL (from our previous fix)
        if " is null" in condition.lower():
            field = condition.lower().split(" is null")[0].strip()
            return (field, "=", False)
            
        if " is not null" in condition.lower():
            field = condition.lower().split(" is not null")[0].strip()
            return (field, "!=", False)
            
        # Handle LIKE operator
        if " like " in condition.lower():
            parts = condition.lower().split(" like ")
            field = parts[0].strip()
            value = parts[1].strip().strip("'\"")
            # Convert SQL LIKE wildcards to Odoo's ilike
            return (field, "ilike", value.replace('%', ''))
            
        # Handle standard operators
        ops_map = {
            "=": "=",
            ">": ">",
            ">=": ">=",
            "<": "<",
            "<=": "<=",
            "!=": "!=",
            "<>": "!="
        }
        
        for op in ops_map.keys():
            if f" {op} " in condition:
                parts = condition.split(f" {op} ", 1)
                field = parts[0].strip()
                value = parts[1].strip().strip("'\"")
                
                # Handle boolean values
                if value.lower() == 'true':
                    value = True
                elif value.lower() == 'false':
                    value = False
                # Handle numeric values
                elif value.isdigit():
                    value = int(value)
                elif value.replace('.', '', 1).isdigit():
                    value = float(value)
                    
                return (field, ops_map[op], value)
                
        # If we couldn't parse it, return as is
        _logger.warning(f"Could not parse condition: {condition}")
        return (condition, "=", True)

    def _convert_to_odoo_tuple(self, condition: str):
        """
        Convert a simple condition string to an Odoo domain tuple.
        Handles IS NULL, LIKE, IN, =, >, <, etc.
        """
        condition = condition.strip()
        
        if condition.startswith('(') and condition.endswith(')'):
            condition = condition[1:-1].strip()
        
        lower_condition = condition.lower()
        
        if ' is true' in lower_condition:
            field = lower_condition.split(' is true')[0].strip()
            return [(field, '=', True)]
            
        if ' is false' in lower_condition:
            field = lower_condition.split(' is false')[0].strip()
            return [(field, '=', False)]
        
        if ' = true' in lower_condition:
            field = lower_condition.split(' = true')[0].strip()
            return [(field, '=', True)]
            
        if ' = false' in lower_condition:
            field = lower_condition.split(' = false')[0].strip()
            return [(field, '=', False)]
        
        if ' is null' in lower_condition:
            field = lower_condition.split(' is null')[0].strip()
            return [(field, '=', False)]
        
        if ' is not null' in lower_condition:
            field = lower_condition.split(' is not null')[0].strip()
            return [(field, '!=', False)]
        
        if ' like ' in lower_condition:
            parts = condition.split(' like ', 1) if ' like ' in lower_condition else condition.split(' LIKE ', 1)
            field = parts[0].strip()
            value = self._extract_quoted_value(parts[1].strip())
            return [(field, '=like', value)]
        
        if ' in ' in lower_condition:
            parts = condition.split(' in ', 1) if ' in ' in lower_condition else condition.split(' IN ', 1)
            field = parts[0].strip()
            values_str = parts[1].strip()
            
            if values_str.startswith('(') and values_str.endswith(')'):
                values_str = values_str[1:-1].strip()
            
            values = []
            for val in values_str.split(','):
                val = val.strip()
                if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
                    values.append(val[1:-1])
                elif val.isdigit():
                    values.append(int(val))
                elif val.replace('.', '', 1).isdigit():
                    values.append(float(val))
                else:
                    values.append(val)
            
            return [(field, 'in', values)]
        
        for op in ['!=', '>=', '<=', '=', '>', '<']:
            if f' {op} ' in condition:
                parts = condition.split(f' {op} ', 1)
                field = parts[0].strip()
                value = self._parse_value(parts[1].strip())
                return [(field, op, value)]
        
        words = condition.strip().split()
        if len(words) == 1:
            return [(words[0], '=', True)]
        
        if "true" in lower_condition.split():
            parts = lower_condition.split()
            field_index = parts.index("true") - 1 if "true" in parts else 0
            if field_index >= 0:
                return [(parts[field_index], '=', True)]
        
        if "false" in lower_condition.split():
            parts = lower_condition.split()
            field_index = parts.index("false") - 1 if "false" in parts else 0
            if field_index >= 0:
                return [(parts[field_index], '=', False)]
        
        return [(condition, '=', True)]

    def _extract_quoted_value(self, value_str: str):
        """Extract a value from quotes."""
        if (value_str.startswith("'") and value_str.endswith("'")) or (value_str.startswith('"') and value_str.endswith('"')):
            return value_str[1:-1]
        return value_str

    def _parse_value(self, value_str: str):
        """Parse a value string into the appropriate Python type."""
        if (value_str.startswith("'") and value_str.endswith("'")) or (value_str.startswith('"') and value_str.endswith('"')):
            return value_str[1:-1]
        
        if value_str.isdigit():
            return int(value_str)
        
        try:
            return float(value_str)
        except ValueError:
            pass
        
        if value_str.upper() == 'TRUE':
            return True
        if value_str.upper() == 'FALSE':
            return False
        
        return value_str

    def format_number(self, result_value):
        if isinstance(result_value, (int, float)):
            result_value = "{:,}".format(result_value)
            return result_value

    # Function to extract main table name from SQL query
    def extract_main_table(self, sql_query):
        # Simple regex to extract the table name after FROM
        from_match = re.search(r'\bfrom\s+([a-zA-Z0-9_\.]+)', sql_query, re.IGNORECASE)
        if from_match:
            return from_match.group(1).strip()
        return None

    @http.route('/dashboard/stats', auth='public', type='json')
    def getAllstats(self, cco, branches_id, datepicked, **kw):
        # Get current user ID
        user_id = request.env.user.id
        
        # Use the security service to check if user is CCO or CO
        is_cco = self.security_service.is_cco_user()
        is_co = self.security_service.is_co_user()
        
        # If user is CO or CCO, treat them like CCO for stats access
        if is_co or is_cco:
            cco = True
            if is_co:
                _logger.info(f"CO user {user_id} accessing stats with CCO privileges")
        
        # Get unique identifier
        unique_id = get_unique_client_identifier()
        
        # if isinstance(branches_id, list):
        #     branches_str = json.dumps(branches_id)
        # else:
        #     branches_str = "[]"# Generate consistent cache key components
        cco_str, branches_str, datepicked_str, unique_id = normalize_cache_key_components(
            cco, branches_id, datepicked, unique_id
        )
        
        # Generate cache key
        cache_key = f"all_stats_{cco_str}_{branches_str}_{datepicked_str}_{unique_id}"
        
        
        # Generate cache key that includes CCO/CO status
        # cache_key = f"all_stats_{str(cco).lower()}_{branches_str}_{datepicked}_{unique_id}"
        
        _logger.info(f"This is the stats cache key: {cache_key}")
        
        # Check if we have valid cache for this user
        cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)
        if cache_data:
            return cache_data
        
        # Define excluded tables
        excluded_tables = ["res_branch", "res_risk_universe"]
        
        # Make sure branches_id is a proper list for non-CCO/CO users
        if not cco:
            branches_array = self.check_branches_id(branches_id)
            # If non-CCO/CO user with no branches, they should see nothing
            if not branches_array:
                return {"data": [], "total": 0}
        
        # First get all compliance stats
        query = """
            SELECT rcs.*
            FROM res_compliance_stat rcs
            WHERE rcs.state = 'active'
            ORDER BY rcs.id
        """
        request.env.cr.execute(query)
        
        # Get column names and results
        columns = [desc[0] for desc in request.env.cr.description]
        stat_records = [dict(zip(columns, row)) for row in request.env.cr.fetchall()]
        
        computed_results = []
        
        # Process each statistic in its own transaction for resilience
        for stat in stat_records:
            # Create a new cursor for each statistic to avoid transaction aborts affecting others
            with request.env.registry.cursor() as cr:
                try:
                    stat_id = stat['id']
                    view_name = f"stat_view_{stat_id}"
                    result_value = None
                    
                    # Try to get value from materialized view first
                    use_view = stat.get('use_materialized_view', False)
                    
                    if use_view:
                        # Check if view exists
                        cr.execute("""
                            SELECT EXISTS (
                                SELECT FROM pg_catalog.pg_class c
                                WHERE c.relname = %s AND c.relkind = 'm'
                            )
                        """, (view_name,))
                        
                        view_exists = cr.fetchone()[0]
                        
                        if view_exists:
                            # First, get ALL columns from the view dynamically
                            cr.execute(f"SELECT * FROM {view_name} LIMIT 0")
                            view_columns = [desc[0] for desc in cr.description]
                            
                            # Start with basic query
                            filter_query = f"SELECT * FROM {view_name}"
                            
                            # For non-CCO/CO users, apply branch filtering dynamically
                            if not cco and branches_id:
                                # Extract the main table from original query
                                original_query = stat['sql_query'].lower()
                                main_table = self.extract_main_table(original_query)
                                
                                # Skip excluded tables
                                if main_table in excluded_tables:
                                    continue
                                
                                # Find branch column
                                branch_column = self._find_branch_column_dynamically(cr, view_columns, main_table)
                                
                                # Apply branch filtering if we found a suitable column
                                if branch_column:
                                    branches_array = list(map(int, branches_id))
                                    if branches_array:
                                        if len(branches_array) == 1:
                                            filter_query += f" WHERE {branch_column} = {branches_array[0]}"
                                        else:
                                            filter_query += f" WHERE {branch_column} IN {tuple(branches_array)}"
                                    else:
                                        # No branches for non-CCO/CO user
                                        continue
                            
                            # Query the view with the proper filters
                            try:
                                cr.execute(f"{filter_query} LIMIT 1")
                                result_row = cr.fetchone()
                                if result_row:
                                    result_value = result_row[0] if result_row else 0
                            except Exception as view_error:
                                _logger.warning(f"Error querying view for stat {stat_id}: {view_error}")
                    
                    # If materialized view approach didn't work, fall back to direct SQL
                    if result_value is None:
                        original_query = stat['sql_query']
                        query = original_query.lower()
                        
                        # Extract the main table from the query
                        main_table = self.extract_main_table(query)
                        
                        # Skip if the main table is in excluded_tables
                        if main_table in excluded_tables:
                            continue
                        
                        # Dynamically check for branch columns
                        needs_modification = False
                        has_branch_id = False
                        branch_column_name = None
                        has_res_partner = re.search(r"\bres_partner\b", query, re.IGNORECASE) is not None
                        
                        if main_table:
                            branch_column_name = self._check_table_for_branch_column(cr, main_table)
                            has_branch_id = bool(branch_column_name)
                        
                        if has_res_partner or has_branch_id:
                            needs_modification = True
                            # Remove trailing semicolon if present
                            if query.endswith(";"):
                                query = query[:-1]
                                original_query = original_query[:-1]
                            has_where = bool(re.search(r'\bwhere\b', query))
                            
                            # Prepare conditions to add
                            conditions = []
                            
                            # Add branch filter if branches are specified AND table has branch column
                            # Only apply for non-CCO/CO users
                            if not cco and has_branch_id and branch_column_name:
                                branches_array = list(map(int, branches_id)) if branches_id else []
                                if branches_array:
                                    if len(branches_array) == 1:
                                        conditions.append(f"{branch_column_name} = {branches_array[0]}")
                                    else:
                                        conditions.append(f"{branch_column_name} IN {tuple(branches_array)}")
                                else:
                                    # If no branches and table has branch_id, add a condition that returns no results
                                    conditions.append("1=0")
                            
                            # Add origin filter for partner tables
                            if has_res_partner:
                                conditions.append("origin IN ('demo','test','prod')")
                            
                            # Build the final condition string
                            if conditions:
                                if has_where:
                                    condition_str = " AND " + " AND ".join(conditions)
                                else:
                                    condition_str = " WHERE " + " AND ".join(conditions)
                                
                                # Find the position to insert the condition (before any clause)
                                clauses = ['group by', 'order by', 'limit', 'offset', 'having']
                                clause_pos = -1
                                for clause in clauses:
                                    pos = query.find(' ' + clause + ' ')
                                    if pos > -1:
                                        if clause_pos == -1 or pos < clause_pos:
                                            clause_pos = pos
                                
                                # Insert the condition at the appropriate position
                                if clause_pos > -1:
                                    original_query = original_query[:clause_pos] + condition_str + original_query[clause_pos:]
                                else:
                                    original_query += condition_str
                        
                        # Execute the query
                        try:
                            cr.execute(original_query)
                            result_row = cr.fetchone()
                            result_value = result_row[0] if result_row is not None else 0
                        except Exception as e:
                            _logger.error(f"Error executing SQL query for stat {stat['name']}: {str(e)}")
                            computed_results.append({
                                "name": stat["name"],
                                "scope": stat["scope"],
                                "val": "Error",
                                "id": stat["id"],
                                "scope_color": stat["scope_color"],
                                "query": stat["sql_query"]
                            })
                            continue
                    
                    # Add the result
                    computed_results.append({
                        "name": stat["name"],
                        "scope": stat["scope"],
                        "val": self.format_number(result_value) if result_value is not None else 0.0,
                        "id": stat["id"],
                        "scope_color": stat["scope_color"],
                        "query": stat["sql_query"]
                    })
                        
                except Exception as e:
                    _logger.error(f"Error processing stat {stat.get('name', 'Unknown')}: {str(e)}")
                    computed_results.append({
                        "name": stat.get("name", "Unknown"),
                        "scope": stat.get("scope", "Unknown"),
                        "val": "Error",
                        "id": stat.get("id", 0),
                        "scope_color": stat.get("scope_color", ""),
                        "query": stat.get("sql_query", "")
                    })
        
        result = {
            "data": computed_results,
            "total": len(computed_results)
        }
        
        # Store in cache before returning
        request.env['res.dashboard.cache'].set_cache(cache_key, result, user_id)
        
        return result
    
    def _find_branch_column_dynamically(self, cr, columns, table_name=None):
        """
        Find branch column from a list of columns using intelligent detection
        """
        # First, check if branch_id exists (most common case)
        if 'branch_id' in columns:
            return 'branch_id'
        
        # If not, check the database for foreign key relationships
        if table_name:
            cr.execute("""
                SELECT column_name 
                FROM information_schema.columns
                WHERE table_name = %s 
                AND column_name IN %s
                AND (column_name LIKE '%%branch%%' OR column_name LIKE '%%_id')
                ORDER BY 
                    CASE 
                        WHEN column_name = 'branch_id' THEN 1
                        WHEN column_name LIKE '%%branch%%' THEN 2
                        ELSE 3
                    END
                LIMIT 1
            """, (table_name, tuple(columns)))
            
            result = cr.fetchone()
            if result:
                return result[0]
        
        return None

    def _check_table_for_branch_column(self, cr, table_name):
        """
        Check if a table has a branch-related column
        """
        if '.' in table_name:
            schema, table = table_name.split('.')
            query = """
                SELECT column_name 
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s 
                AND (column_name = 'branch_id' OR column_name LIKE '%%branch%%')
                ORDER BY CASE WHEN column_name = 'branch_id' THEN 1 ELSE 2 END
                LIMIT 1
            """
            cr.execute(query, (schema, table))
        else:
            query = """
                SELECT column_name 
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s 
                AND (column_name = 'branch_id' OR column_name LIKE '%%branch%%')
                ORDER BY CASE WHEN column_name = 'branch_id' THEN 1 ELSE 2 END
                LIMIT 1
            """
            cr.execute(query, (table_name,))
        
        result = cr.fetchone()
        return result[0] if result else None

    @http.route('/dashboard/statsbycategory', auth='public', type='json')
    def getAllstatsByCategory(self, cco, branches_id, category, datepicked, **kw):
        today = datetime.now().date()  # Get today's date
        prevDate = today - timedelta(days=datepicked)  # Get previous date

        # Convert to datetime for start and end of the day
        start_of_prev_day = fields.Datetime.to_string(datetime.combine(prevDate, datetime.min.time()))
        end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))

        # Use the security service to check if user is CO or CCO
        is_cco = self.security_service.is_cco_user()
        is_co = self.security_service.is_co_user()
        
        # If user is CO or CCO, set cco to true
        if is_co or is_cco:
            cco = True
            if is_co:
                _logger.info(f"CO user {request.env.user.id} accessing stats by category with CCO privileges")

        # Convert branches_id to array before any conditional logic
        branches_array = list(map(int, branches_id)) if branches_id else []
        
        if cco:  # This now includes both CCO and CO users
            # For CCO/CO users, filter stats by category
            results = request.env["res.compliance.stat"].search([
                ('create_date', '>=', start_of_prev_day), 
                ('create_date', '<', end_of_today),
                ('scope', '=', category)  # Add category filter here for CCO/CO
            ])

            computed_results = []

            for result in results:
                original_query = result['sql_query']
                query = original_query.lower()
                needs_modification = False
                
                # Check if we need to modify this query
                if any(table in query for table in ["res_partner", "res.partner", "tier", "transaction"]):
                    needs_modification = True
                    
                    # Remove trailing semicolon if present
                    if query.endswith(";"):
                        query = query[:-1]
                        original_query = original_query[:-1]
                    
                    has_where = bool(re.search(r'\bwhere\b', query))
                    
                    # Prepare conditions to add
                    conditions = []
                    
                    # Add origin filter for partner tables
                    if "res_partner" in query or "res.partner" in query:
                        conditions.append("origin IN ('demo','test','prod')")
                    
                    # Build the final condition string
                    if conditions:
                        if has_where:
                            condition_str = " AND " + " AND ".join(conditions)
                        else:
                            condition_str = " WHERE " + " AND ".join(conditions)
                    
                        # Find the position to insert the condition (before any clause)
                        clauses = ['group by', 'order by', 'limit', 'offset', 'having']
                        clause_pos = -1
                        for clause in clauses:
                            pos = query.find(' ' + clause + ' ')
                            if pos > -1:
                                if clause_pos == -1 or pos < clause_pos:
                                    clause_pos = pos
                        
                        # Insert the condition at the appropriate position
                        if clause_pos > -1:
                            original_query = original_query[:clause_pos] + condition_str + original_query[clause_pos:]
                        else:
                            original_query += condition_str
                
                    # Execute the modified query
                    request.env.cr.execute(original_query)
                else:
                    # For queries that don't need modification, just execute them directly
                    request.env.cr.execute(original_query)
                
                # Get the result value
                result_value = request.env.cr.fetchone()[0] if request.env.cr.rowcount > 0 else 0
                
                # Always add results to the collection regardless of whether the query was modified
                computed_results.append({
                    "name": result["name"],
                    "scope": result["scope"],
                    "val": self.format_number(result_value),
                    "id": result["id"],
                    "scope_color": result["scope_color"],
                    "query": result["sql_query"]
                })
                    
            return {
                "data": computed_results,
                "total": len(results)
            }
        else:
            # First get all compliance stats in the date range
            query = """
                SELECT rcs.*
                FROM res_compliance_stat rcs
                WHERE rcs.create_date >= %s
                AND rcs.create_date < %s AND rcs.scope = %s;
            """

            request.env.cr.execute(query, (start_of_prev_day, end_of_today, category))

            # Get column names and results
            columns = [desc[0] for desc in request.env.cr.description]
            stat_records = [dict(zip(columns, row)) for row in request.env.cr.fetchall()]

            computed_results = []
            for stat in stat_records:
                original_query = stat['sql_query']
                query = original_query.lower()  # Use lowercase for checks but keep original for execution
                needs_modification = False
                
                # Check if we need to modify this query
                if any(table in query for table in ["res_partner", "res.partner", "transaction"]):
                    needs_modification = True
                    
                    # Remove trailing semicolon if present
                    if query.endswith(";"):
                        query = query[:-1]
                        original_query = original_query[:-1]
                    
                    has_where = bool(re.search(r'\bwhere\b', query))
                    
                    # Prepare conditions to add
                    conditions = []
                    
                    # Add branch filter if branches are specified
                    if branches_array:
                        conditions.append(f"branch_id IN {tuple(branches_array)}")
                    else:
                        # If no branches, add a condition that returns no results
                        conditions.append("1=0")
                    
                    # Add origin filter for partner tables
                    if "res_partner" in query or "res.partner" in query:
                        conditions.append("origin IN ('demo','test','prod')")
                    
                    # Build the final condition string
                    if conditions:
                        if has_where:
                            condition_str = " AND " + " AND ".join(conditions)
                        else:
                            condition_str = " WHERE " + " AND ".join(conditions)
                    
                        # Find the position to insert the condition (before any clause)
                        clauses = ['group by', 'order by', 'limit', 'offset', 'having']
                        clause_pos = -1
                        for clause in clauses:
                            pos = query.find(' ' + clause + ' ')
                            if pos > -1:
                                if clause_pos == -1 or pos < clause_pos:
                                    clause_pos = pos
                        
                        # Insert the condition at the appropriate position
                        if clause_pos > -1:
                            original_query = original_query[:clause_pos] + condition_str + original_query[clause_pos:]
                        else:
                            original_query += condition_str
                        
                        request.env.cr.execute(original_query)
                        result_value = request.env.cr.fetchone()[0] if request.env.cr.rowcount > 0 else 0
                
                        # Add the results to our collection
                        computed_results.append({
                            "name": stat["name"],
                            "scope": stat["scope"],
                            "val": self.format_number(result_value),
                            "id": stat["id"],
                            "scope_color": stat["scope_color"],
                            "query": stat["sql_query"]
                        })

            return {
                "data": computed_results,
                "total": len(computed_results)
            }
            

# # # -*- coding: utf-8 -*-

# from odoo import http

# from odoo.http import request

# from datetime import datetime, timedelta

# from odoo import fields

# import re

# from ..utils.cache_key_unique_identifier import get_unique_client_identifier

# import logging



# _logger = logging.getLogger(__name__)





# class Compliance(http.Controller):

#     @http.route('/dashboard/user', auth='public', type='json')

#     def index(self, **kw):



#         user = request.env.user



#         is_superuser = user.has_group('base.group_system')



#         group = any(group.name.lower() == 'chief compliance officer' for group in user.groups_id)

#         branch = [branch.id for branch in user.branches_id]

        

#         unique_id = get_unique_client_identifier() 



#         result = {

#             "group": group,

#             "branch": branch,

#             "unique_id": unique_id,

#         }

#         return result

    

#     def check_branches_id(self, branches_id):

#         # Ensure branches_id is a list

#         if not isinstance(branches_id, list):

#             branches_id = [branches_id]  # Convert to list if it's a single integer

#             return branches_id

#         else:

#             return branches_id



    

#     @http.route('/dashboard/dynamic_sql', auth='public', type='json')

#     def extract_table_and_domain(self, sql_query: str, branches_id, cco):

#         """

#         Extract table names and WHERE conditions from SQL queries using regex patterns.

#         Ignores COUNT aggregation function while still blocking other aggregation functions.

#         """

#         lower_query = sql_query.lower()

#         table = None

#         domain = []



#         # Check for aggregation functions (sum, avg, min, max) in the SELECT clause

#         # Specifically exclude COUNT from the check

#         if re.search(r"\b(?:sum|avg|min|max)\s*\(", lower_query):

#             return None



#         # Extract table name (improved to handle more cases)

#         from_match = re.search(r"\bfrom\s+([\w.]+)", lower_query)

#         if from_match:

#             table = from_match.group(1)

#         else:

#             join_match = re.search(r"\b(?:inner|left|right|full outer)?\s+join\s+([\w.]+)", lower_query)

#             if join_match:

#                 # We're not handling complex joins in this version

#                 return None

        

#         # Extract WHERE clause conditions and convert to Odoo domain format

#         where_match = re.search(r"\bwhere\s+(.+?)(?:\s+(?:group\s+by|order\s+by|limit|having)\s+|\s*$)", lower_query, re.DOTALL)

#         if where_match:

#             condition_string = where_match.group(1).strip()

#             domain = self.parse_condition_string(condition_string) 

        

#         # Collect additional filters

#         additional_filters = []







#         if table == "res_partner":

#             additional_filters.append(("origin", "in", ["demo", "test", "prod"]))



#         check_query = """SELECT 1 FROM information_schema.columns 

#                         WHERE table_name = %s AND column_name = 'branch_id'

#                      """

#         request.env.cr.execute(check_query, (table,))

#         has_branch_id = request.env.cr.fetchone() is not None



#         if not cco and has_branch_id:

#             branch_ids = self.check_branches_id(branches_id)



#             additional_filters.append(("branch_id", "in", branch_ids))

            



#         # Combine domain with additional filters

#         if additional_filters:

#             if domain:

                

#                 # Check if domain is complex (contains '|')

#                 is_complex = any(op == '|' for op in domain if isinstance(op, str))

#                 if is_complex:

#                     # Wrap complex domain to preserve precedence

#                     domain = ['&'] + domain + [additional_filters[0]]

#                     # Add remaining additional filters with '&' operators

#                     for filter_item in additional_filters[1:]:

#                         domain = ['&'] + domain + [filter_item]

#                 else:

#                     # # For simple domain, add '&' operators

#                     # domain = ['&'] * (len(additional_filters) - 1 + 1) + [domain] + additional_filters

#                     # Simple domain: append filters directly without '&'

#                     # domain = domain + additional_filters

#                     # Simple domain: append filters with '&' operators if needed

#                     for filter_item in additional_filters:

#                         domain = ['&'] + domain + [filter_item]

#             else:

#                 # No parsed domain, use only additional filters

#                 domain = additional_filters

#         else:

#             # No additional filters, keep parsed domain

#             pass



#         _logger.info(f"Final domain: {domain}")

       



#         return {'table': table, 'domain': domain}

    

#     def parse_condition_string(self, condition_string):

#         """

#         Parse SQL WHERE conditions into Odoo domain format

#         with proper handling of AND, OR operators and parentheses

#         """

#         if not condition_string:

#             return []

            

#         # Clean up the condition string

#         condition_string = condition_string.strip()

        

#         # Track nesting of parentheses and operators

#         def parse_expression(expr, depth=0):

#             _logger.info(f"Parsing expression (depth {depth}): {expr}")

            

#             # Handle empty expression

#             if not expr.strip():

#                 return []

                

#             # Split by OR at the current nesting level

#             or_parts = self._split_by_operator(expr, "OR")

            

#             if len(or_parts) > 1:

#                 # This is an OR expression

#                 result = []

#                 for i in range(len(or_parts) - 1):

#                     result.append('|')

#                 for part in or_parts:

#                     result.extend(parse_expression(part, depth + 1))

#                 return result

                

#             # Split by AND at the current nesting level

#             and_parts = self._split_by_operator(expr, "AND")

            

#             if len(and_parts) > 1:

#                 # This is an AND expression

#                 result = []

#                 for part in and_parts[:-1]:

#                     result.append('&')

#                     result.extend(parse_expression(part, depth + 1))

#                 result.extend(parse_expression(and_parts[-1], depth + 1))

#                 return result

                

#             # Handle parenthesized expression

#             if expr.strip().startswith('(') and expr.strip().endswith(')'):

#                 inner_expr = expr.strip()[1:-1].strip()

#                 return parse_expression(inner_expr, depth + 1)

                

#             # Base case: single condition

#             return [self._parse_single_condition(expr)]

            

#         # Parse the entire condition string

#         try:

#             domain = parse_expression(condition_string)

#             _logger.info(f"Parsed domain: {domain}")

#             return domain

#         except Exception as e:

#             _logger.error(f"Error parsing condition: {e}")

#             return []

            

#     def _split_by_operator(self, expr, operator):

#         """

#         Split a SQL expression by an operator (AND/OR) while respecting parentheses

#         """

#         operator = f" {operator} "  # Add spaces to match only the operator keyword

#         parts = []

#         current_part = ""

#         paren_level = 0

#         quote_char = None

        

#         i = 0

#         while i < len(expr):

#             char = expr[i]

            

#             # Handle quotes

#             if char in ["'", '"'] and (i == 0 or expr[i-1] != '\\'):

#                 if quote_char is None:

#                     quote_char = char

#                 elif quote_char == char:

#                     quote_char = None

                    

#             # Skip everything inside quotes

#             if quote_char is not None:

#                 current_part += char

#                 i += 1

#                 continue

                

#             # Handle parentheses

#             if char == '(':

#                 paren_level += 1

#             elif char == ')':

#                 paren_level -= 1

                

#             # Check for operator at the current position

#             if (paren_level == 0 and 

#                 i + len(operator) <= len(expr) and 

#                 expr[i:i+len(operator)].upper() == operator):

#                 # Found the operator at current level

#                 parts.append(current_part.strip())

#                 current_part = ""

#                 i += len(operator)

#             else:

#                 current_part += char

#                 i += 1

                

#         # Add the last part

#         if current_part:

#             parts.append(current_part.strip())

            

#         return parts

        

#     def _parse_single_condition(self, condition):

#         """

#         Parse a single SQL condition into an Odoo domain tuple

#         """

#         condition = condition.strip()

        

#         # Handle IS TRUE/FALSE conditions

#         is_true_match = re.search(r"(\w+)\s+is\s+true", condition.lower())

#         if is_true_match:

#             field = is_true_match.group(1).strip()

#             return (field, "=", True)

        

#         is_false_match = re.search(r"(\w+)\s+is\s+false", condition.lower())

#         if is_false_match:

#             field = is_false_match.group(1).strip()

#             return (field, "=", False)

        

#         # Handle IS NULL/NOT NULL (from our previous fix)

#         if " is null" in condition.lower():

#             field = condition.lower().split(" is null")[0].strip()

#             return (field, "=", False)

            

#         if " is not null" in condition.lower():

#             field = condition.lower().split(" is not null")[0].strip()

#             return (field, "!=", False)

            

#         # Handle LIKE operator

#         if " like " in condition.lower():

#             parts = condition.lower().split(" like ")

#             field = parts[0].strip()

#             value = parts[1].strip().strip("'\"")

#             # Convert SQL LIKE wildcards to Odoo's ilike

#             return (field, "ilike", value.replace('%', ''))

            

#         # Handle standard operators

#         ops_map = {

#             "=": "=",

#             ">": ">",

#             ">=": ">=",

#             "<": "<",

#             "<=": "<=",

#             "!=": "!=",

#             "<>": "!="

#         }

        

#         for op in ops_map.keys():

#             if f" {op} " in condition:

#                 parts = condition.split(f" {op} ", 1)

#                 field = parts[0].strip()

#                 value = parts[1].strip().strip("'\"")

                

#                 # Handle boolean values

#                 if value.lower() == 'true':

#                     value = True

#                 elif value.lower() == 'false':

#                     value = False

#                 # Handle numeric values

#                 elif value.isdigit():

#                     value = int(value)

#                 elif value.replace('.', '', 1).isdigit():

#                     value = float(value)

                    

#                 return (field, ops_map[op], value)

                

#         # If we couldn't parse it, return as is

#         _logger.warning(f"Could not parse condition: {condition}")

#         return (condition, "=", True)



#     # def parse_condition_string(self, condition_string: str):

#     #     """

#     #     Parse a condition string into Odoo domain format.

#     #     Handles AND/OR operators and respects parentheses and quoted strings.

#     #     """

#     #     # Split by AND operators

#     #     and_conditions = self._split_by_operator(condition_string, ' AND ')

        

#     #     if len(and_conditions) == 1:

#     #         return self._parse_single_condition(and_conditions[0])

        

#     #     domain = []

#     #     for i, cond in enumerate(and_conditions):

#     #         parsed_condition = self._parse_single_condition(cond)

#     #         if i < len(and_conditions) - 1:

#     #             domain.append('&')

#     #         domain.extend(parsed_condition)

            

#     #     return domain



#     # def _split_by_operator(self, condition_string: str, operator: str):

#     #     """

#     #     Split a condition string by a specified operator,

#     #     respecting parentheses and quoted strings.

#     #     """

#     #     conditions = []

#     #     current_condition = ""

#     #     paren_level = 0

#     #     in_quote = False

#     #     quote_char = None

        

#     #     i = 0

#     #     while i < len(condition_string):

#     #         c = condition_string[i]

            

#     #         if c in ("'", '"') and (quote_char is None or c == quote_char):

#     #             in_quote = not in_quote

#     #             quote_char = c if in_quote else None

#     #             current_condition += c

#     #         elif c == '(' and not in_quote:

#     #             paren_level += 1

#     #             current_condition += c

#     #         elif c == ')' and not in_quote:

#     #             paren_level -= 1

#     #             current_condition += c

#     #         elif (paren_level == 0 and not in_quote and 

#     #               i + len(operator) <= len(condition_string) and

#     #               condition_string[i:i+len(operator)].upper() == operator):

#     #             conditions.append(current_condition.strip())

#     #             current_condition = ""

#     #             i += len(operator) - 1

#     #         else:

#     #             current_condition += c

            

#     #         i += 1

        

#     #     if current_condition:

#     #         conditions.append(current_condition.strip())

        

#     #     return conditions



#     # def _parse_single_condition(self, condition: str):

#     #     """

#     #     Parse a single condition which might contain OR operators.

#     #     Returns a list in Odoo domain format.

#     #     """

#     #     or_conditions = self._split_by_operator(condition, ' OR ')

        

#     #     if len(or_conditions) == 1:

#     #         condition = or_conditions[0].strip()

#     #         if condition.startswith('(') and condition.endswith(')'):

#     #             inner_condition = condition[1:-1].strip()

#     #             inner_or_conditions = self._split_by_operator(inner_condition, ' OR ')

#     #             if len(inner_or_conditions) > 1:

#     #                 or_conditions = inner_or_conditions

#     #             else:

#     #                 return self._convert_to_odoo_tuple(inner_condition)

#     #         else:

#     #             return self._convert_to_odoo_tuple(condition)

        

#     #     domain = []

#     #     for i in range(len(or_conditions) - 1):

#     #         domain.append('|')

        

#     #     for cond in or_conditions:

#     #         parsed = self._convert_to_odoo_tuple(cond.strip())

#     #         domain.extend(parsed)

            

#     #     return domain



#     def _convert_to_odoo_tuple(self, condition: str):

#         """

#         Convert a simple condition string to an Odoo domain tuple.

#         Handles IS NULL, LIKE, IN, =, >, <, etc.

#         """

#         condition = condition.strip()

        

#         if condition.startswith('(') and condition.endswith(')'):

#             condition = condition[1:-1].strip()

        

#         lower_condition = condition.lower()

        

#         if ' is true' in lower_condition:

#             field = lower_condition.split(' is true')[0].strip()

#             return [(field, '=', True)]

            

#         if ' is false' in lower_condition:

#             field = lower_condition.split(' is false')[0].strip()

#             return [(field, '=', False)]

        

#         if ' = true' in lower_condition:

#             field = lower_condition.split(' = true')[0].strip()

#             return [(field, '=', True)]

            

#         if ' = false' in lower_condition:

#             field = lower_condition.split(' = false')[0].strip()

#             return [(field, '=', False)]

        

#         if ' is null' in lower_condition:

#             field = lower_condition.split(' is null')[0].strip()

#             return [(field, '=', False)]

        

#         if ' is not null' in lower_condition:

#             field = lower_condition.split(' is not null')[0].strip()

#             return [(field, '!=', False)]

        

#         if ' like ' in lower_condition:

#             parts = condition.split(' like ', 1) if ' like ' in lower_condition else condition.split(' LIKE ', 1)

#             field = parts[0].strip()

#             value = self._extract_quoted_value(parts[1].strip())

#             return [(field, '=like', value)]

        

#         if ' in ' in lower_condition:

#             parts = condition.split(' in ', 1) if ' in ' in lower_condition else condition.split(' IN ', 1)

#             field = parts[0].strip()

#             values_str = parts[1].strip()

            

#             if values_str.startswith('(') and values_str.endswith(')'):

#                 values_str = values_str[1:-1].strip()

            

#             values = []

#             for val in values_str.split(','):

#                 val = val.strip()

#                 if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):

#                     values.append(val[1:-1])

#                 elif val.isdigit():

#                     values.append(int(val))

#                 elif val.replace('.', '', 1).isdigit():

#                     values.append(float(val))

#                 else:

#                     values.append(val)

            

#             return [(field, 'in', values)]

        

#         for op in ['!=', '>=', '<=', '=', '>', '<']:

#             if f' {op} ' in condition:

#                 parts = condition.split(f' {op} ', 1)

#                 field = parts[0].strip()

#                 value = self._parse_value(parts[1].strip())

#                 return [(field, op, value)]

        

#         words = condition.strip().split()

#         if len(words) == 1:

#             return [(words[0], '=', True)]

        

#         if "true" in lower_condition.split():

#             parts = lower_condition.split()

#             field_index = parts.index("true") - 1 if "true" in parts else 0

#             if field_index >= 0:

#                 return [(parts[field_index], '=', True)]

        

#         if "false" in lower_condition.split():

#             parts = lower_condition.split()

#             field_index = parts.index("false") - 1 if "false" in parts else 0

#             if field_index >= 0:

#                 return [(parts[field_index], '=', False)]

        

#         return [(condition, '=', True)]



#     def _extract_quoted_value(self, value_str: str):

#         """Extract a value from quotes."""

#         if (value_str.startswith("'") and value_str.endswith("'")) or (value_str.startswith('"') and value_str.endswith('"')):

#             return value_str[1:-1]

#         return value_str



#     def _parse_value(self, value_str: str):

#         """Parse a value string into the appropriate Python type."""

#         if (value_str.startswith("'") and value_str.endswith("'")) or (value_str.startswith('"') and value_str.endswith('"')):

#             return value_str[1:-1]

        

#         if value_str.isdigit():

#             return int(value_str)

        

#         try:

#             return float(value_str)

#         except ValueError:

#             pass

        

#         if value_str.upper() == 'TRUE':

#             return True

#         if value_str.upper() == 'FALSE':

#             return False

        

#         return value_str



#     def check_branches_id(self, branches_id):

#         """

#         Placeholder method to validate and return branch IDs.

#         Replace with actual implementation.

#         """

#         # Example implementation (replace with actual logic)

#         return branches_id if isinstance(branches_id, list) else [branches_id]

        

#     def format_number(self, result_value):

#         if isinstance(result_value, (int, float)):

#             result_value = "{:,}".format(result_value)

#             return result_value



#     # Function to extract main table name from SQL query

#     def extract_main_table(self, sql_query):

#         # Simple regex to extract the table name after FROM

#         from_match = re.search(r'\bfrom\s+([a-zA-Z0-9_\.]+)', sql_query, re.IGNORECASE)

#         if from_match:

#             return from_match.group(1).strip()

#         return None



#     # @http.route('/dashboard/stats', auth='public', type='json')

#     # def getAllstats(self, cco, branches_id, datepicked, **kw):

#     #     # Get current user ID

#     #     user_id = request.env.user.id



#     #     # Get unique identifier

#     #     unique_id = get_unique_client_identifier()

        

#     #     # Generate cache key

#     #     cache_key = f"all_stats_{cco}_{branches_id}_{datepicked}_{unique_id}"



#     #     _logger.info(f"This is the cache key: {cache_key}")

        

#     #     # Check if we have valid cache for this user

#     #     cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)

#     #     if cache_data:

#     #         return cache_data



#     #     # Define excluded tables

#     #     excluded_tables = ["res_branch", "res_risk_universe"]



#     #     # Make sure branches_id is a proper list for non-CCO users

#     #     if not cco:

#     #         branches_array = self.check_branches_id(branches_id)

#     #         # If non-CCO user with no branches, they should see nothing

#     #         if not branches_array:

#     #             return {"data": [], "total": 0}

        

#     #     # First get all compliance stats

#     #     query = """

#     #         SELECT rcs.*

#     #         FROM res_compliance_stat rcs

#     #         WHERE rcs.state = 'active'

#     #         ORDER BY rcs.id

#     #     """

#     #     request.env.cr.execute(query)



#     #     # Get column names and results

#     #     columns = [desc[0] for desc in request.env.cr.description]

#     #     stat_records = [dict(zip(columns, row)) for row in request.env.cr.fetchall()]



#     #     computed_results = []

        

#     #     # Process each statistic

#     #     for stat in stat_records:

#     #         try:

#     #             # Define view name variable at the beginning of the loop

#     #             view_name = f"stat_view_{stat['id']}"

                

#     #             # Try to get value from materialized view first if enabled

#     #             result_value = None

#     #             use_view = stat.get('use_materialized_view', False)

                

#     #             if use_view:

#     #                 # For non-CCO users, we need to check for branch filtering

#     #                 # Materialized views won't have branch filtering built-in

#     #                 filter_query = f"SELECT * FROM {view_name}"

                    

#     #                 if not cco and branches_id:

#     #                     # Check if the original query references a table with branch_id

#     #                     original_query = stat['sql_query'].lower()

#     #                     main_table = self.extract_main_table(original_query)

                        

#     #                     # Skip excluded tables

#     #                     if main_table in excluded_tables:

#     #                         continue

                        

#     #                     # Check if the table has branch_id

#     #                     if main_table:

#     #                         has_branch_id = False

                            

#     #                         # Check if the table has branch_id column

#     #                         check_query = """

#     #                             SELECT 1 FROM information_schema.columns 

#     #                             WHERE table_name = %s AND column_name = 'branch_id'

#     #                         """

#     #                         request.env.cr.execute(check_query, (main_table,))

#     #                         has_branch_id = bool(request.env.cr.fetchone())

                            

#     #                         # If table has branch_id but user has no branches, return nothing

#     #                         if has_branch_id and not branches_id:

#     #                             continue

                            

#     #                         # If table has branch_id and user has branches, add the filter

#     #                         if has_branch_id and branches_id:

#     #                             branches_array = list(map(int, branches_id))

#     #                             if len(branches_array) == 1:

#     #                                 filter_query += f" WHERE branch_id = {branches_array[0]}"

#     #                             else:

#     #                                 filter_query += f" WHERE branch_id IN {tuple(branches_array)}"

                    

#     #                 # Now check if the view exists and query it

#     #                 request.env.cr.execute("""

#     #                     SELECT EXISTS (

#     #                         SELECT FROM pg_catalog.pg_class c

#     #                         WHERE c.relname = %s AND c.relkind = 'm'

#     #                     )

#     #                 """, (view_name,))

                    

#     #                 view_exists = request.env.cr.fetchone()[0]

                    

#     #                 if view_exists:

#     #                     try:

#     #                         # Query the view

#     #                         request.env.cr.execute(f"{filter_query} LIMIT 1")

#     #                         result_row = request.env.cr.fetchone()

#     #                         if result_row:

#     #                             result_value = result_row[0] if result_row else 0

#     #                     except Exception as view_error:

#     #                         _logger.warning(f"Error querying view for stat {stat['id']}: {view_error}")

                

#     #             # If materialized view approach didn't work, fall back to direct SQL

#     #             if result_value is None:

#     #                 original_query = stat['sql_query']

#     #                 query = original_query.lower()

#     #                 needs_modification = False

                    

#     #                 # Extract the main table from the query

#     #                 main_table = self.extract_main_table(query)

                    

#     #                 # Skip if the main table is in excluded_tables

#     #                 if main_table in excluded_tables:

#     #                     continue

                    

#     #                 needs_modification = False

#     #                 has_branch_id = False

                    

#     #                 # Check if the main table has a branch_id column

#     #                 if main_table:

#     #                     # Handle possible schema.table format

#     #                     if '.' in main_table:

#     #                         schema, table = main_table.split('.')

#     #                         check_query = """

#     #                             SELECT 1 FROM information_schema.columns

#     #                             WHERE table_schema = %s AND table_name = %s AND column_name = 'branch_id'

#     #                         """

#     #                         request.env.cr.execute(check_query, (schema, table))

#     #                         has_branch_id = bool(request.env.cr.fetchone())

#     #                     else:

#     #                         # Check in public schema

#     #                         check_query = """

#     #                             SELECT 1 FROM information_schema.columns

#     #                             WHERE table_schema = 'public' AND table_name = %s AND column_name = 'branch_id'

#     #                         """

#     #                         request.env.cr.execute(check_query, (main_table,))

#     #                         has_branch_id = bool(request.env.cr.fetchone())

                    

#     #                 has_res_partner = re.search(r"\bres_partner\b", query, re.IGNORECASE) is not None

#     #                 if has_res_partner or has_branch_id:

#     #                     needs_modification = True

#     #                     # Remove trailing semicolon if present

#     #                     if query.endswith(";"):

#     #                         query = query[:-1]

#     #                         original_query = original_query[:-1]

#     #                     has_where = bool(re.search(r'\bwhere\b', query))

#     #                     # Prepare conditions to add

#     #                     conditions = []

                        

#     #                     # Add branch filter if branches are specified AND table has branch_id column

#     #                     if not cco and has_branch_id:

#     #                         # Convert branches_id to a proper array parameter

#     #                         branches_array = list(map(int, branches_id)) if branches_id else []

#     #                         if branches_array:

#     #                             if len(branches_array) == 1:

#     #                                 conditions.append(f"branch_id = {branches_array[0]}")

#     #                             else:

#     #                                 conditions.append(f"branch_id IN {tuple(branches_array)}")

#     #                         else:

#     #                             # If no branches and table has branch_id, add a condition that returns no results

#     #                             conditions.append("1=0")

                                

#     #                     # Add origin filter for partner tables

#     #                     if has_res_partner:

#     #                         conditions.append("origin IN ('demo','test','prod')")

                            

#     #                     # Build the final condition string

#     #                     if conditions:

#     #                         if has_where:

#     #                             condition_str = " AND " + " AND ".join(conditions)

#     #                         else:

#     #                             condition_str = " WHERE " + " AND ".join(conditions)

                                

#     #                         # Find the position to insert the condition (before any clause)

#     #                         clauses = ['group by', 'order by', 'limit', 'offset', 'having']

#     #                         clause_pos = -1

#     #                         for clause in clauses:

#     #                             pos = query.find(' ' + clause + ' ')

#     #                             if pos > -1:

#     #                                 if clause_pos == -1 or pos < clause_pos:

#     #                                     clause_pos = pos

                                        

#     #                         # Insert the condition at the appropriate position

#     #                         if clause_pos > -1:

#     #                             original_query = original_query[:clause_pos] + condition_str + original_query[clause_pos:]

#     #                         else:

#     #                             original_query += condition_str

                    

#     #                 # Execute the query

#     #                 try:

#     #                     request.env.cr.execute(original_query)

#     #                     result_row = request.env.cr.fetchone()

#     #                     result_value = result_row[0] if result_row is not None else 0

#     #                 except Exception as e:

#     #                     _logger.error(f"Error executing SQL query for stat {stat['name']}: {str(e)}")

#     #                     computed_results.append({

#     #                         "name": stat["name"],

#     #                         "scope": stat["scope"],

#     #                         "val": "Error",

#     #                         "id": stat["id"],

#     #                         "scope_color": stat["scope_color"],

#     #                         "query": stat["sql_query"]

#     #                     })

#     #                     continue

                

#     #             # Add the result

#     #             computed_results.append({

#     #                 "name": stat["name"],

#     #                 "scope": stat["scope"],

#     #                 "val": self.format_number(result_value) if result_value is not None else 0.0,

#     #                 "id": stat["id"],

#     #                 "scope_color": stat["scope_color"],

#     #                 "query": stat["sql_query"]

#     #             })

                    

#     #         except Exception as e:

#     #             _logger.error(f"Error processing stat {stat.get('name', 'Unknown')}: {str(e)}")

#     #             computed_results.append({

#     #                 "name": stat.get("name", "Unknown"),

#     #                 "scope": stat.get("scope", "Unknown"),

#     #                 "val": "Error",

#     #                 "id": stat.get("id", 0),

#     #                 "scope_color": stat.get("scope_color", ""),

#     #                 "query": stat.get("sql_query", "")

#     #             })

        

#     #     result = {

#     #         "data": computed_results,

#     #         "total": len(computed_results)

#     #     }

        

#     #     # Store in cache before returning

#     #     request.env['res.dashboard.cache'].set_cache(cache_key, result, user_id)

        

#     #     return result

    

#     @http.route('/dashboard/stats', auth='public', type='json')
#     def getAllstats(self, cco, branches_id, datepicked, **kw):
#         # Get current user ID
#         user_id = request.env.user.id

#         # Get unique identifier
#         unique_id = get_unique_client_identifier()
        
#         # Generate cache key
#         cache_key = f"all_stats_{cco}_{branches_id}_{datepicked}_{unique_id}"

#         _logger.info(f"This is the cache key: {cache_key}")
        
#         # Check if we have valid cache for this user
#         cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)
#         if cache_data:
#             return cache_data

#         # Define excluded tables
#         excluded_tables = ["res_branch", "res_risk_universe"]

#         # Make sure branches_id is a proper list for non-CCO users
#         if not cco:
#             branches_array = self.check_branches_id(branches_id)
#             # If non-CCO user with no branches, they should see nothing
#             if not branches_array:
#                 return {"data": [], "total": 0}
        
#         # First get all compliance stats
#         query = """
#             SELECT rcs.*
#             FROM res_compliance_stat rcs
#             WHERE rcs.state = 'active'
#             ORDER BY rcs.id
#         """
#         request.env.cr.execute(query)

#         # Get column names and results
#         columns = [desc[0] for desc in request.env.cr.description]
#         stat_records = [dict(zip(columns, row)) for row in request.env.cr.fetchall()]

#         computed_results = []
        
#         # Process each statistic in its own transaction for resilience
#         for stat in stat_records:
#             # Create a new cursor for each statistic to avoid transaction aborts affecting others
#             with request.env.registry.cursor() as cr:
#                 try:
#                     stat_id = stat['id']
#                     view_name = f"stat_view_{stat_id}"
#                     result_value = None
                    
#                     # Try to get value from materialized view first
#                     use_view = stat.get('use_materialized_view', False)
                    
#                     if use_view:
#                         # Check if view exists
#                         cr.execute("""
#                             SELECT EXISTS (
#                                 SELECT FROM pg_catalog.pg_class c
#                                 WHERE c.relname = %s AND c.relkind = 'm'
#                             )
#                         """, (view_name,))
                        
#                         view_exists = cr.fetchone()[0]
                        
#                         if view_exists:
#                             # **CRITICAL FIX: Dynamic column detection**
#                             # First, get ALL columns from the view dynamically
#                             cr.execute(f"SELECT * FROM {view_name} LIMIT 0")
#                             view_columns = [desc[0] for desc in cr.description]
                            
#                             # Start with basic query
#                             filter_query = f"SELECT * FROM {view_name}"
                            
#                             # For non-CCO users, apply branch filtering dynamically
#                             if not cco and branches_id:
#                                 # Extract the main table from original query
#                                 original_query = stat['sql_query'].lower()
#                                 main_table = self.extract_main_table(original_query)
                                
#                                 # Skip excluded tables
#                                 if main_table in excluded_tables:
#                                     continue
                                
#                                 # **DYNAMIC BRANCH COLUMN DETECTION**
#                                 branch_column = self._find_branch_column_dynamically(cr, view_columns, main_table)
                                
#                                 # Apply branch filtering if we found a suitable column
#                                 if branch_column:
#                                     branches_array = list(map(int, branches_id))
#                                     if branches_array:
#                                         if len(branches_array) == 1:
#                                             filter_query += f" WHERE {branch_column} = {branches_array[0]}"
#                                         else:
#                                             filter_query += f" WHERE {branch_column} IN {tuple(branches_array)}"
#                                     else:
#                                         # No branches for non-CCO user
#                                         continue
                            
#                             # Query the view with the proper filters
#                             try:
#                                 cr.execute(f"{filter_query} LIMIT 1")
#                                 result_row = cr.fetchone()
#                                 if result_row:
#                                     result_value = result_row[0] if result_row else 0
#                             except Exception as view_error:
#                                 _logger.warning(f"Error querying view for stat {stat_id}: {view_error}")
                    
#                     # If materialized view approach didn't work, fall back to direct SQL
#                     if result_value is None:
#                         original_query = stat['sql_query']
#                         query = original_query.lower()
                        
#                         # Extract the main table from the query
#                         main_table = self.extract_main_table(query)
                        
#                         # Skip if the main table is in excluded_tables
#                         if main_table in excluded_tables:
#                             continue
                        
#                         # **DYNAMIC BRANCH COLUMN DETECTION FOR DIRECT QUERY**
#                         needs_modification = False
#                         has_branch_id = False
#                         branch_column_name = None
#                         has_res_partner = re.search(r"\bres_partner\b", query, re.IGNORECASE) is not None
                        
#                         # Dynamically check for branch columns
#                         if main_table:
#                             branch_column_name = self._check_table_for_branch_column(cr, main_table)
#                             has_branch_id = bool(branch_column_name)
                        
#                         if has_res_partner or has_branch_id:
#                             needs_modification = True
#                             # Remove trailing semicolon if present
#                             if query.endswith(";"):
#                                 query = query[:-1]
#                                 original_query = original_query[:-1]
#                             has_where = bool(re.search(r'\bwhere\b', query))
                            
#                             # Prepare conditions to add
#                             conditions = []
                            
#                             # Add branch filter if branches are specified AND table has branch column
#                             if not cco and has_branch_id and branch_column_name:
#                                 branches_array = list(map(int, branches_id)) if branches_id else []
#                                 if branches_array:
#                                     if len(branches_array) == 1:
#                                         conditions.append(f"{branch_column_name} = {branches_array[0]}")
#                                     else:
#                                         conditions.append(f"{branch_column_name} IN {tuple(branches_array)}")
#                                 else:
#                                     # If no branches and table has branch_id, add a condition that returns no results
#                                     conditions.append("1=0")
                            
#                             # Add origin filter for partner tables
#                             if has_res_partner:
#                                 conditions.append("origin IN ('demo','test','prod')")
                            
#                             # Build the final condition string
#                             if conditions:
#                                 if has_where:
#                                     condition_str = " AND " + " AND ".join(conditions)
#                                 else:
#                                     condition_str = " WHERE " + " AND ".join(conditions)
                                
#                                 # Find the position to insert the condition (before any clause)
#                                 clauses = ['group by', 'order by', 'limit', 'offset', 'having']
#                                 clause_pos = -1
#                                 for clause in clauses:
#                                     pos = query.find(' ' + clause + ' ')
#                                     if pos > -1:
#                                         if clause_pos == -1 or pos < clause_pos:
#                                             clause_pos = pos
                                
#                                 # Insert the condition at the appropriate position
#                                 if clause_pos > -1:
#                                     original_query = original_query[:clause_pos] + condition_str + original_query[clause_pos:]
#                                 else:
#                                     original_query += condition_str
                        
#                         # Execute the query
#                         try:
#                             cr.execute(original_query)
#                             result_row = cr.fetchone()
#                             result_value = result_row[0] if result_row is not None else 0
#                         except Exception as e:
#                             _logger.error(f"Error executing SQL query for stat {stat['name']}: {str(e)}")
#                             computed_results.append({
#                                 "name": stat["name"],
#                                 "scope": stat["scope"],
#                                 "val": "Error",
#                                 "id": stat["id"],
#                                 "scope_color": stat["scope_color"],
#                                 "query": stat["sql_query"]
#                             })
#                             continue
                    
#                     # Add the result
#                     computed_results.append({
#                         "name": stat["name"],
#                         "scope": stat["scope"],
#                         "val": self.format_number(result_value) if result_value is not None else 0.0,
#                         "id": stat["id"],
#                         "scope_color": stat["scope_color"],
#                         "query": stat["sql_query"]
#                     })
                        
#                 except Exception as e:
#                     _logger.error(f"Error processing stat {stat.get('name', 'Unknown')}: {str(e)}")
#                     computed_results.append({
#                         "name": stat.get("name", "Unknown"),
#                         "scope": stat.get("scope", "Unknown"),
#                         "val": "Error",
#                         "id": stat.get("id", 0),
#                         "scope_color": stat.get("scope_color", ""),
#                         "query": stat.get("sql_query", "")
#                     })
        
#         result = {
#             "data": computed_results,
#             "total": len(computed_results)
#         }
        
#         # Store in cache before returning
#         request.env['res.dashboard.cache'].set_cache(cache_key, result, user_id)
        
#         return result
    
#     def _find_branch_column_dynamically(self, cr, columns, table_name=None):
#         """
#         Find branch column from a list of columns using the configured field or intelligent detection
#         """
#         # First, check if branch_id exists (most common case)
#         if 'branch_id' in columns:
#             return 'branch_id'
        
#         # If not, check the database for foreign key relationships
#         if table_name:
#             cr.execute("""
#                 SELECT column_name 
#                 FROM information_schema.columns
#                 WHERE table_name = %s 
#                 AND column_name IN %s
#                 AND (column_name LIKE '%%branch%%' OR column_name LIKE '%%_id')
#                 ORDER BY 
#                     CASE 
#                         WHEN column_name = 'branch_id' THEN 1
#                         WHEN column_name LIKE '%%branch%%' THEN 2
#                         ELSE 3
#                     END
#                 LIMIT 1
#             """, (table_name, tuple(columns)))
            
#             result = cr.fetchone()
#             if result:
#                 return result[0]
        
#         return None

#     def _check_table_for_branch_column(self, cr, table_name):
#         """
#         Check if a table has a branch-related column
#         """
#         if '.' in table_name:
#             schema, table = table_name.split('.')
#             query = """
#                 SELECT column_name 
#                 FROM information_schema.columns
#                 WHERE table_schema = %s AND table_name = %s 
#                 AND (column_name = 'branch_id' OR column_name LIKE '%%branch%%')
#                 ORDER BY CASE WHEN column_name = 'branch_id' THEN 1 ELSE 2 END
#                 LIMIT 1
#             """
#             cr.execute(query, (schema, table))
#         else:
#             query = """
#                 SELECT column_name 
#                 FROM information_schema.columns
#                 WHERE table_schema = 'public' AND table_name = %s 
#                 AND (column_name = 'branch_id' OR column_name LIKE '%%branch%%')
#                 ORDER BY CASE WHEN column_name = 'branch_id' THEN 1 ELSE 2 END
#                 LIMIT 1
#             """
#             cr.execute(query, (table_name,))
        
#         result = cr.fetchone()
#         return result[0] if result else None



#     # @http.route('/dashboard/stats', auth='public', type='json')

#     # def getAllstats(self, cco, branches_id, datepicked, **kw):

        

#     #     # Get current user ID

#     #     user_id = request.env.user.id



#     #     # Get unique identifier

#     #     unique_id = get_unique_client_identifier()

        

#     #     # Generate cache key

#     #     cache_key = f"all_stats_{cco}_{branches_id}_{datepicked}_{unique_id}"



#     #     _logger.info(f"This is the cache key: {cache_key}")



#     #     # # Generate user ip for unique cache key

#     #     # user_ip = request.httprequest.remote_addr

#     #     # # Add timestamp for additional uniqueness

#     #     # timestamp = datetime.now().strftime('%Y%m%d%H%M%S')



#     #     # # Generate cache key - include user ID and IP to make it user-specific

#     #     # cache_key = f"all_stats_{cco}_{branches_id}_{datepicked}_{user_ip}_{timestamp}"     

        

#     #     # Check if we have valid cache for this user

#     #     cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)

#     #     if cache_data:

#     #         return cache_data



        



#     #     pattern = r"\bres_partner\b"



#     #     if cco == True:

#     #         # fetch all data for chief compliance officer

#     #         results = request.env["res.compliance.stat"].search([])



#     #         computed_results = []



#     #         for result in results:

#     #             original_query = result['sql_query']

#     #             query = original_query.lower()  # Use lowercase for checks but keep original for execution

#     #             needs_modification = False

                

#     #             tables_to_check = ["res.partner", "tier", "transaction"]

#     #             has_res_partner = re.search(r"\bres_partner\b", query, re.IGNORECASE) is not None

#     #             if has_res_partner or any(table in query for table in tables_to_check):

#     #                 needs_modification = True

#     #                 # Remove trailing semicolon if present

#     #                 if query.endswith(";"):

#     #                     query = query[:-1]

#     #                     original_query = original_query[:-1]

                        

#     #                 has_where = bool(re.search(r'\bwhere\b', query))

#     #                 # Prepare conditions to add

#     #                 conditions = []

#     #                 # Add origin filter for partner tables

#     #                 if re.search(pattern, query, re.IGNORECASE):

#     #                     conditions.append("origin IN ('demo','test','prod')")

#     #                 # Build the final condition string

#     #                 if conditions:

#     #                     if has_where:

#     #                         condition_str = " AND " + " AND ".join(conditions)

#     #                     else:

#     #                         condition_str = " WHERE " + " AND ".join(conditions)

#     #                     # Find the position to insert the condition (before any clause)

#     #                     clauses = ['group by', 'order by', 'limit', 'offset', 'having']

#     #                     clause_pos = -1

#     #                     for clause in clauses:

#     #                         pos = query.find(' ' + clause + ' ')

#     #                         if pos > -1:

#     #                             if clause_pos == -1 or pos < clause_pos:

#     #                                 clause_pos = pos

#     #                     # Insert the condition at the appropriate position

#     #                     if clause_pos > -1:

#     #                         original_query = original_query[:clause_pos] + condition_str + original_query[clause_pos:]

#     #                     else:

#     #                         original_query += condition_str

                

                   

#     #             try:

#     #                 request.env.cr.execute(original_query)

#     #                 # For count queries, we expect a single row with a single value

#     #                 result_value = request.env.cr.fetchone()

#     #                 result_value = result_value[0] if result_value is not None else 0

#     #                 computed_results.append({

#     #                     "name": result["name"],

#     #                     "scope": result["scope"],

#     #                     "val": self.format_number(result_value) if result_value is not None else 0.0,

#     #                     "id": result["id"],

#     #                     "scope_color": result["scope_color"],

#     #                     "query": result['sql_query']

#     #                 })

#     #             except Exception as e:

#     #                 _logger.error(f"Error executing SQL query for stat {result['name']}: {str(e)}")

#     #                 computed_results.append({

#     #                     "name": result["name"],

#     #                     "scope": result["scope"],

#     #                     "val": "Error",

#     #                     "id": result["id"],

#     #                 "scope_color": result["scope_color"],

#     #                 "query": result['sql_query']

#     #                 })

                        

                

#     #         result = {

#     #             "data": computed_results,

#     #             "total": len(results)

#     #         }

            

#     #         # Store in cache before returning - use user_id instead of primary_group_id

#     #         request.env['res.dashboard.cache'].set_cache(cache_key, result, user_id)

            

#     #         return result

#     #     else:



           

#     #         # Define excluded tables

#     #         excluded_tables = ["res_branch", "res_risk_universe"]



#     #         # First get all compliance stats

#     #         query = """

#     #             SELECT rcs.*

#     #             FROM res_compliance_stat rcs

#     #             ORDER BY rcs.id

#     #         """

#     #         request.env.cr.execute(query)



#     #         # Get column names and results

#     #         columns = [desc[0] for desc in request.env.cr.description]

#     #         stat_records = [dict(zip(columns, row)) for row in request.env.cr.fetchall()]



#     #         # Convert branches_id to a proper PostgreSQL array parameter

#     #         branches_array = list(map(int, branches_id)) if branches_id else []



            



#     #         # Process each compliance stat and execute its SQL query with branch filtering

#     #         computed_results = []

#     #         for stat in stat_records:

#     #             original_query = stat['sql_query']

#     #             query = original_query.lower() 

 



#     #             # Extract the main table from the query

#     #             main_table = self.extract_main_table(query)





#     #             # Skip if the main table is in excluded_tables

#     #             if main_table in excluded_tables :

#     #                 continue  # Skip processing this stat



#     #             needs_modification = False

#     #             has_branch_id = False



                 



#     #             # Check if the main table has a branch_id column

#     #             if main_table:

#     #                 # Handle possible schema.table format

#     #                 if '.' in main_table:

#     #                     schema, table = main_table.split('.')

#     #                     check_query = """

#     #                         SELECT 1 FROM information_schema.columns

#     #                         WHERE table_schema = %s AND table_name = %s AND column_name = 'branch_id'

#     #                     """

#     #                     request.env.cr.execute(check_query, (schema, table))

#     #                     has_branch_id = bool(request.env.cr.fetchone())

#     #                 else:

#     #                     # Check in public schema

#     #                     check_query = """

#     #                         SELECT 1 FROM information_schema.columns

#     #                         WHERE table_schema = 'public' AND table_name = %s AND column_name = 'branch_id'

#     #                     """

#     #                     request.env.cr.execute(check_query, (main_table,))

#     #                     has_branch_id = bool(request.env.cr.fetchone())

                

#     #             has_res_partner = re.search(r"\bres_partner\b", query, re.IGNORECASE) is not None

#     #             if has_res_partner or has_branch_id:

#     #                 needs_modification = True

#     #                 # Remove trailing semicolon if present

#     #                 if query.endswith(";"):

#     #                     query = query[:-1]

#     #                     original_query = original_query[:-1]

#     #                 has_where = bool(re.search(r'\bwhere\b', query))

#     #                 # Prepare conditions to add

#     #                 conditions = []

#     #                 # Add branch filter if branches are specified AND table has branch_id column

#     #                 if branches_array and has_branch_id:

#     #                     conditions.append(f"branch_id IN {tuple(branches_array)}" if len(branches_array) > 1 else f"branch_id = {branches_array[0]}")

#     #                 elif branches_array and not has_branch_id:

#     #                     # Skip branch filtering for tables without branch_id column

#     #                     pass

#     #                 elif not branches_array and has_branch_id:

#     #                     # If no branches and table has branch_id, add a condition that returns no results

#     #                     conditions.append("1=0")

#     #                 # Add origin filter for partner tables

#     #                 if has_res_partner:

#     #                     conditions.append("origin IN ('demo','test','prod')")

#     #                 # Build the final condition string

#     #                 if conditions:

#     #                     if has_where:

#     #                         condition_str = " AND " + " AND ".join(conditions)

#     #                     else:

#     #                         condition_str = " WHERE " + " AND ".join(conditions)

#     #                     # Find the position to insert the condition (before any clause)

#     #                     clauses = ['group by', 'order by', 'limit', 'offset', 'having']

#     #                     clause_pos = -1

#     #                     for clause in clauses:

#     #                         pos = query.find(' ' + clause + ' ')

#     #                         if pos > -1:

#     #                             if clause_pos == -1 or pos < clause_pos: 

#     #                                 clause_pos = pos

#     #                     # Insert the condition at the appropriate position

#     #                     if clause_pos > -1:

#     #                         original_query = original_query[:clause_pos] + condition_str + original_query[clause_pos:]

#     #                     else:

#     #                         original_query += condition_str



#     #             # Execute the query with or without parameters based on conditions

                

#     #             try:

#     #                 request.env.cr.execute(original_query)

#     #                 result_row = request.env.cr.fetchone()

#     #                 result_value = result_row[0] if result_row is not None else 0

        

#     #                 computed_results.append({

#     #                     "name": stat["name"],

#     #                     "scope": stat["scope"],

#     #                     "val": self.format_number(result_value) if result_value is not None else 0.0,

#     #                     "id": stat["id"],

#     #                     "scope_color": stat["scope_color"],

#     #                     "query": stat["sql_query"]

#     #                 })

#     #             except Exception as e:

#     #                 _logger.error(f"Error executing SQL query for stat {stat['name']}: {str(e)}")

#     #                 computed_results.append({

#     #                     "name": stat["name"],

#     #                     "scope": stat["scope"],

#     #                     "val": "Error",

#     #                     "id": stat["id"],

#     #                     "scope_color": stat["scope_color"],

#     #                     "query": stat["sql_query"]

#     #                 })

            

    

#     #         result = {

#     #             "data": computed_results,

#     #             "total": len(computed_results)

#     #         }



#     #         # Store in cache before returning

#     #         request.env['res.dashboard.cache'].set_cache(cache_key, result, user_id)

            

#     #         return result









#     @http.route('/dashboard/statsbycategory', auth='public', type='json')

#     def getAllstatsByCategory(self, cco, branches_id, category, datepicked, **kw):

#         today = datetime.now().date()  # Get today's date

#         prevDate = today - timedelta(days=datepicked)  # Get previous date



#         # Convert to datetime for start and end of the day

#         start_of_prev_day = fields.Datetime.to_string(datetime.combine(prevDate, datetime.min.time()))

#         end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))



#         # Convert branches_id to array before any conditional logic

#         branches_array = list(map(int, branches_id)) if branches_id else []

        

#         if cco == True:

#             # For CCO users, filter stats by category

#             results = request.env["res.compliance.stat"].search([

#                 ('create_date', '>=', start_of_prev_day), 

#                 ('create_date', '<', end_of_today),

#                 ('scope', '=', category)  # Add category filter here for CCO

#             ])



#             computed_results = []



#             for result in results:

#                 original_query = result['sql_query']

#                 query = original_query.lower()

#                 needs_modification = False

                

#                 # Check if we need to modify this query

#                 if any(table in query for table in ["res_partner", "res.partner", "tier", "transaction"]):

#                     needs_modification = True

                    

#                     # Remove trailing semicolon if present

#                     if query.endswith(";"):

#                         query = query[:-1]

#                         original_query = original_query[:-1]

                    

#                     has_where = bool(re.search(r'\bwhere\b', query))

                    

#                     # Prepare conditions to add

#                     conditions = []

                    

#                     # Add origin filter for partner tables

#                     if "res_partner" in query or "res.partner" in query:

#                         conditions.append("origin IN ('demo','test','prod')")

                    

#                     # Build the final condition string

#                     if conditions:

#                         if has_where:

#                             condition_str = " AND " + " AND ".join(conditions)

#                         else:

#                             condition_str = " WHERE " + " AND ".join(conditions)

                    

#                         # Find the position to insert the condition (before any clause)

#                         clauses = ['group by', 'order by', 'limit', 'offset', 'having']

#                         clause_pos = -1

#                         for clause in clauses:

#                             pos = query.find(' ' + clause + ' ')

#                             if pos > -1:

#                                 if clause_pos == -1 or pos < clause_pos:

#                                     clause_pos = pos

                        

#                         # Insert the condition at the appropriate position

#                         if clause_pos > -1:

#                             original_query = original_query[:clause_pos] + condition_str + original_query[clause_pos:]

#                         else:

#                             original_query += condition_str

                

#                     # Execute the modified query

#                     request.env.cr.execute(original_query)

#                 else:

#                     # For queries that don't need modification, just execute them directly

#                     request.env.cr.execute(original_query)

                

#                 # Get the result value

#                 result_value = request.env.cr.fetchone()[0] if request.env.cr.rowcount > 0 else 0

                

#                 # Always add results to the collection regardless of whether the query was modified

#                 computed_results.append({

#                     "name": result["name"],

#                     "scope": result["scope"],

#                     "val": self.format_number(result_value),

#                     "id": result["id"],

#                     "scope_color": result["scope_color"],

#                     "query": result["sql_query"]

#                 })

                    

#             return {

#                 "data": computed_results,

#                 "total": len(results)

#             }

#         else:

#             # First get all compliance stats in the date range

#             query = """

#                 SELECT rcs.*

#                 FROM res_compliance_stat rcs

#                 WHERE rcs.create_date >= %s

#                 AND rcs.create_date < %s AND rcs.scope = %s;

#             """



#             request.env.cr.execute(query, (start_of_prev_day, end_of_today, category))



#             # Get column names and results

#             columns = [desc[0] for desc in request.env.cr.description]

#             stat_records = [dict(zip(columns, row)) for row in request.env.cr.fetchall()]



#             computed_results = []

#             for stat in stat_records:

#                 original_query = stat['sql_query']

#                 query = original_query.lower()  # Use lowercase for checks but keep original for execution

#                 needs_modification = False

                

#                 # Check if we need to modify this query

#                 if any(table in query for table in ["res_partner", "res.partner", "transaction"]):

#                     needs_modification = True

                    

#                     # Remove trailing semicolon if present

#                     if query.endswith(";"):

#                         query = query[:-1]

#                         original_query = original_query[:-1]

                    

#                     has_where = bool(re.search(r'\bwhere\b', query))

                    

#                     # Prepare conditions to add

#                     conditions = []

                    

#                     # Add branch filter if branches are specified

#                     if branches_array:

#                         conditions.append(f"branch_id IN {tuple(branches_array)}")

#                     else:

#                         # If no branches, add a condition that returns no results

#                         conditions.append("1=0")

                    

#                     # Add origin filter for partner tables

#                     if "res_partner" in query or "res.partner" in query:

#                         conditions.append("origin IN ('demo','test','prod')")

                    

#                     # Build the final condition string

#                     if conditions:

#                         if has_where:

#                             condition_str = " AND " + " AND ".join(conditions)

#                         else:

#                             condition_str = " WHERE " + " AND ".join(conditions)

                    

#                         # Find the position to insert the condition (before any clause)

#                         clauses = ['group by', 'order by', 'limit', 'offset', 'having']

#                         clause_pos = -1

#                         for clause in clauses:

#                             pos = query.find(' ' + clause + ' ')

#                             if pos > -1:

#                                 if clause_pos == -1 or pos < clause_pos:

#                                     clause_pos = pos

                        

#                         # Insert the condition at the appropriate position

#                         if clause_pos > -1:

#                             original_query = original_query[:clause_pos] + condition_str + original_query[clause_pos:]

#                         else:

#                             original_query += condition_str

                        

#                         request.env.cr.execute(original_query, (branches_array,))

#                         result_value = request.env.cr.fetchone()[0] if request.env.cr.rowcount > 0 else 0

                

#                         # Add the results to our collection

#                         computed_results.append({

#                             "name": stat["name"],

#                             "scope": stat["scope"],

#                             "val": self.format_number(result_value),

#                             "id": stat["id"],

#                             "scope_color": stat["scope_color"],

#                             "query": stat["sql_query"]

#                         })



#             return {

#                 "data": computed_results,

#                 "total": len(computed_results)

#             }

            

            

            

        

#     # @http.route('/dashboard/statsbycategory', auth='public', type='json')

#     # def getAllstatsByCategory(self, cco, branches_id, category, datepicked, **kw):



    

#     #     today = datetime.now().date()  # Get today's date

#     #     prevDate = today - timedelta(days=datepicked)  # Get previous date



#     #     # Convert to datetime for start and end of the day

#     #     start_of_prev_day = fields.Datetime.to_string(datetime.combine(prevDate, datetime.min.time()))



#     #     end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))



#     #     if cco == True:

#     #         results = request.env["res.compliance.stat"].search([('create_date', '>=', start_of_prev_day), ('create_date', '<', end_of_today)])



#     #         computed_results = []



#     #         for result in results:



#     #             original_query = result['sql_query']

#     #             query = original_query.lower()  # Use lowercase for checks but keep original for execution

#     #             needs_modification = False

                

#     #               # Check if we need to modify this query

#     #             if any(table in query for table in ["res_partner", "res.partner", "tier", "transaction"]):

#     #                 needs_modification = True

                    

#     #                 # Remove trailing semicolon if present

#     #                 if query.endswith(";"):

#     #                     query = query[:-1]

#     #                     original_query = original_query[:-1]

                    

                    

#     #                 has_where = bool(re.search(r'\bwhere\b', query))

                    

#     #                 # Prepare conditions to add

#     #                 conditions = []

                    

#     #                 # Add branch filter if branches are specified

#     #                 if branches_array:

#     #                     conditions.append(f"branch_id = ANY(%s::integer[])")

#     #                 else:

#     #                     # If no branches, add a condition that returns no results

#     #                     conditions.append("1=0")

                    

#     #                 # Add origin filter for partner tables

#     #                 if "res_partner" in query or "res.partner" in query:

#     #                     conditions.append("origin IN ('demo','test','prod')")

                    

#     #                 # Build the final condition string

#     #                 if conditions:

#     #                     if has_where:

#     #                         condition_str = " AND " + " AND ".join(conditions)

#     #                     else:

#     #                         condition_str = " WHERE " + " AND ".join(conditions)

                    

#     #                     # Find the position to insert the condition (before any clause)

#     #                     clauses = ['group by', 'order by', 'limit', 'offset', 'having']

#     #                     clause_pos = -1

#     #                     for clause in clauses:

#     #                         pos = query.find(' ' + clause + ' ')

#     #                         if pos > -1:

#     #                             if clause_pos == -1 or pos < clause_pos:

#     #                                 clause_pos = pos

                        

#     #                     # Insert the condition at the appropriate position

#     #                     if clause_pos > -1:

#     #                         original_query = original_query[:clause_pos] + condition_str + original_query[clause_pos:]

#     #                     else:

#     #                         original_query += condition_str

                

#     #                     # Execute the query with or without parameters

#     #                     request.env.cr.execute(original_query, (branches_array,))

                        

                

#     #                     # For count queries, we expect a single row with a single value

#     #                     result_value = request.env.cr.fetchone()[0] if request.env.cr.rowcount > 0 else 0

                        

#     #                     # Add the results to our collection

#     #                     computed_results.append({

#     #                         "name": stat["name"],

#     #                         "scope": stat["scope"],

#     #                         "val": result_value,

#     #                         "id": stat["id"],

#     #                         "scope_color": stat["scope_color"],

#     #                         "query": stat["sql_query"]

#     #                     })

#     #         return {

#     #             "data": computed_results,

#     #             "total": len(results)

#     #         }

#     #     else:

#     #          # First get all compliance stats in the date range

#     #         query = """

#     #             SELECT rcs.*

#     #             FROM res_compliance_stat rcs

#     #             WHERE rcs.create_date >= %s

#     #             AND rcs.create_date < %s AND rcs.scope = %s;

#     #         """



#     #         request.env.cr.execute(query, (start_of_prev_day, end_of_today, category))



#     #         # Get column names and results

#     #         columns = [desc[0] for desc in request.env.cr.description]

#     #         stat_records = [dict(zip(columns, row)) for row in request.env.cr.fetchall()]



#     #         # Convert branches_id to a proper PostgreSQL array parameter

#     #         branches_array = list(map(int, branches_id))  # Make sure all elements are integers



#     #         computed_results = []

#     #         for stat in stat_records:

#     #             original_query = stat['sql_query']

#     #             query = original_query.lower()  # Use lowercase for checks but keep original for execution

#     #             needs_modification = False

                

#     #             # Check if we need to modify this query

#     #             if any(table in query for table in ["res_partner", "res.partner", "transaction"]):

#     #                 needs_modification = True

                    

#     #                 # Remove trailing semicolon if present

#     #                 if query.endswith(";"):

#     #                     query = query[:-1]

#     #                     original_query = original_query[:-1]

                    

                    

#     #                 has_where = bool(re.search(r'\bwhere\b', query))

                    

#     #                 # Prepare conditions to add

#     #                 conditions = []

                    

#     #                 # Add branch filter if branches are specified

#     #                 if branches_array:

#     #                     conditions.append(f"branch_id = ANY(%s::integer[])")

#     #                 else:

#     #                     # If no branches, add a condition that returns no results

#     #                     conditions.append("1=0")

                    

#     #                 # Add origin filter for partner tables

#     #                 if "res_partner" in query or "res.partner" in query:

#     #                     conditions.append("origin IN ('demo','test','prod')")

                    

#     #                 # Build the final condition string

#     #                 if conditions:

#     #                     if has_where:

#     #                         condition_str = " AND " + " AND ".join(conditions)

#     #                     else:

#     #                         condition_str = " WHERE " + " AND ".join(conditions)

                    

#     #                     # Find the position to insert the condition (before any clause)

#     #                     clauses = ['group by', 'order by', 'limit', 'offset', 'having']

#     #                     clause_pos = -1

#     #                     for clause in clauses:

#     #                         pos = query.find(' ' + clause + ' ')

#     #                         if pos > -1:

#     #                             if clause_pos == -1 or pos < clause_pos:

#     #                                 clause_pos = pos

                        

#     #                     # Insert the condition at the appropriate position

#     #                     if clause_pos > -1:

#     #                         original_query = original_query[:clause_pos] + condition_str + original_query[clause_pos:]

#     #                     else:

#     #                         original_query += condition_str

                        

#     #                     request.env.cr.execute(original_query, (branches_array,))

#     #                     result_value = request.env.cr.fetchone()[0] if request.env.cr.rowcount > 0 else 0

                

#     #                     # Add the results to our collection

#     #                     computed_results.append({

#     #                         "name": stat["name"],

#     #                         "scope": stat["scope"],

#     #                         "val": result_value,

#     #                         "id": stat["id"],

#     #                         "scope_color": stat["scope_color"],

#     #                         "query": stat["sql_query"]

#     #                     })



#     #         return {

#     #                 "data": computed_results,

#     #                 "total": len(computed_results)

#     #         }


