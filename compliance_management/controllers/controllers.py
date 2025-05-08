# # -*- coding: utf-8 -*-
from odoo import http
from odoo.http import request
from datetime import datetime, timedelta
from odoo import fields
import re
import logging
_logger = logging.getLogger(__name__)


class Compliance(http.Controller):
    @http.route('/dashboard/user', auth='public', type='json')
    def index(self, **kw):

        user = request.env.user

        is_superuser = user.has_group('base.group_system')

        group = any(group.name.lower() == 'chief compliance officer' for group in user.groups_id)
        branch = [branch.id for branch in user.branches_id] 

        result = {
            "group": group,
            "branch": branch,
        }
        return result
    
    def check_branches_id(self, branches_id):
        # Ensure branches_id is a list
        if not isinstance(branches_id, list):
            branches_id = [branches_id]  # Convert to list if it's a single integer
            return branches_id
        else:
            return branches_id

    # @http.route('/dashboard/dynamic_sql', auth='public', type='json')
    # def extract_table_and_domain(self, sql_query: str, branches_id, cco):
    #     """
    #     Extract table names and WHERE conditions from SQL queries using regex patterns.
    #     Ignores COUNT aggregation function while still blocking other aggregation functions.
    #     """
    #     lower_query = sql_query.lower()
    #     table = None
    #     domain = []

    #     # Check for aggregation functions (sum, avg, min, max) in the SELECT clause
    #     # Specifically exclude COUNT from the check
    #     if re.search(r"\b(?:sum|avg|min|max)\s*\(", lower_query):
    #         return None

    #     # Extract table name (improved to handle more cases)
    #     from_match = re.search(r"\bfrom\s+([\w.]+)", lower_query)
    #     if from_match:
    #         table = from_match.group(1)
    #     else:
    #         join_match = re.search(r"\b(?:inner|left|right|full outer)?\s+join\s+([\w.]+)", lower_query)
    #         if join_match:
    #             # We're not handling complex joins in this version
    #             return None
        
    #     # Extract WHERE clause conditions and convert to Odoo domain format
    #     where_match = re.search(r"\bwhere\s+(.+?)(?:\s+(?:group\s+by|order\s+by|limit|having)\s+|\s*$)", lower_query, re.DOTALL)
    #     if where_match:
    #         condition_string = where_match.group(1).strip()
    #         domain = self._parse_conditions_to_odoo_domain(condition_string)  
        
    #     if table == "res_partner":
    #         domain.append(["origin", "in", ["demo", "test", "prod"]])
        
    #     # check if it is not cco
    #     if not cco and has_branch_id:
    #         domain.append(["branch_id", "in", self.check_branches_id(branches_id)])

    #     return {'table': table, 'domain': domain}
    
    @http.route('/dashboard/dynamic_sql', auth='public', type='json')
    def extract_table_and_domain(self, sql_query: str, branches_id, cco):
        """
        Extract table names and WHERE conditions from SQL queries using regex patterns.
        Ignores COUNT aggregation function while still blocking other aggregation functions.
        """
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
            domain = self._parse_conditions_to_odoo_domain(condition_string)  
        
        if table == "res_partner":
            domain.append(["origin", "in", ["demo", "test", "prod"]])

        
        check_query = """SELECT 1 FROM information_schema.columns 
                            WHERE table_name = %s AND column_name = 'branch_id'
                        """
        request.env.cr.execute(check_query, (table,))
           
        has_branch_id = request.env.cr.fetchone() is not None

        
        # check if it is not cco
        if not cco and has_branch_id:
            domain.append(["branch_id", "in", self.check_branches_id(branches_id)])

        return {'table': table, 'domain': domain}

    def _parse_conditions_to_odoo_domain(self, condition_string: str):
        """
        Parse SQL WHERE conditions and convert to Odoo domain format.
        Handles multiple AND conditions and IS NULL/IS NOT NULL.
        """
        python_values = {
            "null": None,
            "true": True,
            "false": False
        }
        
        domain = []
        # Split AND conditions, respecting parentheses
        conditions = self._split_and_conditions(condition_string)
        
        for cond in conditions:
            cond = cond.strip()
            
            # Handle IS NULL and IS NOT NULL specially
            null_match = re.match(r"([\w.]+)\s+is\s+(not\s+)?null", cond, re.IGNORECASE)
            if null_match:
                field = null_match.group(1).strip()
                is_not = null_match.group(2) is not None
                domain.append([field, '!=' if is_not else '=', None])
                continue

             # Handle IS TRUE specially
            true_match = re.match(r"([\w.]+)\s+is\s+true", cond, re.IGNORECASE)
            if true_match:
                field = true_match.group(1).strip()
                domain.append([field, '=', True])
                continue
            
            # Handle IS FALSE specially
            false_match = re.match(r"([\w.]+)\s+is\s+false", cond, re.IGNORECASE)
            if false_match:
                field = false_match.group(1).strip()
                domain.append([field, '=', False])
                continue
            
            # Handle standard operators
            operators = ['>=', '<=', '!=', '<>', '=', '>', '<', ' like ', ' ilike ', ' in ', ' not in ']
            operator_found = False
            
            for op in operators:
                if f" {op.strip()} " in f" {cond} ":
                    parts = cond.split(op, 1)
                    if len(parts) == 2:
                        field = parts[0].strip()
                        value = parts[1].strip()
                        
                        # Clean the value
                        if value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        elif value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        
                        # Handle IN and NOT IN lists
                        if op.strip() in ('in', 'not in'):
                            if value.startswith('(') and value.endswith(')'):
                                # Extract values from parentheses and convert to list
                                value = value[1:-1]  # Remove parentheses
                                value_list = []
                                
                                # Split by comma, handling quoted strings properly
                                in_quote = False
                                quote_char = None
                                current_item = ""
                                
                                for char in value:
                                    if char in ("'", '"') and (quote_char is None or char == quote_char):
                                        in_quote = not in_quote
                                        quote_char = char if in_quote else None
                                        continue
                                    
                                    if char == ',' and not in_quote:
                                        value_list.append(current_item.strip().strip("'\""))
                                        current_item = ""
                                    else:
                                        current_item += char
                                
                                if current_item:
                                    value_list.append(current_item.strip().strip("'\""))
                                
                                value = value_list
                        
                        # Convert Python values
                        if isinstance(value, str) and value.lower() in python_values:
                            value = python_values[value.lower()]
                        
                        # Map SQL operators to Odoo operators
                        odoo_op = self._map_operator_to_odoo(op.strip())
                        domain.append([field, odoo_op, value])
                        operator_found = True
                        break
            
            # If no standard operator found, check for more complex conditions
            if not operator_found and ' or ' in cond.lower():
                # Basic OR handling (simplified)
                or_conditions = cond.split(' or ')
                or_domain = []
                
                for or_cond in or_conditions:
                    sub_domain = self._parse_conditions_to_odoo_domain(or_cond)
                    if sub_domain:
                        or_domain.extend(sub_domain)
                
                if or_domain:
                    # Add Odoo's OR operator syntax
                    or_operators = ['|'] * (len(or_domain) - 1)
                    domain.extend(or_operators + or_domain)
        
        return domain

    def _split_and_conditions(self, condition_string: str):
        """
        Split a condition string by AND operators,
        respecting parentheses and quoted strings.
        """
        conditions = []
        current_condition = ""
        
        # State tracking
        paren_level = 0
        in_quote = False
        quote_char = None
        
        i = 0
        while i < len(condition_string):
            c = condition_string[i]
            
            # Handle quotes
            if c in ("'", '"') and (quote_char is None or c == quote_char):
                in_quote = not in_quote
                quote_char = c if in_quote else None
                current_condition += c
            
            # Handle parentheses
            elif c == '(' and not in_quote:
                paren_level += 1
                current_condition += c
            elif c == ')' and not in_quote:
                paren_level -= 1
                current_condition += c
            
            # Check for AND operator, but only when not inside quotes or parentheses
            elif (paren_level == 0 and not in_quote and 
                condition_string[i:i+5].lower() == ' and '):
                conditions.append(current_condition.strip())
                current_condition = ""
                i += 4  # Skip ahead past "and"
            else:
                current_condition += c
            
            i += 1
        
        if current_condition:
            conditions.append(current_condition.strip())
        
        return conditions

    def _map_operator_to_odoo(self, sql_op: str):
        """Map SQL operators to Odoo domain operators."""
        odoo_operators = {
            '=': '=',
            '>': '>',
            '<': '<',
            '>=': '>=',
            '<=': '<=',
            '!=': '!=',
            '<>': '!=',
            'like': 'like',
            'ilike': 'ilike',
            'in': 'in',
            'not in': 'not in'
        }
        
        return odoo_operators.get(sql_op, sql_op)
    
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
        
        # Generate cache key - include user ID to make it user-specific
        cache_key = f"all_stats_{cco}_{branches_id}_{datepicked}"
        
        # Check if we have valid cache for this user
        cache_data = request.env['res.dashboard.cache'].get_cache(cache_key, user_id)
        if cache_data:
            return cache_data

        today = datetime.now().date()  # Get today's date
        prevDate = today - timedelta(days=datepicked)  # Get previous date
        
        # Pattern to match res_partner exactly (not as part of other table names)
        pattern = r"\bres_partner\b"

        # Convert to datetime for start and end of the day
        start_of_prev_day = fields.Datetime.to_string(datetime.combine(prevDate, datetime.min.time()))
        end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))

        if cco == True:
            # fetch all data for chief compliance officer
            results = request.env["res.compliance.stat"].search([('create_date', '>=', start_of_prev_day), ('create_date', '<', end_of_today)])

            computed_results = []

            for result in results:
                original_query = result['sql_query']
                query = original_query.lower()  # Use lowercase for checks but keep original for execution
                needs_modification = False
                
                # Check if we need to modify this query
                # Using a more precise check to differentiate between res_partner and res_partner_watchlist
                tables_to_check = ["res.partner", "tier", "transaction"]
                has_res_partner = re.search(r"\bres_partner\b", query, re.IGNORECASE) is not None
                
                if has_res_partner or any(table in query for table in tables_to_check):
                    needs_modification = True
                    
                    # Remove trailing semicolon if present
                    if query.endswith(";"):
                        query = query[:-1]
                        original_query = original_query[:-1]
                    
                    has_where = bool(re.search(r'\bwhere\b', query))
                    
                    # Prepare conditions to add
                    conditions = []

                    # Add origin filter for partner tables
                    if re.search(pattern, query, re.IGNORECASE):
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
                    request.env.cr.execute(original_query)
                    # For count queries, we expect a single row with a single value
                    result_value = request.env.cr.fetchone()[0] if request.env.cr.rowcount > 0 else 0
                    computed_results.append({
                        "name": result["name"],
                        "scope": result["scope"], 
                        "val": self.format_number(result_value), 
                        "id": result["id"], 
                        "scope_color": result["scope_color"], 
                        "query": result['sql_query']
                    })
                except Exception as e:
                    _logger.error(f"Error executing SQL query for stat {result['name']}: {str(e)}")
                    computed_results.append({
                        "name": result["name"],
                        "scope": result["scope"], 
                        "val": "Error", 
                        "id": result["id"], 
                        "scope_color": result["scope_color"], 
                        "query": result['sql_query']
                    })

            return {
                "data": computed_results,
                "total": len(results)
            }
            
            # Store in cache before returning - use user_id instead of primary_group_id
            request.env['res.dashboard.cache'].set_cache(cache_key, result, user_id)
            
            return result
        else:
            # First get all compliance stats in the date range
            query = """
                SELECT rcs.*
                FROM res_compliance_stat rcs
                WHERE rcs.create_date >= %s
                AND rcs.create_date < %s;
            """

            # Execute the query first (assuming parameters are defined elsewhere)
            request.env.cr.execute(query, (start_of_prev_day, end_of_today))

            # Get column names and results
            columns = [desc[0] for desc in request.env.cr.description]
            stat_records = [dict(zip(columns, row)) for row in request.env.cr.fetchall()]

            # Convert branches_id to a proper PostgreSQL array parameter
            branches_array = list(map(int, branches_id))  # Make sure all elements are integers

            # Process each compliance stat and execute its SQL query with branch filtering
            computed_results = []
            for stat in stat_records:
                original_query = stat['sql_query']
                query = original_query.lower()  # Use lowercase for checks but keep original for execution
                needs_modification = False
                    
                # Extract main table name from query
                main_table = self.extract_main_table(query)
                has_branch_id = False
                
                # Check if the table has branch_id column
                if main_table:
                    # Handle possible schema.table format
                    if '.' in main_table:
                        schema, table = main_table.split('.')
                        check_query = """
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = %s AND table_name = %s AND column_name = 'branch_id'
                        """
                        request.env.cr.execute(check_query, (schema, table))
                        has_branch_id = bool(request.env.cr.fetchone())  # Store result of check
                    else:
                        # Check if the table has branch_id column in public schema
                        check_query = """
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_schema = 'public' AND table_name = %s AND column_name = 'branch_id'
                        """
                        request.env.cr.execute(check_query, (main_table,))
                        has_branch_id = bool(request.env.cr.fetchone())  # Store result of check
                        
                    # Now determine if we need to modify the query
                    tables_to_check = ["res.partner", "transaction"]
                    has_res_partner = re.search(r"\bres_partner\b", query, re.IGNORECASE) is not None
                    
                    if has_res_partner or any(table in query for table in tables_to_check) or has_branch_id:
                        needs_modification = True
                        
                        # Remove trailing semicolon if present
                        if query.endswith(";"):
                            query = query[:-1]
                            original_query = original_query[:-1]
                        
                        has_where = bool(re.search(r'\bwhere\b', query))
                        
                        # Prepare conditions to add
                        conditions = []
                        
                        # Add branch filter if branches are specified AND table has branch_id column
                        if branches_array and has_branch_id:
                            conditions.append(f"branch_id = ANY(%s::integer[])")
                        elif branches_array and not has_branch_id:
                            # Skip branch filtering for tables without branch_id column
                            pass
                        elif not branches_array and has_branch_id:
                            # If no branches and table has branch_id, add a condition that returns no results
                            conditions.append("1=0")
                        
                        # Add origin filter for partner tables - using exact pattern
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
                
                    # Execute the query with or without parameters based on conditions
                    try:
                        if branches_array and has_branch_id and needs_modification:
                            request.env.cr.execute(original_query, (branches_array,))
                        else:
                            request.env.cr.execute(original_query)
                        
                        # For count queries, we expect a single row with a single value
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
                else:
                    # For queries that don't need modification
                    try:
                        request.env.cr.execute(original_query)
                        result_value = request.env.cr.fetchone()[0] if request.env.cr.rowcount > 0 else 0
                        
                        computed_results.append({
                            "name": stat["name"],
                            "scope": stat["scope"],
                            "val": self.format_number(result_value),
                            "id": stat["id"],
                            "scope_color": stat["scope_color"],
                            "query": stat["sql_query"]
                        })
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

            return {
                "data": computed_results,
                "total": len(computed_results)
            }
            
            # Store in cache before returning - use user_id instead of primary_group_id
            request.env['res.dashboard.cache'].set_cache(cache_key, result, user_id)
            
            return result


    @http.route('/dashboard/statsbycategory', auth='public', type='json')
    def getAllstatsByCategory(self, cco, branches_id, category, datepicked, **kw):
        today = datetime.now().date()  # Get today's date
        prevDate = today - timedelta(days=datepicked)  # Get previous date

        # Convert to datetime for start and end of the day
        start_of_prev_day = fields.Datetime.to_string(datetime.combine(prevDate, datetime.min.time()))
        end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))

        # Convert branches_id to array before any conditional logic
        branches_array = list(map(int, branches_id)) if branches_id else []
        
        if cco == True:
            # For CCO users, filter stats by category
            results = request.env["res.compliance.stat"].search([
                ('create_date', '>=', start_of_prev_day), 
                ('create_date', '<', end_of_today),
                ('scope', '=', category)  # Add category filter here for CCO
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
                        conditions.append(f"branch_id = ANY(%s::integer[])")
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
                        
                        request.env.cr.execute(original_query, (branches_array,))
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
            
            
            
        
    # @http.route('/dashboard/statsbycategory', auth='public', type='json')
    # def getAllstatsByCategory(self, cco, branches_id, category, datepicked, **kw):

    
    #     today = datetime.now().date()  # Get today's date
    #     prevDate = today - timedelta(days=datepicked)  # Get previous date

    #     # Convert to datetime for start and end of the day
    #     start_of_prev_day = fields.Datetime.to_string(datetime.combine(prevDate, datetime.min.time()))

    #     end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))

    #     if cco == True:
    #         results = request.env["res.compliance.stat"].search([('create_date', '>=', start_of_prev_day), ('create_date', '<', end_of_today)])

    #         computed_results = []

    #         for result in results:

    #             original_query = result['sql_query']
    #             query = original_query.lower()  # Use lowercase for checks but keep original for execution
    #             needs_modification = False
                
    #               # Check if we need to modify this query
    #             if any(table in query for table in ["res_partner", "res.partner", "tier", "transaction"]):
    #                 needs_modification = True
                    
    #                 # Remove trailing semicolon if present
    #                 if query.endswith(";"):
    #                     query = query[:-1]
    #                     original_query = original_query[:-1]
                    
                    
    #                 has_where = bool(re.search(r'\bwhere\b', query))
                    
    #                 # Prepare conditions to add
    #                 conditions = []
                    
    #                 # Add branch filter if branches are specified
    #                 if branches_array:
    #                     conditions.append(f"branch_id = ANY(%s::integer[])")
    #                 else:
    #                     # If no branches, add a condition that returns no results
    #                     conditions.append("1=0")
                    
    #                 # Add origin filter for partner tables
    #                 if "res_partner" in query or "res.partner" in query:
    #                     conditions.append("origin IN ('demo','test','prod')")
                    
    #                 # Build the final condition string
    #                 if conditions:
    #                     if has_where:
    #                         condition_str = " AND " + " AND ".join(conditions)
    #                     else:
    #                         condition_str = " WHERE " + " AND ".join(conditions)
                    
    #                     # Find the position to insert the condition (before any clause)
    #                     clauses = ['group by', 'order by', 'limit', 'offset', 'having']
    #                     clause_pos = -1
    #                     for clause in clauses:
    #                         pos = query.find(' ' + clause + ' ')
    #                         if pos > -1:
    #                             if clause_pos == -1 or pos < clause_pos:
    #                                 clause_pos = pos
                        
    #                     # Insert the condition at the appropriate position
    #                     if clause_pos > -1:
    #                         original_query = original_query[:clause_pos] + condition_str + original_query[clause_pos:]
    #                     else:
    #                         original_query += condition_str
                
    #                     # Execute the query with or without parameters
    #                     request.env.cr.execute(original_query, (branches_array,))
                        
                
    #                     # For count queries, we expect a single row with a single value
    #                     result_value = request.env.cr.fetchone()[0] if request.env.cr.rowcount > 0 else 0
                        
    #                     # Add the results to our collection
    #                     computed_results.append({
    #                         "name": stat["name"],
    #                         "scope": stat["scope"],
    #                         "val": result_value,
    #                         "id": stat["id"],
    #                         "scope_color": stat["scope_color"],
    #                         "query": stat["sql_query"]
    #                     })
    #         return {
    #             "data": computed_results,
    #             "total": len(results)
    #         }
    #     else:
    #          # First get all compliance stats in the date range
    #         query = """
    #             SELECT rcs.*
    #             FROM res_compliance_stat rcs
    #             WHERE rcs.create_date >= %s
    #             AND rcs.create_date < %s AND rcs.scope = %s;
    #         """

    #         request.env.cr.execute(query, (start_of_prev_day, end_of_today, category))

    #         # Get column names and results
    #         columns = [desc[0] for desc in request.env.cr.description]
    #         stat_records = [dict(zip(columns, row)) for row in request.env.cr.fetchall()]

    #         # Convert branches_id to a proper PostgreSQL array parameter
    #         branches_array = list(map(int, branches_id))  # Make sure all elements are integers

    #         computed_results = []
    #         for stat in stat_records:
    #             original_query = stat['sql_query']
    #             query = original_query.lower()  # Use lowercase for checks but keep original for execution
    #             needs_modification = False
                
    #             # Check if we need to modify this query
    #             if any(table in query for table in ["res_partner", "res.partner", "transaction"]):
    #                 needs_modification = True
                    
    #                 # Remove trailing semicolon if present
    #                 if query.endswith(";"):
    #                     query = query[:-1]
    #                     original_query = original_query[:-1]
                    
                    
    #                 has_where = bool(re.search(r'\bwhere\b', query))
                    
    #                 # Prepare conditions to add
    #                 conditions = []
                    
    #                 # Add branch filter if branches are specified
    #                 if branches_array:
    #                     conditions.append(f"branch_id = ANY(%s::integer[])")
    #                 else:
    #                     # If no branches, add a condition that returns no results
    #                     conditions.append("1=0")
                    
    #                 # Add origin filter for partner tables
    #                 if "res_partner" in query or "res.partner" in query:
    #                     conditions.append("origin IN ('demo','test','prod')")
                    
    #                 # Build the final condition string
    #                 if conditions:
    #                     if has_where:
    #                         condition_str = " AND " + " AND ".join(conditions)
    #                     else:
    #                         condition_str = " WHERE " + " AND ".join(conditions)
                    
    #                     # Find the position to insert the condition (before any clause)
    #                     clauses = ['group by', 'order by', 'limit', 'offset', 'having']
    #                     clause_pos = -1
    #                     for clause in clauses:
    #                         pos = query.find(' ' + clause + ' ')
    #                         if pos > -1:
    #                             if clause_pos == -1 or pos < clause_pos:
    #                                 clause_pos = pos
                        
    #                     # Insert the condition at the appropriate position
    #                     if clause_pos > -1:
    #                         original_query = original_query[:clause_pos] + condition_str + original_query[clause_pos:]
    #                     else:
    #                         original_query += condition_str
                        
    #                     request.env.cr.execute(original_query, (branches_array,))
    #                     result_value = request.env.cr.fetchone()[0] if request.env.cr.rowcount > 0 else 0
                
    #                     # Add the results to our collection
    #                     computed_results.append({
    #                         "name": stat["name"],
    #                         "scope": stat["scope"],
    #                         "val": result_value,
    #                         "id": stat["id"],
    #                         "scope_color": stat["scope_color"],
    #                         "query": stat["sql_query"]
    #                     })

    #         return {
    #                 "data": computed_results,
    #                 "total": len(computed_results)
    #         }


    