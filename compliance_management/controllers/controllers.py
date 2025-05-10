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
                    # # For simple domain, add '&' operators
                    # domain = ['&'] * (len(additional_filters) - 1 + 1) + [domain] + additional_filters
                    # Simple domain: append filters directly without '&'
                    # domain = domain + additional_filters
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

    def parse_condition_string(self, condition_string: str):
        """
        Parse a condition string into Odoo domain format.
        Handles AND/OR operators and respects parentheses and quoted strings.
        """
        # Split by AND operators
        and_conditions = self._split_by_operator(condition_string, ' AND ')
        
        if len(and_conditions) == 1:
            return self._parse_single_condition(and_conditions[0])
        
        domain = []
        for i, cond in enumerate(and_conditions):
            parsed_condition = self._parse_single_condition(cond)
            if i < len(and_conditions) - 1:
                domain.append('&')
            domain.extend(parsed_condition)
            
        return domain

    def _split_by_operator(self, condition_string: str, operator: str):
        """
        Split a condition string by a specified operator,
        respecting parentheses and quoted strings.
        """
        conditions = []
        current_condition = ""
        paren_level = 0
        in_quote = False
        quote_char = None
        
        i = 0
        while i < len(condition_string):
            c = condition_string[i]
            
            if c in ("'", '"') and (quote_char is None or c == quote_char):
                in_quote = not in_quote
                quote_char = c if in_quote else None
                current_condition += c
            elif c == '(' and not in_quote:
                paren_level += 1
                current_condition += c
            elif c == ')' and not in_quote:
                paren_level -= 1
                current_condition += c
            elif (paren_level == 0 and not in_quote and 
                  i + len(operator) <= len(condition_string) and
                  condition_string[i:i+len(operator)].upper() == operator):
                conditions.append(current_condition.strip())
                current_condition = ""
                i += len(operator) - 1
            else:
                current_condition += c
            
            i += 1
        
        if current_condition:
            conditions.append(current_condition.strip())
        
        return conditions

    def _parse_single_condition(self, condition: str):
        """
        Parse a single condition which might contain OR operators.
        Returns a list in Odoo domain format.
        """
        or_conditions = self._split_by_operator(condition, ' OR ')
        
        if len(or_conditions) == 1:
            condition = or_conditions[0].strip()
            if condition.startswith('(') and condition.endswith(')'):
                inner_condition = condition[1:-1].strip()
                inner_or_conditions = self._split_by_operator(inner_condition, ' OR ')
                if len(inner_or_conditions) > 1:
                    or_conditions = inner_or_conditions
                else:
                    return self._convert_to_odoo_tuple(inner_condition)
            else:
                return self._convert_to_odoo_tuple(condition)
        
        domain = []
        for i in range(len(or_conditions) - 1):
            domain.append('|')
        
        for cond in or_conditions:
            parsed = self._convert_to_odoo_tuple(cond.strip())
            domain.extend(parsed)
            
        return domain

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

    def check_branches_id(self, branches_id):
        """
        Placeholder method to validate and return branch IDs.
        Replace with actual implementation.
        """
        # Example implementation (replace with actual logic)
        return branches_id if isinstance(branches_id, list) else [branches_id]
        
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

        

        pattern = r"\bres_partner\b"

        if cco == True:
            # fetch all data for chief compliance officer
            results = request.env["res.compliance.stat"].search([])

            computed_results = []

            for result in results:
                original_query = result['sql_query']
                query = original_query.lower()  # Use lowercase for checks but keep original for execution
                needs_modification = False
                
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
                
                   
                try:
                    request.env.cr.execute(original_query)
                    # For count queries, we expect a single row with a single value
                    result_value = request.env.cr.fetchone()
                    result_value = result_value[0] if result_value is not None else 0
                    computed_results.append({
                        "name": result["name"],
                        "scope": result["scope"],
                        "val": self.format_number(result_value) if result_value is not None else 0.0,
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
                        
                
            result = {
                "data": computed_results,
                "total": len(results)
            }
            
            # Store in cache before returning - use user_id instead of primary_group_id
            request.env['res.dashboard.cache'].set_cache(cache_key, result, user_id)
            
            return result
        else:

           
            # Define excluded tables
            excluded_tables = ["res_branch", "res_risk_universe"]

            # First get all compliance stats
            query = """
                SELECT rcs.*
                FROM res_compliance_stat rcs
            """
            request.env.cr.execute(query)

            # Get column names and results
            columns = [desc[0] for desc in request.env.cr.description]
            stat_records = [dict(zip(columns, row)) for row in request.env.cr.fetchall()]

            # Convert branches_id to a proper PostgreSQL array parameter
            branches_array = list(map(int, branches_id)) if branches_id else []

            

            # Process each compliance stat and execute its SQL query with branch filtering
            computed_results = []
            for stat in stat_records:
                original_query = stat['sql_query']
                query = original_query.lower() 
 

                # Extract the main table from the query
                main_table = self.extract_main_table(query)


                # Skip if the main table is in excluded_tables
                if main_table in excluded_tables :
                    continue  # Skip processing this stat

                needs_modification = False
                has_branch_id = False

                 

                # Check if the main table has a branch_id column
                if main_table:
                    # Handle possible schema.table format
                    if '.' in main_table:
                        schema, table = main_table.split('.')
                        check_query = """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_schema = %s AND table_name = %s AND column_name = 'branch_id'
                        """
                        request.env.cr.execute(check_query, (schema, table))
                        has_branch_id = bool(request.env.cr.fetchone())
                    else:
                        # Check in public schema
                        check_query = """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_schema = 'public' AND table_name = %s AND column_name = 'branch_id'
                        """
                        request.env.cr.execute(check_query, (main_table,))
                        has_branch_id = bool(request.env.cr.fetchone())
                
                has_res_partner = re.search(r"\bres_partner\b", query, re.IGNORECASE) is not None
                if has_res_partner or has_branch_id:
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
                        conditions.append(f"branch_id IN {tuple(branches_array)}" if len(branches_array) > 1 else f"branch_id = {branches_array[0]}")
                    elif branches_array and not has_branch_id:
                        # Skip branch filtering for tables without branch_id column
                        pass
                    elif not branches_array and has_branch_id:
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

                # Execute the query with or without parameters based on conditions
                
                try:
                    request.env.cr.execute(original_query)
                    result_row = request.env.cr.fetchone()
                    result_value = result_row[0] if result_row is not None else 0
        
                    computed_results.append({
                        "name": stat["name"],
                        "scope": stat["scope"],
                        "val": self.format_number(result_value) if result_value is not None else 0.0,
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
            
    
            result = {
                "data": computed_results,
                "total": len(computed_results)
            }

            # Store in cache before returning
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


    