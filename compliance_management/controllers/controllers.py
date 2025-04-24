# # -*- coding: utf-8 -*-
from odoo import http
from odoo.http import request
from datetime import datetime, timedelta
from odoo import fields
import re


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

        lower_query = sql_query.lower()
        table = None
        domain = []

        # Check for aggregation functions (sum, avg, min, max) in the SELECT clause
        if re.search(r"\b(?:sum|avg|min|max)\s*\(", lower_query):
           return None

        # Extract table name (simplified, assumes single primary table or first table in JOIN)
        from_match = re.search(r"\bfrom\s+([\w.]+)", lower_query)
        if from_match:
            table = from_match.group(1)
        else:
            join_match = re.search(r"\b(?:inner|left|right|full outer)?\s+join\s+([\w.]+)", lower_query)
            if join_match:
                # table = join_match.group(1) # Gets the first table in the JOIN
                return None
        
        # Extract WHERE clause conditions and convert to Odoo domain format
        where_match = re.search(r"\bwhere\s+(.+)", lower_query)
        if where_match:
            condition_string = where_match.group(1)
            domain = self._parse_condition_to_odoo_domain(condition_string)  
        
        # check if it is not cco
        if not cco:
            domain.append(["branch_id", "in", self.check_branches_id(branches_id)])

        return {'table': table, 'domain': domain}

    def _parse_condition_to_odoo_domain(self, condition_string: str):

        python_values = {
            "null": None,
            "true": True,
            "false": False
        }
       
        domain = []
        # Basic splitting of AND conditions (very simplified)
        conditions = re.split(r"\s+and\s+", condition_string)
        for cond in conditions:
            parts = re.split(r"(is|=|>|<|>=|<=|!=|like|ilike|in|not\s+in)\s+", cond.strip(), maxsplit=1) # Split only once
            if len(parts) == 3:
                field, operator, value = parts
                field = field.strip()
                operator = operator.strip().lower()
                value = value.strip() # select count(id) from res_partner where bvn is null

        
                # Clean the value
                if value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                elif value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                
                # Convert SQL operators to Odoo domain operators
                if operator == '=':
                    odoo_operator = '='
                elif operator == 'is':
                    odoo_operator = '='
                elif operator == '>':
                    odoo_operator = '>'
                elif operator == '<':
                    odoo_operator = '<'
                elif operator == '>=':
                    odoo_operator = '>='
                elif operator == '<=':
                    odoo_operator = '<='
                elif operator == '!=':
                    odoo_operator = '!='
                elif operator == 'like':
                    odoo_operator = 'like'
                elif operator == 'ilike':
                    odoo_operator = 'ilike'
                elif operator == 'in':
                    odoo_operator = 'in'
                    value = value.replace("(", '[').replace(")", ']')
                elif operator == 'not in':
                    odoo_operator = 'not in'
                    value = value.replace("(", '[').replace(")", ']')
                else:
                    continue # Ignore unsupported operators

                for word in python_values:
                    if word == value:
                        value = python_values[word]
                        break

                domain.append([field, odoo_operator, value])
        return domain

    @http.route('/dashboard/stats', auth='public', type='json')
    def getAllstats(self, cco, branches_id, datepicked, **kw):

        today = datetime.now().date()  # Get today's date
        prevDate = today - timedelta(days=datepicked)  # Get previous date

        # Convert to datetime for start and end of the day
        start_of_prev_day = fields.Datetime.to_string(datetime.combine(prevDate, datetime.min.time()))

        end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))

        if cco == True:
            # fetch all data for chief compliance officer
            results = request.env["res.compliance.stat"].search([('create_date', '>=', start_of_prev_day), ('create_date', '<', end_of_today)])

            computed_results = []

            for result in results:
                computed_results.append({"name": result["name"],"scope": result["scope"], "val": result["val"], "id": result["id"], "scope_color": result["scope_color"], "query": result['sql_query']})

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
                AND rcs.create_date < %s;
            """

            request.env.cr.execute(query, (start_of_prev_day, end_of_today))

            # Get column names and results
            columns = [desc[0] for desc in request.env.cr.description]
            stat_records = [dict(zip(columns, row)) for row in request.env.cr.fetchall()]

            # Convert branches_id to a proper PostgreSQL array parameter
            branches_array = list(map(int, branches_id))  # Make sure all elements are integers

            # Process each compliance stat and execute its SQL query with branch filtering
            computed_results = []
            for stat in stat_records:
                query:str = stat['sql_query']
            
                
                if "res_partner" in query or "res.partner" in query or "tier" in query or "transaction" in query:
                    # Remove trailing semicolon if present
                    if query.endswith(";"):
                        query = query[:-1]
                    
                    # Add branch filtering to the query
                    if branches_array:
                        if " where " in query.lower() or " WHERE " in query:
                            query += f" AND branch_id = ANY(%s::integer[])"
                        else:
                            query += f" WHERE branch_id = ANY(%s::integer[])"
                            
                        # Execute the query with branch filter
                        request.env.cr.execute(query, (branches_array,))
                    else:
                        # If no branches, add a condition that returns no results
                        if " where " in query.lower() or " WHERE " in query:
                            query += " AND 1=0"
                        else:
                            query += " WHERE 1=0"
                            
                        request.env.cr.execute(query)
                
                    # For count queries, we expect a single row with a single value
                    result_value = request.env.cr.fetchone()[0] if request.env.cr.rowcount > 0 else 0

                    
                    # Add the results to our collection with the specific format you want
                    computed_results.append({
                        "name": stat["name"],
                        "scope": stat["scope"],
                        "val": result_value,  # This is the result of the SQL query
                        "id": stat["id"],
                        "scope_color": stat["scope_color"],
                        "query": stat["sql_query"]
                    })

            return {
                "data": computed_results,
                "total": len(computed_results)
            }

           

    @http.route('/dashboard/statsbycategory', auth='public', type='json')
    def getAllstatsByCategory(self, cco, branches_id, category, datepicked, **kw):

    
        today = datetime.now().date()  # Get today's date
        prevDate = today - timedelta(days=datepicked)  # Get previous date

        # Convert to datetime for start and end of the day
        start_of_prev_day = fields.Datetime.to_string(datetime.combine(prevDate, datetime.min.time()))

        end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))

        if cco == True:
            # fetch all data for chief compliance officer
            results = request.env["res.compliance.stat"].search([('scope', '=', category),('create_date', '>=', start_of_prev_day), ('create_date', '<', end_of_today)])

            computed_results = []

            for result in results:
                computed_results.append({"name": result["name"],"scope": result["scope"], "val": result["val"], "id": result["id"], "scope_color": result["scope_color"], "query": result['sql_query']})

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

            # Convert branches_id to a proper PostgreSQL array parameter
            branches_array = list(map(int, branches_id))  # Make sure all elements are integers

            # Process each compliance stat and execute its SQL query with branch filtering
            computed_results = []
            for stat in stat_records:
                query:str = stat['sql_query']
                
                
                if "res_partner" in query or "res.partner" in query or "tier" in query or "transaction" in query:
                    # Remove trailing semicolon if present
                    if query.endswith(";"):
                        query = query[:-1]
                    
                    # Add branch filtering to the query
                    if branches_array:
                        if " where " in query.lower() or " WHERE " in query:
                            query += f" AND branch_id = ANY(%s::integer[])"
                        else:
                            query += f" WHERE branch_id = ANY(%s::integer[])"
                            
                        # Execute the query with branch filter
                        request.env.cr.execute(query, (branches_array,))
                    else:
                        # If no branches, add a condition that returns no results
                        if " where " in query.lower() or " WHERE " in query:
                            query += " AND 1=0"
                        else:
                            query += " WHERE 1=0"
                            
                    request.env.cr.execute(query)
                    
                    # For count queries, we expect a single row with a single value
                    result_value = request.env.cr.fetchone()[0] if request.env.cr.rowcount > 0 else 0
                    
                    # Add the results to our collection with the specific format you want
                    computed_results.append({
                        "name": stat["name"],
                        "scope": stat["scope"],
                        "val": result_value,  # This is the result of the SQL query
                        "id": stat["id"],
                        "scope_color": stat["scope_color"],
                        "query": stat["sql_query"]
                    })

            return {
                "data": computed_results,
                "total": len(computed_results)
            }


    