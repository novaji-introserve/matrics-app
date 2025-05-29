# -*- coding: utf-8 -*-

from odoo import http
from odoo.http import request
from datetime import datetime, timedelta
from odoo import fields
import re
import logging
import json

# Import services and utilities
from ..services.security_service import SecurityService
from ..services.database_service import DatabaseService
from ..services.cache_service import CacheService
from ..services.materialized_view import MaterializedViewService
from ..utils.cache_key_unique_identifier import get_unique_client_identifier, normalize_cache_key_components
from ..services.query_service import QueryService

_logger = logging.getLogger(__name__)

class Compliance(http.Controller):
    """
    A controller to manage compliance-related operations for dashboards.

    This class handles user permissions, dynamic SQL extraction, caching, and 
    statistics retrieval based on user roles and branches.
    """
    
    def __init__(self):
        """
        Initialize the Compliance controller and its security service.

        This sets up the services for security, database operations, and caching.
        """
        super(Compliance, self).__init__()
        self.security_service = SecurityService()
        self.database_service = DatabaseService()
        self.cache_service = CacheService()
        self.query_service = QueryService()
        self.materialized_view_service = MaterializedViewService()
        self.get_unique_client_identifier = get_unique_client_identifier
        self.normalize_cache_key_components = normalize_cache_key_components

    @http.route("/dashboard/user", auth="public", type="json")
    def index(self, **kw):
        """
        Retrieve user information for dashboard display.

        This method checks user roles (superuser, CCO, CO) and returns
        relevant user data including branches and a unique client identifier.

        Returns:
            dict: A dictionary containing user role information and unique client ID.
        """
        user = request.env.user
        is_superuser = user.has_group("base.group_system")
        is_cco = self.security_service.is_cco_user()
        is_co = self.security_service.is_co_user()
        branch = [branch.id for branch in user.branches_id]
        unique_id = get_unique_client_identifier()
        
        result = {
            "group": is_cco,
            "is_cco": is_cco,
            "is_co": is_co,
            "branch": branch,
            "unique_id": unique_id,
        }
        
        return result

    def check_branches_id(self, branches_id):
        """
        Ensure branches_id is always a list.

        This method checks the type of branches_id and converts it to a list if necessary.

        Args:
            branches_id (list or any): The branches ID to check.

        Returns:
            list: A list of branches IDs.
        """
        return self.security_service.check_branches_id(branches_id)

    @http.route("/dashboard/dynamic_sql", auth="public", type="json")
    def extract_table_and_domain(self, sql_query: str, branches_id, cco):
        """
        Extract table names and WHERE conditions from SQL queries using regex.

        This method ignores COUNT aggregation and validates SQL structure for security.

        Args:
            sql_query (str): The SQL query string to analyze.
            branches_id (list): The IDs of branches to filter on.
            cco (bool): Indicates if the user is a CCO.

        Returns:
            dict: A dictionary containing the extracted table name and domain conditions.
        """
        is_cco = self.security_service.is_cco_user()
        is_co = self.security_service.is_co_user()
        
        if is_co:
            cco = True
            _logger.info(
                f"CO user {request.env.user.id} accessing dynamic SQL with CCO privileges"
            )
            
        # Extract main table from query
        lower_query = sql_query.lower()
        table = None
        domain = []
        
        # Skip aggregation queries
        if re.search(r"\b(?:sum|avg|min|max)\s*\(", lower_query):
            return None
            
        # Find the main table
        table = self.query_service.extract_main_table(sql_query)
        if not table:
            join_match = re.search(
                r"\b(?:inner|left|right|full outer)?\s+join\s+([\w.]+)", lower_query
            )
            if join_match:
                return None
                
        # Extract where conditions
        where_match = re.search(
            r"\bwhere\s+(.+?)(?:\s+(?:group\s+by|order\s+by|limit|having)\s+|\s*$)",
            lower_query,
            re.DOTALL,
        )
        
        if where_match:
            condition_string = where_match.group(1).strip()
            domain = self.query_service.parse_condition_string(condition_string)
            
        # Add additional filters
        additional_filters = []
        if table == "res_partner":
            additional_filters.append(("origin", "in", ["demo", "test", "prod"]))
            
        # Check if table has branch_id column
        db_service = DatabaseService(request.env)
        has_branch_id = db_service.check_table_for_branch_column(table) is not None
        
        # Apply branch filtering if needed
        if not cco and has_branch_id:
            branch_ids = self.check_branches_id(branches_id)
            additional_filters.append(("branch_id", "in", branch_ids))
            
        # Combine filters with domain
        if additional_filters:
            if domain:
                is_complex = any(op == "|" for op in domain if isinstance(op, str))
                if is_complex:
                    domain = ["&"] + domain + [additional_filters[0]]
                    for filter_item in additional_filters[1:]:
                        domain = ["&"] + domain + [filter_item]
                else:
                    for filter_item in additional_filters:
                        domain = ["&"] + domain + [filter_item]
            else:
                domain = additional_filters
                
        _logger.info(f"Final domain: {domain}")
        return {"table": table, "domain": domain}

    def format_number(self, result_value):
        """
        Format a number with commas for better readability.

        Args:
            result_value (int or float): The number to format.

        Returns:
            str: The formatted number as a string.
        """
        if isinstance(result_value, (int, float)):
            result_value = "{:,}".format(result_value)
            return result_value
        return result_value

    @http.route("/dashboard/stats", auth="public", type="json")
    def getAllstats(self, cco, branches_id, datepicked, **kw):
        """
        Retrieve all statistics for the dashboard.

        This method checks user permissions and retrieves statistics based on
        the given date and branches, caching results where applicable.

        Args:
            cco (bool): Indicates if the user is a CCO.
            branches_id (list): The IDs of branches to filter on.
            datepicked (int): The number of days to consider for the statistics.

        Returns:
            dict: A dictionary containing computed statistics and total count.
        """
        user_id = request.env.user.id
        is_cco = self.security_service.is_cco_user()
        is_co = self.security_service.is_co_user()
        
        if is_co or is_cco:
            cco = True
            if is_co:
                _logger.info(f"CO user {user_id} accessing stats with CCO privileges")
                
        # Generate cache key
        unique_id = get_unique_client_identifier()
        cco_str, branches_str, datepicked_str, unique_id = normalize_cache_key_components(
            cco, branches_id, datepicked, unique_id
        )
        cache_key = f"all_stats_{cco_str}_{branches_str}_{datepicked_str}_{unique_id}"
        _logger.info(f"This is the stats cache key: {cache_key}")
        
        # Check cache
        cache_service = CacheService(request.env)
        cache_data = cache_service.get_cache(cache_key, user_id)
        
        if cache_data:
            return cache_data
            
        # Tables to exclude from branch filtering for non-CCO users
        excluded_tables = ["res_branch", "res_risk_universe"]
        
        # For non-CCO users, verify branch access
        if not cco:
            branches_array = self.check_branches_id(branches_id)
            if not branches_array:
                return {"data": [], "total": 0}
                
        # Get all active statistics
        query = """
            SELECT rcs.*
            FROM res_compliance_stat rcs
            WHERE rcs.state = 'active'
            ORDER BY rcs.id
        """
        
        db_service = DatabaseService(request.env)
        success, stat_records, _ = db_service.execute_query_with_timeout(query)
        
        if not success or not stat_records:
            return {"data": [], "total": 0}
            
        computed_results = []
        
        # Process each statistic
        for stat in stat_records:
            with request.env.registry.cursor() as cr:
                try:
                    stat_id = stat["id"]
                    view_name = f"stat_view_{stat_id}"
                    result_value = None
                    use_view = stat.get("use_materialized_view", False)
                    
                    # Try to get data from materialized view if enabled
                    if use_view and db_service.check_view_exists(view_name):
                        # Get view columns
                        columns = db_service.get_table_columns(view_name)
                        
                        # Build filter query
                        filter_query = f"SELECT * FROM {view_name}"
                        
                        # Apply branch filtering for non-CCO users
                        if not cco and branches_id:
                            original_query = stat["sql_query"].lower()
                            main_table = self.query_service.extract_main_table(original_query)
                            
                            # Skip excluded tables for non-CCO users
                            if not cco and main_table in excluded_tables:
                                continue
                                
                            # Find branch column
                            branch_column = self.query_service.find_branch_column_in_view(columns)
                            
                            # Apply branch filtering
                            if branch_column:
                                branches_array = list(map(int, branches_id))
                                if branches_array:
                                    if len(branches_array) == 1:
                                        filter_query += f" WHERE {branch_column} = {branches_array[0]}"
                                    else:
                                        filter_query += f" WHERE {branch_column} IN {tuple(branches_array)}"
                                else:
                                    continue
                                    
                        # Execute query
                        try:
                            cr.execute(f"{filter_query} LIMIT 1")
                            result_row = cr.fetchone()
                            if result_row:
                                result_value = result_row[0] if result_row else 0
                        except Exception as view_error:
                            _logger.warning(f"Error querying view for stat {stat_id}: {view_error}")
                            
                    # If no result from view or view not used, execute direct query
                    if result_value is None:
                        original_query = stat["sql_query"]
                        query = original_query.lower()
                        main_table = self.query_service.extract_main_table(query)
                        
                        # Skip excluded tables for non-CCO users
                        if not cco and main_table in excluded_tables:
                            continue
                            
                        # Check if query needs modification
                        needs_modification = False
                        has_branch_id = False
                        branch_column_name = None
                        has_res_partner = (
                            re.search(r"\bres_partner\b", query, re.IGNORECASE)
                            is not None
                        )
                        
                        # Check if table has branch column
                        if main_table:
                            branch_column_name = db_service.check_table_for_branch_column(main_table)
                            has_branch_id = bool(branch_column_name)
                            
                        # Modify query for partners or branch filtering
                        if has_res_partner or has_branch_id:
                            needs_modification = True
                            if query.endswith(";"):
                                query = query[:-1]
                                original_query = original_query[:-1]
                                
                            has_where = bool(re.search(r"\bwhere\b", query))
                            conditions = []
                            
                            # Add branch condition for non-CCO users
                            if not cco and has_branch_id and branch_column_name:
                                branches_array = (
                                    list(map(int, branches_id)) if branches_id else []
                                )
                                if branches_array:
                                    if len(branches_array) == 1:
                                        conditions.append(
                                            f"{branch_column_name} = {branches_array[0]}"
                                        )
                                    else:
                                        conditions.append(
                                            f"{branch_column_name} IN {tuple(branches_array)}"
                                        )
                                else:
                                    conditions.append("1=0")
                                    
                            # Add partner origin condition
                            if has_res_partner:
                                conditions.append("origin IN ('demo','test','prod')")
                                
                            # Apply conditions to query
                            if conditions:
                                if has_where:
                                    condition_str = " AND " + " AND ".join(conditions)
                                else:
                                    condition_str = " WHERE " + " AND ".join(conditions)
                                    
                                # Insert conditions before GROUP BY, ORDER BY, etc.
                                clauses = [
                                    "group by",
                                    "order by",
                                    "limit",
                                    "offset",
                                    "having",
                                ]
                                clause_pos = -1
                                
                                for clause in clauses:
                                    pos = query.find(" " + clause + " ")
                                    if pos > -1:
                                        if clause_pos == -1 or pos < clause_pos:
                                            clause_pos = pos
                                            
                                if clause_pos > -1:
                                    original_query = (
                                        original_query[:clause_pos]
                                        + condition_str
                                        + original_query[clause_pos:]
                                    )
                                else:
                                    original_query += condition_str
                                    
                        # Execute query
                        try:
                            cr.execute(original_query)
                            result_row = cr.fetchone()
                            result_value = (
                                result_row[0] if result_row is not None else 0
                            )
                        except Exception as e:
                            _logger.error(
                                f"Error executing SQL query for stat {stat['name']}: {str(e)}"
                            )
                            computed_results.append(
                                {
                                    "name": stat["name"],
                                    "scope": stat["scope"],
                                    "val": "Error",
                                    "id": stat["id"],
                                    "scope_color": stat["scope_color"],
                                    "query": stat["sql_query"],
                                }
                            )
                            continue
                            
                    # Add result to computed results
                    computed_results.append(
                        {
                            "name": stat["name"],
                            "scope": stat["scope"],
                            "val": (
                                self.format_number(result_value)
                                if result_value is not None
                                else 0.0
                            ),
                            "id": stat["id"],
                            "scope_color": stat["scope_color"],
                            "query": stat["sql_query"],
                        }
                    )
                except Exception as e:
                    _logger.error(
                        f"Error processing stat {stat.get('name', 'Unknown')}: {str(e)}"
                    )
                    computed_results.append(
                        {
                            "name": stat.get("name", "Unknown"),
                            "scope": stat.get("scope", "Unknown"),
                            "val": "Error",
                            "id": stat.get("id", 0),
                            "scope_color": stat.get("scope_color", ""),
                            "query": stat.get("sql_query", ""),
                        }
                    )
                    
        # Cache results
        result = {"data": computed_results, "total": len(computed_results)}
        cache_service.set_cache(cache_key, result, user_id)
        
        return result

    @http.route("/dashboard/statsbycategory", auth="public", type="json")
    def getAllstatsByCategory(self, cco, branches_id, category, datepicked, **kw):
        """
        Retrieve statistics grouped by a specified category.

        This method handles user permissions, retrieves statistics based on
        the given category and date range, and returns formatted results.

        Args:
            cco (bool): Indicates if the user is a CCO.
            branches_id (list): The IDs of branches to filter on.
            category (str): The category to filter statistics by.
            datepicked (int): The number of days to consider for the statistics.

        Returns:
            dict: A dictionary containing computed statistics and total count.
        """
        # Calculate date range
        today = datetime.now().date()
        prevDate = today - timedelta(days=datepicked)
        start_of_prev_day = fields.Datetime.to_string(
            datetime.combine(prevDate, datetime.min.time())
        )
        end_of_today = fields.Datetime.to_string(
            datetime.combine(today, datetime.max.time())
        )
        
        # Check user permissions
        is_cco = self.security_service.is_cco_user()
        is_co = self.security_service.is_co_user()
        
        if is_co or is_cco:
            cco = True
            if is_co:
                _logger.info(
                    f"CO user {request.env.user.id} accessing stats by category with CCO privileges"
                )
                
        # Convert branches_id to array of integers
        branches_array = list(map(int, branches_id)) if branches_id else []
        
        # For CCO users
        if cco:
            # Get statistics for the category
            results = request.env["res.compliance.stat"].search(
                [
                    ("create_date", ">=", start_of_prev_day),
                    ("create_date", "<", end_of_today),
                    ("scope", "=", category),
                ]
            )
            
            computed_results = []
            
            # Process each statistic
            for result in results:
                original_query = result["sql_query"]
                query = original_query.lower()
                needs_modification = False
                
                # Check if query needs modification for partners or branches
                if any(
                    table in query
                    for table in ["res_partner", "res.partner", "tier", "transaction"]
                ):
                    needs_modification = True
                    if query.endswith(";"):
                        query = query[:-1]
                        original_query = original_query[:-1]
                        
                    has_where = bool(re.search(r"\bwhere\b", query))
                    conditions = []
                    
                    # Add partner origin condition
                    if "res_partner" in query or "res.partner" in query:
                        conditions.append("origin IN ('demo','test','prod')")
                        
                    # Apply conditions to query
                    if conditions:
                        if has_where:
                            condition_str = " AND " + " AND ".join(conditions)
                        else:
                            condition_str = " WHERE " + " AND ".join(conditions)
                            
                        # Insert conditions before GROUP BY, ORDER BY, etc.
                        clauses = ["group by", "order by", "limit", "offset", "having"]
                        clause_pos = -1
                        
                        for clause in clauses:
                            pos = query.find(" " + clause + " ")
                            if pos > -1:
                                if clause_pos == -1 or pos < clause_pos:
                                    clause_pos = pos
                                    
                        if clause_pos > -1:
                            original_query = (
                                original_query[:clause_pos]
                                + condition_str
                                + original_query[clause_pos:]
                            )
                        else:
                            original_query += condition_str
                            
                    # Execute query
                    request.env.cr.execute(original_query)
                else:
                    # Execute original query without modification
                    request.env.cr.execute(original_query)
                    
                # Get result
                result_value = (
                    request.env.cr.fetchone()[0] if request.env.cr.rowcount > 0 else 0
                )
                
                # Add result to computed results
                computed_results.append(
                    {
                        "name": result["name"],
                        "scope": result["scope"],
                        "val": self.format_number(result_value),
                        "id": result["id"],
                        "scope_color": result["scope_color"],
                        "query": result["sql_query"],
                    }
                )
                
            return {"data": computed_results, "total": len(results)}
        else:
            # For non-CCO users
            # Get statistics for the category
            query = """
                SELECT rcs.*
                FROM res_compliance_stat rcs
                WHERE rcs.create_date >= %s
                AND rcs.create_date < %s AND rcs.scope = %s;
            """
            
            request.env.cr.execute(query, (start_of_prev_day, end_of_today, category))
            stat_records = [
                dict(zip([desc[0] for desc in request.env.cr.description], row)) 
                for row in request.env.cr.fetchall()
            ]
            
            computed_results = []
            
            # Process each statistic
            for stat in stat_records:
                original_query = stat["sql_query"]
                query = original_query.lower()
                needs_modification = False
                
                # Check if query needs modification for partners or branches
                if any(
                    table in query
                    for table in ["res_partner", "res.partner", "transaction"]
                ):
                    needs_modification = True
                    if query.endswith(";"):
                        query = query[:-1]
                        original_query = original_query[:-1]
                        
                    has_where = bool(re.search(r"\bwhere\b", query))
                    conditions = []
                    
                    # Add branch condition
                    if branches_array:
                        conditions.append(f"branch_id IN {tuple(branches_array)}")
                    else:
                        conditions.append("1=0")
                        
                    # Add partner origin condition
                    if "res_partner" in query or "res.partner" in query:
                        conditions.append("origin IN ('demo','test','prod')")
                        
                    # Apply conditions to query
                    if conditions:
                        if has_where:
                            condition_str = " AND " + " AND ".join(conditions)
                        else:
                            condition_str = " WHERE " + " AND ".join(conditions)
                            
                        # Insert conditions before GROUP BY, ORDER BY, etc.
                        clauses = ["group by", "order by", "limit", "offset", "having"]
                        clause_pos = -1
                        
                        for clause in clauses:
                            pos = query.find(" " + clause + " ")
                            if pos > -1:
                                if clause_pos == -1 or pos < clause_pos:
                                    clause_pos = pos
                                    
                        if clause_pos > -1:
                            original_query = (
                                original_query[:clause_pos]
                                + condition_str
                                + original_query[clause_pos:]
                            )
                        else:
                            original_query += condition_str
                            
                        # Execute query
                        request.env.cr.execute(original_query)
                        result_value = (
                            request.env.cr.fetchone()[0]
                            if request.env.cr.rowcount > 0
                            else 0
                        )
                        
                        # Add result to computed results
                        computed_results.append(
                            {
                                "name": stat["name"],
                                "scope": stat["scope"],
                                "val": self.format_number(result_value),
                                "id": stat["id"],
                                "scope_color": stat["scope_color"],
                                "query": stat["sql_query"],
                            }
                        )
                        
            return {"data": computed_results, "total": len(computed_results)}