import logging
from odoo.http import request
import re

_logger = logging.getLogger(__name__)

class ChartSecurityService:
    """Service for enforcing branch-level security for charts"""
    
    @staticmethod
    def get_user_branch_ids():
        """Get the branches accessible to the current user"""
        if not request or not request.env:
            return []
            
        user = request.env.user
        
        # If user is admin or has access to all branches, return empty list (no restriction)
        if user.has_group('base.group_system') or user.has_group('base.group_erp_manager'):
            return []
            
        # Get user's branch
        user_branch_id = user.branch_id.id if hasattr(user, 'branch_id') and user.branch_id else False
        
        # Get all branches user has access to via security rules
        accessible_branches = []
        
        if user_branch_id:
            accessible_branches.append(user_branch_id)
            
        # Add any additional branches the user has access to based on roles/permissions
        if hasattr(user, 'branches_id'):
            for branch in user.branches_id:
                if branch.id not in accessible_branches:
                    accessible_branches.append(branch.id)
        
        # Check if there's a branch access model that grants extra access
        branch_access_model = 'res.branch.access'
        if branch_access_model in request.env:
            branch_access = request.env[branch_access_model].search([('user_id', '=', user.id)])
            for access in branch_access:
                if access.branch_id and access.branch_id.id not in accessible_branches:
                    accessible_branches.append(access.branch_id.id)
        
        return accessible_branches
    
    @staticmethod
    def is_cco_user():
        """Check if current user is a Chief Compliance Officer"""
        if not request or not request.env:
            return False
            
        user = request.env.user
        
        # Check if user belongs to CCO group
        return any(group.name.lower() == 'chief compliance officer' for group in user.groups_id)
    
    # @staticmethod
    # def apply_branch_security_filter(query, branch_field, cco=False, branches_id=None):
    #     """Apply branch security filter to query based on user access"""
    #     if not query or not branch_field:
    #         return query
                
    #     # If CCO, no filtering needed
    #     if cco or ChartSecurityService.is_cco_user():
    #         return query
                
    #     # Get user's accessible branches
    #     user_branches = ChartSecurityService.get_user_branch_ids()
        
    #     # If user has no branch restrictions, only apply filters passed from UI
    #     if not user_branches:
    #         if branches_id and len(branches_id) > 0:
    #             # Apply branch filter specified in the UI parameters
    #             return ChartSecurityService._add_branch_filter_to_query(query, branch_field, branches_id)
    #         return query
        
    #     # User has branch restrictions, ensure only authorized branches are accessible
    #     effective_branches = []
        
    #     if branches_id and len(branches_id) > 0:
    #         # If branches specified in UI, only use those that user can access
    #         for branch_id in branches_id:
    #             if branch_id in user_branches:
    #                 effective_branches.append(branch_id)
    #     else:
    #         # Otherwise use all branches user has access to
    #         effective_branches = user_branches
        
    #     # If no effective branches (intersection is empty), return empty result query
    #     if not effective_branches:
    #         return ChartSecurityService._add_condition_to_query(query, "1 = 0")
        
    #     # Apply branch filter with effective branches
    #     return ChartSecurityService._add_branch_filter_to_query(query, branch_field, effective_branches)
    
    @staticmethod
    def apply_branch_security_filter(query, branch_field, cco=False, branches_id=None):
        """Apply branch security filter to query with improved subquery handling"""
        if not query or not branch_field:
            return query
                
        # If CCO, no filtering needed
        if cco or ChartSecurityService.is_cco_user():
            return query
                
        # Get user's accessible branches
        user_branches = ChartSecurityService.get_user_branch_ids()
        
        # If user has no branch restrictions, only apply filters passed from UI
        if not user_branches:
            if branches_id and len(branches_id) > 0:
                # Check for subqueries to determine approach
                has_subqueries = '(' in query and 'SELECT' in query.upper() and 'FROM' in query.upper()
                if has_subqueries:
                    return ChartSecurityService._apply_branch_filter_with_laterals(query, branch_field, branches_id)
                else:
                    return ChartSecurityService._add_branch_filter_to_query(query, branch_field, branches_id)
            return query
        
        # User has branch restrictions, ensure only authorized branches are accessible
        effective_branches = []
        
        if branches_id and len(branches_id) > 0:
            # If branches specified in UI, only use those that user can access
            for branch_id in branches_id:
                if branch_id in user_branches:
                    effective_branches.append(branch_id)
        else:
            # Otherwise use all branches user has access to
            effective_branches = user_branches
        
        # If no effective branches (intersection is empty), return empty result query
        if not effective_branches:
            return ChartSecurityService._add_condition_to_query(query, "1 = 0")
        
        # Apply branch filter with effective branches
        has_subqueries = '(' in query and 'SELECT' in query.upper() and 'FROM' in query.upper()
        if has_subqueries:
            return ChartSecurityService._apply_branch_filter_with_laterals(query, branch_field, effective_branches)
        else:
            return ChartSecurityService._add_branch_filter_to_query(query, branch_field, effective_branches)
    
    @staticmethod
    def _add_branch_filter_to_query(query, branch_field, branches_id):
        """Add branch filter to a query"""
        if not branches_id or len(branches_id) == 0:
            return ChartSecurityService._add_condition_to_query(query, "1 = 0")
            
        if len(branches_id) == 1:
            condition = f"{branch_field} = {branches_id[0]}"
        else:
            condition = f"{branch_field} IN {tuple(branches_id)}"
            
        return ChartSecurityService._add_condition_to_query(query, condition)
    
    @staticmethod
    def _add_condition_to_query(query, condition):
        """Add a WHERE condition to an SQL query with improved syntax handling"""
        # Clean up query
        query = query.strip()
        if query.endswith(';'):
            query = query[:-1]
        
        # Check if the condition is already in the query
        if condition in query:
            return query
        
        # Validate the condition to ensure it doesn't cause syntax errors
        if not condition or not condition.strip():
            return query
        
        # Never add WHERE keyword in the condition itself
        condition = condition.strip()
        if condition.upper().startswith('WHERE '):
            condition = condition[6:].strip()
        
        # Check if query has a WHERE clause
        has_where = bool(re.search(r'\bWHERE\b', query, re.IGNORECASE))
        
        if has_where:
            # Add condition with AND
            for clause in ['GROUP BY', 'ORDER BY', 'LIMIT', 'OFFSET', 'HAVING']:
                clause_pattern = r'\b' + clause + r'\b'
                clause_match = re.search(clause_pattern, query, re.IGNORECASE)
                
                if clause_match:
                    # Insert before this clause
                    position = clause_match.start()
                    return query[:position] + f" AND ({condition}) " + query[position:]
            
            # No other clauses, append to the end
            return query + f" AND ({condition})"
        else:
            # No WHERE clause, add one
            for clause in ['GROUP BY', 'ORDER BY', 'LIMIT', 'OFFSET', 'HAVING']:
                clause_pattern = r'\b' + clause + r'\b'
                clause_match = re.search(clause_pattern, query, re.IGNORECASE)
                
                if clause_match:
                    # Insert before this clause
                    position = clause_match.start()
                    return query[:position] + f" WHERE ({condition}) " + query[position:]
            
            # No other clauses, append to the end
            return query + f" WHERE ({condition})"
    
    @staticmethod
    def apply_partner_origin_filter(query):
        """Add origin filter for partner tables with improved subquery handling"""
        # Clean up query
        query = query.strip()
        if query.endswith(';'):
            query = query[:-1]
            
        # Early exit if no res_partner table referenced
        if not re.search(r'\bres_partner\b', query, re.IGNORECASE):
            return query
        
        # Check for existing origin filters to avoid duplication
        origin_patterns = [
            r'\borigin\s+IN\s+\(.*?\)',
            r'\brp\.origin\s+IN\s+\(.*?\)',
            r'\bres_partner\.origin\s+IN\s+\(.*?\)'
        ]
        
        for pattern in origin_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                return query
        
        # Handle case with subqueries by processing the query as text
        modified_query = query
        
        # Look for subqueries including res_partner
        subquery_pattern = r'(\(\s*SELECT\s+.*?FROM\s+.*?res_partner\b.*?)(\s+WHERE\s+.*?)(\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|\))'
        subquery_matches = list(re.finditer(subquery_pattern, query, re.IGNORECASE | re.DOTALL))
        
        if subquery_matches:
            # Process each matching subquery
            for match in subquery_matches:
                subquery_start = match.group(1)
                where_clause = match.group(2)
                subquery_end = match.group(3)
                
                # Add origin condition to WHERE clause
                new_where = where_clause + " AND origin IN ('demo', 'test', 'prod') "
                
                # Replace in the original query
                modified_subquery = subquery_start + new_where + subquery_end
                modified_query = modified_query.replace(match.group(0), modified_subquery)
        else:
            # No subqueries with WHERE clauses found, check for subqueries without WHERE
            basic_subquery = r'(\(\s*SELECT\s+.*?FROM\s+.*?res_partner\b.*?)(\s+GROUP BY\b|\s+ORDER BY\b|\s+LIMIT\b|\))'
            basic_matches = list(re.finditer(basic_subquery, query, re.IGNORECASE | re.DOTALL))
            
            if basic_matches:
                # Process each basic subquery
                for match in basic_matches:
                    subquery_part = match.group(1)
                    end_part = match.group(2)
                    
                    # Add WHERE clause
                    modified_subquery = subquery_part + " WHERE origin IN ('demo', 'test', 'prod') " + end_part
                    modified_query = modified_query.replace(match.group(0), modified_subquery)
            else:
                # No subqueries, apply to main query
                # Find res_partner table and its alias if any
                table_pattern = r'\bres_partner\b(?:\s+AS\s+|\s+)(\w+)'
                table_match = re.search(table_pattern, query, re.IGNORECASE)
                
                if table_match:
                    # Use alias
                    alias = table_match.group(1)
                    condition = f"{alias}.origin IN ('demo', 'test', 'prod')"
                else:
                    # No alias
                    condition = "origin IN ('demo', 'test', 'prod')"
                    
                # Add to the query
                if re.search(r'\bWHERE\b', modified_query, re.IGNORECASE):
                    # Add to existing WHERE
                    modified_query = re.sub(
                        r'(\bWHERE\b.*?)(\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)', 
                        r'\1 AND ' + condition + r' \2', 
                        modified_query, 
                        flags=re.IGNORECASE | re.DOTALL
                    )
                else:
                    # Add new WHERE
                    for clause in ['GROUP BY', 'ORDER BY', 'LIMIT']:
                        if re.search(r'\b' + clause + r'\b', modified_query, re.IGNORECASE):
                            modified_query = re.sub(
                                r'(\b' + clause + r'\b)', 
                                r' WHERE ' + condition + r' \1', 
                                modified_query, 
                                flags=re.IGNORECASE
                            )
                            break
                    else:
                        # No clause to insert before
                        modified_query += " WHERE " + condition
        
        return modified_query
        
    # @staticmethod
    # def secure_chart_query(chart, cco=False, branches_id=None):
    #     """Apply all security filters to a chart query with enhanced error prevention"""
    #     if not chart or not chart.query:
    #         return chart.query
                
    #     query = chart.query
        
    #     # Clean query for processing
    #     query = query.strip()
    #     if query.endswith(';'):
    #         query = query[:-1]
        
    #     # Check query for syntax issues before applying filters
    #     if 'WHERE WHERE' in query.upper() or 'AND WHERE' in query.upper():
    #         # Found syntax issues, attempt to clean up first
    #         query = re.sub(r'\bWHERE\s+WHERE\b', 'WHERE', query, flags=re.IGNORECASE)
    #         query = re.sub(r'\bAND\s+WHERE\b', 'AND', query, flags=re.IGNORECASE)
        
    #     # Apply partner origin filter - this is causing problems in subqueries
    #     original_query = query
    #     query = ChartSecurityService.apply_partner_origin_filter(query)
        
    #     # Apply branch filter if needed and not in CCO mode
    #     if not cco and chart.branch_filter and chart.branch_field:
    #         # Handle branch filters properly for both main query and subqueries
    #         branch_condition = ChartSecurityService._build_branch_condition(chart.branch_field, branches_id)
    #         if branch_condition:
    #             query = ChartSecurityService._add_condition_to_query_safely(query, branch_condition)
        
    #     # Final validation
    #     if 'WHERE WHERE' in query.upper() or 'AND WHERE' in query.upper():
    #         # Still has syntax issues, revert to original query
    #         _logger.error(f"Security filter created syntax error, using original query for chart {chart.id}")
    #         query = original_query
        
    #     # Add semicolon for final query
    #     if not query.endswith(';'):
    #         query += ';'
                
    #     return query
    
    @staticmethod
    def secure_chart_query(chart, cco=False, branches_id=None):
        """Apply all security filters to a chart query with improved subquery handling"""
        if not chart or not chart.query:
            return chart.query
                
        query = chart.query
        
        # Clean query for consistent processing
        query = query.strip()
        if query.endswith(';'):
            query = query[:-1]
        
        # Check if query has subqueries
        has_subqueries = '(' in query and 'SELECT' in query.upper() and 'FROM' in query.upper()
        
        # Apply partner origin filter first
        query = ChartSecurityService.apply_partner_origin_filter(query)
        
        # Apply branch filter if needed, with special handling for subqueries
        if chart.branch_filter and chart.branch_field and not cco and not ChartSecurityService.is_cco_user():
            # Get user's branch access
            user_branches = ChartSecurityService.get_user_branch_ids()
            
            # Determine effective branches
            effective_branches = []
            if branches_id:
                # If branches specified, intersect with user access
                if user_branches:
                    effective_branches = [b for b in branches_id if b in user_branches]
                else:
                    effective_branches = branches_id
            elif user_branches:
                effective_branches = user_branches
            
            # Apply branch filter only if we have effective branches
            if effective_branches:
                # Special handling for subqueries
                if has_subqueries:
                    # Use LATERAL JOIN approach for subqueries
                    query = ChartSecurityService._apply_branch_filter_with_laterals(
                        query, chart.branch_field, effective_branches
                    )
                else:
                    # Simple case - use normal branch filter
                    branch_condition = ChartSecurityService._build_branch_condition(
                        chart.branch_field, effective_branches
                    )
                    query = ChartSecurityService._add_condition_to_query(query, branch_condition)
            else:
                # No effective branches - return empty result
                query = ChartSecurityService._add_condition_to_query(query, "1 = 0")
                
        # Add semicolon for final query
        if not query.endswith(';'):
            query += ';'
                
        return query
    
    @staticmethod
    def _apply_branch_filter_with_laterals(query, branch_field, branches_id):
        """Apply branch filtering to queries with subqueries using LATERAL JOIN approach"""
        # Check if it's a query with LEFT JOIN and a subquery
        if 'LEFT JOIN (' in query.upper() and 'SELECT' in query.upper():
            # Find the table alias and branch field to create a proper condition
            # Let's parse the branch field to handle table aliases properly
            parts = branch_field.split('.')
            if len(parts) == 2:
                table_alias = parts[0]
                field_name = parts[1]
            else:
                # Try to find the table alias in the query
                match = re.search(r'\bFROM\s+(\w+)', query, re.IGNORECASE)
                if match:
                    table_alias = match.group(1)
                    field_name = branch_field
                else:
                    # Fall back to unqualified field name
                    table_alias = None
                    field_name = branch_field
            
            # Create a branch condition outside the subquery
            if table_alias:
                # Create a properly formatted condition
                if len(branches_id) == 1:
                    branch_condition = f"{table_alias}.{field_name} = {branches_id[0]}"
                else:
                    branch_condition = f"{table_alias}.{field_name} IN {tuple(branches_id)}"
                    
                # Apply to the main query, not inside subqueries
                if 'WHERE' in query.upper():
                    # Add to existing WHERE clause
                    query = re.sub(
                        r'(\bWHERE\b.*?)(\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)',
                        r'\1 AND ' + branch_condition + r' \2',
                        query,
                        flags=re.IGNORECASE | re.DOTALL,
                        count=1
                    )
                else:
                    # Add new WHERE clause
                    for clause in ['GROUP BY', 'ORDER BY', 'LIMIT']:
                        if re.search(r'\b' + clause + r'\b', query, re.IGNORECASE):
                            query = re.sub(
                                r'(\b' + clause + r'\b)',
                                r' WHERE ' + branch_condition + r' \1',
                                query,
                                flags=re.IGNORECASE,
                                count=1
                            )
                            break
                    else:
                        # No clause to insert before
                        query += " WHERE " + branch_condition
                
                return query
                
            else:
                # If we can't determine the table alias, use a different approach
                # Convert the query to use LATERAL JOIN - this is more complex 
                # and would require restructuring the query
                
                # For now, log a warning and return the original query
                _logger.warning(f"Could not apply branch filter to complex query with subqueries: {query}")
                return query
        else:
            # For other types of queries, use the standard approach
            branch_condition = ChartSecurityService._build_branch_condition(branch_field, branches_id)
            return ChartSecurityService._add_condition_to_query(query, branch_condition)
    
    @staticmethod
    def _build_branch_condition(branch_field, branches_id):
        """Build a branch filter condition safely"""
        if not branch_field or not branches_id:
            return None
            
        # Handle case where branches_id is empty
        if not branches_id:
            return None
            
        if len(branches_id) == 1:
            return f"{branch_field} = {branches_id[0]}"
        else:
            return f"{branch_field} IN {tuple(branches_id)}"

    @staticmethod
    def _add_condition_to_query_safely(query, condition):
        """Add a condition to query with extra safety checks for subqueries"""
        if not condition:
            return query
            
        # Clean condition to ensure no keywords are included
        condition = condition.strip()
        if condition.upper().startswith('WHERE '):
            condition = condition[6:].strip()
        
        # For queries with subqueries, we need special handling
        if '(' in query and ')' in query and 'SELECT' in query.upper() and 'FROM' in query.upper():
            # Main query condition first
            main_query = query
            
            # Check if main query has WHERE clause
            if re.search(r'\bWHERE\b', main_query, re.IGNORECASE):
                # Add condition with AND to main query WHERE clause
                main_query = re.sub(
                    r'\bWHERE\b(.*?)(?=\(\s*SELECT|\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)', 
                    r'WHERE\1 AND ' + condition + ' ', 
                    main_query,
                    flags=re.IGNORECASE | re.DOTALL,
                    count=1
                )
            else:
                # Add new WHERE clause to main query
                for clause in ['GROUP BY', 'ORDER BY', 'LIMIT', 'OFFSET', 'HAVING']:
                    clause_pattern = r'\b' + clause + r'\b'
                    if re.search(clause_pattern, main_query, re.IGNORECASE):
                        main_query = re.sub(
                            clause_pattern,
                            r'WHERE ' + condition + ' ' + clause,
                            main_query,
                            flags=re.IGNORECASE,
                            count=1
                        )
                        break
                else:
                    # No clause found, add WHERE at the end
                    main_query = main_query + " WHERE " + condition
                    
            return main_query
        else:
            # Simple case - just use the regular method
            if re.search(r'\bWHERE\b', query, re.IGNORECASE):
                # Add condition with AND
                for clause in ['GROUP BY', 'ORDER BY', 'LIMIT', 'OFFSET', 'HAVING']:
                    clause_pattern = r'\b' + clause + r'\b'
                    clause_match = re.search(clause_pattern, query, re.IGNORECASE)
                    
                    if clause_match:
                        # Insert before this clause
                        position = clause_match.start()
                        return query[:position] + f" AND {condition} " + query[position:]
                
                # No other clauses, append to the end
                return query + f" AND {condition}"
            else:
                # No WHERE clause, add one
                for clause in ['GROUP BY', 'ORDER BY', 'LIMIT', 'OFFSET', 'HAVING']:
                    clause_pattern = r'\b' + clause + r'\b'
                    clause_match = re.search(clause_pattern, query, re.IGNORECASE)
                    
                    if clause_match:
                        # Insert before this clause
                        position = clause_match.start()
                        return query[:position] + f" WHERE {condition} " + query[position:]
                
                # No other clauses, append to the end
                return query + f" WHERE {condition}"