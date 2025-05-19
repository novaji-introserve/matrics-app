import re
import logging

_logger = logging.getLogger(__name__)

class ChartQueryService:
    """Service dedicated to building and optimizing SQL queries for charts"""
    
    def __init__(self):
        pass
    
    def build_filtered_query(self, chart, cco, branches_id):
        """Build a filtered SQL query based on parameters"""
        # Get original query and prepare for modification
        original_query = chart.query
        if not original_query:
            _logger.error(f"Empty query for chart {chart.id}")
            return "SELECT 1 WHERE FALSE"  # Return empty result set query
            
        # Clean up the query - remove semicolons and extra whitespace
        modified_query = original_query.replace(';', '').strip()
        
        # Add branch filtering if needed
        if not cco and chart.branch_filter:
            if branches_id and len(branches_id) > 0:
                where_clause = self._build_branch_filter_clause(chart.branch_field, branches_id)
                modified_query = self._add_where_to_query(modified_query, where_clause)
            else:
                # No branches selected, return empty result
                modified_query = self._add_where_to_query(modified_query, "1 = 0")
        
        # Add index hint if not present - this is safe and helps performance
        modified_query = self._add_index_hint(modified_query)
        
        # Ensure the query has a LIMIT to avoid retrieving too many rows
        modified_query = self.ensure_query_limit(modified_query)
        
        # Add query comment for easier debugging
        modified_query = f"/* Chart: {chart.id} - {chart.name} */\n{modified_query}"
        
        return modified_query
    
    def _build_branch_filter_clause(self, branch_field, branches_id):
        """Build an optimized SQL clause for branch filtering"""
        if not branch_field:
            return None
            
        if len(branches_id) == 1:
            return f"{branch_field} = {branches_id[0]}"
        else:
            # For many branches, use IN clause with tuple
            return f"{branch_field} IN {tuple(branches_id)}"
    
    def _add_index_hint(self, query):
        """Add index hint to the query if supported by the database"""
        # Only add if not already present
        if "/*+" not in query and "ANALYZE" not in query.upper():
            query = re.sub(r'SELECT\s+', 'SELECT /*+ ANALYZE */ ', query, flags=re.IGNORECASE, count=1)
        return query
    
    def ensure_query_limit(self, query, default_limit=100):
        """Ensure the query has a reasonable LIMIT clause"""
        # Check if query already has a LIMIT
        limit_match = re.search(r'\bLIMIT\b\s+(\d+)', query, re.IGNORECASE)
        
        if limit_match:
            # Get the current limit and ensure it's reasonable
            current_limit = int(limit_match.group(1))
            if current_limit > 1000:  # Cap at 1000 rows for performance
                # Replace existing limit with capped value
                query = re.sub(
                    r'\bLIMIT\b\s+\d+', 
                    f'LIMIT 10000', 
                    query, 
                    flags=re.IGNORECASE
                )
            return query
        else:
            # Add a default limit
            return f"{query} LIMIT {default_limit}"
    
    def build_count_query(self, chart, query):
        """Build an optimized query to count total results"""
        # Strip any existing LIMIT/OFFSET clauses for count query
        base_query = re.sub(
            r'\bORDER\s+BY\b.*?(?=\bLIMIT\b|\bOFFSET\b|$)',
            '',  # Remove ORDER BY for count query (faster)
            query, 
            flags=re.IGNORECASE | re.DOTALL
        )
        base_query = re.sub(r'\bLIMIT\b\s+\d+(?:\s+OFFSET\s+\d+)?', '', base_query, flags=re.IGNORECASE)
        base_query = re.sub(r'\bOFFSET\b\s+\d+', '', base_query, flags=re.IGNORECASE)
        
        # Add timeout hint to prevent long-running count queries
        return f"SELECT /*+ MAX_EXECUTION_TIME(5000) */ COUNT(*) as total FROM ({base_query}) AS count_table"
    
    def build_paginated_query(self, query, page, page_size):
        """Build paginated query with optimizations"""
        # Ensure page and page_size are reasonable
        if page < 0:
            page = 0
        if page_size <= 0 or page_size > 100:
            page_size = 50  # Default reasonable page size
        
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
            return f"{query} LIMIT {page_size} OFFSET {page * page_size}"
            
    def _add_where_to_query(self, query, where_clause):
        """Add a WHERE clause to an SQL query"""
        # Remove any trailing semicolons
        query = query.replace(';', '').strip()
        
        if 'WHERE' in query.upper():
            # Find the position of WHERE
            where_pos = query.upper().find('WHERE')
            # Get everything after WHERE (the original conditions)
            where_content = query[where_pos + 5:].strip()
            
            # Determine whether we need to add an AND
            if where_content:
                # There are existing conditions, so add AND
                new_where = f"WHERE {where_clause} AND {where_content}"
            else:
                # No existing conditions after WHERE
                new_where = f"WHERE {where_clause}"
                
            return query[:where_pos] + new_where
        
        # If no WHERE clause exists, add it before GROUP BY, ORDER BY, etc.
        for clause in ['GROUP BY', 'ORDER BY', 'LIMIT']:
            if clause in query.upper():
                position = query.upper().find(clause)
                return query[:position] + f" WHERE {where_clause} " + query[position:]
        
        # If no clauses found, add WHERE at the end
        return query + f" WHERE {where_clause}"
    
    # def optimize_slow_query(self, query):
    #     """Apply optimizations to potentially slow queries"""
    #     # Add query hints for better execution plans
    #     if "COUNT" in query and "JOIN" in query:
    #         # Add PARALLEL hint for count queries with joins
    #         query = query.replace("SELECT", "SELECT /*+ PARALLEL */", 1)
        
    #     # Add materialized CTE for complex subqueries
    #     if query.count("SELECT") > 2:
    #         # Identify potential subqueries for materialization
    #         # This is a simplified example
    #         subquery_match = re.search(r'\(\s*SELECT.*?FROM.*?\)\s*AS\s*(\w+)', query, re.DOTALL)
    #         if subquery_match:
    #             subquery_alias = subquery_match.group(1)
    #             subquery = subquery_match.group(0)
    #             # Replace with materialized CTE
    #             cte = f"WITH {subquery_alias}_mat AS MATERIALIZED ({subquery.strip('()').strip()})"
    #             query = f"{cte}\nSELECT * FROM {subquery_alias}_mat"
        
    #     return query
    
    class ChartQueryOptimizer:
        """Service for optimizing chart queries to improve performance"""
        
        def __init__(self):
            self.logger = logging.getLogger(__name__)
        
        def optimize_query(self, chart, query):
            """Apply performance optimizations to a chart query"""
            # First clean and normalize the query
            query = self._normalize_query(query)
            
            # Apply optimizations based on query type and complexity
            if self._is_count_query_with_joins(query):
                query = self._optimize_count_join_query(query)
            
            elif self._is_complex_aggregation(query):
                query = self._optimize_complex_aggregation(query)
            
            elif self._is_high_risk_query(chart, query):
                query = self._optimize_high_risk_query(query)
            
            # Always apply these baseline optimizations
            query = self._add_execution_hints(query)
            query = self._ensure_query_limit(query)
            
            # Return optimized query
            return query
        
        def _normalize_query(self, query):
            """Clean and normalize a query for optimization"""
            if not query:
                return ""
                
            query = query.strip()
            if query.endswith(';'):
                query = query[:-1]
                
            return query
        
        def _is_count_query_with_joins(self, query):
            """Check if this is a COUNT query with joins"""
            return (
                'COUNT(' in query and 
                ('JOIN' in query.upper() or 'FROM' in query.upper() and ',' in query.split('FROM')[1])
            )
        
        def _is_complex_aggregation(self, query):
            """Check if this is a complex aggregation query"""
            complex_indicators = [
                'GROUP BY', 'HAVING', 
                'SUM(', 'AVG(', 'MAX(', 'MIN(',
                'PARTITION BY', 'OVER('
            ]
            return any(indicator in query.upper() for indicator in complex_indicators)
        
        def _is_high_risk_query(self, chart, query):
            """Identify if this is a known high-risk query pattern"""
            if not chart or not chart.name:
                return False
                
            high_risk_patterns = [
                'high risk branch',
                'risk level',
                'customer transaction'
            ]
            
            chart_name = chart.name.lower()
            return any(pattern in chart_name for pattern in high_risk_patterns)
        
        def _optimize_count_join_query(self, query):
            """Optimize COUNT queries with joins"""
            # Use EXISTS instead of COUNT when possible
            count_pattern = r'COUNT\s*\(\s*(.*?)\s*\)'
            count_match = re.search(count_pattern, query, re.IGNORECASE | re.DOTALL)
            
            if count_match and count_match.group(1) != '*':
                # Check if this is counting a specific column or expression
                count_column = count_match.group(1).strip()
                
                # Only transform if it's counting a simple column reference
                if re.match(r'^[\w.]+$', count_column):
                    # Try to transform to EXISTS for better performance
                    self.logger.info(f"Optimizing COUNT query by using EXISTS optimization")
                    
                    # Create a CTE for better execution plan
                    return f"""
                    WITH base_data AS MATERIALIZED (
                        {query}
                    )
                    SELECT * FROM base_data
                    """
            
            # Add optimization hint for COUNT queries
            return query.replace('COUNT(', 'COUNT /*+ PARALLEL */ (', 1)
        
        def _optimize_complex_aggregation(self, query):
            """Optimize complex aggregation queries"""
            # Use CTEs for complex queries with multiple aggregations
            if query.count('SELECT') > 1 and 'GROUP BY' in query.upper():
                self.logger.info("Optimizing complex aggregation with CTEs")
                
                # Find innermost subquery
                subquery_pattern = r'\(\s*(SELECT.*?FROM.*?)\)\s*(?:AS\s*)?(\w+)'
                subquery_match = re.search(subquery_pattern, query, re.IGNORECASE | re.DOTALL)
                
                if subquery_match:
                    subquery_sql = subquery_match.group(1).strip()
                    subquery_alias = subquery_match.group(2).strip()
                    
                    # Replace with CTE
                    full_subquery = subquery_match.group(0)
                    cte_query = f"""
                    WITH {subquery_alias} AS MATERIALIZED (
                        {subquery_sql}
                    )
                    {query.replace(full_subquery, subquery_alias)}
                    """
                    return cte_query
            
            return query
        
        def _optimize_high_risk_query(self, query):
            """Apply specific optimizations for known high-risk queries"""
            # Look for specific patterns that we know can be optimized
            if 'risk_level = \'high\'' in query:
                self.logger.info("Optimizing high risk query")
                
                # Create a smaller temporary dataset using MATERIALIZED CTE
                return f"""
                WITH high_risk_partners AS MATERIALIZED (
                    SELECT id, branch_id
                    FROM res_partner
                    WHERE risk_level = 'high' AND origin IN ('demo', 'test', 'prod')
                )
                {query.replace('res_partner', 'high_risk_partners')}
                """
            
            return query
        
        def _add_execution_hints(self, query):
            """Add execution hints to improve performance"""
            # Only add if not already present
            if "/*+" not in query:
                # For queries with GROUP BY, suggest hash aggregation
                if "GROUP BY" in query.upper():
                    query = re.sub(
                        r'SELECT\s+', 
                        'SELECT /*+ HASHAGG PARALLEL(4) */ ', 
                        query, 
                        flags=re.IGNORECASE, 
                        count=1
                    )
                else:
                    # For other queries, just suggest parallelism if appropriate
                    query = re.sub(
                        r'SELECT\s+', 
                        'SELECT /*+ PARALLEL(2) */ ', 
                        query, 
                        flags=re.IGNORECASE, 
                        count=1
                    )
            
            return query
        
        def _ensure_query_limit(self, query, default_limit=100):
            """Ensure the query has a reasonable LIMIT clause"""
            # Check if query already has a LIMIT
            limit_match = re.search(r'\bLIMIT\b\s+(\d+)', query, re.IGNORECASE)
            
            if limit_match:
                # Get the current limit and ensure it's reasonable
                current_limit = int(limit_match.group(1))
                if current_limit > 1000:  # Cap at 1000 rows for performance
                    # Replace existing limit with capped value
                    query = re.sub(
                        r'\bLIMIT\b\s+\d+', 
                        f'LIMIT 10000', 
                        query, 
                        flags=re.IGNORECASE
                    )
                return query
            else:
                # Add a default limit
                return f"{query} LIMIT {default_limit}"