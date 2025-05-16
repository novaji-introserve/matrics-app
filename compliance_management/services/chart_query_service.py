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
                    f'LIMIT 1000', 
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