import logging
import re
from odoo import tools

_logger = logging.getLogger(__name__)

class DynamicQueryBuilder:
    """Service for building dynamic SQL queries without hardcoding"""
    
    def __init__(self):
        pass
    
    def build_aggregate_query(self, table_name, aggregate_field, aggregate_func='COUNT',
                             group_by_field=None, filter_conditions=None, order_by=None, 
                             join_tables=None, limit=10):
        """
        Build a dynamic aggregate query without hardcoding
        
        Args:
            table_name (str): Main table to query
            aggregate_field (str): Field to aggregate
            aggregate_func (str): Aggregation function (COUNT, SUM, AVG, etc.)
            group_by_field (str): Field to group by
            filter_conditions (list): List of tuples (field, operator, value)
            order_by (tuple): Tuple of (field, direction)
            join_tables (list): List of dictionaries with join parameters
            limit (int): Limit results
            
        Returns:
            str: SQL query
        """
        # Validate inputs to prevent SQL injection
        if not self._is_valid_identifier(table_name):
            raise ValueError(f"Invalid table name: {table_name}")
            
        if aggregate_field != '*' and not self._is_valid_identifier(aggregate_field):
            raise ValueError(f"Invalid aggregate field: {aggregate_field}")
            
        if group_by_field and not self._is_valid_identifier(group_by_field):
            raise ValueError(f"Invalid group by field: {group_by_field}")
        
        # Start building the query
        query = "SELECT "
        
        # Add group by field if provided
        if group_by_field:
            query += f"{group_by_field}, "
        
        # Add aggregate function
        query += f"{aggregate_func}({aggregate_field}) as aggregate_value "
        
        # FROM clause
        query += f"FROM {table_name} "
        
        # Add JOINs if provided
        if join_tables and isinstance(join_tables, list):
            for join in join_tables:
                if isinstance(join, dict) and 'table' in join and 'condition' in join:
                    join_type = join.get('type', 'INNER').upper()
                    if join_type not in ('INNER', 'LEFT', 'RIGHT', 'FULL'):
                        join_type = 'INNER'
                    
                    if not self._is_valid_identifier(join['table']):
                        continue
                        
                    query += f"{join_type} JOIN {join['table']} ON {join['condition']} "
        
        # Add WHERE conditions if provided
        if filter_conditions and isinstance(filter_conditions, list):
            where_clauses = []
            
            for condition in filter_conditions:
                if len(condition) >= 3 and self._is_valid_identifier(condition[0]):
                    field, operator, value = condition[0], condition[1], condition[2]
                    
                    # Validate operator
                    if operator not in ('=', '!=', '>', '<', '>=', '<=', 'IN', 'NOT IN', 'LIKE', 'ILIKE', 'IS NULL', 'IS NOT NULL'):
                        continue
                    
                    if operator in ('IS NULL', 'IS NOT NULL'):
                        where_clauses.append(f"{field} {operator}")
                    elif operator in ('IN', 'NOT IN') and isinstance(value, (list, tuple)):
                        # Format the list for IN clause
                        formatted_values = ', '.join(str(self._format_value(v)) for v in value)
                        where_clauses.append(f"{field} {operator} ({formatted_values})")
                    else:
                        # For other operators, format the value appropriately
                        formatted_value = self._format_value(value)
                        where_clauses.append(f"{field} {operator} {formatted_value}")
            
            if where_clauses:
                query += "WHERE " + " AND ".join(where_clauses) + " "
        
        # Add GROUP BY if needed
        if group_by_field:
            query += f"GROUP BY {group_by_field} "
        
        # Add ORDER BY if provided
        if order_by and isinstance(order_by, tuple) and len(order_by) >= 2:
            field, direction = order_by[0], order_by[1].upper()
            
            if direction not in ('ASC', 'DESC'):
                direction = 'DESC'
                
            if self._is_valid_identifier(field):
                if field == group_by_field:
                    query += f"ORDER BY {field} {direction} "
                else:
                    query += f"ORDER BY aggregate_value {direction} "
        elif group_by_field:
            # Default ordering if group_by is specified but not order_by
            query += f"ORDER BY aggregate_value DESC "
        
        # Add LIMIT
        if isinstance(limit, int) and limit > 0:
            query += f"LIMIT {limit}"
        
        return query
    
    def build_branch_customers_query(self, limit=10):
        """Build a dynamic query for branch by customer count"""
        return self.build_aggregate_query(
            table_name='res_partner',
            aggregate_field='id',
            aggregate_func='COUNT',
            group_by_field='branch_id',
            filter_conditions=[
                ('branch_id', 'IS NOT', 'NULL')
            ],
            order_by=('aggregate_value', 'DESC'),
            join_tables=[
                {
                    'type': 'LEFT',
                    'table': 'res_branch',
                    'condition': 'res_partner.branch_id = res_branch.id'
                }
            ],
            limit=limit
        )
    
    def build_high_risk_branch_query(self, limit=10):
        """Build a dynamic query for high risk customers by branch"""
        return self.build_aggregate_query(
            table_name='res_partner',
            aggregate_field='id',
            aggregate_func='COUNT',
            group_by_field='branch_id',
            filter_conditions=[
                ('branch_id', 'IS NOT', 'NULL'),
                ('risk_level', '=', 'high')
            ],
            order_by=('aggregate_value', 'DESC'),
            join_tables=[
                {
                    'type': 'LEFT',
                    'table': 'res_branch',
                    'condition': 'res_partner.branch_id = res_branch.id'
                }
            ],
            limit=limit
        )
    
    def build_transaction_rules_query(self, limit=10):
        """Build a dynamic query for transaction by rules"""
        return self.build_aggregate_query(
            table_name='res_customer_transaction',
            aggregate_field='id',
            aggregate_func='COUNT',
            group_by_field='rule_id',
            filter_conditions=[
                ('rule_id', 'IS NOT', 'NULL')
            ],
            order_by=('aggregate_value', 'DESC'),
            join_tables=[
                {
                    'type': 'LEFT',
                    'table': 'res_transaction_screening_rule',
                    'condition': 'res_customer_transaction.rule_id = res_transaction_screening_rule.id'
                }
            ],
            limit=limit
        )
    
    def _is_valid_identifier(self, identifier):
        """Check if a string is a valid SQL identifier to prevent injection"""
        if not identifier or not isinstance(identifier, str):
            return False
            
        # Only allow alphanumeric characters, underscores, and periods (for table.column)
        pattern = r'^[a-zA-Z0-9_\.]+$'
        return bool(re.match(pattern, identifier))
    
    def _format_value(self, value):
        """Format a value for SQL query to prevent injection"""
        if value is None:
            return 'NULL'
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            # Escape single quotes
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        else:
            # For other types, convert to string and escape
            escaped_value = str(value).replace("'", "''")
            return f"'{escaped_value}'"
    
    def build_materialized_view_query(self, name, base_query):
        """Build a query to create or refresh a materialized view"""
        # Validate input
        if not self._is_valid_identifier(name):
            raise ValueError(f"Invalid view name: {name}")
            
        # Create the view query
        query = f"""
        DROP MATERIALIZED VIEW IF EXISTS {name};
        CREATE MATERIALIZED VIEW {name} AS
        {base_query}
        WITH DATA;
        CREATE INDEX IF NOT EXISTS {name}_idx ON {name} (id);
        """
        
        return query
    
    def build_refresh_materialized_view_query(self, name):
        """Build a query to refresh a materialized view"""
        # Validate input
        if not self._is_valid_identifier(name):
            raise ValueError(f"Invalid view name: {name}")
            
        return f"REFRESH MATERIALIZED VIEW {name};"