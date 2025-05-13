import re

class SQLParser:
    """Service for parsing SQL queries and converting to Odoo domains"""
    
    def extract_where_clauses(self, sql_query):
        """Extract all clauses attached to the WHERE statement in an SQL query, handling BETWEEN clauses properly."""
        # Normalize the query
        sql_query = ' '.join(sql_query.strip().split())
        
        # Find the WHERE clause (if any)
        where_pattern = re.compile(r'\bWHERE\b(.*?)(?:\bGROUP BY\b|\bHAVING\b|\bORDER BY\b|\bLIMIT\b|\bOFFSET\b|$)', 
                                re.IGNORECASE | re.DOTALL)
        where_match = where_pattern.search(sql_query)
        
        if not where_match:
            return []
        
        where_content = where_match.group(1).strip()
        
        if not where_content:
            return []
        
        # Parse the WHERE clause efficiently
        return self._parse_where_content(where_content)
    
    def _parse_where_content(self, where_content):
        """Parse WHERE clause content efficiently"""
        clauses = []
        i = 0
        current_clause = ""
        paren_level = 0
        in_between = False
        in_quotes = False
        quote_char = None
        
        while i < len(where_content):
            char = where_content[i]
            current_clause += char
            
            # Handle quotes - track when we're inside a string literal
            if char in ["'", '"'] and (i == 0 or where_content[i-1] != '\\'):
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                    quote_char = None
            
            # Only process special characters if we're not inside quotes
            if not in_quotes:
                if char == '(':
                    paren_level += 1
                elif char == ')':
                    paren_level -= 1
                
                # Check if we're starting/ending a BETWEEN clause
                if paren_level == 0:
                    if re.search(r'\bBETWEEN\b\s*$', current_clause, re.IGNORECASE):
                        in_between = True
                    elif in_between and self._is_between_end(where_content, i):
                        in_between = False
                
                # Only split at top-level AND/OR operators that are not part of BETWEEN
                if paren_level == 0 and not in_between:
                    and_match = re.search(r'\bAND\b\s*$', current_clause, re.IGNORECASE)
                    or_match = re.search(r'\bOR\b\s*$', current_clause, re.IGNORECASE)
                    
                    if and_match:
                        clauses.append(current_clause[:-len(and_match.group(0))].strip())
                        current_clause = ""
                    elif or_match:
                        clauses.append(current_clause[:-len(or_match.group(0))].strip())
                        current_clause = ""
            
            i += 1
        
        if current_clause.strip():
            clauses.append(current_clause.strip())
        
        return clauses
    
    def _is_between_end(self, content, position):
        """Check if position is at the end of a BETWEEN clause"""
        if position < 3 or position >= len(content):
            return False
            
        # Check if we just processed an AND part of BETWEEN...AND...
        if content[position-3:position+1].upper() == ' AND':
            j = position + 1
            # Skip whitespace
            while j < len(content) and content[j].isspace():
                j += 1
            
            # Handle quoted or non-quoted value
            if j < len(content):
                if content[j] in ["'", '"']:
                    # Skip until end of quoted string
                    quote = content[j]
                    j += 1
                    while j < len(content) and content[j] != quote:
                        j += 1
                    if j < len(content):  # Found closing quote
                        j += 1
                else:
                    # Skip until end of non-quoted value
                    while j < len(content) and content[j] not in [' ', '\t', '\n']:
                        j += 1
            
            # Skip whitespace after value
            while j < len(content) and content[j].isspace():
                j += 1
            
            # Check if next token is AND/OR (end of BETWEEN clause)
            if j < len(content):
                next_token = content[j:j+5].upper()
                return next_token.startswith('AND ') or next_token.startswith('OR ')
        
        return False
    
    def clean_sql_value(self, value):
        """Clean SQL values by removing quotes and converting special values."""
        value = value.strip()
        # Remove quotes if present
        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            value = value[1:-1]
        # Convert SQL NULL to Python None/False
        elif value.upper() == 'NULL':
            value = False
        # Convert numbers
        elif value.isdigit():
            value = int(value)
        elif re.match(r'^-?\d+(\.\d+)?$', value):
            value = float(value)
        return value

    def sql_where_to_odoo_domain(self, sql_query, exclude_date_fields=True):
        """Convert SQL WHERE conditions to Odoo domain format"""
        clauses = self.extract_where_clauses(sql_query)
        if not clauses:
            return []
        
        domain = []
        date_related_keywords = ['date', 'date_created', 'date_create', 'create_date', 'write_date', 'time', 'datetime'] if exclude_date_fields else []
        
        # Extract table aliases from the SQL query
        table_aliases = self._extract_table_aliases(sql_query)
        
        # Define field mappings based on table and field patterns
        field_patterns = {
            'branch': {  # This covers any alias for res_branch table
                'id': 'branch_id'  # Map any_branch_alias.id to branch_id
            },
            # Add other tables as needed
        }
        
        for clause in clauses:
            # Skip date-related fields if needed
            if exclude_date_fields and any(date_keyword in clause.lower() for date_keyword in date_related_keywords):
                continue
            
            # Map the clause to an Odoo domain operation
            domain_operation = self._map_clause_to_domain(clause, table_aliases, field_patterns)
            if domain_operation:
                domain.append(domain_operation)
        
        return domain
    
    def _map_clause_to_domain(self, clause, table_aliases, field_patterns):
        """Map a single SQL clause to an Odoo domain operation"""
        # Handle different operators
        operators = {
            ' = ': '=',
            ' >= ': '>=',
            ' <= ': '<=',
            ' > ': '>',
            ' < ': '<',
            ' LIKE ': 'ilike',
            ' IN ': 'in',
            ' IS NULL': '=',
            ' IS NOT NULL': '!='
        }
        
        for sql_op, odoo_op in operators.items():
            if sql_op.upper() in clause.upper():
                parts = clause.upper().split(sql_op.upper(), 1)
                field = parts[0].strip()
                value = parts[1].strip() if len(parts) > 1 else False
                
                field_name = self._map_field_name(field.lower(), table_aliases, field_patterns)
                
                if sql_op.upper() == ' LIKE ':
                    value = self.clean_sql_value(value)
                    value = value.replace('%', '*')
                    return (field_name, odoo_op, value)
                elif sql_op.upper() == ' IN ':
                    if value.startswith('(') and value.endswith(')'):
                        value_list = value[1:-1]
                        values = [self.clean_sql_value(v.strip()) for v in value_list.split(',')]
                        return (field_name, odoo_op, values)
                elif sql_op.upper() == ' IS NULL':
                    return (field_name, odoo_op, False)
                elif sql_op.upper() == ' IS NOT NULL':
                    return (field_name, odoo_op, False)
                else:
                    value = self.clean_sql_value(value)
                    return (field_name, odoo_op, value)
        
        # Handle BETWEEN clause
        if 'BETWEEN' in clause.upper() and ' AND ' in clause.upper():
            parts = re.split(r'\bBETWEEN\b', clause, flags=re.IGNORECASE)
            if len(parts) == 2:
                field = parts[0].strip()
                field_name = self._map_field_name(field, table_aliases, field_patterns)
                between_parts = re.split(r'\bAND\b', parts[1], flags=re.IGNORECASE, maxsplit=1)
                if len(between_parts) == 2:
                    start_val = self.clean_sql_value(between_parts[0])
                    end_val = self.clean_sql_value(between_parts[1])
                    return ['&', (field_name, '>=', start_val), (field_name, '<=', end_val)]
        
        return None
    
    def _extract_table_aliases(self, sql_query):
        """Extract table aliases from SQL query.
        Returns a dictionary mapping aliases to table names."""
        aliases = {}
        
        # Normalize query and convert to lowercase for easier parsing
        sql_query = ' '.join(sql_query.strip().split()).lower()
        
        # Extract FROM clause
        from_match = re.search(r'\bfrom\b(.*?)(?:\bwhere\b|\bjoin\b|\bgroup by\b|\bhaving\b|\border by\b|\blimit\b|\boffset\b|$)', 
                            sql_query, re.IGNORECASE | re.DOTALL)
        if from_match:
            from_clause = from_match.group(1).strip()
            # Extract table name and alias
            table_match = re.search(r'(\w+)(?:\s+as)?\s+(\w+)', from_clause, re.IGNORECASE)
            if table_match:
                table_name, alias = table_match.group(1), table_match.group(2)
                aliases[alias] = table_name
        
        # Extract JOIN clauses
        join_pattern = re.compile(r'\bjoin\b\s+(\w+)(?:\s+as)?\s+(\w+)', re.IGNORECASE)
        for match in join_pattern.finditer(sql_query):
            table_name, alias = match.group(1), match.group(2)
            aliases[alias] = table_name
        
        return aliases
    
    def _map_field_name(self, field, table_aliases, field_patterns):
        """Map a table-prefixed field to the appropriate Odoo field name."""
        if '.' not in field:
            return field  # No table prefix, return as is
        
        alias, field_name = field.split('.', 1)
        
        # If we have an alias match
        if alias in table_aliases:
            table_name = table_aliases[alias]
            
            # Look for table patterns (e.g., if table contains 'branch')
            for pattern, field_mappings in field_patterns.items():
                if pattern in table_name and field_name in field_mappings:
                    return field_mappings[field_name]
        
        # Default fallback: return just the field part
        return field_name
    
    def add_where_to_query(self, query, where_clause):
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