# -*- coding: utf-8 -*-

import logging
import re

_logger = logging.getLogger(__name__)

class QueryService:
    """Service for SQL query manipulation and validation."""

    @staticmethod
    def validate_query_syntax(query):
        """Thoroughly validate SQL query syntax before execution.

        Args:
            query (str): The SQL query to validate.

        Returns:
            tuple: A tuple containing a boolean indicating validity and an error message if invalid.
        """
        try:
            query = re.sub(r"--.*?$", "", query, flags=re.MULTILINE)
            query = re.sub(r"/\*.*?\*/", "", query, flags=re.DOTALL)
            error_patterns = [
                (r"WHERE\s+WHERE", "Duplicate WHERE clause"),
                (r"AND\s+WHERE", "Invalid AND WHERE sequence"),
                (r"\(\s*WHERE", "WHERE inside parentheses without SELECT/FROM"),
                (r"WHERE\s*\)", "WHERE followed directly by closing parenthesis"),
                (r"WHERE\s+OR\b", "WHERE followed directly by OR"),
                (r"WHERE\s+ORDER", "WHERE followed directly by ORDER"),
                (r"WHERE\s+GROUP", "WHERE followed directly by GROUP"),
                (r"WHERE\s+HAVING", "WHERE followed directly by HAVING"),
                (r"AND\s+OR\b", "Mixed AND OR without parentheses"),
                (r"OR\s+AND\b", "Mixed OR AND without parentheses"),
                (r"WHERE\s*$", "WHERE at end of query without conditions"),
                (r"WHERE\s+SELECT", "WHERE followed by SELECT without comparison"),
                (r"SELECT\s+FROM\s+WHERE", "FROM followed directly by WHERE"),
                (r"FROM\s+WHERE\s+\w+", "FROM WHERE sequence (missing table)"),
                (r"\.\s*IN\s*\(", "Potential syntax error with IN clause"),
                (r"\.\s*WHERE", "Table.WHERE syntax error"),
            ]
            for pattern, error in error_patterns:
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    return False, f"SQL syntax error: {error} at '{match.group(0)}'"
            if query.count("(") != query.count(")"):
                return False, "Unbalanced parentheses in query"
            subquery_pattern = r"\(\s*SELECT.*?FROM.*?\)"
            subqueries = re.finditer(subquery_pattern, query, re.IGNORECASE | re.DOTALL)
            for match in subqueries:
                subquery = match.group(0)
                for pattern, error in error_patterns:
                    submatch = re.search(pattern, subquery, re.IGNORECASE)
                    if submatch:
                        return (
                            False,
                            f"Subquery syntax error: {error} at '{submatch.group(0)}'",
                        )
            return True, "Query syntax appears valid"
        except Exception as e:
            return False, f"Query validation error: {str(e)}"

    @staticmethod
    def is_safe_query(query):
        """Check if a query is safe to execute.

        Args:
            query (str): The SQL query to check.

        Returns:
            bool: True if the query is safe, False otherwise.
        """
        if not query:
            return False
        if ";" in query and not query.strip().endswith(";"):
            return False
        unsafe_commands = [
            "UPDATE",
            "DELETE",
            "INSERT",
            "ALTER",
            "DROP",
            "TRUNCATE",
            "CREATE",
            "GRANT",
            "REVOKE",
            "SET ROLE",
        ]
        for cmd in unsafe_commands:
            if re.search(r"\b" + cmd + r"\b", query, re.IGNORECASE):
                return False
        return True

    @staticmethod
    def add_condition_to_query(query, condition):
        """Add a WHERE condition to an SQL query.

        Args:
            query (str): The SQL query to modify.
            condition (str): The condition to add.

        Returns:
            str: The modified SQL query with the new condition.
        """
        query = query.strip()
        if query.endswith(";"):
            query = query[:-1]
        if condition in query:
            return query
        if not condition or not condition.strip():
            return query
        condition = condition.strip()
        if condition.upper().startswith("WHERE "):
            condition = condition[6:].strip()
        has_where = bool(re.search(r"\bWHERE\b", query, re.IGNORECASE))
        if has_where:
            for clause in ["GROUP BY", "ORDER BY", "LIMIT", "OFFSET", "HAVING"]:
                clause_pattern = r"\b" + clause + r"\b"
                clause_match = re.search(clause_pattern, query, re.IGNORECASE)
                if clause_match:
                    position = clause_match.start()
                    return query[:position] + f" AND ({condition}) " + query[position:]
            return query + f" AND ({condition})"
        else:
            for clause in ["GROUP BY", "ORDER BY", "LIMIT", "OFFSET", "HAVING"]:
                clause_pattern = r"\b" + clause + r"\b"
                clause_match = re.search(clause_pattern, query, re.IGNORECASE)
                if clause_match:
                    position = clause_match.start()
                    return (
                        query[:position] + f" WHERE ({condition}) " + query[position:]
                    )
            return query + f" WHERE ({condition})"

    @staticmethod
    def extract_main_table(query):
        """Extract the main table name from an SQL query.

        Args:
            query (str): The SQL query to extract the table from.

        Returns:
            str: The name of the main table, or None if not found.
        """
        from_match = re.search(r"\bfrom\s+([a-zA-Z0-9_\.]+)", query, re.IGNORECASE)
        if from_match:
            return from_match.group(1).strip()
        return None

    @staticmethod
    def convert_inner_joins_to_left_joins(query):
        """Convert INNER JOINs to LEFT JOINs in a query.

        Args:
            query (str): The SQL query to convert.

        Returns:
            str: The converted SQL query.
        """
        enhanced_query = query
        enhanced_query = re.sub(
            r"\bJOIN\b", "LEFT JOIN", enhanced_query, flags=re.IGNORECASE
        )
        _logger.info(f"Converted INNER JOINs to LEFT JOINs for inclusive branch coverage")
        return enhanced_query

    @staticmethod
    def get_friendly_error_message(error_msg):
        """Convert technical SQL errors to user-friendly messages.

        Args:
            error_msg (str): The raw error message from SQL execution.

        Returns:
            str: A user-friendly error message.
        """
        if "syntax error" in error_msg.lower():
            return "SQL syntax error. Please check your query format."
        elif "timeout" in error_msg.lower():
            return "Query timed out. Please simplify your query or enable the materialized view option."
        elif "does not exist" in error_msg.lower():
            if "column" in error_msg.lower():
                column = re.search(r'column\s+"([^"]+)"', error_msg)
                if column:
                    return f"Column '{column.group(1)}' does not exist. Please check field names."
            elif "relation" in error_msg.lower():
                table = re.search(r'relation\s+"([^"]+)"', error_msg)
                if table:
                    return f"Table '{table.group(1)}' does not exist. Please check table names."
            return "Referenced column or table does not exist. Please check your query."
        else:
            return f"Database error: {error_msg}"

    @staticmethod
    def find_column_in_view(field_name, column_names):
        """Find the most appropriate column name in a materialized view.

        Args:
            field_name (str): The field name to find.
            column_names (list): The list of column names in the materialized view.

        Returns:
            str: The most appropriate column name if found, otherwise None.
        """
        original_field = field_name
        if "." in field_name:
            _, field_name = field_name.split(".", 1)
        if field_name in column_names:
            _logger.debug(f"Found exact column match: {field_name} for {original_field}")
            return field_name
        field_lower = field_name.lower()
        for col in column_names:
            if col.lower() == field_lower:
                _logger.debug(f"Found case-insensitive match: {col} for {original_field}")
                return col
        for col in column_names:
            if field_lower in col.lower():
                _logger.debug(f"Found partial match: {col} for {original_field}")
                return col
        if field_lower.endswith("_id"):
            base_name = field_lower[:-3]
            for col in column_names:
                if col.lower() == base_name or col.lower().startswith(base_name):
                    _logger.debug(f"Found match without '_id' suffix: {col} for {original_field}")
                    return col
        if field_lower == "id" and "branch_id" in column_names:
            _logger.debug(f"Found special case match 'branch_id' for 'id' field")
            return "branch_id"
        _logger.warning(f"Could not find column match for {original_field} in columns: {column_names}")
        return None

    @staticmethod
    def find_branch_column_in_view(columns, preferred_field=None):
        """Dynamically find branch column from view columns using the chart's configured field.

        Args:
            columns (list): The list of columns in the view.
            preferred_field (str, optional): The preferred field to look for.

        Returns:
            str: The name of the branch column if found, otherwise None.
        """
        _logger.info(
            f"Looking for branch column in {columns} with preferred_field: {preferred_field}"
        )
        if preferred_field:
            field_name = (
                preferred_field.split(".")[-1]
                if "." in preferred_field
                else preferred_field
            )
            if field_name in columns:
                _logger.info(f"Found exact match: {field_name}")
                return field_name
            for col in columns:
                if col.lower() == field_name.lower():
                    _logger.info(f"Found case-insensitive match: {col}")
                    return col
        branch_patterns = ["branch_id", "id", "branch"]
        for pattern in branch_patterns:
            if pattern in columns:
                _logger.info(f"Found fallback pattern '{pattern}': {pattern}")
                return pattern
        for col in columns:
            if "branch" in col.lower():
                _logger.info(f"Found branch-related column: {col}")
                return col
        _logger.warning(f"No branch column found in {columns}")
        return None

    @staticmethod
    def find_sort_column_in_view(columns, preferred_field=None):
        """Dynamically find sort column from view columns using the chart's configured field.

        Args:
            columns (list): The list of columns in the view.
            preferred_field (str, optional): The preferred field to look for.

        Returns:
            str: The name of the sort column if found, otherwise None.
        """
        if preferred_field:
            field_name = (
                preferred_field.split(".")[-1]
                if "." in preferred_field
                else preferred_field
            )
            if field_name in columns:
                return field_name
            for col in columns:
                if col.lower() == field_name.lower():
                    return col
        value_patterns = ["count", "amount", "total", "sum", "value", "risk", "hit"]
        for col in columns:
            col_lower = col.lower()
            for pattern in value_patterns:
                if pattern in col_lower:
                    return col
        return None

    @staticmethod
    def parse_condition_string(condition_string):
        """Parse SQL WHERE conditions into Odoo domain format.

        Args:
            condition_string (str): The condition string to parse.

        Returns:
            list: A list representing the Odoo domain.
        """
        if not condition_string:
            return []
        condition_string = condition_string.strip()

        def parse_expression(expr, depth=0):
            _logger.info(f"Parsing expression (depth {depth}): {expr}")
            if not expr.strip():
                return []
            
            or_parts = QueryService._split_by_operator(expr, "OR")
            if len(or_parts) > 1:
                result = []
                for i in range(len(or_parts) - 1):
                    result.append("|")
                for part in or_parts:
                    result.extend(parse_expression(part, depth + 1))
                return result
            
            and_parts = QueryService._split_by_operator(expr, "AND")
            if len(and_parts) > 1:
                result = []
                for part in and_parts[:-1]:
                    result.append("&")
                    result.extend(parse_expression(part, depth + 1))
                result.extend(parse_expression(and_parts[-1], depth + 1))
                return result
            
            if expr.strip().startswith("(") and expr.strip().endswith(")"):
                inner_expr = expr.strip()[1:-1].strip()
                return parse_expression(inner_expr, depth + 1)
            
            return [QueryService._parse_single_condition(expr)]

        try:
            domain = parse_expression(condition_string)
            _logger.info(f"Parsed domain: {domain}")
            return domain
        except Exception as e:
            _logger.error(f"Error parsing condition: {e}")
            return []

    @staticmethod
    def _split_by_operator(expr, operator):
        """Split a SQL expression by an operator while respecting parentheses.

        Args:
            expr (str): The SQL expression to split.
            operator (str): The operator to split by (AND/OR).

        Returns:
            list: A list of split expressions.
        """
        operator = f" {operator} "
        parts = []
        current_part = ""
        paren_level = 0
        quote_char = None
        i = 0
        while i < len(expr):
            char = expr[i]
            if char in ["'", '"'] and (i == 0 or expr[i - 1] != "\\"):
                if quote_char is None:
                    quote_char = char
                elif quote_char == char:
                    quote_char = None
            if quote_char is not None:
                current_part += char
                i += 1
                continue
            if char == "(":
                paren_level += 1
            elif char == ")":
                paren_level -= 1
            if (
                paren_level == 0
                and i + len(operator) <= len(expr)
                and expr[i : i + len(operator)].upper() == operator
            ):
                parts.append(current_part.strip())
                current_part = ""
                i += len(operator)
            else:
                current_part += char
                i += 1
        if current_part:
            parts.append(current_part.strip())
        return parts

    @staticmethod
    def _parse_single_condition(condition):
        """Parse a single SQL condition into an Odoo domain tuple.

        Args:
            condition (str): The condition string to parse.

        Returns:
            tuple: A tuple representing the Odoo domain condition.
        """
        condition = condition.strip()
        is_true_match = re.search(r"(\w+)\s+is\s+true", condition.lower())
        if is_true_match:
            field = is_true_match.group(1).strip()
            return (field, "=", True)
        is_false_match = re.search(r"(\w+)\s+is\s+false", condition.lower())
        if is_false_match:
            field = is_false_match.group(1).strip()
            return (field, "=", False)
        if " is null" in condition.lower():
            field = condition.lower().split(" is null")[0].strip()
            return (field, "=", False)
        if " is not null" in condition.lower():
            field = condition.lower().split(" is not null")[0].strip()
            return (field, "!=", False)
        if " like " in condition.lower():
            parts = condition.lower().split(" like ")
            field = parts[0].strip()
            value = parts[1].strip().strip("'\"")
            return (field, "ilike", value.replace("%", ""))
        ops_map = {
            "=": "=",
            ">": ">",
            ">=": ">=",
            "<": "<",
            "<=": "<=",
            "!=": "!=",
            "<>": "!=",
        }
        for op in ops_map.keys():
            if f" {op} " in condition:
                parts = condition.split(f" {op} ", 1)
                field = parts[0].strip()
                value = parts[1].strip().strip("'\"")
                if value.lower() == "true":
                    value = True
                elif value.lower() == "false":
                    value = False
                elif value.isdigit():
                    value = int(value)
                elif value.replace(".", "", 1).isdigit():
                    value = float(value)
                return (field, ops_map[op], value)
        _logger.warning(f"Could not parse condition: {condition}")
        return (condition, "=", True)

    @staticmethod
    def _extract_quoted_value(value_str):
        """Extract a value from quotes.

        Args:
            value_str (str): The string to extract the value from.

        Returns:
            str: The extracted value without quotes.
        """
        if (value_str.startswith("'") and value_str.endswith("'")) or (
            value_str.startswith('"') and value_str.endswith('"')
        ):
            return value_str[1:-1]
        return value_str

    @staticmethod
    def _parse_value(value_str):
        """Parse a value string into the appropriate Python type.

        Args:
            value_str (str): The string representing the value.

        Returns:
            any: The parsed value in its appropriate type.
        """
        if (value_str.startswith("'") and value_str.endswith("'")) or (
            value_str.startswith('"') and value_str.endswith('"')
        ):
            return value_str[1:-1]
        if value_str.isdigit():
            return int(value_str)
        try:
            return float(value_str)
        except ValueError:
            pass
        if value_str.upper() == "TRUE":
            return True
        if value_str.upper() == "FALSE":
            return False
        return value_str
    