# -*- coding: utf-8 -*-

from odoo import http
from odoo.http import request
from datetime import datetime, timedelta
from odoo import fields
import re
from ..utils.cache_key_unique_identifier import (
    get_unique_client_identifier,
    normalize_cache_key_components,
)
import logging
from ..services.branch_security import ChartSecurityService

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

        This sets up the ChartSecurityService for managing user permissions.
        """
        super(Compliance, self).__init__()
        self.security_service = ChartSecurityService()

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
        if not isinstance(branches_id, list):
            branches_id = [branches_id]
            return branches_id
        else:
            return branches_id

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
        lower_query = sql_query.lower()
        table = None
        domain = []
        if re.search(r"\b(?:sum|avg|min|max)\s*\(", lower_query):
            return None
        from_match = re.search(r"\bfrom\s+([\w.]+)", lower_query)
        if from_match:
            table = from_match.group(1)
        else:
            join_match = re.search(
                r"\b(?:inner|left|right|full outer)?\s+join\s+([\w.]+)", lower_query
            )
            if join_match:
                return None
        where_match = re.search(
            r"\bwhere\s+(.+?)(?:\s+(?:group\s+by|order\s+by|limit|having)\s+|\s*$)",
            lower_query,
            re.DOTALL,
        )
        if where_match:
            condition_string = where_match.group(1).strip()
            domain = self.parse_condition_string(condition_string)
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
        else:
            pass
        _logger.info(f"Final domain: {domain}")
        return {"table": table, "domain": domain}

    def parse_condition_string(self, condition_string):
        """
        Parse SQL WHERE conditions into Odoo domain format.

        This method handles AND, OR operators and parentheses for correct domain parsing.

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
            or_parts = self._split_by_operator(expr, "OR")
            if len(or_parts) > 1:
                result = []
                for i in range(len(or_parts) - 1):
                    result.append("|")
                for part in or_parts:
                    result.extend(parse_expression(part, depth + 1))
                return result
            and_parts = self._split_by_operator(expr, "AND")
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
            return [self._parse_single_condition(expr)]

        try:
            domain = parse_expression(condition_string)
            _logger.info(f"Parsed domain: {domain}")
            return domain
        except Exception as e:
            _logger.error(f"Error parsing condition: {e}")
            return []

    def _split_by_operator(self, expr, operator):
        """
        Split a SQL expression by an operator while respecting parentheses.

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

    def _parse_single_condition(self, condition):
        """
        Parse a single SQL condition into an Odoo domain tuple.

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

    def _convert_to_odoo_tuple(self, condition: str):
        """
        Convert a simple condition string to an Odoo domain tuple.

        Args:
            condition (str): The condition string to convert.

        Returns:
            list: A list containing the Odoo domain tuple.
        """
        condition = condition.strip()
        if condition.startswith("(") and condition.endswith(")"):
            condition = condition[1:-1].strip()
        lower_condition = condition.lower()
        if " is true" in lower_condition:
            field = lower_condition.split(" is true")[0].strip()
            return [(field, "=", True)]
        if " is false" in lower_condition:
            field = lower_condition.split(" is false")[0].strip()
            return [(field, "=", False)]
        if " = true" in lower_condition:
            field = lower_condition.split(" = true")[0].strip()
            return [(field, "=", True)]
        if " = false" in lower_condition:
            field = lower_condition.split(" = false")[0].strip()
            return [(field, "=", False)]
        if " is null" in lower_condition:
            field = lower_condition.split(" is null")[0].strip()
            return [(field, "=", False)]
        if " is not null" in lower_condition:
            field = lower_condition.split(" is not null")[0].strip()
            return [(field, "!=", False)]
        if " like " in lower_condition:
            parts = (
                condition.split(" like ", 1)
                if " like " in lower_condition
                else condition.split(" LIKE ", 1)
            )
            field = parts[0].strip()
            value = self._extract_quoted_value(parts[1].strip())
            return [(field, "=like", value)]
        if " in " in lower_condition:
            parts = (
                condition.split(" in ", 1)
                if " in " in lower_condition
                else condition.split(" IN ", 1)
            )
            field = parts[0].strip()
            values_str = parts[1].strip()
            if values_str.startswith("(") and values_str.endswith(")"):
                values_str = values_str[1:-1].strip()
            values = []
            for val in values_str.split(","):
                val = val.strip()
                if (val.startswith("'") and val.endswith("'")) or (
                    val.startswith('"') and val.endswith('"')
                ):
                    values.append(val[1:-1])
                elif val.isdigit():
                    values.append(int(val))
                elif val.replace(".", "", 1).isdigit():
                    values.append(float(val))
                else:
                    values.append(val)
            return [(field, "in", values)]
        for op in ["!=", ">=", "<=", "=", ">", "<"]:
            if f" {op} " in condition:
                parts = condition.split(f" {op} ", 1)
                field = parts[0].strip()
                value = self._parse_value(parts[1].strip())
                return [(field, op, value)]
        words = condition.strip().split()
        if len(words) == 1:
            return [(words[0], "=", True)]
        if "true" in lower_condition.split():
            parts = lower_condition.split()
            field_index = parts.index("true") - 1 if "true" in parts else 0
            if field_index >= 0:
                return [(parts[field_index], "=", True)]
        if "false" in lower_condition.split():
            parts = lower_condition.split()
            field_index = parts.index("false") - 1 if "false" in parts else 0
            if field_index >= 0:
                return [(parts[field_index], "=", False)]
        return [(condition, "=", True)]

    def _extract_quoted_value(self, value_str: str):
        """
        Extract a value from quotes.

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

    def _parse_value(self, value_str: str):
        """
        Parse a value string into the appropriate Python type.

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

    def extract_main_table(self, sql_query):
        """
        Extract the main table name from an SQL query.

        Args:
            sql_query (str): The SQL query to extract the table from.

        Returns:
            str: The name of the main table, or None if not found.
        """
        from_match = re.search(r"\bfrom\s+([a-zA-Z0-9_\.]+)", sql_query, re.IGNORECASE)
        if from_match:
            return from_match.group(1).strip()
        return None

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
        unique_id = get_unique_client_identifier()
        cco_str, branches_str, datepicked_str, unique_id = (
            normalize_cache_key_components(cco, branches_id, datepicked, unique_id)
        )
        cache_key = f"all_stats_{cco_str}_{branches_str}_{datepicked_str}_{unique_id}"
        _logger.info(f"This is the stats cache key: {cache_key}")
        cache_data = request.env["res.dashboard.cache"].get_cache(cache_key, user_id)
        if cache_data:
            return cache_data
        excluded_tables = ["res_branch", "res_risk_universe"]
        if not cco:
            branches_array = self.check_branches_id(branches_id)
            if not branches_array:
                return {"data": [], "total": 0}
        query = """
            SELECT rcs.*
            FROM res_compliance_stat rcs
            WHERE rcs.state = 'active'
            ORDER BY rcs.id
        """
        request.env.cr.execute(query)
        columns = [desc[0] for desc in request.env.cr.description]
        stat_records = [dict(zip(columns, row)) for row in request.env.cr.fetchall()]
        computed_results = []
        for stat in stat_records:
            with request.env.registry.cursor() as cr:
                try:
                    stat_id = stat["id"]
                    view_name = f"stat_view_{stat_id}"
                    result_value = None
                    use_view = stat.get("use_materialized_view", False)
                    if use_view:
                        cr.execute(
                            """
                            SELECT EXISTS (
                                SELECT FROM pg_catalog.pg_class c
                                WHERE c.relname = %s AND c.relkind = 'm'
                            )
                        """,
                            (view_name,),
                        )
                        view_exists = cr.fetchone()[0]
                        if view_exists:
                            cr.execute(f"SELECT * FROM {view_name} LIMIT 0")
                            view_columns = [desc[0] for desc in cr.description]
                            filter_query = f"SELECT * FROM {view_name}"
                            if not cco and branches_id:
                                original_query = stat["sql_query"].lower()
                                main_table = self.extract_main_table(original_query)
                                # if main_table in excluded_tables:
                                if not cco and main_table in excluded_tables:
                                    continue
                                branch_column = self._find_branch_column_dynamically(
                                    cr, view_columns, main_table
                                )
                                if branch_column:
                                    branches_array = list(map(int, branches_id))
                                    if branches_array:
                                        if len(branches_array) == 1:
                                            filter_query += f" WHERE {branch_column} = {branches_array[0]}"
                                        else:
                                            filter_query += f" WHERE {branch_column} IN {tuple(branches_array)}"
                                    else:
                                        continue
                            try:
                                cr.execute(f"{filter_query} LIMIT 1")
                                result_row = cr.fetchone()
                                if result_row:
                                    result_value = result_row[0] if result_row else 0
                            except Exception as view_error:
                                _logger.warning(
                                    f"Error querying view for stat {stat_id}: {view_error}"
                                )
                    if result_value is None:
                        original_query = stat["sql_query"]
                        query = original_query.lower()
                        main_table = self.extract_main_table(query)
                        # if main_table in excluded_tables:
                        if not cco and main_table in excluded_tables:
                            continue
                        needs_modification = False
                        has_branch_id = False
                        branch_column_name = None
                        has_res_partner = (
                            re.search(r"\bres_partner\b", query, re.IGNORECASE)
                            is not None
                        )
                        if main_table:
                            branch_column_name = self._check_table_for_branch_column(
                                cr, main_table
                            )
                            has_branch_id = bool(branch_column_name)
                        if has_res_partner or has_branch_id:
                            needs_modification = True
                            if query.endswith(";"):
                                query = query[:-1]
                                original_query = original_query[:-1]
                            has_where = bool(re.search(r"\bwhere\b", query))
                            conditions = []
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
                            if has_res_partner:
                                conditions.append("origin IN ('demo','test','prod')")
                            if conditions:
                                if has_where:
                                    condition_str = " AND " + " AND ".join(conditions)
                                else:
                                    condition_str = " WHERE " + " AND ".join(conditions)
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
        result = {"data": computed_results, "total": len(computed_results)}
        request.env["res.dashboard.cache"].set_cache(cache_key, result, user_id)
        return result

    def _find_branch_column_dynamically(self, cr, columns, table_name=None):
        """
        Find the branch column from a list of columns using intelligent detection.

        Args:
            cr: The database cursor for executing queries.
            columns (list): The list of columns to search through.
            table_name (str): The name of the table being checked.

        Returns:
            str: The name of the branch column if found, otherwise None.
        """
        if "branch_id" in columns:
            return "branch_id"
        if table_name:
            cr.execute(
                """
                SELECT column_name 
                FROM information_schema.columns
                WHERE table_name = %s 
                AND column_name IN %s
                AND (column_name LIKE '%%branch%%' OR column_name LIKE '%%_id')
                ORDER BY 
                    CASE 
                        WHEN column_name = 'branch_id' THEN 1
                        WHEN column_name LIKE '%%branch%%' THEN 2
                        ELSE 3
                    END
                LIMIT 1
            """,
                (table_name, tuple(columns)),
            )
            result = cr.fetchone()
            if result:
                return result[0]
        return None

    def _check_table_for_branch_column(self, cr, table_name):
        """
        Check if a table has a branch-related column.

        Args:
            cr: The database cursor for executing queries.
            table_name (str): The name of the table to check.

        Returns:
            str: The name of the branch column if found, otherwise None.
        """
        if "." in table_name:
            schema, table = table_name.split(".")
            query = """
                SELECT column_name 
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s 
                AND (column_name = 'branch_id' OR column_name LIKE '%%branch%%')
                ORDER BY CASE WHEN column_name = 'branch_id' THEN 1 ELSE 2 END
                LIMIT 1
            """
            cr.execute(query, (schema, table))
        else:
            query = """
                SELECT column_name 
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s 
                AND (column_name = 'branch_id' OR column_name LIKE '%%branch%%')
                ORDER BY CASE WHEN column_name = 'branch_id' THEN 1 ELSE 2 END
                LIMIT 1
            """
            cr.execute(query, (table_name,))
        result = cr.fetchone()
        return result[0] if result else None

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
        today = datetime.now().date()
        prevDate = today - timedelta(days=datepicked)
        start_of_prev_day = fields.Datetime.to_string(
            datetime.combine(prevDate, datetime.min.time())
        )
        end_of_today = fields.Datetime.to_string(
            datetime.combine(today, datetime.max.time())
        )
        is_cco = self.security_service.is_cco_user()
        is_co = self.security_service.is_co_user()
        if is_co or is_cco:
            cco = True
            if is_co:
                _logger.info(
                    f"CO user {request.env.user.id} accessing stats by category with CCO privileges"
                )
        branches_array = list(map(int, branches_id)) if branches_id else []
        if cco:
            results = request.env["res.compliance.stat"].search(
                [
                    ("create_date", ">=", start_of_prev_day),
                    ("create_date", "<", end_of_today),
                    ("scope", "=", category),
                ]
            )
            computed_results = []
            for result in results:
                original_query = result["sql_query"]
                query = original_query.lower()
                needs_modification = False
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
                    if "res_partner" in query or "res.partner" in query:
                        conditions.append("origin IN ('demo','test','prod')")
                    if conditions:
                        if has_where:
                            condition_str = " AND " + " AND ".join(conditions)
                        else:
                            condition_str = " WHERE " + " AND ".join(conditions)
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
                    request.env.cr.execute(original_query)
                else:
                    request.env.cr.execute(original_query)
                result_value = (
                    request.env.cr.fetchone()[0] if request.env.cr.rowcount > 0 else 0
                )
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
            query = """
                SELECT rcs.*
                FROM res_compliance_stat rcs
                WHERE rcs.create_date >= %s
                AND rcs.create_date < %s AND rcs.scope = %s;
            """
            request.env.cr.execute(query, (start_of_prev_day, end_of_today, category))
            columns = [desc[0] for desc in request.env.cr.description]
            stat_records = [
                dict(zip(columns, row)) for row in request.env.cr.fetchall()
            ]
            computed_results = []
            for stat in stat_records:
                original_query = stat["sql_query"]
                query = original_query.lower()
                needs_modification = False
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
                    if branches_array:
                        conditions.append(f"branch_id IN {tuple(branches_array)}")
                    else:
                        conditions.append("1=0")
                    if "res_partner" in query or "res.partner" in query:
                        conditions.append("origin IN ('demo','test','prod')")
                    if conditions:
                        if has_where:
                            condition_str = " AND " + " AND ".join(conditions)
                        else:
                            condition_str = " WHERE " + " AND ".join(conditions)
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
                        request.env.cr.execute(original_query)
                        result_value = (
                            request.env.cr.fetchone()[0]
                            if request.env.cr.rowcount > 0
                            else 0
                        )
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
        
