# -*- coding: utf-8 -*-

import logging
import re
from odoo.http import request

_logger = logging.getLogger(__name__)

class SecurityService:
    """Service for security-related operations."""

    @staticmethod
    def get_user_branch_ids():
        """Get the branches accessible to the current user.

        Returns:
            list: A list of branch IDs that the current user can access.
        """
        if not request or not request.env:
            return []
        user = request.env.user
        if user.has_group("base.group_system") or user.has_group(
            "base.group_erp_manager"
        ):
            return []
        user_branch_id = (
            user.branch_id.id
            if hasattr(user, "branch_id") and user.branch_id
            else False
        )
        accessible_branches = []
        if user_branch_id:
            accessible_branches.append(user_branch_id)
        if hasattr(user, "branches_id"):
            for branch in user.branches_id:
                if branch.id not in accessible_branches:
                    accessible_branches.append(branch.id)
        branch_access_model = "res.branch.access"
        if branch_access_model in request.env:
            branch_access = request.env[branch_access_model].search(
                [("user_id", "=", user.id)]
            )
            for access in branch_access:
                if access.branch_id and access.branch_id.id not in accessible_branches:
                    accessible_branches.append(access.branch_id.id)
        return accessible_branches

    @staticmethod
    def is_cco_user():
        """Check if the current user is a Chief Compliance Officer.

        Returns:
            bool: True if the user is a CCO, False otherwise.
        """
        if not request or not request.env:
            return False
        user = request.env.user
        return any(
            group.name.lower() == "chief compliance officer" for group in user.groups_id
        )

    @staticmethod
    def is_co_user():
        """Check if the current user is a Compliance Officer.

        Returns:
            bool: True if the user is a Compliance Officer, False otherwise.
        """
        if not request or not request.env:
            return False
        user = request.env.user
        return any(
            group.name.lower() == "compliance officer" for group in user.groups_id
        )

    @staticmethod
    def secure_chart_query(chart, cco=False, branches_id=None):
        """Apply all security filters to a chart query.

        Args:
            chart (record): The chart record containing the query.
            cco (bool, optional): Indicates if the user is a CCO.
            branches_id (list, optional): List of branch IDs from the UI.

        Returns:
            str: The secured SQL query.
        """
        from ..services.query_service import QueryService

        if not chart or not chart.query:
            return chart.query
        query = chart.query
        query = query.strip()
        if query.endswith(";"):
            query = query[:-1]
        has_subqueries = (
            "(" in query and "SELECT" in query.upper() and "FROM" in query.upper()
        )
        query = SecurityService.apply_partner_origin_filter(query)
        if (
            chart.branch_filter
            and chart.branch_field
            and not cco
            and not SecurityService.is_cco_user()
        ):
            user_branches = SecurityService.get_user_branch_ids()
            effective_branches = []
            if branches_id:
                if user_branches:
                    effective_branches = [b for b in branches_id if b in user_branches]
                else:
                    effective_branches = branches_id
            elif user_branches:
                effective_branches = user_branches
            if effective_branches:
                if has_subqueries:
                    query = SecurityService._apply_branch_filter_with_laterals(
                        query, chart.branch_field, effective_branches
                    )
                else:
                    branch_condition = SecurityService._build_branch_condition(
                        chart.branch_field, effective_branches
                    )
                    query = QueryService.add_condition_to_query(
                        query, branch_condition
                    )
            else:
                query = QueryService.add_condition_to_query(query, "1 = 0")
        if not query.endswith(";"):
            query += ";"
        return query

    @staticmethod
    def apply_partner_origin_filter(query):
        """Add an origin filter for partner tables.

        Args:
            query (str): The SQL query to modify.

        Returns:
            str: The modified SQL query with the origin filter applied.
        """
        from ..services.query_service import QueryService

        query = query.strip()
        if query.endswith(";"):
            query = query[:-1]
        if not re.search(r"\bres_partner\b", query, re.IGNORECASE):
            return query
        origin_patterns = [
            r"\borigin\s+IN\s+\(.*?\)",
            r"\brp\.origin\s+IN\s+\(.*?\)",
            r"\bres_partner\.origin\s+IN\s+\(.*?\)",
        ]
        for pattern in origin_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                return query
        modified_query = query
        subquery_pattern = r"(\(\s*SELECT\s+.*?FROM\s+.*?res_partner\b.*?)(\s+WHERE\s+.*?)(\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|\))"
        subquery_matches = list(
            re.finditer(subquery_pattern, query, re.IGNORECASE | re.DOTALL)
        )
        if subquery_matches:
            for match in subquery_matches:
                subquery_start = match.group(1)
                where_clause = match.group(2)
                subquery_end = match.group(3)
                new_where = where_clause + " AND origin IN ('demo', 'test', 'prod') "
                modified_subquery = subquery_start + new_where + subquery_end
                modified_query = modified_query.replace(
                    match.group(0), modified_subquery
                )
        else:
            basic_subquery = r"(\(\s*SELECT\s+.*?FROM\s+.*?res_partner\b.*?)(\s+GROUP BY\b|\s+ORDER BY\b|\s+LIMIT\b|\))"
            basic_matches = list(
                re.finditer(basic_subquery, query, re.IGNORECASE | re.DOTALL)
            )
            if basic_matches:
                for match in basic_matches:
                    subquery_part = match.group(1)
                    end_part = match.group(2)
                    modified_subquery = (
                        subquery_part
                        + " WHERE origin IN ('demo', 'test', 'prod') "
                        + end_part
                    )
                    modified_query = modified_query.replace(
                        match.group(0), modified_subquery
                    )
            else:
                table_pattern = r"\bres_partner\b(?:\s+AS\s+|\s+)(\w+)"
                table_match = re.search(table_pattern, query, re.IGNORECASE)
                if table_match:
                    alias = table_match.group(1)
                    condition = f"{alias}.origin IN ('demo', 'test', 'prod')"
                else:
                    condition = "origin IN ('demo', 'test', 'prod')"
                if re.search(r"\bWHERE\b", modified_query, re.IGNORECASE):
                    modified_query = re.sub(
                        r"(\bWHERE\b.*?)(\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)",
                        r"\1 AND " + condition + r" \2",
                        modified_query,
                        flags=re.IGNORECASE | re.DOTALL,
                    )
                else:
                    for clause in ["GROUP BY", "ORDER BY", "LIMIT"]:
                        if re.search(
                            r"\b" + clause + r"\b", modified_query, re.IGNORECASE
                        ):
                            modified_query = re.sub(
                                r"(\b" + clause + r"\b)",
                                r" WHERE " + condition + r" \1",
                                modified_query,
                                flags=re.IGNORECASE,
                            )
                            break
                    else:
                        modified_query += " WHERE " + condition
        return modified_query

    @staticmethod
    def _apply_branch_filter_with_laterals(query, branch_field, branches_id):
        """Apply branch filtering to queries with subqueries using LATERAL JOIN.

        Args:
            query (str): The SQL query to modify.
            branch_field (str): The field name for branch filtering.
            branches_id (list): List of branch IDs to filter by.

        Returns:
            str: The modified SQL query with branch filtering applied.
        """
        from ..services.query_service import QueryService

        if "LEFT JOIN (" in query.upper() and "SELECT" in query.upper():
            parts = branch_field.split(".")
            if len(parts) == 2:
                table_alias = parts[0]
                field_name = parts[1]
            else:
                match = re.search(r"\bFROM\s+(\w+)", query, re.IGNORECASE)
                if match:
                    table_alias = match.group(1)
                    field_name = branch_field
                else:
                    table_alias = None
                    field_name = branch_field
            if table_alias:
                if len(branches_id) == 1:
                    branch_condition = f"{table_alias}.{field_name} = {branches_id[0]}"
                else:
                    branch_condition = (
                        f"{table_alias}.{field_name} IN {tuple(branches_id)}"
                    )
                if "WHERE" in query.upper():
                    query = re.sub(
                        r"(\bWHERE\b.*?)(\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)",
                        r"\1 AND " + branch_condition + r" \2",
                        query,
                        flags=re.IGNORECASE | re.DOTALL,
                        count=1,
                    )
                else:
                    for clause in ["GROUP BY", "ORDER BY", "LIMIT"]:
                        if re.search(r"\b" + clause + r"\b", query, re.IGNORECASE):
                            query = re.sub(
                                r"(\b" + clause + r"\b)",
                                r" WHERE " + branch_condition + r" \1",
                                query,
                                flags=re.IGNORECASE,
                                count=1,
                            )
                            break
                    else:
                        query += " WHERE " + branch_condition
                return query
            else:
                _logger.warning(
                    f"Could not apply branch filter to complex query with subqueries: {query}"
                )
                return query
        else:
            branch_condition = SecurityService._build_branch_condition(
                branch_field, branches_id
            )
            return QueryService.add_condition_to_query(query, branch_condition)

    @staticmethod
    def _build_branch_condition(branch_field, branches_id):
        """Build a branch filter condition.

        Args:
            branch_field (str): The field name for branch filtering.
            branches_id (list): List of branch IDs to filter by.

        Returns:
            str: The branch condition for the SQL query, or None if not applicable.
        """
        if not branch_field or not branches_id:
            return None
        if not branches_id:
            return None
        if len(branches_id) == 1:
            return f"{branch_field} = {branches_id[0]}"
        else:
            return f"{branch_field} IN {tuple(branches_id)}"

    @staticmethod
    def check_branches_id(branches_id):
        """Ensure branches_id is always a list.

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