# -*- coding: utf-8 -*-

import logging
import re
import html
import json
from odoo.http import request
from odoo import fields
from odoo.exceptions import UserError, ValidationError

from ..services.query_service import QueryService

_logger = logging.getLogger(__name__)

class SecurityService:
    """Service for security-related operations and SQL injection prevention."""
    
    # Comprehensive dangerous SQL patterns - covers all major attack vectors
    DANGEROUS_SQL_PATTERNS = [
        # ======= TIME-BASED INJECTION PATTERNS =======
        # PostgreSQL time delays
        r'\bpg_sleep\b',
        r'\bselect\s+pg_sleep\s*\(',
        r'\bpg_sleep\s*\(\s*\d+\s*\)',
        # MySQL time delays
        r'\bSLEEP\s*\(',
        r'\bbenchmark\s*\(',
        r'\bselect\s+sleep\s*\(',
        r'\bselect\s+benchmark\s*\(',
        # SQL Server time delays
        r'\bwaitfor\s+delay\b',
        r'\bdbms_pipe\.receive_message\b',
        r'\bdbms_lock\.sleep\b',
        # Oracle time delays
        r'\bdbms_lock\.sleep\s*\(',
        r'\bdbms_pipe\.receive_message\s*\(',
        r'\butl_inaddr\.get_host_name\s*\(',
        
        # ======= BOOLEAN-BASED INJECTION PATTERNS =======
        # Classic boolean bypasses
        r'\bOR\s+1\s*=\s*1\b',
        r'\bAND\s+1\s*=\s*1\b',
        r'\bOR\s+1\s*=\s*2\b',
        r'\bAND\s+1\s*=\s*2\b',
        r'\bOR\s+\'1\'\s*=\s*\'1\'\b',
        r'\bAND\s+\'1\'\s*=\s*\'1\'\b',
        r'\bOR\s+\"1\"\s*=\s*\"1\"\b',
        r'\bAND\s+\"1\"\s*=\s*\"1\"\b',
        r'\bOR\s+\'x\'\s*=\s*\'x\'\b',
        r'\bAND\s+\'x\'\s*=\s*\'x\'\b',
        r'\bOR\s+\'admin\'\s*=\s*\'admin\'\b',
        r'\bOR\s+\'test\'\s*=\s*\'test\'\b',
        # Tautology variations
        r'\bOR\s+\w+\s*=\s*\w+\b',
        r'\bAND\s+\w+\s*=\s*\w+\b',
        r'\bOR\s+true\b',
        r'\bAND\s+true\b',
        
        # ======= UNION-BASED INJECTION PATTERNS =======
        r'\bUNION\s+SELECT\b',
        r'\bUNION\s+ALL\s+SELECT\b',
        r'\bUNION\s+DISTINCT\s+SELECT\b',
        r'\/\*!\d+\s+UNION\*\/\s+SELECT\b',
        r'\bUNION\s*\/\*.*?\*\/\s*SELECT\b',
        
        # ======= ERROR-BASED INJECTION PATTERNS =======
        r'\bextractvalue\s*\(',
        r'\bupdatexml\s*\(',
        r'\bexp\s*\(\s*~\s*\(',
        r'\bcast\s*\(\s*0x\w+\s+as\s+char\s*\)',
        r'\bconvert\s*\(\s*int\s*,\s*\w+\s*\)',
        r'\bxmltype\s*\(',
        
        # ======= DDL/DML OPERATIONS =======
        r'\b(DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|TRUNCATE)\s+\w+\b',
        r'\b(EXEC|EXECUTE)\s+\w*\b',
        r'\bSP_\w+\b',
        r'\bXP_\w+\b',
        
        # ======= COMMENT-BASED BYPASSES =======
        r';\s*--',
        r'--\s*[^\r\n]*',
        r'\/\*.*?\*\/',
        # r'#.*$',
        r'\/\*!\d+.*?\*\/',
        
        # ======= STACKED QUERIES =======
        r';\s*(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)\b',
        r';\s*EXEC\b',
        r';\s*EXECUTE\b',
        
        # ======= OUT-OF-BAND INJECTION PATTERNS =======
        r'\bINTO\s+OUTFILE\b',
        r'\bINTO\s+DUMPFILE\b',
        r'\bLOAD_FILE\s*\(',
        r'\bload\s+data\s+infile\b',
        
        # ======= DATABASE SYSTEM FUNCTIONS =======
        # PostgreSQL specific
        r'\bcopy\s+\w+\s+from\b',
        r'\blo_import\s*\(',
        r'\blo_export\s*\(',
        # MySQL specific
        r'\bload_file\s*\(',
        r'\bselect\s+.*\s+into\s+outfile\b',
        # SQL Server specific
        r'\bxp_cmdshell\b',
        r'\bsp_oacreate\b',
        r'\bopenrowset\s*\(',
        r'\bopendatasource\s*\(',
        # Oracle specific
        r'\butl_file\.\w+\b',
        r'\butl_http\.\w+\b',
        r'\bdbms_random\.\w+\b',
        
        # ======= ADVANCED INJECTION TECHNIQUES =======
        # Blind injection with substring
        r'\bsubstring\s*\(\s*.*,\s*\d+\s*,\s*\d+\s*\)',
        r'\bmid\s*\(\s*.*,\s*\d+\s*,\s*\d+\s*\)',
        r'\bleft\s*\(\s*.*,\s*\d+\s*\)',
        r'\bright\s*\(\s*.*,\s*\d+\s*\)',
        # ASCII/CHAR manipulation
        r'\bascii\s*\(',
        r'\bchar\s*\(',
        r'\bchr\s*\(',
        r'\bord\s*\(',
        
        # ======= WAF BYPASS TECHNIQUES =======
        # Case variations and encoding
        # r'\b[sS][eE][lL][eE][cC][tT]\b',
        # r'\b[uU][nN][iI][oO][nN]\b',
        # r'\b[dD][rR][oO][pP]\b',
        # Hex encoding patterns
        r'0x[0-9a-fA-F]+',
        r'CHAR\s*\(\s*\d+\s*\)',
        # Double encoding
        r'%25[0-9a-fA-F]{2}',
        
        # ======= INFORMATION GATHERING =======
        r'\b@@version\b',
        r'\b@@servername\b',
        r'\bversion\s*\(\s*\)',
        r'\buser\s*\(\s*\)',
        r'\bdatabase\s*\(\s*\)',
        r'\bschema\s*\(\s*\)',
        r'\binformation_schema\b',
        r'\bsys\.\w+\b',
        r'\bpg_\w+\b',
        
        # ======= LOGICAL OPERATORS ABUSE =======
        r'\bOR\s+NOT\s+\w+\b',
        r'\bAND\s+NOT\s+\w+\b',
        # r'\bIS\s+NULL\b',
        r'\bIS\s+NOT\s+NULL\b',
        
        # ======= STRING MANIPULATION =======
        r'\bCONCAT\s*\(\s*.*,.*\)',
        r'\bCONCAT_WS\s*\(',
        r'\bGROUP_CONCAT\s*\(',
        r'\b\|\|\s*\w+',  # PostgreSQL concatenation
        r'\+\s*\w+\s*\+',  # SQL Server concatenation
        
        # ======= CONDITIONAL STATEMENTS =======
        r'\bCASE\s+WHEN\s+.*\s+THEN\s+.*\s+ELSE\s+.*\s+END\b',
        r'\bIF\s*\(\s*.*,.*,.*\)',
        r'\bIIF\s*\(\s*.*,.*,.*\)',
        
        # ======= ENCODING BYPASS ATTEMPTS =======
        r'UNHEX\s*\(',
        r'HEX\s*\(',
        r'BASE64\s*\(',
        r'URL_DECODE\s*\(',
        
        # ======= XSS IN SQL CONTEXT =======
        r'<script[^>]*>.*?</script>',
        r'javascript:',
        r'vbscript:',
        r'onload\s*=',
        r'onerror\s*=',
        r'onclick\s*=',
        r'eval\s*\(',
        r'expression\s*\(',
        
        # ======= BLIND INJECTION MATH OPERATIONS =======
        r'\bMOD\s*\(',
        r'\bPOW\s*\(',
        r'\bSQRT\s*\(',
        r'\bFLOOR\s*\(',
        r'\bCEILING\s*\(',
        
        # ======= SECOND-ORDER INJECTION =======
        r'\'.*;\s*--',
        r'\".*;\s*--',
        
        # ======= NULL BYTE INJECTION =======
        r'%00',
        r'\\x00',
        r'\\0',
        
        # ======= ALTERNATIVE WHITESPACE =======
        r'\/\*\*\/',
        r'\t',
        # r'\n',
        r'\r',
        r'\f',
        r'\v',
        
        # ======= POLYGLOT PAYLOADS =======
        r'\'.*OR.*\'',
        r'\".*OR.*\"',
        r'\).*OR.*\(',
        
        # ======= TIME-BASED VARIATIONS =======
        r'select.*from.*where.*=.*and.*sleep',
        r'select.*from.*where.*=.*and.*benchmark',
        r'select.*from.*where.*=.*and.*pg_sleep',
        
        # ======= ADVANCED BOOLEAN PATTERNS =======
        r'EXISTS\s*\(\s*SELECT\s+.*\)',
        r'NOT\s+EXISTS\s*\(\s*SELECT\s+.*\)',
        
        # ======= ENCODED SQL KEYWORDS =======
        r'%53%45%4C%45%43%54',  # SELECT
        r'%55%4E%49%4F%4E',     # UNION
        r'%44%52%4F%50',        # DROP
        
        # ======= MYSQL-SPECIFIC FUNCTIONS =======
        r'\bDATA_TYPE\s*\(',
        r'\bCOLUMN_NAME\s*\(',
        r'\bTABLE_SCHEMA\s*\(',
        
        # ======= ORACLE-SPECIFIC FUNCTIONS =======
        r'\bDUAL\b',
        r'\bROWNUM\b',
        r'\bSYSDATE\b',
        
        # ======= SQL SERVER-SPECIFIC =======
        r'\bMASTER\.\.',
        r'\bSYSOBJECTS\b',
        r'\bSYSCOLUMNS\b',
        
        # ======= POSTGRESQL-SPECIFIC =======
        r'\bPG_STAT_ACTIVITY\b',
        r'\bPG_DATABASE\b',
        r'\bCURRENT_SCHEMA\s*\(\s*\)',
        
        # ======= INJECTION IN ORDER BY =======
        r'ORDER\s+BY\s+\d+\s*(--|#)',
        r'ORDER\s+BY\s+IF\s*\(',
        r'ORDER\s+BY\s+CASE\s+WHEN',
        
        # ======= INJECTION IN LIMIT =======
        r'LIMIT\s+\d+\s*,\s*\d+\s*(--|#)',
        r'LIMIT\s+IF\s*\(',
        
        # ======= DNS EXFILTRATION =======
        r'nslookup\s+',
        r'ping\s+-c\s+\d+',
        r'host\s+',
        
        # ======= OBFUSCATION TECHNIQUES =======
        r'\/\*!\d+.*?\*\/',
        r'SELECT\s*\/\*.*?\*\/\s*FROM',
        r'\w+\s*\/\*.*?\*\/\s*=',
        
        # ======= ALTERNATIVE SYNTAX =======
        r'SELECT.*WHERE.*BETWEEN.*AND',
        r'SELECT.*WHERE.*IN\s*\(',
        r'SELECT.*WHERE.*LIKE\s*[\'\"]\%',
        
        # ======= PRIVILEGE ESCALATION =======
        r'GRANT\s+ALL\s+ON',
        r'REVOKE\s+ALL\s+ON',
        r'CREATE\s+USER\s+',
        r'ALTER\s+USER\s+',
        
        # ======= BACKUP/RESTORE OPERATIONS =======
        r'BACKUP\s+DATABASE\s+',
        r'RESTORE\s+DATABASE\s+',
        r'DUMP\s+',
        
        # ======= DYNAMIC SQL EXECUTION =======
        r'EXEC\s*\(\s*@',
        r'EXECUTE\s*\(\s*@',
        r'sp_executesql\s*@',
        
        # ======= ADDITIONAL CRITICAL PATTERNS =======
        # Substring manipulation in WHERE clauses
        r'substring\s*\(\s*\w+\s*,\s*\d+\s*,\s*\d+\s*\)\s*=',
        r'mid\s*\(\s*\w+\s*,\s*\d+\s*,\s*\d+\s*\)\s*=',
        # UNION with DISTINCT variations
        r'\bUNION\s+DISTINCT\b',
        # Permission/privilege operations
        r'\bGRANT\s+\w+\s+ON\b',
        r'\bREVOKE\s+\w+\s+FROM\b',
        # Database operations
        r'\bBACKUP\s+DATABASE\b',
        r'\bRESTORE\s+DATABASE\b',
        # Variable execution
        r'\bEXEC\s*\(\s*@\w+\s*\)',
        r'\bEXECUTE\s*\(\s*@\w+\s*\)',
        # Oracle DUAL table
        r'\bFROM\s+DUAL\b',
        r'\bSELECT\s+.*\s+FROM\s+DUAL\b',
        # Additional substring variations
        r'\bsubstring\s*\(\s*password\s*,',
        r'\bsubstring\s*\(\s*user\s*\(',
        r'\bleft\s*\(\s*password\s*,',
        r'\bright\s*\(\s*password\s*,'
    ]
    
    # Allowed SQL functions for legitimate queries
    ALLOWED_SQL_FUNCTIONS = [
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COALESCE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
        'DISTINCT', 'GROUP_CONCAT', 'CONCAT', 'SUBSTRING', 'LENGTH', 'UPPER', 'LOWER',
        'DATE', 'EXTRACT', 'DATE_PART', 'NOW', 'CURRENT_DATE', 'CURRENT_TIMESTAMP'
    ]

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
                new_where = where_clause + " AND origin IN ('demo', 'test', 'prod'); "
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

    @staticmethod
    def validate_sql_query(query):
        """Validate SQL query against injection attacks.
        
        Args:
            query (str): The SQL query to validate.
            
        Returns:
            tuple: (is_safe, error_message)
        """
        if not query or not isinstance(query, str):
            return False, "Invalid query format"
            
        # Normalize query for analysis
        normalized_query = query.upper().strip()
        
        # Check for dangerous patterns
        for pattern in SecurityService.DANGEROUS_SQL_PATTERNS:
            if re.search(pattern, normalized_query, re.IGNORECASE | re.MULTILINE):
                _logger.warning(f"Dangerous SQL pattern detected: {pattern} in query: {query[:100]}...")
                return False, f"Query contains potentially dangerous pattern: {pattern}"
        
        # Ensure query starts with SELECT (read-only)
        if not normalized_query.startswith('SELECT'):
            return False, "Only SELECT queries are allowed"
            
        # Check for multiple statements (basic check)
        semicolon_count = query.count(';')
        if semicolon_count > 1 or (semicolon_count == 1 and not query.strip().endswith(';')):
            return False, "Multiple SQL statements are not allowed"
            
        return True, "Query is safe"
    
    @staticmethod
    def sanitize_sql_parameter(param):
        """Sanitize a single SQL parameter.
        
        Args:
            param: The parameter to sanitize.
            
        Returns:
            The sanitized parameter.
        """
        if param is None:
            return None
            
        if isinstance(param, str):
            # Remove dangerous characters and escape quotes
            param = html.escape(param)
            param = param.replace("'", "''")
            param = param.replace('"', '""')
            param = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', param)
            return param
            
        if isinstance(param, (int, float)):
            return param
            
        if isinstance(param, (list, tuple)):
            return [SecurityService.sanitize_sql_parameter(item) for item in param]
            
        if isinstance(param, dict):
            return {k: SecurityService.sanitize_sql_parameter(v) for k, v in param.items()}
            
        return str(param)
    
    @staticmethod
    def validate_and_sanitize_request_data(data):
        """Validate and sanitize incoming request data.
        
        Args:
            data (dict): The request data to validate.
            
        Returns:
            dict: The sanitized data.
            
        Raises:
            ValidationError: If dangerous content is detected.
        """
        if not isinstance(data, dict):
            return data
            
        sanitized_data = {}
        
        for key, value in data.items():
            # Sanitize the key
            clean_key = SecurityService.sanitize_sql_parameter(key)
            
            # Special handling for SQL queries
            if key == 'sql_query' and isinstance(value, str):
                is_safe, error_msg = SecurityService.validate_sql_query(value)
                if not is_safe:
                    _logger.error(f"SQL injection attempt detected: {error_msg} - Query: {value[:200]}...")
                    raise ValidationError(f"Invalid query: {error_msg}")
                sanitized_data[clean_key] = value  # Keep original for legitimate queries
            else:
                # Sanitize other parameters
                sanitized_data[clean_key] = SecurityService.sanitize_sql_parameter(value)
                
        return sanitized_data
    
    @staticmethod
    def secure_execute_query(cr, query, params=None, timeout=30000):
        """Safely execute a SQL query with parameter binding.
        
        Args:
            cr: Database cursor
            query (str): SQL query with placeholders
            params (tuple/list): Parameters for the query
            timeout (int): Query timeout in milliseconds
            
        Returns:
            tuple: (success, results, error_message)
        """
        try:
            # Validate the query first
            is_safe, error_msg = SecurityService.validate_sql_query(query)
            if not is_safe:
                return False, None, error_msg
            
            # Set timeout
            cr.execute(f"SET LOCAL statement_timeout = {timeout}")
            
            # Execute with parameters (this prevents SQL injection)
            if params:
                cr.execute(query, params)
            else:
                cr.execute(query)
                
            return True, cr.fetchall(), None
            
        except Exception as e:
            _logger.error(f"Error executing secure query: {str(e)}")
            return False, None, str(e)
    
    @staticmethod
    def build_safe_where_condition(field_name, operator, value):
        """Build a safe WHERE condition using parameterized queries.
        
        Args:
            field_name (str): The field name
            operator (str): The SQL operator (=, IN, LIKE, etc.)
            value: The value to compare
            
        Returns:
            tuple: (condition_string, parameters)
        """
        # Validate field name (should only contain alphanumeric, underscore, dot)
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_\.]*$', field_name):
            raise ValidationError(f"Invalid field name: {field_name}")
            
        # Validate operator
        allowed_operators = ['=', '!=', '<>', '<', '>', '<=', '>=', 'IN', 'NOT IN', 'LIKE', 'ILIKE', 'IS', 'IS NOT']
        if operator.upper() not in allowed_operators:
            raise ValidationError(f"Invalid operator: {operator}")
            
        if operator.upper() in ['IN', 'NOT IN']:
            if isinstance(value, (list, tuple)):
                placeholders = ','.join(['%s'] * len(value))
                return f"{field_name} {operator} ({placeholders})", tuple(value)
            else:
                return f"{field_name} {operator} (%s)", (value,)
        else:
            return f"{field_name} {operator} %s", (value,)
    
    @staticmethod
    def log_security_event(event_type, details, user_id=None):
        """Log security-related events for monitoring.
        
        Args:
            event_type (str): Type of security event
            details (str): Event details
            user_id (int): User ID if available
        """
        if not user_id and request and request.env:
            user_id = request.env.user.id
            
        _logger.warning(f"SECURITY EVENT [{event_type}] User: {user_id} - {details}")
        
        # You could also store this in a security log table if needed
        try:
            if request and request.env:
                # Create a security log entry (if you have such a model)
                # request.env['security.log'].sudo().create({
                #     'event_type': event_type,
                #     'details': details,
                #     'user_id': user_id,
                #     'timestamp': fields.Datetime.now(),
                #     'ip_address': request.httprequest.remote_addr,
                # })
                pass
        except Exception as e:
            _logger.error(f"Failed to log security event: {str(e)}")