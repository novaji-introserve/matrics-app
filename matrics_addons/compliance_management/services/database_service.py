# -*- coding: utf-8 -*-

import logging
import time
from odoo import api
import psycopg2

_logger = logging.getLogger(__name__)

class DatabaseService:
    """Service for common database operations."""

    def __init__(self, env=None):
        """Initialize the DatabaseService.

        Args:
            env (Environment, optional): The Odoo environment. Defaults to None.
        """
        self.env = env

    def execute_query_with_timeout(self, query, params=None, timeout=30000):
        """Execute a query with a timeout.

        Args:
            query (str): The SQL query to execute.
            params (tuple, optional): Parameters for the query. Defaults to None.
            timeout (int, optional): Timeout in milliseconds. Defaults to 30000.

        Returns:
            tuple: (success, result, error_message)
        """
        if not self.env:
            return False, None, "No environment provided"

        try:
            self.env.cr.execute(f"SET LOCAL statement_timeout = {timeout}")
            start_time = time.time()
            if params:
                self.env.cr.execute(query, params)
            else:
                self.env.cr.execute(query)
            execution_time = (time.time() - start_time) * 1000
            results = self.env.cr.dictfetchall()
            return True, results, execution_time
        except psycopg2.Error as e:
            self.env.cr.rollback()
            return False, None, str(e)
        finally:
            try:
                self.env.cr.execute("RESET statement_timeout")
            except:
                pass

    def get_table_columns(self, table_name):
        """Get the columns of a table or view.

        Args:
            table_name (str): The name of the table or view.

        Returns:
            list: A list of column names, or empty list if an error occurred.
        """
        if not self.env:
            return []

        try:
            self.env.cr.execute(f"SELECT * FROM {table_name} LIMIT 0")
            return [desc[0] for desc in self.env.cr.description]
        except Exception as e:
            _logger.error(f"Error getting columns for {table_name}: {e}")
            return []

    def check_table_for_branch_column(self, table_name):
        """Check if a table has a branch-related column.

        Args:
            table_name (str): The name of the table to check.

        Returns:
            str: The name of the branch column if found, otherwise None.
        """
        if not self.env:
            return None

        try:
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
                self.env.cr.execute(query, (schema, table))
            else:
                query = """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                    AND (column_name = 'branch_id' OR column_name LIKE '%%branch%%')
                    ORDER BY CASE WHEN column_name = 'branch_id' THEN 1 ELSE 2 END
                    LIMIT 1
                """
                self.env.cr.execute(query, (table_name,))
            result = self.env.cr.fetchone()
            return result[0] if result else None
        except Exception as e:
            _logger.error(f"Error checking for branch column: {e}")
            return None

    def initialize_db_settings(self):
        """Initialize database settings for optimal performance.

        Returns:
            bool: True if the settings were initialized successfully, False otherwise.
        """
        if not self.env:
            return False

        try:
            self.env.cr.execute("SET work_mem = '32MB'")
            self.env.cr.execute("SET statement_timeout = '30s'")
            self.env.cr.execute("SET max_parallel_workers_per_gather = 4")
            self.env.cr.execute("SET enable_parallel_append = on")
            self.env.cr.execute("SET enable_parallel_hash = on")
            self.env.cr.execute("SET enable_partition_pruning = on")
            _logger.info("Database settings initialized for optimal performance")
            return True
        except Exception as e:
            _logger.error(f"Error initializing database settings: {e}")
            return False

    def record_execution_stats(self, chart_id, execution_time, status, error_message=None):
        """Record execution statistics for a chart with error isolation.

        Args:
            chart_id (int): The ID of the chart being executed.
            execution_time (float): The time taken to execute the query.
            status (str): The execution status (success/error).
            error_message (str, optional): An error message if applicable.
        """
        if not self.env:
            return

        try:
            registry = self.env.registry
            with registry.cursor() as cr:
                env = api.Environment(cr, self.env.uid, self.env.context.copy())
                chart = env["res.dashboard.charts"].browse(chart_id)
                if chart.exists():
                    values = {
                        "last_execution_time": execution_time,
                        "last_execution_status": status,
                    }
                    if error_message:
                        values["last_error_message"] = error_message
                    else:
                        values["last_error_message"] = False
                    chart.write(values)
                    cr.commit()
        except Exception as e:
            _logger.error(f"Failed to record execution statistics: {e}")

