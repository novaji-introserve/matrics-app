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

    def check_view_exists(self, view_name):
        """Check if a materialized view exists.
        
        Args:
            view_name (str): The name of the materialized view.
            
        Returns:
            bool: True if the view exists, False otherwise.
        """
        if not self.env:
            return False

        try:
            self.env.cr.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = %s
                    AND c.relkind = 'm'
                )
            """, (view_name,))
            return self.env.cr.fetchone()[0]
        except Exception as e:
            _logger.error(f"Error checking if view {view_name} exists: {e}")
            return False

    def create_materialized_view(self, view_name, query, with_data=True):
        """Create a materialized view.
        
        Args:
            view_name (str): The name of the materialized view.
            query (str): The SQL query to create the view.
            with_data (bool, optional): Create the view with data. Defaults to True.
            
        Returns:
            bool: True if the view was created successfully, False otherwise.
        """
        if not self.env:
            return False

        try:
            if self.check_view_exists(view_name):
                _logger.info(f"Materialized view {view_name} already exists")
                return True

            with_data_clause = "WITH DATA" if with_data else "WITH NO DATA"
            create_view_query = f"""
                CREATE MATERIALIZED VIEW {view_name} AS
                {query}
                {with_data_clause}
            """
            self.env.cr.execute(create_view_query)
            _logger.info(f"Created materialized view: {view_name}")
            return True
        except Exception as e:
            _logger.error(f"Error creating materialized view {view_name}: {e}")
            return False

    def refresh_materialized_view(self, view_name, concurrently=True):
        """Refresh a materialized view.
        
        Args:
            view_name (str): The name of the materialized view.
            concurrently (bool, optional): Refresh concurrently. Defaults to True.
            
        Returns:
            bool: True if the refresh was successful, False otherwise.
        """
        if not self.env:
            return False

        try:
            if not self.check_view_exists(view_name):
                _logger.warning(f"Materialized view {view_name} does not exist")
                return False

            if concurrently:
                try:
                    self.env.cr.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}")
                except Exception as e:
                    _logger.info(f"CONCURRENTLY refresh failed, using regular refresh: {e}")
                    self.env.cr.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
            else:
                self.env.cr.execute(f"REFRESH MATERIALIZED VIEW {view_name}")

            _logger.info(f"Refreshed materialized view: {view_name}")
            return True
        except Exception as e:
            _logger.error(f"Error refreshing materialized view {view_name}: {e}")
            return False

    def drop_materialized_view(self, view_name):
        """Drop a materialized view if it exists.
        
        Args:
            view_name (str): The name of the materialized view.
            
        Returns:
            bool: True if the view was dropped successfully, False otherwise.
        """
        if not self.env:
            return False

        try:
            self.env.cr.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name}")
            _logger.info(f"Dropped materialized view: {view_name}")
            return True
        except Exception as e:
            _logger.error(f"Error dropping materialized view {view_name}: {e}")
            return False

    def create_index_on_view(self, view_name, column_name, unique=False, index_name=None):
        """Create an index on a materialized view.
        
        Args:
            view_name (str): The name of the materialized view.
            column_name (str): The column to create the index on.
            unique (bool, optional): Create a unique index. Defaults to False.
            index_name (str, optional): The name for the index. Defaults to None.
            
        Returns:
            bool: True if the index was created successfully, False otherwise.
        """
        if not self.env:
            return False

        try:
            if not index_name:
                index_name = f"{view_name}_{column_name}_idx"
            
            unique_clause = "UNIQUE" if unique else ""
            self.env.cr.execute(f"CREATE {unique_clause} INDEX {index_name} ON {view_name} ({column_name})")
            _logger.info(f"Created index {index_name} on {view_name}({column_name})")
            return True
        except Exception as e:
            _logger.error(f"Error creating index on {view_name}({column_name}): {e}")
            return False

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
            