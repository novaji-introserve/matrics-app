# -*- coding: utf-8 -*-

import logging
from odoo import api
from datetime import datetime, timedelta

_logger = logging.getLogger(__name__)

class MaterializedViewService:
    """Service for managing materialized views for chart optimization"""

    def __init__(self, env=None):
        """Initialize the MaterializedViewService.

        Args:
            env (Environment, optional): The Odoo environment. Defaults to None.
        """
        self.env = env

    def ensure_view_exists(self, view_name, query, indexes=None):
        """Ensure a materialized view exists; create it if it does not.

        Args:
            view_name (str): The name of the materialized view.
            query (str): The SQL query to create the view.
            indexes (list, optional): A list of indexes to create on the view.

        Returns:
            bool: True if the view exists or was created successfully, False otherwise.
        """
        if not self.env:
            return False
        try:
            self.env.cr.execute(
                """
                SELECT EXISTS (
                    SELECT FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = %s
                    AND c.relkind = 'm'
                )
            """,
                (view_name,),
            )
            view_exists = self.env.cr.fetchone()[0]
            if not view_exists:
                create_view_query = f"""
                    CREATE MATERIALIZED VIEW {view_name} AS
                    {query}
                    WITH DATA
                """
                self.env.cr.execute(create_view_query)
                _logger.info(f"Created materialized view: {view_name}")
                if indexes and isinstance(indexes, list):
                    for idx in indexes:
                        if isinstance(idx, dict) and "columns" in idx:
                            columns = idx["columns"]
                            idx_name = idx.get(
                                "name", f"{view_name}_idx_{columns.replace(',', '_')}"
                            )
                            unique = "UNIQUE" if idx.get("unique", False) else ""
                            idx_query = f"""
                                CREATE {unique} INDEX {idx_name} ON {view_name} ({columns})
                            """
                            self.env.cr.execute(idx_query)
                            _logger.info(f"Created index {idx_name} on {view_name}")
                return True
            return True
        except Exception as e:
            _logger.error(f"Error ensuring materialized view {view_name}: {e}")
            return False

    def refresh_view(self, view_name, force=False):
        """Refresh a materialized view if needed.

        Args:
            view_name (str): The name of the materialized view.
            force (bool, optional): Force refresh even if not required. Defaults to False.

        Returns:
            bool: True if the refresh was successful, False otherwise.
        """
        if not self.env:
            return False
        try:
            self.env.cr.execute(
                """
                CREATE TABLE IF NOT EXISTS materialized_view_refresh_log (
                    view_name VARCHAR PRIMARY KEY,
                    last_refresh TIMESTAMP WITH TIME ZONE NOT NULL,
                    refresh_interval INTEGER NOT NULL DEFAULT 3600
                )
            """
            )
            self.env.cr.execute(
                """
                SELECT last_refresh, refresh_interval 
                FROM materialized_view_refresh_log
                WHERE view_name = %s
            """,
                (view_name,),
            )
            result = self.env.cr.fetchone()
            refresh_needed = True
            if result and not force:
                last_refresh, interval = result
                if datetime.now() - last_refresh < timedelta(seconds=interval):
                    refresh_needed = False
            if refresh_needed:
                self.env.cr.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
                self.env.cr.execute(
                    """
                    INSERT INTO materialized_view_refresh_log (view_name, last_refresh)
                    VALUES (%s, NOW())
                    ON CONFLICT (view_name) 
                    DO UPDATE SET last_refresh = NOW()
                """,
                    (view_name,),
                )
                self.env.cr.commit()
                _logger.info(f"Refreshed materialized view: {view_name}")
            return True
        except Exception as e:
            _logger.error(f"Error refreshing materialized view {view_name}: {e}")
            return False

    def set_refresh_interval(self, view_name, interval_seconds):
        """Set the refresh interval for a materialized view.

        Args:
            view_name (str): The name of the materialized view.
            interval_seconds (int): The refresh interval in seconds.

        Returns:
            bool: True if the update was successful, False otherwise.
        """
        if not self.env:
            return False
        try:
            self.env.cr.execute(
                """
                UPDATE materialized_view_refresh_log
                SET refresh_interval = %s
                WHERE view_name = %s
            """,
                (interval_seconds, view_name),
            )
            if self.env.cr.rowcount == 0:
                self.env.cr.execute(
                    """
                    INSERT INTO materialized_view_refresh_log (view_name, last_refresh, refresh_interval)
                    VALUES (%s, NOW() - INTERVAL '1 day', %s)
                """,
                    (view_name, interval_seconds),
                )
            return True
        except Exception as e:
            _logger.error(f"Error setting refresh interval for {view_name}: {e}")
            return False

    def query_view(
        self, view_name, where_clause=None, order_by=None, limit=None, offset=None
    ):
        """Query a materialized view with optional filtering.

        Args:
            view_name (str): The name of the materialized view.
            where_clause (str, optional): SQL WHERE clause for filtering.
            order_by (str, optional): SQL ORDER BY clause.
            limit (int, optional): Limit the number of results.
            offset (int, optional): Offset for pagination.

        Returns:
            list: A list of records fetched from the materialized view.
        """
        if not self.env:
            return []
        try:
            view_exists = self.ensure_view_exists(view_name, "")
            if not view_exists:
                return []
            query = f"SELECT * FROM {view_name}"
            params = []
            if where_clause:
                query += f" WHERE {where_clause}"
            if order_by:
                query += f" ORDER BY {order_by}"
            if limit:
                query += f" LIMIT %s"
                params.append(limit)
            if offset:
                query += f" OFFSET %s"
                params.append(offset)
            self.env.cr.execute(query, tuple(params))
            return self.env.cr.dictfetchall()
        except Exception as e:
            _logger.error(f"Error querying materialized view {view_name}: {e}")
            return []

    def get_view_definition(self, view_name):
        """Get the SQL definition of a materialized view.

        Args:
            view_name (str): The name of the materialized view.

        Returns:
            str: The SQL definition of the view, or None if it does not exist.
        """
        if not self.env:
            return None
        try:
            self.env.cr.execute(
                """
                SELECT pg_get_viewdef(c.oid, true) as definition
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s
                AND c.relkind = 'm'
            """,
                (view_name,),
            )
            result = self.env.cr.fetchone()
            return result[0] if result else None
        except Exception as e:
            _logger.error(f"Error getting definition for {view_name}: {e}")
            return None
