import logging
from odoo import api
from datetime import datetime, timedelta

_logger = logging.getLogger(__name__)

class MaterializedViewService:
    """Service for managing materialized views for chart optimization"""
    
    def __init__(self, env=None):
        self.env = env
    
    def ensure_view_exists(self, view_name, query, indexes=None):
        """Ensure a materialized view exists, create if not"""
        if not self.env:
            return False
        
        try:
            # Check if the view already exists
            self.env.cr.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = %s
                    AND c.relkind = 'm'
                )
            """, (view_name,))
            
            view_exists = self.env.cr.fetchone()[0]
            
            if not view_exists:
                # Create the materialized view
                create_view_query = f"""
                    CREATE MATERIALIZED VIEW {view_name} AS
                    {query}
                    WITH DATA
                """
                self.env.cr.execute(create_view_query)
                _logger.info(f"Created materialized view: {view_name}")
                
                # Create indexes if specified
                if indexes and isinstance(indexes, list):
                    for idx in indexes:
                        if isinstance(idx, dict) and 'columns' in idx:
                            columns = idx['columns']
                            idx_name = idx.get('name', f"{view_name}_idx_{columns.replace(',', '_')}")
                            unique = "UNIQUE" if idx.get('unique', False) else ""
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
        """Refresh a materialized view if needed"""
        if not self.env:
            return False
        
        try:
            # Check last refresh time from a dedicated tracking table
            self.env.cr.execute("""
                CREATE TABLE IF NOT EXISTS materialized_view_refresh_log (
                    view_name VARCHAR PRIMARY KEY,
                    last_refresh TIMESTAMP WITH TIME ZONE NOT NULL,
                    refresh_interval INTEGER NOT NULL DEFAULT 3600
                )
            """)
            
            # Get last refresh info
            self.env.cr.execute("""
                SELECT last_refresh, refresh_interval 
                FROM materialized_view_refresh_log
                WHERE view_name = %s
            """, (view_name,))
            
            result = self.env.cr.fetchone()
            
            # Check if refresh is needed
            refresh_needed = True
            if result and not force:
                last_refresh, interval = result
                if datetime.now() - last_refresh < timedelta(seconds=interval):
                    refresh_needed = False
            
            if refresh_needed:
                # Refresh the view
                self.env.cr.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
                
                # Update refresh log
                self.env.cr.execute("""
                    INSERT INTO materialized_view_refresh_log (view_name, last_refresh)
                    VALUES (%s, NOW())
                    ON CONFLICT (view_name) 
                    DO UPDATE SET last_refresh = NOW()
                """, (view_name,))
                
                self.env.cr.commit()  # Ensure changes are committed
                _logger.info(f"Refreshed materialized view: {view_name}")
                
            return True
            
        except Exception as e:
            _logger.error(f"Error refreshing materialized view {view_name}: {e}")
            return False
    
    def set_refresh_interval(self, view_name, interval_seconds):
        """Set the refresh interval for a materialized view"""
        if not self.env:
            return False
        
        try:
            self.env.cr.execute("""
                UPDATE materialized_view_refresh_log
                SET refresh_interval = %s
                WHERE view_name = %s
            """, (interval_seconds, view_name))
            
            # If no row was updated, insert one
            if self.env.cr.rowcount == 0:
                self.env.cr.execute("""
                    INSERT INTO materialized_view_refresh_log (view_name, last_refresh, refresh_interval)
                    VALUES (%s, NOW() - INTERVAL '1 day', %s)
                """, (view_name, interval_seconds))
            
            return True
            
        except Exception as e:
            _logger.error(f"Error setting refresh interval for {view_name}: {e}")
            return False
    
    def query_view(self, view_name, where_clause=None, order_by=None, limit=None, offset=None):
        """Query a materialized view with optional filtering"""
        if not self.env:
            return []
        
        try:
            # Ensure view exists and is refreshed
            view_exists = self.ensure_view_exists(view_name, "")
            if not view_exists:
                return []
            
            # Build the query
            query = f"SELECT * FROM {view_name}"
            params = []
            
            # Add WHERE clause if specified
            if where_clause:
                query += f" WHERE {where_clause}"
            
            # Add ORDER BY if specified
            if order_by:
                query += f" ORDER BY {order_by}"
            
            # Add LIMIT if specified
            if limit:
                query += f" LIMIT %s"
                params.append(limit)
            
            # Add OFFSET if specified
            if offset:
                query += f" OFFSET %s"
                params.append(offset)
            
            # Execute the query
            self.env.cr.execute(query, tuple(params))
            return self.env.cr.dictfetchall()
            
        except Exception as e:
            _logger.error(f"Error querying materialized view {view_name}: {e}")
            return []
    
    def get_view_definition(self, view_name):
        """Get the SQL definition of a materialized view"""
        if not self.env:
            return None
        
        try:
            self.env.cr.execute("""
                SELECT pg_get_viewdef(c.oid, true) as definition
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s
                AND c.relkind = 'm'
            """, (view_name,))
            
            result = self.env.cr.fetchone()
            return result[0] if result else None
            
        except Exception as e:
            _logger.error(f"Error getting definition for {view_name}: {e}")
            return None