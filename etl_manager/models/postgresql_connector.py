# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError
import logging
import time
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2.pool import ThreadedConnectionPool
import threading
from contextlib import contextmanager
import re

_logger = logging.getLogger(__name__)

class PostgreSQLConnector(models.AbstractModel):
    _name = 'etl.connector.postgresql'
    _description = 'PostgreSQL Database Connector'
    
    # Class level connection pools for different databases
    _connection_pools = {}
    _connection_pools_lock = threading.Lock()
    
    @api.model
    def _get_connection_pool(self, connection_config):
        """Get a connection pool for the given connection string"""
        connection_string = connection_config.get_connection_string()
        key = f"{connection_config.id}_{self.env.cr.dbname}"
        
        with self._connection_pools_lock:
            if key not in self._connection_pools:
                # Create a connection pool with min and max connections
                _logger.info(f"Creating new connection pool for PostgreSQL: {connection_config.name}")
                pool = ThreadedConnectionPool(
                    minconn=1, 
                    maxconn=10, 
                    dsn=connection_string
                )
                self._connection_pools[key] = {
                    'pool': pool,
                    'last_used': time.time()
                }
                
            # Update last used time
            self._connection_pools[key]['last_used'] = time.time()
            return self._connection_pools[key]['pool']
    
    @classmethod
    def _cleanup_stale_pools(cls, max_idle_time=300):
        """Cleanup pools that haven't been used for a while"""
        with cls._connection_pools_lock:
            current_time = time.time()
            to_close = []
            
            for key, pool_info in list(cls._connection_pools.items()):
                if current_time - pool_info['last_used'] > max_idle_time:
                    to_close.append((key, pool_info['pool']))
            
            for key, pool in to_close:
                _logger.info(f"Closing stale connection pool: {key}")
                try:
                    pool.closeall()
                except Exception as e:
                    _logger.warning(f"Error closing pool {key}: {e}")
                del cls._connection_pools[key]
    
    @contextmanager
    def get_connection(self, connection_config):
        """Get a connection from the pool"""
        pool = self._get_connection_pool(connection_config)
        conn = None
        
        try:
            conn = pool.getconn()
            yield conn
        finally:
            if conn:
                pool.putconn(conn)
    
    @contextmanager
    def cursor(self, connection_config):
        """Get a cursor with real dict support"""
        with self.get_connection(connection_config) as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            try:
                yield cursor
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
    
    def connect(self, connection_config):
        """Connect to PostgreSQL database"""
        try:
            # Test connection by getting and returning it
            with self.get_connection(connection_config) as conn:
                return conn
        except Exception as e:
            _logger.error("PostgreSQL connection error: %s", str(e))
            raise UserError(f"Failed to connect to PostgreSQL: {str(e)}")
    
    def test_connection(self, connection_config):
        """Test the PostgreSQL connection"""
        try:
            with self.cursor(connection_config) as cursor:
                cursor.execute("SELECT version()")
                version = cursor.fetchone()['version']
                _logger.info(f"Connected to PostgreSQL: {version}")
                return True
        except Exception as e:
            _logger.error("PostgreSQL connection test failed: %s", str(e))
            raise UserError(f"PostgreSQL connection test failed: {str(e)}")
    
    def execute_query(self, connection_config, query, params=None):
        """Execute a query and return results"""
        with self.cursor(connection_config) as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if cursor.description:
                return cursor.fetchall()
            return None
    
    def execute_many(self, connection_config, query, params_list):
        """Execute a batch query with multiple parameter sets"""
        with self.cursor(connection_config) as cursor:
            cursor.executemany(query, params_list)
    
    def get_columns(self, connection_config, table_name):
        """Get the columns for a table with data types"""
        query = """
            SELECT column_name, data_type, column_default, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
        """
        
        with self.cursor(connection_config) as cursor:
            cursor.execute(query, (table_name,))
            columns = cursor.fetchall()
            
            # Return a dictionary mapping lowercase column names to the original case
            column_map = {}
            column_info = {}
            
            for col in columns:
                column_map[col['column_name'].lower()] = col['column_name']
                column_info[col['column_name'].lower()] = {
                    'name': col['column_name'],
                    'type': col['data_type'],
                    'default': col['column_default'],
                    'nullable': col['is_nullable'] == 'YES'
                }
            
            return column_map, column_info
    
    def get_table_count(self, connection_config, table_name):
        """Get the row count for a table using estimation for large tables"""
        # First try to get an estimated count from statistics (fast)
        estimate_query = """
            SELECT reltuples::bigint AS estimate
            FROM pg_class
            WHERE relname = %s
        """
        
        with self.cursor(connection_config) as cursor:
            cursor.execute(estimate_query, (table_name,))
            result = cursor.fetchone()
            
            if result and result['estimate'] > 0:
                # If table is large enough to have statistics, return the estimate
                return result['estimate']
            
            # For small tables or if statistics are not up to date, perform an exact count
            cursor.execute(f'SELECT COUNT(*) AS count FROM "{table_name}"')
            return cursor.fetchone()['count']
    
    def format_query(self, connection_config, query_type, **kwargs):
        """Format a query for PostgreSQL"""
        if query_type == 'select':
            table = kwargs.get('table')
            columns = kwargs.get('columns', '*')
            where = kwargs.get('where', '')
            limit = kwargs.get('limit', '')
            order_by = kwargs.get('order_by', '')
            offset = kwargs.get('offset', '')
            
            # Format columns
            if isinstance(columns, list):
                columns_str = ", ".join([f'"{col}"' for col in columns])
            else:
                columns_str = columns
            
            # Build query
            query = f'SELECT {columns_str} FROM "{table}"'
            
            if where:
                query += f" WHERE {where}"
            
            if order_by:
                query += f" ORDER BY {order_by}"
            
            if limit:
                query += f" LIMIT {limit}"
            
            if offset:
                query += f" OFFSET {offset}"
            
            return query
            
        return ""
    
    def batch_update(self, connection_config, table, primary_key, columns, rows):
        """Efficient batch update using PostgreSQL's ON CONFLICT (upsert)"""
        if not rows:
            return
        
        try:
            # Build column list and placeholders
            column_names = ', '.join([f'"{col.lower()}"' for col in columns])
            placeholders = ', '.join(['%s'] * len(columns))
            
            # Create update clause for conflict resolution
            update_sets = []
            for col in columns:
                if col.lower() != primary_key.lower():
                    update_sets.append(f'"{col.lower()}" = EXCLUDED."{col.lower()}"')
            update_clause = ', '.join(update_sets)
            
            # Build the UPSERT query
            insert_query = f"""
                INSERT INTO "{table}" ({column_names})
                VALUES %s
                ON CONFLICT ("{primary_key}")
                DO UPDATE SET {update_clause}
            """
            
            # Prepare values for execute_values
            values = []
            for row in rows:
                row_values = tuple(row.get(col, None) for col in columns)
                values.append(row_values)
            
            with self.cursor(connection_config) as cursor:
                # Use execute_values for efficient bulk insert
                execute_values(
                    cursor, 
                    re.sub(r'VALUES\s+%s', 'VALUES', insert_query), 
                    values,
                    template=f"({', '.join(['%s'] * len(columns))})",
                    page_size=1000
                )
            
            _logger.info(f"Processed {len(rows)} rows in {table} using PostgreSQL bulk upsert")
            
        except Exception as e:
            _logger.error(f"Error in PostgreSQL batch_update for table {table}: {str(e)}")
            raise UserError(f"Failed to update records in PostgreSQL table {table}: {str(e)}")
