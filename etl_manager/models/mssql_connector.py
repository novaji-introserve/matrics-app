# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError
import logging
import time
import threading
import pyodbc
from contextlib import contextmanager

_logger = logging.getLogger(__name__)

class MSSQLConnector(models.AbstractModel):
    _name = 'etl.connector.mssql'
    _description = 'Microsoft SQL Server Connector'
    
    # Class level connection pools
    _connection_pools = {}
    _connection_pools_lock = threading.Lock()
    
    @api.model
    def _get_connection_pool(self, connection_config):
        """Get or create a connection pool for the given configuration"""
        key = f"{connection_config.id}_{self.env.cr.dbname}"
        
        with self._connection_pools_lock:
            if key not in self._connection_pools:
                _logger.info(f"Creating new connection pool for SQL Server: {connection_config.name}")
                
                # Create a pool with multiple connections
                pool = {
                    'connections': [],
                    'max_size': 10,
                    'last_used': time.time()
                }
                self._connection_pools[key] = pool
                
            # Update last used time
            self._connection_pools[key]['last_used'] = time.time()
            return self._connection_pools[key]
    
    @classmethod
    def _cleanup_stale_pools(cls, max_idle_time=300):
        """Cleanup pools that haven't been used for a while"""
        with cls._connection_pools_lock:
            current_time = time.time()
            to_close = []
            
            for key, pool in list(cls._connection_pools.items()):
                if current_time - pool['last_used'] > max_idle_time:
                    to_close.append((key, pool))
            
            for key, pool in to_close:
                _logger.info(f"Closing stale connection pool: {key}")
                for conn in pool['connections']:
                    try:
                        conn.close()
                    except Exception as e:
                        _logger.warning(f"Error closing connection: {e}")
                del cls._connection_pools[key]
    
    @contextmanager
    def get_connection(self, connection_config):
        """Get a connection from the pool or create a new one"""
        pool = self._get_connection_pool(connection_config)
        conn = None
        
        with self._connection_pools_lock:
            # Try to find an available connection in the pool
            for i, connection in enumerate(pool['connections']):
                try:
                    # Test if the connection is still valid
                    cursor = connection.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                    
                    # Connection is valid, mark as in use and return
                    conn = connection
                    pool['connections'].pop(i)
                    break
                except Exception:
                    # Connection is stale, close it
                    try:
                        connection.close()
                    except:
                        pass
                    pool['connections'].pop(i)
                    break
            
            # Create a new connection if needed
            if conn is None:
                conn_string = connection_config.get_connection_string()
                conn = pyodbc.connect(conn_string)
        
        try:
            yield conn
        finally:
            # Return connection to the pool if it's still valid
            try:
                if conn:
                    # Test connection before returning to pool
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                    
                    with self._connection_pools_lock:
                        if len(pool['connections']) < pool['max_size']:
                            pool['connections'].append(conn)
                        else:
                            conn.close()
            except Exception:
                # Connection is invalid, close it
                try:
                    if conn:
                        conn.close()
                except:
                    pass
    
    @contextmanager
    def cursor(self, connection_config):
        """Get a cursor from a connection"""
        with self.get_connection(connection_config) as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
    
    def connect(self, connection_config):
        """Connect to SQL Server database"""
        try:
            # Check driver availability
            self._check_driver_availability(connection_config)
            
            # Test connection by getting and returning it
            with self.get_connection(connection_config) as conn:
                return conn
        except Exception as e:
            _logger.error("SQL Server connection error: %s", str(e))
            raise UserError(f"Failed to connect to SQL Server: {str(e)}")
    
    def _check_driver_availability(self, connection_config):
        """Check if the specified ODBC driver is installed"""
        if connection_config.driver_name:
            drivers = pyodbc.drivers()
            if connection_config.driver_name not in drivers:
                available_drivers = "\n- " + "\n- ".join(drivers) if drivers else "No ODBC drivers found"
                raise UserError(_(
                    "The specified ODBC driver '%s' is not installed on this system.\n\n"
                    "Available drivers:\n%s\n\n"
                    "Please install the required driver or select one that is available."
                ) % (connection_config.driver_name, available_drivers))
    
    def test_connection(self, connection_config):
        """Test the SQL Server connection"""
        try:
            self._check_driver_availability(connection_config)
            
            with self.cursor(connection_config) as cursor:
                cursor.execute("SELECT @@VERSION")
                version = cursor.fetchone()[0]
                _logger.info(f"Connected to SQL Server: {version}")
                return True
        except Exception as e:
            _logger.error("SQL Server connection test failed: %s", str(e))
            raise UserError(f"SQL Server connection test failed: {str(e)}")
    
    def _rows_to_dict_list(self, cursor, rows):
        """Convert ODBC rows to a list of dictionaries"""
        if not rows:
            return []
            
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in rows]
    
    def execute_query(self, connection_config, query, params=None):
        """Execute a query and return results as a list of dictionaries"""
        with self.cursor(connection_config) as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if cursor.description:
                rows = cursor.fetchall()
                return self._rows_to_dict_list(cursor, rows)
            return None
    
    def execute_many(self, connection_config, query, params_list):
        """Execute a batch query with multiple parameter sets"""
        with self.cursor(connection_config) as cursor:
            cursor.executemany(query, params_list)
    
    def get_columns(self, connection_config, table_name):
        """Get the columns for a table with data types"""
        # Get column information
        query = f"""
            SELECT 
                c.name AS column_name,
                t.name AS data_type,
                c.is_nullable,
                c.column_id AS ordinal_position,
                COLUMNPROPERTY(c.object_id, c.name, 'IsIdentity') AS is_identity,
                OBJECT_DEFINITION(c.default_object_id) AS column_default
            FROM 
                sys.columns c
            INNER JOIN 
                sys.types t ON c.user_type_id = t.user_type_id
            WHERE 
                OBJECT_ID = OBJECT_ID(%s)
            ORDER BY 
                c.column_id
        """
        
        with self.cursor(connection_config) as cursor:
            cursor.execute(query, (table_name,))
            columns = self._rows_to_dict_list(cursor, cursor.fetchall())
            
            # Return a dictionary mapping lowercase column names to the original case
            column_map = {}
            column_info = {}
            
            for col in columns:
                column_map[col['column_name'].lower()] = col['column_name']
                column_info[col['column_name'].lower()] = {
                    'name': col['column_name'],
                    'type': col['data_type'],
                    'default': col['column_default'],
                    'nullable': col['is_nullable'] == 1,
                    'identity': col['is_identity'] == 1
                }
            
            return column_map, column_info
    
    def get_table_count(self, connection_config, table_name):
        """Get the row count for a table"""
        # Use COUNT_BIG for large tables
        query = f"SELECT COUNT_BIG(*) AS count FROM [{table_name}]"
        
        with self.cursor(connection_config) as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0]
    
    def format_query(self, connection_config, query_type, **kwargs):
        """Format a query for SQL Server"""
        if query_type == 'select':
            table = kwargs.get('table')
            columns = kwargs.get('columns', '*')
            where = kwargs.get('where', '')
            limit = kwargs.get('limit', '')
            order_by = kwargs.get('order_by', '')
            offset = kwargs.get('offset', '')
            
            # Format columns
            if isinstance(columns, list):
                columns_str = ", ".join([f'[{col}]' for col in columns])
            else:
                columns_str = columns
            
            # Build query
            query = f"SELECT "
            
            # Handle limit using TOP
            if limit and not offset:
                query += f"TOP {limit} "
            
            query += f"{columns_str} FROM [{table}]"
            
            if where:
                query += f" WHERE {where}"
            
            if order_by:
                query += f" ORDER BY {order_by}"
            
            # Handle OFFSET/FETCH for pagination
            if offset and order_by:
                query += f" OFFSET {offset} ROWS"
                if limit:
                    query += f" FETCH NEXT {limit} ROWS ONLY"
            
            return query
            
        return ""
    
    def batch_update(self, connection_config, table, primary_key, columns, rows):
        """Efficient batch update using SQL Server's MERGE statement"""
        if not rows:
            return
        
        try:
            # Format column names
            column_names = ', '.join([f"[{col.lower()}]" for col in columns])
            
            # Process in batches to avoid memory issues
            batch_size = 1000
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                
                with self.cursor(connection_config) as cursor:
                    # Create a temporary table to hold the batch data
                    temp_table = f"#temp_{table}_{int(time.time())}"
                    
                    # Create the temporary table with the same structure
                    cursor.execute(f"SELECT TOP 0 {column_names} INTO {temp_table} FROM [{table}]")
                    
                    # Insert data into the temporary table
                    insert_placeholders = ', '.join(['?'] * len(columns))
                    insert_sql = f"INSERT INTO {temp_table} VALUES ({insert_placeholders})"
                    
                    for row in batch:
                        values = [row.get(col, None) for col in columns]
                        cursor.execute(insert_sql, values)
                    
                    # Build the update clause
                    update_clause = []
                    for col in columns:
                        if col.lower() != primary_key.lower():
                            update_clause.append(f"T.[{col.lower()}] = S.[{col.lower()}]")
                    
                    # Execute MERGE statement
                    if update_clause:
                        merge_sql = f"""
                            MERGE INTO [{table}] AS T
                            USING {temp_table} AS S
                            ON T.[{primary_key}] = S.[{primary_key}]
                            WHEN MATCHED THEN
                                UPDATE SET {', '.join(update_clause)}
                            WHEN NOT MATCHED THEN
                                INSERT ({column_names})
                                VALUES ({', '.join([f'S.[{col.lower()}]' for col in columns])});
                        """
                        
                        cursor.execute(merge_sql)
                    
                    # Clean up the temporary table
                    cursor.execute(f"DROP TABLE {temp_table}")
                
                _logger.info(f"Processed batch of {len(batch)} rows in {table} using SQL Server MERGE")
            
        except Exception as e:
            _logger.error(f"Error in SQL Server batch_update for table {table}: {str(e)}")
            raise UserError(f"Failed to update records in SQL Server table {table}: {str(e)}")
