# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError, ValidationError
import logging
import time
import threading
import importlib
from contextlib import contextmanager
import gc
import psycopg2
from psycopg2.extras import execute_values

_logger = logging.getLogger(__name__)

class ConnectionManager:
    """
    Singleton connection manager that prevents recursion issues
    """
    _instance = None
    _lock = threading.Lock()
    
    @classmethod
    def get_instance(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance
    
    def __init__(self):
        self.connections = {}
        self.thread_local = threading.local()
        self.max_idle_time = 300  # 5 minutes
    
    def get_connection(self, connection_config, env):
        """Get a database connection directly without recursion"""
        # Initialize thread-local storage if needed
        if not hasattr(self.thread_local, 'connections'):
            self.thread_local.connections = {}
        
        # Create a unique key for this connection
        conn_key = f"{connection_config.id}_{env.cr.dbname}_{threading.get_ident()}"
        
        # Return existing connection if valid
        if conn_key in self.thread_local.connections:
            conn_info = self.thread_local.connections[conn_key]
            
            # Check if connection is still valid and not expired
            if (time.time() - conn_info['last_used']) < self.max_idle_time:
                try:
                    conn = conn_info['connection']
                    # Basic connection check
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                    
                    # Update last used time
                    conn_info['last_used'] = time.time()
                    return conn
                except Exception:
                    # Connection is stale, will create a new one
                    _logger.info(f"Connection {conn_key} is stale, creating a new one")
                    if 'connection' in conn_info:
                        try:
                            conn_info['connection'].close()
                        except:
                            pass
        
        # Create a new connection
        conn = self._create_direct_connection(connection_config)
        
        # Store connection
        self.thread_local.connections[conn_key] = {
            'connection': conn,
            'last_used': time.time()
        }
        
        # Limit number of connections per thread
        if len(self.thread_local.connections) > 10:  # Arbitrary limit
            self._cleanup_oldest_connection()
            
        return conn
    
    def _create_direct_connection(self, connection_config):
        """Create a direct connection without going through abstraction layers"""
        try:
            # Import the driver module
            module_name = connection_config.db_type_id.driver_module
            module = importlib.import_module(module_name)
            
            # Check driver for ODBC connections
            if connection_config.db_type_id.requires_driver and connection_config.db_type_code == 'mssql':
                self._check_mssql_driver(connection_config)
            
            # Get connection string or parameters
            conn_string = connection_config.get_connection_string()
            connection_method = connection_config.db_type_id.connection_method
            
            # Connect based on the method
            if connection_method == 'string':
                return module.connect(conn_string)
            elif connection_method == 'params':
                # Parse connection string into parameters
                params = {}
                for param in conn_string.split(','):
                    if '=' in param:
                        key, value = param.split('=', 1)
                        params[key.strip()] = value.strip()
                return module.connect(**params)
            elif connection_method == 'uri':
                return module.connect(conn_string)
            elif connection_method == 'dsn':
                return module.connect(dsn=conn_string)
            else:
                return module.connect(conn_string)
                
        except ImportError:
            raise UserError(_(f"Python module '{module_name}' not installed. Please install it on the server."))
        except Exception as e:
            _logger.error(f"Connection error: {str(e)}")
            raise UserError(_(f"Failed to connect to database: {str(e)}"))
    
    def _check_mssql_driver(self, connection_config):
        """Check if required ODBC driver is available"""
        try:
            import pyodbc
            drivers = pyodbc.drivers()
            driver_name = connection_config.driver_name
            
            if driver_name not in drivers:
                available_drivers = "\n- " + "\n- ".join(drivers) if drivers else "No ODBC drivers found"
                raise UserError(_(
                    f"The specified ODBC driver '{driver_name}' is not installed on this system.\n\n"
                    f"Available drivers:\n{available_drivers}\n\n"
                    "Please install the required driver or select one that is available."
                ))
        except ImportError:
            raise UserError(_("pyodbc module not installed. Please install it on the server."))
    
    def _cleanup_oldest_connection(self):
        """Close the oldest connection to free up resources"""
        if not hasattr(self.thread_local, 'connections'):
            return
            
        if not self.thread_local.connections:
            return
            
        oldest_time = float('inf')
        oldest_key = None
        
        # Find oldest connection
        for key, conn_info in self.thread_local.connections.items():
            if conn_info['last_used'] < oldest_time:
                oldest_time = conn_info['last_used']
                oldest_key = key
        
        # Close oldest connection
        if oldest_key:
            try:
                conn_info = self.thread_local.connections[oldest_key]
                if 'connection' in conn_info:
                    conn_info['connection'].close()
                del self.thread_local.connections[oldest_key]
            except Exception as e:
                _logger.warning(f"Error closing oldest connection: {str(e)}")
    
    def cleanup_idle_connections(self):
        """Cleanup idle connections"""
        if not hasattr(self.thread_local, 'connections'):
            return
            
        current_time = time.time()
        keys_to_remove = []
        
        for key, conn_info in self.thread_local.connections.items():
            if (current_time - conn_info['last_used']) > self.max_idle_time:
                try:
                    if 'connection' in conn_info:
                        conn_info['connection'].close()
                except Exception as e:
                    _logger.warning(f"Error closing idle connection: {str(e)}")
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.thread_local.connections[key]
    
    def close_all_connections(self):
        """Close all connections"""
        if not hasattr(self.thread_local, 'connections'):
            return
            
        for key, conn_info in list(self.thread_local.connections.items()):
            try:
                if 'connection' in conn_info:
                    conn_info['connection'].close()
            except Exception as e:
                _logger.warning(f"Error closing connection: {str(e)}")
        
        self.thread_local.connections = {}

class DatabaseConnectorService(models.AbstractModel):
    """Direct database connector service without abstraction layers and efficient batch operations"""
    _name = 'etl.database.connector.service'
    _description = 'ETL Database Connector Service'
    
    @api.model
    def get_connection(self, connection_config):
        """Get a database connection directly without going through abstraction layers"""
        connection_manager = ConnectionManager.get_instance()
        return connection_manager.get_connection(connection_config, self.env)
    
    @api.model
    @contextmanager
    def cursor(self, connection_config):
        """Get a cursor for database operations"""
        conn = None
        cursor = None
        try:
            # Get a connection
            conn = self.get_connection(connection_config)
            
            # Create cursor
            cursor = conn.cursor()
            
            # Yield cursor for operations
            yield cursor
            
            # Commit changes
            conn.commit()
        except Exception as e:
            # Rollback on error
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            
            _logger.error(f"Database operation error: {str(e)}")
            raise e
        finally:
            # Close cursor
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
    
    @api.model
    def execute_query(self, connection_config, query, params=None):
        """Execute a query and return results"""
        with self.cursor(connection_config) as cursor:
            # Execute query
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Return results if available
            if cursor.description:
                columns = [column[0] for column in cursor.description]
                result = []
                
                # Fetch rows
                rows = cursor.fetchall()
                
                # Convert to dictionaries
                for row in rows:
                    # Handle both tuple and dict row types
                    if isinstance(row, dict):
                        result.append(row)
                    else:
                        result.append(dict(zip(columns, row)))
                
                return result
            
            return None
    
    @api.model
    def test_connection(self, connection_config=None):
        """Test database connection with backward compatibility"""
        try:
            # If connection_config is not provided (original method call pattern)
            if connection_config is None:
                _logger.warning("test_connection() called without connection_config, using compatibility mode")
                # This is for direct calls from the original pattern
                if hasattr(self, 'connection_config'):
                    connection_config = self.connection_config
                else:
                    raise UserError(_("Cannot test connection: no connection configuration provided"))
                
            # Simple connection test
            conn = self.get_connection(connection_config)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception as e:
            _logger.error(f"Connection test failed: {str(e)}")
            raise UserError(_(f"Connection test failed: {str(e)}"))
    
    @api.model
    def get_table_count(self, connection_config, table_name):
        """Get row count for a table"""
        # Format table name based on database type
        db_type = connection_config.db_type_code
        if db_type == 'mssql':
            formatted_table = f"[{table_name}]"
        elif db_type == 'mysql':
            formatted_table = f"`{table_name}`"
        elif db_type == 'postgresql':
            formatted_table = f"\"{table_name}\""
        else:
            formatted_table = table_name
        
        # Execute count query
        query = f"SELECT COUNT(*) AS count FROM {formatted_table}"
        result = self.execute_query(connection_config, query)
        
        if result and len(result) > 0:
            # Extract count from result
            return result[0]['count']
        
        return 0
    
    @api.model
    def get_columns(self, connection_config, table_name):
        """Get column information for a table"""
        db_type = connection_config.db_type_code
        result = {}
        
        try:
            if db_type == 'mssql':
                # For SQL Server
                query = f"SELECT TOP 0 * FROM [{table_name}]"
                with self.cursor(connection_config) as cursor:
                    cursor.execute(query)
                    result = {col[0].lower(): col[0] for col in cursor.description}
            
            elif db_type == 'postgresql':
                # For PostgreSQL
                query = """
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                """
                results = self.execute_query(connection_config, query, [table_name])
                result = {row['column_name'].lower(): row['column_name'] for row in results}
            
            elif db_type == 'mysql':
                # For MySQL
                query = f"SHOW COLUMNS FROM `{table_name}`"
                results = self.execute_query(connection_config, query)
                result = {row['Field'].lower(): row['Field'] for row in results}
            
            else:
                # Generic approach
                query = f"SELECT * FROM {table_name} LIMIT 1"
                with self.cursor(connection_config) as cursor:
                    cursor.execute(query)
                    if cursor.description:
                        result = {col[0].lower(): col[0] for col in cursor.description}
        
        except Exception as e:
            _logger.warning(f"Error getting columns for table {table_name}: {str(e)}")
        
        return result
    
    @api.model
    def format_query(self, connection_config, query_type, **kwargs):
        """Format a query based on database type"""
        db_type = connection_config.db_type_code
        
        if query_type == 'select':
            table = kwargs.get('table')
            columns = kwargs.get('columns', '*')
            where = kwargs.get('where', '')
            limit = kwargs.get('limit', '')
            order_by = kwargs.get('order_by', '')
            
            # Format table and columns
            if db_type == 'mssql':
                table_str = f"[{table}]"
                if isinstance(columns, list):
                    columns_str = ", ".join([f"[{col}]" for col in columns])
                else:
                    columns_str = columns
            elif db_type == 'mysql':
                table_str = f"`{table}`"
                if isinstance(columns, list):
                    columns_str = ", ".join([f"`{col}`" for col in columns])
                else:
                    columns_str = columns
            elif db_type == 'postgresql':
                table_str = f"\"{table}\""
                if isinstance(columns, list):
                    columns_str = ", ".join([f"\"{col}\"" for col in columns])
                else:
                    columns_str = columns
            else:
                table_str = table
                if isinstance(columns, list):
                    columns_str = ", ".join(columns)
                else:
                    columns_str = columns
            
            # Build base query
            query = "SELECT "
            
            # Handle SQL Server TOP
            if db_type == 'mssql' and limit:
                query += f"TOP {limit} "
            
            # Add columns and table
            query += f"{columns_str} FROM {table_str}"
            
            # Add filters
            if where:
                query += f" WHERE {where}"
            
            # Add sorting
            if order_by:
                query += f" ORDER BY {order_by}"
            
            # Add limit for non-SQL Server
            if limit and db_type != 'mssql':
                query += f" LIMIT {limit}"
            
            return query
        
        return ""
    
    @api.model
    def batch_update(self, connection_config, table, primary_key, columns, rows):
        """Efficient batch update with proper upsert operations"""
        if not rows:
            _logger.info(f"No rows to update for table {table}")
            return
            
        db_type = connection_config.db_type_code
        
        try:
            _logger.info(f"Starting batch update for {len(rows)} rows in table {table}")
            
            # Format column names based on database type
            if db_type == 'mssql':
                column_names = ', '.join([f"[{col.lower()}]" for col in columns])
                pk_str = f"[{primary_key}]"
            elif db_type == 'mysql':
                column_names = ', '.join([f"`{col.lower()}`" for col in columns])
                pk_str = f"`{primary_key}`"
            elif db_type == 'postgresql':
                column_names = ', '.join([f"\"{col.lower()}\"" for col in columns])
                pk_str = f"\"{primary_key}\""
            else:
                column_names = ', '.join([col.lower() for col in columns])
                pk_str = primary_key
            
            # Create placeholders for values based on database type
            if db_type == 'postgresql' or db_type == 'mysql':
                placeholders = ', '.join(['%s'] * len(columns))
            else:
                placeholders = ', '.join(['?'] * len(columns))
            
            # Use specialized batch update methods based on database type
            if db_type == 'postgresql':
                self._postgresql_batch_update(connection_config, table, column_names, pk_str, primary_key, columns, rows)
            elif db_type == 'mysql':
                self._mysql_batch_update(connection_config, table, column_names, pk_str, primary_key, columns, rows)
            elif db_type == 'mssql':
                self._mssql_batch_update(connection_config, table, column_names, pk_str, primary_key, columns, rows)
            elif db_type == 'oracle':
                self._oracle_batch_update(connection_config, table, column_names, pk_str, primary_key, columns, rows)
            else:
                # Generic approach for other databases
                self._generic_batch_update(connection_config, table, column_names, pk_str, primary_key, columns, rows)
            
            _logger.info(f"Completed batch update for table {table}")
                    
        except Exception as e:
            _logger.error(f"Error in batch_update for table {table}: {str(e)}")
            raise UserError(_(f"Failed to update records in table {table}: {str(e)}"))
    
    def _postgresql_batch_update(self, connection_config, table, column_names, pk_str, primary_key, columns, rows):
        """Efficient batch update for PostgreSQL using ON CONFLICT"""
        batch_size = 1000  # Process in smaller batches
        
        # Build the upsert query
        update_sets = []
        for col in columns:
            if col.lower() != primary_key.lower():
                update_sets.append(f"\"{col.lower()}\" = EXCLUDED.\"{col.lower()}\"")
        update_clause = ', '.join(update_sets)
        
        base_query = f"""
            INSERT INTO {table} ({column_names})
            VALUES %s
            ON CONFLICT ({pk_str})
            DO UPDATE SET {update_clause}
        """
        
        # Process in batches
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            values = []
            
            for row in batch:
                row_values = tuple(row.get(col, None) for col in columns)
                values.append(row_values)
            
            with self.cursor(connection_config) as cursor:
                # Use psycopg2's execute_values for efficient bulk insert
                execute_values(
                    cursor,
                    base_query,
                    values,
                    template=None,  # Auto-create template
                    page_size=100   # Number of rows per execute
                )
            
            _logger.info(f"Processed batch of {len(batch)} rows in {table}")
    
    def _mysql_batch_update(self, connection_config, table, column_names, pk_str, primary_key, columns, rows):
        """Efficient batch update for MySQL using ON DUPLICATE KEY UPDATE"""
        batch_size = 500  # Smaller batches for MySQL due to packet size limits
        
        # Build the upsert query
        update_sets = []
        for col in columns:
            if col.lower() != primary_key.lower():
                update_sets.append(f"`{col.lower()}` = VALUES(`{col.lower()}`)")
        update_clause = ', '.join(update_sets)
        
        query = f"""
            INSERT INTO {table} ({column_names})
            VALUES ({', '.join(['%s'] * len(columns))})
            ON DUPLICATE KEY UPDATE {update_clause}
        """
        
        # Process in batches
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            values_list = []
            
            for row in batch:
                row_values = [row.get(col, None) for col in columns]
                values_list.append(row_values)
            
            with self.cursor(connection_config) as cursor:
                cursor.executemany(query, values_list)
            
            _logger.info(f"Processed batch of {len(batch)} rows in {table}")
    
    def _mssql_batch_update(self, connection_config, table, column_names, pk_str, primary_key, columns, rows):
        """Efficient batch update for SQL Server using MERGE statement"""
        batch_size = 1000
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            
            # Use a temporary table for the batch data
            temp_table = f"#temp_{table}_{int(time.time())}"
            
            with self.cursor(connection_config) as cursor:
                # Create temporary table
                cursor.execute(f"IF OBJECT_ID('tempdb..{temp_table}') IS NOT NULL DROP TABLE {temp_table}")
                cursor.execute(f"SELECT TOP 0 {column_names} INTO {temp_table} FROM [{table}]")
                
                # Insert data into temporary table
                for row in batch:
                    placeholders = ', '.join(['?'] * len(columns))
                    insert_sql = f"INSERT INTO {temp_table} VALUES ({placeholders})"
                    row_values = [row.get(col, None) for col in columns]
                    cursor.execute(insert_sql, row_values)
                
                # Create MERGE statement
                merge_sets = []
                for col in columns:
                    if col.lower() != primary_key.lower():
                        merge_sets.append(f"T.[{col.lower()}] = S.[{col.lower()}]")
                
                if merge_sets:
                    merge_update = f"UPDATE SET {', '.join(merge_sets)}"
                else:
                    merge_update = ""
                
                merge_sql = f"""
                    MERGE INTO [{table}] AS T
                    USING {temp_table} AS S
                    ON T.[{primary_key}] = S.[{primary_key}]
                    WHEN MATCHED THEN {merge_update}
                    WHEN NOT MATCHED THEN
                        INSERT ({column_names})
                        VALUES ({', '.join([f'S.[{col.lower()}]' for col in columns])});
                """
                
                cursor.execute(merge_sql)
                
                # Clean up temporary table
                cursor.execute(f"DROP TABLE {temp_table}")
            
            _logger.info(f"Processed batch of {len(batch)} rows in {table}")
    
    def _oracle_batch_update(self, connection_config, table, column_names, pk_str, primary_key, columns, rows):
        """Batch update for Oracle using MERGE statement"""
        batch_size = 250  # Smaller batches for Oracle
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            with self.cursor(connection_config) as cursor:
                for row in batch:
                    bind_vars = {}
                    for col in columns:
                        bind_vars[col.lower()] = row.get(col, None)
                    
                    # Build dynamic SQL with bind variables
                    merge_sets = []
                    for col in columns:
                        if col.lower() != primary_key.lower():
                            merge_sets.append(f"{col.lower()} = SRC.{col.lower()}")
                    
                    if merge_sets:
                        merge_update = f"UPDATE SET {', '.join(merge_sets)}"
                    else:
                        merge_update = ""
                    
                    merge_sql = f"""
                        MERGE INTO {table} TGT
                        USING (SELECT {', '.join([f':{col.lower()} as {col.lower()}' for col in columns])} FROM DUAL) SRC
                        ON (TGT.{primary_key} = SRC.{primary_key})
                        WHEN MATCHED THEN {merge_update}
                        WHEN NOT MATCHED THEN
                            INSERT ({column_names})
                            VALUES ({', '.join([f'SRC.{col.lower()}' for col in columns])})
                    """
                    
                    cursor.execute(merge_sql, bind_vars)
            
            _logger.info(f"Processed batch of {len(batch)} rows in {table}")
    
    def _generic_batch_update(self, connection_config, table, column_names, pk_str, primary_key, columns, rows):
        """Generic batch update for databases without native UPSERT"""
        batch_size = 500
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            
            # Use batch operations if supported
            with self.cursor(connection_config) as cursor:
                for row in batch:
                    pk_value = row.get(primary_key)
                    if not pk_value:
                        _logger.warning(f"Skipping row with no primary key value")
                        continue
                    
                    # Check if record exists
                    check_query = f"SELECT 1 FROM {table} WHERE {pk_str} = ?"
                    cursor.execute(check_query, [pk_value])
                    result = cursor.fetchone()
                    
                    if result:
                        # Update existing record
                        set_clauses = []
                        update_values = []
                        for col in columns:
                            if col.lower() != primary_key.lower():
                                set_clauses.append(f"{col.lower()} = ?")
                                update_values.append(row.get(col, None))
                        
                        if set_clauses:
                            update_query = f"UPDATE {table} SET {', '.join(set_clauses)} WHERE {pk_str} = ?"
                            update_values.append(pk_value)
                            cursor.execute(update_query, update_values)
                    else:
                        # Insert new record
                        placeholders = ', '.join(['?'] * len(columns))
                        insert_query = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
                        insert_values = [row.get(col, None) for col in columns]
                        cursor.execute(insert_query, insert_values)
            
            _logger.info(f"Processed batch of {len(batch)} rows in {table}")


class ETLConnectorFactory(models.AbstractModel):
    _name = 'etl.connector.factory'
    _description = 'ETL Connector Factory'
    
    @api.model
    def get_connector(self, connection_config):
        """Get database connector for connection config"""
        db_type = connection_config.db_type_code
        
        # Return the appropriate connector based on database type
        if db_type == 'mssql':
            return self.env['etl.connector.mssql']
        elif db_type == 'postgresql':
            return self.env['etl.connector.postgresql']
        elif db_type == 'mysql':
            # If you have a MySQL connector
            if self.env.get('etl.connector.mysql'):
                return self.env['etl.connector.mysql']
        elif db_type == 'oracle':
            # If you have an Oracle connector
            if self.env.get('etl.connector.oracle'):
                return self.env['etl.connector.oracle']
            
        # Fall back to generic service for other database types
        return self.env['etl.database.connector.service']
