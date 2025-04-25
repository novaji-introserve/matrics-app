# -*- coding: utf-8 -*-
from datetime import date, datetime
from decimal import Decimal
import time
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import json
import logging
import math
import gc
import psutil
import os
import threading
from contextlib import contextmanager
import hashlib
import re
from functools import wraps

_logger = logging.getLogger(__name__)

class ConnectionManager:
    """
    Enhanced connection manager with adaptive pooling and monitoring
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
        self.connection_pools = {}
        self.thread_local = threading.local()
        self.stats = {}
        self.default_max_idle_time = 300  # 5 minutes
        self.cleanup_thread = None
        self.monitoring_enabled = True
        
        # Start background cleanup thread
        self._start_cleanup_thread()
        
        # Register at_exit handler to close all connections on process termination
        import atexit
        atexit.register(self.close_all_connections)
        
    def _start_cleanup_thread(self):
        """Start background thread for cleanup operations"""
        def cleanup_worker():
            while self.monitoring_enabled:
                try:
                    self._cleanup_stale_connections()
                    self._adjust_pool_sizes()
                    self._log_connection_stats()
                except Exception as e:
                    _logger.error(f"Error in connection pool cleanup thread: {str(e)}")
                
                # Sleep for 60 seconds
                time.sleep(60)
        
        # Create and start the thread
        self.cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        self.cleanup_thread.start()
    
    def get_connection(self, connection_config, env, purpose="general"):
        """Get a database connection with enhanced management"""
        # Initialize thread-local storage if needed
        if not hasattr(self.thread_local, 'connections'):
            self.thread_local.connections = {}
        
        # Create a unique key for this connection
        conn_key = self._generate_connection_key(connection_config, env)
        
        # First try to get a connection from thread-local pool
        conn = self._get_connection_from_pool(conn_key, connection_config, purpose)
        if conn:
            return conn
        
        # If no connection in pool, create a new one
        _logger.debug(f"Creating new connection for {connection_config.name} (purpose: {purpose})")
        conn = self._create_direct_connection(connection_config)
        
        # Store connection with metadata
        self.thread_local.connections[conn_key] = {
            'connection': conn,
            'last_used': time.time(),
            'created': time.time(),
            'operations': 0,
            'purpose': purpose,
            'config_id': connection_config.id
        }
        
        # Update stats
        self._update_stats(connection_config.id, 'created')
        
        # Ensure we don't have too many connections per thread
        self._cleanup_oldest_connection_if_needed()
        
        return conn
    
    def _generate_connection_key(self, connection_config, env):
        """Generate a unique key for connection pooling"""
        # Include thread ID, connection ID, database name and purpose
        thread_id = threading.get_ident()
        return f"{connection_config.id}_{env.cr.dbname}_{thread_id}"
    
    def _get_connection_from_pool(self, conn_key, connection_config, purpose):
        """Get a valid connection from the pool if available"""
        if conn_key in self.thread_local.connections:
            conn_info = self.thread_local.connections[conn_key]
            
            # Check if connection is still valid and not expired
            if (time.time() - conn_info['last_used']) < self._get_idle_timeout(connection_config):
                try:
                    conn = conn_info['connection']
                    
                    # Basic connection validation
                    if self._validate_connection(conn, connection_config):
                        # Update usage information
                        conn_info['last_used'] = time.time()
                        conn_info['operations'] += 1
                        conn_info['purpose'] = purpose
                        
                        # Update stats
                        self._update_stats(connection_config.id, 'reused')
                        
                        return conn
                except Exception as e:
                    _logger.debug(f"Connection {conn_key} is stale: {str(e)}")
                    # Connection is invalid, will create a new one
                    self._close_single_connection(conn_key, conn_info)
        
        return None
    
    def _validate_connection(self, connection, config):
        """Validate that a connection is still usable with proper timeout"""
        import select
        
        try:
            cursor = None
            try:
                # Set socket timeout if possible
                if hasattr(connection, 'connection') and hasattr(connection.connection, 'settimeout'):
                    connection.connection.settimeout(5.0)  # 5 second timeout
                    
                cursor = connection.cursor()
                
                # Use different validation queries based on database type
                if config.db_type_code == 'postgresql':
                    # For PostgreSQL, use a cancel-safe query with timeout
                    if hasattr(cursor, 'connection') and hasattr(cursor.connection, 'cancel'):
                        # Get raw cursor connection
                        raw_conn = cursor.connection
                        
                        # Setup non-blocking operation for timeout
                        import threading
                        cancel_timer = threading.Timer(5.0, lambda: raw_conn.cancel())
                        try:
                            cancel_timer.start()
                            cursor.execute("SELECT 1")
                            cursor.fetchone()
                        finally:
                            cancel_timer.cancel()
                    else:
                        # Fallback to standard execution with timeout parameter if available
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                elif config.db_type_code == 'mssql':
                    # For MSSQL, use query timeout parameter
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                elif config.db_type_code == 'mysql':
                    # For MySQL
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                else:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    
                return True
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except Exception as e:
                        _logger.debug(f"Error closing cursor during validation: {str(e)}")
        except Exception as e:
            _logger.debug(f"Connection validation failed: {str(e)}")
            
            # Try to cancel any pending query if possible
            if cursor and hasattr(cursor, 'connection') and hasattr(cursor.connection, 'cancel'):
                try:
                    cursor.connection.cancel()
                except:
                    pass
                    
            # Try to terminate the connection if it's hanging
            if connection:
                try:
                    connection.close()
                except:
                    pass
                    
            return False
    
    def _get_idle_timeout(self, connection_config):
        """Get the idle timeout for a connection based on its type"""
        # Adjust idle timeout based on connection type
        if connection_config.db_type_code == 'postgresql':
            return 600  # 10 minutes for PostgreSQL
        elif connection_config.db_type_code == 'mssql':
            return 300  # 5 minutes for MSSQL
        elif connection_config.db_type_code == 'mysql':
            return 300  # 5 minutes for MySQL
        else:
            return self.default_max_idle_time
    
    def _create_direct_connection(self, connection_config):
        """Create a direct connection with optimized parameters"""
        try:
            # Import the driver module dynamically
            module_name = connection_config.db_type_id.driver_module
            try:
                module = __import__(module_name)
                # Handle multi-level imports (e.g., mysql.connector)
                for part in module_name.split('.')[1:]:
                    module = getattr(module, part)
            except ImportError:
                raise ValidationError(_(f"Python module '{module_name}' not installed. Please install it."))
            
            # Check driver for ODBC connections
            if connection_config.db_type_id.requires_driver and connection_config.db_type_code == 'mssql':
                self._check_mssql_driver(connection_config)
            
            # Get connection string or parameters
            conn_string = connection_config.get_connection_string()
            connection_method = connection_config.db_type_id.connection_method
            
            # Apply optimized connection parameters based on database type
            if connection_config.db_type_code == 'postgresql':
                # Add connection pooling and timeout parameters for PostgreSQL
                if connection_method == 'params':
                    params = self._parse_connection_string(conn_string)
                    # Add optimized parameters
                    params.update({
                        'connect_timeout': 30,
                        'keepalives': 1,
                        'keepalives_idle': 60,
                        'keepalives_interval': 30,
                        'keepalives_count': 5
                    })
                    return module.connect(**params)
                else:
                    # Add parameters to string
                    conn_string += " connect_timeout=30 keepalives=1 keepalives_idle=60 keepalives_interval=30 keepalives_count=5"
                    return module.connect(conn_string)
            
            elif connection_config.db_type_code == 'mssql':
                # Add optimized parameters for MSSQL
                if 'TrustServerCertificate' not in conn_string:
                    conn_string += ";TrustServerCertificate=yes"
                if 'ConnectionTimeout' not in conn_string:
                    conn_string += ";ConnectionTimeout=30"
                if 'Pooling' not in conn_string:
                    conn_string += ";Pooling=yes"
                return module.connect(conn_string)
            
            elif connection_config.db_type_code == 'mysql':
                # Add optimized parameters for MySQL
                if connection_method == 'params':
                    params = self._parse_connection_string(conn_string)
                    # Add optimized parameters
                    params.update({
                        'connect_timeout': 30,
                        'pool_size': 5,
                        'pool_reset_session': True
                    })
                    return module.connect(**params)
                else:
                    # Add parameters to string
                    if ',' in conn_string:
                        conn_string += ",connect_timeout=30,pool_size=5,pool_reset_session=True"
                    else:
                        conn_string += "?connect_timeout=30&pool_size=5&pool_reset_session=True"
                    return module.connect(conn_string)
            
            # Connect based on the method for other database types
            if connection_method == 'string':
                return module.connect(conn_string)
            elif connection_method == 'params':
                params = self._parse_connection_string(conn_string)
                return module.connect(**params)
            elif connection_method == 'uri':
                return module.connect(conn_string)
            elif connection_method == 'dsn':
                return module.connect(dsn=conn_string)
            else:
                return module.connect(conn_string)
                
        except Exception as e:
            _logger.error(f"Connection error: {str(e)}")
            raise ValidationError(_(f"Failed to connect to database: {str(e)}"))
    
    def _parse_connection_string(self, conn_string):
        """Parse a connection string into a dictionary of parameters"""
        params = {}
        
        # Handle both comma and space-separated parameters
        if ',' in conn_string:
            separator = ','
        else:
            separator = ' '
            
        for param in conn_string.split(separator):
            if '=' in param:
                key, value = param.split('=', 1)
                params[key.strip()] = value.strip()
        
        return params
    
    def _check_mssql_driver(self, connection_config):
        """Check if required ODBC driver is available"""
        try:
            import pyodbc
            drivers = pyodbc.drivers()
            driver_name = connection_config.driver_name
            
            if driver_name not in drivers:
                available_drivers = "\n- " + "\n- ".join(drivers) if drivers else "No ODBC drivers found"
                raise ValidationError(_(
                    f"The specified ODBC driver '{driver_name}' is not installed on this system.\n\n"
                    f"Available drivers:\n{available_drivers}\n\n"
                    "Please install the required driver or select one that is available."
                ))
        except ImportError:
            raise ValidationError(_("pyodbc module not installed. Please install it on the server."))
    
    def _cleanup_oldest_connection_if_needed(self):
        """Close the oldest connection if we have too many"""
        if not hasattr(self.thread_local, 'connections'):
            return
            
        if not self.thread_local.connections:
            return
            
        # Max connections per thread based on CPU cores
        max_connections_per_thread = max(5, os.cpu_count() or 4)
        
        if len(self.thread_local.connections) > max_connections_per_thread:
            oldest_time = float('inf')
            oldest_key = None
            
            # Find oldest connection
            for key, conn_info in self.thread_local.connections.items():
                if conn_info['last_used'] < oldest_time:
                    oldest_time = conn_info['last_used']
                    oldest_key = key
            
            # Close oldest connection
            if oldest_key:
                conn_info = self.thread_local.connections[oldest_key]
                self._close_single_connection(oldest_key, conn_info)
    
    def _close_single_connection(self, key, conn_info):
        """Close a single connection and remove it from pool"""
        try:
            if 'connection' in conn_info:
                conn = conn_info['connection']
                try:
                    conn.close()
                except Exception as e:
                    _logger.warning(f"Error closing connection: {str(e)}")
                
                # Update stats
                if 'config_id' in conn_info:
                    self._update_stats(conn_info['config_id'], 'closed')
            
            # Remove from pool
            if key in self.thread_local.connections:
                del self.thread_local.connections[key]
                
        except Exception as e:
            _logger.warning(f"Error in close_single_connection: {str(e)}")
    
    def _cleanup_stale_connections(self):
        """Periodic cleanup of idle connections"""
        threads_checked = 0
        connections_closed = 0
        
        try:
            # First get a snapshot of all thread locals
            threads_to_check = []
            
            # Get all thread IDs that might have connections
            for thread_id, tl_dict in list(self.connection_pools.items()):
                if 'connections' in tl_dict:
                    threads_to_check.append(thread_id)
            
            # Check each thread's connections
            for thread_id in threads_to_check:
                threads_checked += 1
                tl_dict = self.connection_pools.get(thread_id)
                
                if not tl_dict or 'connections' not in tl_dict:
                    continue
                    
                connections = tl_dict['connections']
                keys_to_remove = []
                
                # Find stale connections
                current_time = time.time()
                for key, conn_info in connections.items():
                    # Get appropriate idle timeout
                    idle_timeout = self.default_max_idle_time
                    if 'config_id' in conn_info:
                        config_id = conn_info['config_id']
                        if config_id in self.stats:
                            db_type = self.stats[config_id].get('db_type')
                            if db_type == 'postgresql':
                                idle_timeout = 600
                            elif db_type in ('mssql', 'mysql'):
                                idle_timeout = 300
                    
                    # Check if connection is stale
                    if (current_time - conn_info['last_used']) > idle_timeout:
                        try:
                            if 'connection' in conn_info:
                                conn_info['connection'].close()
                                connections_closed += 1
                                
                                # Update stats
                                if 'config_id' in conn_info:
                                    self._update_stats(conn_info['config_id'], 'closed')
                        except Exception as e:
                            _logger.warning(f"Error closing idle connection: {str(e)}")
                        
                        keys_to_remove.append(key)
                
                # Remove closed connections
                for key in keys_to_remove:
                    del connections[key]
            
            if connections_closed > 0:
                _logger.info(f"Cleaned up {connections_closed} idle connections from {threads_checked} threads")
                
            # Trigger garbage collection after cleanup
            gc.collect()
            
        except Exception as e:
            _logger.error(f"Error in connection cleanup: {str(e)}")
    
    def _adjust_pool_sizes(self):
        """Dynamically adjust connection pool sizes based on usage patterns"""
        try:
            # Analyze usage patterns
            for config_id, stats in self.stats.items():
                if 'usage_history' not in stats:
                    continue
                
                # Calculate average connections used in the last 5 minutes
                recent_usage = stats['usage_history'][-5:] if len(stats['usage_history']) >= 5 else stats['usage_history']
                if not recent_usage:
                    continue
                
                avg_connections = sum(usage['active'] for usage in recent_usage) / len(recent_usage)
                max_connections = max(usage['active'] for usage in recent_usage)
                
                # Adjust pool size based on usage
                new_pool_size = max(5, min(50, int(max_connections * 1.2)))  # 20% buffer
                
                # Update stats
                stats['recommended_pool_size'] = new_pool_size
                
                _logger.debug(f"Adjusted pool size for config {config_id}: {new_pool_size} (avg: {avg_connections:.1f}, max: {max_connections})")
                
        except Exception as e:
            _logger.error(f"Error adjusting pool sizes: {str(e)}")
    
    def _log_connection_stats(self):
        """Log connection statistics periodically"""
        try:
            total_connections = 0
            total_active = 0
            total_idle = 0
            
            # Gather current statistics
            for config_id, stats in self.stats.items():
                # Count current active connections
                active_connections = 0
                idle_connections = 0
                
                for thread_id, tl_dict in list(self.connection_pools.items()):
                    if 'connections' in tl_dict:
                        connections = tl_dict['connections']
                        for key, conn_info in connections.items():
                            if conn_info.get('config_id') == config_id:
                                if (time.time() - conn_info['last_used']) < 60:  # Active in last minute
                                    active_connections += 1
                                else:
                                    idle_connections += 1
                
                # Update stats
                stats['active_connections'] = active_connections
                stats['idle_connections'] = idle_connections
                
                # Add to usage history (keep last 30 data points = 30 minutes)
                if 'usage_history' not in stats:
                    stats['usage_history'] = []
                
                stats['usage_history'].append({
                    'timestamp': time.time(),
                    'active': active_connections,
                    'idle': idle_connections
                })
                
                # Keep history at most 30 entries
                if len(stats['usage_history']) > 30:
                    stats['usage_history'] = stats['usage_history'][-30:]
                
                # Add to totals
                total_connections += active_connections + idle_connections
                total_active += active_connections
                total_idle += idle_connections
            
            # Log summary if there are connections
            if total_connections > 0:
                _logger.info(f"Connection stats: {total_connections} total ({total_active} active, {total_idle} idle)")
                
                # More detailed logging at debug level
                for config_id, stats in self.stats.items():
                    if stats.get('active_connections', 0) > 0 or stats.get('idle_connections', 0) > 0:
                        _logger.debug(f"DB {stats.get('name', config_id)}: {stats.get('active_connections', 0)} active, "
                                     f"{stats.get('idle_connections', 0)} idle, {stats.get('created', 0)} created, "
                                     f"{stats.get('reused', 0)} reused, {stats.get('closed', 0)} closed")
            
        except Exception as e:
            _logger.error(f"Error logging connection stats: {str(e)}")
    
    def _update_stats(self, config_id, action):
        """Update connection statistics"""
        if config_id not in self.stats:
            # Initialize stats for this config
            self.stats[config_id] = {
                'created': 0,
                'reused': 0,
                'closed': 0,
                'errors': 0,
                'active_connections': 0,
                'idle_connections': 0,
                'last_updated': time.time()
            }
        
        # Update the specified counter
        if action in self.stats[config_id]:
            self.stats[config_id][action] += 1
        
        self.stats[config_id]['last_updated'] = time.time()
    
    def register_config(self, connection_config):
        """Register connection configuration details for better stats"""
        if connection_config.id not in self.stats:
            self.stats[connection_config.id] = {
                'created': 0,
                'reused': 0,
                'closed': 0,
                'errors': 0,
                'active_connections': 0,
                'idle_connections': 0,
                'last_updated': time.time(),
                'name': connection_config.name,
                'db_type': connection_config.db_type_code
            }
        else:
            # Update name and type
            self.stats[connection_config.id].update({
                'name': connection_config.name,
                'db_type': connection_config.db_type_code
            })
    
    def close_all_connections(self):
        """Close all connections when shutting down"""
        _logger.info("Closing all database connections")
        
        # Stop monitoring thread
        self.monitoring_enabled = False
        
        # Close connections in thread-local storage
        if hasattr(self.thread_local, 'connections'):
            for key, conn_info in list(self.thread_local.connections.items()):
                try:
                    if 'connection' in conn_info:
                        conn_info['connection'].close()
                except Exception as e:
                    _logger.warning(f"Error closing connection during shutdown: {str(e)}")
            
            # Clear thread-local storage
            self.thread_local.connections = {}
        
        # Close connections in all threads
        for thread_id, tl_dict in list(self.connection_pools.items()):
            if 'connections' in tl_dict:
                connections = tl_dict['connections']
                for key, conn_info in list(connections.items()):
                    try:
                        if 'connection' in conn_info:
                            conn_info['connection'].close()
                    except Exception as e:
                        _logger.warning(f"Error closing connection during shutdown: {str(e)}")
                
                # Clear connections
                connections.clear()
        
        # Clear pools
        self.connection_pools.clear()
        
        _logger.info("All database connections closed")


class DatabaseConnectorService(models.AbstractModel):
    """Direct database connector service without abstraction layers and efficient batch operations"""
    _name = 'etl.database.connector.service'
    _description = 'ETL Database Connector Service'
    
    @api.model
    def get_connection(self, connection_config, purpose="general"):
        """Get a database connection with usage tracking"""
        connection_manager = ConnectionManager.get_instance()
        
        # Register this config for better stats
        connection_manager.register_config(connection_config)
        
        return connection_manager.get_connection(connection_config, self.env, purpose)
    
    @api.model
    @contextmanager
    def cursor(self, connection_config, purpose="query"):
        """Enhanced cursor context manager with performance tracking"""
        conn = None
        cursor = None
        start_time = time.time()
        
        try:
            # Get a connection with purpose
            conn = self.get_connection(connection_config, purpose)
            
            # Create cursor with appropriate type based on database
            if connection_config.db_type_code == 'postgresql':
                # Use DictCursor for PostgreSQL for easier column access
                try:
                    import psycopg2.extras
                    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                except (ImportError, AttributeError):
                    cursor = conn.cursor()
            elif connection_config.db_type_code == 'mysql':
                # Use dictionary cursor for MySQL
                try:
                    cursor = conn.cursor(dictionary=True)
                except (AttributeError, TypeError):
                    cursor = conn.cursor()
            else:
                cursor = conn.cursor()
            
            # Yield cursor for operations
            yield cursor
            
            # If we get here without exception, commit changes
            conn.commit()
            
        except Exception as e:
            # Rollback on error
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            
            _logger.error(f"Database operation error: {str(e)}")
            
            # Update error stats
            connection_manager = ConnectionManager.get_instance()
            if connection_manager and hasattr(connection_config, 'id'):
                connection_manager._update_stats(connection_config.id, 'errors')
                
            raise e
            
        finally:
            # Close cursor but keep connection in pool
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            
            # Log execution time if slow
            execution_time = time.time() - start_time
            if execution_time > 1.0:  # Log if slower than 1 second
                _logger.info(f"Slow DB operation ({purpose}): {execution_time:.2f}s")
    
    @api.model
    def execute_query(self, connection_config, query, params=None, fetch_all=True):
        """Execute a query with enhanced error handling and performance tracking"""
        start_time = time.time()
        query_hash = hashlib.md5(query.encode()).hexdigest()
        
        try:
            with self.cursor(connection_config, f"query_{query_hash[:8]}") as cursor:
                # Execute query
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Return results if available
                if cursor.description:
                    if fetch_all:
                        # Fetch all rows
                        result = self._cursor_fetchall(cursor)
                    else:
                        # Fetch just one row
                        result = self._cursor_fetchone(cursor)
                    
                    # Log query time if slow
                    execution_time = time.time() - start_time
                    if execution_time > 1.0:  # Log slow queries
                        truncated_query = (query[:100] + '...') if len(query) > 100 else query
                        _logger.info(f"Slow query ({execution_time:.2f}s): {truncated_query}")
                        
                        # If very slow, log with params for debugging
                        if execution_time > 5.0:
                            _logger.debug(f"Very slow query ({execution_time:.2f}s): {query} - Params: {params}")
                    
                    return result
                
                return None
                
        except Exception as e:
            # Enhance error message with query information
            truncated_query = (query[:150] + '...') if len(query) > 150 else query
            error_message = f"Query error: {str(e)}\nQuery: {truncated_query}"
            if params:
                error_message += f"\nParams: {params}"
                
            _logger.error(error_message)
            raise ValidationError(error_message)
    
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
                    raise ValidationError(_("Cannot test connection: no connection configuration provided"))
                
            # Simple connection test
            conn = self.get_connection(connection_config)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception as e:
            _logger.error(f"Connection test failed: {str(e)}")
            raise ValidationError(_(f"Connection test failed: {str(e)}"))
    
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
    
    def _cursor_fetchall(self, cursor):
        """Convert cursor results to standardized dictionary format"""
        # Handle different cursor types
        try:
            # Some cursors already return dictionaries (RealDictCursor)
            rows = cursor.fetchall()
            
            # Check if we need to convert to dictionaries
            if rows and not isinstance(rows[0], dict):
                # Convert to dictionaries for consistent return format
                columns = [column[0] for column in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
            
            return rows
            
        except Exception as e:
            _logger.error(f"Error fetching results: {str(e)}")
            return []
    
    def _cursor_fetchone(self, cursor):
        """Fetch a single row and convert to dictionary"""
        row = cursor.fetchone()
        
        if row:
            # Check if we need to convert to dictionary
            if not isinstance(row, dict):
                columns = [column[0] for column in cursor.description]
                return dict(zip(columns, row))
                
            return row
            
        return None
    
    @api.model
    def batch_update(self, connection_config, table, primary_key, columns, rows):
        """Enhanced batch update with retry logic and telemetry"""
        if not rows:
            _logger.info(f"No rows to update for table {table}")
            return
            
        db_type = connection_config.db_type_code
        start_time = time.time()
        
        try:
            # Monitor memory usage
            process = psutil.Process(os.getpid())
            mem_before = process.memory_info().rss / 1024 / 1024  # MB
            
            # Delegate to database-specific methods
            if db_type == 'postgresql':
                result = self._postgresql_batch_update(connection_config, table, primary_key, columns, rows)
            elif db_type == 'mssql':
                result = self._mssql_batch_update(connection_config, table, primary_key, columns, rows)
            elif db_type == 'mysql':
                result = self._mysql_batch_update(connection_config, table, primary_key, columns, rows)
            else:
                result = self._generic_batch_update(connection_config, table, primary_key, columns, rows)
            
            # Calculate execution time
            execution_time = time.time() - start_time
            
            # Monitor memory after operation
            mem_after = process.memory_info().rss / 1024 / 1024  # MB
            mem_diff = mem_after - mem_before
            
            # Log performance metrics
            _logger.info(f"Batch update for table {table}: {len(rows)} rows in {execution_time:.2f}s "
                         f"(memory: {mem_before:.1f}MB → {mem_after:.1f}MB, diff: {mem_diff:+.1f}MB)")
            
            # If operation took longer than 10 seconds, log to performance log
            if execution_time > 10.0:
                self.env['etl.performance.log'].sudo().create({
                    'name': f'batch_update_{table}',
                    'execution_time': execution_time,
                    'memory_before': mem_before,
                    'memory_after': mem_after,
                    'memory_diff': mem_diff,
                    'details': json.dumps({
                        'table': table,
                        'row_count': len(rows),
                        'db_type': db_type,
                        'columns': len(columns)
                    })
                })
            
            return result
                    
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in batch_update for table {table}: {error_message}")
            
            # Check if error is retryable
            if self._is_retryable_error(error_message, db_type):
                _logger.info(f"Retrying batch update for table {table} due to retryable error")
                
                # Wait briefly before retry
                time.sleep(1)
                
                # Retry with smaller batches
                return self._retry_batch_update(connection_config, table, primary_key, columns, rows)
            
            # Not retryable, re-raise
            raise ValidationError(_(f"Failed to update records in table {table}: {error_message}"))
        
    def _is_retryable_error(self, error_message, db_type):
        """Check if an error is retryable based on database type and error message"""
        if db_type == 'postgresql':
            # PostgreSQL retryable errors
            retryable_patterns = [
                "deadlock detected",
                "could not serialize access",
                "too many clients",
                "connection has been closed unexpectedly",
                "server closed the connection unexpectedly"
            ]
        elif db_type == 'mssql':
            # MSSQL retryable errors
            retryable_patterns = [
                "deadlock victim",
                "connection is broken",
                "connection reset",
                "operation timed out",
                "transaction was deadlocked"
            ]
        elif db_type == 'mysql':
            # MySQL retryable errors
            retryable_patterns = [
                "deadlock found",
                "lock wait timeout",
                "server has gone away", 
                "too many connections",
                "connection was killed"
            ]
        else:
            # Generic retryable errors
            retryable_patterns = [
                "deadlock",
                "timeout",
                "connection",
                "too many"
            ]
        
        # Check if error matches any pattern
        for pattern in retryable_patterns:
            if pattern in error_message.lower():
                return True
                
        return False
    
    def _retry_batch_update(self, connection_config, table, primary_key, columns, rows):
        """Retry batch update with smaller batches"""
        # Use smaller batch size for retry
        batch_size = min(100, len(rows) // 10) or 1
        
        _logger.info(f"Retrying with batch size {batch_size} (original: {len(rows)} rows)")
        
        results = []
        
        # Process in smaller batches
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            
            try:
                # Use database-specific methods
                db_type = connection_config.db_type_code
                if db_type == 'postgresql':
                    result = self._postgresql_batch_update(connection_config, table, primary_key, columns, batch)
                elif db_type == 'mssql':
                    result = self._mssql_batch_update(connection_config, table, primary_key, columns, batch)
                elif db_type == 'mysql':
                    result = self._mysql_batch_update(connection_config, table, primary_key, columns, batch)
                else:
                    result = self._generic_batch_update(connection_config, table, primary_key, columns, batch)
                
                results.append(result)
                
                # Short pause between batches
                time.sleep(0.1)
                
            except Exception as e:
                _logger.error(f"Error in retry batch {i//batch_size + 1}: {str(e)}")
                raise
        
        return results    
        
    def _postgresql_batch_update(self, connection_config, table, primary_key, columns, rows):
        """Optimized batch update for PostgreSQL using execute_values"""
        if not rows:
            return {'new': 0, 'updated': 0, 'total': 0}
        
        from psycopg2.errors import DeadlockDetected
        
        # Validate rows
        for row in rows:
            if not row.get(primary_key):
                _logger.warning(f"Row missing primary key {primary_key}: {row}")
                raise ValidationError(f"Row missing primary key {primary_key}")
        
        # Validate connection
        conn = self.get_connection(connection_config)
        if not ConnectionManager.get_instance()._validate_connection(conn, connection_config):
            _logger.warning("Invalid connection detected, acquiring new connection")
            conn = ConnectionManager.get_instance()._create_direct_connection(connection_config)
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Get a connection and cursor
                with self.cursor(connection_config) as cursor:
                    from psycopg2.extras import execute_values
                    
                    # First count existing rows to determine inserts vs updates
                    primary_keys = [str(row.get(primary_key)) for row in rows if row.get(primary_key)]
                    
                    existing_count = 0
                    if primary_keys:
                        placeholders = ', '.join(['%s' for _ in primary_keys])
                        count_query = f"""
                            SELECT COUNT(*) AS count FROM "{table}" 
                            WHERE "{primary_key.lower()}" IN ({placeholders})
                        """
                        
                        cursor.execute(count_query, primary_keys)
                        result = cursor.fetchone()
                        existing_count = result['count'] if result else 0
                    
                    # Prepare the upsert query
                    columns_str = ", ".join([f'"{col.lower()}"' for col in columns])
                    
                    # Build update statement for conflict resolution
                    update_cols = []
                    for col in columns:
                        if col.lower() != primary_key.lower():
                            update_cols.append(f'"{col.lower()}" = EXCLUDED."{col.lower()}"')
                    
                    update_stmt = ", ".join(update_cols)
                    
                    # Construct the INSERT ... ON CONFLICT (UPSERT) query
                    upsert_query = f"""
                        INSERT INTO "{table}" ({columns_str})
                        VALUES %s
                        ON CONFLICT ("{primary_key.lower()}")
                        DO UPDATE SET {update_stmt}
                    """
                    
                    # Prepare values
                    values = []
                    for row in rows:
                        row_values = []
                        for col in columns:
                            row_values.append(row.get(col))
                        values.append(row_values)
                    
                    # Use execute_values for efficient batch upsert
                    execute_values(cursor, upsert_query, values)
                    
                    # Calculate stats based on count
                    total_count = len(rows)
                    updated_count = existing_count
                    new_count = total_count - updated_count
                    
                    return {
                        'new': new_count,
                        'updated': updated_count,
                        'total': total_count
                    }
                        
            except DeadlockDetected as e:
                if attempt == max_retries - 1:
                    _logger.error(f"Deadlock detected after {max_retries} attempts: {str(e)}", exc_info=True)
                    raise
                _logger.warning(f"Deadlock detected, retrying ({attempt + 1}/{max_retries})")
                time.sleep(1)
            except Exception as e:
                _logger.error(f"PostgreSQL batch update error: {str(e)}", exc_info=True)
                
                # Try fallback method
                try:
                    _logger.warning(f"Trying fallback method for PostgreSQL batch update")
                    return self._postgresql_fallback_batch_update(connection_config, table, primary_key, columns, rows)
                except Exception as fallback_error:
                    _logger.error(f"PostgreSQL fallback batch update error: {str(fallback_error)}", exc_info=True)
                    raise
                
    def _postgresql_fallback_batch_update(self, connection_config, table, primary_key, columns, rows):
        """Fallback method using individual row operations for PostgreSQL"""
        new_count = 0
        update_count = 0
        
        try:
            # Process rows individually
            with self.cursor(connection_config) as cursor:
                for row in rows:
                    # Check if record exists
                    check_query = f'SELECT 1 FROM "{table}" WHERE "{primary_key}" = %s'
                    cursor.execute(check_query, [row.get(primary_key)])
                    result = cursor.fetchone()
                    
                    if result:
                        # Update
                        update_parts = []
                        update_values = []
                        
                        for col in columns:
                            if col.lower() != primary_key.lower():
                                update_parts.append(f'"{col.lower()}" = %s')
                                update_values.append(row.get(col))
                        
                        if update_parts:
                            update_query = f'UPDATE "{table}" SET {", ".join(update_parts)} WHERE "{primary_key.lower()}" = %s'
                            update_values.append(row.get(primary_key))
                            cursor.execute(update_query, update_values)
                            update_count += 1
                    else:
                        # Insert
                        column_str = ", ".join([f'"{col.lower()}"' for col in columns])
                        value_params = ", ".join(["%s"] * len(columns))
                        
                        insert_query = f'INSERT INTO "{table}" ({column_str}) VALUES ({value_params})'
                        values = [row.get(col) for col in columns]
                        
                        cursor.execute(insert_query, values)
                        new_count += 1
                        
            return {'new': new_count, 'updated': update_count, 'total': new_count + update_count}
        except Exception as e:
            _logger.error(f"PostgreSQL individual operations error: {str(e)}")
            raise
        
    def _mssql_batch_update(self, connection_config, table, primary_key, columns, rows):
        """Optimized batch update for SQL Server using TVP or bulk operations"""
        if not rows:
            return {'new': 0, 'updated': 0, 'total': 0}
            
        try:
            # Track statistics
            stats = {'new': 0, 'updated': 0, 'total': len(rows)}
            
            # Use TVP (Table-Valued Parameters) for SQL Server 2008+ with pyodbc
            batch_size = 1000  # Process in smaller batches to avoid SQL Server limitations
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                
                with self.cursor(connection_config) as cursor:
                    # Create a temp table for the batch
                    temp_table = f"#tmp_{int(time.time())}"
                    columns_str = ", ".join([f"[{col.lower()}] NVARCHAR(MAX)" for col in columns])
                    
                    cursor.execute(f"CREATE TABLE {temp_table} ({columns_str})")
                    
                    # Insert into temp table
                    for row in batch:
                        # Build insert statement with parameters
                        cols = ", ".join([f"[{col.lower()}]" for col in columns])
                        params = ", ".join(["?"] * len(columns))
                        insert_sql = f"INSERT INTO {temp_table} ({cols}) VALUES ({params})"
                        
                        # Get values preserving order
                        values = [row.get(col, None) for col in columns]
                        cursor.execute(insert_sql, values)
                    
                    # Build MERGE statement with OUTPUT to track inserts vs updates
                    merge_sets = []
                    for col in columns:
                        if col.lower() != primary_key.lower():
                            merge_sets.append(f"T.[{col.lower()}] = S.[{col.lower()}]")
                    
                    merge_update = f"UPDATE SET {', '.join(merge_sets)}" if merge_sets else ""
                    
                    # Build final MERGE with OUTPUT to track operations
                    merge_sql = f"""
                        DECLARE @MergeOutput TABLE ([ACTION] NVARCHAR(10), [MergeKey] NVARCHAR(255));
                        
                        MERGE INTO [{table}] AS T
                        USING {temp_table} AS S
                        ON T.[{primary_key.lower()}] = S.[{primary_key.lower()}]
                        WHEN MATCHED THEN
                            {merge_update}
                        WHEN NOT MATCHED THEN
                            INSERT ({", ".join([f"[{col.lower()}]" for col in columns])})
                            VALUES ({", ".join([f"S.[{col.lower()}]" for col in columns])})
                        OUTPUT $action, INSERTED.[{primary_key.lower()}] INTO @MergeOutput;
                        
                        SELECT
                            SUM(CASE WHEN [ACTION] = 'INSERT' THEN 1 ELSE 0 END) AS new_rows,
                            SUM(CASE WHEN [ACTION] = 'UPDATE' THEN 1 ELSE 0 END) AS updated_rows
                        FROM @MergeOutput;
                    """
                    
                    cursor.execute(merge_sql)
                    result = cursor.fetchone()
                    
                    if result:
                        batch_stats = {
                            'new': result[0] if result[0] is not None else 0,
                            'updated': result[1] if result[1] is not None else 0
                        }
                        stats['new'] += batch_stats['new']
                        stats['updated'] += batch_stats['updated']
                    
                    # Clean up
                    cursor.execute(f"DROP TABLE {temp_table}")
                
                _logger.info(f"MSSQL batch {i//batch_size + 1} completed: {len(batch)} rows processed")
            
            _logger.info(f"MSSQL batch update completed for {len(rows)} rows: {stats['new']} new, {stats['updated']} updated")
            return stats
            
        except Exception as e:
            _logger.error(f"MSSQL batch update error: {str(e)}")
            
            # Try fallback method if merge fails
            try:
                _logger.warning(f"Trying fallback method for MSSQL batch update")
                return self._mssql_fallback_batch_update(connection_config, table, primary_key, columns, rows)
            except Exception as fallback_error:
                _logger.error(f"MSSQL fallback batch update error: {str(fallback_error)}")
                raise

    def _mssql_fallback_batch_update(self, connection_config, table, primary_key, columns, rows):
        """Fallback method using individual operations for MSSQL"""
        new_count = 0
        update_count = 0
        
        try:
            # Process in small batches
            batch_size = 100
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                
                with self.cursor(connection_config) as cursor:
                    for row in batch:
                        # Check if record exists
                        check_query = f"SELECT 1 FROM [{table}] WHERE [{primary_key.lower()}] = ?"
                        cursor.execute(check_query, [row.get(primary_key)])
                        result = cursor.fetchone()
                        
                        if result:
                            # Update existing record
                            update_parts = []
                            update_values = []
                            
                            for col in columns:
                                if col.lower() != primary_key.lower():
                                    update_parts.append(f"[{col.lower()}] = ?")
                                    update_values.append(row.get(col))
                            
                            if update_parts:
                                update_query = f"UPDATE [{table}] SET {', '.join(update_parts)} WHERE [{primary_key.lower()}] = ?"
                                update_values.append(row.get(primary_key))
                                cursor.execute(update_query, update_values)
                                update_count += 1
                        else:
                            # Insert new record
                            cols = ", ".join([f"[{col.lower()}]" for col in columns])
                            placeholders = ", ".join(["?"] * len(columns))
                            values = [row.get(col) for col in columns]
                            
                            insert_query = f"INSERT INTO [{table}] ({cols}) VALUES ({placeholders})"
                            cursor.execute(insert_query, values)
                            new_count += 1
                            
            return {'new': new_count, 'updated': update_count, 'total': new_count + update_count}
        except Exception as e:
            _logger.error(f"MSSQL individual operations error: {str(e)}")
            raise
    
    def _mysql_batch_update(self, connection_config, table, primary_key, columns, rows):
        """Optimized batch update for MySQL using multi-row INSERT"""
        if not rows:
            return {'new': 0, 'updated': 0, 'total': 0}
            
        try:
            # For MySQL, use bulk insert with ON DUPLICATE KEY UPDATE
            # Process in smaller batches due to packet size limitations
            batch_size = 500
            total_new = 0
            total_updated = 0
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                
                with self.cursor(connection_config) as cursor:
                    # Prepare multi-row INSERT with ON DUPLICATE KEY UPDATE
                    columns_str = ", ".join([f"`{col.lower()}`" for col in columns])
                    placeholders = ", ".join([f"({', '.join(['%s'] * len(columns))})" for _ in range(len(batch))])
                    
                    # Build the update part for duplicate keys
                    update_parts = []
                    for col in columns:
                        if col.lower() != primary_key.lower():
                            update_parts.append(f"`{col.lower()}` = VALUES(`{col.lower()}`)")
                    
                    update_clause = ", ".join(update_parts)
                    
                    # Complete SQL statement
                    sql = f"""
                        INSERT INTO `{table}` ({columns_str})
                        VALUES {placeholders}
                        ON DUPLICATE KEY UPDATE {update_clause}
                    """
                    
                    # Flatten values for the prepared statement
                    values = []
                    for row in batch:
                        row_values = [row.get(col, None) for col in columns]
                        values.extend(row_values)
                    
                    # Execute
                    cursor.execute(sql, values)
                    
                    # MySQL specific: affected_rows() = 1 for insert, 2 for update, 0 for no change
                    # Get the affected rows count
                    if hasattr(cursor, 'rowcount'):
                        affected_rows = cursor.rowcount
                    else:
                        # If rowcount isn't available, try to get it from the connection
                        affected_rows = cursor.connection.affected_rows()
                    
                    # For MySQL, we need to determine inserts vs updates:
                    # - For each INSERT, affected_rows += 1
                    # - For each UPDATE, affected_rows += 2
                    # - For each row with no change, affected_rows += 0
                    # So: updates = (affected_rows - rows) / 1
                    # And: inserts = rows - updates
                    
                    # Assuming all rows resulted in either an insert or update (MySQL ≥ 5.5)
                    if affected_rows > len(batch):
                        # Some updates occurred
                        batch_updates = (affected_rows - len(batch))
                        batch_new = len(batch) - batch_updates
                    else:
                        # All were inserts
                        batch_new = affected_rows
                        batch_updates = 0
                    
                    total_new += batch_new
                    total_updated += batch_updates
                
                _logger.info(f"MySQL batch {i//batch_size + 1} completed: {len(batch)} rows processed")
            
            stats = {
                'new': total_new,
                'updated': total_updated,
                'total': len(rows)
            }
            
            _logger.info(f"MySQL batch update completed for {len(rows)} rows: {stats['new']} new, {stats['updated']} updated")
            return stats
            
        except Exception as e:
            _logger.error(f"MySQL batch update error: {str(e)}")
            
            # Try fallback method using individual operations
            try:
                _logger.warning(f"Trying fallback method for MySQL batch update")
                return self._mysql_fallback_batch_update(connection_config, table, primary_key, columns, rows)
            except Exception as fallback_error:
                _logger.error(f"MySQL fallback batch update error: {str(fallback_error)}")
                raise

    def _mysql_fallback_batch_update(self, connection_config, table, primary_key, columns, rows):
        """Fallback method using individual operations for MySQL"""
        new_count = 0
        update_count = 0
        
        try:
            # Process in smaller batches
            batch_size = 100
            
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                
                with self.cursor(connection_config) as cursor:
                    for row in batch:
                        # Check if record exists
                        check_query = f"SELECT 1 FROM `{table}` WHERE `{primary_key.lower()}` = %s"
                        cursor.execute(check_query, [row.get(primary_key)])
                        result = cursor.fetchone()
                        
                        if result:
                            # Update
                            update_parts = []
                            update_values = []
                            
                            for col in columns:
                                if col.lower() != primary_key.lower():
                                    update_parts.append(f"`{col.lower()}` = %s")
                                    update_values.append(row.get(col))
                            
                            if update_parts:
                                update_query = f"UPDATE `{table}` SET {', '.join(update_parts)} WHERE `{primary_key.lower()}` = %s"
                                update_values.append(row.get(primary_key))
                                cursor.execute(update_query, update_values)
                                update_count += 1
                        else:
                            # Insert
                            cols = ", ".join([f"`{col.lower()}`" for col in columns])
                            placeholders = ", ".join(["%s"] * len(columns))
                            values = [row.get(col) for col in columns]
                            
                            insert_query = f"INSERT INTO `{table}` ({cols}) VALUES ({placeholders})"
                            cursor.execute(insert_query, values)
                            new_count += 1
            
            return {'new': new_count, 'updated': update_count, 'total': new_count + update_count}
        except Exception as e:
            _logger.error(f"MySQL individual operations error: {str(e)}")
            raise
        
    def _generic_batch_update(self, connection_config, table, primary_key, columns, rows):
        """Generic batch update for other databases with retry and monitoring"""
        if not rows:
            return {'new': 0, 'updated': 0, 'total': 0}
            
        try:
            # Process in smaller batches for reliability
            batch_size = 200
            new_count = 0
            update_count = 0
            error_count = 0
            retry_count = 0
            
            # Get database type for logging
            db_type = connection_config.db_type_code or "unknown"
            
            # Loop through batches
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                batch_start_time = time.time()
                
                # Track batch numbers for better logging
                batch_num = i // batch_size + 1
                total_batches = (len(rows) + batch_size - 1) // batch_size
                
                _logger.info(f"Processing generic batch {batch_num}/{total_batches} for database type {db_type}")
                
                try:
                    # Process the batch
                    with self.cursor(connection_config) as cursor:
                        for row in batch:
                            pk_value = row.get(primary_key)
                            if not pk_value:
                                _logger.warning(f"Skipping row with no primary key value")
                                continue
                                
                            try:
                                # Determine if the record exists
                                check_query = self._format_check_query(db_type, table, primary_key)
                                cursor.execute(check_query, [pk_value])
                                result = cursor.fetchone()
                                
                                if result:
                                    # Update existing record
                                    batch_update_result = self._update_existing_record(
                                        cursor, db_type, table, primary_key, columns, row
                                    )
                                    if batch_update_result:
                                        update_count += 1
                                else:
                                    # Insert new record
                                    batch_insert_result = self._insert_new_record(
                                        cursor, db_type, table, columns, row
                                    )
                                    if batch_insert_result:
                                        new_count += 1
                                        
                            except Exception as row_error:
                                # Handle individual row error
                                error_count += 1
                                _logger.warning(f"Error processing row {pk_value}: {str(row_error)}")
                                # Continue with next row
                                continue
                    
                    # Log batch completion
                    batch_time = time.time() - batch_start_time
                    _logger.info(f"Batch {batch_num}/{total_batches} completed in {batch_time:.2f}s: "
                            f"{len(batch)} rows ({new_count} new, {update_count} updated)")
                    
                except Exception as batch_error:
                    # Handle batch error
                    _logger.error(f"Error processing batch {batch_num}/{total_batches}: {str(batch_error)}")
                    
                    # Retry with smaller batch size if this looks like a size-related issue
                    if "too many" in str(batch_error).lower() or "packet" in str(batch_error).lower() or "size" in str(batch_error).lower():
                        if len(batch) > 10:
                            _logger.info(f"Retrying batch {batch_num} with smaller size")
                            retry_count += 1
                            
                            # Process in even smaller batches
                            smaller_batch_size = len(batch) // 2
                            for j in range(0, len(batch), smaller_batch_size):
                                smaller_batch = batch[j:j + smaller_batch_size]
                                
                                try:
                                    # Process the smaller batch
                                    with self.cursor(connection_config) as cursor:
                                        for row in smaller_batch:
                                            pk_value = row.get(primary_key)
                                            if not pk_value:
                                                continue
                                                
                                            # Check if record exists
                                            check_query = self._format_check_query(db_type, table, primary_key)
                                            cursor.execute(check_query, [pk_value])
                                            result = cursor.fetchone()
                                            
                                            if result:
                                                # Update existing record
                                                batch_update_result = self._update_existing_record(
                                                    cursor, db_type, table, primary_key, columns, row
                                                )
                                                if batch_update_result:
                                                    update_count += 1
                                            else:
                                                # Insert new record
                                                batch_insert_result = self._insert_new_record(
                                                    cursor, db_type, table, columns, row
                                                )
                                                if batch_insert_result:
                                                    new_count += 1
                                                    
                                except Exception as retry_error:
                                    _logger.error(f"Error in retry batch: {str(retry_error)}")
                                    error_count += len(smaller_batch)
                        else:
                            # Batch is already small, count as errors
                            error_count += len(batch)
                    else:
                        # Not a size-related issue, count as errors
                        error_count += len(batch)
            
            # Prepare stats
            stats = {
                'new': new_count,
                'updated': update_count,
                'errors': error_count,
                'retries': retry_count,
                'total': len(rows)
            }
            
            _logger.info(f"Generic batch update completed: {stats['total']} total, {stats['new']} new, "
                    f"{stats['updated']} updated, {stats['errors']} errors, {stats['retries']} retries")
            
            return stats
            
        except Exception as e:
            _logger.error(f"Generic batch update error: {str(e)}")
            raise

    def _format_check_query(self, db_type, table, primary_key):
        """Format the check query based on database type"""
        if db_type == 'postgresql':
            return f'SELECT 1 FROM "{table}" WHERE "{primary_key}" = %s'
        elif db_type == 'mssql':
            return f"SELECT 1 FROM [{table}] WHERE [{primary_key}] = ?"
        elif db_type == 'mysql':
            return f"SELECT 1 FROM `{table}` WHERE `{primary_key}` = %s"
        else:
            return f"SELECT 1 FROM {table} WHERE {primary_key} = ?"

    def _update_existing_record(self, cursor, db_type, table, primary_key, columns, row):
        """Update an existing record with database-specific formatting"""
        try:
            update_parts = []
            update_values = []
            
            for col in columns:
                if col.lower() != primary_key.lower():
                    if db_type == 'postgresql':
                        update_parts.append(f'"{col.lower()}" = %s')
                    elif db_type == 'mssql':
                        update_parts.append(f"[{col.lower()}] = ?")
                    elif db_type == 'mysql':
                        update_parts.append(f"`{col.lower()}` = %s")
                    else:
                        update_parts.append(f"{col.lower()} = ?")
                        
                    update_values.append(row.get(col))
            
            if not update_parts:
                return False  # Nothing to update
            
            # Build the update query
            if db_type == 'postgresql':
                update_query = f'UPDATE "{table}" SET {", ".join(update_parts)} WHERE "{primary_key.lower()}" = %s'
            elif db_type == 'mssql':
                update_query = f"UPDATE [{table}] SET {', '.join(update_parts)} WHERE [{primary_key.lower()}] = ?"
            elif db_type == 'mysql':
                update_query = f"UPDATE `{table}` SET {', '.join(update_parts)} WHERE `{primary_key.lower()}` = %s"
            else:
                update_query = f"UPDATE {table} SET {', '.join(update_parts)} WHERE {primary_key.lower()} = ?"
            
            # Add primary key value to params
            update_values.append(row.get(primary_key))
            
            # Execute the update
            cursor.execute(update_query, update_values)
            return True
            
        except Exception as e:
            _logger.warning(f"Error updating record: {str(e)}")
            raise

    def _insert_new_record(self, cursor, db_type, table, columns, row):
        """Insert a new record with database-specific formatting"""
        try:
            # Format columns and placeholders
            if db_type == 'postgresql':
                cols = ", ".join([f'"{col.lower()}"' for col in columns])
                placeholders = ", ".join(["%s"] * len(columns))
            elif db_type == 'mssql':
                cols = ", ".join([f"[{col.lower()}]" for col in columns])
                placeholders = ", ".join(["?"] * len(columns))
            elif db_type == 'mysql':
                cols = ", ".join([f"`{col.lower()}`" for col in columns])
                placeholders = ", ".join(["%s"] * len(columns))
            else:
                cols = ", ".join([col.lower() for col in columns])
                placeholders = ", ".join(["?"] * len(columns))
            
            # Prepare values
            values = [row.get(col) for col in columns]
            
            # Build the insert query
            if db_type == 'postgresql':
                insert_query = f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})'
            elif db_type == 'mssql':
                insert_query = f"INSERT INTO [{table}] ({cols}) VALUES ({placeholders})"
            elif db_type == 'mysql':
                insert_query = f"INSERT INTO `{table}` ({cols}) VALUES ({placeholders})"
            else:
                insert_query = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
            
            # Execute the insert
            cursor.execute(insert_query, values)
            return True
            
        except Exception as e:
            _logger.warning(f"Error inserting record: {str(e)}")
            raise

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
