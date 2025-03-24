# # -*- coding: utf-8 -*-
# from odoo import models, fields, api, _
# from odoo.exceptions import UserError, ValidationError
# import pyodbc
# import psycopg2
# from psycopg2 import pool
# from datetime import datetime
# import logging
# import json
# from contextlib import contextmanager
# import hashlib
# from decimal import Decimal
# import backoff
# from typing import Dict, List, Any, Set, Tuple, Optional
# from threading import Lock
# import time
# from dotenv import load_dotenv
# import math
# import os
# # from odoo.addons.queue.queue_job.job import Job
# # from odoo.addons.queue.queue_job.job import identity_exact


# load_dotenv()

# _logger = logging.getLogger(__name__)


# class ConnectionPool:
#     """Connection pool manager for database connections"""

#     def __init__(self):
#         self.pg_pools = {}
#         self.source_pools = {}
#         self.lock = Lock()

#     def get_pg_pool(self, conn_string, min_conn=2, max_conn=10):
#         """Get or create a PostgreSQL connection pool"""
#         with self.lock:
#             if conn_string not in self.pg_pools:
#                 try:
#                     _logger.info(f"Creating new PostgreSQL connection pool")
#                     self.pg_pools[conn_string] = pool.ThreadedConnectionPool(
#                         min_conn, max_conn, conn_string)
#                 except Exception as e:
#                     _logger.error(
#                         f"Failed to create PostgreSQL pool: {str(e)}")
#                     raise
#             return self.pg_pools[conn_string]

#     def get_mssql_connection(self, conn_string):
#         """Get a new MSSQL connection (pyodbc doesn't support proper pooling)"""
#         # For MSSQL we rely on the ODBC driver's built-in connection pooling
#         try:
#             return pyodbc.connect(conn_string, timeout=100)
#         except Exception as e:
#             _logger.error(f"Failed to create MSSQL connection: {str(e)}")
#             raise

#     def get_pg_connection(self, pool_key):
#         """Get a connection from the PostgreSQL pool"""
#         pg_pool = self.pg_pools.get(pool_key)
#         if not pg_pool:
#             raise ValueError(f"PostgreSQL pool not initialized for {pool_key}")

#         try:
#             return pg_pool.getconn()
#         except Exception as e:
#             _logger.error(f"Failed to get connection from pool: {str(e)}")
#             raise

#     def return_pg_connection(self, pool_key, conn):
#         """Return a connection to the PostgreSQL pool"""
#         pg_pool = self.pg_pools.get(pool_key)
#         if pg_pool and conn:
#             try:
#                 pg_pool.putconn(conn)
#             except Exception as e:
#                 _logger.error(f"Failed to return connection to pool: {str(e)}")

#     def close_all_pools(self):
#         """Close all connection pools"""
#         with self.lock:
#             # Close PostgreSQL pools
#             for key, pg_pool in self.pg_pools.items():
#                 try:
#                     pg_pool.closeall()
#                     _logger.info(f"Closed PostgreSQL connection pool")
#                 except Exception as e:
#                     _logger.error(f"Error closing PostgreSQL pool: {str(e)}")

#             self.pg_pools = {}
#             self.source_pools = {}


# # Initialize the connection pool manager

# class ETLManager(models.AbstractModel):
#     _name = 'etl.manager'
#     _description = 'ETL Process Manager'

#     # Class level attributes for cache and locks
#     _lookup_cache = {}
#     _lookup_cache_lock = Lock()
#     _processed_tables = set()
#     _processed_tables_lock = Lock()
    
#     connection_pool_manager = ConnectionPool()


#     @api.model
#     def get_db_connections(self):
#         """Get database connections based on system parameters"""
#         ICPSudo = self.env['ir.config_parameter'].sudo()

#         # Retrieve connection strings
#         mssql_conn_string = ICPSudo.get_param('etl.mssql_connection_string')
#         pg_conn_string = ICPSudo.get_param('etl.postgres_connection_string')
#         source_pg_conn = ICPSudo.get_param(
#             'etl.source_postgres')  # This is etl_source_postgres

#         # Validation
#         if not pg_conn_string:
#             raise UserError(
#                 _("Target PostgreSQL connection string not configured!"))

#         # Determine source connection
#         if source_pg_conn:  # If etl.source_postgres is set
#             source_conn_string = source_pg_conn
#             source_type = 'postgres'
#         else:  # Default to MSSQL
#             if not mssql_conn_string:
#                 raise UserError(
#                     _("MSSQL source connection string not configured!"))
#             source_conn_string = mssql_conn_string
#             source_type = 'mssql'

#         return source_conn_string, pg_conn_string, source_type

#     @contextmanager
#     def get_connections(self):
#         """Context manager for database connections with retry logic"""
#         source_conn_string, pg_conn_string, source_type = self.get_db_connections()
#         source_conn = None
#         pg_conn = None

#         try:
#             # Establish source connection based on explicit source_type
#             if source_type == 'postgres':
#                 # Source is PostgreSQL (etl.source_postgres)
#                 source_conn = psycopg2.connect(source_conn_string)
#                 _logger.info(
#                     "Connected to source PostgreSQL from etl.source_postgres")
#             elif source_type == 'mssql':
#                 # Source is MSSQL
#                 source_conn = pyodbc.connect(source_conn_string, timeout=100)
#                 _logger.info(
#                     "Connected to source MSSQL from etl.mssql_connection_string")
#             else:
#                 raise UserError(_("Unknown source database type!"))

#             # Target is always PostgreSQL (Odoo's DB)
#             pg_conn = psycopg2.connect(pg_conn_string)
#             _logger.info("Connected to target PostgreSQL")

#             # Yield the connections to the caller
#             yield source_conn, pg_conn

#         except Exception as e:
#             _logger.error(f"Connection error: {str(e)}")
#             raise UserError(
#                 f"Failed to establish database connection: {str(e)}")

#         finally:
#             # Clean up connections
#             if source_conn:
#                 source_conn.close()
#             if pg_conn:
#                 pg_conn.close()

#     @api.model
#     def init_connection_pools(self):
#         """Initialize connection pools on server startup"""
#         try:
#             source_conn_string, pg_conn_string, source_type = self.get_db_connections()

#             # Initialize the PostgreSQL pool
#             # connection_pool_manager = ConnectionPool()

#             self.connection_pool_manager.get_pg_pool(pg_conn_string)

#             if source_type == 'postgres':
#                 # Initialize source PostgreSQL pool if using PostgreSQL as source
#                 self.connection_pool_manager.get_pg_pool(source_conn_string)

#             _logger.info("Connection pools initialized successfully")
#         except Exception as e:
#             _logger.error(f"Failed to initialize connection pools: {str(e)}")


    
#     @contextmanager
#     def get_pooled_connections(self):
#         """Context manager for database connections using connection pooling"""
#         source_conn_string, pg_conn_string, source_type = self.get_db_connections()
#         source_conn = None
#         pg_conn = None

#         try:
#             # Get PostgreSQL connection from pool
#             pg_conn = self.connection_pool_manager.get_pg_connection(pg_conn_string)

#             # Get source connection
#             if source_type == 'postgres':
#                 # Source is PostgreSQL
#                 source_conn = self.connection_pool_manager.get_pg_connection(
#                     source_conn_string)
#                 _logger.debug("Using pooled PostgreSQL connection for source")
#             elif source_type == 'mssql':
#                 # Source is MSSQL
#                 source_conn = self.connection_pool_manager.get_mssql_connection(
#                     source_conn_string)
#                 _logger.debug("Using MSSQL connection")
#             else:
#                 raise UserError(_("Unknown source database type!"))

#             # Yield the connections to the caller
#             yield source_conn, pg_conn

#         except Exception as e:
#             _logger.error(f"Connection error: {str(e)}")
#             raise UserError(f"Failed to establish database connection: {str(e)}")

#         finally:
#             # Return PostgreSQL connection to pool
#             if pg_conn:
#                 self.connection_pool_manager.return_pg_connection(
#                     pg_conn_string, pg_conn)

#             # Return source PostgreSQL connection to pool or close MSSQL connection
#             if source_conn:
#                 if source_type == 'postgres':
#                     self.connection_pool_manager.return_pg_connection(
#                         source_conn_string, source_conn)
#                 else:
#                     source_conn.close()
        
        
#     def clear_lookup_cache(self):
#         """Clear the lookup cache"""
#         with self._lookup_cache_lock:
#             self._lookup_cache.clear()

#     def get_lookup_cache_key(self, table: str, key_value: str, lookup_key: str) -> str:
#         """Generate cache key for lookups"""
#         return f"{table}:{lookup_key}:{key_value}"

#     def get_from_lookup_cache(self, table: str, key_value: str, lookup_key: str, lookup_value: str) -> Optional[Any]:
#         """Get value from lookup cache"""
#         with self._lookup_cache_lock:
#             cache_key = self.get_lookup_cache_key(table, lookup_key, key_value)
#             cache_dict = self._lookup_cache.get(cache_key, {})
#             return cache_dict.get(lookup_value)

#     def set_in_lookup_cache(self, table: str, key_value: str, lookup_key: str, lookup_value: str, value: Any):
#         """Set value in lookup cache"""
#         with self._lookup_cache_lock:
#             cache_key = self.get_lookup_cache_key(table, lookup_key, key_value)
#             if cache_key not in self._lookup_cache:
#                 self._lookup_cache[cache_key] = {}
#             self._lookup_cache[cache_key][lookup_value] = value

#     def clear_processed_tables(self):
#         """Clear the set of processed tables"""
#         with self._processed_tables_lock:
#             self._processed_tables.clear()

#     def mark_table_processed(self, table_id: int):
#         """Mark a table as processed"""
#         with self._processed_tables_lock:
#             self._processed_tables.add(table_id)

#     def is_table_processed(self, table_id: int) -> bool:
#         """Check if a table has been processed"""
#         with self._processed_tables_lock:
#             return table_id in self._processed_tables

#     def calculate_row_hash(self, row: Dict[str, Any]) -> str:
#         """Calculate a hash for a row based on its values"""
#         processed_row = {}
#         for key, value in row.items():
#             if value is None:
#                 processed_row[key] = 'NULL'
#             elif isinstance(value, datetime):
#                 processed_row[key] = value.isoformat()
#             elif isinstance(value, Decimal):
#                 processed_row[key] = str(value)
#             else:
#                 processed_row[key] = str(value)
#         row_str = json.dumps(processed_row, sort_keys=True)
#         return hashlib.sha256(row_str.encode()).hexdigest()

#     @backoff.on_exception(
#         backoff.expo,
#         (psycopg2.Error, ValueError),
#         max_tries=3
#     )
    
#     def lookup_value(self, pg_cursor, table: str, key_column: str, value_column: str, key_value: str) -> Optional[Any]:
#         """Look up a value in the PostgreSQL database with caching and retry"""
#         if key_value is None or (isinstance(key_value, str) and not key_value.strip()):
#             return None

#         cached_value = self.get_from_lookup_cache(
#             table, key_value, key_column, value_column)
#         if cached_value is not None:
#             return cached_value

#         try:
#             query = f"SELECT {value_column} FROM {table} WHERE {key_column} = %s"
#             pg_cursor.execute(query, (key_value,))
#             result = pg_cursor.fetchone()

#             if result:
#                 self.set_in_lookup_cache(
#                     table, key_value, key_column, value_column, result[0])
#                 return result[0]

#             _logger.debug(
#                 f"No matching record found in {table} for {key_column}={key_value}")
#             return None
#         except psycopg2.Error as e:
#             # Get the connection object from the cursor
#             connection = pg_cursor.connection

#             # Roll back the transaction to clear the error state
#             connection.rollback()

#             # Log the error
#             _logger.warning(
#                 f"Lookup error for {table}.{key_column}={key_value}: {str(e)}")

#             # Return None for the lookup value
#             return None

#     def bulk_lookup_values(self, pg_cursor, table: str, key_column: str, value_column: str, key_values: List[str]) -> Dict[str, Any]:
#         """Look up multiple values at once to reduce database queries"""
#         if not key_values:
#             return {}

#         # Filter out None and empty values
#         key_values = [k for k in key_values if k is not None and (
#             not isinstance(k, str) or k.strip())]
#         if not key_values:
#             return {}

#         # First check cache for all values
#         result = {}
#         values_to_lookup = []

#         for key_value in key_values:
#             cached_value = self.get_from_lookup_cache(
#                 table, key_value, key_column, value_column)
#             if cached_value is not None:
#                 result[key_value] = cached_value
#             else:
#                 values_to_lookup.append(key_value)

#         if not values_to_lookup:
#             return result

#         try:
#             # Build a query that looks up all values at once
#             placeholders = ','.join(['%s'] * len(values_to_lookup))
#             query = f"SELECT {key_column}, {value_column} FROM {table} WHERE {key_column} IN ({placeholders})"

#             pg_cursor.execute(query, values_to_lookup)

#             # Process results and update cache
#             for key, value in pg_cursor.fetchall():
#                 result[str(key)] = value
#                 self.set_in_lookup_cache(table, str(
#                     key), key_column, value_column, value)

#             return result
#         except psycopg2.Error as e:
#             # Rollback the transaction
#             connection = pg_cursor.connection
#             connection.rollback()
#             _logger.warning(
#                 f"Bulk lookup error for {table}.{key_column}: {str(e)}")

#             # Fall back to individual lookups if bulk fails
#             for key_value in values_to_lookup:
#                 try:
#                     value = self.lookup_value(
#                         pg_cursor, table, key_column, value_column, key_value)
#                     if value is not None:
#                         result[key_value] = value
#                 except Exception as inner_e:
#                     _logger.error(
#                         f"Individual lookup fallback failed for {key_value}: {str(inner_e)}")

#             return result

    
#     # def transform_value(self, mapping: dict, value: Any, pg_cursor) -> Any:
#     #     """Transform a value based on mapping configuration"""
#     #     if value is None or (isinstance(value, str) and not value.strip()):
#     #         if mapping['type'] == 'lookup':
#     #             return None
#     #         return value

#     #     if isinstance(value, str):
#     #         value = value.strip()

#     #     if mapping['type'] == 'direct':
#     #         return value
#     #     elif mapping['type'] == 'lookup':
#     #         try:
#     #             lookup_result = self.lookup_value(
#     #                 pg_cursor,
#     #                 mapping['lookup_table'],
#     #                 mapping['lookup_key'],
#     #                 mapping['lookup_value'],
#     #                 str(value)
#     #             )
#     #             _logger.debug(f"Lookup result for {value}: {lookup_result}")
#     #             return lookup_result
#     #         except Exception as e:
#     #             _logger.warning(
#     #                 f"Lookup failed for table {mapping['lookup_table']}, "
#     #                 f"key {mapping['lookup_key']}={value}: {str(e)}"
#     #             )
#     #             # Return None for failed lookups - this allows the process to continue
#     #             return None
#     #     else:
#     #         raise ValueError(f"Unknown transformation type: {mapping['type']}")

#     def transform_value(self, mapping: dict, value: Any, pg_cursor) -> Any:
#         """Transform a value based on mapping configuration"""
#         if value is None or (isinstance(value, str) and not value.strip()):
#             if mapping['type'] == 'lookup':
#                 return None
#             return value

#         if isinstance(value, str):
#             value = value.strip()

#         if mapping['type'] == 'direct':
#             return value
#         elif mapping['type'] == 'lookup':
#             try:
#                 lookup_result = self.lookup_value(
#                     pg_cursor,
#                     mapping['lookup_table'],
#                     mapping['lookup_key'],
#                     mapping['lookup_value'],
#                     str(value)
#                 )
#                 _logger.debug(f"Lookup result for {value}: {lookup_result}")
#                 return lookup_result
#             except Exception as e:
#                 _logger.warning(
#                     f"Lookup failed for table {mapping['lookup_table']}, "
#                     f"key {mapping['lookup_key']}={value}: {str(e)}"
#                 )
#                 # Return None for failed lookups - this allows the process to continue
#                 return None
#         else:
#             raise ValueError(f"Unknown transformation type: {mapping['type']}")

    
#     def batch_transform_values(self, pg_cursor, batch_rows: List[Dict[str, Any]], config: dict) -> List[Dict[str, Any]]:
#         """Transform multiple rows at once to minimize database operations"""
#         if not batch_rows:
#             return []

#         # Collect all lookup values needed by column and table
#         # Structure: {(lookup_table, lookup_key, lookup_value): [values_to_lookup]}
#         lookup_needs = {}
#         lookup_mappings = {}  # Structure: {source_col: mapping_config}

#         for source_col, mapping in config['mappings'].items():
#             if mapping['type'] == 'lookup':
#                 lookup_key = (mapping['lookup_table'],
#                             mapping['lookup_key'], mapping['lookup_value'])
#                 lookup_needs.setdefault(lookup_key, set())
#                 lookup_mappings[source_col] = mapping

#         # Gather all unique values that need to be looked up
#         for row in batch_rows:
#             for source_col, mapping in lookup_mappings.items():
#                 value = row.get(source_col)
#                 if value is not None and (not isinstance(value, str) or value.strip()):
#                     lookup_key = (
#                         mapping['lookup_table'], mapping['lookup_key'], mapping['lookup_value'])
#                     lookup_needs[lookup_key].add(str(value))

#         # Perform bulk lookups for each unique table/column combination
#         # Structure: {(lookup_table, lookup_key, lookup_value): {key: value}}
#         lookup_results = {}
#         for lookup_key, values in lookup_needs.items():
#             lookup_table, lookup_key_col, lookup_value_col = lookup_key
#             lookup_results[lookup_key] = self.bulk_lookup_values(
#                 pg_cursor, lookup_table, lookup_key_col, lookup_value_col, list(
#                     values)
#             )

#         # Transform rows using the cached lookup values
#         transformed_rows = []
#         for row in batch_rows:
#             transformed_row = {}

#             for source_col, mapping in config['mappings'].items():
#                 value = row.get(source_col)

#                 if value is None or (isinstance(value, str) and not value.strip()):
#                     if mapping['type'] == 'lookup':
#                         # Skip None/empty values for lookups
#                         continue
#                     transformed_row[mapping['target'].lower()] = value
#                     continue

#                 if mapping['type'] == 'direct':
#                     transformed_row[mapping['target'].lower()] = value
#                 elif mapping['type'] == 'lookup':
#                     lookup_key = (
#                         mapping['lookup_table'], mapping['lookup_key'], mapping['lookup_value'])
#                     lookup_dict = lookup_results.get(lookup_key, {})
#                     lookup_result = lookup_dict.get(str(value))
#                     transformed_row[mapping['target'].lower()] = lookup_result

#             if transformed_row:  # Only add non-empty rows
#                 transformed_rows.append(transformed_row)

#         return transformed_rows

#     def get_last_sync_info(self, table_id: int) -> Tuple[datetime, Dict[str, str]]:
#         """Get the last sync time and row hashes"""
#         sync_log = self.env['etl.sync.log'].search([
#             ('table_id', '=', table_id),
#             ('status', '=', 'success')
#         ], order='create_date desc', limit=1)

#         if sync_log:
#             try:
#                 row_hashes = json.loads(sync_log.row_hashes or '{}')
#                 return sync_log.start_time, row_hashes
#             except json.JSONDecodeError:
#                 _logger.error(
#                     f"Failed to decode row hashes for table {table_id}")
#                 return sync_log.start_time, {}
#         return fields.Datetime.now(), {}

#     # def batch_update_rows(self, pg_cursor, config: dict, rows: List[Dict[str, Any]]):
#     #     """Batch update rows into the target PostgreSQL table"""
#     #     if not rows:
#     #         return

#     #     try:
#     #         # Get target table columns
#     #         pg_cursor.execute("""
#     #             SELECT column_name, data_type 
#     #             FROM information_schema.columns 
#     #             WHERE table_name = %s
#     #         """, (config['target_table'],))

#     #         table_columns = {row[0].lower(): row[1]
#     #                          for row in pg_cursor.fetchall()}

#     #         # Debug logging
#     #         _logger.debug(f"First row to update: {rows[0]}")
#     #         _logger.debug(
#     #             f"Available columns in target table: {table_columns}")

#     #         # Get columns from first row
#     #         columns = list(rows[0].keys())

#     #         # Validate columns
#     #         for col in columns:
#     #             if col.lower() not in table_columns:
#     #                 raise ValueError(
#     #                     f"Column {col} not found in table {config['target_table']}")

#     #         # Prepare SQL
#     #         column_names = ', '.join(f'"{col.lower()}"' for col in columns)
#     #         placeholders = ', '.join(['%s'] * len(columns))

#     #         # Get primary key mapping
#     #         primary_key_target = None
#     #         for source_col, mapping in config['mappings'].items():
#     #             if source_col.lower() == config['primary_key'].lower():
#     #                 primary_key_target = mapping['target'].lower()
#     #                 break

#     #         if not primary_key_target:
#     #             raise ValueError(
#     #                 f"Primary key mapping not found for {config['primary_key']}")

#     #         # Prepare update clause
#     #         update_sets = []
#     #         for col in columns:
#     #             if col.lower() != primary_key_target:
#     #                 update_sets.append(
#     #                     f'"{col.lower()}" = EXCLUDED."{col.lower()}"')
#     #         update_clause = ', '.join(update_sets)

#     #         # Final query
#     #         insert_query = f"""
#     #             INSERT INTO {config['target_table']} ({column_names})
#     #             VALUES ({placeholders})
#     #             ON CONFLICT ("{primary_key_target}")
#     #             DO UPDATE SET {update_clause}
#     #         """

#     #         _logger.debug(f"Executing query: {insert_query}")

#     #         # Execute in batches
#     #         batch_size = 4000
#     #         for i in range(0, len(rows), batch_size):
#     #             batch = rows[i:i + batch_size]
#     #             values = []
#     #             for row in batch:
#     #                 row_values = [row.get(col) for col in columns]
#     #                 values.append(row_values)

#     #             pg_cursor.executemany(insert_query, values)

#     #     except Exception as e:
#     #         _logger.error(f"Error in batch_update_rows: {str(e)}")
#     #         _logger.error(f"Target table: {config['target_table']}")
#     #         _logger.error(f"Columns being updated: {columns}")
#     #         raise

    
#     def optimized_batch_update_rows(self, pg_cursor, config: dict, rows: List[Dict[str, Any]]):
#         """Optimized batch update with better prepared statements and memory management"""
#         if not rows:
#             return

#         try:
#             # Get column information once and cache it
#             table_name = config['target_table']
#             primary_key_target = None

#             # Find primary key target column
#             for source_col, mapping in config['mappings'].items():
#                 if source_col.lower() == config['primary_key'].lower():
#                     primary_key_target = mapping['target'].lower()
#                     break

#             if not primary_key_target:
#                 raise ValueError(
#                     f"Primary key mapping not found for {config['primary_key']}")

#             # Get existing columns in first row
#             columns = list(rows[0].keys())

#             # Verify columns exist in target table
#             column_query = """
#                 SELECT column_name, data_type 
#                 FROM information_schema.columns 
#                 WHERE table_name = %s
#                 AND column_name = ANY(%s)
#             """
#             pg_cursor.execute(column_query, (table_name, [
#                             col.lower() for col in columns]))
#             valid_columns = {row[0].lower(): row[1]
#                             for row in pg_cursor.fetchall()}

#             # Only use columns that exist in the target table
#             filtered_columns = [
#                 col for col in columns if col.lower() in valid_columns]

#             if not filtered_columns:
#                 _logger.error(f"No valid columns found for table {table_name}")
#                 return

#             if primary_key_target.lower() not in [col.lower() for col in filtered_columns]:
#                 filtered_columns.append(primary_key_target)

#             # Use optimized batch size
#             batch_size = 5000

#             # Create a more efficient query with prepared statement
#             column_names = ', '.join(
#                 f'"{col.lower()}"' for col in filtered_columns)
#             placeholders = ', '.join(['%s'] * len(filtered_columns))

#             # Prepare update clause
#             update_sets = []
#             for col in filtered_columns:
#                 if col.lower() != primary_key_target.lower():
#                     update_sets.append(
#                         f'"{col.lower()}" = EXCLUDED."{col.lower()}"')

#             if not update_sets:
#                 # Only have primary key, nothing to update
#                 _logger.warning(f"No columns to update for table {table_name}")
#                 return

#             update_clause = ', '.join(update_sets)

#             # Final query
#             insert_query = f"""
#                 INSERT INTO {table_name} ({column_names})
#                 VALUES ({placeholders})
#                 ON CONFLICT ("{primary_key_target.lower()}")
#                 DO UPDATE SET {update_clause}
#             """

#             _logger.debug(
#                 f"Executing optimized batch update query: {insert_query}")

#             # Execute in optimized batches
#             for i in range(0, len(rows), batch_size):
#                 batch = rows[i:i + batch_size]

#                 # Create batch values with memory efficiency
#                 batch_values = []
#                 for row in batch:
#                     row_values = []
#                     for col in filtered_columns:
#                         row_values.append(row.get(col))
#                     batch_values.append(tuple(row_values))

#                 # Execute the batch
#                 pg_cursor.executemany(insert_query, batch_values)

#                 # Log progress
#                 _logger.debug(
#                     f"Processed batch {i//batch_size + 1} with {len(batch)} rows")

#         except Exception as e:
#             _logger.error(f"Error in optimized_batch_update_rows: {str(e)}")
#             _logger.error(f"Target table: {config['target_table']}")
#             if 'columns' in locals():
#                 _logger.error(f"Attempted columns: ")
#                 # _logger.error(f"Attempted columns: {columns}")
#             raise

    
    
#     # def process_table(self, table_config):
#     #     """Process a single table with pagination for large datasets"""
#     #     table_id = table_config.id
#     #     start_time = fields.Datetime.now()

#     #     # Create sync log entry
#     #     sync_log = self.env['etl.sync.log'].create({
#     #         'table_id': table_id,
#     #         'start_time': start_time,
#     #         'status': 'running'
#     #     })

#     #     try:
#     #         # Process dependencies first
#     #         for dep in table_config.dependency_ids:
#     #             if not self.is_table_processed(dep.id):
#     #                 self.process_table(dep)

#     #         with self.get_connections() as (source_conn, pg_conn):
#     #             source_cursor = source_conn.cursor()
#     #             pg_cursor = pg_conn.cursor()

#     #             config = table_config.get_config_json()
#     #             _logger.info(f"Processing table with config: {config}")

#     #             last_sync_time, last_hashes = self.get_last_sync_info(table_id)

#     #             # Determine source DB type for query syntax
#     #             is_postgres_source = isinstance(
#     #                 source_conn, psycopg2.extensions.connection)
#     #             table_delimiter = '"' if is_postgres_source else '['
#     #             table_delimiter_end = '"' if is_postgres_source else ']'

#     #             # Get source columns with proper case handling
#     #             query = f"SELECT TOP 0 * FROM {table_delimiter}{config['source_table']}{table_delimiter_end}" if not is_postgres_source else f'SELECT * FROM "{config["source_table"]}" LIMIT 0'
#     #             source_cursor.execute(query)
#     #             source_columns = {col[0].lower(): col[0]
#     #                               for col in source_cursor.description}

#     #             # Prepare query columns
#     #             query_columns = []
#     #             column_map = {}  # Map for translating result set back
#     #             for source_col in config['mappings'].keys():
#     #                 original_col = source_columns.get(source_col.lower())
#     #                 if original_col:
#     #                     query_columns.append(original_col)
#     #                     column_map[original_col] = source_col

#     #             # Ensure the primary key is included in the columns
#     #             primary_key_original = source_columns.get(
#     #                 config['primary_key'].lower())
#     #             if primary_key_original and primary_key_original not in query_columns:
#     #                 query_columns.append(primary_key_original)
#     #                 column_map[primary_key_original] = config['primary_key']

#     #             # Stats to track progress
#     #             current_hashes = {}
#     #             stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}

#     #             # Count total records for progress tracking
#     #             try:
#     #                 count_query = f"SELECT COUNT(*) FROM {table_delimiter}{config['source_table']}{table_delimiter_end}"
#     #                 source_cursor.execute(count_query)
#     #                 total_count = source_cursor.fetchone()[0]
#     #                 _logger.info(
#     #                     f"Total records in source table: {total_count}")
#     #             except Exception as e:
#     #                 _logger.warning(
#     #                     f"Could not get count, using standard processing: {str(e)}")
#     #                 total_count = None

#     #             # Process in smaller batches
#     #             # Use smaller batches
#     #             batch_size = min(config['batch_size'], 5000)

#     #             # For tables with a manageable number of rows, process all at once
#     #             if total_count and total_count < 20000:
#     #                 # Query all at once for smaller tables
#     #                 cols = ', '.join(
#     #                     [f'{table_delimiter}{col}{table_delimiter_end}' for col in query_columns])
#     #                 query = f"SELECT {cols} FROM {table_delimiter}{config['source_table']}{table_delimiter_end}"
#     #                 _logger.info(f"Executing query for small table: {query}")
#     #                 source_cursor.execute(query)

#     #                 rows_to_update = []
#     #                 for row in source_cursor.fetchall():
#     #                     # Create row dict using column map
#     #                     row_dict = {
#     #                         column_map[col]: val
#     #                         for col, val in zip(query_columns, row)
#     #                     }

#     #                     # Transform values
#     #                     transformed_row = {}
#     #                     for source_col, mapping in config['mappings'].items():
#     #                         source_value = row_dict.get(source_col)
#     #                         if source_value is not None:
#     #                             transformed_value = self.transform_value(
#     #                                 mapping, source_value, pg_cursor
#     #                             )
#     #                             transformed_row[mapping['target'].lower(
#     #                             )] = transformed_value

#     #                     if transformed_row:  # Only process if we have values
#     #                         row_hash = self.calculate_row_hash(transformed_row)
#     #                         pk_value = str(row_dict[config['primary_key']])
#     #                         current_hashes[pk_value] = row_hash

#     #                         # Add to update batch if new or changed
#     #                         if pk_value not in last_hashes:
#     #                             stats['new_rows'] += 1
#     #                             rows_to_update.append(transformed_row)
#     #                         elif last_hashes[pk_value] != row_hash:
#     #                             stats['updated_rows'] += 1
#     #                             rows_to_update.append(transformed_row)

#     #                         stats['total_rows'] += 1

#     #                     # Update in smaller batches
#     #                     if len(rows_to_update) >= 1000:
#     #                         self.batch_update_rows(
#     #                             pg_cursor, config, rows_to_update)
#     #                         pg_conn.commit()  # Commit each batch
#     #                         rows_to_update = []

#     #                 # Final batch update
#     #                 if rows_to_update:
#     #                     self.batch_update_rows(
#     #                         pg_cursor, config, rows_to_update)
#     #                     pg_conn.commit()

#     #             else:
#     #                 # For larger tables, process in batches
#     #                 processed = 0
#     #                 last_pk_value = None

#     #                 while True:
#     #                     # Build query with pagination
#     #                     cols = ', '.join(
#     #                         [f'{table_delimiter}{col}{table_delimiter_end}' for col in query_columns])
#     #                     if last_pk_value is None:
#     #                         # First batch
#     #                         if is_postgres_source:
#     #                             batch_query = f"""
#     #                                 SELECT {cols} 
#     #                                 FROM {table_delimiter}{config['source_table']}{table_delimiter_end}
#     #                                 ORDER BY {table_delimiter}{primary_key_original}{table_delimiter_end}
#     #                                 LIMIT {batch_size}
#     #                             """
#     #                         else:
#     #                             batch_query = f"""
#     #                                 SELECT TOP {batch_size} {cols} 
#     #                                 FROM {table_delimiter}{config['source_table']}{table_delimiter_end}
#     #                                 ORDER BY {table_delimiter}{primary_key_original}{table_delimiter_end}
#     #                             """
#     #                     else:
#     #                         # Subsequent batches - get next set of records
#     #                         if is_postgres_source:
#     #                             batch_query = f"""
#     #                                 SELECT {cols} 
#     #                                 FROM {table_delimiter}{config['source_table']}{table_delimiter_end}
#     #                                 WHERE {table_delimiter}{primary_key_original}{table_delimiter_end} > %s
#     #                                 ORDER BY {table_delimiter}{primary_key_original}{table_delimiter_end}
#     #                                 LIMIT {batch_size}
#     #                             """
#     #                             source_cursor.execute(
#     #                                 batch_query, (last_pk_value,))
#     #                         else:
#     #                             batch_query = f"""
#     #                                 SELECT TOP {batch_size} {cols} 
#     #                                 FROM {table_delimiter}{config['source_table']}{table_delimiter_end}
#     #                                 WHERE {table_delimiter}{primary_key_original}{table_delimiter_end} > '{last_pk_value}'
#     #                                 ORDER BY {table_delimiter}{primary_key_original}{table_delimiter_end}
#     #                             """
#     #                             source_cursor.execute(batch_query)

#     #                     _logger.info(f"Executing batch query: {batch_query}")
#     #                     if is_postgres_source and last_pk_value is not None:
#     #                         rows = source_cursor.fetchall()
#     #                     else:
#     #                         source_cursor.execute(batch_query)
#     #                         rows = source_cursor.fetchall()

#     #                     if not rows:
#     #                         break  # No more data

#     #                     rows_to_update = []
#     #                     batch_count = 0

#     #                     for row in rows:
#     #                         batch_count += 1

#     #                         # Create row dict using column map
#     #                         row_dict = {
#     #                             column_map[col]: val
#     #                             for col, val in zip(query_columns, row)
#     #                         }

#     #                         # Keep track of the last primary key value for pagination
#     #                         last_pk_value = row_dict[config['primary_key']]

#     #                         # Transform values
#     #                         transformed_row = {}
#     #                         for source_col, mapping in config['mappings'].items():
#     #                             source_value = row_dict.get(source_col)
#     #                             if source_value is not None:
#     #                                 transformed_value = self.transform_value(
#     #                                     mapping, source_value, pg_cursor
#     #                                 )
#     #                                 transformed_row[mapping['target'].lower(
#     #                                 )] = transformed_value

#     #                         if transformed_row:  # Only process if we have values
#     #                             row_hash = self.calculate_row_hash(
#     #                                 transformed_row)
#     #                             pk_value = str(row_dict[config['primary_key']])
#     #                             current_hashes[pk_value] = row_hash

#     #                             # Add to update batch if new or changed
#     #                             if pk_value not in last_hashes:
#     #                                 stats['new_rows'] += 1
#     #                                 rows_to_update.append(transformed_row)
#     #                             elif last_hashes[pk_value] != row_hash:
#     #                                 stats['updated_rows'] += 1
#     #                                 rows_to_update.append(transformed_row)

#     #                             stats['total_rows'] += 1

#     #                     # Update in smaller batches
#     #                     if rows_to_update:
#     #                         self.batch_update_rows(
#     #                             pg_cursor, config, rows_to_update)
#     #                         pg_conn.commit()  # Commit each batch

#     #                     # Update progress
#     #                     processed += batch_count
#     #                     if total_count:
#     #                         progress = round(
#     #                             100.0 * processed / total_count, 2)
#     #                         _logger.info(
#     #                             f"Progress: {progress}% - Processed {processed} of {total_count} rows")
#     #                     else:
#     #                         _logger.info(f"Processed {processed} rows so far")

#     #                     # If we got fewer rows than the batch size, we're done
#     #                     if batch_count < batch_size:
#     #                         break

#     #                     # Update sync log periodically
#     #                     if stats['total_rows'] % 50000 == 0:
#     #                         sync_log.write({
#     #                             'status': 'running',
#     #                             'total_records': stats['total_rows'],
#     #                             'new_records': stats['new_rows'],
#     #                             'updated_records': stats['updated_rows']
#     #                         })

#     #             # Update sync log
#     #             sync_log.write({
#     #                 'end_time': fields.Datetime.now(),
#     #                 'status': 'success',
#     #                 'total_records': stats['total_rows'],
#     #                 'new_records': stats['new_rows'],
#     #                 'updated_records': stats['updated_rows'],
#     #                 'row_hashes': json.dumps(current_hashes)
#     #             })

#     #             # Update table status
#     #             table_config.write({
#     #                 'last_sync_time': fields.Datetime.now(),
#     #                 'last_sync_status': 'success',
#     #                 'total_records_synced': stats['total_rows']
#     #             })

#     #             # Mark as processed
#     #             self.mark_table_processed(table_id)

#     #     except Exception as e:
#     #         error_message = str(e)
#     #         _logger.error(
#     #             f"Error processing table {table_config.name}: {error_message}")

#     #         sync_log.write({
#     #             'end_time': fields.Datetime.now(),
#     #             'status': 'failed',
#     #             'error_message': error_message
#     #         })

#     #         table_config.write({
#     #             'last_sync_status': 'failed',
#     #             'last_sync_message': error_message
#     #         })

#     #         raise

#     def improved_process_table(self, table_config):
#         """Significantly improved table processing with better batch handling and optimizations"""
#         table_id = table_config.id
#         start_time = fields.Datetime.now()

#         # Create sync log entry
#         sync_log = self.env['etl.sync.log'].create({
#             'table_id': table_id,
#             'start_time': start_time,
#             'status': 'running'
#         })

#         try:
#             # Process dependencies first
#             for dep in table_config.dependency_ids:
#                 if not self.is_table_processed(dep.id):
#                     # self.process_table(dep)
#                     self.improved_process_table(dep)

#             with self.get_connections() as (source_conn, pg_conn):
#                 source_cursor = source_conn.cursor()
#                 pg_cursor = pg_conn.cursor()

#                 config = table_config.get_config_json()
#                 _logger.info(f"Processing table with config: {config}")

#                 last_sync_time, last_hashes = self.get_last_sync_info(table_id)

#                 # Determine source DB type for query syntax
#                 is_postgres_source = isinstance(
#                     source_conn, psycopg2.extensions.connection)
#                 table_delimiter = '"' if is_postgres_source else '['
#                 table_delimiter_end = '"' if is_postgres_source else ']'

#                 # Get source columns with proper case handling
#                 query = f"SELECT TOP 0 * FROM {table_delimiter}{config['source_table']}{table_delimiter_end}" if not is_postgres_source else f'SELECT * FROM "{config["source_table"]}" LIMIT 0'
#                 source_cursor.execute(query)
#                 source_columns = {col[0].lower(): col[0]
#                                 for col in source_cursor.description}

#                 # Prepare query columns
#                 query_columns = []
#                 column_map = {}  # Map for translating result set back
#                 for source_col in config['mappings'].keys():
#                     original_col = source_columns.get(source_col.lower())
#                     if original_col:
#                         query_columns.append(original_col)
#                         column_map[original_col] = source_col

#                 # Ensure the primary key is included in the columns
#                 primary_key_original = source_columns.get(
#                     config['primary_key'].lower())
#                 if primary_key_original and primary_key_original not in query_columns:
#                     query_columns.append(primary_key_original)
#                     column_map[primary_key_original] = config['primary_key']

#                 # Stats to track progress
#                 current_hashes = {}
#                 stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}

#                 # Count total records for progress tracking
#                 try:
#                     count_query = f"SELECT COUNT(*) FROM {table_delimiter}{config['source_table']}{table_delimiter_end}"
#                     source_cursor.execute(count_query)
#                     total_count = source_cursor.fetchone()[0]
#                     _logger.info(f"Total records in source table: {total_count}")
#                 except Exception as e:
#                     _logger.warning(
#                         f"Could not get count, using standard processing: {str(e)}")
#                     total_count = None

#                 # Optimize batch size based on table size
#                 if total_count is not None:
#                     if total_count < 1000:
#                         batch_size = total_count  # Process all at once for tiny tables
#                     elif total_count < 10000:
#                         batch_size = 5000
#                     elif total_count < 100000:
#                         batch_size = 10000
#                     else:
#                         batch_size = 20000  # Large batches for big tables
#                 else:
#                     batch_size = 10000  # Default batch size

#                 batch_size = min(batch_size, config.get('batch_size', 10000))

#                 # For very small tables, process all at once
#                 if total_count and total_count < 5000:
#                     # Query all at once for smaller tables
#                     cols = ', '.join(
#                         [f'{table_delimiter}{col}{table_delimiter_end}' for col in query_columns])
#                     query = f"SELECT {cols} FROM {table_delimiter}{config['source_table']}{table_delimiter_end}"
#                     _logger.info(f"Executing query for small table: {query}")
#                     source_cursor.execute(query)

#                     raw_rows = []
#                     for row in source_cursor.fetchall():
#                         # Create row dict using column map
#                         row_dict = {
#                             column_map[col]: val
#                             for col, val in zip(query_columns, row)
#                         }
#                         raw_rows.append(row_dict)

#                     # Transform all rows at once using batch transform
#                     transformed_rows = self.batch_transform_values(
#                         pg_cursor, raw_rows, config)

#                     # Process hashes and filtering
#                     rows_to_update = []
#                     for transformed_row in transformed_rows:
#                         if transformed_row:  # Only process if we have values
#                             # Find the original row for the primary key
#                             pk_original = None
#                             for raw_row in raw_rows:
#                                 if raw_row[config['primary_key']] == transformed_row.get(config['primary_key'].lower()):
#                                     pk_original = raw_row[config['primary_key']]
#                                     break

#                             if pk_original is None:
#                                 continue

#                             # Calculate hash and compare
#                             row_hash = self.calculate_row_hash(transformed_row)
#                             pk_value = str(pk_original)
#                             current_hashes[pk_value] = row_hash

#                             # Add to update batch if new or changed
#                             if pk_value not in last_hashes:
#                                 stats['new_rows'] += 1
#                                 rows_to_update.append(transformed_row)
#                             elif last_hashes[pk_value] != row_hash:
#                                 stats['updated_rows'] += 1
#                                 rows_to_update.append(transformed_row)

#                             stats['total_rows'] += 1

#                     # Update all rows at once
#                     if rows_to_update:
#                         self.optimized_batch_update_rows(
#                             pg_cursor, config, rows_to_update)
#                         pg_conn.commit()
#                 else:
#                     # For larger tables, process in optimized batches
#                     processed = 0
#                     last_pk_value = None

#                     while True:
#                         # Build query with pagination
#                         cols = ', '.join(
#                             [f'{table_delimiter}{col}{table_delimiter_end}' for col in query_columns])
#                         if last_pk_value is None:
#                             # First batch
#                             if is_postgres_source:
#                                 batch_query = f"""
#                                     SELECT {cols} 
#                                     FROM {table_delimiter}{config['source_table']}{table_delimiter_end}
#                                     ORDER BY {table_delimiter}{primary_key_original}{table_delimiter_end}
#                                     LIMIT {batch_size}
#                                 """
#                             else:
#                                 batch_query = f"""
#                                     SELECT TOP {batch_size} {cols} 
#                                     FROM {table_delimiter}{config['source_table']}{table_delimiter_end}
#                                     ORDER BY {table_delimiter}{primary_key_original}{table_delimiter_end}
#                                 """
#                             source_cursor.execute(batch_query)
#                         else:
#                             # Subsequent batches - get next set of records
#                             if is_postgres_source:
#                                 batch_query = f"""
#                                     SELECT {cols} 
#                                     FROM {table_delimiter}{config['source_table']}{table_delimiter_end}
#                                     WHERE {table_delimiter}{primary_key_original}{table_delimiter_end} > %s
#                                     ORDER BY {table_delimiter}{primary_key_original}{table_delimiter_end}
#                                     LIMIT {batch_size}
#                                 """
#                                 source_cursor.execute(
#                                     batch_query, (last_pk_value,))
#                             else:
#                                 batch_query = f"""
#                                     SELECT TOP {batch_size} {cols} 
#                                     FROM {table_delimiter}{config['source_table']}{table_delimiter_end}
#                                     WHERE {table_delimiter}{primary_key_original}{table_delimiter_end} > '{last_pk_value}'
#                                     ORDER BY {table_delimiter}{primary_key_original}{table_delimiter_end}
#                                 """
#                                 source_cursor.execute(batch_query)

#                         rows = source_cursor.fetchall()
#                         if not rows:
#                             break  # No more data

#                         batch_count = len(rows)
#                         raw_rows = []

#                         for row in rows:
#                             # Create row dict using column map
#                             row_dict = {
#                                 column_map[col]: val
#                                 for col, val in zip(query_columns, row)
#                             }
#                             raw_rows.append(row_dict)

#                             # Keep track of the last primary key value for pagination
#                             last_pk_value = row_dict[config['primary_key']]

#                         # Transform all rows in the batch at once
#                         transformed_rows = self.batch_transform_values(
#                             pg_cursor, raw_rows, config)

#                         # Process hashes and filtering
#                         rows_to_update = []
#                         for transformed_row in transformed_rows:
#                             if transformed_row:  # Only process if we have values
#                                 # Find the primary key in the original data
#                                 pk_value = None
#                                 for raw_row in raw_rows:
#                                     for source_col, mapping in config['mappings'].items():
#                                         if source_col.lower() == config['primary_key'].lower():
#                                             target_col = mapping['target'].lower()
#                                             if target_col in transformed_row and transformed_row[target_col] == raw_row[config['primary_key']]:
#                                                 pk_value = str(
#                                                     raw_row[config['primary_key']])
#                                                 break
#                                     if pk_value:
#                                         break

#                                 if not pk_value:
#                                     continue

#                                 # Calculate hash and compare
#                                 row_hash = self.calculate_row_hash(transformed_row)
#                                 current_hashes[pk_value] = row_hash

#                                 # Add to update batch if new or changed
#                                 if pk_value not in last_hashes:
#                                     stats['new_rows'] += 1
#                                     rows_to_update.append(transformed_row)
#                                 elif last_hashes[pk_value] != row_hash:
#                                     stats['updated_rows'] += 1
#                                     rows_to_update.append(transformed_row)

#                                 stats['total_rows'] += 1

#                         # Update this batch
#                         if rows_to_update:
#                             self.optimized_batch_update_rows(
#                                 pg_cursor, config, rows_to_update)
#                             pg_conn.commit()  # Commit each batch

#                         # Update progress
#                         processed += batch_count
#                         if total_count:
#                             progress = round(100.0 * processed / total_count, 2)
#                             _logger.info(
#                                 f"Progress: {progress}% - Processed {processed} of {total_count} rows")
#                         else:
#                             _logger.info(f"Processed {processed} rows so far")

#                         # Update sync log periodically
#                         if stats['total_rows'] % 50000 == 0:
#                             sync_log.write({
#                                 'status': 'running',
#                                 'total_records': stats['total_rows'],
#                                 'new_records': stats['new_rows'],
#                                 'updated_records': stats['updated_rows']
#                             })

#                 # Update sync log
#                 sync_log.write({
#                     'end_time': fields.Datetime.now(),
#                     'status': 'success',
#                     'total_records': stats['total_rows'],
#                     'new_records': stats['new_rows'],
#                     'updated_records': stats['updated_rows'],
#                     'row_hashes': json.dumps(current_hashes)
#                 })

#                 # Update table status
#                 table_config.write({
#                     'last_sync_time': fields.Datetime.now(),
#                     'last_sync_status': 'success',
#                     'total_records_synced': stats['total_rows']
#                 })

#                 # Mark as processed
#                 self.mark_table_processed(table_id)

#                 return stats

#         except Exception as e:
#             error_message = str(e)
#             _logger.error(
#                 f"Error processing table {table_config.name}: {error_message}")

#             sync_log.write({
#                 'end_time': fields.Datetime.now(),
#                 'status': 'failed',
#                 'error_message': error_message
#             })

#             table_config.write({
#                 'last_sync_status': 'failed',
#                 'last_sync_message': error_message
#             })

#             raise
    
    
#     def process_table_chunk(self, table_config, min_id, max_id):
#         """Process a specific chunk of a table with ID range"""
#         table_id = table_config.id
#         start_time = fields.Datetime.now()

#         # Create sync log entry for this chunk
#         sync_log = self.env['etl.sync.log'].create({
#             'table_id': table_id,
#             'start_time': start_time,
#             'status': 'running'
#         })

#         try:
#             # Process dependencies if they haven't been processed yet
#             for dep in table_config.dependency_ids:
#                 if not self.is_table_processed(dep.id):
#                     # self.process_table(dep)
#                     self.improved_process_table(dep)

#             with self.get_connections() as (source_conn, pg_conn):
#                 source_cursor = source_conn.cursor()
#                 pg_cursor = pg_conn.cursor()

#                 config = table_config.get_config_json()
#                 _logger.info(f"Processing table chunk with config: {config}")
#                 _logger.info(f"ID range: {min_id} to {max_id}")

#                 last_sync_time, last_hashes = self.get_last_sync_info(table_id)

#                 # Determine source DB type for query syntax
#                 is_postgres_source = isinstance(
#                     source_conn, psycopg2.extensions.connection)
#                 table_delimiter = '"' if is_postgres_source else '['
#                 table_delimiter_end = '"' if is_postgres_source else ']'

#                 # Get source columns with proper case handling
#                 query = (
#                     f"SELECT TOP 0 * FROM {table_delimiter}{config['source_table']}{table_delimiter_end}"
#                     if not is_postgres_source
#                     else f'SELECT * FROM "{config["source_table"]}" LIMIT 0'
#                 )
#                 source_cursor.execute(query)
#                 source_columns = {col[0].lower(): col[0]
#                                   for col in source_cursor.description}

#                 # Prepare query columns
#                 query_columns = []
#                 column_map = {}  # Map for translating result set back
#                 for source_col in config['mappings'].keys():
#                     original_col = source_columns.get(source_col.lower())
#                     if original_col:
#                         query_columns.append(original_col)
#                         column_map[original_col] = source_col

#                 # Ensure the primary key is included in the columns
#                 primary_key_original = source_columns.get(
#                     config['primary_key'].lower())
#                 if primary_key_original and primary_key_original not in query_columns:
#                     query_columns.append(primary_key_original)
#                     column_map[primary_key_original] = config['primary_key']

#                 # Stats to track progress
#                 current_hashes = {}
#                 stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}

#                 # Count total records in this chunk for progress tracking
#                 try:
#                     count_query = f"""
#                         SELECT COUNT(*) FROM {table_delimiter}{config['source_table']}{table_delimiter_end}
#                         WHERE {table_delimiter}{primary_key_original}{table_delimiter_end} >= %s
#                         AND {table_delimiter}{primary_key_original}{table_delimiter_end} <= %s
#                     """ if is_postgres_source else f"""
#                         SELECT COUNT(*) FROM [{config['source_table']}]
#                         WHERE [{primary_key_original}] >= '{min_id}'
#                         AND [{primary_key_original}] <= '{max_id}'
#                     """
#                     if is_postgres_source:
#                         source_cursor.execute(count_query, (min_id, max_id))
#                     else:
#                         source_cursor.execute(count_query)
#                     chunk_total_count = source_cursor.fetchone()[0]
#                     _logger.info(
#                         f"Total records in chunk: {chunk_total_count}")
#                 except Exception as e:
#                     _logger.warning(
#                         f"Could not get chunk count, using standard processing: {str(e)}")
#                     chunk_total_count = None

#                 # Process in smaller batches
#                 # Use smaller batches
#                 batch_size = min(config['batch_size'], 5000)
#                 processed = 0
#                 last_pk_value = min_id  # Start from min_id

#                 while True:
#                     # Build query with pagination within the chunk
#                     cols = ', '.join(
#                         [f'{table_delimiter}{col}{table_delimiter_end}' for col in query_columns])
#                     if is_postgres_source:
#                         batch_query = f"""
#                             SELECT {cols} 
#                             FROM {table_delimiter}{config['source_table']}{table_delimiter_end}
#                             WHERE {table_delimiter}{primary_key_original}{table_delimiter_end} >= %s
#                             AND {table_delimiter}{primary_key_original}{table_delimiter_end} <= %s
#                             ORDER BY {table_delimiter}{primary_key_original}{table_delimiter_end}
#                             LIMIT {batch_size}
#                         """
#                         source_cursor.execute(
#                             batch_query, (last_pk_value, max_id))
#                     else:
#                         batch_query = f"""
#                             SELECT TOP {batch_size} {cols} 
#                             FROM {table_delimiter}{config['source_table']}{table_delimiter_end}
#                             WHERE {table_delimiter}{primary_key_original}{table_delimiter_end} >= '{last_pk_value}'
#                             AND {table_delimiter}{primary_key_original}{table_delimiter_end} <= '{max_id}'
#                             ORDER BY {table_delimiter}{primary_key_original}{table_delimiter_end}
#                         """
#                         source_cursor.execute(batch_query)

#                     _logger.info(f"Executing chunk batch query: {batch_query}")
#                     rows = source_cursor.fetchall()
#                     if not rows:
#                         break  # No more data in this chunk

#                     rows_to_update = []
#                     batch_count = 0

#                     for row in rows:
#                         batch_count += 1

#                         # Create row dict using column map
#                         row_dict = {
#                             column_map[col]: val
#                             for col, val in zip(query_columns, row)
#                         }

#                         # Keep track of the last primary key value for pagination
#                         last_pk_value = row_dict[config['primary_key']]

#                         # Transform values
#                         transformed_row = {}
#                         for source_col, mapping in config['mappings'].items():
#                             source_value = row_dict.get(source_col)
#                             if source_value is not None:
                              
#                                 transformed_value = self.transform_value(
#                                     mapping, source_value, pg_cursor
#                                 )
#                                 transformed_row[mapping['target'].lower(
#                                 )] = transformed_value

#                         if transformed_row:  # Only process if we have values
#                             row_hash = self.calculate_row_hash(transformed_row)
#                             pk_value = str(row_dict[config['primary_key']])
#                             current_hashes[pk_value] = row_hash

#                             # Add to update batch if new or changed
#                             if pk_value not in last_hashes:
#                                 stats['new_rows'] += 1
#                                 rows_to_update.append(transformed_row)
#                             elif last_hashes[pk_value] != row_hash:
#                                 stats['updated_rows'] += 1
#                                 rows_to_update.append(transformed_row)

#                             stats['total_rows'] += 1

#                     # Update in smaller batches
#                     if rows_to_update:
#                         self.optimized_batch_update_rows(
#                             pg_cursor, config, rows_to_update)
#                         pg_conn.commit()  # Commit each batch

#                     # Update progress
#                     processed += batch_count
#                     if chunk_total_count:
#                         progress = round(100.0 * processed /
#                                          chunk_total_count, 2)
#                         _logger.info(
#                             f"Chunk progress: {progress}% - Processed {processed} of {chunk_total_count} rows")
#                     else:
#                         _logger.info(
#                             f"Processed {processed} rows in chunk so far")

#                     # If we got fewer rows than the batch size or reached the max_id, we're done
#                     if batch_count < batch_size or str(last_pk_value) >= str(max_id):
#                         break

#                     # Update sync log periodically
#                     if stats['total_rows'] % 20000 == 0:
#                         sync_log.write({
#                             'status': 'running',
#                             'total_records': stats['total_rows'],
#                             'new_records': stats['new_rows'],
#                             'updated_records': stats['updated_rows']
#                         })

#                 # Update sync log for this chunk
#                 sync_log.write({
#                     'end_time': fields.Datetime.now(),
#                     'status': 'success',
#                     'total_records': stats['total_rows'],
#                     'new_records': stats['new_rows'],
#                     'updated_records': stats['updated_rows'],
#                     'row_hashes': json.dumps(current_hashes)
#                 })

#                 # Update table status with chunk info
#                 table_config.write({
#                     'last_sync_time': fields.Datetime.now(),
#                     'last_sync_status': 'success',
#                     'last_sync_message': f'Successfully processed chunk from {min_id} to {max_id}',
#                     'total_records_synced': table_config.total_records_synced + stats['total_rows']
#                 })

#                 return stats

#         except Exception as e:
#             error_message = str(e)
#             _logger.error(
#                 f"Error processing table chunk {table_config.name}: {error_message}")

#             sync_log.write({
#                 'end_time': fields.Datetime.now(),
#                 'status': 'failed',
#                 'error_message': error_message
#             })

#             table_config.write({
#                 'last_sync_status': 'failed',
#                 'last_sync_message': f'Failed to process chunk from {min_id} to {max_id}: {error_message}'
#             })

#             raise

#     @api.model
#     def run_scheduled_sync(self, frequency_code='daily'):
#         """Enhanced scheduled synchronization with optimized processing strategies"""
#         self.clear_lookup_cache()
#         self.clear_processed_tables()

#         # Initialize connection pools if available
#         if hasattr(self, 'init_connection_pools'):
#             try:
#                 self.init_connection_pools()
#             except Exception as e:
#                 _logger.warning(
#                     f"Connection pool initialization failed: {str(e)}")

#         frequency = self.env['etl.frequency'].search(
#             [('code', '=', frequency_code)], limit=1)
#         if not frequency:
#             _logger.error(f"Frequency '{frequency_code}' not found")
#             return

#         tables = self.env['etl.source.table'].search([
#             ('frequency_id', '=', frequency.id),
#             ('active', '=', True)
#         ])

#         # Group tables by dependency to process base tables first
#         base_tables = tables.filtered(lambda t: t.is_base_table)
#         dependent_tables = tables - base_tables

#         _logger.info(f"Scheduled sync for frequency {frequency_code}: {len(tables)} tables "
#                      f"({len(base_tables)} base tables, {len(dependent_tables)} dependent tables)")

#         processed_count = 0

#         # Process base tables first
#         for table in base_tables:
#             try:
#                 processed_count += 1
#                 _logger.info(
#                     f"Queueing base table {processed_count}/{len(base_tables)}: {table.name}")

#                 # Determine optimal processing strategy based on table size and settings
#                 try:
#                     # Try to get table size to make an informed decision
#                     with self.get_connections() as (source_conn, pg_conn):
#                         config = table.get_config_json()
#                         source_cursor = source_conn.cursor()

#                         # Determine source DB type
#                         is_postgres_source = isinstance(
#                             source_conn, psycopg2.extensions.connection)
#                         table_delimiter = '"' if is_postgres_source else '['
#                         table_delimiter_end = '"' if is_postgres_source else ']'

#                         # Count records
#                         count_query = f"SELECT COUNT(*) FROM {table_delimiter}{config['source_table']}{table_delimiter_end}"
#                         source_cursor.execute(count_query)
#                         total_count = source_cursor.fetchone()[0]

#                         # if table.use_parallel_processing and total_count > 50000:
#                         if table and total_count > 50000:
#                             # Use parallel processing for large tables
#                             num_workers = min(8, max(2, total_count // 100000))

#                             table.with_delay(
#                                 description=f"Parallel sync for table: {table.name}  workers)",
#                                 channel='etl',
#                                 priority=10
#                             ).parallel_process_table_job()

#                             table.write({
#                                 'job_status': 'pending',
#                                 'last_sync_message': f'Scheduled parallel sync with  workers'
#                             })
#                         elif total_count > 500000:
#                             # Use chunking for very large tables
#                             chunk_size = 200000
#                             chunks = math.ceil(total_count / chunk_size)

#                             # Create a main tracking job
#                             main_job = table.with_delay(
#                                 description=f"Main sync job for table: {table.name}",
#                                 channel='etl_main'
#                             ).sync_table_job_main(chunks)

#                             table.write({
#                                 'job_uuid': main_job.uuid,
#                                 'job_status': 'pending'
#                             })

#                             # Get ID range for chunking
#                             primary_key = config['primary_key']
#                             primary_key_original = None

#                             # Get original column name
#                             query = f"SELECT TOP 1 * FROM {table_delimiter}{config['source_table']}{table_delimiter_end}" if not is_postgres_source else f'SELECT * FROM "{config["source_table"]}" LIMIT 1'
#                             source_cursor.execute(query)
#                             for col in source_cursor.description:
#                                 if col[0].lower() == primary_key.lower():
#                                     primary_key_original = col[0]
#                                     break

#                             # Get min/max for chunking
#                             min_query = f"SELECT MIN({table_delimiter}{primary_key_original}{table_delimiter_end}) FROM {table_delimiter}{config['source_table']}{table_delimiter_end}"
#                             max_query = f"SELECT MAX({table_delimiter}{primary_key_original}{table_delimiter_end}) FROM {table_delimiter}{config['source_table']}{table_delimiter_end}"
#                             source_cursor.execute(min_query)
#                             min_id = source_cursor.fetchone()[0]
#                             source_cursor.execute(max_query)
#                             max_id = source_cursor.fetchone()[0]

#                             # Queue chunk jobs
#                             for i in range(chunks):
#                                 if isinstance(min_id, str) and isinstance(max_id, str):
#                                     chunk_min = min_id if i == 0 else f"{table.name}_chunk_{i}"
#                                     chunk_max = max_id if i == chunks - \
#                                         1 else f"{table.name}_chunk_{i+1}"
#                                 else:
#                                     chunk_min = min_id + \
#                                         (i * (max_id - min_id) // chunks)
#                                     chunk_max = min_id + \
#                                         ((i + 1) * (max_id - min_id) // chunks)
#                                     if i == chunks - 1:
#                                         chunk_max = max_id

#                                 table.with_delay(
#                                     description=f"Optimized sync: {table.name} (chunk {i+1}/{chunks})",
#                                     channel='etl_chunk',
#                                     priority=10
#                                 ).process_table_chunk_job(chunk_min, chunk_max)
#                         else:
#                             # Use optimized single-job processing for smaller tables
#                             table.with_delay(
#                                 description=f"Optimized sync for table: {table.name}",
#                                 channel='etl',
#                                 priority=10
#                             ).sync_table_job()

#                             table.write({
#                                 'job_status': 'pending',
#                                 'last_sync_message': 'Scheduled optimized sync'
#                             })

#                 except Exception as size_error:
#                     # Fall back to standard job if size determination fails
#                     _logger.warning(
#                         f"Could not determine optimal strategy for {table.name}: {str(size_error)}")

#                     table.with_delay(
#                         description=f"Scheduled sync for table: {table.name}",
#                         channel='etl',
#                         priority=10
#                     ).sync_table_job()

#                     table.write({
#                         'job_status': 'pending',
#                         'last_sync_message': 'Scheduled standard sync (fallback)'
#                     })

#                 _logger.info(
#                     f"Successfully queued sync job for table {table.name}")

#             except Exception as e:
#                 _logger.error(
#                     f"Failed to queue sync job for table {table.name}: {str(e)}")

#         # Now process dependent tables
#         for table in dependent_tables:
#             try:
#                 processed_count += 1
#                 _logger.info(
#                     f"Queueing dependent table {processed_count-len(base_tables)}/{len(dependent_tables)}: {table.name}")

#                 # Use a similar approach as base tables, but with lower priority
#                 # to ensure dependencies are processed first
#                 try:
#                     with self.get_connections() as (source_conn, pg_conn):
#                         config = table.get_config_json()
#                         source_cursor = source_conn.cursor()

#                         is_postgres_source = isinstance(
#                             source_conn, psycopg2.extensions.connection)
#                         table_delimiter = '"' if is_postgres_source else '['
#                         table_delimiter_end = '"' if is_postgres_source else ']'

#                         count_query = f"SELECT COUNT(*) FROM {table_delimiter}{config['source_table']}{table_delimiter_end}"
#                         source_cursor.execute(count_query)
#                         total_count = source_cursor.fetchone()[0]

#                         # Similar logic but with lower priority
#                         # if table.use_parallel_processing and total_count > 50000:
#                         if table and total_count > 50000:
#                             # Use fewer workers for dependent tables
#                             num_workers = min(6, max(2, total_count // 150000))

#                             table.with_delay(
#                                 description=f"Parallel sync for table: {table.name} workers)",
#                                 channel='etl',
#                                 priority=20  # Lower priority (higher number)
#                             ).parallel_process_table_job()
#                         else:
#                             # For most dependent tables, use standard method with lower priority
#                             table.with_delay(
#                                 description=f"Scheduled sync for table: {table.name}",
#                                 channel='etl',
#                                 priority=20  # Lower priority
#                             ).sync_table_job()

#                         table.write({
#                             'job_status': 'pending'
#                         })

#                 except Exception as size_error:
#                     # Fall back to standard job
#                     _logger.warning(
#                         f"Could not determine optimal strategy for {table.name}: {str(size_error)}")

#                     table.with_delay(
#                         description=f"Scheduled sync for table: {table.name}",
#                         channel='etl',
#                         priority=20  # Lower priority
#                     ).sync_table_job()

#                     table.write({
#                         'job_status': 'pending'
#                     })

#                 _logger.info(
#                     f"Successfully queued sync job for dependent table {table.name}")

#             except Exception as e:
#                 _logger.error(
#                     f"Failed to queue sync job for dependent table {table.name}: {str(e)}")

#         return f"Scheduled sync completed: {len(tables)} tables queued"
    
    
#     @api.model
#     def parallel_process_table(self, table_config):
#         """Process a table using parallel workers for massive speedup"""
#         table_id = table_config.id
#         start_time = fields.Datetime.now()

#         # Create main sync log entry
#         sync_log = self.env['etl.sync.log'].create({
#             'table_id': table_id,
#             'start_time': start_time,
#             'status': 'running'
#         })

#         try:
#             # Process dependencies first (sequentially)
#             for dep in table_config.dependency_ids:
#                 if not self.is_table_processed(dep.id):
#                     # self.process_table(dep)
#                     self.improved_process_table(dep)

#             # Get configuration
#             config = table_config.get_config_json()

#             # Determine ID ranges for parallel processing
#             with self.get_connections() as (source_conn, pg_conn):
#                 source_cursor = source_conn.cursor()

#                 # Determine source DB type for query syntax
#                 is_postgres_source = isinstance(
#                     source_conn, psycopg2.extensions.connection)
#                 table_delimiter = '"' if is_postgres_source else '['
#                 table_delimiter_end = '"' if is_postgres_source else ']'

#                 # Get min and max IDs
#                 primary_key = config['primary_key']
#                 if is_postgres_source:
#                     min_max_query = f"""
#                         SELECT MIN({table_delimiter}{primary_key}{table_delimiter_end}), 
#                             MAX({table_delimiter}{primary_key}{table_delimiter_end})
#                         FROM {table_delimiter}{config['source_table']}{table_delimiter_end}
#                     """
#                 else:
#                     min_max_query = f"""
#                         SELECT MIN([{primary_key}]), MAX([{primary_key}])
#                         FROM [{config['source_table']}]
#                     """

#                 source_cursor.execute(min_max_query)
#                 min_id, max_id = source_cursor.fetchone()

#                 if min_id is None or max_id is None:
#                     _logger.info(
#                         f"Table {config['source_table']} is empty. Nothing to process.")
#                     return

#                 # Count total records
#                 count_query = f"""
#                     SELECT COUNT(*) FROM {table_delimiter}{config['source_table']}{table_delimiter_end}
#                 """
#                 source_cursor.execute(count_query)
#                 total_count = source_cursor.fetchone()[0]

#                 _logger.info(
#                     f"Parallel processing table {config['source_table']} with {total_count} records")

#                 # Calculate chunk size and ranges
#                 records_per_worker = max(1000, total_count // 4)

#                 # For string or UUID primary keys, we need a different approach
#                 # For simplicity, this example assumes numeric IDs
#                 # For non-numeric IDs, you would need to get a sorted list of IDs and chunk by position

#                 if isinstance(min_id, (int, float)) and isinstance(max_id, (int, float)):
#                     id_range = max_id - min_id
#                     chunk_size = max(1000, id_range // 4)

#                     chunks = []
#                     current_min = min_id

#                     while current_min <= max_id:
#                         current_max = min(current_min + chunk_size, max_id)
#                         chunks.append((current_min, current_max))
#                         current_min = current_max + 1

#                     _logger.info(
#                         f"Created {len(chunks)} chunks for parallel processing")

#                     # Process chunks in parallel using Odoo's queue_job
#                     chunk_jobs = []
#                     for i, (chunk_min, chunk_max) in enumerate(chunks):
#                         # Queue each chunk as a separate job
#                         job = table_config.with_delay(
#                             description=f"Chunk {i+1}/{len(chunks)} for table: {table_config.name}",
#                             channel='etl',
#                             priority=10
#                         ).process_table_chunk_job(chunk_min, chunk_max)

#                         chunk_jobs.append(job)

#                     # Update table status
#                     table_config.write({
#                         'job_status': 'processing',
#                         'last_sync_message': f'Processing in {len(chunks)} parallel chunks'
#                     })

#                     return {
#                         'sync_log_id': sync_log.id,
#                         'chunk_count': len(chunks),
#                         'total_records': total_count
#                     }
#                 else:
#                     # For non-numeric IDs, fall back to single-threaded processing
#                     _logger.info(
#                         f"Non-numeric primary key detected. Falling back to standard processing.")
#                     # return self.process_table(table_config)
#                     return self.improved_process_table(table_config)

#         except Exception as e:
#             error_message = str(e)
#             _logger.error(
#                 f"Error setting up parallel processing for table {table_config.name}: {error_message}")

#             sync_log.write({
#                 'end_time': fields.Datetime.now(),
#                 'status': 'failed',
#                 'error_message': error_message
#             })

#             table_config.write({
#                 'last_sync_status': 'failed',
#                 'last_sync_message': error_message
#             })

#             raise

    