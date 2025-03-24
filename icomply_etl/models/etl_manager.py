# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError, ValidationError
import pyodbc
import psycopg2
from datetime import datetime
import logging
import json
from contextlib import contextmanager
import hashlib
from decimal import Decimal
import backoff
from typing import Dict, List, Any, Set, Tuple, Optional
from threading import Lock
import time

_logger = logging.getLogger(__name__)


class ETLManager(models.AbstractModel):
    _name = 'etl.manager'
    _description = 'ETL Process Manager'

    # Class level attributes for cache and locks
    _lookup_cache = {}
    _lookup_cache_lock = Lock()
    _processed_tables = set()
    _processed_tables_lock = Lock()

    @api.model
    def get_db_connections(self):
        """Get database connections based on system parameters"""
        ICPSudo = self.env['ir.config_parameter'].sudo()

        mssql_conn_string = ICPSudo.get_param('etl.mssql_connection_string')
        pg_conn_string = ICPSudo.get_param('etl.postgres_connection_string')

        if not mssql_conn_string or not pg_conn_string:
            raise UserError(_("Database connection strings not configured!"))

        return mssql_conn_string, pg_conn_string

    @contextmanager
    def get_connections(self):
        """Context manager for database connections with retry logic"""
        mssql_conn_string, pg_conn_string = self.get_db_connections()
        mssql_conn = None
        pg_conn = None

        try:
            mssql_conn = pyodbc.connect(mssql_conn_string, timeout=100)
            pg_conn = psycopg2.connect(pg_conn_string)
            yield mssql_conn, pg_conn
        except Exception as e:
            _logger.error(f"Connection error: {str(e)}")
            raise UserError(
                f"Failed to establish database connection: {str(e)}")
        finally:
            if mssql_conn:
                mssql_conn.close()
            if pg_conn:
                pg_conn.close()

    def clear_lookup_cache(self):
        """Clear the lookup cache"""
        with self._lookup_cache_lock:
            self._lookup_cache.clear()

    def get_lookup_cache_key(self, table: str, key_value: str, lookup_key: str) -> str:
        """Generate cache key for lookups"""
        return f"{table}:{lookup_key}:{key_value}"

    def get_from_lookup_cache(self, table: str, key_value: str, lookup_key: str, lookup_value: str) -> Optional[Any]:
        """Get value from lookup cache"""
        with self._lookup_cache_lock:
            cache_key = self.get_lookup_cache_key(table, lookup_key, key_value)
            cache_dict = self._lookup_cache.get(cache_key, {})
            return cache_dict.get(lookup_value)

    def set_in_lookup_cache(self, table: str, key_value: str, lookup_key: str, lookup_value: str, value: Any):
        """Set value in lookup cache"""
        with self._lookup_cache_lock:
            cache_key = self.get_lookup_cache_key(table, lookup_key, key_value)
            if cache_key not in self._lookup_cache:
                self._lookup_cache[cache_key] = {}
            self._lookup_cache[cache_key][lookup_value] = value

    def clear_processed_tables(self):
        """Clear the set of processed tables"""
        with self._processed_tables_lock:
            self._processed_tables.clear()

    def mark_table_processed(self, table_id: int):
        """Mark a table as processed"""
        with self._processed_tables_lock:
            self._processed_tables.add(table_id)

    def is_table_processed(self, table_id: int) -> bool:
        """Check if a table has been processed"""
        with self._processed_tables_lock:
            return table_id in self._processed_tables

    def calculate_row_hash(self, row: Dict[str, Any]) -> str:
        """Calculate a hash for a row based on its values"""
        processed_row = {}
        for key, value in row.items():
            if value is None:
                processed_row[key] = 'NULL'
            elif isinstance(value, datetime):
                processed_row[key] = value.isoformat()
            elif isinstance(value, Decimal):
                processed_row[key] = str(value)
            else:
                processed_row[key] = str(value)
        row_str = json.dumps(processed_row, sort_keys=True)
        return hashlib.sha256(row_str.encode()).hexdigest()

    # @backoff.on_exception(
    #     backoff.expo,
    #     (psycopg2.Error, ValueError),
    #     max_tries=3
    # )
    # def lookup_value(self, pg_cursor, table: str, key_column: str, value_column: str, key_value: str) -> Optional[Any]:
    #     """Look up a value in the PostgreSQL database with caching and retry"""
    #     if key_value is None or (isinstance(key_value, str) and not key_value.strip()):
    #         return None

    #     cached_value = self.get_from_lookup_cache(table, key_value, key_column, value_column)
    #     if cached_value is not None:
    #         return cached_value

    #     query = f"SELECT {value_column} FROM {table} WHERE {key_column} = %s"
    #     pg_cursor.execute(query, (key_value,))
    #     result = pg_cursor.fetchone()

    #     if result:
    #         self.set_in_lookup_cache(table, key_value, key_column, value_column, result[0])
    #         return result[0]

    #     _logger.debug(f"No matching record found in {table} for {key_column}={key_value}")
    #     return None

    @backoff.on_exception(
        backoff.expo,
        (psycopg2.Error, ValueError),
        max_tries=3
    )
    def lookup_value(self, pg_cursor, table: str, key_column: str, value_column: str, key_value: str) -> Optional[Any]:
        """Look up a value in the PostgreSQL database with caching and retry"""
        if key_value is None or (isinstance(key_value, str) and not key_value.strip()):
            return None

        cached_value = self.get_from_lookup_cache(
            table, key_value, key_column, value_column)
        if cached_value is not None:
            return cached_value

        try:
            query = f"SELECT {value_column} FROM {table} WHERE {key_column} = %s"
            pg_cursor.execute(query, (key_value,))
            result = pg_cursor.fetchone()

            if result:
                self.set_in_lookup_cache(
                    table, key_value, key_column, value_column, result[0])
                return result[0]

            _logger.debug(
                f"No matching record found in {table} for {key_column}={key_value}")
            return None
        except psycopg2.Error as e:
            # Get the connection object from the cursor
            connection = pg_cursor.connection

            # Roll back the transaction to clear the error state
            connection.rollback()

            # Log the error
            _logger.warning(
                f"Lookup error for {table}.{key_column}={key_value}: {str(e)}")

            # Return None for the lookup value
            return None

    # def transform_value(self, mapping: dict, value: Any, pg_cursor) -> Any:
    #     """Transform a value based on mapping configuration"""
    #     if value is None or (isinstance(value, str) and not value.strip()):
    #         if mapping['type'] == 'lookup':
    #             return None
    #         return value

    #     if isinstance(value, str):
    #         value = value.strip()

    #     if mapping['type'] == 'direct':
    #         return value
    #     elif mapping['type'] == 'lookup':
    #         try:
    #             lookup_result = self.lookup_value(
    #                 pg_cursor,
    #                 mapping['lookup_table'],
    #                 mapping['lookup_key'],
    #                 mapping['lookup_value'],
    #                 str(value)
    #             )
    #             _logger.debug(f"Lookup result for {value}: {lookup_result}")
    #             return lookup_result
    #         except Exception as e:
    #             _logger.warning(
    #                 f"Lookup failed for table {mapping['lookup_table']}, "
    #                 f"key {mapping['lookup_key']}={value}: {str(e)}"
    #             )
    #             return None
    #     else:
    #         raise ValueError(f"Unknown transformation type: {mapping['type']}")

    def transform_value(self, mapping: dict, value: Any, pg_cursor) -> Any:
        """Transform a value based on mapping configuration"""
        if value is None or (isinstance(value, str) and not value.strip()):
            if mapping['type'] == 'lookup':
                return None
            return value

        if isinstance(value, str):
            value = value.strip()

        if mapping['type'] == 'direct':
            return value
        elif mapping['type'] == 'lookup':
            try:
                lookup_result = self.lookup_value(
                    pg_cursor,
                    mapping['lookup_table'],
                    mapping['lookup_key'],
                    mapping['lookup_value'],
                    str(value)
                )
                _logger.debug(f"Lookup result for {value}: {lookup_result}")
                return lookup_result
            except Exception as e:
                _logger.warning(
                    f"Lookup failed for table {mapping['lookup_table']}, "
                    f"key {mapping['lookup_key']}={value}: {str(e)}"
                )
                # Return None for failed lookups - this allows the process to continue
                return None
        else:
            raise ValueError(f"Unknown transformation type: {mapping['type']}")

    def get_last_sync_info(self, table_id: int) -> Tuple[datetime, Dict[str, str]]:
        """Get the last sync time and row hashes"""
        sync_log = self.env['etl.sync.log'].search([
            ('table_id', '=', table_id),
            ('status', '=', 'success')
        ], order='create_date desc', limit=1)

        if sync_log:
            try:
                row_hashes = json.loads(sync_log.row_hashes or '{}')
                return sync_log.start_time, row_hashes
            except json.JSONDecodeError:
                _logger.error(
                    f"Failed to decode row hashes for table {table_id}")
                return sync_log.start_time, {}
        return fields.Datetime.now(), {}

    def batch_update_rows(self, pg_cursor, config: dict, rows: List[Dict[str, Any]]):
        """Batch update rows into the target PostgreSQL table"""
        if not rows:
            return

        try:
            # Get target table columns
            pg_cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (config['target_table'],))

            table_columns = {row[0].lower(): row[1]
                             for row in pg_cursor.fetchall()}

            # Debug logging
            _logger.debug(f"First row to update: {rows[0]}")
            _logger.debug(
                f"Available columns in target table: {table_columns}")

            # Get columns from first row
            columns = list(rows[0].keys())

            # Validate columns
            for col in columns:
                if col.lower() not in table_columns:
                    raise ValueError(
                        f"Column {col} not found in table {config['target_table']}")

            # Prepare SQL
            column_names = ', '.join(f'"{col.lower()}"' for col in columns)
            placeholders = ', '.join(['%s'] * len(columns))

            # Get primary key mapping
            primary_key_target = None
            for source_col, mapping in config['mappings'].items():
                if source_col.lower() == config['primary_key'].lower():
                    primary_key_target = mapping['target'].lower()
                    break

            if not primary_key_target:
                raise ValueError(
                    f"Primary key mapping not found for {config['primary_key']}")

            # Prepare update clause
            update_sets = []
            for col in columns:
                if col.lower() != primary_key_target:
                    update_sets.append(
                        f'"{col.lower()}" = EXCLUDED."{col.lower()}"')
            update_clause = ', '.join(update_sets)

            # Final query
            insert_query = f"""
                INSERT INTO {config['target_table']} ({column_names})
                VALUES ({placeholders})
                ON CONFLICT ("{primary_key_target}")
                DO UPDATE SET {update_clause}
            """

            _logger.debug(f"Executing query: {insert_query}")

            # Execute in batches
            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                values = []
                for row in batch:
                    row_values = [row.get(col) for col in columns]
                    values.append(row_values)

                pg_cursor.executemany(insert_query, values)

        except Exception as e:
            _logger.error(f"Error in batch_update_rows: {str(e)}")
            _logger.error(f"Target table: {config['target_table']}")
            _logger.error(f"Columns being updated: {columns}")
            raise

    # def process_table(self, table_config):
    #     """Process a single table"""
    #     table_id = table_config.id
    #     start_time = fields.Datetime.now()

    #     # Create sync log entry
    #     sync_log = self.env['etl.sync.log'].create({
    #         'table_id': table_id,
    #         'start_time': start_time,
    #         'status': 'running'
    #     })

    #     try:
    #         # Process dependencies first
    #         for dep in table_config.dependency_ids:
    #             if not self.is_table_processed(dep.id):
    #                 self.process_table(dep)

    #         with self.get_connections() as (mssql_conn, pg_conn):
    #             mssql_cursor = mssql_conn.cursor()
    #             pg_cursor = pg_conn.cursor()

    #             config = table_config.get_config_json()
    #             _logger.info(f"Processing table with config: {config}")

    #             last_sync_time, last_hashes = self.get_last_sync_info(table_id)

    #             # Get source columns with proper case handling
    #             query = f"SELECT TOP 0 * FROM [{config['source_table']}]"
    #             mssql_cursor.execute(query)
    #             source_columns = {col[0].lower(): col[0] for col in mssql_cursor.description}

    #             # Prepare query using proper case from source
    #             query_columns = []
    #             column_map = {}  # Map for translating result set back
    #             for source_col in config['mappings'].keys():
    #                 original_col = source_columns.get(source_col.lower())
    #                 if original_col:
    #                     query_columns.append(original_col)
    #                     column_map[original_col] = source_col

    #             query = f"SELECT {', '.join([f'[{col}]' for col in query_columns])} FROM [{config['source_table']}]"
    #             _logger.info(f"Executing query: {query}")

    #             mssql_cursor.execute(query)

    #             # Process rows
    #             current_hashes = {}
    #             stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
    #             rows_to_update = []

    #             while True:
    #                 rows = mssql_cursor.fetchmany(config['batch_size'])
    #                 if not rows:
    #                     break

    #                 for row in rows:
    #                     # Create row dict using column map
    #                     row_dict = {
    #                         column_map[col]: val
    #                         for col, val in zip(query_columns, row)
    #                     }

    #                     # Transform values
    #                     transformed_row = {}
    #                     for source_col, mapping in config['mappings'].items():
    #                         source_value = row_dict.get(source_col)
    #                         if source_value is not None:
    #                             transformed_value = self.transform_value(
    #                                 mapping, source_value, pg_cursor
    #                             )
    #                             transformed_row[mapping['target'].lower()] = transformed_value

    #                     if transformed_row:  # Only process if we have values
    #                         row_hash = self.calculate_row_hash(transformed_row)
    #                         pk_value = str(row_dict[config['primary_key']])
    #                         current_hashes[pk_value] = row_hash

    #                         # Add to update batch if new or changed
    #                         if pk_value not in last_hashes:
    #                             stats['new_rows'] += 1
    #                             rows_to_update.append(transformed_row)
    #                         elif last_hashes[pk_value] != row_hash:
    #                             stats['updated_rows'] += 1
    #                             rows_to_update.append(transformed_row)

    #                         stats['total_rows'] += 1

    #                     # Batch update if needed
    #                     if len(rows_to_update) >= config['batch_size']:
    #                         self.batch_update_rows(pg_cursor, config, rows_to_update)
    #                         rows_to_update = []

    #             # Final batch update
    #             if rows_to_update:
    #                 self.batch_update_rows(pg_cursor, config, rows_to_update)

    #             # Update sync log
    #             sync_log.write({
    #                 'end_time': fields.Datetime.now(),
    #                 'status': 'success',
    #                 'total_records': stats['total_rows'],
    #                 'new_records': stats['new_rows'],
    #                 'updated_records': stats['updated_rows'],
    #                 'row_hashes': json.dumps(current_hashes)
    #             })

    #             # Update table status
    #             table_config.write({
    #                 'last_sync_time': fields.Datetime.now(),
    #                 'last_sync_status': 'success',
    #                 'total_records_synced': stats['total_rows']
    #             })

    #             pg_conn.commit()

    #             # Mark as processed
    #             self.mark_table_processed(table_id)

    #     except Exception as e:
    #         error_message = str(e)
    #         _logger.error(f"Error processing table {table_config.name}: {error_message}")

    #         sync_log.write({
    #             'end_time': fields.Datetime.now(),
    #             'status': 'failed',
    #             'error_message': error_message
    #         })

    #         table_config.write({
    #             'last_sync_status': 'failed',
    #             'last_sync_message': error_message
    #         })

    #         raise

    def process_table(self, table_config):
        """Process a single table with pagination for large datasets"""
        table_id = table_config.id
        start_time = fields.Datetime.now()

        # Create sync log entry
        sync_log = self.env['etl.sync.log'].create({
            'table_id': table_id,
            'start_time': start_time,
            'status': 'running'
        })

        try:
            # Process dependencies first
            for dep in table_config.dependency_ids:
                if not self.is_table_processed(dep.id):
                    self.process_table(dep)

            with self.get_connections() as (mssql_conn, pg_conn):
                mssql_cursor = mssql_conn.cursor()
                pg_cursor = pg_conn.cursor()

                config = table_config.get_config_json()
                _logger.info(f"Processing table with config: {config}")

                last_sync_time, last_hashes = self.get_last_sync_info(table_id)

                # Get source columns with proper case handling
                query = f"SELECT TOP 0 * FROM [{config['source_table']}]"
                mssql_cursor.execute(query)
                source_columns = {col[0].lower(): col[0]
                                  for col in mssql_cursor.description}

                # Prepare query columns
                query_columns = []
                column_map = {}  # Map for translating result set back
                for source_col in config['mappings'].keys():
                    original_col = source_columns.get(source_col.lower())
                    if original_col:
                        query_columns.append(original_col)
                        column_map[original_col] = source_col

                # Ensure the primary key is included in the columns
                primary_key_original = source_columns.get(
                    config['primary_key'].lower())
                if primary_key_original and primary_key_original not in query_columns:
                    query_columns.append(primary_key_original)
                    column_map[primary_key_original] = config['primary_key']

                # Stats to track progress
                current_hashes = {}
                stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}

                # Count total records for progress tracking
                try:
                    count_query = f"SELECT COUNT(*) FROM [{config['source_table']}]"
                    mssql_cursor.execute(count_query)
                    total_count = mssql_cursor.fetchone()[0]
                    _logger.info(
                        f"Total records in source table: {total_count}")
                except Exception as e:
                    _logger.warning(
                        f"Could not get count, using standard processing: {str(e)}")
                    total_count = None

                # Process in smaller batches
                # Use smaller batches
                batch_size = min(config['batch_size'], 5000)

                # For tables with a manageable number of rows, process all at once
                if total_count and total_count < 20000:
                    # Query all at once for smaller tables
                    query = f"SELECT {', '.join([f'[{col}]' for col in query_columns])} FROM [{config['source_table']}]"
                    _logger.info(f"Executing query for small table: {query}")
                    mssql_cursor.execute(query)

                    rows_to_update = []
                    for row in mssql_cursor.fetchall():
                        # Create row dict using column map
                        row_dict = {
                            column_map[col]: val
                            for col, val in zip(query_columns, row)
                        }

                        # Transform values
                        transformed_row = {}
                        for source_col, mapping in config['mappings'].items():
                            source_value = row_dict.get(source_col)
                            if source_value is not None:
                                transformed_value = self.transform_value(
                                    mapping, source_value, pg_cursor
                                )
                                transformed_row[mapping['target'].lower(
                                )] = transformed_value

                        if transformed_row:  # Only process if we have values
                            row_hash = self.calculate_row_hash(transformed_row)
                            pk_value = str(row_dict[config['primary_key']])
                            current_hashes[pk_value] = row_hash

                            # Add to update batch if new or changed
                            if pk_value not in last_hashes:
                                stats['new_rows'] += 1
                                rows_to_update.append(transformed_row)
                            elif last_hashes[pk_value] != row_hash:
                                stats['updated_rows'] += 1
                                rows_to_update.append(transformed_row)

                            stats['total_rows'] += 1

                        # Update in smaller batches
                        if len(rows_to_update) >= 1000:
                            self.batch_update_rows(
                                pg_cursor, config, rows_to_update)
                            pg_conn.commit()  # Commit each batch
                            rows_to_update = []

                    # Final batch update
                    if rows_to_update:
                        self.batch_update_rows(
                            pg_cursor, config, rows_to_update)
                        pg_conn.commit()

                else:
                    # For larger tables, process in batches
                    processed = 0
                    last_pk_value = None

                    while True:
                        # Build query with pagination
                        if last_pk_value is None:
                            # First batch
                            batch_query = f"""
                                SELECT TOP {batch_size} {', '.join([f'[{col}]' for col in query_columns])} 
                                FROM [{config['source_table']}]
                                ORDER BY [{primary_key_original}]
                            """
                        else:
                            # Subsequent batches - get next set of records
                            batch_query = f"""
                                SELECT TOP {batch_size} {', '.join([f'[{col}]' for col in query_columns])} 
                                FROM [{config['source_table']}]
                                WHERE [{primary_key_original}] > '{last_pk_value}'
                                ORDER BY [{primary_key_original}]
                            """

                        _logger.info(f"Executing batch query: {batch_query}")
                        mssql_cursor.execute(batch_query)

                        rows = mssql_cursor.fetchall()
                        if not rows:
                            break  # No more data

                        rows_to_update = []
                        batch_count = 0

                        for row in rows:
                            batch_count += 1

                            # Create row dict using column map
                            row_dict = {
                                column_map[col]: val
                                for col, val in zip(query_columns, row)
                            }

                            # Keep track of the last primary key value for pagination
                            last_pk_value = row_dict[config['primary_key']]

                            # Transform values
                            transformed_row = {}
                            for source_col, mapping in config['mappings'].items():
                                source_value = row_dict.get(source_col)
                                if source_value is not None:
                                    transformed_value = self.transform_value(
                                        mapping, source_value, pg_cursor
                                    )
                                    transformed_row[mapping['target'].lower(
                                    )] = transformed_value

                            if transformed_row:  # Only process if we have values
                                row_hash = self.calculate_row_hash(
                                    transformed_row)
                                pk_value = str(row_dict[config['primary_key']])
                                current_hashes[pk_value] = row_hash

                                # Add to update batch if new or changed
                                if pk_value not in last_hashes:
                                    stats['new_rows'] += 1
                                    rows_to_update.append(transformed_row)
                                elif last_hashes[pk_value] != row_hash:
                                    stats['updated_rows'] += 1
                                    rows_to_update.append(transformed_row)

                                stats['total_rows'] += 1

                        # Update in smaller batches
                        if rows_to_update:
                            self.batch_update_rows(
                                pg_cursor, config, rows_to_update)
                            pg_conn.commit()  # Commit each batch

                        # Update progress
                        processed += batch_count
                        if total_count:
                            progress = round(
                                100.0 * processed / total_count, 2)
                            _logger.info(
                                f"Progress: {progress}% - Processed {processed} of {total_count} rows")
                        else:
                            _logger.info(f"Processed {processed} rows so far")

                        # If we got fewer rows than the batch size, we're done
                        if batch_count < batch_size:
                            break

                        # Update sync log periodically
                        if stats['total_rows'] % 50000 == 0:
                            sync_log.write({
                                'status': 'running',
                                'total_records': stats['total_rows'],
                                'new_records': stats['new_rows'],
                                'updated_records': stats['updated_rows']
                            })

                # Update sync log
                sync_log.write({
                    'end_time': fields.Datetime.now(),
                    'status': 'success',
                    'total_records': stats['total_rows'],
                    'new_records': stats['new_rows'],
                    'updated_records': stats['updated_rows'],
                    'row_hashes': json.dumps(current_hashes)
                })

                # Update table status
                table_config.write({
                    'last_sync_time': fields.Datetime.now(),
                    'last_sync_status': 'success',
                    'total_records_synced': stats['total_rows']
                })

                # Mark as processed
                self.mark_table_processed(table_id)

        except Exception as e:
            error_message = str(e)
            _logger.error(
                f"Error processing table {table_config.name}: {error_message}")

            sync_log.write({
                'end_time': fields.Datetime.now(),
                'status': 'failed',
                'error_message': error_message
            })

            table_config.write({
                'last_sync_status': 'failed',
                'last_sync_message': error_message
            })

            raise

            # Add this method to your ETLManager class
    def process_table_chunk(self, table_config, min_id, max_id):
        """Process a specific chunk of a table with ID range"""
        table_id = table_config.id
        start_time = fields.Datetime.now()

        # Create sync log entry for this chunk
        sync_log = self.env['etl.sync.log'].create({
            'table_id': table_id,
            'start_time': start_time,
            'status': 'running'
        })

        try:
            # Process dependencies if they haven't been processed yet
            for dep in table_config.dependency_ids:
                if not self.is_table_processed(dep.id):
                    self.process_table(dep)

            with self.get_connections() as (mssql_conn, pg_conn):
                mssql_cursor = mssql_conn.cursor()
                pg_cursor = pg_conn.cursor()

                config = table_config.get_config_json()
                _logger.info(f"Processing table chunk with config: {config}")
                _logger.info(f"ID range: {min_id} to {max_id}")

                last_sync_time, last_hashes = self.get_last_sync_info(table_id)

                # Get source columns with proper case handling
                query = f"SELECT TOP 0 * FROM [{config['source_table']}]"
                mssql_cursor.execute(query)
                source_columns = {col[0].lower(): col[0]
                                  for col in mssql_cursor.description}

                # Prepare query columns
                query_columns = []
                column_map = {}  # Map for translating result set back
                for source_col in config['mappings'].keys():
                    original_col = source_columns.get(source_col.lower())
                    if original_col:
                        query_columns.append(original_col)
                        column_map[original_col] = source_col

                # Ensure the primary key is included in the columns
                primary_key_original = source_columns.get(
                    config['primary_key'].lower())
                if primary_key_original and primary_key_original not in query_columns:
                    query_columns.append(primary_key_original)
                    column_map[primary_key_original] = config['primary_key']

                # Stats to track progress
                current_hashes = {}
                stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}

                # Count total records in this chunk for progress tracking
                try:
                    count_query = f"""
                        SELECT COUNT(*) FROM [{config['source_table']}]
                        WHERE [{primary_key_original}] >= '{min_id}'
                        AND [{primary_key_original}] <= '{max_id}'
                    """
                    mssql_cursor.execute(count_query)
                    chunk_total_count = mssql_cursor.fetchone()[0]
                    _logger.info(
                        f"Total records in chunk: {chunk_total_count}")
                except Exception as e:
                    _logger.warning(
                        f"Could not get chunk count, using standard processing: {str(e)}")
                    chunk_total_count = None

                # Process in smaller batches
                # Use smaller batches
                batch_size = min(config['batch_size'], 5000)
                processed = 0
                last_pk_value = min_id  # Start from min_id

                while True:
                    # Build query with pagination within the chunk
                    batch_query = f"""
                        SELECT TOP {batch_size} {', '.join([f'[{col}]' for col in query_columns])} 
                        FROM [{config['source_table']}]
                        WHERE [{primary_key_original}] >= '{last_pk_value}'
                        AND [{primary_key_original}] <= '{max_id}'
                        ORDER BY [{primary_key_original}]
                    """

                    _logger.info(f"Executing chunk batch query: {batch_query}")
                    mssql_cursor.execute(batch_query)

                    rows = mssql_cursor.fetchall()
                    if not rows:
                        break  # No more data in this chunk

                    rows_to_update = []
                    batch_count = 0

                    for row in rows:
                        batch_count += 1

                        # Create row dict using column map
                        row_dict = {
                            column_map[col]: val
                            for col, val in zip(query_columns, row)
                        }

                        # Keep track of the last primary key value for pagination
                        last_pk_value = row_dict[config['primary_key']]

                        # Transform values
                        transformed_row = {}
                        for source_col, mapping in config['mappings'].items():
                            source_value = row_dict.get(source_col)
                            if source_value is not None:
                                transformed_value = self.transform_value(
                                    mapping, source_value, pg_cursor
                                )
                                transformed_row[mapping['target'].lower(
                                )] = transformed_value

                        if transformed_row:  # Only process if we have values
                            row_hash = self.calculate_row_hash(transformed_row)
                            pk_value = str(row_dict[config['primary_key']])
                            current_hashes[pk_value] = row_hash

                            # Add to update batch if new or changed
                            if pk_value not in last_hashes:
                                stats['new_rows'] += 1
                                rows_to_update.append(transformed_row)
                            elif last_hashes[pk_value] != row_hash:
                                stats['updated_rows'] += 1
                                rows_to_update.append(transformed_row)

                            stats['total_rows'] += 1

                    # Update in smaller batches
                    if rows_to_update:
                        self.batch_update_rows(
                            pg_cursor, config, rows_to_update)
                        pg_conn.commit()  # Commit each batch

                    # Update progress
                    processed += batch_count
                    if chunk_total_count:
                        progress = round(100.0 * processed /
                                         chunk_total_count, 2)
                        _logger.info(
                            f"Chunk progress: {progress}% - Processed {processed} of {chunk_total_count} rows")
                    else:
                        _logger.info(
                            f"Processed {processed} rows in chunk so far")

                    # If we got fewer rows than the batch size or reached the max_id, we're done
                    if batch_count < batch_size or last_pk_value >= max_id:
                        break

                    # Update sync log periodically
                    if stats['total_rows'] % 20000 == 0:
                        sync_log.write({
                            'status': 'running',
                            'total_records': stats['total_rows'],
                            'new_records': stats['new_rows'],
                            'updated_records': stats['updated_rows']
                        })

                # Update sync log for this chunk
                sync_log.write({
                    'end_time': fields.Datetime.now(),
                    'status': 'success',
                    'total_records': stats['total_rows'],
                    'new_records': stats['new_rows'],
                    'updated_records': stats['updated_rows'],
                    'row_hashes': json.dumps(current_hashes)
                })

                # Update table status with chunk info
                table_config.write({
                    'last_sync_time': fields.Datetime.now(),
                    'last_sync_status': 'success',
                    'last_sync_message': f'Successfully processed chunk from {min_id} to {max_id}',
                    'total_records_synced': table_config.total_records_synced + stats['total_rows']
                })

                return stats

        except Exception as e:
            error_message = str(e)
            _logger.error(
                f"Error processing table chunk {table_config.name}: {error_message}")

            sync_log.write({
                'end_time': fields.Datetime.now(),
                'status': 'failed',
                'error_message': error_message
            })

            table_config.write({
                'last_sync_status': 'failed',
                'last_sync_message': f'Failed to process chunk from {min_id} to {max_id}: {error_message}'
            })

            raise

    # @api.model
    # def run_scheduled_sync(self, frequency='daily'):
    #     """Run scheduled synchronization for tables"""
    #     self.clear_lookup_cache()
    #     self.clear_processed_tables()

    #     tables = self.env['etl.source.table'].search([
    #         ('frequency', '=', frequency),
    #         ('active', '=', True)
    #     ])

    #     for table in tables:
    #         try:
    #             self.process_table(table)
    #             _logger.info(f"Successfully processed table {table.name}")
    #         except Exception as e:
    #             _logger.error(f"Failed to process table {table.name}: {str(e)}")
    #             continue

    #     self.clear_lookup_cache()

    # @api.model
    # def run_scheduled_sync(self, frequency_code='daily'):
    #     """Run scheduled synchronization for tables"""
    #     self.clear_lookup_cache()
    #     self.clear_processed_tables()

    #     frequency = self.env['etl.frequency'].search([('code', '=', frequency_code)], limit=1)
    #     if not frequency:
    #         _logger.error(f"Frequency '{frequency_code}' not found")
    #         return

    #     tables = self.env['etl.source.table'].search([
    #         ('frequency_id', '=', frequency.id),
    #         ('active', '=', True)
    #     ])

    #     for table in tables:
    #         try:
    #             self.process_table(table)
    #             _logger.info(f"Successfully processed table {table.name}")
    #         except Exception as e:
    #             _logger.error(f"Failed to process table {table.name}: {str(e)}")
    #             continue

    #     self.clear_lookup_cache()

    @api.model
    def run_scheduled_sync(self, frequency_code='daily'):
        """Run scheduled synchronization for tables"""
        self.clear_lookup_cache()
        self.clear_processed_tables()

        frequency = self.env['etl.frequency'].search(
            [('code', '=', frequency_code)], limit=1)
        if not frequency:
            _logger.error(f"Frequency '{frequency_code}' not found")
            return

        tables = self.env['etl.source.table'].search([
            ('frequency_id', '=', frequency.id),
            ('active', '=', True)
        ])

        for table in tables:
            try:
                # Queue the sync as a job
                table.with_delay(
                    description=f"Scheduled sync for table: {table.name}"
                ).sync_table_job()

                table.write({
                    'job_status': 'pending'
                })

                _logger.info(f"Sync job queued for table {table.name}")
            except Exception as e:
                _logger.error(
                    f"Failed to queue sync job for table {table.name}: {str(e)}")
