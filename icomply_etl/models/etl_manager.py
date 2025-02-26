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
            raise UserError(f"Failed to establish database connection: {str(e)}")
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

    @backoff.on_exception(
        backoff.expo,
        (psycopg2.Error, ValueError),
        max_tries=3
    )
    def lookup_value(self, pg_cursor, table: str, key_column: str, value_column: str, key_value: str) -> Optional[Any]:
        """Look up a value in the PostgreSQL database with caching and retry"""
        if key_value is None or (isinstance(key_value, str) and not key_value.strip()):
            return None
            
        cached_value = self.get_from_lookup_cache(table, key_value, key_column, value_column)
        if cached_value is not None:
            return cached_value

        query = f"SELECT {value_column} FROM {table} WHERE {key_column} = %s"
        pg_cursor.execute(query, (key_value,))
        result = pg_cursor.fetchone()
        
        if result:
            self.set_in_lookup_cache(table, key_value, key_column, value_column, result[0])
            return result[0]

        _logger.debug(f"No matching record found in {table} for {key_column}={key_value}")
        return None

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
                _logger.error(f"Failed to decode row hashes for table {table_id}")
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
            
            table_columns = {row[0].lower(): row[1] for row in pg_cursor.fetchall()}
            
            # Debug logging
            _logger.debug(f"First row to update: {rows[0]}")
            _logger.debug(f"Available columns in target table: {table_columns}")

            # Get columns from first row
            columns = list(rows[0].keys())
            
            # Validate columns
            for col in columns:
                if col.lower() not in table_columns:
                    raise ValueError(f"Column {col} not found in table {config['target_table']}")

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
                raise ValueError(f"Primary key mapping not found for {config['primary_key']}")

            # Prepare update clause
            update_sets = []
            for col in columns:
                if col.lower() != primary_key_target:
                    update_sets.append(f'"{col.lower()}" = EXCLUDED."{col.lower()}"')
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

    def process_table(self, table_config):
        """Process a single table"""
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
                source_columns = {col[0].lower(): col[0] for col in mssql_cursor.description}

                # Prepare query using proper case from source
                query_columns = []
                column_map = {}  # Map for translating result set back
                for source_col in config['mappings'].keys():
                    original_col = source_columns.get(source_col.lower())
                    if original_col:
                        query_columns.append(original_col)
                        column_map[original_col] = source_col

                query = f"SELECT {', '.join([f'[{col}]' for col in query_columns])} FROM [{config['source_table']}]"
                _logger.info(f"Executing query: {query}")
                
                mssql_cursor.execute(query)
                
                # Process rows
                current_hashes = {}
                stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
                rows_to_update = []
                
                while True:
                    rows = mssql_cursor.fetchmany(config['batch_size'])
                    if not rows:
                        break
                        
                    for row in rows:
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
                                transformed_row[mapping['target'].lower()] = transformed_value
                        
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
                        
                        # Batch update if needed
                        if len(rows_to_update) >= config['batch_size']:
                            self.batch_update_rows(pg_cursor, config, rows_to_update)
                            rows_to_update = []
                
                # Final batch update
                if rows_to_update:
                    self.batch_update_rows(pg_cursor, config, rows_to_update)
                
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
                
                pg_conn.commit()
                
                # Mark as processed
                self.mark_table_processed(table_id)
                
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error processing table {table_config.name}: {error_message}")
            
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

    @api.model
    def run_scheduled_sync(self, frequency_code='daily'):
        """Run scheduled synchronization for tables"""
        self.clear_lookup_cache()
        self.clear_processed_tables()
        
        frequency = self.env['etl.frequency'].search([('code', '=', frequency_code)], limit=1)
        if not frequency:
            _logger.error(f"Frequency '{frequency_code}' not found")
            return
        
        tables = self.env['etl.source.table'].search([
            ('frequency_id', '=', frequency.id),
            ('active', '=', True)
        ])
        
        for table in tables:
            try:
                self.process_table(table)
                _logger.info(f"Successfully processed table {table.name}")
            except Exception as e:
                _logger.error(f"Failed to process table {table.name}: {str(e)}")
                continue
                
        self.clear_lookup_cache()