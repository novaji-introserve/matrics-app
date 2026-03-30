#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standalone ETL Engine - No Odoo Dependencies
Main entry point for running ETL syncs independently
"""
import os
import sys
import json
import logging
import argparse
import traceback
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal
import re

# Add scripts directory to path for imports
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

from database_adapters import create_adapter, DatabaseError

# Dateutil import with fallback
try:
    from dateutil import parser
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False

# Setup logging - log to file only (no stdout/stderr)
# Note: When run as subprocess, scheduler redirects stdout/stderr to log file
# This ensures no logging goes to terminal or Odoo's main log
LOG_DIR = SCRIPT_DIR.parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Remove any existing handlers
logger.handlers = []

# Add file handler (but don't create a new file - let scheduler handle that)
# Since scheduler redirects stdout/stderr, we'll use a NullHandler to be safe
# or log to a separate file if needed
file_handler = logging.FileHandler(LOG_DIR / 'etl_engine.log', mode='a')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Prevent propagation to root logger (which might go to Odoo's log)
logger.propagate = False

_logger = logger


class StandaloneETLProcessor:
    """Standalone ETL Processor - No Odoo dependencies"""
    
    def __init__(self, table_config_path, db_config_path):
        """Initialize processor with config files"""
        try:
            _logger.info(f"Loading table config from: {table_config_path}")
            self.table_config_path = Path(table_config_path)
            self.db_config_path = Path(db_config_path)
            
            # Load configs
            with open(self.table_config_path) as f:
                self.table_config = json.load(f)
            _logger.info(f"Table config loaded: {self.table_config.get('table_name', 'unknown')}")
            
            _logger.info(f"Loading DB config from: {db_config_path}")
            with open(self.db_config_path) as f:
                self.db_config = json.load(f)
            _logger.info("DB config loaded")
            
            # Get connection configs
            source_conn_id = self.table_config['source_connection_id']
            target_conn_id = self.table_config['target_connection_id']
            _logger.info(f"Source connection ID: {source_conn_id}, Target connection ID: {target_conn_id}")
            
            # Convert to string since JSON keys are strings
            source_conn_key = str(source_conn_id)
            target_conn_key = str(target_conn_id)
            
            if source_conn_key not in self.db_config['connections']:
                raise KeyError(f"Source connection ID {source_conn_id} not found in db_config. Available connections: {list(self.db_config['connections'].keys())}")
            if target_conn_key not in self.db_config['connections']:
                raise KeyError(f"Target connection ID {target_conn_id} not found in db_config. Available connections: {list(self.db_config['connections'].keys())}")
            
            self.source_conn_config = self.db_config['connections'][source_conn_key]
            self.target_conn_config = self.db_config['connections'][target_conn_key]
            
            # Create adapters
            _logger.info("Creating database adapters...")
            self.source_adapter = create_adapter(self.source_conn_config)
            self.target_adapter = create_adapter(self.target_conn_config)
            _logger.info("Database adapters created")
            
            # Get ETL config (same format as Odoo's get_config_json)
            _logger.info("Building ETL config...")
            self.config = self._build_etl_config()
            _logger.info("ETL config built successfully")
        except Exception as e:
            error_traceback = traceback.format_exc()
            _logger.error(f"Failed to initialize ETL processor: {str(e)}")
            _logger.error(f"Full traceback:\n{error_traceback}")
            raise
    
    def _build_etl_config(self):
        """Build ETL config from table config (same format as Odoo's get_config_json)"""
        source_is_postgres = self.source_conn_config['database_type'] == 'postgresql'
        target_is_postgres = self.target_conn_config['database_type'] == 'postgresql'
        
        # Get quoted table names
        source_table = self._quote_table_name(
            self.table_config['source_table_name'],
            self.source_conn_config['database_type']
        )
        target_table = self._quote_table_name(
            self.table_config['target_table_name'],
            self.target_conn_config['database_type']
        )
        
        # Get quoted primary key
        primary_key = self._quote_column_name(
            self.table_config['primary_key_unique'],
            self.source_conn_config['database_type']
        )
        
        # Normalize mappings
        normalized_mappings = {}
        for mapping in self.table_config['mappings']:
            source_col = mapping['source_column']
            if source_is_postgres:
                source_column_key = source_col
            else:
                source_column_key = source_col.lower()
            
            mapping_dict = {
                'target': mapping['target_column'] if target_is_postgres else mapping['target_column'].lower(),
                'type': mapping['mapping_type'],
            }
            
            if mapping['mapping_type'] == 'lookup':
                mapping_dict.update({
                    'lookup_table': mapping['lookup_table'] if target_is_postgres else mapping['lookup_table'].lower(),
                    'lookup_key': mapping['lookup_key'] if target_is_postgres else mapping['lookup_key'].lower(),
                    'lookup_value': mapping['lookup_value'] if target_is_postgres else mapping['lookup_value'].lower()
                })
            
            normalized_mappings[source_column_key] = mapping_dict
        
        return {
            'source_connection_type': self.source_conn_config['database_type'],
            'target_connection_type': self.target_conn_config['database_type'],
            'source_table': source_table,
            'target_table': target_table,
            'primary_key_unique': primary_key,
            'batch_size': self.table_config.get('batch_size', 2000),
            'mappings': normalized_mappings,
            'source_is_postgres': source_is_postgres,
            'target_is_postgres': target_is_postgres,
            # Store unquoted names for queries
            'source_table_unquoted': self.table_config['source_table_name'],
            'target_table_unquoted': self.table_config['target_table_name'],
            'primary_key_unquoted': self.table_config['primary_key_unique'],
        }
    
    def _quote_identifier(self, identifier, connection_type):
        """Quote identifier based on database type"""
        if not identifier:
            return identifier
        
        if connection_type == 'postgresql':
            return f'"{identifier}"'
        elif connection_type == 'mssql':
            return f'[{identifier}]'
        elif connection_type == 'mysql':
            return f'`{identifier}`'
        else:
            return identifier
    
    def _quote_table_name(self, table_name, connection_type):
        """Quote table name"""
        return self._quote_identifier(table_name, connection_type)
    
    def _quote_column_name(self, column_name, connection_type):
        """Quote column name"""
        return self._quote_identifier(column_name, connection_type)
    
    def _is_postgres_connection_type(self, connection_type):
        """Check if connection type is PostgreSQL"""
        return connection_type == 'postgresql'
    
    def _compare_column_names(self, col1, col2, is_postgres):
        """Compare column names considering case sensitivity"""
        if is_postgres:
            return col1 == col2
        else:
            return col1.lower() == col2.lower()
    
    def _normalize_column_for_lookup(self, column_name, is_postgres):
        """Normalize column name for dictionary lookups"""
        return column_name if is_postgres else column_name.lower()
    
    def process_full_sync(self):
        """Process full synchronization"""
        start_time = datetime.now()
        table_name = self.table_config.get('name', 'unknown')
        
        try:
            _logger.info(f"Starting full sync for table {table_name}")
            
            # Process dependencies first (if any)
            dependencies = self.table_config.get('dependencies', [])
            for dep_table_name in dependencies:
                _logger.info(f"Processing dependency: {dep_table_name}")
                # Note: Dependencies would need separate config files
                # For now, we'll skip or handle separately
            
            # Process the table
            source_conn = self.source_adapter.create_connection()
            target_conn = self.target_adapter.create_connection()
            
            try:
                # Get source table information
                source_table_unquoted = self.config['source_table_unquoted']
                source_columns = self.source_adapter.get_table_columns(source_conn, source_table_unquoted)
                
                # Get record count
                try:
                    total_records = self.source_adapter.get_record_count(source_conn, source_table_unquoted)
                except Exception as e:
                    _logger.warning(f"Adapter count failed, using direct query: {str(e)}")
                    # Fallback to direct query
                    if self.config['source_connection_type'] == 'postgresql':
                        quoted_table = self.config['source_table']
                        cursor = source_conn.cursor()
                        try:
                            cursor.execute(f'SELECT COUNT(*) FROM {quoted_table}')
                            result = cursor.fetchone()
                            total_records = result[0] if result else 0
                        finally:
                            cursor.close()
                    else:
                        raise e
                
                _logger.info(f"Full sync: {total_records:,} records to process")
                
                # Process in batches
                stats = self._process_table_data(
                    source_conn, target_conn, source_columns, total_records
                )
                
                _logger.info(f"Full sync completed: {stats}")
                return {
                    'success': True,
                    'stats': stats,
                    'start_time': start_time.isoformat(),
                    'end_time': datetime.now().isoformat()
                }
                
            finally:
                self.source_adapter.close_connection(source_conn)
                self.target_adapter.close_connection(target_conn)
                
        except Exception as e:
            error_message = str(e)
            error_traceback = traceback.format_exc()
            _logger.error(f"Full sync failed: {error_message}")
            _logger.error(f"Full traceback:\n{error_traceback}")
            return {
                'success': False,
                'error': error_message,
                'traceback': error_traceback,
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat()
            }
    
    def process_incremental_sync(self, watermark=None):
        """Process incremental synchronization"""
        start_time = datetime.now()
        table_name = self.table_config.get('name', 'unknown')
        
        if watermark is None:
            # Try to read last_incremental_sync from config file
            last_sync_str = self.table_config.get('last_incremental_sync')
            if last_sync_str:
                try:
                    # Parse the date string
                    if DATEUTIL_AVAILABLE:
                        watermark = parser.parse(last_sync_str)
                    else:
                        # Fallback to standard datetime parsing with multiple formats
                        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f']:
                            try:
                                watermark = datetime.strptime(last_sync_str, fmt)
                                break
                            except ValueError:
                                continue
                        else:
                            raise ValueError(f"Could not parse date: {last_sync_str}")
                    _logger.info(f"Using last_incremental_sync from config: {last_sync_str}")
                except (ValueError, TypeError) as e:
                    _logger.warning(f"Failed to parse last_incremental_sync '{last_sync_str}': {e}. Using default.")
                    watermark = None
            
            # If still None, use default (1 week ago)
            if watermark is None:
                watermark = datetime.now() - timedelta(days=7)
                _logger.info(f"No last_incremental_sync found, using default: {watermark.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            _logger.info(f"Starting incremental sync for table {table_name}")
            
            source_conn = self.source_adapter.create_connection()
            target_conn = self.target_adapter.create_connection()
            
            try:
                stats = self._process_incremental_data(
                    source_conn, target_conn, watermark, start_time
                )
                
                # If sync was successful and records were synced, update last_incremental_sync
                if stats.get('total_rows', 0) > 0:
                    try:
                        max_synced_date = self._get_max_synced_date(target_conn)
                        if max_synced_date:
                            self._update_last_incremental_sync(max_synced_date)
                            _logger.info(f"Updated last_incremental_sync to: {max_synced_date.strftime('%Y-%m-%d %H:%M:%S')}")
                    except Exception as e:
                        _logger.warning(f"Failed to update last_incremental_sync: {e}")
                
                _logger.info(f"Incremental sync completed: {stats}")
                return {
                    'success': True,
                    'stats': stats,
                    'start_time': start_time.isoformat(),
                    'end_time': datetime.now().isoformat()
                }
                
            finally:
                self.source_adapter.close_connection(source_conn)
                self.target_adapter.close_connection(target_conn)
                
        except Exception as e:
            error_message = str(e)
            error_traceback = traceback.format_exc()
            _logger.error(f"Incremental sync failed: {error_message}")
            _logger.error(f"Full traceback:\n{error_traceback}")
            return {
                'success': False,
                'error': error_message,
                'traceback': error_traceback,
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat()
            }
    
    def _process_table_data(self, source_conn, target_conn, source_columns, total_records):
        """Process table data with batching"""
        table_name = self.table_config.get('name', 'unknown')
        _logger.info(f"ETL Sync started: {table_name} ({total_records:,} records)")
        
        source_is_postgres = self.config.get('source_is_postgres', False)
        target_is_postgres = self.config.get('target_is_postgres', False)
        
        # Get column mappings
        query_columns = []
        column_map = {}
        
        # First, ensure primary key is included
        primary_key_original = None
        primary_key_unquoted = self.config['primary_key_unquoted']
        
        for col_info in source_columns:
            if self._compare_column_names(col_info['column_name'], primary_key_unquoted, source_is_postgres):
                primary_key_original = col_info['column_name']
                query_columns.append(primary_key_original)
                
                if self.config['source_connection_type'] == 'oracle':
                    column_map[primary_key_original] = primary_key_original.lower()
                else:
                    column_map[primary_key_original] = self._normalize_column_for_lookup(primary_key_unquoted, source_is_postgres)
                break
        
        if not primary_key_original:
            raise ValueError(f"Primary key '{primary_key_unquoted}' not found in source table")
        
        # Add other mapped columns
        for mapping in self.table_config['mappings']:
            mapping_source_col = mapping['source_column']
            
            for col_info in source_columns:
                if self._compare_column_names(col_info['column_name'], mapping_source_col, source_is_postgres):
                    if col_info['column_name'] != primary_key_original:
                        query_columns.append(col_info['column_name'])
                    
                    if self.config['source_connection_type'] == 'oracle':
                        column_map[col_info['column_name']] = mapping_source_col.lower()
                    else:
                        column_map[col_info['column_name']] = self._normalize_column_for_lookup(mapping_source_col, source_is_postgres)
                    break
        
        # Initialize statistics
        stats = {
            'total_rows': 0,
            'new_rows': 0,
            'updated_rows': 0
        }
        
        # Process in batches
        batch_size = self.config['batch_size']
        offset = 0
        
        while True:
            # Build query with proper database-specific syntax
            source_table_unquoted = self.config['source_table_unquoted']
            
            if self.config['source_connection_type'] == 'mssql':
                quoted_columns = [self._quote_identifier(col, 'mssql') for col in query_columns]
                quoted_table = self._quote_identifier(source_table_unquoted, 'mssql')
                quoted_pk = self._quote_identifier(primary_key_original, 'mssql')
                
                query = f"SELECT TOP {batch_size} {', '.join(quoted_columns)} FROM {quoted_table}"
                if offset > 0:
                    query += f" ORDER BY {quoted_pk} OFFSET {offset} ROWS"
                    
            elif self.config['source_connection_type'] == 'oracle':
                quoted_columns = ', '.join(query_columns)
                quoted_table = source_table_unquoted  # Oracle handles schema.table
                
                if offset == 0:
                    query = f"SELECT {quoted_columns} FROM {quoted_table} WHERE ROWNUM <= {batch_size}"
                else:
                    end_row = offset + batch_size
                    query = f"SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (SELECT {quoted_columns} FROM {quoted_table}) a WHERE ROWNUM <= {end_row}) WHERE rnum > {offset}"
                    
            elif self.config['source_connection_type'] == 'postgresql':
                quoted_columns = [f'"{col}"' for col in query_columns]
                quoted_table = f'"{source_table_unquoted}"'
                
                query = f"SELECT {', '.join(quoted_columns)} FROM {quoted_table} LIMIT {batch_size} OFFSET {offset}"
                
            elif self.config['source_connection_type'] == 'mysql':
                quoted_columns = [f'`{col}`' for col in query_columns]
                quoted_table = f'`{source_table_unquoted}`'
                
                query = f"SELECT {', '.join(quoted_columns)} FROM {quoted_table} LIMIT {batch_size} OFFSET {offset}"
                
            else:
                query = f"SELECT {', '.join(query_columns)} FROM {source_table_unquoted} LIMIT {batch_size} OFFSET {offset}"
            
            rows = self.source_adapter.execute_query(source_conn, query)
            
            if not rows:
                break
            
            # Process batch
            batch_stats = self._process_batch(
                rows, column_map, target_conn, query_columns
            )
            
            stats['total_rows'] += batch_stats['total_rows']
            stats['new_rows'] += batch_stats['new_rows']
            stats['updated_rows'] += batch_stats['updated_rows']
            
            offset += batch_size
            
            # Log progress
            if total_records > 0:
                progress = min((stats['total_rows'] / total_records) * 100, 100)
                _logger.info(f"Progress: {progress:.1f}% - {stats['total_rows']:,}/{total_records:,} records synced")
            
            # Break if we got fewer rows than batch size
            if len(rows) < batch_size:
                break
        
        _logger.info(f"ETL Sync complete: {table_name} - {stats['total_rows']:,} total, {stats['new_rows']:,} new, {stats['updated_rows']:,} updated")
        
        return stats
    
    def _process_batch(self, rows, column_map, target_conn, query_columns):
        """Process a batch of rows"""
        batch_stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
        rows_to_upsert = []
        
        for row_data in rows:
            batch_stats['total_rows'] += 1
            
            # Convert row to dict if needed
            if isinstance(row_data, dict):
                row_dict = row_data
            else:
                row_dict = dict(zip(query_columns, row_data))
            
            # Transform values
            transformed_row = self._transform_row(row_dict, column_map, target_conn)
            
            if transformed_row:
                rows_to_upsert.append(transformed_row)
                batch_stats['new_rows'] += 1
        
        # Batch upsert all rows
        if rows_to_upsert:
            self._batch_upsert_rows(target_conn, rows_to_upsert)
        
        return batch_stats
    
    def _transform_row(self, row_dict, column_map, target_conn):
        """Transform a single row based on mappings"""
        transformed_row = {}
        
        for source_col, mapping in self.config['mappings'].items():
            source_value = None
            
            # Check row_dict directly first (works for Oracle lowercase keys)
            if source_col in row_dict:
                source_value = row_dict[source_col]
            else:
                # Fallback: Use column_map logic
                for col_name, mapped_col in column_map.items():
                    if mapped_col == source_col:
                        source_value = row_dict.get(col_name)
                        break
            
            if source_value is not None:
                transformed_value = self._transform_value(mapping, source_value, target_conn)
                transformed_row[mapping['target']] = transformed_value
        
        return transformed_row
    
    def _transform_value(self, mapping, value, target_conn):
        """Transform a value based on mapping configuration"""
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        
        if isinstance(value, str):
            value = value.strip()
        
        if mapping['type'] == 'direct':
            converted_value = self._convert_date_if_needed(value)
            if converted_value is None:
                return None
            return self.target_adapter.transform_value(converted_value)
        elif mapping['type'] == 'lookup':
            try:
                lookup_query = f"SELECT {mapping['lookup_value']} FROM {mapping['lookup_table']} WHERE {mapping['lookup_key']} = %s"
                result = self.target_adapter.execute_query(target_conn, lookup_query, (str(value),))
                if result:
                    return result[0][mapping['lookup_value']] if isinstance(result[0], dict) else result[0][0]
                return None
            except Exception as e:
                _logger.warning(f"Lookup failed for {mapping['lookup_table']}.{mapping['lookup_key']}={value}: {str(e)}")
                return None
        else:
            raise ValueError(f"Unknown transformation type: {mapping['type']}")
    
    def _convert_date_if_needed(self, value):
        """Automatically detect and convert date formats to YYYY-MM-DD"""
        if not value:
            return value
        
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d')
        
        if not isinstance(value, str):
            return value
        
        value_str = str(value).strip()
        
        # Handle error strings
        error_strings = [
            'invalid date', 'invalid_date', 'invaliddate',
            'null', 'none', 'n/a', 'na', 'nil', 'empty',
            'error', 'unknown', 'undefined', 'missing',
            'bad date', 'baddate', 'no date', 'nodate',
            'invalid', 'err', '0000-00-00', '00/00/0000',
            '1900-01-01', '1970-01-01'
        ]
        
        if value_str.lower() in error_strings:
            return None
        
        # If already in YYYY-MM-DD format
        if re.match(r'^\d{4}-\d{2}-\d{2}$', value_str):
            try:
                datetime.strptime(value_str, '%Y-%m-%d')
                return value_str
            except ValueError:
                return None
        
        # Skip UUIDs and long numbers
        if re.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', value_str, re.IGNORECASE):
            return value
        if re.match(r'^\d{10,}$', value_str):
            return value
        
        # Try dateutil parser if available
        if DATEUTIL_AVAILABLE:
            try:
                parsed_date = parser.parse(str(value), fuzzy=False)
                if 1900 <= parsed_date.year <= 2100:
                    return parsed_date.strftime('%Y-%m-%d')
                else:
                    return None
            except (ValueError, TypeError, Exception):
                return None
        
        # Manual parsing fallback
        try:
            common_formats = [
                '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d',
                '%m-%d-%Y', '%d-%m-%Y', '%d.%m.%Y', '%Y.%m.%d',
            ]
            
            for fmt in common_formats:
                try:
                    parsed_date = datetime.strptime(value_str, fmt)
                    if 1900 <= parsed_date.year <= 2100:
                        return parsed_date.strftime('%Y-%m-%d')
                    else:
                        return None
                except ValueError:
                    continue
        except Exception:
            return None
        
        return None
    
    def _process_incremental_data(self, source_conn, target_conn, watermark, current_time):
        """Process incremental data since watermark with batching"""
        watermark_str = watermark.strftime('%Y-%m-%d %H:%M:%S')
        source_table_unquoted = self.config['source_table_unquoted']
        date_column = self.table_config.get('incremental_date_column')
        
        if not date_column:
            raise ValueError("incremental_date_column not specified in table config")
        
        # Get batch size
        batch_size = self.config.get('batch_size', 50000)
        
        # Get primary key for ordering
        primary_key_unquoted = self.config['primary_key_unquoted']
        
        # First, get total count for logging
        try:
            if self.config['source_connection_type'] == 'postgresql':
                count_query = f'SELECT COUNT(*) FROM "{source_table_unquoted}" WHERE "{date_column}" > %s'
                count_result = self.source_adapter.execute_query(source_conn, count_query, (watermark_str,))
            elif self.config['source_connection_type'] == 'mysql':
                count_query = f'SELECT COUNT(*) FROM `{source_table_unquoted}` WHERE `{date_column}` > %s'
                count_result = self.source_adapter.execute_query(source_conn, count_query, (watermark_str,))
            elif self.config['source_connection_type'] == 'mssql':
                count_query = f'SELECT COUNT(*) FROM [{source_table_unquoted}] WHERE [{date_column}] > ?'
                count_result = self.source_adapter.execute_query(source_conn, count_query, (watermark_str,))
            else:
                count_query = f'SELECT COUNT(*) FROM {source_table_unquoted} WHERE {date_column} > ?'
                count_result = self.source_adapter.execute_query(source_conn, count_query, (watermark_str,))
            
            # Handle both dict and tuple results
            if count_result and len(count_result) > 0:
                first_row = count_result[0]
                if isinstance(first_row, dict):
                    # RealDictCursor returns dict - get first value
                    total_records = list(first_row.values())[0] if first_row else 0
                else:
                    # Regular cursor returns tuple
                    total_records = first_row[0] if len(first_row) > 0 else 0
            else:
                total_records = 0
            _logger.info(f"Incremental sync: Found {total_records:,} new records since {watermark_str}")
        except Exception as e:
            _logger.warning(f"Could not get record count: {e}, proceeding without count")
            total_records = 0
        
        # Process records in batches
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
        offset = 0
        source_is_postgres = self.config.get('source_is_postgres', False)
        
        # Get source columns once
        try:
            source_columns = self.source_adapter.get_table_columns(source_conn, source_table_unquoted)
            column_names = [col['column_name'] for col in source_columns]
        except Exception:
            column_names = None
        
        while True:
            # Build batched query
            if self.config['source_connection_type'] == 'postgresql':
                quoted_table = f'"{source_table_unquoted}"'
                quoted_date_column = f'"{date_column}"'
                quoted_pk = f'"{primary_key_unquoted}"'
                query = f'SELECT * FROM {quoted_table} WHERE {quoted_date_column} > %s ORDER BY {quoted_pk} LIMIT {batch_size} OFFSET {offset}'
                rows = self.source_adapter.execute_query(source_conn, query, (watermark_str,))
            elif self.config['source_connection_type'] == 'mysql':
                quoted_table = f'`{source_table_unquoted}`'
                quoted_date_column = f'`{date_column}`'
                quoted_pk = f'`{primary_key_unquoted}`'
                query = f'SELECT * FROM {quoted_table} WHERE {quoted_date_column} > %s ORDER BY {quoted_pk} LIMIT {batch_size} OFFSET {offset}'
                rows = self.source_adapter.execute_query(source_conn, query, (watermark_str,))
            elif self.config['source_connection_type'] == 'mssql':
                quoted_table = f'[{source_table_unquoted}]'
                quoted_date_column = f'[{date_column}]'
                quoted_pk = f'[{primary_key_unquoted}]'
                query = f'SELECT TOP {batch_size} * FROM {quoted_table} WHERE {quoted_date_column} > ? ORDER BY {quoted_pk} OFFSET {offset} ROWS'
                rows = self.source_adapter.execute_query(source_conn, query, (watermark_str,))
            elif self.config['source_connection_type'] == 'oracle':
                end_row = offset + batch_size
                query = f"SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (SELECT * FROM {source_table_unquoted} WHERE {date_column} > :1 ORDER BY {primary_key_unquoted}) a WHERE ROWNUM <= {end_row}) WHERE rnum > {offset}"
                rows = self.source_adapter.execute_query(source_conn, query, [watermark_str])
            else:
                query = f'SELECT * FROM {source_table_unquoted} WHERE {date_column} > ? ORDER BY {primary_key_unquoted} LIMIT {batch_size} OFFSET {offset}'
                rows = self.source_adapter.execute_query(source_conn, query, (watermark_str,))
            
            if not rows:
                break
            
            # Process batch
            rows_to_upsert = []
            for row_data in rows:
                stats['total_rows'] += 1
                
                if isinstance(row_data, dict):
                    row_dict = row_data
                else:
                    if column_names:
                        row_dict = dict(zip(column_names, row_data))
                    else:
                        row_dict = dict(zip([f'col_{i}' for i in range(len(row_data))], row_data))
                
                # Create column map
                column_map = {}
                for mapping in self.table_config['mappings']:
                    mapping_source_col = mapping['source_column']
                    for col_name in row_dict.keys():
                        if self._compare_column_names(col_name, mapping_source_col, source_is_postgres):
                            column_map[col_name] = self._normalize_column_for_lookup(mapping_source_col, source_is_postgres)
                            break
                
                transformed_row = self._transform_row(row_dict, column_map, target_conn)
                
                if transformed_row:
                    stats['new_rows'] += 1
                    rows_to_upsert.append(transformed_row)
            
            # Upsert batch
            if rows_to_upsert:
                self._batch_upsert_rows(target_conn, rows_to_upsert)
            
            # Log progress
            if total_records > 0:
                progress = min((stats['total_rows'] / total_records) * 100, 100)
                _logger.info(f"Incremental sync progress: {progress:.1f}% - {stats['total_rows']:,}/{total_records:,} records processed")
            else:
                _logger.info(f"Incremental sync: Processed {stats['total_rows']:,} records so far...")
            
            # Break if we got fewer rows than batch size
            if len(rows) < batch_size:
                break
            
            offset += batch_size
        
        _logger.info(f"Incremental sync complete: {stats['total_rows']:,} total, {stats['new_rows']:,} new, {stats['updated_rows']:,} updated")
        return stats
    
    def _get_max_synced_date(self, target_conn):
        """Query target table for MAX date of synced records"""
        # Find the target column that maps to incremental_date_column
        incremental_date_col = self.table_config.get('incremental_date_column')
        if not incremental_date_col:
            return None
        
        # Find the target column name from mappings
        target_date_column = None
        for mapping in self.table_config.get('mappings', []):
            if mapping.get('source_column') == incremental_date_col:
                target_date_column = mapping.get('target_column')
                break
        
        if not target_date_column:
            _logger.warning(f"Could not find target column mapping for incremental_date_column: {incremental_date_col}")
            return None
        
        # Query target table for MAX date
        target_table_unquoted = self.config['target_table_unquoted']
        target_connection_type = self.config['target_connection_type']
        
        try:
            if target_connection_type == 'postgresql':
                quoted_table = f'"{target_table_unquoted}"'
                quoted_column = f'"{target_date_column}"'
                query = f'SELECT MAX({quoted_column}) FROM {quoted_table}'
            elif target_connection_type == 'mysql':
                quoted_table = f'`{target_table_unquoted}`'
                quoted_column = f'`{target_date_column}`'
                query = f'SELECT MAX({quoted_column}) FROM {quoted_table}'
            elif target_connection_type == 'mssql':
                quoted_table = f'[{target_table_unquoted}]'
                quoted_column = f'[{target_date_column}]'
                query = f'SELECT MAX({quoted_column}) FROM {quoted_table}'
            elif target_connection_type == 'oracle':
                query = f'SELECT MAX({target_date_column}) FROM {target_table_unquoted}'
            else:
                query = f'SELECT MAX({target_date_column}) FROM {target_table_unquoted}'
            
            result = self.target_adapter.execute_query(target_conn, query)
            
            if result and len(result) > 0:
                # Handle different result formats
                row = result[0]
                if isinstance(row, dict):
                    # Result is a dict, get first value
                    max_date = list(row.values())[0] if row else None
                elif isinstance(row, (list, tuple)):
                    # Result is a list/tuple, get first element
                    max_date = row[0] if len(row) > 0 else None
                else:
                    # Result is a single value
                    max_date = row
                
                if max_date:
                    # Convert to datetime if it's not already
                    if isinstance(max_date, datetime):
                        return max_date
                    elif isinstance(max_date, str):
                        # Try parsing with dateutil first (handles more formats)
                        if DATEUTIL_AVAILABLE:
                            try:
                                return parser.parse(max_date)
                            except (ValueError, TypeError):
                                pass
                        # Fallback to standard formats
                        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f']:
                            try:
                                return datetime.strptime(max_date, fmt)
                            except ValueError:
                                continue
                        _logger.warning(f"Could not parse date string: {max_date}")
                    else:
                        _logger.warning(f"Unexpected date type: {type(max_date)}, value: {max_date}")
            
            return None
            
        except Exception as e:
            _logger.error(f"Error querying max synced date: {e}")
            return None
    
    def _update_last_incremental_sync(self, max_date):
        """Update config file with last_incremental_sync"""
        try:
            # Update in-memory config
            self.table_config['last_incremental_sync'] = max_date.strftime('%Y-%m-%d %H:%M:%S')
            
            # Write back to file
            with open(self.table_config_path, 'w') as f:
                json.dump(self.table_config, f, indent=2)
            
            _logger.info(f"Updated config file with last_incremental_sync: {max_date.strftime('%Y-%m-%d %H:%M:%S')}")
            
        except Exception as e:
            _logger.error(f"Failed to update config file: {e}")
            raise
    
    def _batch_upsert_rows(self, target_conn, rows):
        """Batch upsert rows in target database"""
        if not rows:
            return
        
        # Get target table columns
        target_table_unquoted = self.config['target_table_unquoted']
        target_columns = self.target_adapter.get_table_columns(target_conn, target_table_unquoted)
        target_is_postgres = self.config.get('target_is_postgres', False)
        
        # Create normalized lookup and identify NOT NULL columns
        not_null_columns = {}
        if target_is_postgres:
            table_column_names = []
            for col in target_columns:
                col_name = col['column_name'] if isinstance(col, dict) else col[0]
                table_column_names.append(col_name)
                # Check if column is NOT NULL
                if isinstance(col, dict):
                    is_nullable = col.get('is_nullable', 'YES')
                else:
                    # Tuple format: (column_name, data_type, is_nullable, column_default)
                    is_nullable = col[2] if len(col) > 2 else 'YES'
                not_null_columns[col_name] = (is_nullable == 'NO')
        else:
            table_column_names = []
            for col in target_columns:
                col_name = (col['column_name'] if isinstance(col, dict) else col[0]).lower()
                table_column_names.append(col_name)
                # Check if column is NOT NULL
                if isinstance(col, dict):
                    is_nullable = col.get('is_nullable', 'YES')
                else:
                    # Tuple format: (column_name, data_type, is_nullable, column_default)
                    is_nullable = col[2] if len(col) > 2 else 'YES'
                not_null_columns[col_name] = (is_nullable == 'NO')
        
        # Validate columns and filter out rows with NULL required fields
        columns = list(rows[0].keys())
        validated_columns = []
        
        for col in columns:
            if target_is_postgres:
                if col in table_column_names:
                    validated_columns.append(col)
            else:
                if col.lower() in table_column_names:
                    validated_columns.append(col.lower())
        
        if not validated_columns:
            raise ValueError(f"No valid columns found for table {self.config['target_table']}")
        
        # Filter out rows that have NULL for required (NOT NULL) columns
        filtered_rows = []
        skipped_count = 0
        for row in rows:
            skip_row = False
            for col in validated_columns:
                # Check if column is NOT NULL and value is None
                if not_null_columns.get(col, False) and row.get(col) is None:
                    skip_row = True
                    skipped_count += 1
                    break
            if not skip_row:
                filtered_rows.append(row)
        
        if skipped_count > 0:
            _logger.warning(f"Skipped {skipped_count} rows with NULL values in required columns")
        
        if not filtered_rows:
            _logger.warning("All rows were skipped due to NULL required fields")
            return
        
        rows = filtered_rows
        
        # Find primary key
        pk_unquoted = self.config['primary_key_unquoted']
        pk_key_normalized = self._normalize_column_for_lookup(pk_unquoted, self.config.get('source_is_postgres', False))
        
        if pk_key_normalized not in self.config['mappings']:
            raise ValueError(f"Primary key '{pk_unquoted}' must be explicitly mapped")
        
        primary_key_target = self.config['mappings'][pk_key_normalized]['target']
        
        # Verify primary key is in validated columns
        if primary_key_target not in validated_columns:
            found = False
            for col in validated_columns:
                if self._compare_column_names(col, primary_key_target, target_is_postgres):
                    primary_key_target = col
                    found = True
                    break
            
            if not found:
                raise ValueError(f"Primary key target column '{primary_key_target}' not found in target table")
        
        # Call database-specific upsert
        if self.config['target_connection_type'] == 'postgresql':
            self._batch_upsert_postgresql(target_conn, rows, validated_columns, primary_key_target)
        elif self.config['target_connection_type'] == 'mysql':
            self._batch_upsert_mysql(target_conn, rows, validated_columns, primary_key_target)
        elif self.config['target_connection_type'] == 'mssql':
            self._batch_upsert_mssql(target_conn, rows, validated_columns, primary_key_target)
        elif self.config['target_connection_type'] == 'oracle':
            self._batch_upsert_oracle(target_conn, rows, validated_columns, primary_key_target)
        elif self.config['target_connection_type'] == 'sqlite':
            self._batch_upsert_sqlite(target_conn, rows, validated_columns, primary_key_target)
        else:
            raise ValueError(f"Unsupported target database type: {self.config['target_connection_type']}")
    
    def _batch_upsert_postgresql(self, conn, rows, columns, primary_key):
        """Batch upsert for PostgreSQL"""
        target_table = self.config['target_table']
        column_names = ', '.join([f'"{col}"' for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        
        update_sets = []
        for col in columns:
            if col != primary_key:
                update_sets.append(f'"{col}" = EXCLUDED."{col}"')
        update_clause = ', '.join(update_sets)
        
        query = f'''
            INSERT INTO {target_table} ({column_names})
            VALUES ({placeholders})
            ON CONFLICT ("{primary_key}")
            DO UPDATE SET {update_clause}
        '''
        
        batch_data = [[row.get(col) for col in columns] for row in rows]
        
        cursor = conn.cursor()
        try:
            cursor.executemany(query, batch_data)
            conn.commit()
        finally:
            cursor.close()
    
    def _batch_upsert_mysql(self, conn, rows, columns, primary_key):
        """Batch upsert for MySQL"""
        target_table = self.config['target_table']
        column_names = ', '.join([f'`{col}`' for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        
        update_sets = []
        for col in columns:
            if col != primary_key:
                update_sets.append(f'`{col}` = VALUES(`{col}`)')
        update_clause = ', '.join(update_sets)
        
        query = f'''
            INSERT INTO {target_table} ({column_names})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
        '''
        
        batch_data = [[row.get(col) for col in columns] for row in rows]
        
        cursor = conn.cursor()
        try:
            cursor.executemany(query, batch_data)
            conn.commit()
        finally:
            cursor.close()
    
    def _batch_upsert_mssql(self, conn, rows, columns, primary_key):
        """Batch upsert for MSSQL"""
        target_table = self.config['target_table']
        
        for row in rows:
            quoted_pk = f'[{primary_key}]'
            check_query = f"SELECT COUNT(*) FROM {target_table} WHERE {quoted_pk} = ?"
            cursor = conn.cursor()
            
            try:
                cursor.execute(check_query, (row[primary_key],))
                exists = cursor.fetchone()[0] > 0
                
                if exists:
                    set_clause = ', '.join([f'[{col}] = ?' for col in columns if col != primary_key])
                    update_query = f"UPDATE {target_table} SET {set_clause} WHERE {quoted_pk} = ?"
                    update_values = [row.get(col) for col in columns if col != primary_key] + [row[primary_key]]
                    cursor.execute(update_query, update_values)
                else:
                    column_names = ', '.join([f'[{col}]' for col in columns])
                    placeholders = ', '.join(['?'] * len(columns))
                    insert_query = f"INSERT INTO {target_table} ({column_names}) VALUES ({placeholders})"
                    insert_values = [row.get(col) for col in columns]
                    cursor.execute(insert_query, insert_values)
            finally:
                cursor.close()
        
        conn.commit()
    
    def _batch_upsert_oracle(self, conn, rows, columns, primary_key):
        """Batch upsert for Oracle"""
        target_table = self.config['target_table']
        
        for row in rows:
            check_query = f"SELECT COUNT(*) FROM {target_table} WHERE {primary_key} = :1"
            cursor = conn.cursor()
            
            try:
                cursor.execute(check_query, [row[primary_key]])
                exists = cursor.fetchone()[0] > 0
                
                if exists:
                    set_clause = ', '.join([f'{col} = :{i+1}' for i, col in enumerate(columns) if col != primary_key])
                    update_query = f"UPDATE {target_table} SET {set_clause} WHERE {primary_key} = :{len([c for c in columns if c != primary_key]) + 1}"
                    update_values = [row.get(col) for col in columns if col != primary_key] + [row[primary_key]]
                    cursor.execute(update_query, update_values)
                else:
                    column_names = ', '.join(columns)
                    placeholders = ', '.join([f':{i+1}' for i in range(len(columns))])
                    insert_query = f"INSERT INTO {target_table} ({column_names}) VALUES ({placeholders})"
                    insert_values = [row.get(col) for col in columns]
                    cursor.execute(insert_query, insert_values)
            finally:
                cursor.close()
        
        conn.commit()
    
    def _batch_upsert_sqlite(self, conn, rows, columns, primary_key):
        """Batch upsert for SQLite"""
        target_table = self.config['target_table'].replace('"', '').replace('[', '').replace(']', '').replace('`', '')
        column_names = ', '.join(columns)
        placeholders = ', '.join(['?'] * len(columns))
        
        query = f"INSERT OR REPLACE INTO {target_table} ({column_names}) VALUES ({placeholders})"
        batch_data = [[row.get(col) for col in columns] for row in rows]
        
        cursor = conn.cursor()
        try:
            cursor.executemany(query, batch_data)
            conn.commit()
        finally:
            cursor.close()


def main():
    """Main entry point"""
    _logger.info("=" * 80)
    _logger.info("ETL Engine starting...")
    
    parser = argparse.ArgumentParser(description='Standalone ETL Engine')
    parser.add_argument('--db-config', required=True, help='Path to database config JSON')
    parser.add_argument('--table-config', required=True, help='Path to table config JSON')
    parser.add_argument('--sync-type', required=True, choices=['full', 'incremental'], help='Sync type')
    
    args = parser.parse_args()
    _logger.info(f"Arguments: db-config={args.db_config}, table-config={args.table_config}, sync-type={args.sync_type}")
    
    try:
        _logger.info("Initializing ETL processor...")
        processor = StandaloneETLProcessor(args.table_config, args.db_config)
        _logger.info("ETL processor initialized successfully")
        
        _logger.info(f"Starting {args.sync_type} sync...")
        if args.sync_type == 'full':
            result = processor.process_full_sync()
        else:
            result = processor.process_incremental_sync()
        
        _logger.info(f"Sync completed. Success: {result.get('success', False)}")
        
        # Output result as JSON
        print(json.dumps(result, indent=2))
        
        sys.exit(0 if result['success'] else 1)
        
    except Exception as e:
        error_message = str(e)
        error_traceback = traceback.format_exc()
        _logger.error(f"ETL Engine failed: {error_message}")
        _logger.error(f"Full traceback:\n{error_traceback}")
        result = {
            'success': False,
            'error': error_message,
            'traceback': error_traceback
        }
        print(json.dumps(result, indent=2))
        sys.exit(1)


if __name__ == '__main__':
    main()


