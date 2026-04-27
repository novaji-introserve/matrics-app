# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError
import logging
import json
import hashlib
import re
from datetime import datetime, timedelta
from decimal import Decimal

# Add dateutil import with fallback
try:
    from dateutil import parser
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False

_logger = logging.getLogger(__name__)

class ETLProcessor(models.AbstractModel):
    _name = 'etl.processor'
    _description = 'ETL Processing Engine - Simplified with Proper Case Handling'

    # CASE SENSITIVITY AND QUOTING HELPER METHODS (RESTORED)
    def _is_postgres_connection_type(self, connection_type):
        """Check if connection type is PostgreSQL"""
        return connection_type == 'postgresql'
    
    def _compare_column_names(self, col1, col2, is_postgres):
        """Compare column names considering case sensitivity"""
        if is_postgres:
            return col1 == col2  # Case-sensitive comparison
        else:
            return col1.lower() == col2.lower()  # Case-insensitive comparison
    
    def _normalize_column_for_lookup(self, column_name, is_postgres):
        """Normalize column name for dictionary lookups"""
        return column_name if is_postgres else column_name.lower()

    def _quote_identifier(self, identifier, connection_type):
        """Quote identifier (table or column name) based on database type"""
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

    def _build_column_list(self, columns, connection_type):
        """Build properly quoted column list for SQL queries"""
        if connection_type == 'postgresql':
            return ', '.join([f'"{col}"' for col in columns])
        elif connection_type == 'mssql':
            return ', '.join([f'[{col}]' for col in columns])
        elif connection_type == 'mysql':
            return ', '.join([f'`{col}`' for col in columns])
        else:
            return ', '.join(columns)

    def _build_select_query(self, table_name, columns, connection_type, where_clause=None, order_clause=None, limit=None):
        """Build properly quoted SELECT query"""
        # Quote table name
        quoted_table = self._quote_identifier(table_name, connection_type)
        
        # Build column list
        column_list = self._build_column_list(columns, connection_type)
        
        # Start with basic SELECT
        query = f"SELECT {column_list} FROM {quoted_table}"
        
        # Add WHERE clause
        if where_clause:
            query += f" WHERE {where_clause}"
        
        # Add ORDER BY clause
        if order_clause:
            query += f" ORDER BY {order_clause}"
        
        # Add LIMIT clause (database-specific)
        if limit:
            if connection_type == 'mssql':
                if 'ORDER BY' not in query:
                    # MSSQL requires ORDER BY for TOP
                    query += f" ORDER BY {self._quote_identifier(columns[0], connection_type)}"
                query = query.replace('SELECT', f'SELECT TOP {limit}', 1)
            elif connection_type == 'oracle':
                query = f"SELECT * FROM ({query}) WHERE ROWNUM <= {limit}"
            else:
                query += f" LIMIT {limit}"
        
        return query

    @api.model
    def process_table_full_sync(self, table_config):
        """Process full synchronization for a table - WITH POSTGRESQL QUOTING"""
        
        start_time = fields.Datetime.now()
        
        # Create initial log entry
        log_data = {
            'table_id': table_config.id,
            'sync_type': 'full',
            'start_time': start_time,
            'status': 'running'
        }
        
        try:
            _logger.info(f"Starting full sync for table {table_config.name}")
            
            # Get adapters
            adapter_factory = self.env['etl.database.adapter.factory']
            source_adapter = adapter_factory.create_adapter(table_config.source_connection_id)
            target_adapter = adapter_factory.create_adapter(table_config.target_connection_id)
            
            # Get table configuration
            config = table_config.get_config_json()
            
            # Process dependencies first
            for dep in table_config.dependency_ids:
                if dep.active and dep.full_sync_enabled:
                    _logger.info(f"Processing dependency: {dep.name}")
                    self.process_table_full_sync(dep)
            
            # Process the table
            with source_adapter.create_connection() as source_conn:
                with target_adapter.create_connection() as target_conn:
                    
                    # Get source table information - FIXED FOR POSTGRESQL
                    source_table_unquoted = table_config.source_table_name
                    source_columns = source_adapter.get_table_columns(source_conn, source_table_unquoted)
                    
                    # Get record count with proper PostgreSQL handling - FIXED
                    try:
                        # Try using adapter first
                        total_records = source_adapter.get_record_count(source_conn, source_table_unquoted)
                    except Exception as e:
                        # If adapter fails, use direct query with proper quoting
                        if table_config._is_postgres_connection(table_config.source_connection_id):
                            _logger.info(f"Using direct PostgreSQL count query: {str(e)}")
                            quoted_table = table_config._quote_table_name(source_table_unquoted, table_config.source_connection_id)
                            
                            cursor = source_conn.cursor()
                            try:
                                cursor.execute(f'SELECT COUNT(*) FROM {quoted_table}')
                                result = cursor.fetchone()
                                total_records = result[0] if result else 0
                                _logger.info(f"PostgreSQL direct count successful: {total_records} records")
                            finally:
                                cursor.close()
                        else:
                            # For non-PostgreSQL, re-raise the original error
                            raise e
                    
                    _logger.info(f"Full sync: {total_records:,} records to process for {table_config.name}")
                    
                    # Process in batches
                    stats = self._process_table_data(
                        table_config, config, source_adapter, target_adapter,
                        source_conn, target_conn, source_columns, total_records
                    )
            
            # Update success log
            end_time = fields.Datetime.now()
            log_data.update({
                'end_time': end_time,
                'status': 'success',
                'total_records': stats['total_rows'],
                'new_records': stats['new_rows'],
                'updated_records': stats['updated_rows']
            })
            
            # Update table status
            table_config.write({
                'last_full_sync_time': end_time,
                'last_sync_status': 'success',
                'last_sync_message': f'Full sync completed: {stats["total_rows"]:,} total, {stats["new_rows"]:,} new, {stats["updated_rows"]:,} updated',
                'total_records_synced': stats['total_rows'],
                'estimated_record_count': total_records
            })
                        
            _logger.info(f"Full sync completed for {table_config.name}: {stats}")
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Full sync failed for {table_config.name}: {error_message}")
            
            # Update error log
            log_data.update({
                'end_time': fields.Datetime.now(),
                'status': 'failed',
                'error_message': error_message
            })
            
            # Update table status
            table_config.write({
                'last_sync_status': 'failed',
                'last_sync_message': f'Full sync failed: {error_message}'
            })
            
            raise
        
        finally:
            # Write sync log with error handling
            try:
                self.env['etl.sync.log'].create(log_data)
            except Exception as log_error:
                _logger.error(f"Failed to create sync log for {table_config.name}: {str(log_error)}")

    @api.model
    def process_table_incremental_sync(self, table_config):
        """Process incremental synchronization for a table - WITH POSTGRESQL QUOTING"""
        
        start_time = fields.Datetime.now()
        
        # Create initial log entry
        log_data = {
            'table_id': table_config.id,
            'sync_type': 'incremental',
            'start_time': start_time,
            'status': 'running'
        }
        
        try:
            _logger.info(f"Starting incremental sync for table {table_config.name}")
            
            # Get adapters
            adapter_factory = self.env['etl.database.adapter.factory']
            source_adapter = adapter_factory.create_adapter(table_config.source_connection_id)
            target_adapter = adapter_factory.create_adapter(table_config.target_connection_id)
            
            # Get table configuration
            config = table_config.get_config_json()
            
            # Get watermark timestamp
            watermark = table_config.last_incremental_sync or (start_time - timedelta(days=365))  # Temporary: 1 year for testing
            
            with source_adapter.create_connection() as source_conn:
                with target_adapter.create_connection() as target_conn:
                    
                    # Get new records since watermark
                    stats = self._process_incremental_data(
                        table_config, config, source_adapter, target_adapter,
                        source_conn, target_conn, watermark, start_time
                    )
            
            # Update success log
            end_time = fields.Datetime.now()
            log_data.update({
                'end_time': end_time,
                'status': 'success',
                'total_records': stats['total_rows'],
                'new_records': stats['new_rows'],
                'updated_records': stats['updated_rows']
            })
            
            # Update table status
            table_config.write({
                'last_incremental_sync': start_time,
                'last_sync_message': f'Incremental sync: {stats["new_rows"]} new records added',
                'total_records_synced': table_config.total_records_synced + stats['new_rows']
            })
            
            _logger.info(f"Incremental sync completed for {table_config.name}: {stats}")
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Incremental sync failed for {table_config.name}: {error_message}")
            
            # Update error log
            log_data.update({
                'end_time': fields.Datetime.now(),
                'status': 'failed',
                'error_message': error_message
            })
            
            # Update table status
            table_config.write({
                'last_sync_message': f'Incremental sync failed: {error_message}'
            })
            
            raise
        
        finally:
            # Write sync log with error handling
            try:
                self.env['etl.sync.log'].create(log_data)
            except Exception as log_error:
                _logger.error(f"Failed to create sync log for {table_config.name}: {str(log_error)}")

    # def _process_table_data(self, table_config, config, source_adapter, target_adapter,
    #                     source_conn, target_conn, source_columns, total_records):
    #     """Process table data with batching and optimization - WITH ALL ORACLE FIXES"""
        
    #     # Get database type flags
    #     source_is_postgres = config.get('source_is_postgres', False)
    #     target_is_postgres = config.get('target_is_postgres', False)
        
    #     # Get column mappings
    #     query_columns = []
    #     column_map = {}
        
    #     # First, ensure primary key is included - CASE SENSITIVE HANDLING WITH QUOTING
    #     primary_key_original = None
    #     for col_info in source_columns:
    #         if self._compare_column_names(col_info['column_name'], table_config.primary_key_unique, source_is_postgres):
    #             primary_key_original = col_info['column_name']
    #             query_columns.append(primary_key_original)
                
    #             # FIXED: For Oracle, use the actual column name as returned by Oracle adapter (lowercase)
    #             if config['source_connection_type'] == 'oracle':
    #                 # Oracle adapter returns lowercase keys, so use lowercase key
    #                 column_map[primary_key_original] = primary_key_original.lower()
    #             else:
    #                 # Other databases - use normalized key for storage in column_map
    #                 column_map[primary_key_original] = self._normalize_column_for_lookup(table_config.primary_key_unique, source_is_postgres)
    #             break
        
    #     if not primary_key_original:
    #         raise ValueError(f"Primary key '{table_config.primary_key_unique}' not found in source table")
        
    #     # Add other mapped columns (skip primary key if already added) - CASE SENSITIVE HANDLING
    #     for mapping in table_config.mapping_ids:
    #         # Get the actual source column name from mapping (preserve case)
    #         mapping_source_col = mapping.source_column
            
    #         for col_info in source_columns:
    #             if self._compare_column_names(col_info['column_name'], mapping_source_col, source_is_postgres):
    #                 # Don't add primary key twice
    #                 if col_info['column_name'] != primary_key_original:
    #                     query_columns.append(col_info['column_name'])
                    
    #                 # FIXED: For Oracle, use the actual column name as key and value
    #                 if config['source_connection_type'] == 'oracle':
    #                     # Oracle adapter returns lowercase keys, so map correctly
    #                     column_map[col_info['column_name']] = mapping_source_col.lower()
    #                 else:
    #                     # Other databases - Always update column_map - use actual column name as key, normalized for lookup value
    #                     column_map[col_info['column_name']] = self._normalize_column_for_lookup(mapping_source_col, source_is_postgres)
    #                 break
        
    #     # Initialize statistics
    #     stats = {
    #         'total_rows': 0,
    #         'new_rows': 0,
    #         'updated_rows': 0
    #     }
        
    #     # Process in batches
    #     batch_size = config['batch_size']
    #     offset = 0
        
    #     while True:
    #         # CRITICAL FIX: Initialize quoted_table for all database types
    #         quoted_table = table_config.source_table_name  # Default fallback
            
    #         # Build query with proper database-specific syntax and quoting
    #         if config['source_connection_type'] == 'mssql':
    #             quoted_columns = [self._quote_identifier(col, 'mssql') for col in query_columns]
    #             quoted_table = self._quote_identifier(table_config.source_table_name, 'mssql')  # Override
    #             quoted_pk = self._quote_identifier(primary_key_original, 'mssql')
                
    #             query = f"SELECT TOP {batch_size} {', '.join(quoted_columns)} FROM {quoted_table}"
    #             if offset > 0:
    #                 query += f" ORDER BY {quoted_pk} OFFSET {offset} ROWS"
                    
    #         elif config['source_connection_type'] == 'oracle':
    #             # FIXED: Oracle query building - handle schema.table properly
    #             quoted_columns = ', '.join(query_columns)
    #             quoted_table = table_config.source_table_name  # Override with Oracle-specific handling
                
    #             # Simple Oracle pagination
    #             if offset == 0:
    #                 query = f"SELECT {quoted_columns} FROM {quoted_table} WHERE ROWNUM <= {batch_size}"
    #             else:
    #                 # Simple offset query for Oracle
    #                 end_row = offset + batch_size
    #                 query = f"SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (SELECT {quoted_columns} FROM {quoted_table}) a WHERE ROWNUM <= {end_row}) WHERE rnum > {offset}"
                    
    #         elif config['source_connection_type'] == 'postgresql':
    #             # PostgreSQL - use quoted identifiers
    #             quoted_columns = [f'"{col}"' for col in query_columns]
    #             quoted_table = f'"{table_config.source_table_name}"'  # Override
                
    #             query = f"SELECT {', '.join(quoted_columns)} FROM {quoted_table} LIMIT {batch_size} OFFSET {offset}"
                
    #         elif config['source_connection_type'] == 'mysql':
    #             # MySQL - use backticks
    #             quoted_columns = [f'`{col}`' for col in query_columns]
    #             quoted_table = f'`{table_config.source_table_name}`'  # Override
                
    #             query = f"SELECT {', '.join(quoted_columns)} FROM {quoted_table} LIMIT {batch_size} OFFSET {offset}"
                
    #         else:
    #             # Default (SQLite) - quoted_table already initialized above
    #             query = f"SELECT {', '.join(query_columns)} FROM {quoted_table} LIMIT {batch_size} OFFSET {offset}"
            
    #         # Now quoted_table is GUARANTEED to be defined
    #         rows = source_adapter.execute_query(source_conn, query)
            
    #         if not rows:
    #             break
            
    #         # Process batch
    #         batch_stats = self._process_batch(
    #             rows, config, column_map, target_adapter, target_conn, query_columns
    #         )
            
    #         stats['total_rows'] += batch_stats['total_rows']
    #         stats['new_rows'] += batch_stats['new_rows']
    #         stats['updated_rows'] += batch_stats['updated_rows']
            
    #         offset += batch_size
            
    #         # Log progress
    #         if total_records > 0:
    #             progress = (offset / total_records) * 100
    #             _logger.info(f"Progress: {progress:.1f}% - {offset:,} of {total_records:,} rows processed")
            
    #         # Break if we got fewer rows than batch size
    #         if len(rows) < batch_size:
    #             break
        
    #     return stats
   
   
    def _process_table_data(self, table_config, config, source_adapter, target_adapter,
                        source_conn, target_conn, source_columns, total_records):
        """Process table data with batching and optimization - WITH CLEAN ESSENTIAL LOGGING"""
        
        # Log start
        _logger.info(f"ETL Sync started: {table_config.name} ({total_records:,} records)")
        
        # Get database type flags
        source_is_postgres = config.get('source_is_postgres', False)
        target_is_postgres = config.get('target_is_postgres', False)
        
        # Get column mappings
        query_columns = []
        column_map = {}
        
        # First, ensure primary key is included - CASE SENSITIVE HANDLING WITH QUOTING
        primary_key_original = None
        for col_info in source_columns:
            if self._compare_column_names(col_info['column_name'], table_config.primary_key_unique, source_is_postgres):
                primary_key_original = col_info['column_name']
                query_columns.append(primary_key_original)
                
                # FIXED: For Oracle, use the actual column name as returned by Oracle adapter (lowercase)
                if config['source_connection_type'] == 'oracle':
                    # Oracle adapter returns lowercase keys, so use lowercase key
                    column_map[primary_key_original] = primary_key_original.lower()
                else:
                    # Other databases - use normalized key for storage in column_map
                    column_map[primary_key_original] = self._normalize_column_for_lookup(table_config.primary_key_unique, source_is_postgres)
                break
        
        if not primary_key_original:
            raise ValueError(f"Primary key '{table_config.primary_key_unique}' not found in source table")
        
        # Add other mapped columns (skip primary key if already added) - CASE SENSITIVE HANDLING
        for mapping in table_config.mapping_ids:
            # Get the actual source column name from mapping (preserve case)
            mapping_source_col = mapping.source_column
            
            for col_info in source_columns:
                if self._compare_column_names(col_info['column_name'], mapping_source_col, source_is_postgres):
                    # Don't add primary key twice
                    if col_info['column_name'] != primary_key_original:
                        query_columns.append(col_info['column_name'])
                    
                    # FIXED: For Oracle, use the actual column name as key and value
                    if config['source_connection_type'] == 'oracle':
                        # Oracle adapter returns lowercase keys, so map correctly
                        column_map[col_info['column_name']] = mapping_source_col.lower()
                    else:
                        # Other databases - Always update column_map - use actual column name as key, normalized for lookup value
                        column_map[col_info['column_name']] = self._normalize_column_for_lookup(mapping_source_col, source_is_postgres)
                    break
        
        # Initialize statistics
        stats = {
            'total_rows': 0,
            'new_rows': 0,
            'updated_rows': 0
        }
        
        # Process in batches
        batch_size = config['batch_size']
        offset = 0
        
        while True:
            # CRITICAL FIX: Initialize quoted_table for all database types
            quoted_table = table_config.source_table_name  # Default fallback
            
            # Build query with proper database-specific syntax and quoting
            if config['source_connection_type'] == 'mssql':
                quoted_columns = [self._quote_identifier(col, 'mssql') for col in query_columns]
                quoted_table = self._quote_identifier(table_config.source_table_name, 'mssql')  # Override
                quoted_pk = self._quote_identifier(primary_key_original, 'mssql')
                
                query = f"SELECT TOP {batch_size} {', '.join(quoted_columns)} FROM {quoted_table}"
                if offset > 0:
                    query += f" ORDER BY {quoted_pk} OFFSET {offset} ROWS"
                    
            elif config['source_connection_type'] == 'oracle':
                # FIXED: Oracle query building - handle schema.table properly
                quoted_columns = ', '.join(query_columns)
                quoted_table = table_config.source_table_name  # Override with Oracle-specific handling
                
                # Simple Oracle pagination
                if offset == 0:
                    query = f"SELECT {quoted_columns} FROM {quoted_table} WHERE ROWNUM <= {batch_size}"
                else:
                    # Simple offset query for Oracle
                    end_row = offset + batch_size
                    query = f"SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (SELECT {quoted_columns} FROM {quoted_table}) a WHERE ROWNUM <= {end_row}) WHERE rnum > {offset}"
                    
            elif config['source_connection_type'] == 'postgresql':
                # PostgreSQL - use quoted identifiers
                quoted_columns = [f'"{col}"' for col in query_columns]
                quoted_table = f'"{table_config.source_table_name}"'  # Override
                
                query = f"SELECT {', '.join(quoted_columns)} FROM {quoted_table} LIMIT {batch_size} OFFSET {offset}"
                
            elif config['source_connection_type'] == 'mysql':
                # MySQL - use backticks
                quoted_columns = [f'`{col}`' for col in query_columns]
                quoted_table = f'`{table_config.source_table_name}`'  # Override
                
                query = f"SELECT {', '.join(quoted_columns)} FROM {quoted_table} LIMIT {batch_size} OFFSET {offset}"
                
            else:
                # Default (SQLite) - quoted_table already initialized above
                query = f"SELECT {', '.join(query_columns)} FROM {quoted_table} LIMIT {batch_size} OFFSET {offset}"
            
            # Now quoted_table is GUARANTEED to be defined
            rows = source_adapter.execute_query(source_conn, query)
            
            if not rows:
                break
            
            # Process batch
            batch_stats = self._process_batch(
                rows, config, column_map, target_adapter, target_conn, query_columns
            )
            
            stats['total_rows'] += batch_stats['total_rows']
            stats['new_rows'] += batch_stats['new_rows']
            stats['updated_rows'] += batch_stats['updated_rows']
            
            offset += batch_size
            
            # Log progress - CLEAN VERSION
            if total_records > 0:
                progress = min((stats['total_rows'] / total_records) * 100, 100)
                _logger.info(f"Progress: {progress:.1f}% - {stats['total_rows']:,}/{total_records:,} records synced")
            
            # Break if we got fewer rows than batch size
            if len(rows) < batch_size:
                break
        
        # Log completion
        _logger.info(f"ETL Sync complete: {table_config.name} - {stats['total_rows']:,} total, {stats['new_rows']:,} new, {stats['updated_rows']:,} updated")
        
        return stats
   
    def _process_batch(self, rows, config, column_map, target_adapter, target_conn, query_columns):
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
            transformed_row = self._transform_row(row_dict, config, column_map, target_conn, target_adapter)
            
            if transformed_row:
                rows_to_upsert.append(transformed_row)
                batch_stats['new_rows'] += 1  # Simplified - count all as new
        
        # Batch upsert all rows
        if rows_to_upsert:
            self._batch_upsert_rows(target_adapter, target_conn, config, rows_to_upsert)
        
        return batch_stats

    def _process_incremental_data(self, table_config, config, source_adapter, target_adapter,
                                source_conn, target_conn, watermark, current_time):
        """Process incremental data since watermark - WITH POSTGRESQL QUOTING"""
        
        # Build query for new records - WITH PROPER QUOTING
        watermark_str = watermark.strftime('%Y-%m-%d %H:%M:%S')
        
        # Quote table and column names properly
        if config['source_connection_type'] == 'postgresql':
            quoted_table = f'"{table_config.source_table_name}"'
            quoted_date_column = f'"{table_config.incremental_date_column}"'
            query = f'SELECT * FROM {quoted_table} WHERE {quoted_date_column} > %s'
            rows = source_adapter.execute_query(source_conn, query, (watermark_str,))
        elif config['source_connection_type'] == 'mssql':
            quoted_table = f'[{table_config.source_table_name}]'
            quoted_date_column = f'[{table_config.incremental_date_column}]'
            query = f"SELECT * FROM {quoted_table} WHERE {quoted_date_column} > ?"
            rows = source_adapter.execute_query(source_conn, query, (watermark_str,))
        elif config['source_connection_type'] == 'mysql':
            quoted_table = f'`{table_config.source_table_name}`'
            quoted_date_column = f'`{table_config.incremental_date_column}`'
            query = f"SELECT * FROM {quoted_table} WHERE {quoted_date_column} > %s"
            rows = source_adapter.execute_query(source_conn, query, (watermark_str,))
        # elif config['source_connection_type'] == 'oracle':
        #     quoted_table = table_config.source_table_name.upper()
        #     quoted_date_column = table_config.incremental_date_column.upper()
        #     query = f"SELECT * FROM {quoted_table} WHERE {quoted_date_column} > :1"
        #     rows = source_adapter.execute_query(source_conn, query, [watermark_str])      
        elif config['source_connection_type'] == 'oracle':
            # FIXED: Oracle incremental query - no hardcoding, handle schema.table
            table_name = table_config.source_table_name  # Use exactly as configured
            date_column = table_config.incremental_date_column
            
            query = f"SELECT * FROM {table_name} WHERE {date_column} > :1"
            rows = source_adapter.execute_query(source_conn, query, [watermark_str])
        else:
            # SQLite
            query = f"SELECT * FROM {table_config.source_table_name} WHERE {table_config.incremental_date_column} > ?"
            rows = source_adapter.execute_query(source_conn, query, (watermark_str,))
        
        _logger.info(f"Incremental sync: Found {len(rows)} new records since {watermark_str}")
        
        # Process new records
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
        rows_to_upsert = []
        
        source_is_postgres = config.get('source_is_postgres', False)
        
        for row_data in rows:
            stats['total_rows'] += 1
            
            # Convert row to dict if needed
            if isinstance(row_data, dict):
                row_dict = row_data
            else:
                # Get column names from source table
                try:
                    source_columns = source_adapter.get_table_columns(source_conn, table_config.source_table_name)
                    column_names = [col['column_name'] for col in source_columns]
                    row_dict = dict(zip(column_names, row_data))
                except Exception:
                    # Fallback - use generic column names
                    column_names = [f'col_{i}' for i in range(len(row_data))]
                    row_dict = dict(zip(column_names, row_data))
            
            # Create column map for this row - CASE SENSITIVE HANDLING
            column_map = {}
            for mapping in table_config.mapping_ids:
                mapping_source_col = mapping.source_column
                for col_name in row_dict.keys():
                    if self._compare_column_names(col_name, mapping_source_col, source_is_postgres):
                        column_map[col_name] = self._normalize_column_for_lookup(mapping_source_col, source_is_postgres)
                        break
            
            # Transform values
            transformed_row = self._transform_row(row_dict, config, column_map, target_conn, target_adapter)
            
            if transformed_row:
                stats['new_rows'] += 1
                rows_to_upsert.append(transformed_row)
        
        # Batch upsert all rows
        if rows_to_upsert:
            self._batch_upsert_rows(target_adapter, target_conn, config, rows_to_upsert)
        
        return stats

    # def _transform_row(self, row_dict, config, column_map, target_conn, target_adapter):
    #     """Transform a single row based on mappings - CASE SENSITIVITY FIXED"""
        
    #     transformed_row = {}
        
    #     for source_col, mapping in config['mappings'].items():
    #         # Find the actual column name in the row - CASE SENSITIVE HANDLING
    #         source_value = None
    #         for col_name, mapped_col in column_map.items():
    #             if mapped_col == source_col:  # This comparison uses normalized keys
    #                 source_value = row_dict.get(col_name)
    #                 break
            
    #         if source_value is not None:
    #             transformed_value = self._transform_value(
    #                 mapping, source_value, target_conn, target_adapter
    #             )
    #             # Target column is already normalized in config
    #             transformed_row[mapping['target']] = transformed_value
        
    #     return transformed_row

    def _transform_row(self, row_dict, config, column_map, target_conn, target_adapter):
        """Transform a single row based on mappings - FIXED for Oracle case handling"""
        
        transformed_row = {}
        
        for source_col, mapping in config['mappings'].items():
            # Find the actual column name in the row - FIXED for Oracle
            source_value = None
            
            # FIXED: Check row_dict directly first (works for Oracle lowercase keys)
            if source_col in row_dict:
                source_value = row_dict[source_col]
            else:
                # Fallback: Use column_map logic for other databases
                for col_name, mapped_col in column_map.items():
                    if mapped_col == source_col:
                        source_value = row_dict.get(col_name)
                        break
            
            if source_value is not None:
                transformed_value = self._transform_value(mapping, source_value, target_conn, target_adapter)
                # Target column is already normalized in config
                transformed_row[mapping['target']] = transformed_value
        
        return transformed_row

    def _transform_value(self, mapping, value, target_conn, target_adapter):
        """Transform a value based on mapping configuration - WITH PROPER QUOTING AND DATE CONVERSION"""
        
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        
        if isinstance(value, str):
            value = value.strip()
        
        if mapping['type'] == 'direct':
            # Check if this looks like a date value and convert it
            converted_value = self._convert_date_if_needed(value)
            
            # CRITICAL FIX: If date conversion returned None, return None directly
            # Don't pass None to target_adapter.transform_value()
            if converted_value is None:
                return None
            
            return target_adapter.transform_value(converted_value)
        elif mapping['type'] == 'lookup':
            try:
                # Perform lookup - lookup columns are already normalized/quoted in config
                lookup_query = f"SELECT {mapping['lookup_value']} FROM {mapping['lookup_table']} WHERE {mapping['lookup_key']} = %s"
                
                result = target_adapter.execute_query(target_conn, lookup_query, (str(value),))
                if result:
                    return result[0][mapping['lookup_value']] if isinstance(result[0], dict) else result[0][0]
                
                return None
            except Exception as e:
                _logger.warning(f"Lookup failed for {mapping['lookup_table']}.{mapping['lookup_key']}={value}: {str(e)}")
                return None
        else:
            raise ValueError(f"Unknown transformation type: {mapping['type']}")

    def _convert_date_if_needed(self, value):
        """Automatically detect and convert date formats to YYYY-MM-DD with smart filtering"""
        
        if not value:
            return value
        
        # If it's already a datetime object, format it
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d')
        
        # If it's not a string, return as-is
        if not isinstance(value, str):
            return value
        
        # Clean up the value
        value_str = str(value).strip()
        
        # FIRST CHECK: Handle common error/null strings - return None for these
        error_strings = [
            'invalid date', 'invalid_date', 'invaliddate',
            'null', 'none', 'n/a', 'na', 'nil', 'empty',
            'error', 'unknown', 'undefined', 'missing',
            'bad date', 'baddate', 'no date', 'nodate',
            'invalid', 'err', '0000-00-00', '00/00/0000',
            '1900-01-01', '1970-01-01'  # Common default/error dates
        ]
        
        if value_str.lower() in error_strings:
            return None
        
        # SECOND CHECK: If it's already in YYYY-MM-DD format, return as-is
        if re.match(r'^\d{4}-\d{2}-\d{2}$', value_str):
            # Double check it's a valid date by trying to parse it
            try:
                datetime.strptime(value_str, '%Y-%m-%d')
                return value_str  # Return as-is, already in correct format
            except ValueError:
                # Invalid date in YYYY-MM-DD format, return None (no logging - too noisy)
                return None
        
        # SMART FILTERING: Skip values that are clearly NOT dates
        
        # Skip if it looks like a UUID (contains hyphens with hex characters)
        if re.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', value_str, re.IGNORECASE):
            return value
        
        # Skip if it's a very long number (probably phone/ID number)
        if re.match(r'^\d{10,}$', value_str):
            return value
        
        # Skip if it contains non-date-like text (but allow month names)
        # BUT first check if it's a known date error string
        if re.search(r'[a-zA-Z]{4,}', value_str):
            # Check if it's a date-related error string
            if re.search(r'\b(date|invalid|error|null|none|missing|unknown|bad)\b', value_str, re.IGNORECASE):
                return None  # It's a date-related error string
            # Check if it contains month names (legitimate date)
            elif re.search(r'\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b', value_str, re.IGNORECASE):
                pass  # Contains month name, continue processing as potential date
            else:
                return value  # Regular text value, return as-is
        
        # VALIDATE dates in MM-DD-YYYY or DD-MM-YYYY format BEFORE processing
        if re.match(r'^\d{1,2}-\d{1,2}-\d{4}$', value_str):
            parts = value_str.split('-')
            try:
                first, second, year = int(parts[0]), int(parts[1]), int(parts[2])
                
                # Check if year is reasonable
                if not (1900 <= year <= 2100):
                    return None  # Invalid year, return None silently
                
                # Check if either MM-DD-YYYY or DD-MM-YYYY interpretation is valid
                mm_dd_valid = (1 <= first <= 12) and (1 <= second <= 31)
                dd_mm_valid = (1 <= first <= 31) and (1 <= second <= 12)
                
                if not (mm_dd_valid or dd_mm_valid):
                    # Neither interpretation is valid (like 01-52-2024) - return None silently
                    return None
                
            except (ValueError, IndexError):
                return None  # Return None silently for parsing errors
        
        # VALIDATE dates in MM/DD/YYYY or DD/MM/YYYY format BEFORE processing  
        if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', value_str):
            parts = value_str.split('/')
            try:
                first, second, year = int(parts[0]), int(parts[1]), int(parts[2])
                
                # Check if year is reasonable
                if not (1900 <= year <= 2100):
                    return None  # Invalid year, return None silently
                
                # Check if either MM/DD/YYYY or DD/MM/YYYY interpretation is valid
                mm_dd_valid = (1 <= first <= 12) and (1 <= second <= 31)
                dd_mm_valid = (1 <= first <= 31) and (1 <= second <= 12)
                
                if not (mm_dd_valid or dd_mm_valid):
                    return None  # Return None silently
                
            except (ValueError, IndexError):
                return None  # Return None silently
        
        # More restrictive date pattern matching
        restrictive_date_patterns = [
            r'^\d{4}-\d{1,2}-\d{1,2}$',      # YYYY-MM-DD or YYYY-M-D (strict)
            r'^\d{1,2}/\d{1,2}/\d{4}$',      # MM/DD/YYYY or M/D/YYYY (strict)
            r'^\d{1,2}-\d{1,2}-\d{4}$',      # MM-DD-YYYY or M-D-YYYY (strict) - added back
            r'^\d{4}/\d{1,2}/\d{1,2}$',      # YYYY/MM/DD or YYYY/M/D (strict)
            r'^\d{1,2}\.\d{1,2}\.\d{4}$',    # DD.MM.YYYY or D.M.YYYY (strict)
            r'^\d{4}\.\d{1,2}\.\d{1,2}$',    # YYYY.MM.DD or YYYY.M.D (strict)
            r'^\d{1,2} \w{3,9} \d{4}$',      # DD Month YYYY (e.g., "15 January 2025")
            r'^\w{3,9} \d{1,2}, \d{4}$',     # Month DD, YYYY (e.g., "January 15, 2025")
            r'^\d{8}$',                      # YYYYMMDD (but we'll validate this more strictly)
        ]
        
        # Check if the value matches any restrictive date pattern
        looks_like_date = any(re.match(pattern, value_str) for pattern in restrictive_date_patterns)
        
        if not looks_like_date:
            return value  # Return original value if it doesn't look like a date
        
        # Extra validation for YYYYMMDD format
        if re.match(r'^\d{8}$', value_str):
            try:
                year = int(value_str[0:4])
                month = int(value_str[4:6])
                day = int(value_str[6:8])
                
                # Basic validation for reasonable date ranges
                if not (1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31):
                    return None  # Not a reasonable date, return None silently
            except (ValueError, IndexError):
                return None  # Return None silently
        
        # Try dateutil parser first if available (very flexible)
        if DATEUTIL_AVAILABLE:
            try:
                parsed_date = parser.parse(str(value), fuzzy=False)
                # Additional validation: check if the parsed date makes sense
                if 1900 <= parsed_date.year <= 2100:
                    return parsed_date.strftime('%Y-%m-%d')
                else:
                    return None  # Year out of reasonable range, return None silently
            except (ValueError, TypeError, Exception):
                # If dateutil can't parse it, it's probably not a valid date
                return None  # Return None silently for unparseable dates
        
        # Manual parsing fallback with validation
        try:
            # Try YYYYMMDD format with strict validation
            if re.match(r'^\d{8}$', value_str):
                date_str = value_str
                year = date_str[0:4]
                month = date_str[4:6]
                day = date_str[6:8]
                parsed_date = datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d')
                return parsed_date.strftime('%Y-%m-%d')
            
            # Try other common formats manually
            common_formats = [
                '%Y-%m-%d',
                '%m/%d/%Y',
                '%d/%m/%Y', 
                '%Y/%m/%d',
                '%m-%d-%Y',
                '%d-%m-%Y',
                '%d.%m.%Y',
                '%Y.%m.%d',
            ]
            
            for fmt in common_formats:
                try:
                    parsed_date = datetime.strptime(value_str, fmt)
                    # Validate year is reasonable
                    if 1900 <= parsed_date.year <= 2100:
                        return parsed_date.strftime('%Y-%m-%d')
                    else:
                        return None  # Return None silently
                except ValueError:
                    continue
                    
        except Exception:
            return None  # Return None silently
        
        # If all parsing attempts fail, return None ONLY if it looked like a date
        # For strings that matched date patterns but couldn't be parsed
        return None

    def _batch_upsert_rows(self, target_adapter, target_conn, config, rows):
        """Batch upsert rows in target database - WITH POSTGRESQL QUOTING"""
        
        if not rows:
            return
        
        try:
            # Get target table columns - use unquoted table name for adapter
            target_table_unquoted = config['target_table'].replace('"', '').replace('[', '').replace(']', '').replace('`', '')
            target_columns = target_adapter.get_table_columns(target_conn, target_table_unquoted)
            target_is_postgres = config.get('target_is_postgres', False)
            
            # Create normalized lookup for target columns
            table_column_names = []
            if target_is_postgres:
                table_column_names = [col['column_name'] for col in target_columns]
            else:
                table_column_names = [col['column_name'].lower() for col in target_columns]
            
            # Validate and prepare rows
            columns = list(rows[0].keys())
            validated_columns = []
            
            for col in columns:
                if target_is_postgres:
                    if col in table_column_names:
                        validated_columns.append(col)
                    else:
                        _logger.warning(f"Column {col} not found in target table {config['target_table']}")
                else:
                    if col.lower() in table_column_names:
                        validated_columns.append(col.lower())
                    else:
                        _logger.warning(f"Column {col} not found in target table {config['target_table']}")
            
            if not validated_columns:
                _logger.error(f"No valid columns found for table {config['target_table']}")
                return
            
            # Find primary key for upsert - CASE SENSITIVE HANDLING
            primary_key_target = None
            # Remove quotes from primary key for comparison
            pk_unquoted = config['primary_key_unique'].replace('"', '').replace('[', '').replace(']', '').replace('`', '')
            pk_key_normalized = self._normalize_column_for_lookup(pk_unquoted, config.get('source_is_postgres', False))
            
            if pk_key_normalized in config['mappings']:
                primary_key_target = config['mappings'][pk_key_normalized]['target']
                _logger.info(f"Found explicit primary key mapping: {pk_unquoted} -> {primary_key_target}")
            else:
                raise ValueError(f"Primary key '{pk_unquoted}' must be explicitly mapped in column mappings.")

            # Verify primary key is in validated columns
            if primary_key_target not in validated_columns:
                # Try to find it with different case
                found = False
                for col in validated_columns:
                    if self._compare_column_names(col, primary_key_target, target_is_postgres):
                        primary_key_target = col
                        found = True
                        break
                
                if not found:
                    raise ValueError(f"Primary key target column '{primary_key_target}' not found in target table. Available columns: {', '.join(validated_columns)}")

            _logger.info(f"Using primary key for upsert: {primary_key_target}")
            
            # Prepare batch insert/update with proper case handling
            if config['target_connection_type'] == 'postgresql':
                self._batch_upsert_postgresql(target_conn, config, rows, validated_columns, primary_key_target)
            elif config['target_connection_type'] == 'mysql':
                self._batch_upsert_mysql(target_conn, config, rows, validated_columns, primary_key_target)
            elif config['target_connection_type'] == 'mssql':
                self._batch_upsert_mssql(target_conn, config, rows, validated_columns, primary_key_target)
            elif config['target_connection_type'] == 'oracle':
                self._batch_upsert_oracle(target_conn, config, rows, validated_columns, primary_key_target)
            elif config['target_connection_type'] == 'sqlite':
                self._batch_upsert_sqlite(target_conn, config, rows, validated_columns, primary_key_target)
            else:
                raise ValueError(f"Unsupported target database type: {config['target_connection_type']}")
            
        except Exception as e:
            _logger.error(f"Batch upsert failed: {str(e)}")
            raise

    def _batch_upsert_postgresql(self, conn, config, rows, columns, primary_key):
        """Batch upsert for PostgreSQL using ON CONFLICT - WITH PROPER QUOTING"""
        
        # Use quoted table name from config (already quoted)
        target_table = config['target_table']
        
        # Build properly quoted column names and placeholders
        column_names = ', '.join([f'"{col}"' for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Build update sets for ON CONFLICT
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
        
        # Prepare batch data
        batch_data = []
        for row in rows:
            row_values = [row.get(col) for col in columns]
            batch_data.append(row_values)
        
        # Execute batch
        cursor = conn.cursor()
        try:
            cursor.executemany(query, batch_data)
            conn.commit()
        finally:
            cursor.close()

    def _batch_upsert_mysql(self, conn, config, rows, columns, primary_key):
        """Batch upsert for MySQL using ON DUPLICATE KEY UPDATE - WITH PROPER QUOTING"""
        
        # Use quoted table name from config (already quoted)
        target_table = config['target_table']
        
        # Build properly quoted column names and placeholders
        column_names = ', '.join([f'`{col}`' for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Build update sets for ON DUPLICATE KEY UPDATE
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
        
        # Prepare batch data
        batch_data = []
        for row in rows:
            row_values = [row.get(col) for col in columns]
            batch_data.append(row_values)
        
        # Execute batch
        cursor = conn.cursor()
        try:
            cursor.executemany(query, batch_data)
            conn.commit()
        finally:
            cursor.close()

    def _batch_upsert_mssql(self, conn, config, rows, columns, primary_key):
        """Batch upsert for MSSQL using individual upserts - WITH PROPER QUOTING"""
        
        # Use quoted table name from config (already quoted)
        target_table = config['target_table']
        
        for row in rows:
            # Build properly quoted column references
            quoted_pk = f'[{primary_key}]'
            
            # Check if record exists
            check_query = f"SELECT COUNT(*) FROM {target_table} WHERE {quoted_pk} = ?"
            cursor = conn.cursor()
            
            try:
                cursor.execute(check_query, (row[primary_key],))
                exists = cursor.fetchone()[0] > 0
                
                if exists:
                    # Update - build quoted SET clause
                    set_clause = ', '.join([f'[{col}] = ?' for col in columns if col != primary_key])
                    update_query = f"UPDATE {target_table} SET {set_clause} WHERE {quoted_pk} = ?"
                    update_values = [row.get(col) for col in columns if col != primary_key] + [row[primary_key]]
                    cursor.execute(update_query, update_values)
                else:
                    # Insert - build quoted column names and placeholders
                    column_names = ', '.join([f'[{col}]' for col in columns])
                    placeholders = ', '.join(['?'] * len(columns))
                    insert_query = f"INSERT INTO {target_table} ({column_names}) VALUES ({placeholders})"
                    insert_values = [row.get(col) for col in columns]
                    cursor.execute(insert_query, insert_values)
                
            finally:
                cursor.close()
        
        conn.commit()

    # def _batch_upsert_oracle(self, conn, config, rows, columns, primary_key):
    #     """Batch upsert for Oracle using individual INSERTs/UPDATEs"""
        
    #     # Use table name from config
    #     target_table = config['target_table'].replace('"', '').replace('[', '').replace(']', '').replace('`', '')
        
    #     for row in rows:
    #         # Check if record exists
    #         check_query = f"SELECT COUNT(*) FROM {target_table.upper()} WHERE {primary_key.upper()} = :1"
    #         cursor = conn.cursor()
            
    #         try:
    #             cursor.execute(check_query, [row[primary_key]])
    #             exists = cursor.fetchone()[0] > 0
                
    #             if exists:
    #                 # Update
    #                 set_clause = ', '.join([f'{col.upper()} = :{i+1}' for i, col in enumerate(columns) if col != primary_key])
    #                 update_query = f"UPDATE {target_table.upper()} SET {set_clause} WHERE {primary_key.upper()} = :{len([c for c in columns if c != primary_key]) + 1}"
    #                 update_values = [row.get(col) for col in columns if col != primary_key] + [row[primary_key]]
    #                 cursor.execute(update_query, update_values)
    #             else:
    #                 # Insert
    #                 column_names = ', '.join([col.upper() for col in columns])
    #                 placeholders = ', '.join([f':{i+1}' for i in range(len(columns))])
    #                 insert_query = f"INSERT INTO {target_table.upper()} ({column_names}) VALUES ({placeholders})"
    #                 insert_values = [row.get(col) for col in columns]
    #                 cursor.execute(insert_query, insert_values)
                
    #         finally:
    #             cursor.close()
        
    #     conn.commit()
    
    def _batch_upsert_oracle(self, conn, config, rows, columns, primary_key):
        """Batch upsert for Oracle - FIXED for schema.table"""
        
        # FIXED: Use target table name exactly as provided (handles schema.table)
        target_table = config['target_table']
        
        for row in rows:
            # Check if record exists
            check_query = f"SELECT COUNT(*) FROM {target_table} WHERE {primary_key} = :1"
            cursor = conn.cursor()
            
            try:
                cursor.execute(check_query, [row[primary_key]])
                exists = cursor.fetchone()[0] > 0
                
                if exists:
                    # Update
                    set_clause = ', '.join([f'{col} = :{i+1}' for i, col in enumerate(columns) if col != primary_key])
                    update_query = f"UPDATE {target_table} SET {set_clause} WHERE {primary_key} = :{len([c for c in columns if c != primary_key]) + 1}"
                    update_values = [row.get(col) for col in columns if col != primary_key] + [row[primary_key]]
                    cursor.execute(update_query, update_values)
                else:
                    # Insert
                    column_names = ', '.join(columns)
                    placeholders = ', '.join([f':{i+1}' for i in range(len(columns))])
                    insert_query = f"INSERT INTO {target_table} ({column_names}) VALUES ({placeholders})"
                    insert_values = [row.get(col) for col in columns]
                    cursor.execute(insert_query, insert_values)
                
            finally:
                cursor.close()
        
        conn.commit()

    def _batch_upsert_sqlite(self, conn, config, rows, columns, primary_key):
        """Batch upsert for SQLite using INSERT OR REPLACE"""
        
        target_table = config['target_table'].replace('"', '').replace('[', '').replace(']', '').replace('`', '')
        
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