# -*- coding: utf-8 -*-
from odoo import models, fields, api, _, tools
from odoo.exceptions import UserError, ValidationError
from datetime import datetime
import logging
import json
from contextlib import contextmanager
import hashlib
from decimal import Decimal
import backoff
from typing import Dict, List, Any, Set, Tuple, Optional
import time
import psycopg2
import functools
import gc

_logger = logging.getLogger(__name__)

def log_execution_time(func):
    """Decorator to log execution time of functions"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        _logger.info(f"Function {func.__name__} executed in {end_time - start_time:.2f} seconds")
        return result
    return wrapper

class ETLManager(models.AbstractModel):
    _name = 'etl.manager'
    _description = 'ETL Process Manager'
    
    @contextmanager
    def get_connections(self, table_config):
        """Context manager for database connections with fixed implementation"""
        source_db = table_config.source_db_connection_id
        target_db = table_config.target_db_connection_id
        
        try:
            # Get direct connector service
            connector_service = self.env['etl.database.connector.service']
            
            # Yield the service - this is key to fixing the recursion issue
            # We pass the connection configs but not the connections themselves
            yield connector_service, source_db, target_db
            
        except Exception as e:
            _logger.error(f"Error in get_connections: {str(e)}")
            raise UserError(f"Failed to establish database connection: {str(e)}")

    @api.model
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
            # Process dependencies first if they haven't been processed yet
            self._process_dependencies(table_config)

            # Get connectors
            connector_service, source_db, target_db = self._get_connectors(table_config)
            
            # Get config
            config = table_config.get_config_json()
            _logger.info(f"Processing table: {config['source_table']}")
            
            # Get last sync info
            last_sync_time, last_hashes = self._get_last_sync_info(table_id)
            
            # Get source columns
            source_columns = connector_service.get_columns(source_db, config['source_table'])
            _logger.info(f"Source columns: {source_columns}")

            # Prepare query columns
            query_columns, column_map, primary_key_original = self._prepare_columns(config, source_columns)
            
            # Stats to track progress
            current_hashes = {}
            stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
            
            # Get total count for progress tracking
            try:
                total_count = connector_service.get_table_count(source_db, config['source_table'])
                _logger.info(f"Total records in source table: {total_count}")
            except Exception as e:
                _logger.warning(f"Could not get count, using standard processing: {str(e)}")
                total_count = None
            
            # Process data in batches
            self._process_table_data(
                connector_service, 
                source_db, 
                target_db, 
                config, 
                query_columns, 
                column_map, 
                primary_key_original, 
                last_hashes, 
                current_hashes, 
                stats, 
                sync_log
            )
            
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
                'last_sync_message': f"Successfully synced {stats['total_rows']} records ({stats['new_rows']} new, {stats['updated_rows']} updated)",
                'total_records_synced': stats['total_rows']
            })
            
            return stats
                
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

    def _get_connectors(self, table_config):
        """Get database connectors without recursion issues"""
        source_db = table_config.source_db_connection_id
        target_db = table_config.target_db_connection_id
        
        # Get direct connector service
        connector_service = self.env['etl.database.connector.service']
        
        return connector_service, source_db, target_db

    def _process_dependencies(self, table_config):
        """Process table dependencies if not already processed"""
        for dep in table_config.dependency_ids:
            # Check if dependency has been processed today
            if not self._is_dependency_processed(dep.id):
                _logger.info(f"Processing dependency: {dep.name}")
                self.process_table(dep)
    
    def _is_dependency_processed(self, table_id):
        """Check if a dependency has been processed recently"""
        today = fields.Date.today()
        return self.env['etl.sync.log'].search_count([
            ('table_id', '=', table_id),
            ('start_time', '>=', fields.Datetime.combine(today, datetime.min.time())),
            ('status', '=', 'success')
        ]) > 0

    def _get_last_sync_info(self, table_id):
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

    def _prepare_columns(self, config, source_columns):
        """Prepare column mappings for query"""
        query_columns = []
        column_map = {}
        
        # Map source columns to query columns
        for source_col in config['mappings'].keys():
            original_col = source_columns.get(source_col.lower())
            if original_col:
                query_columns.append(original_col)
                column_map[original_col] = source_col
        
        # Ensure primary key is included
        primary_key_original = source_columns.get(config['primary_key'].lower())
        if primary_key_original and primary_key_original not in query_columns:
            query_columns.append(primary_key_original)
            column_map[primary_key_original] = config['primary_key']
            
        return query_columns, column_map, primary_key_original

    def _process_table_data(self, connector_service, source_db, target_db, config, 
                           query_columns, column_map, primary_key_original,
                           last_hashes, current_hashes, stats, sync_log):
        """Process table data in batches"""
        batch_size = min(config['batch_size'], 5000)  # Set reasonable batch size
        
        # Format query columns
        formatted_columns = ', '.join(query_columns)
        
        # Get total count
        try:
            total_count = connector_service.get_table_count(source_db, config['source_table'])
        except:
            total_count = None
        
        # For smaller tables, process all at once
        if total_count is not None and total_count < 20000:
            self._process_small_table(
                connector_service, source_db, target_db, config,
                formatted_columns, column_map, primary_key_original,
                last_hashes, current_hashes, stats, total_count
            )
        else:
            # For larger tables, use batched processing
            self._process_large_table(
                connector_service, source_db, target_db, config,
                formatted_columns, column_map, primary_key_original,
                last_hashes, current_hashes, stats, batch_size, sync_log
            )

    def _process_small_table(self, connector_service, source_db, target_db, config,
                            formatted_columns, column_map, primary_key_original,
                            last_hashes, current_hashes, stats, total_count):
        """Process a small table all at once"""
        # Query all data at once
        query = f"SELECT {formatted_columns} FROM {config['source_table']}"
        
        _logger.info(f"Executing query for small table: {query}")
        result_rows = connector_service.execute_query(source_db, query)
        
        if not result_rows:
            _logger.info(f"No rows found in source table {config['source_table']}")
            return
        
        # Process rows
        rows_to_update = []
        
        for row in result_rows:
            # Process row and collect changes
            transformed_row = self._transform_row(
                row, column_map, config, primary_key_original, 
                connector_service, target_db, last_hashes, 
                current_hashes, stats
            )
            
            if transformed_row:
                rows_to_update.append(transformed_row)
            
            # Batch updates for memory efficiency
            if len(rows_to_update) >= 1000:
                self._batch_update_rows(connector_service, target_db, config, rows_to_update)
                rows_to_update = []
        
        # Final batch update
        if rows_to_update:
            self._batch_update_rows(connector_service, target_db, config, rows_to_update)

    def _process_large_table(self, connector_service, source_db, target_db, config,
                            formatted_columns, column_map, primary_key_original,
                            last_hashes, current_hashes, stats, batch_size, sync_log):
        """Process a large table in batches with pagination"""
        processed = 0
        last_pk_value = None
        
        while True:
            # Build query with pagination
            if last_pk_value is None:
                query = f"SELECT {formatted_columns} FROM {config['source_table']} " \
                        f"ORDER BY {primary_key_original} LIMIT {batch_size}"
            else:
                query = f"SELECT {formatted_columns} FROM {config['source_table']} " \
                        f"WHERE {primary_key_original} > '{last_pk_value}' " \
                        f"ORDER BY {primary_key_original} LIMIT {batch_size}"
            
            _logger.info(f"Executing batch query: {query}")
            result_rows = connector_service.execute_query(source_db, query)
            
            if not result_rows:
                _logger.info("No more rows to process")
                break
            
            # Process rows
            rows_to_update = []
            batch_count = len(result_rows)
            
            for row in result_rows:
                # Track last PK for pagination
                pk_value = row.get(primary_key_original)
                if pk_value:
                    last_pk_value = pk_value
                
                # Transform row
                transformed_row = self._transform_row(
                    row, column_map, config, primary_key_original, 
                    connector_service, target_db, last_hashes, 
                    current_hashes, stats
                )
                
                if transformed_row:
                    rows_to_update.append(transformed_row)
            
            # Update rows
            if rows_to_update:
                self._batch_update_rows(connector_service, target_db, config, rows_to_update)
            
            # Update progress
            processed += batch_count
            progress = round(100.0 * processed / (stats['total_rows'] or 1), 2)
            _logger.info(f"Progress: {progress}% - Processed {processed} rows")
            
            # Update sync log periodically
            if processed % 20000 == 0:
                sync_log.write({
                    'total_records': stats['total_rows'],
                    'new_records': stats['new_rows'],
                    'updated_records': stats['updated_rows']
                })
                # Commit to release locks
                self.env.cr.commit()
            
            # Release memory
            result_rows = None
            rows_to_update = None
            gc.collect()
            
            # Stop if batch is smaller than requested
            if batch_count < batch_size:
                break

    def _transform_row(self, row, column_map, config, primary_key_original, 
                      connector_service, target_db, last_hashes, current_hashes, stats):
        """Transform a row from source to target format"""
        # Get primary key
        pk_value = str(row.get(primary_key_original))
        if not pk_value:
            return None
        
        # Transform values
        transformed_row = {}
        for original_col, source_col in column_map.items():
            source_value = row.get(original_col)
            
            if source_value is not None:
                # Get mapping for this column
                mapping = config['mappings'].get(source_col.lower())
                if mapping:
                    # Transform value based on mapping type
                    if mapping['type'] == 'direct':
                        transformed_row[mapping['target'].lower()] = source_value
                    elif mapping['type'] == 'lookup':
                        lookup_value = self._lookup_value(
                            connector_service, 
                            target_db,
                            mapping['lookup_table'],
                            mapping['lookup_key'],
                            mapping['lookup_value'],
                            str(source_value)
                        )
                        transformed_row[mapping['target'].lower()] = lookup_value
        
        # Hash for change detection
        row_hash = self._calculate_row_hash(transformed_row)
        current_hashes[pk_value] = row_hash
        
        # Check if new or changed
        if pk_value not in last_hashes:
            stats['new_rows'] += 1
            stats['total_rows'] += 1
            return transformed_row
        elif last_hashes[pk_value] != row_hash:
            stats['updated_rows'] += 1
            stats['total_rows'] += 1
            return transformed_row
        else:
            stats['total_rows'] += 1
            return None  # No changes

    def _lookup_value(self, connector_service, target_db, table, key_col, value_col, key_value):
        """Look up a value in the target database"""
        if not key_value or not key_value.strip():
            return None
            
        try:
            # Format the query
            query = f"SELECT {value_col} FROM {table} WHERE {key_col} = %s"
            result = connector_service.execute_query(target_db, query, [key_value])
            
            if result and len(result) > 0:
                # Get first value from first row
                return list(result[0].values())[0]
                
            return None
        except Exception as e:
            _logger.warning(f"Lookup error: {str(e)}")
            return None

    def _calculate_row_hash(self, row):
        """Calculate a hash for a row"""
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
        
        # Create a consistent string representation
        row_str = json.dumps(processed_row, sort_keys=True)
        
        # Hash the string
        return hashlib.sha256(row_str.encode()).hexdigest()

    def _batch_update_rows(self, connector_service, target_db, config, rows):
        """Update rows in the target database using efficient batch operations"""
        if not rows:
            return
            
        try:
            table = config['target_table']
            _logger.info(f"Updating {len(rows)} rows in {table}")
            
            # Get columns and primary key
            columns = list(rows[0].keys())
            
            # Find primary key column
            primary_key = None
            for source_col, mapping in config['mappings'].items():
                if source_col.lower() == config['primary_key'].lower():
                    primary_key = mapping['target'].lower()
                    break
            
            if not primary_key:
                raise ValueError(f"Primary key not found in mappings: {config['primary_key']}")
                
            # Use connector service batch update method
            connector_service.batch_update(target_db, table, primary_key, columns, rows)
                
        except Exception as e:
            _logger.error(f"Error in batch_update_rows: {str(e)}")
            raise UserError(f"Failed to update rows: {str(e)}")

    @api.model
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
            # Get dependencies and connectors
            self._process_dependencies(table_config)
            connector_service, source_db, target_db = self._get_connectors(table_config)
            
            # Get config and sync info
            config = table_config.get_config_json()
            last_sync_time, last_hashes = self._get_last_sync_info(table_id)
            
            # Get source columns
            source_columns = connector_service.get_columns(source_db, config['source_table'])
            query_columns, column_map, primary_key_original = self._prepare_columns(config, source_columns)
            
            # Stats to track progress
            current_hashes = {}
            stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
            
            # Process chunk
            self._process_table_chunk_data(
                connector_service, source_db, target_db, config,
                query_columns, column_map, primary_key_original,
                min_id, max_id, last_hashes, current_hashes, stats, sync_log
            )
            
            # Update sync log
            sync_log.write({
                'end_time': fields.Datetime.now(),
                'status': 'success',
                'total_records': stats['total_rows'],
                'new_records': stats['new_rows'],
                'updated_records': stats['updated_rows'],
                'row_hashes': json.dumps(current_hashes)
            })
            
            return stats
                
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error processing table chunk {table_config.name}: {error_message}")
            
            sync_log.write({
                'end_time': fields.Datetime.now(),
                'status': 'failed',
                'error_message': error_message
            })
            
            raise

    def _process_table_chunk_data(self, connector_service, source_db, target_db, config,
                                 query_columns, column_map, primary_key_original,
                                 min_id, max_id, last_hashes, current_hashes, stats, sync_log):
        """Process a specific chunk of data"""
        batch_size = min(config['batch_size'], 5000)
        formatted_columns = ', '.join(query_columns)
        
        # Process rows in batches
        processed = 0
        current_id = min_id
        
        while True:
            # Build query with ID range
            query = f"SELECT {formatted_columns} FROM {config['source_table']} " \
                    f"WHERE {primary_key_original} >= '{current_id}' " \
                    f"AND {primary_key_original} <= '{max_id}' " \
                    f"ORDER BY {primary_key_original} " \
                    f"LIMIT {batch_size}"
            
            _logger.info(f"Executing chunk query: {query}")
            result_rows = connector_service.execute_query(source_db, query)
            
            if not result_rows:
                _logger.info("No more rows in chunk")
                break
            
            # Process rows
            rows_to_update = []
            batch_count = len(result_rows)
            
            for row in result_rows:
                # Update current ID for next batch
                pk_value = row.get(primary_key_original)
                if pk_value:
                    current_id = pk_value
                
                # Transform row
                transformed_row = self._transform_row(
                    row, column_map, config, primary_key_original, 
                    connector_service, target_db, last_hashes, 
                    current_hashes, stats
                )
                
                if transformed_row:
                    rows_to_update.append(transformed_row)
            
            # Update batch
            if rows_to_update:
                self._batch_update_rows(connector_service, target_db, config, rows_to_update)
            
            # Update progress
            processed += batch_count
            _logger.info(f"Processed {processed} rows in chunk")
            
            # Update log periodically
            if processed % 10000 == 0:
                sync_log.write({
                    'total_records': stats['total_rows'],
                    'new_records': stats['new_rows'],
                    'updated_records': stats['updated_rows']
                })
                self.env.cr.commit()
            
            # Release memory
            result_rows = None
            rows_to_update = None
            gc.collect()
            
            # If we got fewer rows than requested or reached the max ID, we're done
            if batch_count < batch_size or current_id >= max_id:
                break

    @api.model
    def run_scheduled_sync(self, frequency_code='daily'):
        """Run scheduled synchronization for tables"""
        frequency = self.env['etl.frequency'].search([('code', '=', frequency_code)], limit=1)
        if not frequency:
            _logger.error(f"Frequency '{frequency_code}' not found")
            return
        
        tables = self.env['etl.source.table'].search([
            ('frequency_id', '=', frequency.id),
            ('active', '=', True)
        ])
        
        # Process base tables first (no dependencies)
        base_tables = tables.filtered(lambda t: t.is_base_table)
        for table in base_tables:
            try:
                job = table.with_delay(
                    description=f"Scheduled sync for base table: {table.name}",
                    channel="etl"
                ).sync_table_job()
                
                table.write({
                    'job_uuid': job.uuid,
                    'job_status': 'pending'
                })
                
                _logger.info(f"Scheduled sync job queued for base table: {table.name}")
            except Exception as e:
                _logger.error(f"Failed to queue sync job for table {table.name}: {str(e)}")
        
        # Then process tables with dependencies
        dependency_tables = tables - base_tables
        for table in dependency_tables:
            try:
                # Wait for base tables
                job = table.with_delay(
                    description=f"Scheduled sync for dependent table: {table.name}",
                    channel="etl"
                ).sync_table_job()
                
                table.write({
                    'job_uuid': job.uuid,
                    'job_status': 'pending'
                })
                
                _logger.info(f"Scheduled sync job queued for dependent table: {table.name}")
            except Exception as e:
                _logger.error(f"Failed to queue sync job for table {table.name}: {str(e)}")
