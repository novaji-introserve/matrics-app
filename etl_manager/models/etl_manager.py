# -*- coding: utf-8 -*-
from datetime import datetime
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
from psycopg2.extras import execute_values
import re

_logger = logging.getLogger(__name__)

class ETLManager(models.AbstractModel):
    _name = 'etl.manager'
    _description = 'ETL Process Manager'
    
    # Class level attributes for cache and locks
    _lookup_cache = {}
    _lookup_cache_lock = threading.Lock()
    _processed_tables = set()
    _processed_tables_lock = threading.Lock()
    
    @api.model
    def run_scheduled_sync(self, frequency_code):
        """Run scheduled sync based on frequency code"""
        _logger.info(f"Starting scheduled ETL sync for frequency: {frequency_code}")
        
        try:
            # Get frequency ID
            frequency = self.env['etl.frequency'].search([('code', '=', frequency_code)], limit=1)
            if not frequency:
                _logger.error(f"Frequency code not found: {frequency_code}")
                return False
            
            # Get tables for this frequency
            tables = self.env['etl.source.table'].search([
                ('frequency_id', '=', frequency.id),
                ('active', '=', True)
            ])
            
            if not tables:
                _logger.info(f"No active tables found for frequency: {frequency_code}")
                return True
            
            _logger.info(f"Found {len(tables)} tables to sync for frequency: {frequency_code}")
            
            # Group tables by dependencies - tables with no dependencies first
            base_tables = tables.filtered(lambda t: not t.dependency_ids and t.is_base_table)
            dependent_tables = tables - base_tables
            
            # Process base tables first (can be parallelized)
            for table in base_tables:
                # Queue async job for each table
                table.with_delay(
                    description=f"ETL sync {frequency_code}: {table.name}",
                    channel="etl"
                ).sync_table_job()
            
            # Process tables with dependencies in order
            processed_ids = set(base_tables.ids)
            remaining_tables = dependent_tables
            
            # Keep processing until all tables are done or no progress is made
            max_iterations = 10  # Prevent infinite loop
            for iteration in range(max_iterations):
                if not remaining_tables:
                    break
                    
                tables_processed_this_round = self.env['etl.source.table']
                
                for table in remaining_tables:
                    # Check if all dependencies are processed
                    dependency_ids = set(table.dependency_ids.ids)
                    if dependency_ids.issubset(processed_ids):
                        # All dependencies processed, queue this table
                        table.with_delay(
                            description=f"ETL sync {frequency_code}: {table.name}",
                            channel="etl"
                        ).sync_table_job()
                        
                        tables_processed_this_round += table
                        processed_ids.add(table.id)
                
                # Remove processed tables from remaining
                remaining_tables -= tables_processed_this_round
                
                # If no tables were processed this round and we still have tables,
                # we might have circular dependencies
                if not tables_processed_this_round and remaining_tables:
                    _logger.warning(f"Potential circular dependencies in remaining tables: {remaining_tables.mapped('name')}")
                    # Process remaining tables anyway
                    for table in remaining_tables:
                        table.with_delay(
                            description=f"ETL sync {frequency_code}: {table.name} (dependency warning)",
                            channel="etl"
                        ).sync_table_job()
                    break
            
            _logger.info(f"Scheduled ETL sync for frequency {frequency_code} completed")
            return True
            
        except Exception as e:
            _logger.error(f"Error in scheduled ETL sync: {str(e)}")
            return False
    
    def _get_connectors(self, table_config):
        """Get database connectors without recursion issues"""
        source_db = table_config.source_db_connection_id
        target_db = table_config.target_db_connection_id
        
        # Get direct connector service
        connector_service = self.env['etl.database.connector.service']
        
        return connector_service, source_db, target_db
    
    def _is_dependency_processed(self, table_id):
        """Check if a dependency has been processed recently"""
        today = fields.Date.today()
        return self.env['etl.sync.log'].search_count([
            ('table_id', '=', table_id),
            ('start_time', '>=', datetime.combine(today, datetime.min.time())),
            ('status', '=', 'success')
        ]) > 0
    
    def _process_dependencies(self, table_config):
        """Ensure all dependencies are processed first"""
        if not table_config.dependency_ids:
            return
            
        # For each dependency, check if it has been processed today
        dependencies_to_process = []
        
        for dependency in table_config.dependency_ids:
            if not self._is_dependency_processed(dependency.id):
                dependencies_to_process.append(dependency)
        
        if not dependencies_to_process:
            return
            
        _logger.info(f"Processing dependencies for table {table_config.name}: {', '.join(d.name for d in dependencies_to_process)}")
        
        # Process each dependency
        for dependency in dependencies_to_process:
            self.process_table(dependency)

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
    
    @api.model
    def process_table(self, table_config):
        """Process a table with optimized SQL and memory management"""
        start_time = time.time()
        
        # Track memory usage
        process = psutil.Process(os.getpid())
        mem_before = process.memory_info().rss / 1024 / 1024  # MB
        
        # Create sync log
        sync_log = self.env['etl.sync.log'].create({
            'table_id': table_config.id,
            'start_time': fields.Datetime.now(),
            'status': 'running'
        })
        
        try:
            # Process dependencies
            self._process_dependencies(table_config)
            
            # Get connectors
            connector_service, source_db, target_db = self._get_connectors(table_config)
            
            # Get config
            config = table_config.get_config_json()
            
            # Get source columns
            source_columns = connector_service.get_columns(source_db, config['source_table'])
            
            # Prepare query columns
            query_columns, column_map, primary_key_original = self._prepare_columns(config, source_columns)
            
            # Get last sync info
            last_sync_time, last_hashes = self._get_last_sync_info(table_config.id)
            
            # Stats to track progress
            stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
            
            # Check if table is large and needs special handling
            large_table_threshold = 50000  # 50k rows
            try:
                total_rows = connector_service.get_table_count(source_db, config['source_table'])
                if total_rows > large_table_threshold:
                    return self._process_large_table(
                        connector_service, source_db, target_db, 
                        config, query_columns, column_map, 
                        primary_key_original, last_hashes, stats, sync_log
                    )
            except Exception as e:
                _logger.warning(f"Could not get table count, using standard processing: {str(e)}")
            
            # Standard processing for smaller tables
            self._process_standard_table(
                connector_service, source_db, target_db, 
                config, query_columns, column_map,
                primary_key_original, last_hashes, stats
            )
            
            # Update sync log
            current_hashes = {}  # We'll collect these during processing
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
            
            # Calculate memory and time metrics
            mem_after = process.memory_info().rss / 1024 / 1024  # MB
            total_time = time.time() - start_time
            
            _logger.info(f"Completed ETL for table {table_config.name} in {total_time:.2f}s. "
                        f"Rows: {stats['total_rows']} total, {stats['new_rows']} new, {stats['updated_rows']} updated. "
                        f"Memory: {mem_before:.1f}MB → {mem_after:.1f}MB, diff: {mem_after - mem_before:+.1f}MB")
            
            return stats
                
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error processing table {table_config.name}: {error_message}")
            
            # Update sync log
            sync_log.write({
                'end_time': fields.Datetime.now(),
                'status': 'failed',
                'error_message': error_message
            })
            
            # Update table status
            table_config.write({
                'last_sync_status': 'failed',
                'last_sync_message': error_message
            })
            
            raise
        
    def process_batch(self, connector_service, source_db, target_db, config, result_rows, column_map, primary_key_original):
        """
        Process a batch of rows from source data.
        This method doesn't handle transactions - it's called from a context that manages transactions.
        
        Args:
            connector_service: Database connector service
            source_db: Source database connection
            target_db: Target database connection
            config: ETL configuration
            result_rows: Rows fetched from source
            column_map: Column mapping
            primary_key_original: Original primary key column name
            
        Returns:
            dict: Statistics about the processed batch
        """
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
        
        if not result_rows:
            return stats
        
        # Get hash mechanism
        last_sync_time, last_hashes = self._get_last_sync_info(config.get('table_id', 0))
        current_hashes = {}
        
        # Process rows
        rows_to_update = []
        
        for row in result_rows:
            # Get primary key
            pk_value = str(row.get(primary_key_original))
            if not pk_value:
                continue
            
            # Transform row
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
            
            # Determine if new or changed
            is_new = pk_value not in last_hashes
            is_updated = not is_new and last_hashes.get(pk_value) != row_hash
            
            if is_new:
                stats['new_rows'] += 1
                stats['total_rows'] += 1
                rows_to_update.append(transformed_row)
            elif is_updated:
                stats['updated_rows'] += 1
                stats['total_rows'] += 1
                rows_to_update.append(transformed_row)
            else:
                # Still count in total
                stats['total_rows'] += 1
        
        # Batch update rows
        if rows_to_update:
            self._batch_update_rows(connector_service, target_db, config, rows_to_update)
        
        return stats
    
    def _process_standard_table(self, connector_service, source_db, target_db, 
                                config, query_columns, column_map, 
                                primary_key_original, last_hashes, stats):
        """Process a standard (smaller) table efficiently"""
        batch_size = config['batch_size']
        formatted_columns = ', '.join(query_columns)
        
        # Build query for efficiency
        db_type = source_db.db_type_code
        query = self._build_efficient_query(
            db_type, config['source_table'], formatted_columns, batch_size
        )
        
        # Execute query
        result_rows = connector_service.execute_query(source_db, query)
        if not result_rows:
            _logger.info(f"No rows found in table {config['source_table']}")
            return
        
        # Process rows in memory
        rows_to_update = []
        current_hashes = {}
        
        for row in result_rows:
            # Get primary key
            pk_value = str(row.get(primary_key_original))
            if not pk_value:
                continue
            
            # Transform row
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
            
            # Determine if new or changed
            is_new = pk_value not in last_hashes
            is_updated = not is_new and last_hashes.get(pk_value) != row_hash
            
            if is_new:
                stats['new_rows'] += 1
                stats['total_rows'] += 1
                rows_to_update.append(transformed_row)
            elif is_updated:
                stats['updated_rows'] += 1
                stats['total_rows'] += 1
                rows_to_update.append(transformed_row)
        
        # Batch update rows for better performance
        if rows_to_update:
            self._batch_update_rows(connector_service, target_db, config, rows_to_update)
    
    def _build_efficient_query(self, db_type, table, formatted_columns, limit=None):
        """Build an optimized query based on database type"""
        if db_type == 'postgresql':
            query = f'SELECT {formatted_columns} FROM "{table}"'
            if limit:
                query += f" LIMIT {limit}"
        elif db_type == 'mssql':
            query = f"SELECT {formatted_columns} FROM [{table}]"
            if limit:
                query = f"SELECT TOP {limit} {formatted_columns} FROM [{table}]"
        elif db_type == 'mysql':
            query = f"SELECT {formatted_columns} FROM `{table}`"
            if limit:
                query += f" LIMIT {limit}"
        else:
            # Generic query format
            query = f"SELECT {formatted_columns} FROM {table}"
            if limit:
                query += f" LIMIT {limit}"
        
        return query
    
    def _process_large_table(self, connector_service, source_db, target_db, 
                            config, query_columns, column_map, 
                            primary_key_original, last_hashes, stats, sync_log):
        """Process a large table with chunking for memory efficiency"""
        batch_size = min(config['batch_size'], 10000)  # Cap batch size for large tables
        formatted_columns = ', '.join(query_columns)
        
        # Determine total rows and calculate chunks
        try:
            total_rows = connector_service.get_table_count(source_db, config['source_table'])
            _logger.info(f"Processing large table {config['source_table']} with {total_rows} rows")
            
            total_chunks = math.ceil(total_rows / batch_size)
            
            # Create temporary table for hashes
            temp_hash_table = f"tmp_hash_{config['target_table']}_{int(time.time())}"
            self._create_temp_hash_table(connector_service, target_db, temp_hash_table)
            
            # Pre-populate temp hash table with existing hashes
            self._populate_temp_hash_table(connector_service, target_db, temp_hash_table, last_hashes)
            
            # Process table in chunks
            current_offset = 0
            current_hashes = {}
            
            for chunk in range(total_chunks):
                # Update progress
                progress = (chunk / total_chunks) * 100
                if hasattr(self.env.context, 'progress_tracker'):
                    self.env.context.progress_tracker(progress, 
                                                 f"Processing chunk {chunk+1}/{total_chunks}")
                
                # Process this chunk
                chunk_start_time = time.time()
                
                chunk_stats = self._process_table_chunk_with_offset(
                    connector_service, source_db, target_db,
                    config, query_columns, column_map,
                    primary_key_original, current_offset, batch_size,
                    temp_hash_table, current_hashes
                )
                
                # Update stats
                stats['total_rows'] += chunk_stats.get('total_rows', 0)
                stats['new_rows'] += chunk_stats.get('new_rows', 0)
                stats['updated_rows'] += chunk_stats.get('updated_rows', 0)
                
                # Update offset for next chunk
                current_offset += batch_size
                
                # Log chunk completion
                chunk_time = time.time() - chunk_start_time
                _logger.info(f"Processed chunk {chunk+1}/{total_chunks} in {chunk_time:.2f}s: "
                           f"Rows: {chunk_stats.get('total_rows', 0)} total, "
                           f"{chunk_stats.get('new_rows', 0)} new, "
                           f"{chunk_stats.get('updated_rows', 0)} updated")
                
                # Update sync log periodically
                if chunk % 5 == 0 or chunk == total_chunks - 1:
                    sync_log.write({
                        'total_records': stats['total_rows'],
                        'new_records': stats['new_rows'],
                        'updated_records': stats['updated_rows']
                    })
                    
                    # Commit transaction to avoid locks
                    self.env.cr.commit()
                
                # Force garbage collection after each chunk
                gc.collect()
            
            # Clean up temporary hash table
            self._drop_temp_hash_table(connector_service, target_db, temp_hash_table)
            
            # Return the collected stats
            return stats
            
        except Exception as e:
            _logger.error(f"Error processing large table: {str(e)}")
            
            # Clean up temp table if created
            if 'temp_hash_table' in locals():
                try:
                    self._drop_temp_hash_table(connector_service, target_db, temp_hash_table)
                except:
                    pass
            
            raise
    
    def _create_temp_hash_table(self, connector_service, target_db, temp_table_name):
        """Create a temporary table for hash storage"""
        db_type = target_db.db_type_code
        
        if db_type == 'postgresql':
            query = f"""
                CREATE TEMP TABLE "{temp_table_name}" (
                    record_key VARCHAR(255) PRIMARY KEY,
                    record_hash VARCHAR(64) NOT NULL,
                    processed BOOLEAN DEFAULT FALSE
                )
            """
        elif db_type == 'mssql':
            query = f"""
                CREATE TABLE #{temp_table_name} (
                    record_key VARCHAR(255) PRIMARY KEY,
                    record_hash VARCHAR(64) NOT NULL,
                    processed BIT DEFAULT 0
                )
            """
        elif db_type == 'mysql':
            query = f"""
                CREATE TEMPORARY TABLE `{temp_table_name}` (
                    record_key VARCHAR(255) PRIMARY KEY,
                    record_hash VARCHAR(64) NOT NULL,
                    processed BOOLEAN DEFAULT FALSE
                )
            """
        else:
            # Generic approach
            query = f"""
                CREATE TEMPORARY TABLE {temp_table_name} (
                    record_key VARCHAR(255) PRIMARY KEY,
                    record_hash VARCHAR(64) NOT NULL,
                    processed BOOLEAN DEFAULT FALSE
                )
            """
        
        connector_service.execute_query(target_db, query)
        _logger.debug(f"Created temporary hash table: {temp_table_name}")
    
    def _populate_temp_hash_table(self, connector_service, target_db, temp_table_name, hashes):
        """Populate temporary hash table with existing hashes"""
        if not hashes:
            return
            
        db_type = target_db.db_type_code
        batch_size = 1000
        
        # Convert hashes dict to list of tuples for batch insert
        hash_items = list(hashes.items())
        
        for i in range(0, len(hash_items), batch_size):
            batch = hash_items[i:i + batch_size]
            
            if db_type == 'postgresql':
                # Use execute_values for efficient batch insert
                from psycopg2.extras import execute_values
                
                with connector_service.cursor(target_db) as cursor:
                    # Create a list of tuples (key, hash, processed)
                    values = [(k, v, False) for k, v in batch]
                    
                    insert_query = f"""
                        INSERT INTO "{temp_table_name}" (record_key, record_hash, processed)
                        VALUES %s
                    """
                    
                    execute_values(cursor, insert_query, values)
                    
            elif db_type == 'mssql':
                # For MSSQL, use individual inserts for now
                for key, value in batch:
                    query = f"""
                        INSERT INTO #{temp_table_name} (record_key, record_hash, processed)
                        VALUES (?, ?, 0)
                    """
                    connector_service.execute_query(target_db, query, [key, value])
                    
            elif db_type == 'mysql':
                # For MySQL, use multi-value INSERT
                placeholders = ", ".join(["(%s, %s, FALSE)"] * len(batch))
                values = []
                
                for key, value in batch:
                    values.extend([key, value])
                
                insert_query = f"""
                    INSERT INTO `{temp_table_name}` (record_key, record_hash, processed)
                    VALUES {placeholders}
                """
                
                connector_service.execute_query(target_db, insert_query, values)
                
            else:
                # Generic approach
                for key, value in batch:
                    query = f"""
                        INSERT INTO {temp_table_name} (record_key, record_hash, processed)
                        VALUES (%s, %s, FALSE)
                    """
                    connector_service.execute_query(target_db, query, [key, value])
    
    def _drop_temp_hash_table(self, connector_service, target_db, temp_table_name):
        """Drop the temporary hash table"""
        db_type = target_db.db_type_code
        
        try:
            if db_type == 'postgresql':
                query = f'DROP TABLE IF EXISTS "{temp_table_name}"'
            elif db_type == 'mssql':
                query = f"IF OBJECT_ID('tempdb..#{temp_table_name}') IS NOT NULL DROP TABLE #{temp_table_name}"
            elif db_type == 'mysql':
                query = f"DROP TEMPORARY TABLE IF EXISTS `{temp_table_name}`"
            else:
                query = f"DROP TABLE IF EXISTS {temp_table_name}"
            
            connector_service.execute_query(target_db, query)
            _logger.debug(f"Dropped temporary hash table: {temp_table_name}")
        except Exception as e:
            _logger.warning(f"Error dropping temporary hash table {temp_table_name}: {str(e)}")
    
    def _process_table_chunk_with_offset(self, connector_service, source_db, target_db,
                                       config, query_columns, column_map,
                                       primary_key_original, offset, limit,
                                       temp_hash_table, current_hashes):
        """Process a chunk of the table using OFFSET/LIMIT pagination"""
        db_type = source_db.db_type_code
        formatted_columns = ', '.join(query_columns)
        
        # Build optimized query based on database type
        if db_type == 'postgresql':
            query = f"""
                SELECT {formatted_columns} FROM "{config['source_table']}" 
                ORDER BY "{primary_key_original}" 
                LIMIT {limit} OFFSET {offset}
            """
        elif db_type == 'mssql':
            # For SQL Server, use optimized pagination
            if offset == 0:
                query = f"""
                    SELECT TOP {limit} {formatted_columns}
                    FROM [{config['source_table']}]
                    ORDER BY [{primary_key_original}]
                """
            else:
                # For SQL Server 2012+, use OFFSET-FETCH
                query = f"""
                    SELECT {formatted_columns}
                    FROM [{config['source_table']}]
                    ORDER BY [{primary_key_original}]
                    OFFSET {offset} ROWS
                    FETCH NEXT {limit} ROWS ONLY
                """
        elif db_type == 'mysql':
            query = f"""
                SELECT {formatted_columns} FROM `{config['source_table']}` 
                ORDER BY `{primary_key_original}` 
                LIMIT {limit} OFFSET {offset}
            """
        else:
            # Generic fallback
            query = f"""
                SELECT {formatted_columns} FROM {config['source_table']} 
                ORDER BY {primary_key_original} 
                LIMIT {limit} OFFSET {offset}
            """
        
        # Execute query
        result_rows = connector_service.execute_query(source_db, query)
        
        # Process stats
        chunk_stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
        
        if not result_rows:
            return chunk_stats
        
        # Process rows
        rows_to_update = []
        
        for row in result_rows:
            # Get primary key
            pk_value = str(row.get(primary_key_original))
            if not pk_value:
                continue
            
            # Transform row
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
            
            # Check in temp hash table
            is_new, is_updated = self._check_hash_in_temp_table(
                connector_service, target_db, temp_hash_table, pk_value, row_hash
            )
            
            if is_new:
                chunk_stats['new_rows'] += 1
                chunk_stats['total_rows'] += 1
                rows_to_update.append(transformed_row)
            elif is_updated:
                chunk_stats['updated_rows'] += 1
                chunk_stats['total_rows'] += 1
                rows_to_update.append(transformed_row)
            else:
                # Still count the row even if not changed
                chunk_stats['total_rows'] += 1
        
        # Batch update rows
        if rows_to_update:
            self._batch_update_rows(connector_service, target_db, config, rows_to_update)
        
        return chunk_stats
    
    def _check_hash_in_temp_table(self, connector_service, target_db, temp_table_name, key, hash_value):
        """Check if hash exists in temp table and mark as processed"""
        db_type = target_db.db_type_code
        
        # Query to check if key exists
        if db_type == 'postgresql':
            query = f"""
                SELECT record_hash FROM "{temp_table_name}" 
                WHERE record_key = %s
            """
        elif db_type == 'mssql':
            query = f"""
                SELECT record_hash FROM #{temp_table_name} 
                WHERE record_key = ?
            """
        elif db_type == 'mysql':
            query = f"""
                SELECT record_hash FROM `{temp_table_name}` 
                WHERE record_key = %s
            """
        else:
            query = f"""
                SELECT record_hash FROM {temp_table_name} 
                WHERE record_key = %s
            """
        
        result = connector_service.execute_query(target_db, query, [key])
        
        if not result:
            # Key doesn't exist, it's a new record
            # Insert the new hash
            if db_type == 'postgresql':
                insert_query = f"""
                    INSERT INTO "{temp_table_name}" (record_key, record_hash, processed)
                    VALUES (%s, %s, TRUE)
                """
            elif db_type == 'mssql':
                insert_query = f"""
                    INSERT INTO #{temp_table_name} (record_key, record_hash, processed)
                    VALUES (?, ?, 1)
                """
            elif db_type == 'mysql':
                insert_query = f"""
                    INSERT INTO `{temp_table_name}` (record_key, record_hash, processed)
                    VALUES (%s, %s, TRUE)
                """
            else:
                insert_query = f"""
                    INSERT INTO {temp_table_name} (record_key, record_hash, processed)
                    VALUES (%s, %s, TRUE)
                """
                
            connector_service.execute_query(target_db, insert_query, [key, hash_value])
            return True, False  # New record
        
        # Record exists, check if hash matches
        existing_hash = result[0]['record_hash']
        is_updated = existing_hash != hash_value
        
        # Update hash and mark as processed
        if is_updated:
            if db_type == 'postgresql':
                update_query = f"""
                    UPDATE "{temp_table_name}" 
                    SET record_hash = %s, processed = TRUE
                    WHERE record_key = %s
                """
            elif db_type == 'mssql':
                update_query = f"""
                    UPDATE #{temp_table_name} 
                    SET record_hash = ?, processed = 1
                    WHERE record_key = ?
                """
            elif db_type == 'mysql':
                update_query = f"""
                    UPDATE `{temp_table_name}` 
                    SET record_hash = %s, processed = TRUE
                    WHERE record_key = %s
                """
            else:
                update_query = f"""
                    UPDATE {temp_table_name} 
                    SET record_hash = %s, processed = TRUE
                    WHERE record_key = %s
                """
                
            connector_service.execute_query(target_db, update_query, [hash_value, key])
        else:
            # Just mark as processed
            if db_type == 'postgresql':
                update_query = f"""
                    UPDATE "{temp_table_name}" 
                    SET processed = TRUE
                    WHERE record_key = %s
                """
            elif db_type == 'mssql':
                update_query = f"""
                    UPDATE #{temp_table_name} 
                    SET processed = 1
                    WHERE record_key = ?
                """
            elif db_type == 'mysql':
                update_query = f"""
                    UPDATE `{temp_table_name}` 
                    SET processed = TRUE
                    WHERE record_key = %s
                """
            else:
                update_query = f"""
                    UPDATE {temp_table_name} 
                    SET processed = TRUE
                    WHERE record_key = %s
                """
                
            connector_service.execute_query(target_db, update_query, [key])
        
        return False, is_updated  # Existing record (possibly updated)
    
    def _lookup_value(self, connector_service, target_db, table, key_col, value_col, key_value):
        """Optimized lookup implementation with caching"""
        if not key_value or not key_value.strip():
            return None
            
        # Cache key
        cache_key = f"{table}:{key_col}:{key_value}"
        
        # Use thread-safe access to cache
        with self._lookup_cache_lock:
            if cache_key in self._lookup_cache:
                return self._lookup_cache[cache_key]
            
        try:
            # Format the query based on DB type
            db_type = target_db.db_type_code
            
            if db_type == 'postgresql':
                query = f'SELECT "{value_col}" FROM "{table}" WHERE "{key_col}" = %s LIMIT 1'
            elif db_type == 'mssql':
                query = f"SELECT [{value_col}] FROM [{table}] WHERE [{key_col}] = ?"
            elif db_type == 'mysql':
                query = f"SELECT `{value_col}` FROM `{table}` WHERE `{key_col}` = %s LIMIT 1"
            else:
                query = f"SELECT {value_col} FROM {table} WHERE {key_col} = %s"
                
            result = connector_service.execute_query(target_db, query, [key_value])
            
            if result and len(result) > 0:
                # Get first value column
                value = result[0][value_col] if value_col in result[0] else list(result[0].values())[0]
                
                # Cache the result
                with self._lookup_cache_lock:
                    self._lookup_cache[cache_key] = value
                
                return value
                
            # Cache null result too
            with self._lookup_cache_lock:
                self._lookup_cache[cache_key] = None
            
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
        if not rows:
            return
        try:
            table = config['target_table']
            _logger.info(f"Updating {len(rows)} rows in {table}")
            columns = list(rows[0].keys())
            _logger.debug(f"Columns to update: {columns}")
            _logger.debug(f"Sample row: {rows[0]}")
            _logger.debug(f"Config: {config}")
            
            if 'mappings' not in config or not config['mappings']:
                _logger.error(f"No mappings defined in config for table {table}")
                raise ValidationError("No column mappings defined for the table")
            
            primary_key = None
            for source_col, mapping in config['mappings'].items():
                if not isinstance(mapping, dict) or 'target' not in mapping:
                    _logger.error(f"Invalid mapping for column {source_col}: {mapping}")
                    raise ValidationError(f"Invalid mapping for column {source_col}")
                if source_col.lower() == config['primary_key'].lower():
                    primary_key = mapping['target'].lower()
                    break
            
            if not primary_key:
                _logger.error(f"Primary key {config['primary_key']} not found in mappings")
                raise ValueError(f"Primary key not found in mappings: {config['primary_key']}")
            
            connector_service.batch_update(target_db, table, primary_key, columns, rows)
        except Exception as e:
            _logger.error(f"Error in batch_update: {str(e)}")
            raise ValidationError(f"Failed to update rows: {str(e)}")
    
    @api.model
    def process_table_chunk(self, table_config, min_id, max_id):
        """Process a specific chunk of a table based on ID range"""
        start_time = time.time()
        
        # Create sync log
        sync_log = self.env['etl.sync.log'].create({
            'table_id': table_config.id,
            'start_time': fields.Datetime.now(),
            'status': 'running'
        })
        
        try:
            # Get connectors
            connector_service, source_db, target_db = self._get_connectors(table_config)
            
            # Get config
            config = table_config.get_config_json()
            
            # Get source columns
            source_columns = connector_service.get_columns(source_db, config['source_table'])
            
            # Prepare query columns
            query_columns, column_map, primary_key_original = self._prepare_columns(config, source_columns)
            
            # Get last sync info
            last_sync_time, last_hashes = self._get_last_sync_info(table_config.id)
            
            # Stats to track progress
            stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
            current_hashes = {}
            
            # Build query with ID range
            formatted_columns = ', '.join(query_columns)
            db_type = source_db.db_type_code
            
            if db_type == 'postgresql':
                query = f"""
                    SELECT {formatted_columns} FROM "{config['source_table']}" 
                    WHERE "{primary_key_original}" >= %s AND "{primary_key_original}" < %s
                """
                params = [min_id, max_id]
            elif db_type == 'mssql':
                query = f"""
                    SELECT {formatted_columns} FROM [{config['source_table']}] 
                    WHERE [{primary_key_original}] >= ? AND [{primary_key_original}] < ?
                """
                params = [min_id, max_id]
            elif db_type == 'mysql':
                query = f"""
                    SELECT {formatted_columns} FROM `{config['source_table']}` 
                    WHERE `{primary_key_original}` >= %s AND `{primary_key_original}` < %s
                """
                params = [min_id, max_id]
            else:
                # Generic approach
                query = f"""
                    SELECT {formatted_columns} FROM {config['source_table']} 
                    WHERE {primary_key_original} >= %s AND {primary_key_original} < %s
                """
                params = [min_id, max_id]
            
            # Execute query
            result_rows = connector_service.execute_query(source_db, query, params)
            
            if not result_rows:
                _logger.info(f"No rows found in chunk {min_id} - {max_id}")
                
                # Update sync log
                sync_log.write({
                    'end_time': fields.Datetime.now(),
                    'status': 'success',
                    'total_records': 0
                })
                
                return stats
            
            # Process rows
            rows_to_update = []
            
            for row in result_rows:
                # Get primary key
                pk_value = str(row.get(primary_key_original))
                if not pk_value:
                    continue
                
                # Transform row
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
                
                # Determine if new or changed
                is_new = pk_value not in last_hashes
                is_updated = not is_new and last_hashes.get(pk_value) != row_hash
                
                if is_new:
                    stats['new_rows'] += 1
                    stats['total_rows'] += 1
                    rows_to_update.append(transformed_row)
                elif is_updated:
                    stats['updated_rows'] += 1
                    stats['total_rows'] += 1
                    rows_to_update.append(transformed_row)
                else:
                    # Count the row even if unchanged
                    stats['total_rows'] += 1
            
            # Batch update rows
            if rows_to_update:
                self._batch_update_rows(connector_service, target_db, config, rows_to_update)
            
            # Update sync log
            sync_log.write({
                'end_time': fields.Datetime.now(),
                'status': 'success',
                'total_records': stats['total_rows'],
                'new_records': stats['new_rows'],
                'updated_records': stats['updated_rows'],
                'row_hashes': json.dumps(current_hashes)
            })
            
            total_time = time.time() - start_time
            _logger.info(f"Processed chunk {min_id} - {max_id} in {total_time:.2f}s: "
                       f"{stats['total_rows']} rows ({stats['new_rows']} new, {stats['updated_rows']} updated)")
            
            return stats
                
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error processing chunk {min_id} - {max_id}: {error_message}")
            
            # Update sync log
            sync_log.write({
                'end_time': fields.Datetime.now(),
                'status': 'failed',
                'error_message': error_message
            })
            
            raise
    
    def run_scheduled_sync(self, frequency_code):
        """Run scheduled sync for tables with the given frequency"""
        # Get all active tables with the given frequency
        tables = self.env['etl.source.table'].search([
            ('active', '=', True),
            ('frequency_id.code', '=', frequency_code)
        ])
        
        if not tables:
            _logger.info(f"No tables found with frequency: {frequency_code}")
            return
        
        _logger.info(f"Running scheduled sync for {len(tables)} tables with frequency: {frequency_code}")
        
        # Process tables with dependencies in correct order
        processed_tables = set()
        
        # First process tables without dependencies
        base_tables = tables.filtered(lambda t: not t.dependency_ids or all(d.id in processed_tables for d in t.dependency_ids))
        
        for table in base_tables:
            try:
                # Queue a sync job
                job = table.with_delay(
                    description=f"Scheduled sync for {table.name}",
                    channel="etl_scheduled",
                    priority=5
                ).sync_table_job()
                
                # Mark as queued
                table.write({
                    'job_uuid': job.uuid,
                    'job_status': 'pending',
                    'last_sync_status': 'running',
                    'last_sync_message': 'Scheduled sync job queued',
                    'progress_percentage': 0
                })
                
                processed_tables.add(table.id)
                
            except Exception as e:
                _logger.error(f"Error scheduling sync for {table.name}: {str(e)}")
        
        # Then process remaining tables
        remaining_tables = tables - self.env['etl.source.table'].browse(list(processed_tables))
        
        # Group by dependency level
        dependency_levels = self._group_by_dependency_level(remaining_tables)
        
        # Process each level
        for level, level_tables in dependency_levels.items():
            for table in level_tables:
                try:
                    # Queue a sync job
                    job = table.with_delay(
                        description=f"Scheduled sync for {table.name}",
                        channel="etl_scheduled",
                        priority=level + 5  # Higher levels get lower priority
                    ).sync_table_job()
                    
                    # Mark as queued
                    table.write({
                        'job_uuid': job.uuid,
                        'job_status': 'pending',
                        'last_sync_status': 'running',
                        'last_sync_message': 'Scheduled sync job queued',
                        'progress_percentage': 0
                    })
                    
                except Exception as e:
                    _logger.error(f"Error scheduling sync for {table.name}: {str(e)}")
    
    def _group_by_dependency_level(self, tables):
        """Group tables by dependency level to process in correct order"""
        level_map = {}
        unassigned = tables
        
        # Find tables with no dependencies first (level 0)
        level = 0
        while unassigned:
            current_level = []
            
            for table in unassigned:
                # Check if all dependencies are in previous levels
                deps_in_tables = table.dependency_ids & tables
                if not deps_in_tables or all(table2.id in [table3.id for level_tables in level_map.values() for table3 in level_tables] for table2 in deps_in_tables):
                    current_level.append(table)
            
            if not current_level:
                # If we get here, there might be circular dependencies
                # Just add remaining tables to current level
                current_level = unassigned
            
            level_map[level] = current_level
            unassigned = unassigned - self.env['etl.source.table'].browse([table.id for table in current_level])
            level += 1
            
            if level > 10:  # Safety check
                level_map[level] = unassigned
                break
        
        return level_map

