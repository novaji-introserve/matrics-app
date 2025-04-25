# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import gc
import traceback
from odoo import models, fields, api, _
import logging
import threading
import time
import json
import os
import psutil

_logger = logging.getLogger(__name__)

class ETLParallelProcessor(models.AbstractModel):
    _name = 'etl.parallel.processor'
    _description = 'ETL Parallel Processing Manager'
    
    # Class-level thread-safe storage
    _process_locks = {}
    _locks_mutex = threading.RLock()
    _job_metrics = {}
    _metrics_mutex = threading.RLock()
    
    @api.model
    def process_table_parallel(self, table_config, worker_count=None):
        """
        Process a table using parallel workers with proper coordination.
        
        Args:
            table_config: ETLSourceTable record to process
            worker_count: Number of parallel workers (default: auto-determine)
            
        Returns:
            Job UUID of the coordinator job
        """
        # Determine optimal worker count if not specified
        if not worker_count:
            worker_count = self._determine_worker_count(table_config)
        
        # Create the coordinator job that will monitor and manage workers
        coordinator_job = self.with_delay(
            description=f"ETL Coordinator: {table_config.name}",
            channel="etl_coordinator",
            priority=5
        ).parallel_coordinator(table_config.id, worker_count)
        
        # Update table config with coordinator job info
        table_config.write({
            'job_uuid': coordinator_job.uuid,
            'job_status': 'pending',
            'last_sync_status': 'running',
            'last_sync_message': f'Parallel sync started with {worker_count} workers',
            'progress_percentage': 0
        })
        
        return coordinator_job.uuid
    
    @api.model
    def _determine_worker_count(self, table_config):
        """Determine optimal number of workers based on system resources and table size"""
        try:
            # Get CPU count 
            cpu_count = os.cpu_count() or 4
            
            # Get memory info
            mem = psutil.virtual_memory()
            total_gb = mem.total / (1024 * 1024 * 1024)
            available_gb = mem.available / (1024 * 1024 * 1024)
            
            # Get table size if possible
            table_size = 0
            try:
                connector_service = self.env['etl.database.connector.service']
                source_db = table_config.source_db_connection_id
                table_size = connector_service.get_table_count(source_db, table_config.name)
            except:
                # Couldn't get table size, use default approach
                pass
            
            # Calculate optimal worker count
            if table_size > 0:
                # Base worker count on table size
                if table_size > 5000000:  # Very large table (>5M rows)
                    base_workers = min(cpu_count, 8)
                elif table_size > 1000000:  # Large table (1-5M rows)
                    base_workers = min(cpu_count, 6)
                elif table_size > 100000:  # Medium table (100K-1M rows)
                    base_workers = min(cpu_count, 4)
                else:  # Small table
                    base_workers = min(cpu_count, 2)
            else:
                # Default based on CPU
                base_workers = max(2, cpu_count // 2)
            
            # Adjust based on available memory
            if available_gb < 2:  # Less than 2GB available
                # Limit workers in low memory
                memory_workers = 2
            elif available_gb < 4:  # 2-4GB available
                memory_workers = 3
            else:  # More than 4GB available
                memory_workers = cpu_count
                
            # Use the smaller of the two constraints
            worker_count = min(base_workers, memory_workers)
            
            # Ensure at least 1, at most 12 workers
            return max(1, min(12, worker_count))
        except:
            # If anything fails, return a safe default
            return 2
        
    @api.model
    def _monitor_parallel_jobs(self, table_config_id, sync_log_id, job_uuids, worker_count):
        """
        Monitor parallel jobs and update progress.
        This runs as a separate queued job through with_delay().
        Returns when all jobs are completed or requires re-queuing for continued monitoring.
        """
        # Check interval in seconds - this method will be re-queued after this interval
        check_interval = 10
        max_retries = 360  # Up to 1 hour (360 * 10 seconds)
        
        # Ensure we have job UUIDs to monitor
        if not job_uuids:
            return {
                'error': 'No job UUIDs provided for monitoring',
                'status': 'failed'
            }
        
        # Get table config and sync log
        table_config = self.env['etl.source.table'].browse(table_config_id)
        sync_log = self.env['etl.sync.log'].browse(sync_log_id)
        
        if not table_config.exists() or not sync_log.exists():
            return {'error': 'Table config or sync log not found', 'status': 'failed'}
        
        # Check job statuses - use direct SQL for efficiency when monitoring many jobs
        self.env.cr.execute("""
            SELECT 
                state, 
                result,
                COUNT(*) as count
            FROM queue_job
            WHERE uuid IN %s
            GROUP BY state, result
        """, (tuple(job_uuids),))
        
        job_stats = self.env.cr.dictfetchall()
        
        # Calculate job completion status
        completed = sum(r['count'] for r in job_stats if r['state'] == 'done')
        failed = sum(r['count'] for r in job_stats if r['state'] == 'failed')
        total_jobs = len(job_uuids)
        
        # Aggregate stats from completed jobs
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'error_rows': 0}
        
        # Get results from completed jobs
        self.env.cr.execute("""
            SELECT uuid, result
            FROM queue_job
            WHERE uuid IN %s AND state = 'done'
        """, (tuple(job_uuids),))
        
        job_results = self.env.cr.dictfetchall()
        
        # Extract stats from job results
        for job_result in job_results:
            result = job_result['result']
            if result:
                # Try to parse result as JSON if it's a string
                if isinstance(result, str):
                    try:
                        result = json.loads(result)
                    except:
                        continue
                
                # Extract stats
                if isinstance(result, dict):
                    stats['total_rows'] += result.get('total_rows', 0)
                    stats['new_rows'] += result.get('new_rows', 0)
                    stats['updated_rows'] += result.get('updated_rows', 0)
                    stats['error_rows'] += result.get('error_rows', 0)
        
        # Update metadata in sync log
        try:
            metadata = json.loads(sync_log.error_message or '{}')
            # Update worker completion count
            metadata['completed_workers'] = completed
            # Update stats
            metadata['stats'] = stats
            # Save updated metadata
            sync_log.write({'error_message': json.dumps(metadata)})
        except Exception as e:
            _logger.warning(f"Error updating sync log metadata: {str(e)}")
        
        # Calculate progress
        progress = (completed + failed) / total_jobs * 100 if total_jobs > 0 else 0
        
        # Update table status with current progress
        progress_message = (f'Progress: {completed}/{total_jobs} workers complete ({progress:.1f}%) - '
                        f'{stats["total_rows"]} records processed so far')
        
        table_config.write({
            'progress_percentage': min(99, progress),  # Keep under 100% until fully complete
            'last_sync_message': progress_message
        })
        
        # All jobs completed?
        if completed + failed >= total_jobs:
            if failed > 0:
                # Some jobs failed - mark as failed
                _logger.error(f"ETL failed: {failed}/{total_jobs} workers failed")
                
                # Update table config
                table_config.write({
                    'job_status': 'failed',
                    'last_sync_status': 'failed',
                    'last_sync_message': f'ETL failed: {failed}/{total_jobs} workers failed, processed {stats["total_rows"]} records',
                    'progress_percentage': progress
                })
                
                # Update sync log
                sync_log.write({
                    'end_time': fields.Datetime.now(),
                    'status': 'failed',
                    'error_message': f'ETL failed: {failed}/{total_jobs} workers failed',
                    'total_records': stats['total_rows'],
                    'new_records': stats['new_rows'],
                    'updated_records': stats['updated_rows']
                })
            else:
                # All jobs succeeded - mark as complete
                _logger.info(f"ETL completed successfully: {stats['total_rows']} records processed")
                
                # Update table config
                table_config.write({
                    'job_status': 'done',
                    'last_sync_status': 'success',
                    'last_sync_message': f'ETL completed successfully with {stats["total_rows"]} records processed',
                    'progress_percentage': 100,
                    'last_sync_time': fields.Datetime.now(),
                    'total_records_synced': stats['total_rows']
                })
                
                # Update sync log
                sync_log.write({
                    'end_time': fields.Datetime.now(),
                    'status': 'success',
                    'total_records': stats['total_rows'],
                    'new_records': stats['new_rows'],
                    'updated_records': stats['updated_rows']
                })
            
            # Commit to ensure all updates are saved
            self.env.cr.commit()
            
            # Return final status
            return {
                'completed': completed,
                'failed': failed,
                'total': total_jobs,
                'stats': stats,
                'status': 'success' if failed == 0 else 'failed'
            }
        
        # Not all jobs completed - re-queue this monitoring job to check again later
        monitor_job = self.with_delay(
            description=f"ETL Monitor: Job {sync_log_id}",
            channel="etl_monitor",
            priority=8,
            eta=datetime.now() + timedelta(seconds=check_interval)
        )._monitor_parallel_jobs(table_config_id, sync_log_id, job_uuids, worker_count)
        
        # Return current progress
        return {
            'status': 'monitoring',
            'current_progress': {
                'completed': completed,
                'failed': failed,
                'total': total_jobs,
                'progress': progress,
                'stats': stats,
                'next_check': check_interval,
                'monitor_job_uuid': monitor_job.uuid
            }
        }
    
    @api.model
    def _calculate_chunk_size(self, total_rows, worker_count):
        """Calculate optimal chunk size based on table size and worker count"""
        if not total_rows or total_rows <= 0:
            # Default to a reasonable size if we don't know the count
            return 50000
        
        # Calculate partition size - each worker should process approximately total_rows/worker_count
        # This ensures all data is covered by the workers
        worker_partition_size = total_rows // worker_count
        
        # Adjust batch size for database operations based on table size (for memory management)
        if total_rows > 5000000:  # Very large tables (>5M rows)
            batch_size = 50000
        elif total_rows > 1000000:  # Large tables (1-5M rows)
            batch_size = 100000
        elif total_rows > 100000:  # Medium tables (100K-1M rows)
            batch_size = 200000
        else:  # Small tables
            batch_size = min(500000, worker_partition_size)
        
        # Ensure minimum reasonable size
        batch_size = max(10000, min(batch_size, worker_partition_size))
        
        # Important: Log the partition and batch sizes for debugging
        _logger.info(f"Worker partition size: {worker_partition_size} rows, batch size: {batch_size} rows")
        
        return batch_size 
    
    @api.model
    def parallel_worker_id_range(self, table_config_id, sync_log_id, worker_num, 
                            total_workers, min_id, max_id):
        """Worker job that processes a specific ID range of a table."""
        # Get table config
        table_config = self.env['etl.source.table'].browse(table_config_id)
        if not table_config.exists():
            return {'error': 'Table config not found'}
        
        # Use thread-safe locking to avoid contention
        worker_key = f"etl_worker_{table_config_id}_{worker_num}"
        
        # Acquire process lock for this worker
        if not self._acquire_process_lock(worker_key):
            return {'error': f'Could not acquire lock for worker {worker_num}'}
            
        try:
            start_time = time.time()
            
            # Get resources needed for processing
            connector_service = self.env['etl.database.connector.service']
            source_db = table_config.source_db_connection_id
            target_db = table_config.target_db_connection_id
            config = table_config.get_config_json()
            
            # Get primary key info
            source_columns = connector_service.get_columns(source_db, config['source_table'])
            primary_key = config['primary_key'].lower()
            primary_key_original = source_columns.get(primary_key)
            
            # Get target column types for conversion
            target_column_types = {}
            with connector_service.cursor(target_db) as cursor:
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = '{config["target_table"]}'
                """)
                for row in cursor.fetchall():
                    target_column_types[row['column_name'].lower()] = {
                        'type': row['data_type'].lower(),
                        'nullable': row['is_nullable'] == 'YES'
                    }
            
            # Prepare query columns
            etl_manager = self.env['etl.manager']
            query_columns, column_map, primary_key_original = etl_manager._prepare_columns(config, source_columns)
            
            # Build query with ID range
            formatted_columns = ', '.join(query_columns)
            
            # Create a transaction manager
            tx_manager = self.env['etl.transaction']
            
            # Process data in batches within the ID range
            offset = 0
            batch_size = 5000  # Small batch size for better memory management
            stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'error_rows': 0}
            
            # Process until no more data in this ID range
            has_more = True
            
            while has_more:
                # Use transaction context for each batch - WITHOUT SPECIFYING ISOLATION LEVEL
                with tx_manager.transaction_context(
                    name=f"worker_{worker_num}_batch_{offset}",
                    retry_count=3
                ) as tx:
                    # Query for a batch of data in ID range
                    query = f"""
                        SELECT {formatted_columns} 
                        FROM "{config['source_table']}" 
                        WHERE "{primary_key_original}" >= %s AND "{primary_key_original}" < %s
                        ORDER BY "{primary_key_original}"
                        LIMIT {batch_size} OFFSET {offset}
                    """
                    result_rows = connector_service.execute_query(source_db, query, [min_id, max_id])
                    
                    if not result_rows or len(result_rows) == 0:
                        has_more = False
                        break
                    
                    # Process rows in memory
                    rows_to_update = []
                    
                    for row in result_rows:
                        # Get primary key
                        pk_value = row.get(primary_key_original)
                        if pk_value is None:
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
                                            str(source_value)  # Convert to string for lookup
                                        )
                                        transformed_row[mapping['target'].lower()] = lookup_value
                        
                        # Add to rows to update if not empty
                        if transformed_row:
                            # Convert values based on target types
                            for col, val in list(transformed_row.items()):
                                if col in target_column_types:
                                    target_type = target_column_types[col]['type']
                                    nullable = target_column_types[col]['nullable']
                                    transformed_row[col] = self._convert_value_for_target(val, target_type, nullable)
                            
                            rows_to_update.append(transformed_row)
                    
                    # Batch update rows with converted values
                    if rows_to_update:
                        batch_stats = etl_manager._batch_update_rows(connector_service, target_db, config, rows_to_update)
                        # Update stats
                        for key in batch_stats:
                            if key in stats:
                                stats[key] += batch_stats[key]
                    
                    # Update offset for next batch
                    offset += len(result_rows)
                    
                    # Check if batch was smaller than requested
                    if len(result_rows) < batch_size:
                        has_more = False
                
                # Force garbage collection
                gc.collect()
            
            # Record metrics
            execution_time = time.time() - start_time
            self._record_job_metrics(worker_key, execution_time, stats)
            
            # Log completion
            _logger.info(f"Worker {worker_num} completed: {stats['total_rows']} rows processed "
                        f"({stats['new_rows']} new, {stats['updated_rows']} updated) in {execution_time:.2f}s")
            
            return stats
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in worker {worker_num}: {error_message}")
            return {'error': error_message, 'error_rows': 1}
            
        finally:
            # Always release the lock
            self._release_process_lock(worker_key)

    @api.model
    def parallel_worker_offset(self, table_config_id, sync_log_id, worker_num, 
                              total_workers, offset, chunk_size):
        """
        Worker job that processes a specific offset/limit range of a table.
        Each worker gets its own transaction and operates independently.
        """
        # Get table config
        table_config = self.env['etl.source.table'].browse(table_config_id)
        if not table_config.exists():
            return {'error': 'Table config not found'}
        
        # Use thread-safe locking to avoid contention
        worker_key = f"etl_worker_{table_config_id}_{worker_num}"
        
        # Acquire process lock for this worker
        if not self._acquire_process_lock(worker_key):
            return {'error': f'Could not acquire lock for worker {worker_num}'}
            
        try:
            start_time = time.time()
            
            # Get resources needed for processing
            connector_service = self.env['etl.database.connector.service']
            source_db = table_config.source_db_connection_id
            target_db = table_config.target_db_connection_id
            config = table_config.get_config_json()
            
            # Get primary key info
            source_columns = connector_service.get_columns(source_db, config['source_table'])
            primary_key = config['primary_key'].lower()
            primary_key_original = source_columns.get(primary_key)
            
            # Prepare query columns
            etl_manager = self.env['etl.manager']
            query_columns, column_map, primary_key_original = etl_manager._prepare_columns(config, source_columns)
            
            # Create the ETL transaction manager
            tx_manager = self.env['etl.transaction']
            
            # Process data from this worker's offset
            formatted_columns = ', '.join(query_columns)
            stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'error_rows': 0}
            
            # Process in smaller batches for better memory management
            local_offset = 0
            batch_size = 5000
            worker_limit = chunk_size
            
            # Process until we've covered the worker's entire chunk
            while local_offset < worker_limit:
                # Calculate current batch size
                current_batch_size = min(batch_size, worker_limit - local_offset)
                
                # Process a batch in a transaction context
                with tx_manager.transaction_context(
                    name=f"worker_{worker_num}_batch_{local_offset}",
                    retry_count=3,
                    # isolation_level="READ COMMITTED"
                ) as tx:
                    # Query for a batch of data from the offset
                    query = f"""
                        SELECT {formatted_columns} 
                        FROM "{config['source_table']}" 
                        ORDER BY "{primary_key_original}"
                        LIMIT {current_batch_size} OFFSET {offset + local_offset}
                    """
                    result_rows = connector_service.execute_query(source_db, query)
                    
                    if not result_rows or len(result_rows) == 0:
                        break
                    
                    # Process batch with ETL manager
                    batch_stats = etl_manager.process_batch(
                        connector_service, source_db, target_db, 
                        config, result_rows, column_map, primary_key_original
                    )
                    
                    # Update stats
                    for key in batch_stats:
                        stats[key] = stats.get(key, 0) + batch_stats.get(key, 0)
                    
                    # Log batch completion
                    _logger.info(f"Worker {worker_num} processed batch of {len(result_rows)} rows")
                    
                    # Update local offset
                    local_offset += len(result_rows)
                    
                    # Check if batch was smaller than requested
                    if len(result_rows) < current_batch_size:
                        break
                
                # Update worker progress (in a separate transaction)
                self._update_worker_progress(
                    table_config_id, sync_log_id, worker_num, total_workers, stats
                )
                
                # Force garbage collection
                gc.collect()
            
            # Record metrics
            execution_time = time.time() - start_time
            self._record_job_metrics(worker_key, execution_time, stats)
            
            # Log completion
            _logger.info(f"Worker {worker_num} completed: {stats['total_rows']} rows processed "
                        f"({stats['new_rows']} new, {stats['updated_rows']} updated) in {execution_time:.2f}s")
            
            return stats
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in worker {worker_num}: {error_message}")
            return {'error': error_message, 'error_rows': 1}
            
        finally:
            # Always release the lock
            self._release_process_lock(worker_key)
            
    @api.model
    def parallel_worker_enhanced_csv_id_range(self, table_config_id, sync_log_id, worker_num, 
                                       total_workers, min_id, max_id):
        """
        Worker job that processes a specific ID range using enhanced CSV chunking.
        Processes entire ID range in multiple batches.
        """
        # Get table config
        table_config = self.env['etl.source.table'].browse(table_config_id)
        if not table_config.exists():
            return {'error': 'Table config not found'}
        
        # Use thread-safe locking to avoid contention
        worker_key = f"etl_worker_{table_config_id}_{worker_num}"
        
        # Acquire process lock for this worker
        if not self._acquire_process_lock(worker_key):
            return {'error': f'Could not acquire lock for worker {worker_num}'}
            
        try:
            start_time = time.time()
            
            # Get resources needed for processing
            connector_service = self.env['etl.database.connector.service']
            source_db = table_config.source_db_connection_id
            target_db = table_config.target_db_connection_id
            config = table_config.get_config_json()
            
            # Get primary key info
            source_columns = connector_service.get_columns(source_db, config['source_table'])
            primary_key = config['primary_key'].lower()
            primary_key_original = source_columns.get(primary_key)
            
            # Get the ETLFastSyncPostgres model
            fast_sync = self.env['etl.fast.sync.postgres']
            
            # Create metrics dictionary
            metrics = {
                'db_query_time': 0,
                'transform_time': 0,
                'csv_write_time': 0,
                'db_load_time': 0,
                'lookup_time': 0,
                'total_time': 0
            }
            
            # Get row count in this worker's ID range to track progress
            count_query = f"""
                SELECT COUNT(*) AS count 
                FROM "{config['source_table']}" 
                WHERE "{primary_key_original}" >= %s AND "{primary_key_original}" < %s
            """
            count_result = connector_service.execute_query(source_db, count_query, [min_id, max_id])
            range_row_count = count_result[0]['count']
            
            # Log the worker's assigned range
            _logger.info(f"Worker {worker_num}: Processing ID range from {min_id} to {max_id} " +
                        f"({range_row_count} rows)")
            
            # Calculate optimal batch size based on row count
            batch_size = 50000  # Default batch size
            if range_row_count > 500000:
                batch_size = 50000
            elif range_row_count > 100000:
                batch_size = 25000
            else:
                batch_size = 10000
                
            # Initialize combined stats
            combined_stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'error_rows': 0}
            
            # Process data in batches through the entire ID range
            # We'll use offset batching within the ID range
            offset = 0
            
            while offset < range_row_count:
                # Get current batch size
                current_batch_size = min(batch_size, range_row_count - offset)
                
                # Skip if no rows left
                if current_batch_size <= 0:
                    break
                    
                _logger.info(f"Worker {worker_num}: Processing ID range batch at offset {offset} " +
                            f"with size {current_batch_size}")
                    
                # Process this batch with ID range filter
                batch_stats = fast_sync._sync_with_enhanced_chunking_filtered(
                    connector_service, source_db, target_db,
                    config, table_config, primary_key,
                    metrics, filter_type='id_range',
                    min_id=min_id, max_id=max_id,
                    primary_key_original=primary_key_original,
                    chunk_size=current_batch_size, 
                    offset=offset
                )
                
                # Update combined stats
                for key in combined_stats:
                    if key in batch_stats:
                        combined_stats[key] += batch_stats[key]
                
                # Update worker progress
                self._update_worker_progress(
                    table_config_id, sync_log_id, worker_num, total_workers, combined_stats
                )
                
                # Move to next batch
                offset += current_batch_size
                
                # Force garbage collection after each batch
                gc.collect()
            
            # Record metrics
            execution_time = time.time() - start_time
            self._record_job_metrics(worker_key, execution_time, combined_stats)
            
            # Log completion of entire worker range
            _logger.info(f"Enhanced CSV Worker {worker_num} completed entire ID range: {combined_stats['total_rows']} rows processed "
                        f"({combined_stats['new_rows']} new, {combined_stats['updated_rows']} updated) in {execution_time:.2f}s")
            
            # Final update of worker progress
            self._update_worker_progress(
                table_config_id, sync_log_id, worker_num, total_workers, combined_stats
            )
            
            # Commit to ensure progress is saved
            self.env.cr.commit()
            
            return combined_stats
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in enhanced CSV worker {worker_num}: {error_message}")
            return {'error': error_message, 'error_rows': 1}
            
        finally:
            # Always release the lock
            self._release_process_lock(worker_key)

    @api.model
    def parallel_worker_enhanced_csv_offset(self, table_config_id, sync_log_id, worker_num, 
                                    total_workers, offset, chunk_size):
        """
        Worker job that processes a specific offset/limit range using enhanced CSV chunking.
        Now processes entire worker partition, not just the first batch.
        """
        # Get table config
        table_config = self.env['etl.source.table'].browse(table_config_id)
        if not table_config.exists():
            return {'error': 'Table config not found'}
        
        # Use thread-safe locking to avoid contention
        worker_key = f"etl_worker_{table_config_id}_{worker_num}"
        
        # Acquire process lock for this worker
        if not self._acquire_process_lock(worker_key):
            return {'error': f'Could not acquire lock for worker {worker_num}'}
            
        try:
            start_time = time.time()
            
            # Get resources needed for processing
            connector_service = self.env['etl.database.connector.service']
            source_db = table_config.source_db_connection_id
            target_db = table_config.target_db_connection_id
            config = table_config.get_config_json()
            
            # Get primary key info
            primary_key = config['primary_key']
            
            # Get the ETLFastSyncPostgres model
            fast_sync = self.env['etl.fast.sync.postgres']
            
            # Create metrics dictionary
            metrics = {
                'db_query_time': 0,
                'transform_time': 0,
                'csv_write_time': 0,
                'db_load_time': 0,
                'lookup_time': 0,
                'total_time': 0
            }
            
            # Get total table size to calculate worker's portion
            total_rows = connector_service.get_table_count(source_db, config['source_table'])
            worker_partition_size = total_rows // total_workers
            
            # Calculate this worker's range
            worker_start = worker_num * worker_partition_size
            worker_end = (worker_num + 1) * worker_partition_size
            if worker_num == total_workers - 1:  # Last worker takes remainder
                worker_end = total_rows
            
            # Log the worker's assigned range
            _logger.info(f"Worker {worker_num}: Processing rows {worker_start} to {worker_end} " +
                        f"({worker_end - worker_start} rows, {worker_partition_size} partition size)")
            
            # Initialize combined stats
            combined_stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'error_rows': 0}
            
            # Process data in batches through the entire worker's portion
            current_offset = worker_start
            
            while current_offset < worker_end:
                # Calculate remaining rows in this worker's partition
                remaining = worker_end - current_offset
                # Use smaller of chunk_size or remaining rows
                current_batch_size = min(chunk_size, remaining)
                
                # Skip if no rows left
                if current_batch_size <= 0:
                    break
                    
                _logger.info(f"Worker {worker_num}: Processing batch at offset {current_offset} " +
                            f"with size {current_batch_size}")
                    
                # Process this batch with offset filter
                batch_stats = fast_sync._sync_with_enhanced_chunking_filtered(
                    connector_service, source_db, target_db,
                    config, table_config, primary_key,
                    metrics, filter_type='offset',
                    offset=current_offset, chunk_size=current_batch_size
                )
                
                # Update combined stats
                for key in combined_stats:
                    if key in batch_stats:
                        combined_stats[key] += batch_stats[key]
                
                # Update worker progress
                self._update_worker_progress(
                    table_config_id, sync_log_id, worker_num, total_workers, combined_stats
                )
                
                # Move to next batch
                current_offset += current_batch_size
                
                # Force garbage collection after each batch
                gc.collect()
            
            # Record metrics
            execution_time = time.time() - start_time
            self._record_job_metrics(worker_key, execution_time, combined_stats)
            
            # Log completion of entire worker partition
            _logger.info(f"Enhanced CSV Worker {worker_num} completed entire partition: {combined_stats['total_rows']} rows processed "
                        f"({combined_stats['new_rows']} new, {combined_stats['updated_rows']} updated) in {execution_time:.2f}s")
            
            # Final update of worker progress
            self._update_worker_progress(
                table_config_id, sync_log_id, worker_num, total_workers, combined_stats
            )
            
            # Commit to ensure progress is saved
            self.env.cr.commit()
            
            return combined_stats
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in enhanced CSV worker {worker_num}: {error_message}")
            return {'error': error_message, 'error_rows': 1}
            
        finally:
            # Always release the lock
            self._release_process_lock(worker_key)
    
    @api.model
    def _acquire_process_lock(self, key, timeout=5):
        """Acquire a thread-safe process lock with timeout"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            with self._locks_mutex:
                if key not in self._process_locks:
                    # Create new lock
                    self._process_locks[key] = threading.RLock()
                
                # Try to acquire the lock
                if self._process_locks[key].acquire(blocking=False):
                    return True
                
            # Wait a bit before trying again
            time.sleep(0.1)
        
        return False
    
    @api.model
    def _release_process_lock(self, key):
        """Release a thread-safe process lock"""
        with self._locks_mutex:
            if key in self._process_locks:
                try:
                    self._process_locks[key].release()
                    return True
                except:
                    # Lock might not be acquired
                    return False
        return False
    
    @api.model
    def _record_job_metrics(self, job_key, execution_time, stats):
        """Record job metrics for analysis"""
        with self._metrics_mutex:
            self._job_metrics[job_key] = {
                'execution_time': execution_time,
                'timestamp': time.time(),
                'stats': stats
            }
    
    @api.model
    def _update_worker_progress(self, table_config_id, sync_log_id, worker_num, total_workers, stats):
        """
        Update worker progress in a separate transaction with improved locking
        and error handling to avoid contention.
        """
        try:
            # Create new cursor to avoid interfering with the worker's transaction
            with self.env.registry.cursor() as new_cr:
                env = api.Environment(new_cr, self.env.uid, self.env.context)
                
                # Acquire advisory lock using worker ID for increased uniqueness
                lock_id = abs(hash(f"etl_worker_progress_{table_config_id}_{worker_num}")) % 2147483647
                new_cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (lock_id,))
                lock_acquired = new_cr.fetchone()[0]
                
                if not lock_acquired:
                    _logger.info(f"Worker {worker_num}: Skipping progress update - couldn't acquire lock")
                    return False
                
                # Get sync log
                sync_log = env['etl.sync.log'].browse(sync_log_id)
                if not sync_log.exists():
                    return False
                
                # Try to get worker progress from metadata
                try:
                    metadata = json.loads(sync_log.error_message or '{}')
                    worker_results = metadata.get('worker_results', {})
                except:
                    worker_results = {}
                
                # Update progress for this worker
                worker_results[str(worker_num)] = {
                    'stats': stats,
                    'timestamp': time.time(),
                    'status': 'completed'
                }
                
                # Count completed workers
                completed_workers = len(worker_results)
                
                # Update metadata
                try:
                    if isinstance(metadata, dict):
                        metadata['worker_results'] = worker_results
                        metadata['completed_workers'] = completed_workers
                    else:
                        metadata = {
                            'worker_results': worker_results,
                            'completed_workers': completed_workers
                        }
                    
                    # Sum up stats from all workers
                    total_stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'error_rows': 0}
                    for worker_data in worker_results.values():
                        worker_stats = worker_data.get('stats', {})
                        for key in total_stats:
                            total_stats[key] += worker_stats.get(key, 0)
                    
                    metadata['stats'] = total_stats
                    
                    # Update sync log
                    sync_log.write({
                        'error_message': json.dumps(metadata)
                    })
                except Exception as e:
                    _logger.warning(f"Worker {worker_num}: Error updating metadata: {str(e)}")
                
                # Calculate overall progress
                overall_progress = (completed_workers / total_workers) * 100
                
                # Sum up stats from all workers again for direct use
                total_stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'error_rows': 0}
                for worker_data in worker_results.values():
                    worker_stats = worker_data.get('stats', {})
                    for key in total_stats:
                        total_stats[key] += worker_stats.get(key, 0)
                
                # Update table config with overall progress
                table_config = env['etl.source.table'].browse(table_config_id)
                if table_config.exists():
                    table_config.write({
                        'progress_percentage': min(99, overall_progress),  # Keep under 100% until all done
                        'last_sync_message': (f'Progress: {completed_workers}/{total_workers} workers '
                                            f'({overall_progress:.1f}%) - {total_stats["total_rows"]} rows processed')
                    })
                
                # Commit the transaction
                new_cr.commit()
                
                return True
                    
        except Exception as e:
            _logger.warning(f"Worker {worker_num}: Error updating worker progress: {str(e)}")
            return False
        
        
    @api.model
    def parallel_worker_enhanced_csv_chunked(self, table_config_id, sync_log_id, worker_num, 
                                    total_workers, offset, worker_partition_size):
        """
        Worker job that processes a specific offset range with improved chunk management.
        Processes smaller chunks of data to avoid memory issues and implements checkpointing.
        """
        # Get table config
        table_config = self.env['etl.source.table'].browse(table_config_id)
        if not table_config.exists():
            return {'error': 'Table config not found'}
        
        # Use thread-safe locking to avoid contention
        worker_key = f"etl_worker_{table_config_id}_{worker_num}"
        
        # Acquire process lock for this worker
        if not self._acquire_process_lock(worker_key):
            return {'error': f'Could not acquire lock for worker {worker_num}'}
            
        try:
            start_time = time.time()
            
            # Get resources needed for processing
            connector_service = self.env['etl.database.connector.service']
            source_db = table_config.source_db_connection_id
            target_db = table_config.target_db_connection_id
            config = table_config.get_config_json()
            
            # Get primary key info
            primary_key = config['primary_key']
            
            # Get the ETLFastSyncPostgres model
            fast_sync = self.env['etl.fast.sync.postgres']
            
            # Create metrics dictionary
            metrics = {
                'db_query_time': 0,
                'transform_time': 0,
                'csv_write_time': 0,
                'db_load_time': 0,
                'lookup_time': 0,
                'total_time': 0
            }
            
            # Calculate worker's range boundaries
            worker_start = offset
            worker_end = offset + worker_partition_size
            
            # Log the worker's assigned range
            _logger.info(f"Worker {worker_num}: Processing rows {worker_start} to {worker_end} "
                        f"({worker_partition_size} rows)")
            
            # Initialize combined stats
            combined_stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'error_rows': 0}
            
            # Use MUCH smaller chunk size to avoid memory issues
            chunk_size = self._determine_optimal_chunk_size_for_worker(worker_partition_size)
            
            # Get or create checkpoint info for this worker
            checkpoint = self._get_worker_checkpoint(table_config_id, worker_num)
            start_offset = checkpoint.get('last_processed_offset', worker_start)
            
            _logger.info(f"Worker {worker_num}: Starting from checkpoint offset {start_offset}")
            
            # Process data in small chunks
            current_offset = start_offset
            
            while current_offset < worker_end:
                # Calculate remaining rows in this worker's partition
                remaining = worker_end - current_offset
                # Use smaller of chunk_size or remaining rows
                current_batch_size = min(chunk_size, remaining)
                
                # Skip if no rows left
                if current_batch_size <= 0:
                    break
                    
                _logger.info(f"Worker {worker_num}: Processing batch at offset {current_offset} "
                            f"with size {current_batch_size}")
                    
                # Monitor memory before processing
                self._check_memory_usage(f"Worker {worker_num} before chunk")
                    
                # Process this batch with offset filter
                batch_stats = fast_sync._sync_with_enhanced_chunking_filtered(
                    connector_service, source_db, target_db,
                    config, table_config, primary_key,
                    metrics, filter_type='offset',
                    offset=current_offset, chunk_size=current_batch_size
                )
                
                # Update combined stats
                for key in combined_stats:
                    if key in batch_stats:
                        combined_stats[key] += batch_stats[key]
                
                # Update worker progress and save checkpoint
                self._update_worker_progress(
                    table_config_id, sync_log_id, worker_num, total_workers, combined_stats
                )
                self._save_worker_checkpoint(table_config_id, worker_num, {
                    'last_processed_offset': current_offset + current_batch_size,
                    'stats': combined_stats
                })
                
                # Move to next batch
                current_offset += current_batch_size
                
                # Check memory after processing
                self._check_memory_usage(f"Worker {worker_num} after chunk")
                
                # Explicit garbage collection after each batch
                gc.collect()
                
                # Add a small sleep to reduce CPU contention between workers
                time.sleep(0.1)
            
            # Record metrics
            execution_time = time.time() - start_time
            self._record_job_metrics(worker_key, execution_time, combined_stats)
            
            # Log completion of entire worker partition
            _logger.info(f"Enhanced CSV Worker {worker_num} completed entire partition: "
                    f"{combined_stats['total_rows']} rows processed "
                    f"({combined_stats['new_rows']} new, {combined_stats['updated_rows']} updated) "
                    f"in {execution_time:.2f}s")
            
            # Final update of worker progress
            self._update_worker_progress(
                table_config_id, sync_log_id, worker_num, total_workers, combined_stats
            )
            
            # Clear checkpoint after successful completion
            self._clear_worker_checkpoint(table_config_id, worker_num)
            
            # Commit to ensure progress is saved
            self.env.cr.commit()
            
            return combined_stats
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in enhanced CSV worker {worker_num}: {error_message}")
            # Don't lose the error details
            return {'error': error_message, 'error_message': traceback.format_exc(), 'error_rows': 1}
            
        finally:
            # Always release the lock
            self._release_process_lock(worker_key)
            
    @api.model
    def _determine_optimal_chunk_size_for_worker(self, worker_partition_size):
        """Determine optimal chunk size based on partition size and available memory"""
        # Always use much smaller batches than before
        if worker_partition_size > 500000:
            return 10000  # Very small batches for huge partitions
        elif worker_partition_size > 100000:
            return 5000   # Smaller batches for large partitions
        else:
            return 2000   # Small batches for regular partitions
            
    @api.model
    def _check_memory_usage(self, label="Memory check"):
        """Check and log current memory usage"""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            memory_percent = process.memory_percent()
            
            # Log only if memory usage is high
            if memory_percent > 60:
                _logger.warning(f"{label}: Memory usage {memory_mb:.2f}MB ({memory_percent:.1f}%)")
            elif memory_percent > 30:
                _logger.info(f"{label}: Memory usage {memory_mb:.2f}MB ({memory_percent:.1f}%)")
        except Exception as e:
            _logger.debug(f"Could not check memory: {str(e)}")
            
    @api.model
    def _get_worker_checkpoint(self, table_config_id, worker_num):
        """Get checkpoint information for a worker"""
        try:
            checkpoint_key = f"etl.checkpoint.{table_config_id}.worker.{worker_num}"
            checkpoint_data = self.env['ir.config_parameter'].sudo().get_param(checkpoint_key, '{}')
            return json.loads(checkpoint_data)
        except Exception as e:
            _logger.warning(f"Error getting checkpoint: {str(e)}")
            return {}
            
    @api.model
    def _save_worker_checkpoint(self, table_config_id, worker_num, checkpoint_data):
        """Save checkpoint information for a worker"""
        try:
            checkpoint_key = f"etl.checkpoint.{table_config_id}.worker.{worker_num}"
            self.env['ir.config_parameter'].sudo().set_param(
                checkpoint_key, json.dumps(checkpoint_data)
            )
            return True
        except Exception as e:
            _logger.warning(f"Error saving checkpoint: {str(e)}")
            return False
            
    @api.model
    def _clear_worker_checkpoint(self, table_config_id, worker_num):
        """Clear checkpoint information for a worker after successful completion"""
        try:
            checkpoint_key = f"etl.checkpoint.{table_config_id}.worker.{worker_num}"
            self.env['ir.config_parameter'].sudo().set_param(checkpoint_key, '{}')
            return True
        except Exception as e:
            _logger.warning(f"Error clearing checkpoint: {str(e)}")
            return False

    @api.model
    def parallel_coordinator(self, table_config_id, worker_count):
        """
        Coordinator job that creates and monitors worker jobs.
        Uses enhanced CSV chunking method with improved resource management.
        """
        # Get table config
        table_config = self.env['etl.source.table'].browse(table_config_id)
        if not table_config.exists():
            return {'error': 'Table config not found'}
        
        # Check system resources and maybe reduce worker count
        adjusted_worker_count = self._adjust_worker_count_for_resources(worker_count)
        if adjusted_worker_count != worker_count:
            _logger.info(f"Adjusted worker count from {worker_count} to {adjusted_worker_count} based on system resources")
            worker_count = adjusted_worker_count
        
        # Mark job as started
        table_config.write({
            'job_status': 'started',
            'last_sync_message': f'Preparing enhanced CSV parallel sync with {worker_count} workers'
        })
        
        # Get total row count for the table
        try:
            connector_service = self.env['etl.database.connector.service']
            source_db = table_config.source_db_connection_id
            total_rows = connector_service.get_table_count(source_db, table_config.name)
        except Exception as e:
            _logger.warning(f"Could not determine row count: {str(e)}")
            total_rows = 0
        
        # Create sync log to track the overall operation
        sync_log = self.env['etl.sync.log'].create({
            'table_id': table_config.id,
            'start_time': fields.Datetime.now(),
            'status': 'running'
        })
        
        # Get config object needed for workers
        config = table_config.get_config_json()
        
        # Calculate partition sizes for optimal distribution
        # Use smaller partitions for better resource management
        total_partitions = worker_count * 4  # Create more partitions than workers
        partition_size = total_rows // total_partitions if total_rows > 0 else 0
        
        # Adjust partition size to be reasonable
        if partition_size > 100000:
            partition_size = 100000
        
        # Calculate actual number of partitions based on adjusted size
        actual_partitions = (total_rows + partition_size - 1) // partition_size if total_rows > 0 and partition_size > 0 else 1
        
        _logger.info(f"Using chunked approach with {actual_partitions} partitions of size {partition_size}")
        
        # Determine job distribution strategy
        job_uuids = []
        
        # Create workers based on offset ranges with enhanced CSV method
        # Each worker processes multiple smaller partitions
        partitions_per_worker = (actual_partitions + worker_count - 1) // worker_count
        
        for i in range(worker_count):
            # Calculate start partition for this worker
            start_partition = i * partitions_per_worker
            if start_partition >= actual_partitions:
                # No more partitions to process
                break
                
            # Calculate end partition (exclusive)
            end_partition = min((i + 1) * partitions_per_worker, actual_partitions)
            
            # Calculate actual offset and range
            offset = start_partition * partition_size
            worker_range = (end_partition - start_partition) * partition_size
            
            # Ensure last worker gets all remaining rows
            if i == worker_count - 1:
                worker_range = total_rows - offset
            
            # Create worker job using enhanced CSV with chunked processing
            job = self.with_delay(
                description=f"Enhanced CSV Worker {i+1}/{worker_count}: {table_config.name}",
                channel="etl_worker",
                priority=10,
                identity_key=f"etl_worker_{table_config.id}_{sync_log.id}_{i}"
            ).parallel_worker_enhanced_csv_chunked(
                table_config.id, 
                sync_log.id, 
                i, 
                worker_count, 
                offset, 
                worker_range
            )
            
            job_uuids.append(job.uuid)
            
            # Add a small delay between worker creation to stagger the start times
            time.sleep(0.5)
        
        # Store metadata about the chunking strategy
        metadata = {
            'strategy': 'enhanced_csv_chunked',
            'total_rows': total_rows,
            'partition_size': partition_size, 
            'total_partitions': actual_partitions,
            'job_uuids': job_uuids,
            'worker_count': worker_count,
            'partitions_per_worker': partitions_per_worker,
            'completed_workers': 0,
            'worker_results': {}
        }
        
        # Store in sync log
        sync_log.write({
            'error_message': json.dumps(metadata)  # Reuse this field for metadata
        })
        
        # Start monitoring with delay
        monitor_job = self.with_delay(
            description=f"ETL Monitor: {table_config.name}",
            channel="etl_monitor",
            priority=8,
            eta=datetime.now() + timedelta(seconds=15)  # Start monitoring after 15 seconds
        )._monitor_parallel_jobs(table_config.id, sync_log.id, job_uuids, worker_count)
        
        return {
            'status': 'started',
            'job_uuids': job_uuids,
            'monitor_job_uuid': monitor_job.uuid
        }

    @api.model
    def _adjust_worker_count_for_resources(self, requested_workers):
        """Dynamically adjust worker count based on current system resources"""
        try:
            import psutil
            
            # Get CPU info
            cpu_count = os.cpu_count() or 4
            cpu_percent = psutil.cpu_percent(interval=0.1)
            
            # Get memory info
            mem = psutil.virtual_memory()
            available_gb = mem.available / (1024 * 1024 * 1024)
            
            # Start with requested workers
            adjusted_workers = requested_workers
            
            # Adjust based on CPU usage
            if cpu_percent > 80:
                adjusted_workers = min(adjusted_workers, max(1, cpu_count // 4))
            elif cpu_percent > 60:
                adjusted_workers = min(adjusted_workers, max(2, cpu_count // 2))
            
            # Adjust based on available memory
            if available_gb < 1:  # Less than 1GB available
                adjusted_workers = min(adjusted_workers, 1)  # Severe restriction
            elif available_gb < 2:  # Less than 2GB available
                adjusted_workers = min(adjusted_workers, 2)
            elif available_gb < 4:  # Less than 4GB available
                adjusted_workers = min(adjusted_workers, 4)
            
            # Ensure at least 1 worker
            return max(1, adjusted_workers)
        except Exception as e:
            _logger.warning(f"Error adjusting worker count: {str(e)}")
            # Conservative fallback
            return min(4, requested_workers)
