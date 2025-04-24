# -*- coding: utf-8 -*-
import csv
from logging import config
import random
import traceback
import psycopg2
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import logging
import time
import gc
from datetime import date, datetime, timedelta
from decimal import Decimal
import json
import os

_logger = logging.getLogger(__name__)

class ETLFastSyncPostgres(models.AbstractModel):
    _name = 'etl.fast.sync.postgres'
    _inherit = 'etl.fast.sync.generic'
    _description = 'Fast PostgreSQL to PostgreSQL ETL'
    
    @api.model
    def _get_lookup_cache(self):
        """Get or create the lookup cache for this model with size limit"""
        if not hasattr(type(self), '_lookup_cache'):
            type(self)._lookup_cache = {}
            type(self)._cache_stats = {'hits': 0, 'misses': 0}
            type(self)._cache_max_size = 100000  # Configurable limit
        
        # Implement cache size management
        if len(type(self)._lookup_cache) > type(self)._cache_max_size:
            # Use LRU strategy or simple clearing
            _logger.info("Cache size limit reached, clearing 50% of entries")
            keys_to_remove = list(type(self)._lookup_cache.keys())[:len(type(self)._lookup_cache)//2]
            for key in keys_to_remove:
                del type(self)._lookup_cache[key]
        
        return type(self)._lookup_cache

    @api.model
    def _lookup_value(self, connector_service, target_db, table, key_col, value_col, key_value):
        """Optimized lookup with caching and metrics"""
        if not key_value:
            return None
            
        # Use class-level cache
        cache = self._get_lookup_cache()
        cache_key = f"{table}:{key_col}:{key_value}"
        
        if cache_key in cache:
            # Update hit stat
            if hasattr(type(self), '_cache_stats'):
                type(self)._cache_stats['hits'] += 1
            return cache[cache_key]
        
        try:
            query = f'SELECT "{value_col}" FROM "{table}" WHERE "{key_col}" = %s LIMIT 1'
            result = connector_service.execute_query(target_db, query, [key_value])
            
            if result and len(result) > 0:
                value = result[0][value_col]
                # Cache the result
                cache[cache_key] = value
                # Update miss stat
                if hasattr(type(self), '_cache_stats'):
                    type(self)._cache_stats['misses'] += 1
                return value
                
            # Cache null result too
            cache[cache_key] = None
            # Update miss stat
            if hasattr(type(self), '_cache_stats'):
                type(self)._cache_stats['misses'] += 1
            return None
        except Exception as e:
            _logger.warning(f"Lookup error: {str(e)}")
            return None
    
    @api.model
    def _convert_field_value(self, value, target_type, nullable=True):
        """Convert a value to the target column type with proper error handling"""
        if value is None:
            return None
            
        try:
            # Handle large integer types - very important for customer IDs!
            if 'int' in target_type:
                # For large integer values, keep them as strings to avoid range issues
                if isinstance(value, str) and value.strip():
                    # Check if the value is too large for a standard int
                    try:
                        if len(value.strip()) > 10 or int(value.strip()) > 2147483647:
                            # Just return the string value for large numbers
                            return value.strip()
                        return int(float(value.strip()))
                    except (ValueError, OverflowError):
                        # If conversion fails, keep as string
                        return value.strip()
                elif isinstance(value, float):
                    if value > 2147483647:
                        # Convert to string for large values
                        return str(int(value))
                    return int(value)
                elif isinstance(value, int):
                    if value > 2147483647:
                        # Convert to string for large values
                        return str(value)
                    return value
                return None
                
            # Handle numeric types
            elif any(t in target_type for t in ('numeric', 'float', 'double', 'decimal')):
                if isinstance(value, str) and value.strip():
                    return float(value.strip())
                elif isinstance(value, (int, float, Decimal)):
                    return float(value) if isinstance(value, Decimal) else value
                return None
                
            # Handle boolean values
            elif 'bool' in target_type:
                if isinstance(value, str):
                    lower_val = value.lower().strip()
                    if lower_val in ('true', 't', 'yes', 'y', '1'):
                        return True
                    elif lower_val in ('false', 'f', 'no', 'n', '0'):
                        return False
                    return None
                return bool(value)
                
            # Handle date values
            elif 'date' in target_type and 'timestamp' not in target_type:
                if isinstance(value, datetime):
                    return value.date().isoformat()
                elif isinstance(value, date):
                    return value.isoformat()
                    
            # Handle timestamp values
            elif 'timestamp' in target_type:
                if isinstance(value, datetime):
                    return value.isoformat()
                    
            # For all other types, return as is
            return value
            
        except Exception as e:
            _logger.warning(f"Type conversion error for {target_type}: {str(e)}")
            # For conversion errors, it's safer to return the original value
            # rather than None, to prevent data loss
            return value if not nullable else None

    @api.model
    def sync_data(self, table_config):
        """
        Highly optimized PostgreSQL to PostgreSQL data sync with automatic parallel processing
        and improved recovery capabilities.
        """
        start_time = time.time()
        
        # Get connector service
        connector_service = self.env['etl.database.connector.service']
        source_db = table_config.source_db_connection_id
        target_db = table_config.target_db_connection_id
        
        # Get configuration
        config = table_config.get_config_json()
        source_table = config['source_table']
        target_table = config['target_table']
        primary_key = config['primary_key']
        
        _logger.info(f"Starting sync for table {source_table}")
        
        # Check for crash recovery
        if self._check_for_recovery(table_config):
            _logger.info(f"Recovery mode detected for {source_table}, resuming previous job")
            return self._resume_crashed_job(table_config)
        
        # Get table size to determine best approach
        try:
            table_size = connector_service.get_table_count(source_db, config['source_table'])
            _logger.info(f"Table {source_table} has {table_size} rows")
        except Exception as e:
            _logger.warning(f"Could not determine table size: {str(e)}")
            table_size = 0
        
        # Check system resource usage before deciding on parallel processing
        system_resources = self._check_system_resources()
        
        # Determine if we should use parallel processing based on table size AND system resources
        should_use_parallel = (self._should_use_parallel_processing(table_size) and 
                            system_resources.get('should_use_parallel', True))
        
        # Log resource and processing decision
        _logger.info(f"System resources: CPU={system_resources.get('cpu_percent', 'unknown')}%, "
                f"Memory={system_resources.get('memory_available_gb', 'unknown')}GB")
        _logger.info(f"Processing decision: {'Parallel' if should_use_parallel else 'Standard'} processing")
        
        if should_use_parallel:
            # Adjust worker count based on system resources
            worker_count = self._determine_worker_count_for_resources(table_size, system_resources)
            _logger.info(f"Using parallel processing for table {source_table} with {table_size} rows "
                    f"and {worker_count} workers")
            
            # Save recovery information
            self._save_job_state(table_config, {
                'mode': 'parallel',
                'table_size': table_size,
                'worker_count': worker_count,
                'start_time': time.time()
            })
            
            return self._sync_data_parallel(table_config, table_size, worker_count)
        else:
            _logger.info(f"Using standard processing for table {source_table}")
            
            # Save recovery information
            self._save_job_state(table_config, {
                'mode': 'standard',
                'table_size': table_size,
                'start_time': time.time()
            })
            
            return self._sync_data_standard(table_config, table_size)

    @api.model
    def _check_for_recovery(self, table_config):
        """Check if we need to recover from a crashed job"""
        try:
            # Get job state
            job_state = self._get_job_state(table_config)
            
            # If there's no job state, no recovery needed
            if not job_state:
                return False
                
            # Check if there's an ongoing job
            if job_state.get('mode') and job_state.get('start_time'):
                # Check if the job is old enough to be considered crashed
                elapsed_time = time.time() - job_state.get('start_time', 0)
                if elapsed_time > 300:  # More than 5 minutes old
                    # Check if there are active queue jobs for this table
                    has_active_jobs = self._check_active_jobs(table_config)
                    
                    # If no active jobs, and state is old, we need to recover
                    return not has_active_jobs
        except Exception as e:
            _logger.warning(f"Error checking for recovery: {str(e)}")
            
        return False

    @api.model
    def _check_active_jobs(self, table_config):
        """Check if there are active queue jobs for this table"""
        try:
            # Find any active jobs for this table
            self.env.cr.execute("""
                SELECT COUNT(*) FROM queue_job
                WHERE state IN ('pending', 'started', 'enqueued')
                AND name LIKE %s
            """, (f"%{table_config.name}%",))
            
            active_jobs = self.env.cr.fetchone()[0]
            return active_jobs > 0
        except Exception as e:
            _logger.warning(f"Error checking active jobs: {str(e)}")
            return False

    @api.model
    def _resume_crashed_job(self, table_config):
        """Resume a crashed job from its saved state"""
        try:
            # Get job state
            job_state = self._get_job_state(table_config)
            
            if not job_state:
                # No state to resume, start new job
                return self.sync_data(table_config)
                
            _logger.info(f"Resuming crashed job for {table_config.name} in {job_state.get('mode')} mode")
            
            # Update job status
            table_config.write({
                'job_status': 'resuming',
                'last_sync_status': 'running',
                'last_sync_message': f'Resuming crashed job in {job_state.get("mode")} mode'
            })
            
            # Resume based on mode
            if job_state.get('mode') == 'parallel':
                # Restart the parallel job with the same worker count
                worker_count = job_state.get('worker_count', 4)
                return self._sync_data_parallel(table_config, job_state.get('table_size', 0), worker_count)
            else:
                # Restart the standard job
                return self._sync_data_standard(table_config, job_state.get('table_size', 0))
                
        except Exception as e:
            _logger.error(f"Error resuming crashed job: {str(e)}")
            # If recovery fails, start a new job with standard processing
            return self._sync_data_standard(table_config, 0)

    @api.model
    def _save_job_state(self, table_config, state):
        """Save job state with improved error handling"""
        try:
            state_key = f"etl.job_state.{table_config.id}"
            state_json = json.dumps(state)
            
            # Use direct SQL with a new cursor to avoid transaction issues
            with self.env.registry.cursor() as cr:
                try:
                    # Don't use any fancy transaction settings or locks for now
                    # Just do a simple update or insert
                    
                    # Try to update existing record
                    cr.execute("""
                        UPDATE ir_config_parameter
                        SET value = %s,
                            write_date = NOW(),
                            write_uid = %s
                        WHERE key = %s
                    """, (state_json, self.env.uid, state_key))
                    
                    # If no record was updated, insert a new one
                    if cr.rowcount == 0:
                        cr.execute("""
                            INSERT INTO ir_config_parameter (key, value, create_date, write_date, create_uid, write_uid)
                            VALUES (%s, %s, NOW(), NOW(), %s, %s)
                        """, (state_key, state_json, self.env.uid, self.env.uid))
                    
                    # Commit the changes
                    cr.commit()
                    return True
                    
                except Exception as e:
                    _logger.warning(f"Error saving job state, trying fallback method: {str(e)}")
                    # Try to use the standard ORM method as fallback
                    try:
                        self.env['ir.config_parameter'].sudo().set_param(state_key, state_json)
                        return True
                    except Exception as e2:
                        _logger.error(f"All attempts to save job state failed: {str(e2)}")
                        return False
        except Exception as e:
            _logger.error(f"Error preparing job state: {str(e)}")
            return False

    # def _save_job_state(self, table_config, state):
    #     """Save job state for recovery purposes"""
    #     try:
    #         state_key = f"etl.job_state.{table_config.id}"
    #         self.env['ir.config_parameter'].sudo().set_param(
    #             state_key, json.dumps(state)
    #         )
    #         return True
    #     except Exception as e:
    #         _logger.warning(f"Error saving job state: {str(e)}")
    #         return False
    
    def _clean_job_state(self, table_config_id):
        """Clean up all job states for a table to avoid issues on next run"""
        with self.env.registry.cursor() as cr:
            # Clean up main job state
            state_key = f"etl.job_state.{table_config_id}"
            cr.execute("DELETE FROM ir_config_parameter WHERE key = %s", (state_key,))
            
            # Clean up checkpoints
            checkpoint_keys = [
                f"etl.checkpoint.{table_config_id}%",    # Worker checkpoints
                f"etl.processed_chunks.{table_config_id}" # Processed chunks
            ]
            
            for key_pattern in checkpoint_keys:
                cr.execute("DELETE FROM ir_config_parameter WHERE key LIKE %s", (key_pattern,))
            
            # Commit changes
            cr.commit()

    @api.model
    def _get_job_state(self, table_config):
        """Get saved job state for recovery"""
        try:
            state_key = f"etl.job_state.{table_config.id}"
            state_json = self.env['ir.config_parameter'].sudo().get_param(state_key, '{}')
            return json.loads(state_json)
        except Exception as e:
            _logger.warning(f"Error getting job state: {str(e)}")
            return {}

    # @api.model
    # def _clear_job_state(self, table_config):
    #     """Clear job state after successful completion"""
    #     try:
    #         state_key = f"etl.job_state.{table_config.id}"
    #         self.env['ir.config_parameter'].sudo().set_param(state_key, '{}')
    #         return True
    #     except Exception as e:
    #         _logger.warning(f"Error clearing job state: {str(e)}")
    #         return False
    
    @api.model
    def _clear_job_state(self, table_config):
        """
        Enhanced method to properly clean up all job state and related parameters.
        
        Args:
            table_config: Table configuration record
            
        Returns:
            bool: Success status
        """
        try:
            # Use a separate cursor to avoid transaction conflicts
            with self.env.registry.cursor() as cr:
                # Get all related key patterns
                key_patterns = [
                    f"etl.job_state.{table_config.id}",           # Main job state
                    f"etl.checkpoint.{table_config.id}%",         # Worker checkpoints
                    f"etl.processed_chunks.{table_config.id}",    # Processed chunks
                    f"etl.sync.config.{table_config.id}"          # Sync configuration
                ]
                
                # Delete each pattern
                for pattern in key_patterns:
                    if "%" in pattern:
                        cr.execute("DELETE FROM ir_config_parameter WHERE key LIKE %s", (pattern,))
                    else:
                        cr.execute("DELETE FROM ir_config_parameter WHERE key = %s", (pattern,))
                
                # Commit changes
                cr.commit()
                
                _logger.info(f"Successfully cleared job state for table {table_config.name} (ID: {table_config.id})")
                return True
                
        except Exception as e:
            _logger.error(f"Error clearing job state: {str(e)}")
            return False

    @api.model
    def _check_system_resources(self):
        """Check current system resources to guide processing decisions"""
        result = {
            'should_use_parallel': True,  # Default to true unless resources are low
            'cpu_percent': 0,
            'memory_available_gb': 0,
            'memory_percent': 0
        }
        
        try:
            import psutil
            
            # Get CPU info
            cpu_percent = psutil.cpu_percent(interval=0.1)
            result['cpu_percent'] = cpu_percent
            
            # Get memory info
            mem = psutil.virtual_memory()
            available_gb = mem.available / (1024 * 1024 * 1024)
            result['memory_available_gb'] = available_gb
            result['memory_percent'] = mem.percent
            
            # Make decision based on resources
            if cpu_percent > 85 or mem.percent > 85:
                # Very high resource usage - avoid parallel
                result['should_use_parallel'] = False
            elif cpu_percent > 70 and mem.percent > 70:
                # High resource usage - be cautious
                result['should_use_parallel'] = available_gb > 2  # Only parallel if >2GB free
            
        except Exception as e:
            _logger.warning(f"Error checking system resources: {str(e)}")
            # Default to more conservative (use parallel only if likely to have resources)
            result['should_use_parallel'] = True
            
        return result

    @api.model
    def _determine_worker_count_for_resources(self, table_size, system_resources):
        """Determine optimal worker count based on table size and current system resources"""
        # Start with base count from table size
        base_count = self._determine_worker_count(table_size)
        
        # Adjust based on current system resources
        cpu_percent = system_resources.get('cpu_percent', 50)
        available_gb = system_resources.get('memory_available_gb', 4)
        
        # Reduce worker count when resources are constrained
        if cpu_percent > 70 or available_gb < 2:
            return max(1, base_count // 2)  # Cut in half, minimum 1
        elif cpu_percent > 50 or available_gb < 4:
            return max(2, base_count - 2)   # Reduce by 2, minimum 2
        
        # Otherwise use the base count
        return base_count

    def _should_use_parallel_processing(self, table_size):
        """Determine if parallel processing should be used based on table size and system resources"""
        # Only use parallel for larger tables
        if table_size < 100000:  # Less than 100K rows
            return False
            
        # Check system resources
        try:
            import psutil
            
            # Get CPU count and usage
            cpu_count = os.cpu_count() or 4
            cpu_percent = psutil.cpu_percent(interval=0.1)
            
            # Get memory info
            mem = psutil.virtual_memory()
            available_gb = mem.available / (1024 * 1024 * 1024)
            
            # Only use parallel if we have enough CPUs and memory
            if cpu_count >= 4 and available_gb >= 2.0 and cpu_percent < 80:
                return True
                
        except ImportError:
            # psutil not available, make decision based just on table size
            return table_size >= 500000  # Use parallel for tables with 500K+ rows
            
        # Default to standard processing if can't determine resources
        return table_size >= 500000  # Use parallel for tables with 500K+ rows

    @api.model
    def _sync_data_parallel(self, table_config, table_size, worker_count=None):
        """Perform data sync using parallel processing with proper job tracking"""
        start_time = time.time()
        
        # Create an instance of the parallel processor
        parallel_processor = self.env['etl.parallel.processor']
        
        try:
            # Determine optimal worker count based on table size and resources if not provided
            if worker_count is None:
                worker_count = self._determine_worker_count(table_size)
            
            # Start the parallel processing - using with_delay() instead of job decorator
            job = parallel_processor.with_delay(
                description=f"ETL Coordinator: {table_config.name}",
                channel="etl_coordinator", 
                priority=5
            ).parallel_coordinator(table_config.id, worker_count)
            
            # Store the job UUID in the table config for reference
            table_config.write({
                'job_uuid': job.uuid,
                'job_status': 'pending',
                'last_sync_status': 'running',
                'last_sync_message': f'Parallel sync started with {worker_count} workers (Job: {job.uuid})',
                'progress_percentage': 0
            })
            
            # Create initial stats for immediate return - processing continues in background
            stats = {
                'total_rows': 0,
                'new_rows': 0, 
                'updated_rows': 0,
                'execution_time': time.time() - start_time,
                'parallel': True,
                'workers': worker_count,
                'job_uuid': job.uuid,
                'status': 'queued'
            }
            
            # Log that processing will continue in background
            _logger.info(f"Parallel processing started for {table_config.name} with {worker_count} workers. "
                        f"Processing will continue in background with job UUID: {job.uuid}")
            
            return stats
                
        except Exception as e:
            _logger.error(f"Error in parallel sync: {str(e)}")
            
            # Update table status to show error
            table_config.write({
                'job_status': 'failed',
                'last_sync_status': 'failed',
                'last_sync_message': f'Parallel sync failed: {str(e)}'
            })
            
            # Fall back to standard processing
            _logger.info(f"Falling back to standard processing after parallel error")
            return self._sync_data_standard(table_config, table_size)

    def _sync_data_standard(self, table_config, table_size=0):
        """Perform data sync using standard non-parallel approach with enhanced transaction handling"""
        start_time = time.time()
        metrics = {
            'db_query_time': 0,
            'transform_time': 0,
            'csv_write_time': 0,
            'db_load_time': 0,
            'lookup_time': 0,
            'total_time': 0
        }
        
        stats = {
            'total_rows': 0,
            'new_rows': 0,
            'updated_rows': 0,
            'unchanged_rows': 0,
            'error_rows': 0,
            'execution_time': 0
        }
        
        # Get connector service
        connector_service = self.env['etl.database.connector.service']
        source_db = table_config.source_db_connection_id
        target_db = table_config.target_db_connection_id
        
        # Get configuration
        config = table_config.get_config_json()
        source_table = config['source_table']
        target_table = config['target_table']
        primary_key = config['primary_key']
        
        # Determine appropriate chunk size based on table characteristics
        if not table_size:
            try:
                table_size = connector_service.get_table_count(source_db, config['source_table'])
            except:
                table_size = 0
                
        chunk_size = self._determine_optimal_chunk_size(table_size)
        total_chunks = (table_size + chunk_size - 1) // chunk_size if table_size > 0 else 1
        
        # Create sync log
        sync_log = self.env['etl.sync.log'].create({
            'table_id': table_config.id,
            'start_time': fields.Datetime.now(),
            'status': 'running'
        })
        
        # Update table status
        table_config.write({
            'job_status': 'started',
            'last_sync_status': 'running',
            'last_sync_message': f'Starting sync with {total_chunks} chunks',
            'progress_percentage': 0
        })
        
        # Process each chunk
        try:
            # Get processed chunks (for resumability)
            processed_chunks = table_config.get_processed_chunks() if hasattr(table_config, 'get_processed_chunks') else []
            
            # Get transaction manager for safe operations
            tx_manager = self.env['etl.transaction']
            
            for chunk_num in range(total_chunks):
                # Skip already processed chunks
                if chunk_num in processed_chunks:
                    _logger.info(f"Skipping already processed chunk {chunk_num+1}/{total_chunks}")
                    continue
                    
                chunk_offset = chunk_num * chunk_size
                
                # Process chunk with safe transaction handling
                chunk_stats = self.process_chunk_with_safe_tx(
                    connector_service, source_db, target_db,
                    config, table_config, primary_key,
                    chunk_num, chunk_offset, chunk_size
                )
                
                # Update stats
                for key in stats:
                    if key in chunk_stats:
                        stats[key] += chunk_stats[key]
                
                # Mark chunk as processed
                if hasattr(table_config, 'set_processed_chunks'):
                    new_processed = processed_chunks + [chunk_num]
                    table_config.set_processed_chunks(new_processed)
                
                # Update progress
                progress = ((chunk_num + 1) / total_chunks) * 100
                table_config.write({
                    'progress_percentage': progress,
                    'last_sync_message': f'Processed {chunk_num+1}/{total_chunks} chunks ({progress:.1f}%)'
                })
                
                # Commit after each chunk to release transaction
                self.env.cr.commit()
                
                # Force garbage collection
                gc.collect()
            
            # Update final status
            table_config.write({
                'job_status': 'done',
                'progress_percentage': 100,
                'last_sync_status': 'success',
                'last_sync_message': f'Successfully synced {stats["total_rows"]} records ({stats["new_rows"]} new, {stats["updated_rows"]} updated)',
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
            
            # Calculate total time
            metrics['total_time'] = time.time() - start_time
            stats['execution_time'] = metrics['total_time']
            
            # Log performance metrics
            _logger.info(f"ETL Performance Metrics for {source_table} -> {target_table}:")
            for metric, value in metrics.items():
                _logger.info(f"  {metric}: {value:.2f}s")
            
            return stats
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in sync_data for table {source_table}: {error_message}")
            
            # Update status to failed
            table_config.write({
                'job_status': 'failed',
                'last_sync_status': 'failed',
                'last_sync_message': error_message
            })
            
            # Update sync log
            sync_log.write({
                'end_time': fields.Datetime.now(),
                'status': 'failed',
                'error_message': error_message
            })
            
            # Re-raise the exception
            raise

    def _determine_worker_count(self, table_size):
        """Determine optimal number of worker processes based on table size and system resources"""
        try:
            # Get CPU count 
            cpu_count = os.cpu_count() or 4
            
            # Get memory info
            import psutil
            mem = psutil.virtual_memory()
            available_gb = mem.available / (1024 * 1024 * 1024)
            
            # Base worker count on table size
            if table_size > 5000000:  # Very large table (>5M rows)
                base_workers = min(cpu_count, 8)
            elif table_size > 1000000:  # Large table (1-5M rows)
                base_workers = min(cpu_count, 6)
            elif table_size > 100000:  # Medium table (100K-1M rows)
                base_workers = min(cpu_count, 4)
            else:  # Small table
                base_workers = min(cpu_count, 2)
            
            # Adjust based on available memory
            if available_gb < 2:  # Less than 2GB available
                memory_workers = 2
            elif available_gb < 4:  # 2-4GB available
                memory_workers = 3
            else:  # More than 4GB available
                memory_workers = cpu_count
                
            # Use the smaller of the two constraints
            worker_count = min(base_workers, memory_workers)
            
            # Ensure at least 1, at most 8 workers
            return max(1, min(8, worker_count))
            
        except Exception as e:
            _logger.warning(f"Error determining worker count: {str(e)}")
            # Default to a conservative value if can't determine
            if table_size > 1000000:
                return 4
            elif table_size > 100000:
                return 2
            else:
                return 1

    def _determine_optimal_chunk_size(self, table_size):
        """Determine the optimal chunk size based on table size and system resources"""
        # Base chunk sizes on table size - MUCH SMALLER FOR LARGE TABLES
        if table_size > 5000000:  # Very large tables (>5M rows)
            chunk_size = 10000    # REDUCED from 25000
        elif table_size > 1000000:  # Large tables (1-5M rows)
            chunk_size = 20000    # REDUCED from 50000
        elif table_size > 100000:  # Medium tables (100K-1M rows)
            chunk_size = 50000    # REDUCED from 100000
        else:  # Small tables
            chunk_size = 100000   # REDUCED from 200000
        
        # Adjust based on available memory
        try:
            import psutil
            mem = psutil.virtual_memory()
            available_mb = mem.available / (1024 * 1024)
            
            # Reduce chunk size if memory is limited
            if available_mb < 1024:  # Less than 1GB available
                chunk_size = min(chunk_size, 5000)  # REDUCED from 10000
            elif available_mb < 4096:  # Less than 4GB available
                chunk_size = min(chunk_size, 10000)  # REDUCED from 25000
        except ImportError:
            # psutil not available, use conservative default
            chunk_size = min(chunk_size, 25000)  # REDUCED from 50000
        
        return chunk_size
    
    @api.model
    def sync_chunk_range(self, table_config_id, start_chunk, end_chunk, chunk_size):
        """Execute a range of chunks for parallel processing"""
        table_config = self.env['etl.table.config'].browse(table_config_id)
        
        # Get sync configuration
        sync_config_json = self.env['ir.config_parameter'].sudo().get_param(
            f'etl.sync.config.{table_config_id}', '{}'
        )
        sync_config = json.loads(sync_config_json)
        
        # Get basic configuration
        connector_service = self.env['etl.database.connector.service']
        config = table_config.get_config_json()
        source_db = table_config.source_db_connection_id
        target_db = table_config.target_db_connection_id
        primary_key = config['primary_key']
        
        _logger.info(f"Processing chunk range {start_chunk}-{end_chunk} for {config['source_table']}")
        
        # Initialize stats for this range
        range_stats = {
            'total_rows': 0,
            'new_rows': 0,
            'updated_rows': 0,
            'unchanged_rows': 0,
            'error_rows': 0
        }
        
        # Process each chunk in the range
        for chunk_num in range(start_chunk, end_chunk):
            # Skip if already processed (handle retries)
            if chunk_num in sync_config.get('processed_chunks', []):
                _logger.info(f"Skipping already processed chunk {chunk_num}")
                continue
            
            chunk_offset = chunk_num * chunk_size
            _logger.info(f"Processing chunk {chunk_num} (offset {chunk_offset})")
            
            try:
                # Process single chunk using enhanced chunking method
                chunk_stats = self._process_single_chunk(
                    connector_service, source_db, target_db,
                    config, table_config, primary_key,
                    chunk_num, chunk_offset, chunk_size
                )
                
                # Update range stats
                for key in range_stats:
                    range_stats[key] += chunk_stats.get(key, 0)
                
                # Mark chunk as processed
                sync_config.setdefault('processed_chunks', []).append(chunk_num)
                self.env['ir.config_parameter'].sudo().set_param(
                    f'etl.sync.config.{table_config_id}', 
                    json.dumps(sync_config)
                )
                
                # Update progress
                total_chunks = sync_config.get('total_chunks', 1)
                processed_count = len(sync_config.get('processed_chunks', []))
                progress = (processed_count / total_chunks) * 100
                
                table_config.write({
                    'progress_percentage': progress,
                    'last_sync_message': f'Processed {processed_count}/{total_chunks} chunks ({progress:.1f}%)'
                })
                
                # Commit transaction to release memory
                self.env.cr.commit()
                
                # Force garbage collection
                gc.collect()
                
            except Exception as e:
                _logger.error(f"Error processing chunk {chunk_num}: {str(e)}")
                range_stats['error_rows'] += chunk_size
                # Continue with next chunk - don't fail entire range
                self.env.cr.rollback()
        
        # Log completion of this chunk range
        _logger.info(f"Completed chunk range {start_chunk}-{end_chunk}: {range_stats['total_rows']} rows processed")
        
        # Check if all chunks are complete
        sync_config = json.loads(self.env['ir.config_parameter'].sudo().get_param(
            f'etl.sync.config.{table_config_id}', '{}'
        ))
        total_chunks = sync_config.get('total_chunks', 0)
        processed_count = len(sync_config.get('processed_chunks', []))
        
        if processed_count >= total_chunks:
            # All chunks processed - update completion
            elapsed_time = time.time() - sync_config.get('start_time', time.time())
            
            # Get final stats by summing all chunks
            final_stats = {
                'total_rows': 0,
                'new_rows': 0,
                'updated_rows': 0,
                'unchanged_rows': 0,
                'error_rows': 0,
                'execution_time': elapsed_time
            }
            
            # TODO: Sum stats from all chunks if stored
            
            table_config.write({
                'sync_status': 'completed',
                'progress_percentage': 100,
                'last_sync_time': fields.Datetime.now(),
                'last_sync_message': f'Processed {final_stats["total_rows"]} rows in {elapsed_time:.1f}s'
            })
        
        return range_stats
    
    @api.model
    def _process_single_chunk(self, connector_service, source_db, target_db,
                         config, table_config, primary_key,
                         chunk_num, chunk_offset, chunk_size):
        """Process a single chunk with proper transaction management and unique savepoints"""
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'unchanged_rows': 0, 'error_rows': 0}
        
        # Create unique savepoint with timestamp to avoid conflicts
        unique_id = int(time.time() * 1000) % 10000  # Millisecond timestamp modulo 10000
        savepoint_name = f"chunk_{table_config.id}_{chunk_num}_{unique_id}"
        
        try:
            # Create savepoint
            self.env.cr.execute(f"SAVEPOINT {savepoint_name}")
            _logger.info(f"Created savepoint {savepoint_name} for chunk {chunk_num}")
            
            # Get source columns information
            source_columns = connector_service.get_columns(source_db, config['source_table'])
            
            # DEBUG: Log the source columns
            _logger.info(f"Source columns: {source_columns}")
            
            # Get target table structure for validation
            target_columns_info = {}
            with connector_service.cursor(target_db) as cursor:
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = '{config["target_table"]}'
                    ORDER BY ordinal_position
                """)
                for row in cursor.fetchall():
                    col_name = row['column_name'].lower()
                    target_columns_info[col_name] = {
                        'type': row['data_type'].lower(),
                        'nullable': row['is_nullable'] == 'YES'
                    }
            
            # DEBUG: Log the target columns
            _logger.info(f"Target columns: {list(target_columns_info.keys())}")
            
            # Prepare mapping configuration with validation
            source_to_target_map = {}
            lookup_configs = {}
            
            # Extract mappings with validation
            for source_col, mapping in config['mappings'].items():
                original_source_col = source_columns.get(source_col.lower())
                if not original_source_col:
                    _logger.warning(f"Source column '{source_col}' not found in actual table columns")
                    continue
                    
                target_col = mapping.get('target', '').lower()
                if not target_col:
                    _logger.warning(f"No target column defined for source column '{source_col}'")
                    continue
                    
                # Validate target column exists in target table
                if target_col not in target_columns_info:
                    _logger.warning(f"Target column '{target_col}' not found in target table schema")
                    continue
                    
                # Check for type compatibility (basic check)
                source_value_example = self._get_sample_value(connector_service, source_db, config['source_table'], original_source_col)
                target_type = target_columns_info[target_col]['type']
                
                if source_value_example is not None:
                    # Log the mapping for debugging
                    _logger.info(f"Mapping source column '{original_source_col}' ({type(source_value_example)}) "
                            f"to target column '{target_col}' ({target_type})")
                    
                    # Check for obvious type mismatches
                    if (isinstance(source_value_example, str) and 'email' in source_value_example and 
                        ('date' in target_type or 'time' in target_type)):
                        _logger.error(f"Type mismatch! Source column '{original_source_col}' contains email-like data "
                                    f"but target column '{target_col}' is of type {target_type}")
                        continue
                
                # Store valid mapping
                source_to_target_map[original_source_col] = target_col
                
                # Store lookup configurations if needed
                if mapping.get('type') == 'lookup':
                    lookup_configs[original_source_col] = {
                        'table': mapping.get('lookup_table'),
                        'key_col': mapping.get('lookup_key'),
                        'value_col': mapping.get('lookup_value'),
                        'target_col': target_col
                    }
            
            # Log the final validated mappings
            _logger.info(f"Validated mappings: {source_to_target_map}")
            
            # Define target columns based on mappings - keep only those that exist in target table
            target_columns = [col for col in set(source_to_target_map.values()) if col in target_columns_info]
            
            if not target_columns:
                _logger.error("No valid target columns found in mappings after validation")
                raise ValidationError(_("No valid target columns found in mappings after validation"))
            
            # Get source data with all needed columns
            source_cols = list(source_to_target_map.keys())
            query = f"""
                SELECT {", ".join([f'"{col}"' for col in source_cols])} 
                FROM "{config['source_table']}" 
                ORDER BY "{primary_key}" 
                LIMIT {chunk_size} OFFSET {chunk_offset}
            """
            
            batch_data = connector_service.execute_query(source_db, query)
            
            if not batch_data or len(batch_data) == 0:
                # Release savepoint - important to clean up!
                self.env.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                return stats
            
            # Log a sample row for debugging
            if batch_data:
                _logger.info(f"Sample source row: {batch_data[0]}")
            
            # Process and transform data - keep original values without conversion
            transformed_data = []
            
            for row in batch_data:
                # Initialize with all target columns set to None
                transformed_row = {col: None for col in target_columns}
                
                # Apply direct mappings first - preserve original values
                for source_col, target_col in source_to_target_map.items():
                    if source_col in row:
                        # Don't apply conversion - keep original value
                        # Check if this is a lookup field
                        if source_col in lookup_configs:
                            continue  # Handle lookups separately
                        transformed_row[target_col] = row[source_col]
                
                # Apply lookups
                for source_col, lookup_config in lookup_configs.items():
                    if source_col not in row or row[source_col] is None:
                        continue
                        
                    source_value = str(row[source_col])
                    target_col = lookup_config['target_col']
                    
                    # Perform lookup
                    lookup_value = self._lookup_value(
                        connector_service,
                        target_db,
                        lookup_config['table'],
                        lookup_config['key_col'],
                        lookup_config['value_col'],
                        source_value
                    )
                    transformed_row[target_col] = lookup_value
                
                # Add to list if not empty
                if any(v is not None for v in transformed_row.values()):
                    transformed_data.append(transformed_row)
            
            if not transformed_data:
                _logger.warning(f"No valid transformed data for chunk {chunk_num}")
                # Release savepoint - important to clean up!
                self.env.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                return stats
            
            # Log a sample transformed row for debugging
            if transformed_data:
                _logger.info(f"Sample transformed row: {transformed_data[0]}")
            
            # Use INSERT statements instead of CSV COPY to ensure columns are properly aligned
            with connector_service.cursor(target_db) as cursor:
                batch_total = 0
                batch_new = 0
                batch_updated = 0
                
                # Process each row with individual UPSERT
                for row in transformed_data:
                    # Prepare column lists
                    cols = []
                    vals = []
                    placeholders = []
                    
                    for col, val in row.items():
                        if val is not None:  # Only include non-NULL values
                            cols.append(f'"{col}"')
                            vals.append(val)
                            placeholders.append('%s')
                    
                    if not cols:  # Skip if no columns to insert
                        continue
                        
                    # Check if record exists
                    check_sql = f'SELECT 1 FROM "{config["target_table"]}" WHERE "{primary_key}" = %s'
                    cursor.execute(check_sql, [row.get(primary_key)])
                    exists = cursor.fetchone() is not None
                    
                    if exists:
                        # Update existing record
                        update_parts = []
                        update_vals = []
                        
                        for col, val in row.items():
                            if col != primary_key and val is not None:
                                update_parts.append(f'"{col}" = %s')
                                update_vals.append(val)
                        
                        if update_parts:
                            update_sql = f'UPDATE "{config["target_table"]}" SET {", ".join(update_parts)} WHERE "{primary_key}" = %s'
                            update_vals.append(row.get(primary_key))
                            cursor.execute(update_sql, update_vals)
                            batch_updated += 1
                    else:
                        # Insert new record
                        insert_sql = f'INSERT INTO "{config["target_table"]}" ({", ".join(cols)}) VALUES ({", ".join(placeholders)})'
                        cursor.execute(insert_sql, vals)
                        batch_new += 1
                    
                    batch_total += 1
                
                # Update stats
                stats['total_rows'] += batch_total
                stats['new_rows'] += batch_new
                stats['updated_rows'] += batch_updated
                
                _logger.info(f"Chunk {chunk_num} results: {batch_total} total rows, {batch_new} new, {batch_updated} updated")
            
            # Update progress information in a separate transaction to avoid contention
            try:
                # Acquire an advisory lock to prevent concurrent updates to this table's progress
                lock_id = abs(hash(f"etl_progress_{table_config.id}")) % 2147483647  # Ensure it's a positive integer within range
                self.env.cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (lock_id,))
                lock_acquired = self.env.cr.fetchone()[0]
                
                if lock_acquired:
                    # Get current processed chunks in a separate select to avoid locking
                    current_chunks = table_config.get_processed_chunks() if hasattr(table_config, 'get_processed_chunks') else []
                    
                    # Add current chunk if not already processed
                    if chunk_num not in current_chunks:
                        current_chunks.append(chunk_num)
                        
                    # Calculate new progress
                    if hasattr(table_config, 'set_processed_chunks'):
                        table_config.set_processed_chunks(current_chunks)
                    else:
                        total_chunks = (table_config.total_records_synced or 1000) // chunk_size + 1
                        progress = min(99.0, len(current_chunks) / total_chunks * 100.0)
                        
                        # Update progress directly with lower isolation level
                        self.env.cr.execute("""
                            UPDATE etl_source_table 
                            SET processed_chunks = %s,
                                progress_percentage = %s,
                                last_sync_message = %s
                            WHERE id = %s
                        """, (
                            json.dumps(current_chunks),
                            progress,
                            f'Processed {len(current_chunks)} chunks ({progress:.1f}%)',
                            table_config.id
                        ))
                else:
                    _logger.info(f"Skipped progress update for chunk {chunk_num} - couldn't acquire lock")
            except Exception as progress_error:
                # Log but don't abort transaction
                _logger.warning(f"Error updating progress: {str(progress_error)}")
            
            # Release savepoint
            self.env.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            
            return stats
            
        except Exception as e:
            # Rollback to savepoint
            try:
                _logger.error(f"Error processing chunk {chunk_num}: {str(e)}")
                self.env.cr.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            except Exception as rollback_error:
                # If rollback fails, log but continue
                _logger.error(f"Error in rollback to savepoint: {str(rollback_error)}")
                # Attempt full transaction rollback as a last resort
                try:
                    self.env.cr.rollback()
                    _logger.warning("Performed full transaction rollback after savepoint rollback failure")
                except:
                    pass  # Avoid nested exception
            
            stats['error_rows'] += chunk_size
            return stats

    @api.model
    def _get_sample_value(self, connector_service, db_conn, table, column):
        """Get a sample value from a column for type checking"""
        try:
            query = f'SELECT "{column}" FROM "{table}" WHERE "{column}" IS NOT NULL LIMIT 1'
            result = connector_service.execute_query(db_conn, query)
            if result and len(result) > 0:
                return result[0][column]
            return None
        except Exception as e:
            _logger.warning(f"Error getting sample value for column {column}: {str(e)}")
            return None
    
    @api.model
    def sync_with_insert_statements(self, connector_service, source_db, target_db, 
                                config, table_config, primary_key, metrics, chunk_size=5000):
        """Synchronize data using direct INSERT statements instead of CSV COPY for better column control"""
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'unchanged_rows': 0, 'error_rows': 0}
        
        # Get total count and calculate number of chunks
        db_query_start = time.time()
        total_count = connector_service.get_table_count(source_db, config['source_table'])
        metrics['db_query_time'] += time.time() - db_query_start
        
        total_chunks = (total_count + chunk_size - 1) // chunk_size
        
        _logger.info(f"Using INSERT statements for table {config['source_table']} with {total_count} rows in {total_chunks} chunks")
        
        # Track processed chunks for resumability
        processed_chunks = []
        try:
            # Try using the new get_processed_chunks method if available
            if hasattr(table_config, 'get_processed_chunks'):
                processed_chunks = table_config.get_processed_chunks()
            # Try accessing the field directly if available
            elif hasattr(table_config, 'processed_chunks') and table_config.processed_chunks:
                try:
                    processed_chunks = json.loads(table_config.processed_chunks)
                except (ValueError, json.JSONDecodeError):
                    processed_chunks = []
        except Exception as e:
            _logger.warning(f"Could not retrieve processed chunks: {str(e)}")
            processed_chunks = []
        
        # Process each chunk
        for chunk_num in range(total_chunks):
            # Skip previously processed chunks (for resumability)
            if chunk_num in processed_chunks:
                _logger.info(f"Skipping already processed chunk {chunk_num+1}")
                continue
                
            chunk_offset = chunk_num * chunk_size
            _logger.info(f"Processing chunk {chunk_num+1}/{total_chunks} (offset {chunk_offset})")
            
            # Create savepoint for this chunk
            savepoint_name = f"chunk_{chunk_num}"
            self.env.cr.execute(f"SAVEPOINT {savepoint_name}")
            
            try:
                # Use the single chunk processor with INSERT statements
                chunk_stats = self._process_single_chunk(
                    connector_service, source_db, target_db,
                    config, table_config, primary_key,
                    chunk_num, chunk_offset, chunk_size
                )
                
                # Update overall stats
                for key in stats:
                    stats[key] += chunk_stats.get(key, 0)
                
                # Mark chunk as processed 
                processed_chunks.append(chunk_num)
                try:
                    # Try using the setter method if available
                    if hasattr(table_config, 'set_processed_chunks'):
                        table_config.set_processed_chunks(processed_chunks)
                    # Otherwise try to write directly to field
                    else:
                        table_config.write({'processed_chunks': json.dumps(processed_chunks)})
                except Exception as e:
                    _logger.warning(f"Could not save processed chunks: {str(e)}")
                    # Fall back to storing in ir.config_parameter
                    self.env['ir.config_parameter'].sudo().set_param(
                        f'etl.processed_chunks.{table_config.id}', 
                        json.dumps(processed_chunks)
                    )
                
                # Update progress
                progress = ((chunk_num + 1) / total_chunks) * 100
                table_config.write({
                    'progress_percentage': progress,
                    'last_sync_message': f'Processed {chunk_num+1}/{total_chunks} chunks ({progress:.1f}%)'
                })
                
                # Release savepoint
                self.env.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                
                # Commit transaction to release memory
                self.env.cr.commit()
                
                # Force garbage collection
                gc.collect()
                
            except Exception as e:
                # Rollback to savepoint instead of whole transaction
                self.env.cr.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                _logger.error(f"Error processing chunk {chunk_num+1}: {str(e)}")
                stats['error_rows'] += chunk_size
        
        return stats
    
    @api.model
    def _sync_with_enhanced_chunking(self, connector_service, source_db, target_db, 
                                config, table_config, primary_key, metrics, chunk_size=50000):
        """Process extremely large tables in small chunks with progress tracking"""
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'unchanged_rows': 0, 'error_rows': 0}
        
        # Get total count and calculate number of chunks
        db_query_start = time.time()
        total_count = connector_service.get_table_count(source_db, config['source_table'])
        metrics['db_query_time'] += time.time() - db_query_start
        
        total_chunks = (total_count + chunk_size - 1) // chunk_size
        
        _logger.info(f"Enhanced chunking: Processing {total_count} rows in {total_chunks} chunks")
        
        # Track processed chunks for resumability - handle both old and new table structure
        processed_chunks = []
        try:
            # Try using the new get_processed_chunks method if available
            if hasattr(table_config, 'get_processed_chunks'):
                processed_chunks = table_config.get_processed_chunks()
            # Try accessing the field directly if available
            elif hasattr(table_config, 'processed_chunks') and table_config.processed_chunks:
                try:
                    processed_chunks = json.loads(table_config.processed_chunks)
                except (ValueError, json.JSONDecodeError):
                    processed_chunks = []
        except Exception as e:
            _logger.warning(f"Could not retrieve processed chunks: {str(e)}")
            processed_chunks = []
        
        # Create temporary staging table
        staging_table = f"tmp_staging_{config['target_table']}_{int(time.time())}"
        with connector_service.cursor(target_db) as cursor:
            cursor.execute(f'CREATE TABLE "{staging_table}" (LIKE "{config["target_table"]}")')
        
        try:
            # Process each chunk
            for chunk_num in range(total_chunks):
                # Skip previously processed chunks (for resumability)
                if chunk_num in processed_chunks:
                    _logger.info(f"Skipping already processed chunk {chunk_num+1}")
                    continue
                    
                chunk_offset = chunk_num * chunk_size
                _logger.info(f"Processing chunk {chunk_num+1}/{total_chunks} (offset {chunk_offset})")
                
                # Create savepoint for this chunk
                savepoint_name = f"chunk_{chunk_num}"
                self.env.cr.execute(f"SAVEPOINT {savepoint_name}")
                
                try:
                    # Use the single chunk processor
                    chunk_stats = self._process_single_chunk(
                        connector_service, source_db, target_db,
                        config, table_config, primary_key,
                        chunk_num, chunk_offset, chunk_size
                    )
                    
                    # Update overall stats
                    for key in stats:
                        stats[key] += chunk_stats.get(key, 0)
                    
                    # Mark chunk as processed - handle both old and new table structure
                    processed_chunks.append(chunk_num)
                    try:
                        # Try using the setter method if available
                        if hasattr(table_config, 'set_processed_chunks'):
                            table_config.set_processed_chunks(processed_chunks)
                        # Otherwise try to write directly to field
                        else:
                            table_config.write({'processed_chunks': json.dumps(processed_chunks)})
                    except Exception as e:
                        _logger.warning(f"Could not save processed chunks: {str(e)}")
                        # Fall back to storing in ir.config_parameter
                        self.env['ir.config_parameter'].sudo().set_param(
                            f'etl.processed_chunks.{table_config.id}', 
                            json.dumps(processed_chunks)
                        )
                    
                    # Update progress
                    progress = ((chunk_num + 1) / total_chunks) * 100
                    table_config.write({
                        'progress_percentage': progress,
                        'last_sync_message': f'Processed {chunk_num+1}/{total_chunks} chunks ({progress:.1f}%)'
                    })
                    
                    # Release savepoint
                    self.env.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    
                    # Commit transaction to release memory
                    self.env.cr.commit()
                    
                    # Force garbage collection
                    gc.collect()
                    
                except Exception as e:
                    # Rollback to savepoint instead of whole transaction
                    self.env.cr.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                    _logger.error(f"Error processing chunk {chunk_num+1}: {str(e)}")
                    stats['error_rows'] += chunk_size
            
            return stats
            
        finally:
            # Clean up staging table
            try:
                with connector_service.cursor(target_db) as cursor:
                    cursor.execute(f'DROP TABLE IF EXISTS "{staging_table}"')
            except Exception as e:
                _logger.warning(f"Failed to drop staging table: {str(e)}")
    
    @api.model
    def _sync_with_batched_copy(self, connector_service, source_db, target_db, 
                             config, table_config, primary_key, metrics, batch_size=50000):
        """Use batched COPY operations for better memory management"""
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'unchanged_rows': 0, 'error_rows': 0}
        
        try:
            # Use cursor-based approach with batches
            offset = 0
            total_new = 0
            total_updated = 0
            
            # Get source columns from mappings
            column_definitions = []
            target_columns = []
            
            for source_col, mapping in config['mappings'].items():
                if isinstance(mapping, dict) and mapping.get('target'):
                    target_columns.append(mapping['target'].lower())
                    column_definitions.append(f'"{mapping["target"].lower()}"')
            
            # Create temporary staging table
            staging_table = f"tmp_staging_{config['target_table']}_{int(time.time())}"
            with connector_service.cursor(target_db) as cursor:
                cursor.execute(f'CREATE TABLE "{staging_table}" (LIKE "{config["target_table"]}")')
            
            # Process in batches
            has_more = True
            batch_num = 0
            
            while has_more:
                batch_num += 1
                _logger.info(f"Processing batch {batch_num} at offset {offset}")
                
                # Create savepoint
                savepoint_name = f"batch_{batch_num}"
                self.env.cr.execute(f"SAVEPOINT {savepoint_name}")
                
                try:
                    # Create batch-specific view in source
                    source_view = f"tmp_export_view_{int(time.time())}_{offset}"
                    
                    # Get column mappings
                    columns_result = connector_service.get_columns(source_db, config['source_table'])
                    
                    # Handle different return formats
                    if isinstance(columns_result, list) and len(columns_result) > 0:
                        source_columns = columns_result[0]
                    elif isinstance(columns_result, dict):
                        source_columns = columns_result
                    else:
                        raise ValueError(f"Invalid column data format: {type(columns_result)}")
                        
                    if not source_columns:
                        raise ValueError(f"No columns found for source table {config['source_table']}")
                    
                    view_columns = []
                    
                    for source_col, mapping in config['mappings'].items():
                        if mapping['type'] == 'direct':
                            original_col = source_columns.get(source_col.lower())
                            if original_col:
                                view_columns.append(f'"{original_col}" AS "{mapping["target"].lower()}"')
                    
                    # Create batch export view
                    db_query_start = time.time()
                    with connector_service.cursor(source_db) as src_cursor:
                        create_view_sql = f"""
                            CREATE TEMPORARY VIEW "{source_view}" AS 
                            SELECT {", ".join(view_columns)} 
                            FROM "{config["source_table"]}" 
                            ORDER BY "{primary_key}" 
                            LIMIT {batch_size} OFFSET {offset}
                        """
                        src_cursor.execute(create_view_sql)
                        
                        # Get count of batch
                        src_cursor.execute(f'SELECT COUNT(*) FROM "{source_view}"')
                        batch_count = src_cursor.fetchone()['count']
                        metrics['db_query_time'] += time.time() - db_query_start
                        
                        if batch_count == 0:
                            has_more = False
                            # Drop the temporary view
                            src_cursor.execute(f'DROP VIEW IF EXISTS "{source_view}"')
                            # Release savepoint
                            self.env.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                            break
                        
                        # Export batch to a CSV file
                        csv_filename = f"/tmp/etl_export_batch_{offset}_{int(time.time())}.csv"
                        
                        csv_start = time.time()
                        copy_to_sql = f"COPY (SELECT * FROM \"{source_view}\") TO STDIN WITH CSV HEADER"
                        
                        with open(csv_filename, 'w') as f:
                            # Get raw connection
                            raw_conn = src_cursor.connection
                            # Use copy_expert for best performance
                            with raw_conn.cursor() as raw_cursor:
                                raw_cursor.copy_expert(copy_to_sql, f)
                        metrics['csv_write_time'] += time.time() - csv_start
                        
                        # Drop the temporary view
                        src_cursor.execute(f'DROP VIEW IF EXISTS "{source_view}"')
                    
                    # Import batch from CSV into staging table
                    db_load_start = time.time()
                    with connector_service.cursor(target_db) as cursor:
                        # Truncate staging table for this batch
                        cursor.execute(f'TRUNCATE TABLE "{staging_table}"')
                        
                        # Copy from CSV to staging
                        with open(csv_filename, 'r') as f:
                            # Get raw connection
                            raw_conn = cursor.connection
                            # Use copy_expert for best performance
                            with raw_conn.cursor() as raw_cursor:
                                copy_from_sql = f'COPY "{staging_table}" FROM STDIN WITH CSV HEADER'
                                raw_cursor.copy_expert(copy_from_sql, f)
                        
                        # Merge from staging to target
                        merge_sql = f"""
                            WITH merge_result AS (
                                INSERT INTO "{config['target_table']}" ({", ".join(column_definitions)})
                                SELECT {", ".join(column_definitions)} FROM "{staging_table}" 
                                ON CONFLICT ("{primary_key}") 
                                DO UPDATE SET 
                                    {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in target_columns if col.lower() != primary_key.lower()])}
                                RETURNING (xmax = 0) AS inserted
                            )
                            SELECT 
                                COUNT(*) AS total,
                                SUM(CASE WHEN inserted THEN 1 ELSE 0 END) AS inserted,
                                SUM(CASE WHEN NOT inserted THEN 1 ELSE 0 END) AS updated
                            FROM merge_result
                        """
                        
                        cursor.execute(merge_sql)
                        result = cursor.fetchone()
                        metrics['db_load_time'] += time.time() - db_load_start
                        
                        batch_total = result['total'] if result else 0
                        batch_new = result['inserted'] if result else 0
                        batch_updated = result['updated'] if result else 0
                        
                        # Update stats
                        stats['total_rows'] += batch_total
                        total_new += batch_new
                        total_updated += batch_updated
                    
                    # Clean up batch CSV file
                    try:
                        os.remove(csv_filename)
                    except Exception as e:
                        _logger.warning(f"Failed to remove CSV file {csv_filename}: {str(e)}")
                    
                    # Update offset for next batch
                    offset += batch_count
                    
                    # Check if we're at the end
                    if batch_count < batch_size:
                        has_more = False
                    
                    # Log progress
                    _logger.info(f"Processed batch at offset {offset}: {batch_total} rows ({batch_new} new, {batch_updated} updated)")
                    
                    # Update table config with progress
                    # This depends on having an estimate of total rows
                    db_query_start = time.time()
                    total_rows = connector_service.get_table_count(source_db, config['source_table'])
                    metrics['db_query_time'] += time.time() - db_query_start
                    
                    progress = min(100, (offset / total_rows) * 100) if total_rows > 0 else 0
                    table_config.write({
                        'progress_percentage': progress,
                        'last_sync_message': f'Processed {stats["total_rows"]} rows ({progress:.1f}%)'
                    })
                    
                    # Release savepoint
                    self.env.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    
                    # Commit transaction to release memory
                    self.env.cr.commit()
                    
                    # Force garbage collection
                    gc.collect()
                    
                except Exception as e:
                    # Rollback to savepoint
                    self.env.cr.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                    _logger.error(f"Error processing batch at offset {offset}: {str(e)}")
                    stats['error_rows'] += batch_size
                    # Continue with next batch
            
            # Clean up staging table
            with connector_service.cursor(target_db) as cursor:
                cursor.execute(f'DROP TABLE IF EXISTS "{staging_table}"')
            
            # Update final stats
            stats['new_rows'] = total_new
            stats['updated_rows'] = total_updated
            
            return stats
            
        except Exception as e:
            _logger.error(f"Error in PostgreSQL batched COPY sync: {str(e)}")
            
            # Attempt to clean up
            try:
                with connector_service.cursor(source_db) as cursor:
                    if 'source_view' in locals() and source_view:
                        cursor.execute(f'DROP VIEW IF EXISTS "{source_view}"')
            except Exception as cleanup_e:
                _logger.warning(f"Cleanup error: {str(cleanup_e)}")
                
            try:
                with connector_service.cursor(target_db) as cursor:
                    if 'staging_table' in locals() and staging_table:
                        cursor.execute(f'DROP TABLE IF EXISTS "{staging_table}"')
            except Exception as cleanup_e:
                _logger.warning(f"Cleanup error: {str(cleanup_e)}")
                
            try:
                if 'csv_filename' in locals() and csv_filename and os.path.exists(csv_filename):
                    os.remove(csv_filename)
            except Exception as cleanup_e:
                _logger.warning(f"Cleanup error: {str(cleanup_e)}")
                
            raise
    
    @api.model
    def _sync_with_bulk_upsert(self, connector_service, source_db, target_db, 
                           config, table_config, primary_key, metrics, batch_size=None):
        """Use bulk upsert operations with optimized batch size"""
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'unchanged_rows': 0, 'error_rows': 0}
        
        # Choose optimal batch size based on column count and types
        column_count = len(config['mappings'])
        # Adjust batch size based on column count - more columns = smaller batches
        if not batch_size:
            if column_count > 50:
                batch_size = 500
            elif column_count > 20:
                batch_size = 1000
            else:
                batch_size = 2000
        
        try:
            # Import only if needed
            db_query_start = time.time()
            from psycopg2.extras import execute_values
            
            # Get source columns and prepare mappings
            columns_result = connector_service.get_columns(source_db, config['source_table'])
            
            # Handle different return formats
            if isinstance(columns_result, list) and len(columns_result) > 0:
                source_columns = columns_result[0]
            elif isinstance(columns_result, dict):
                source_columns = columns_result
            else:
                raise ValueError(f"Invalid column data format: {type(columns_result)}")
                
            if not source_columns:
                raise ValueError(f"No columns found for source table {config['source_table']}")
            metrics['db_query_time'] += time.time() - db_query_start
            
            # Map source to target columns
            column_map = {}
            target_columns = []
            lookup_configs = {}
            
            for source_col, mapping in config['mappings'].items():
                if isinstance(mapping, dict) and mapping.get('target'):
                    original_col = source_columns.get(source_col.lower())
                    if original_col:
                        column_map[original_col] = mapping['target'].lower()
                        target_columns.append(mapping['target'].lower())
                        
                        # Store lookup configs
                        if mapping.get('type') == 'lookup':
                            lookup_configs[original_col] = {
                                'table': mapping.get('lookup_table'),
                                'key_col': mapping.get('lookup_key'),
                                'value_col': mapping.get('lookup_value')
                            }
            
            # Get target column information and types
            target_column_types = {}
            with connector_service.cursor(target_db) as cursor:
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = '{config["target_table"]}'
                """)
                for row in cursor.fetchall():
                    col_name = row['column_name'].lower()
                    target_column_types[col_name] = {
                        'type': row['data_type'].lower(),
                        'nullable': row['is_nullable'] == 'YES'
                    }
            
            # Process in batches with direct sync
            offset = 0
            has_more = True
            batch_num = 0
            
            while has_more:
                batch_num += 1
                _logger.info(f"Processing batch {batch_num} at offset {offset}")
                
                # Create savepoint
                savepoint_name = f"batch_{batch_num}"
                self.env.cr.execute(f"SAVEPOINT {savepoint_name}")
                
                try:
                    # Get a batch of data
                    db_query_start = time.time()
                    query = f"""
                        SELECT * FROM "{config['source_table']}" 
                        ORDER BY "{primary_key}" 
                        LIMIT {batch_size} OFFSET {offset}
                    """
                    
                    batch_data = connector_service.execute_query(source_db, query)
                    metrics['db_query_time'] += time.time() - db_query_start
                    
                    if not batch_data or len(batch_data) == 0:
                        has_more = False
                        # Release savepoint
                        self.env.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                        break
                    
                    # Transform data
                    transform_start = time.time()
                    transformed_data = []
                    for row in batch_data:
                        transformed_row = {}
                        
                        # First pass: direct mappings
                        for original_col, target_col in column_map.items():
                            if original_col in lookup_configs:
                                continue  # Skip lookups for first pass
                                
                            value = row.get(original_col)
                            transformed_row[target_col] = value
                        
                        # Second pass: lookups with caching
                        lookup_start = time.time()
                        for original_col, lookup_config in lookup_configs.items():
                            value = row.get(original_col)
                            if value is None:
                                continue
                                
                            target_col = column_map[original_col]
                            lookup_value = self._lookup_value(
                                connector_service,
                                target_db,
                                lookup_config['table'],
                                lookup_config['key_col'],
                                lookup_config['value_col'],
                                str(value)
                            )
                            transformed_row[target_col] = lookup_value
                        metrics['lookup_time'] += time.time() - lookup_start
                        
                        # Third pass: type conversions
                        for col, value in list(transformed_row.items()):
                            if col in target_column_types:
                                col_type = target_column_types[col]['type']
                                nullable = target_column_types[col]['nullable']
                                transformed_row[col] = self._convert_field_value(value, col_type, nullable)
                        
                        transformed_data.append(transformed_row)
                    metrics['transform_time'] += time.time() - transform_start
                    
                    # Bulk upsert using execute_values
                    db_load_start = time.time()
                    with connector_service.cursor(target_db) as cursor:
                        # Prepare column list and update clause
                        column_list = ', '.join([f'"{col}"' for col in target_columns])
                        update_clause = ', '.join([
                            f'"{col}" = EXCLUDED."{col}"' 
                            for col in target_columns 
                            if col.lower() != primary_key.lower()
                        ])
                        
                        # Prepare SQL and data
                        upsert_sql = f"""
                            INSERT INTO "{config['target_table']}" ({column_list})
                            VALUES %s
                            ON CONFLICT ("{primary_key}")
                            DO UPDATE SET {update_clause}
                            RETURNING 
                                (xmax = 0) AS inserted
                        """
                        
                        # Prepare values for execute_values
                        rows = []
                        for row in transformed_data:
                            value_row = []
                            for col in target_columns:
                                value_row.append(row.get(col))
                            rows.append(tuple(value_row))
                        
                        # Execute the upsert
                        from psycopg2.extras import execute_values
                        cursor.execute("SELECT 1")  # Dummy query to get connection
                        execute_values(
                            cursor, 
                            upsert_sql, 
                            rows,
                            fetch=True
                        )
                        
                        # Get results
                        results = cursor.fetchall()
                        batch_total = len(results)
                        batch_new = sum(1 for r in results if r['inserted'])
                        batch_updated = batch_total - batch_new
                        
                        # Update stats
                        stats['total_rows'] += batch_total
                        stats['new_rows'] += batch_new
                        stats['updated_rows'] += batch_updated
                    metrics['db_load_time'] += time.time() - db_load_start
                    
                    # Update offset for next batch
                    offset += len(batch_data)
                    
                    # Check if we've reached the end
                    if len(batch_data) < batch_size:
                        has_more = False
                    
                    # Log progress
                    _logger.info(f"Processed batch at offset {offset}: {batch_total} rows ({batch_new} new, {batch_updated} updated)")
                    
                    # Update table config with progress
                    db_query_start = time.time()
                    total_rows = connector_service.get_table_count(source_db, config['source_table'])
                    metrics['db_query_time'] += time.time() - db_query_start
                    
                    progress = min(100, (offset / total_rows) * 100) if total_rows > 0 else 0
                    table_config.write({
                        'progress_percentage': progress,
                        'last_sync_message': f'Processed {stats["total_rows"]} rows ({progress:.1f}%)'
                    })
                    
                    # Release savepoint
                    self.env.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    
                    # Commit transaction to release memory
                    self.env.cr.commit()
                    
                    # Force garbage collection
                    gc.collect()
                    
                except Exception as e:
                    # Rollback to savepoint
                    self.env.cr.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                    _logger.error(f"Error processing batch at offset {offset}: {str(e)}")
                    stats['error_rows'] += batch_size
                    # Continue with next batch
            
            return stats
            
        except Exception as e:
            _logger.error(f"Error in PostgreSQL bulk upsert: {str(e)}")
            raise

    def process_chunk_with_safe_tx(self, connector_service, source_db, target_db, config, 
                                table_config, primary_key, chunk_num, chunk_offset, chunk_size):
        """Process a chunk with safe transaction handling"""
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'unchanged_rows': 0, 'error_rows': 0}
        
        # Create unique savepoint with timestamp to avoid conflicts
        unique_id = int(time.time() * 1000) % 10000  # Millisecond timestamp modulo 10000
        savepoint_name = f"chunk_{table_config.id}_{chunk_num}_{unique_id}"
        
        # Add retry mechanism for serialization failures
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Create savepoint
                self.env.cr.execute(f"SAVEPOINT {savepoint_name}")
                _logger.info(f"Created savepoint {savepoint_name} for chunk {chunk_num}")
                
                # Use READ COMMITTED isolation level to reduce serialization conflicts
                self.env.cr.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                
                # Get source columns information
                source_columns = connector_service.get_columns(source_db, config['source_table'])
                
                # DEBUG: Log the source columns
                _logger.info(f"Source columns: {source_columns}")
                
                # Get target table structure for validation
                target_columns_info = {}
                with connector_service.cursor(target_db) as cursor:
                    cursor.execute(f"""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns 
                        WHERE table_name = '{config["target_table"]}'
                        ORDER BY ordinal_position
                    """)
                    for row in cursor.fetchall():
                        col_name = row['column_name'].lower()
                        target_columns_info[col_name] = {
                            'type': row['data_type'].lower(),
                            'nullable': row['is_nullable'] == 'YES'
                        }
                
                # DEBUG: Log the target columns
                _logger.info(f"Target columns: {list(target_columns_info.keys())}")
                
                # Prepare mapping configuration with validation
                source_to_target_map = {}
                lookup_configs = {}
                
                # Extract mappings with validation
                for source_col, mapping in config['mappings'].items():
                    original_source_col = source_columns.get(source_col.lower())
                    if not original_source_col:
                        _logger.warning(f"Source column '{source_col}' not found in actual table columns")
                        continue
                        
                    target_col = mapping.get('target', '').lower()
                    if not target_col:
                        _logger.warning(f"No target column defined for source column '{source_col}'")
                        continue
                        
                    # Validate target column exists in target table
                    if target_col not in target_columns_info:
                        _logger.warning(f"Target column '{target_col}' not found in target table schema")
                        continue
                        
                    # Check for type compatibility (basic check)
                    source_value_example = self._get_sample_value(connector_service, source_db, config['source_table'], original_source_col)
                    target_type = target_columns_info[target_col]['type']
                    
                    if source_value_example is not None:
                        # Log the mapping for debugging
                        _logger.info(f"Mapping source column '{original_source_col}' ({type(source_value_example)}) "
                                f"to target column '{target_col}' ({target_type})")
                        
                        # Check for obvious type mismatches
                        if (isinstance(source_value_example, str) and 'email' in source_value_example and 
                            ('date' in target_type or 'time' in target_type)):
                            _logger.error(f"Type mismatch! Source column '{original_source_col}' contains email-like data "
                                        f"but target column '{target_col}' is of type {target_type}")
                            continue
                    
                    # Store valid mapping
                    source_to_target_map[original_source_col] = target_col
                    
                    # Store lookup configurations if needed
                    if mapping.get('type') == 'lookup':
                        lookup_configs[original_source_col] = {
                            'table': mapping.get('lookup_table'),
                            'key_col': mapping.get('lookup_key'),
                            'value_col': mapping.get('lookup_value'),
                            'target_col': target_col
                        }
                
                # Log the final validated mappings
                _logger.info(f"Validated mappings: {source_to_target_map}")
                
                # Define target columns based on mappings - keep only those that exist in target table
                target_columns = [col for col in set(source_to_target_map.values()) if col in target_columns_info]
                
                if not target_columns:
                    _logger.error("No valid target columns found in mappings after validation")
                    raise ValidationError(_("No valid target columns found in mappings after validation"))
                
                # Get source data with all needed columns
                source_cols = list(source_to_target_map.keys())
                query = f"""
                    SELECT {", ".join([f'"{col}"' for col in source_cols])} 
                    FROM "{config['source_table']}" 
                    ORDER BY "{primary_key}" 
                    LIMIT {chunk_size} OFFSET {chunk_offset}
                """
                
                batch_data = connector_service.execute_query(source_db, query)
                
                if not batch_data or len(batch_data) == 0:
                    # Release savepoint - important to clean up!
                    self.env.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    return stats
                
                # Log a sample row for debugging
                if batch_data:
                    _logger.info(f"Sample source row: {batch_data[0]}")
                
                # Process and transform data - keep original values without conversion
                transformed_data = []
                
                for row in batch_data:
                    # Initialize with all target columns set to None
                    transformed_row = {col: None for col in target_columns}
                    
                    # Apply direct mappings first - preserve original values
                    for source_col, target_col in source_to_target_map.items():
                        if source_col in row:
                            # Don't apply conversion - keep original value
                            # Check if this is a lookup field
                            if source_col in lookup_configs:
                                continue  # Handle lookups separately
                            transformed_row[target_col] = row[source_col]
                    
                    # Apply lookups
                    for source_col, lookup_config in lookup_configs.items():
                        if source_col not in row or row[source_col] is None:
                            continue
                            
                        source_value = str(row[source_col])
                        target_col = lookup_config['target_col']
                        
                        # Perform lookup
                        lookup_value = self._lookup_value(
                            connector_service,
                            target_db,
                            lookup_config['table'],
                            lookup_config['key_col'],
                            lookup_config['value_col'],
                            source_value
                        )
                        transformed_row[target_col] = lookup_value
                    
                    # Add to list if not empty
                    if any(v is not None for v in transformed_row.values()):
                        transformed_data.append(transformed_row)
                
                if not transformed_data:
                    _logger.warning(f"No valid transformed data for chunk {chunk_num}")
                    # Release savepoint - important to clean up!
                    self.env.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    return stats
                
                # Log a sample transformed row for debugging
                if transformed_data:
                    _logger.info(f"Sample transformed row: {transformed_data[0]}")
                
                # Use INSERT statements instead of CSV COPY to ensure columns are properly aligned
                with connector_service.cursor(target_db) as cursor:
                    batch_total = 0
                    batch_new = 0
                    batch_updated = 0
                    
                    # Process each row with individual UPSERT
                    for row in transformed_data:
                        # Prepare column lists
                        cols = []
                        vals = []
                        placeholders = []
                        
                        for col, val in row.items():
                            if val is not None:  # Only include non-NULL values
                                cols.append(f'"{col}"')
                                vals.append(val)
                                placeholders.append('%s')
                        
                        if not cols:  # Skip if no columns to insert
                            continue
                            
                        # Check if record exists - USING TYPE CONVERSION HERE
                        check_sql = f'SELECT 1 FROM "{config["target_table"]}" WHERE "{primary_key}" = %s'
                        pk_value = row.get(primary_key)
                        
                        # Get target type and convert the primary key value
                        target_type = target_columns_info[primary_key]['type']
                        converted_pk = self._convert_value_for_target(pk_value, target_type)
                        
                        cursor.execute(check_sql, [converted_pk])
                        exists = cursor.fetchone() is not None
                        
                        if exists:
                            # Update existing record
                            update_parts = []
                            update_vals = []
                            
                            for col, val in row.items():
                                if col != primary_key and val is not None:
                                    update_parts.append(f'"{col}" = %s')
                                    update_vals.append(val)
                            
                            if update_parts:
                                update_sql = f'UPDATE "{config["target_table"]}" SET {", ".join(update_parts)} WHERE "{primary_key}" = %s'
                                update_vals.append(converted_pk)  # Use converted PK here too
                                cursor.execute(update_sql, update_vals)
                                batch_updated += 1
                        else:
                            # Insert new record with converted values
                            insert_vals = []
                            for col, val in zip(cols, vals):
                                col_name = col.strip('"')
                                col_type = target_columns_info[col_name]['type']
                                insert_vals.append(self._convert_value_for_target(val, col_type))
                            
                            insert_sql = f'INSERT INTO "{config["target_table"]}" ({", ".join(cols)}) VALUES ({", ".join(placeholders)})'
                            cursor.execute(insert_sql, insert_vals)
                            batch_new += 1
                        
                        batch_total += 1
                    
                    # Update stats
                    stats['total_rows'] += batch_total
                    stats['new_rows'] += batch_new
                    stats['updated_rows'] += batch_updated
                    
                    _logger.info(f"Chunk {chunk_num} results: {batch_total} total rows, {batch_new} new, {batch_updated} updated")
                
                # Update progress information in a separate transaction to avoid contention
                try:
                    # Acquire an advisory lock to prevent concurrent updates to this table's progress
                    lock_id = abs(hash(f"etl_progress_{table_config.id}")) % 2147483647  # Ensure it's a positive integer within range
                    self.env.cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (lock_id,))
                    lock_acquired = self.env.cr.fetchone()[0]
                    
                    if lock_acquired:
                        # Get current processed chunks in a separate select to avoid locking
                        current_chunks = table_config.get_processed_chunks() if hasattr(table_config, 'get_processed_chunks') else []
                        
                        # Add current chunk if not already processed
                        if chunk_num not in current_chunks:
                            current_chunks.append(chunk_num)
                            
                        # Calculate new progress
                        if hasattr(table_config, 'set_processed_chunks'):
                            table_config.set_processed_chunks(current_chunks)
                        else:
                            total_chunks = (table_config.total_records_synced or 1000) // chunk_size + 1
                            progress = min(99.0, len(current_chunks) / total_chunks * 100.0)
                            
                            # Update progress directly with lower isolation level
                            self.env.cr.execute("""
                                UPDATE etl_source_table 
                                SET processed_chunks = %s,
                                    progress_percentage = %s,
                                    last_sync_message = %s
                                WHERE id = %s
                            """, (
                                json.dumps(current_chunks),
                                progress,
                                f'Processed {len(current_chunks)} chunks ({progress:.1f}%)',
                                table_config.id
                            ))
                    else:
                        _logger.info(f"Skipped progress update for chunk {chunk_num} - couldn't acquire lock")
                except Exception as progress_error:
                    # Log but don't abort transaction
                    _logger.warning(f"Error updating progress: {str(progress_error)}")
                
                # Release savepoint
                self.env.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                
                return stats
            
            except psycopg2.errors.SerializationFailure as e:
                # Rollback to savepoint
                self.env.cr.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                
                # Increment retry counter and apply exponential backoff
                retry_count += 1
                backoff = (2 ** retry_count) * (0.1 + random.random() * 0.1)  # Exponential backoff with jitter
                
                _logger.warning(f"Serialization failure in chunk {chunk_num}, retry {retry_count}/{max_retries} " + 
                            f"after {backoff:.2f}s backoff: {str(e)}")
                
                # Wait before retrying
                time.sleep(backoff)
                
                # If this was our last retry, let the error propagate
                if retry_count >= max_retries:
                    _logger.error(f"Max retries exceeded for chunk {chunk_num}")
                    raise
                
            except Exception as e:
                # Rollback to savepoint
                try:
                    _logger.error(f"Error processing chunk {chunk_num}: {str(e)}")
                    self.env.cr.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                except Exception as rollback_error:
                    # If rollback fails, log but continue
                    _logger.error(f"Error in rollback to savepoint: {str(rollback_error)}")
                    # Attempt full transaction rollback as a last resort
                    try:
                        self.env.cr.rollback()
                        _logger.warning("Performed full transaction rollback after savepoint rollback failure")
                    except:
                        pass  # Avoid nested exception
                
                stats['error_rows'] += chunk_size
                return stats
    
    @api.model
    def _sync_with_enhanced_chunking_filtered(self, connector_service, source_db, target_db, 
                                            config, table_config, primary_key, metrics,
                                            filter_type='offset', offset=0, chunk_size=5000,  # Reduced chunk size
                                            min_id=None, max_id=None, primary_key_original=None):
        """
        Enhanced CSV chunking with filtering for parallel processing.
        Processes data batch by batch with improved memory management.
        
        Args:
            connector_service: Database connector service
            source_db: Source database connection
            target_db: Target database connection
            config: Configuration dict 
            table_config: Table configuration record
            primary_key: Primary key field name
            metrics: Dictionary for tracking performance metrics
            filter_type: Either 'id_range' or 'offset'
            offset: Starting offset (for offset filtering)
            chunk_size: Size of chunks to process
            min_id: Minimum ID value (for ID range filtering)
            max_id: Maximum ID value (for ID range filtering)
            primary_key_original: Original primary key column name (for ID range filtering)
            
        Returns:
            dict: Statistics about processed records
        """
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'unchanged_rows': 0, 'error_rows': 0}
        
        # Get source and target table names
        source_table = config['source_table']
        target_table = config['target_table']
        
        # Create unique identifier for worker
        worker_id = f"{int(time.time())}_{random.randint(1000, 9999)}"
        
        _logger.info(f"Enhanced CSV chunking for {source_table} with filter_type={filter_type}")
        
        try:
            # Get source columns information
            source_columns = connector_service.get_columns(source_db, source_table)
            
            # Get target table structure for validation
            target_columns_info = {}
            with connector_service.cursor(target_db) as cursor:
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = '{target_table}'
                    ORDER BY ordinal_position
                """)
                for row in cursor.fetchall():
                    col_name = row['column_name'].lower()
                    target_columns_info[col_name] = {
                        'type': row['data_type'].lower(),
                        'nullable': row['is_nullable'] == 'YES'
                    }
            
            # Create column mappings
            column_map = {}
            target_columns = []
            lookup_configs = {}
            
            # Extract mappings with validation
            for source_col, mapping in config['mappings'].items():
                original_source_col = source_columns.get(source_col.lower())
                if not original_source_col:
                    _logger.warning(f"Source column '{source_col}' not found in actual table columns")
                    continue
                    
                target_col = mapping.get('target', '').lower()
                if not target_col:
                    _logger.warning(f"No target column defined for source column '{source_col}'")
                    continue
                    
                # Validate target column exists in target table
                if target_col not in target_columns_info:
                    _logger.warning(f"Target column '{target_col}' not found in target table schema")
                    continue
                    
                # Store valid mapping
                column_map[original_source_col] = target_col
                target_columns.append(target_col)
                
                # Store lookup configurations if needed
                if mapping.get('type') == 'lookup':
                    lookup_configs[original_source_col] = {
                        'table': mapping.get('lookup_table'),
                        'key_col': mapping.get('lookup_key'),
                        'value_col': mapping.get('lookup_value'),
                        'target_col': target_col
                    }
            
            # Create a tx manager for transaction safety
            tx_manager = self.env['etl.transaction']
            
            # Process a single batch with the given filter parameters
            batch_stats = None
            
            # Use transaction context for safety
            with tx_manager.transaction_context(
                name=f"csv_chunk_{worker_id}",
                retry_count=3  # Allow retries for transient errors
            ) as tx:
                batch_stats = self._process_csv_batch_with_direct_inserts(
                    connector_service, source_db, target_db,
                    config, table_config, primary_key,
                    0, offset, chunk_size,
                    metrics, 
                    where_clause="" if filter_type != 'id_range' else f'WHERE "{primary_key_original}" >= %s AND "{primary_key_original}" < %s',
                    where_params=[] if filter_type != 'id_range' else [min_id, max_id],
                    column_map=column_map, 
                    target_columns=target_columns, 
                    target_columns_info=target_columns_info,
                    lookup_configs=lookup_configs
                )
                
                # Update overall stats
                for key in stats:
                    if key in batch_stats:
                        stats[key] += batch_stats[key]
                
                # Explicitly clean up after batch processing
                # (helps with memory management)
                batch_stats = None
                
            # Force explicit garbage collection
            gc.collect()
            
            return stats
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in enhanced CSV chunking: {error_message}")
            _logger.error(traceback.format_exc())
            stats['error_rows'] += 1
            return stats

    @api.model
    def _process_csv_batch_with_direct_inserts(self, connector_service, source_db, target_db, 
                                                config, table_config, primary_key,
                                                batch_num, batch_offset, batch_size,
                                                metrics, where_clause="", where_params=None,
                                                column_map=None, target_columns=None, 
                                                target_columns_info=None, lookup_configs=None):
        """
        Process a batch with direct database inserts/updates with improved memory management.
        
        Args:
            connector_service: Database connector service
            source_db: Source database connection
            target_db: Target database connection
            config: Configuration dict
            table_config: Table configuration record
            primary_key: Primary key field name
            batch_num: Batch number
            batch_offset: Offset for this batch
            batch_size: Size of this batch
            metrics: Metrics dictionary
            where_clause: Optional WHERE clause for filtering
            where_params: Parameters for WHERE clause
            column_map: Mapping of source to target columns
            target_columns: List of target columns
            target_columns_info: Information about target columns
            lookup_configs: Lookup configurations
            
        Returns:
            dict: Batch statistics
        """
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'unchanged_rows': 0, 'error_rows': 0}
        where_params = where_params or []
        
        # Get source and target table names
        source_table = config['source_table']
        target_table = config['target_table']
        
        try:
            # Build SELECT clause with column mappings
            select_parts = []
            for src_col, tgt_col in column_map.items():
                select_parts.append(f'"{src_col}" AS "{tgt_col}"')
            
            # Build query with WHERE clause if provided
            limit_offset = f"LIMIT {batch_size}" + (f" OFFSET {batch_offset}" if batch_offset > 0 else "")
            
            if where_clause:
                query = f"""
                    SELECT {", ".join(select_parts)} 
                    FROM "{source_table}" 
                    {where_clause}
                    ORDER BY "{primary_key}" 
                    {limit_offset}
                """
            else:
                query = f"""
                    SELECT {", ".join(select_parts)} 
                    FROM "{source_table}" 
                    ORDER BY "{primary_key}" 
                    {limit_offset}
                """
            
            # Start timer for DB query
            db_query_start = time.time()
            
            # Get batch data directly with query
            result = connector_service.execute_query(source_db, query, where_params)
            row_count = len(result)
            stats['total_rows'] = row_count
            
            metrics['db_query_time'] += time.time() - db_query_start
            
            if row_count == 0:
                return stats
            
            # Log query performance
            _logger.info(f"Retrieved {row_count} rows in {time.time() - db_query_start:.2f}s")
            
            # Save a sample row for debugging
            if result and len(result) > 0:
                _logger.info(f"Sample source row: {result[0]}")
            
            # Apply transformations in smaller sub-batches to manage memory better
            transform_start = time.time()
            
            # Maximum number of rows to process in a single update operation
            max_update_batch = 200  # Process very small batches at a time
            
            # Process the result in smaller sub-batches
            for sub_batch_start in range(0, row_count, max_update_batch):
                sub_batch_end = min(sub_batch_start + max_update_batch, row_count)
                sub_batch = result[sub_batch_start:sub_batch_end]
                
                # Transform this sub-batch
                transformed_data = []
                
                for row in sub_batch:
                    # Create transformed row with proper type conversion
                    transformed_row = {}
                    
                    # Apply direct mappings
                    for src_col, tgt_col in column_map.items():
                        if src_col in lookup_configs:
                            continue  # Handle lookups separately
                        
                        value = row.get(tgt_col)  # Key is already mapped in the query
                        
                        # Convert type if needed
                        if tgt_col in target_columns_info:
                            target_type = target_columns_info[tgt_col]['type']
                            nullable = target_columns_info[tgt_col]['nullable']
                            transformed_row[tgt_col] = self._convert_field_value(value, target_type, nullable)
                        else:
                            transformed_row[tgt_col] = value
                    
                    # Apply lookups
                    lookup_start = time.time()
                    for src_col, lookup_config in lookup_configs.items():
                        if src_col not in column_map:
                            continue
                        
                        # The key already has the target column name in the result
                        lookup_key = row.get(column_map[src_col])
                        if lookup_key is None:
                            continue
                        
                        target_col = lookup_config['target_col']
                        
                        # Perform lookup
                        lookup_value = self._lookup_value(
                            connector_service,
                            target_db,
                            lookup_config['table'],
                            lookup_config['key_col'],
                            lookup_config['value_col'],
                            str(lookup_key)
                        )
                        
                        # Convert lookup value type if needed
                        if target_col in target_columns_info:
                            target_type = target_columns_info[target_col]['type']
                            nullable = target_columns_info[target_col]['nullable']
                            transformed_row[target_col] = self._convert_field_value(lookup_value, target_type, nullable)
                        else:
                            transformed_row[target_col] = lookup_value
                    
                    metrics['lookup_time'] += time.time() - lookup_start
                    
                    # Add to transformed data
                    if transformed_row:
                        transformed_data.append(transformed_row)
                
                # Insert/update this sub-batch of transformed records
                sub_batch_stats = self._update_transformed_batch(
                    connector_service, target_db, target_table, primary_key, transformed_data
                )
                
                # Update overall batch stats
                for key in sub_batch_stats:
                    if key in stats:
                        stats[key] += sub_batch_stats[key]
                
                # Clear references to help garbage collection
                transformed_data = None
                sub_batch = None
                
                # Force garbage collection after each sub-batch
                gc.collect()
            
            metrics['transform_time'] += time.time() - transform_start
            
            # Clear result reference to free memory
            result = None
            
            # Log final stats
            _logger.info(f"Processed batch: {stats['total_rows']} rows "
                        f"({stats['new_rows']} new, {stats['updated_rows']} updated)")
            
            return stats
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error processing CSV batch: {error_message}")
            _logger.error(traceback.format_exc())
            stats['error_rows'] += batch_size
            return stats


    @api.model
    def _update_transformed_batch(self, connector_service, target_db, target_table, primary_key, transformed_data):
        """
        Update a batch of transformed records in the target database with optimized
        handling for inserts vs updates.
        """
        stats = {'new_rows': 0, 'updated_rows': 0}
        
        if not transformed_data:
            return stats
        
        try:
            with connector_service.cursor(target_db) as cursor:
                # First, group records by whether they exist or not
                # This avoids repetitive existence checks
                existing_pks = []
                if transformed_data and primary_key in transformed_data[0]:
                    # Extract all primary keys
                    all_pks = [row.get(primary_key) for row in transformed_data if primary_key in row]
                    
                    # Check which ones exist in a single batch query
                    if all_pks:
                        # Gather primary keys into batches of 1000 (to avoid too large SQL params)
                        MAX_PKS_PER_QUERY = 1000
                        for i in range(0, len(all_pks), MAX_PKS_PER_QUERY):
                            batch_pks = all_pks[i:i+MAX_PKS_PER_QUERY]
                            placeholders = ', '.join(['%s'] * len(batch_pks))
                            check_sql = f'SELECT "{primary_key}" FROM "{target_table}" WHERE "{primary_key}" IN ({placeholders})'
                            cursor.execute(check_sql, batch_pks)
                            existing_pks.extend([row[primary_key] for row in cursor.fetchall()])
                
                # Group records into new vs update
                rows_to_insert = []
                rows_to_update = []
                
                for row in transformed_data:
                    pk_value = row.get(primary_key)
                    if not pk_value:
                        continue
                        
                    if pk_value in existing_pks:
                        rows_to_update.append(row)
                    else:
                        rows_to_insert.append(row)
                
                # Process inserts in bulk if possible
                if rows_to_insert:
                    # Get common columns from first row
                    if len(rows_to_insert) > 0:
                        cols = list(rows_to_insert[0].keys())
                        
                        # Prepare bulk insert
                        col_str = ', '.join([f'"{col}"' for col in cols])
                        placeholder_str = ', '.join(['%s'] * len(cols))
                        insert_sql = f'INSERT INTO "{target_table}" ({col_str}) VALUES ({placeholder_str})'
                        
                        # Process in small batches to avoid memory issues
                        for i in range(0, len(rows_to_insert), 100):
                            batch = rows_to_insert[i:i+100]
                            
                            # Extract values in the correct order
                            values = []
                            for row in batch:
                                row_vals = []
                                for col in cols:
                                    row_vals.append(row.get(col))
                                values.append(tuple(row_vals))
                            
                            # Execute bulk insert
                            cursor.executemany(insert_sql, values)
                            stats['new_rows'] += len(batch)
                
                # Process updates
                for row in rows_to_update:
                    # Skip if no primary key
                    if primary_key not in row:
                        continue
                        
                    update_parts = []
                    update_vals = []
                    
                    for col, val in row.items():
                        if col != primary_key:
                            update_parts.append(f'"{col}" = %s')
                            update_vals.append(val)
                    
                    # Only update if there are fields to update
                    if update_parts:
                        update_sql = f'UPDATE "{target_table}" SET {", ".join(update_parts)} WHERE "{primary_key}" = %s'
                        update_vals.append(row[primary_key])
                        cursor.execute(update_sql, update_vals)
                        stats['updated_rows'] += 1
            
            return stats
            
        except Exception as e:
            _logger.error(f"Error updating transformed batch: {str(e)}")
            _logger.error(traceback.format_exc())
            # Return empty stats on error - caller should handle this
            return {'new_rows': 0, 'updated_rows': 0}
        
    def _convert_value_for_target(self, value, target_type, nullable=True):
        """Convert a value to match the target column type"""
        if value is None:
            return None
            
        try:
            # Handle string conversions
            if target_type in ('character varying', 'varchar', 'text', 'char', 'character'):
                return str(value) if value is not None else None
            
            # Handle numeric conversions
            elif target_type in ('integer', 'bigint', 'smallint', 'int'):
                if isinstance(value, (int, float)):
                    return int(value)
                elif isinstance(value, str) and value.strip():
                    return int(float(value.strip()))
                return None
                
            # Handle decimal/numeric conversions
            elif target_type in ('numeric', 'decimal', 'real', 'double precision', 'float'):
                if isinstance(value, (int, float, Decimal)):
                    return float(value)
                elif isinstance(value, str) and value.strip():
                    return float(value.strip())
                return None
                
            # Handle boolean conversions
            elif target_type in ('boolean', 'bool'):
                if isinstance(value, bool):
                    return value
                elif isinstance(value, str):
                    return value.lower() in ('true', 't', 'yes', 'y', '1')
                elif isinstance(value, (int, float)):
                    return bool(value)
                return None
                
            # Handle date conversions
            elif target_type == 'date':
                if isinstance(value, datetime):
                    return value.date()
                elif isinstance(value, date):
                    return value
                return value
                
            # Handle timestamp conversions
            elif 'timestamp' in target_type:
                if isinstance(value, date) and not isinstance(value, datetime):
                    return datetime.combine(value, datetime.min.time())
                return value
                
            # Default: return as is
            return value
            
        except Exception as e:
            _logger.warning(f"Type conversion error: {str(e)}")
            return None if nullable else value
    
    @api.model
    def _save_job_state_with_retry(self, table_config, state):
        """Save job state with retry logic to handle concurrency issues"""
        max_retries = 5
        retry_count = 0
        retry_delay = 0.5  # seconds
        
        while retry_count < max_retries:
            try:
                state_key = f"etl.job_state.{table_config.id}"
                state_json = json.dumps(state)
                
                # Use a separate cursor for this operation to avoid affecting the main transaction
                with self.env.registry.cursor() as cr:
                    try:
                        # Try to update existing record with a FOR UPDATE clause to lock the row
                        cr.execute("""
                            UPDATE ir_config_parameter
                            SET value = %s,
                                write_date = NOW(),
                                write_uid = %s
                            WHERE key = %s
                        """, (state_json, self.env.uid, state_key))
                        
                        # If no record was updated, insert a new one
                        if cr.rowcount == 0:
                            cr.execute("""
                                INSERT INTO ir_config_parameter (key, value, create_date, write_date, create_uid, write_uid)
                                VALUES (%s, %s, NOW(), NOW(), %s, %s)
                            """, (state_key, state_json, self.env.uid, self.env.uid))
                        
                        # Commit the changes
                        cr.commit()
                        return True
                    except Exception as e:
                        cr.rollback()
                        _logger.warning(f"Error saving job state (attempt {retry_count+1}): {str(e)}")
                        # Continue to retry logic
            except Exception as e:
                _logger.warning(f"Error in job state transaction (attempt {retry_count+1}): {str(e)}")
                # Continue to retry logic
            
            # Increment retry counter and delay with exponential backoff
            retry_count += 1
            if retry_count < max_retries:
                # Add some random jitter to avoid thundering herd problem
                jitter = random.random() * 0.5  # 0-0.5 seconds of random jitter
                sleep_time = retry_delay * (2 ** (retry_count - 1)) + jitter
                _logger.info(f"Retrying job state save in {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
        
        _logger.error(f"Failed to save job state after {max_retries} attempts")
        return False

    @api.model
    def _clear_job_state_safely(self, table_config):
        """Safely clear job state with retry logic"""
        # Simply set an empty state with retry
        return self._save_job_state_with_retry(table_config, {})

    @api.model
    def sync_data_with_csv(self, table_config):
        """
        Two-phase ETL process using CSV files as intermediate storage.
        With improved concurrency handling to avoid transaction issues.
        """
        start_time = time.time()
        
        try:
            # Get connector service
            connector_service = self.env['etl.database.connector.service']
            source_db = table_config.source_db_connection_id
            target_db = table_config.target_db_connection_id
            
            # Get configuration
            config = table_config.get_config_json()
            source_table = config['source_table']
            target_table = config['target_table']
            primary_key = config['primary_key']
            
            _logger.info(f"Starting CSV-based sync for table {source_table}")
            
            # Create sync log
            sync_log = self.env['etl.sync.log'].create({
                'table_id': table_config.id,
                'start_time': fields.Datetime.now(),
                'status': 'running'
            })
            
            # Update table status
            table_config.write({
                'job_status': 'started',
                'last_sync_status': 'running',
                'last_sync_message': f'Starting CSV-based ETL process for {source_table}',
                'progress_percentage': 0
            })
            
            # Get total row count
            try:
                total_rows = connector_service.get_table_count(source_db, source_table)
                _logger.info(f"Table {source_table} has {total_rows} rows")
            except Exception as e:
                _logger.warning(f"Could not determine table size: {str(e)}")
                total_rows = 0
            
            # Determine batch size - use smaller batches for large tables
            if total_rows > 5000000:  # Very large table
                batch_size = 10000
            elif total_rows > 1000000:  # Large table
                batch_size = 20000
            else:
                batch_size = 50000
            
            # Calculate number of batches
            if total_rows > 0:
                total_batches = (total_rows + batch_size - 1) // batch_size
            else:
                total_batches = 1
            
            # Create directory for CSV files
            csv_dir = self._get_csv_directory(table_config)
            os.makedirs(csv_dir, exist_ok=True)
            
            # Initialize job state with clean state - safely using retry logic
            self._save_job_state_with_retry(table_config, {
                'phase': 'extract',
                'total_rows': total_rows,
                'total_batches': total_batches,
                'batch_size': batch_size,
                'start_time': time.time(),
                'csv_files': [],
                'completed_batches': [],
                'max_concurrent_batches': 5,  # Process 5 batches at a time
                'next_batch_to_queue': 0
            })
            
            # Start the extraction process with proper monitoring
            return self._start_csv_extraction_jobs(table_config, total_batches, batch_size)
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in CSV-based sync: {error_message}")
            _logger.error(traceback.format_exc())
            
            # Try to update table status
            try:
                table_config.write({
                    'job_status': 'failed',
                    'last_sync_status': 'failed',
                    'last_sync_message': f'Error in CSV-based sync: {error_message}'
                })
            except:
                pass
            
            # Return error info
            return {
                'status': 'failed',
                'error': error_message,
                'execution_time': time.time() - start_time
            }
            
    @api.model
    def _start_csv_extraction_jobs(self, table_config, total_batches, batch_size):
        """
        Start extraction jobs for each batch with improved job management.
        
        Args:
            table_config: Table configuration record
            total_batches: Total number of batches to process
            batch_size: Size of each batch
            
        Returns:
            dict: Status information
        """
        _logger.info(f"Starting CSV extraction process with {total_batches} batches for table {table_config.name}")
        
        # Get job state
        job_state = self._get_job_state(table_config)
        
        # Process 5 batches at a time initially as specified
        max_concurrent_batches = 5
        
        # Create coordinator job to monitor extraction progress
        coordinator_job = self.with_delay(
            description=f"CSV Extraction Coordinator: {table_config.name}",
            channel="etl",
            priority=5
        )._monitor_extraction_progress(table_config.id)
        
        # Queue the first group of extraction batches with a little delay between them
        first_group_size = min(max_concurrent_batches, total_batches)
        _logger.info(f"Queueing first {first_group_size} extraction batches")
        
        # Save the batching strategy in job state
        self._save_job_state(table_config, {
            'phase': 'extract',
            'total_rows': job_state.get('total_rows', 0),
            'total_batches': total_batches,
            'batch_size': batch_size,
            'max_concurrent_batches': max_concurrent_batches,
            'extract_time': 0,
            'start_time': time.time(),
            'next_batch_to_queue': first_group_size,  # Track which batch to queue next
            'csv_files': [],
            'completed_batches': []
        })
        
        # Queue the first group of extraction batches
        for batch_num in range(first_group_size):
            # Calculate offset
            offset = batch_num * batch_size
            
            # Add a small delay between jobs
            delay_seconds = batch_num * 2  # 2 seconds between jobs
            
            _logger.info(f"Queueing extraction batch {batch_num+1}/{total_batches}")
            
            # Use a unique identity key to prevent duplicate jobs
            identity_key = f"extract_batch_{table_config.id}_{batch_num}"
            
            self.with_delay(
                description=f"CSV Extraction Batch {batch_num+1}/{total_batches}: {table_config.name}",
                channel="etl",
                priority=10,
                identity_key=identity_key,
                eta=datetime.now() + timedelta(seconds=delay_seconds)
            )._extract_batch_to_csv_independent(table_config.id, 0, batch_num, offset, batch_size)
        
        # Update table status
        table_config.write({
            'job_status': 'started',
            'last_sync_message': f'Started extraction process for {total_batches} batches (processing in groups of {max_concurrent_batches})'
        })
        
        return {
            'status': 'extraction_started',
            'total_batches': total_batches,
            'coordinator_job': coordinator_job.uuid
        }
    
    @api.model
    def _force_execute_pending_jobs(self, table_config_id):
        """Force execute pending jobs if they're stuck"""
        _logger.info(f"Checking for stuck jobs for table {table_config_id}")
        
        # Find pending jobs for this table
        self.env.cr.execute("""
            SELECT id, uuid, name 
            FROM queue_job 
            WHERE state = 'pending' 
            AND name LIKE %s
            ORDER BY id
            LIMIT 5
        """, (f"%{table_config_id}%",))
        
        stuck_jobs = self.env.cr.dictfetchall()
        
        if not stuck_jobs:
            _logger.info("No stuck jobs found")
            return False
        
        _logger.info(f"Found {len(stuck_jobs)} stuck jobs")
        
        # Force them to be requeued by updating state
        for job in stuck_jobs:
            _logger.info(f"Forcing job {job['name']} (UUID: {job['uuid']}) to be requeued")
            
            # Update job state to enqueued
            self.env.cr.execute("""
                UPDATE queue_job 
                SET state = 'enqueued', date_enqueued = NOW() 
                WHERE id = %s
            """, (job['id'],))
        
        self.env.cr.commit()
        return True

    @api.model
    def _extract_batch_to_csv(self, table_config_id, batch_num, offset, batch_size):
        """
        Queue Job to extract a batch of data to CSV file
        This runs as a background job through queue_job
        """
        _logger.info(f"STARTING extraction batch {batch_num} at offset {offset}")
        
        table_config = self.env['etl.source.table'].browse(table_config_id)
        if not table_config.exists():
            _logger.error(f"Table config {table_config_id} not found")
            return {'error': 'Table config not found'}
        
        # Get job state
        job_state = self._get_job_state(table_config)
        completed_batches = job_state.get('completed_batches', [])
        
        # Skip if already processed
        if batch_num in completed_batches:
            _logger.info(f"Batch {batch_num} already processed, skipping")
            return {'status': 'already_processed', 'batch_num': batch_num}
        
        try:
            _logger.info(f"Processing extraction of batch {batch_num} at offset {offset}")
            start_time = time.time()
            
            # Get connector service and config
            connector_service = self.env['etl.database.connector.service']
            source_db = table_config.source_db_connection_id
            config = table_config.get_config_json()
            source_table = config['source_table']
            primary_key = config['primary_key']
            
            # Generate CSV filename
            csv_dir = self._get_csv_directory(table_config)
            csv_filename = os.path.join(csv_dir, f"{source_table}_batch_{batch_num}.csv")
            
            # Make sure directory exists
            os.makedirs(csv_dir, exist_ok=True)
            
            # Extract and transform batch to CSV
            _logger.info(f"Starting data extraction for batch {batch_num}")
            rows_processed = self._extract_transform_to_csv(
                connector_service, source_db, config, primary_key,
                offset, batch_size, csv_filename
            )
            
            _logger.info(f"Extracted {rows_processed} rows to {csv_filename}")
            
            if rows_processed > 0:
                # Get current job state - need to retrieve again as it might have changed
                job_state = self._get_job_state(table_config)
                completed_batches = job_state.get('completed_batches', [])
                csv_files = job_state.get('csv_files', [])
                
                # Update job state with this batch's info
                if batch_num not in completed_batches:
                    completed_batches.append(batch_num)
                if csv_filename not in csv_files:
                    csv_files.append(csv_filename)
                    
                _logger.info(f"Updating job state for batch {batch_num}, completed batches: {len(completed_batches)}")
                
                # Save with updated lists
                self._save_job_state(table_config, {
                    'phase': 'extract',
                    'total_rows': job_state.get('total_rows', 0),
                    'total_batches': job_state.get('total_batches', 0),
                    'batch_size': job_state.get('batch_size', batch_size),
                    'max_concurrent_batches': job_state.get('max_concurrent_batches', 5),
                    'next_batch_to_queue': job_state.get('next_batch_to_queue', 0),
                    'csv_files': csv_files,
                    'completed_batches': completed_batches,
                    'start_time': job_state.get('start_time', time.time())
                })
                
                _logger.info(f"COMPLETED extraction batch {batch_num} with {rows_processed} rows in {time.time() - start_time:.2f}s")
                
                # Commit to ensure our changes are saved
                self.env.cr.commit()
                
                return {
                    'status': 'success',
                    'batch_num': batch_num,
                    'rows_processed': rows_processed,
                    'csv_filename': csv_filename,
                    'execution_time': time.time() - start_time
                }
            else:
                _logger.info(f"No rows processed for batch {batch_num}")
                
                # Even with no rows, mark as complete to avoid stuck job
                job_state = self._get_job_state(table_config)
                completed_batches = job_state.get('completed_batches', [])
                
                if batch_num not in completed_batches:
                    completed_batches.append(batch_num)
                
                self._save_job_state(table_config, {
                    'phase': 'extract',
                    'total_rows': job_state.get('total_rows', 0),
                    'total_batches': job_state.get('total_batches', 0),
                    'batch_size': job_state.get('batch_size', batch_size),
                    'max_concurrent_batches': job_state.get('max_concurrent_batches', 5),
                    'next_batch_to_queue': job_state.get('next_batch_to_queue', 0),
                    'csv_files': job_state.get('csv_files', []),
                    'completed_batches': completed_batches,
                    'start_time': job_state.get('start_time', time.time())
                })
                
                # Commit to ensure our changes are saved
                self.env.cr.commit()
                
                return {
                    'status': 'no_data',
                    'batch_num': batch_num
                }
                
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error extracting batch {batch_num} to CSV: {error_message}")
            _logger.error(traceback.format_exc())
            
            # Try to mark job as failed in job state
            try:
                job_state = self._get_job_state(table_config)
                failed_batches = job_state.get('failed_batches', [])
                failed_batches.append(batch_num)
                
                self._save_job_state(table_config, {
                    'phase': 'extract',
                    'failed_batches': failed_batches,
                    'last_error': error_message,
                    'total_rows': job_state.get('total_rows', 0),
                    'total_batches': job_state.get('total_batches', 0),
                    'batch_size': job_state.get('batch_size', batch_size),
                    'max_concurrent_batches': job_state.get('max_concurrent_batches', 5),
                    'next_batch_to_queue': job_state.get('next_batch_to_queue', 0),
                    'csv_files': job_state.get('csv_files', []),
                    'completed_batches': job_state.get('completed_batches', []),
                    'start_time': job_state.get('start_time', time.time())
                })
                
                # Commit to ensure our changes are saved
                self.env.cr.commit()
            except Exception as inner_e:
                _logger.error(f"Error updating job state after batch failure: {str(inner_e)}")
            
            return {
                'status': 'failed',
                'batch_num': batch_num,
                'error': error_message
            }

    @api.model
    def _monitor_extraction_progress(self, table_config_id):
        """
        Monitor extraction progress and queue additional batch jobs as needed.
        This runs as a separate queue job, maintaining optimal batch parallelism.
        
        Args:
            table_config_id: ID of the ETL table config
        """
        # Use separate cursor to avoid transaction conflicts
        with self.env.registry.cursor() as new_cr:
            env = api.Environment(new_cr, self.env.uid, self.env.context)
            
            # Wait interval between checks
            check_interval_seconds = 15
            stalled_check_count = 0
            
            table_config = env['etl.source.table'].browse(table_config_id)
            if not table_config.exists():
                _logger.error(f"Table config {table_config_id} not found")
                return {'error': 'Table config not found'}
                
            # Get current state
            job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
            
            # Handle job cancellation
            if table_config.job_status == 'cancelled':
                _logger.info(f"ETL job for {table_config.name} was cancelled")
                return {'status': 'cancelled'}
                
            # Validate we're still in extraction phase
            if job_state.get('phase') != 'extract':
                _logger.info(f"ETL job for {table_config.name} is no longer in extraction phase")
                return {'status': 'phase_changed'}
                
            # Get completion status with validation and normalization
            completed_batches = job_state.get('completed_batches', [])
            
            # Ensure completed_batches is a valid list
            if not isinstance(completed_batches, list):
                completed_batches = []
                job_state['completed_batches'] = completed_batches
                self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                
            prev_completed_count = len(completed_batches)
            
            # Ensure we have valid total_batches and batch_size
            total_rows = job_state.get('total_rows', 0)
            if total_rows <= 0 and table_config.total_records_synced:
                total_rows = table_config.total_records_synced
                job_state['total_rows'] = total_rows
                
            batch_size = job_state.get('batch_size', 10000)
            total_batches = job_state.get('total_batches', 0)
            
            # Fix or validate total_batches
            if total_batches <= 0 and total_rows > 0:
                total_batches = (total_rows + batch_size - 1) // batch_size
                job_state['total_batches'] = total_batches
                _logger.info(f"Recalculated total_batches as {total_batches} from total_rows={total_rows} and batch_size={batch_size}")
                self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
            elif total_batches <= 0:
                # If we still can't calculate, use value from logs
                total_batches = 507
                job_state['total_batches'] = total_batches
                _logger.info(f"Setting fixed total_batches={total_batches}")
                self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
            
            # Get batch queueing information - CRITICAL FIX - initialize if missing
            next_batch_to_queue = job_state.get('next_batch_to_queue', 0)
            if next_batch_to_queue == 0 and len(completed_batches) > 0:
                # If next_batch_to_queue is still 0 but we have completed batches,
                # set it to at least the highest completed batch + 1
                next_batch_to_queue = max(completed_batches) + 1
                job_state['next_batch_to_queue'] = next_batch_to_queue
                _logger.warning(f"Fixed next_batch_to_queue to {next_batch_to_queue} based on completed batches")
                self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                
            max_concurrent_batches = job_state.get('max_concurrent_batches', 5)  # Default to 5
            
            # CRITICAL: Check for discrepancies in the job state
            csv_files = job_state.get('csv_files', [])
            actual_completed = []
            
            # If CSV files exist but aren't in completed_batches, fix tracking
            for csv_file in csv_files:
                try:
                    # Extract batch number from filename
                    filename = os.path.basename(csv_file)
                    if '_batch_' in filename:
                        batch_str = filename.split('_batch_')[1].split('.')[0]
                        batch_num = int(batch_str)
                        if batch_num not in completed_batches:
                            actual_completed.append(batch_num)
                except Exception as e:
                    _logger.warning(f"Error parsing batch number from filename {csv_file}: {str(e)}")
            
            # If we found batches that completed but aren't tracked, update the tracking
            if actual_completed:
                _logger.warning(f"Found {len(actual_completed)} completed batches that weren't in tracking: {actual_completed}")
                for batch_num in actual_completed:
                    if batch_num not in completed_batches:
                        completed_batches.append(batch_num)
                
                # Save corrected state
                job_state['completed_batches'] = completed_batches
                self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
            
            # Calculate progress with safety check
            completion_percentage = (len(completed_batches) / total_batches) * 100 if total_batches > 0 else 0
            _logger.info(f"Extraction progress: {len(completed_batches)}/{total_batches} batches ({completion_percentage:.1f}%)")
            _logger.info(f"Completed batches: {completed_batches}")
            
            # Update table status
            table_config.write({
                'progress_percentage': min(50, completion_percentage / 2),  # 50% max for extraction phase
                'last_sync_message': f'Extraction in progress: {len(completed_batches)}/{total_batches} batches ({completion_percentage:.1f}%)'
            })
            
            # Get active pending jobs count - crucial for determining if we need to queue more batches
            pending_jobs = self._count_active_extraction_jobs(new_cr, table_config.name)
            _logger.info(f"Found {pending_jobs} pending/running extraction jobs")
            
            # CRITICAL FIX: Always queue more batches if we have capacity and haven't reached the end
            # This ensures we maintain the desired level of parallel processing
            if len(completed_batches) < total_batches:
                # Calculate how many additional batches we can queue
                # We want to maintain max_concurrent_batches running at all times
                available_slots = max_concurrent_batches - pending_jobs
                
                if available_slots > 0 and next_batch_to_queue < total_batches:
                    batches_to_queue = min(available_slots, total_batches - next_batch_to_queue)
                    
                    if batches_to_queue > 0:
                        _logger.info(f"QUEUEING {batches_to_queue} NEW EXTRACTION BATCHES - maintaining {max_concurrent_batches} concurrent jobs")
                        
                        for i in range(batches_to_queue):
                            batch_num = next_batch_to_queue + i
                            offset = batch_num * batch_size
                            
                            # Make sure we're not queuing a batch that's already completed
                            if batch_num in completed_batches:
                                _logger.warning(f"Skipping already completed batch {batch_num}")
                                continue
                                
                            # Add delay between jobs
                            delay_seconds = i * 5  # 5 seconds between jobs
                            
                            # Use a unique identity key
                            identity_key = f"extract_batch_{table_config_id}_{batch_num}_{int(time.time())}"
                            
                            _logger.info(f"Queueing extraction batch {batch_num+1}/{total_batches}")
                            
                            env['etl.fast.sync.postgres'].with_delay(
                                description=f"CSV Extraction Batch {batch_num+1}/{total_batches}: {table_config.name}",
                                channel="etl",
                                priority=10,
                                identity_key=identity_key,
                                eta=datetime.now() + timedelta(seconds=delay_seconds)
                            )._extract_batch_to_csv_independent(table_config_id, 0, batch_num, offset, batch_size)
                        
                        # Update next batch to queue
                        job_state['next_batch_to_queue'] = next_batch_to_queue + batches_to_queue
                        self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                
            # Check if all batches are complete
            if len(completed_batches) >= total_batches:
                _logger.info(f"All {total_batches} extraction batches complete. Transitioning to loading phase.")
                
                # Calculate total extraction time
                extraction_time = time.time() - job_state.get('start_time', time.time())
                
                # Get all CSV files
                csv_files = job_state.get('csv_files', [])
                
                # Transition to loading phase
                job_state = {
                    'phase': 'load',
                    'csv_files': csv_files,
                    'all_files': csv_files,  # Ensure both lists are set for compatibility
                    'extract_time': extraction_time,
                    'start_time': time.time(),
                    'total_rows': job_state.get('total_rows', 0),
                    'processed_files': [],  # Initialize empty processed files list
                    'next_file_to_queue': 0  # Initialize file queue counter
                }
                self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                
                # Start loading phase
                load_status = self._start_csv_loading_jobs_with_cursor(new_cr, table_config_id, csv_files)
                
                # Commit to ensure phase change is saved
                new_cr.commit()
                
                return {
                    'status': 'complete',
                    'loaded': len(csv_files),
                    'extraction_time': extraction_time,
                    'loading_status': load_status
                }
            
            # Not complete yet, create a new monitoring job
            env['etl.fast.sync.postgres'].with_delay(
                description=f"CSV Extraction Monitor: {table_config.name}",
                channel="etl_monitor",
                priority=5,
                eta=datetime.now() + timedelta(seconds=check_interval_seconds)
            )._monitor_extraction_progress(table_config_id)
            
            # Commit changes
            new_cr.commit()
            
            return {
                'status': 'monitoring_continued',
                'completed': len(completed_batches),
                'total': total_batches,
                'progress': completion_percentage
            }

    @api.model
    def _count_active_extraction_jobs(self, cursor, table_name):
        """
        Count how many extraction jobs are currently active for a given table.
        
        Args:
            cursor: Database cursor
            table_name: Name of the table being processed
            
        Returns:
            int: Number of active extraction jobs
        """
        try:
            # Query active jobs for CSV extraction
            cursor.execute("""
                SELECT COUNT(*) FROM queue_job
                WHERE state IN ('pending', 'enqueued', 'started') 
                AND name LIKE %s
                AND name LIKE %s
            """, (f"%{table_name}%", "%CSV Extraction Batch%"))
            
            count = cursor.fetchone()[0]
            return count
        
        except Exception as e:
            _logger.error(f"Error counting active extraction jobs: {str(e)}")
            return 0 
            
    @api.model
    def _get_in_progress_batches(self, cursor, table_config_id, table_name):
        """
        Helper method to identify which batches are currently in progress
        by examining active jobs in the queue_job table.
        
        Args:
            cursor: Database cursor
            table_config_id: ID of the ETL table config
            table_name: Name of the table being processed
            
        Returns:
            list: List of batch numbers that are currently in progress
        """
        in_progress_batches = []
        
        try:
            # Query active jobs for CSV extraction
            cursor.execute("""
                SELECT name FROM queue_job
                WHERE state IN ('pending', 'enqueued', 'started') 
                AND name LIKE %s
            """, (f"%CSV Extraction Batch%{table_name}%",))
            
            jobs = cursor.fetchall()
            
            # Extract batch numbers from job names
            for job in jobs:
                try:
                    # Example job name: "CSV Extraction Batch 42/507: test_ods_customers"
                    job_name = job[0]
                    if 'Extraction Batch' in job_name:
                        # Extract the batch number (before the slash)
                        batch_part = job_name.split('Batch ')[1].split('/')[0]
                        # Convert to 0-based index
                        batch_num = int(batch_part) - 1
                        in_progress_batches.append(batch_num)
                except (IndexError, ValueError) as e:
                    _logger.warning(f"Error parsing batch number from job name '{job[0]}': {str(e)}")
        
        except Exception as e:
            _logger.error(f"Error getting in-progress batches: {str(e)}")
        
        return in_progress_batches

        
    # @api.model
    # def _monitor_extraction_progress(self, table_config_id):
    #     """
    #     Monitor extraction progress and queue additional batch jobs as needed.
    #     This runs as a separate queue job.
    #     """
    #     # Use separate cursor to avoid transaction conflicts
    #     with self.env.registry.cursor() as new_cr:
    #         env = api.Environment(new_cr, self.env.uid, self.env.context)
            
    #         # Wait interval between checks
    #         check_interval_seconds = 15  # Longer interval to reduce DB load
    #         max_retries = 200           # Up to 50 minutes (200 * 15 seconds)
    #         stalled_count = 0           # Count of checks where no progress is made
            
    #         for attempt in range(max_retries):
    #             table_config = env['etl.source.table'].browse(table_config_id)
    #             if not table_config.exists():
    #                 return {'error': 'Table config not found'}
                    
    #             # Get current state
    #             job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
                
    #             # Handle job cancellation
    #             if table_config.job_status == 'cancelled':
    #                 _logger.info(f"ETL job for {table_config.name} was cancelled")
    #                 return {'status': 'cancelled'}
                    
    #             # Check if we're still in extraction phase
    #             if job_state.get('phase') != 'extract':
    #                 _logger.info(f"ETL job for {table_config.name} is no longer in extraction phase")
    #                 return {'status': 'phase_changed'}
                    
    #             # Get completion status
    #             completed_batches = job_state.get('completed_batches', [])
    #             prev_completed_count = len(completed_batches)
    #             total_batches = job_state.get('total_batches', 0)
    #             next_batch_to_queue = job_state.get('next_batch_to_queue', 0)
    #             batch_size = job_state.get('batch_size', 20000)
                
    #             # Calculate progress
    #             completion_percentage = (len(completed_batches) / total_batches) * 100 if total_batches > 0 else 0
    #             _logger.info(f"Extraction progress: {len(completed_batches)}/{total_batches} batches ({completion_percentage:.1f}%)")
                
    #             # Update table status with current progress
    #             table_config.write({
    #                 'progress_percentage': min(50, completion_percentage / 2),  # 50% max for extraction phase
    #                 'last_sync_message': f'Extraction in progress: {len(completed_batches)}/{total_batches} batches ({completion_percentage:.1f}%)'
    #             })
                
    #             # Check if we're making progress
    #             if prev_completed_count == len(completed_batches):
    #                 stalled_count += 1
    #                 _logger.info(f"No progress detected for {stalled_count} checks")
                    
    #                 # If no progress for a while, requeue any failed batches
    #                 if stalled_count >= 5:  # After 5 checks with no progress
    #                     _logger.warning(f"No progress detected for {stalled_count} checks. Checking for failed batches.")
                        
    #                     # Check for failed batches
    #                     failed_batches = job_state.get('failed_batches', [])
    #                     if failed_batches:
    #                         _logger.info(f"Found {len(failed_batches)} failed batches to requeue")
                            
    #                         # Requeue failed batches (up to 3 at a time)
    #                         batches_to_requeue = failed_batches[:3]
                            
    #                         for i, batch_num in enumerate(batches_to_requeue):
    #                             offset = batch_num * batch_size
                                
    #                             # Add delay between requeues
    #                             delay_seconds = i * 10
                                
    #                             # Use a unique identity key with timestamp to ensure it's different
    #                             identity_key = f"extract_batch_{table_config_id}_{batch_num}_retry_{int(time.time())}"
                                
    #                             _logger.info(f"Requeuing failed extraction batch {batch_num+1}/{total_batches}")
                                
    #                             self.with_delay(
    #                                 description=f"CSV Extraction Batch {batch_num+1}/{total_batches} (Retry): {table_config.name}",
    #                                 channel="etl_extraction",
    #                                 priority=10,
    #                                 identity_key=identity_key,
    #                                 eta=datetime.now() + timedelta(seconds=delay_seconds)
    #                             )._extract_batch_to_csv_independent(table_config_id, 0, batch_num, offset, batch_size)
                            
    #                         # Remove requeued batches from failed list
    #                         for batch_num in batches_to_requeue:
    #                             failed_batches.remove(batch_num)
                            
    #                         # Update job state directly
    #                         job_state['failed_batches'] = failed_batches
    #                         self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                        
    #                     # Reset stalled count after attempting recovery
    #                     stalled_count = 0
    #             else:
    #                 # We made progress, reset stalled count
    #                 stalled_count = 0
                
    #             # Queue additional batches if needed
    #             max_concurrent_batches = 10  # Maximum number of batches in-flight at once
    #             if next_batch_to_queue < total_batches:
    #                 # Calculate how many batches are currently in progress
    #                 in_progress_count = next_batch_to_queue - len(completed_batches)
                    
    #                 # Calculate how many new batches to queue
    #                 batches_to_queue = max(0, max_concurrent_batches - in_progress_count)
    #                 batches_to_queue = min(batches_to_queue, total_batches - next_batch_to_queue)
                    
    #                 if batches_to_queue > 0:
    #                     _logger.info(f"Queueing {batches_to_queue} more extraction batches")
                        
    #                     for i in range(batches_to_queue):
    #                         batch_num = next_batch_to_queue + i
    #                         offset = batch_num * batch_size
                            
    #                         # Add delay between jobs
    #                         delay_seconds = i * 10
                            
    #                         # Use a unique identity key
    #                         identity_key = f"extract_batch_{table_config_id}_{batch_num}"
                            
    #                         _logger.info(f"Queueing extraction batch {batch_num+1}/{total_batches}")
                            
    #                         self.with_delay(
    #                             description=f"CSV Extraction Batch {batch_num+1}/{total_batches}: {table_config.name}",
    #                             channel="etl_extraction",
    #                             priority=10,
    #                             identity_key=identity_key,
    #                             eta=datetime.now() + timedelta(seconds=delay_seconds)
    #                         )._extract_batch_to_csv_independent(table_config_id, 0, batch_num, offset, batch_size)
                        
    #                     # Update next batch to queue
    #                     job_state['next_batch_to_queue'] = next_batch_to_queue + batches_to_queue
    #                     self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                
    #             # Check if all batches are complete
    #             if len(completed_batches) >= total_batches:
    #                 _logger.info(f"All {total_batches} extraction batches complete. Starting loading phase.")
                    
    #                 # Transition to loading phase
    #                 csv_files = job_state.get('csv_files', [])
    #                 extraction_time = time.time() - job_state.get('start_time', time.time())
                    
    #                 # Update job state for loading phase
    #                 job_state = {
    #                     'phase': 'load',
    #                     'csv_files': csv_files,
    #                     'extract_time': extraction_time,
    #                     'start_time': time.time(),
    #                     'total_rows': job_state.get('total_rows', 0)
    #                 }
    #                 self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                    
    #                 # Commit to ensure phase change is saved
    #                 new_cr.commit()
                    
    #                 # Start loading phase with a new environment that has the updated job state
    #                 return self._start_csv_loading_jobs_with_cursor(new_cr, table_config_id, csv_files)
                
    #             # Commit to save any changes and release locks
    #             new_cr.commit()
                
    #             # Not complete yet, create a new monitoring job that will run after the interval
    #             if attempt < max_retries - 1:  # Don't create a new job on the last attempt
    #                 self.with_delay(
    #                     description=f"ETL Monitor: {table_config.name}",
    #                     channel="etl_monitor",
    #                     priority=5,
    #                     eta=datetime.now() + timedelta(seconds=check_interval_seconds)
    #                 )._monitor_extraction_progress(table_config_id)
                    
    #                 return {
    #                     'status': 'monitoring_continued',
    #                     'completed': len(completed_batches),
    #                     'total': total_batches,
    #                     'progress': completion_percentage
    #                 }
                
    #         # If we reached here, we've exceeded max retries
    #         _logger.warning(f"Extraction monitoring timed out for {table_config.name} after {max_retries} checks")
    #         return {'status': 'timeout'}

    
    @api.model
    def _check_stuck_jobs(self):
        """Cron job to check for and fix stuck ETL jobs"""
        _logger.info("Running check for stuck ETL jobs")
        
        # Find tables with in-progress ETL jobs
        tables = self.env['etl.source.table'].search([
            ('job_status', 'in', ['started', 'pending']),
            ('use_csv_mode', '=', True)
        ])
        
        for table in tables:
            # Check how long the job has been running
            if not table.last_sync_time:
                continue
                
            elapsed_time = fields.Datetime.now() - table.last_sync_time
            
            # If it's been more than 15 minutes with no updates, check for stuck jobs
            if elapsed_time.total_seconds() > 900:  # 15 minutes
                _logger.info(f"Table {table.name} has a job running for more than 15 minutes, checking for stuck jobs")
                
                # Force execute pending jobs
                self._force_execute_pending_jobs(table.id)
                
                # If it's been extremely long (over 1 hour), reset the job
                if elapsed_time.total_seconds() > 3600:  # 1 hour
                    _logger.warning(f"Job for table {table.name} has been running for over an hour. Resetting.")
                    
                    table.write({
                        'job_status': 'failed',
                        'last_sync_status': 'failed',
                        'last_sync_message': 'Job reset due to excessive runtime',
                        'progress_percentage': 0
                    })
                    
                    # Clear job state to allow a fresh start
                    self._clear_job_state(table)


    @api.model
    def _start_csv_loading_jobs(self, table_config, csv_files):
        """Start loading jobs for CSV files"""
        if not csv_files:
            _logger.warning(f"No CSV files to load for {table_config.name}")
            # Return properly formatted stats
            return {
                'total_rows': 0,
                'new_rows': 0,
                'updated_rows': 0,
                'unchanged_rows': 0,
                'error_rows': 0,
                'execution_time': 0,
                'status': 'no_files'
            }
            
        _logger.info(f"Starting loading phase with {len(csv_files)} CSV files for {table_config.name}")
        
        # Get job state
        job_state = self._get_job_state(table_config)
        processed_files = job_state.get('processed_files', [])
        
        # Filter out already processed files
        files_to_process = [f for f in csv_files if f not in processed_files]
        
        if not files_to_process:
            _logger.info(f"All CSV files already processed for {table_config.name}")
            # Clean up job state
            self._clear_job_state(table_config)
            
            # Update table status
            table_config.write({
                'job_status': 'done',
                'last_sync_status': 'success',
                'last_sync_message': f'All CSV files already processed',
                'progress_percentage': 100,
                'last_sync_time': fields.Datetime.now()
            })
            
            # Return properly formatted stats
            return {
                'total_rows': job_state.get('total_rows', 0),
                'new_rows': 0,
                'updated_rows': 0,
                'unchanged_rows': 0,
                'error_rows': 0,
                'execution_time': 0,
                'status': 'already_complete'
            }
        
        # Create coordinator job to monitor loading progress
        coordinator_job = self.with_delay(
            description=f"CSV Loading Coordinator: {table_config.name}",
            channel="etl_csv_coordinator",
            priority=5
        )._monitor_loading_progress(table_config.id)
        
        # Update table status
        table_config.write({
            'progress_percentage': 50,  # Start at 50% since extraction is complete
            'last_sync_message': f'Starting loading phase with {len(files_to_process)} CSV files'
        })
        
        # Queue loading jobs with staggered start
        # Process only 2-3 files concurrently to avoid overwhelming the database
        max_concurrent = 2
        for i, csv_file in enumerate(files_to_process):
            # Start jobs with a delay based on position in queue
            delay_group = i // max_concurrent
            seconds_delay = delay_group * 5  # 5 seconds between groups
            
            self.with_delay(
                description=f"CSV Loading File {i+1}/{len(files_to_process)}: {table_config.name}",
                channel="etl_csv_load",
                priority=10,
                eta=datetime.now() + timedelta(seconds=seconds_delay)
            )._load_csv_file_to_db(table_config.id, csv_file)
        
        # Return properly formatted stats
        return {
            'total_rows': job_state.get('total_rows', 0),
            'new_rows': 0,
            'updated_rows': 0,
            'unchanged_rows': 0,
            'error_rows': 0,
            'execution_time': 0,
            'status': 'loading_started',
            'files_to_process': len(files_to_process),
            'coordinator_job': coordinator_job.uuid
        }

    @api.model
    def _load_csv_file_to_db(self, table_config_id, csv_file):
        """
        Queue Job to load a CSV file into the database
        This runs as a background job through queue_job
        """
        table_config = self.env['etl.source.table'].browse(table_config_id)
        if not table_config.exists():
            return {'error': 'Table config not found'}
        
        # Get job state
        job_state = self._get_job_state(table_config)
        processed_files = job_state.get('processed_files', [])
        
        # Skip if already processed
        if csv_file in processed_files:
            _logger.info(f"File {csv_file} already processed, skipping")
            return {'status': 'already_processed', 'file': csv_file}
        
        try:
            start_time = time.time()
            
            # Get connector service and config
            connector_service = self.env['etl.database.connector.service']
            target_db = table_config.target_db_connection_id
            config = table_config.get_config_json()
            target_table = config['target_table']
            primary_key = config['primary_key']
            
            # Check if the file exists
            if not os.path.exists(csv_file):
                _logger.warning(f"CSV file not found: {csv_file}")
                return {'status': 'file_not_found', 'file': csv_file}
            
            # Create transaction manager
            tx_manager = self.env['etl.transaction']
            
            # Load the CSV file
            stats = self._load_csv_file_to_database(
                connector_service, target_db, target_table, primary_key, csv_file, tx_manager
            )
            
            # Update job state with processed file
            processed_files.append(csv_file)
            self._save_job_state(table_config, {
                'phase': 'load',
                'csv_files': job_state.get('csv_files', []),
                'processed_files': processed_files,
                'current_stats': stats
            })
            
            # Log results
            _logger.info(f"Loaded CSV file {csv_file}: {stats['total_rows']} rows "
                        f"({stats['new_rows']} new, {stats['updated_rows']} updated)")
            
            return {
                'status': 'success',
                'file': csv_file,
                'stats': stats,
                'execution_time': time.time() - start_time
            }
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error loading CSV file {csv_file}: {error_message}")
            _logger.error(traceback.format_exc())
            
            return {
                'status': 'failed',
                'file': csv_file,
                'error': error_message
            }
    
    @api.model
    def _monitor_loading_progress(self, table_config_id):
        """
        Job to monitor loading progress and queue additional loading jobs as needed.
        Runs as a separate queue job with the ability to queue more jobs.
        
        Args:
            table_config_id: ID of the ETL table config
            
        Returns:
            dict: Status information
        """
        # Use separate cursor to avoid transaction conflicts
        with self.env.registry.cursor() as new_cr:
            env = api.Environment(new_cr, self.env.uid, self.env.context)
            
            # Wait interval between checks
            check_interval_seconds = 15
            max_concurrent_loading = 10  # Process 10 files at a time as specified by user
            stalled_count = 0   # Count of checks where no progress is made
            
            table_config = env['etl.source.table'].browse(table_config_id)
            if not table_config.exists():
                _logger.error(f"Table config {table_config_id} not found")
                return {'error': 'Table config not found'}
                
            # Get current state
            job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
            
            # Handle job cancellation
            if table_config.job_status == 'cancelled':
                _logger.info(f"ETL job for {table_config.name} was cancelled")
                return {'status': 'cancelled'}
                
            # Check if we're still in loading phase
            if job_state.get('phase') != 'load':
                _logger.info(f"ETL job for {table_config.name} is no longer in loading phase")
                return {'status': 'phase_changed'}
                
            # Get completion status
            processed_files = job_state.get('processed_files', [])
            # Ensure processed_files is a list
            if not isinstance(processed_files, list):
                processed_files = []
                job_state['processed_files'] = []
                self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                
            prev_processed_count = len(processed_files)
            
            # Get files to process - with consistent handling
            all_files = job_state.get('all_files', [])
            csv_files = job_state.get('csv_files', [])
            
            # If all_files is missing but csv_files exists, use csv_files
            if not all_files and csv_files:
                all_files = csv_files
                job_state['all_files'] = csv_files
                self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
            # If csv_files is missing but all_files exists, update csv_files
            elif not csv_files and all_files:
                job_state['csv_files'] = all_files
                self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
            
            # Ensure all files actually exist
            valid_files = [f for f in all_files if os.path.exists(f)]
            if len(valid_files) < len(all_files):
                _logger.warning(f"Some CSV files are missing: {len(all_files) - len(valid_files)} out of {len(all_files)}")
                all_files = valid_files
                job_state['all_files'] = valid_files
                job_state['csv_files'] = valid_files
                self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
            
            # Get next file to queue
            next_file_to_queue = job_state.get('next_file_to_queue', 0)
            # Fix if next_file_to_queue is beyond range
            if next_file_to_queue > len(all_files):
                next_file_to_queue = len(processed_files)
                job_state['next_file_to_queue'] = next_file_to_queue
                self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                
            # CRITICAL FIX: If next_file_to_queue is 0 but we have processed files, update it
            if next_file_to_queue == 0 and len(processed_files) > 0:
                # Find the highest file index that has been processed
                try:
                    processed_indices = []
                    for file_path in processed_files:
                        # Extract index from filename (like test_ods_customers_batch_42.csv)
                        filename = os.path.basename(file_path)
                        if '_batch_' in filename:
                            index_str = filename.split('_batch_')[1].split('.')[0]
                            processed_indices.append(int(index_str))
                            
                    if processed_indices:
                        next_file_to_queue = max(processed_indices) + 1
                        _logger.info(f"Fixed next_file_to_queue to {next_file_to_queue} based on processed files")
                        job_state['next_file_to_queue'] = next_file_to_queue
                        self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                except Exception as e:
                    _logger.warning(f"Error fixing next_file_to_queue: {str(e)}")
            
            # Calculate progress
            total_files = len(all_files)
            completion_percentage = (len(processed_files) / total_files) * 100 if total_files > 0 else 0
            _logger.info(f"Loading progress: {len(processed_files)}/{total_files} files ({completion_percentage:.1f}%)")
            
            # Update table status with current progress
            table_config.write({
                'progress_percentage': 50 + (completion_percentage / 2),  # 50-100% for loading phase
                'last_sync_message': f'Loading in progress: {len(processed_files)}/{total_files} files ({completion_percentage:.1f}%)'
            })
            
            # CRITICAL FIX: Get the actual count of active loading jobs
            pending_jobs = self._count_active_loading_jobs(new_cr, table_config.name)
            _logger.info(f"Found {pending_jobs} pending/running loading jobs")
            
            # CRITICAL FIX: Always queue more files if we have capacity and more files to process
            if len(processed_files) < total_files:
                # Calculate how many additional files we can queue to maintain concurrency
                available_slots = max_concurrent_loading - pending_jobs
                
                if available_slots > 0 and next_file_to_queue < len(all_files):
                    # Calculate how many new files to queue
                    files_to_queue = min(available_slots, len(all_files) - next_file_to_queue)
                    
                    if files_to_queue > 0:
                        _logger.info(f"QUEUEING {files_to_queue} NEW LOADING JOBS - maintaining {max_concurrent_loading} concurrent jobs")
                        
                        for i in range(files_to_queue):
                            file_index = next_file_to_queue + i
                            if file_index < len(all_files):
                                csv_file = all_files[file_index]
                                
                                # Skip if file doesn't exist or is already processed
                                if not os.path.exists(csv_file):
                                    _logger.warning(f"File not found, skipping: {csv_file}")
                                    continue
                                    
                                if csv_file in processed_files:
                                    _logger.warning(f"File already processed, skipping: {csv_file}")
                                    continue
                                
                                # Generate unique identity key for this file with timestamp to prevent conflicts
                                file_id = os.path.basename(csv_file).replace('.csv', '')
                                identity_key = f"load_file_{table_config_id}_{file_id}_{int(time.time())}"
                                
                                # Add delay between jobs to stagger them
                                delay_seconds = i * 5  # 5 seconds between jobs
                                
                                _logger.info(f"Queueing loading job for file {file_index+1}/{len(all_files)}: {csv_file}")
                                
                                env['etl.fast.sync.postgres'].with_delay(
                                    description=f"CSV Loading File {file_index+1}/{len(all_files)}: {table_config.name}",
                                    channel="etl_loader",
                                    priority=10,
                                    identity_key=identity_key,
                                    eta=datetime.now() + timedelta(seconds=delay_seconds)
                                )._load_csv_file_to_db_independent(table_config_id, csv_file)
                        
                        # Update next file to queue
                        job_state['next_file_to_queue'] = next_file_to_queue + files_to_queue
                        self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                
                # CRITICAL FIX: If no progress but still have files to process, try to unstick jobs
                if prev_processed_count == len(processed_files) and stalled_count >= 3:
                    _logger.warning(f"Loading appears stalled. Attempting to force unstick jobs.")
                    self._force_unstick_jobs(table_config_id)
            
            # Check if all files are processed
            all_processed = len(processed_files) >= len(all_files)
            
            # Additional verification: check if every file in all_files is in processed_files
            if all_processed:
                processed_set = set(processed_files)
                all_files_set = set(all_files)
                
                # Check if all required files are processed
                missing_files = all_files_set - processed_set
                
                if missing_files:
                    all_processed = False
                    _logger.warning(f"Found {len(missing_files)} unprocessed files")
                    
                    # Add to failed_files list for retry
                    failed_files = job_state.get('failed_files', [])
                    for file_path in missing_files:
                        if file_path not in failed_files:
                            failed_files.append(file_path)
                    
                    job_state['failed_files'] = failed_files
                    self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
            
            if all_processed:
                _logger.info(f"All {len(all_files)} CSV files loaded. Finalizing ETL process.")
                
                # Get current stats
                stats = job_state.get('current_stats', {})
                if not isinstance(stats, dict):
                    stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
                
                # Calculate total time
                total_time = time.time() - job_state.get('start_time', time.time())
                if job_state.get('extract_time'):
                    total_time += job_state.get('extract_time')
                
                # Update table status
                table_config.write({
                    'job_status': 'done',
                    'last_sync_status': 'success',
                    'last_sync_message': f'ETL complete: {stats.get("total_rows", 0)} rows processed '
                                        f'({stats.get("new_rows", 0)} new, {stats.get("updated_rows", 0)} updated) '
                                        f'in {total_time:.1f}s',
                    'progress_percentage': 100,
                    'last_sync_time': fields.Datetime.now(),
                    'total_records_synced': stats.get('total_rows', 0)
                })
                
                # Update sync log
                sync_log = env['etl.sync.log'].search([
                    ('table_id', '=', table_config.id),
                    ('status', '=', 'running')
                ], limit=1)
                
                if sync_log:
                    sync_log.write({
                        'end_time': fields.Datetime.now(),
                        'status': 'success',
                        'total_records': stats.get('total_rows', 0),
                        'new_records': stats.get('new_rows', 0),
                        'updated_records': stats.get('updated_rows', 0)
                    })
                
                # Clean up job state
                self._clear_job_state(table_config)
                
                # Commit changes
                new_cr.commit()
                
                return {
                    'status': 'complete',
                    'stats': stats,
                    'total_time': total_time
                }
            
            # Not complete yet, create a new monitoring job
            env['etl.fast.sync.postgres'].with_delay(
                description=f"CSV Loading Monitor: {table_config.name}",
                channel="etl_loader_monitor",
                priority=5,
                eta=datetime.now() + timedelta(seconds=check_interval_seconds)
            )._monitor_loading_progress(table_config_id)
            
            # Commit changes to ensure the new job is created
            new_cr.commit()
            
            return {
                'status': 'monitoring_continued',
                'completed': len(processed_files),
                'total': total_files,
                'progress': completion_percentage
            }

    @api.model
    def _count_active_loading_jobs(self, cursor, table_name):
        """
        Count the number of active loading jobs for a table.
        
        Args:
            cursor: Database cursor
            table_name: Name of the table being loaded
            
        Returns:
            int: Number of active loading jobs
        """
        try:
            # Query active jobs for CSV loading
            cursor.execute("""
                SELECT COUNT(*) FROM queue_job
                WHERE state IN ('pending', 'enqueued', 'started') 
                AND name LIKE %s
                AND name LIKE %s
            """, (f"%{table_name}%", "%CSV Loading File%"))
            
            count = cursor.fetchone()[0]
            return count
        
        except Exception as e:
            _logger.error(f"Error counting active loading jobs: {str(e)}")
            return 0

    # @api.model
    # def _monitor_loading_progress(self, table_config_id):
    #     """
    #     Job to monitor loading progress and queue additional loading jobs as needed.
    #     Runs as a separate queue job with the ability to queue more jobs.
        
    #     Args:
    #         table_config_id: ID of the ETL table config
            
    #     Returns:
    #         dict: Status information
    #     """
    #     # Use separate cursor to avoid transaction conflicts
    #     with self.env.registry.cursor() as new_cr:
    #         env = api.Environment(new_cr, self.env.uid, self.env.context)
            
    #         # Wait interval between checks
    #         check_interval_seconds = 15
    #         max_concurrent_loading = 10  # Process 10 files at a time as specified by user
    #         stalled_count = 0   # Count of checks where no progress is made
            
    #         table_config = env['etl.source.table'].browse(table_config_id)
    #         if not table_config.exists():
    #             _logger.error(f"Table config {table_config_id} not found")
    #             return {'error': 'Table config not found'}
                
    #         # Get current state
    #         job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
            
    #         # Handle job cancellation
    #         if table_config.job_status == 'cancelled':
    #             _logger.info(f"ETL job for {table_config.name} was cancelled")
    #             return {'status': 'cancelled'}
                
    #         # Check if we're still in loading phase
    #         if job_state.get('phase') != 'load':
    #             _logger.info(f"ETL job for {table_config.name} is no longer in loading phase")
    #             return {'status': 'phase_changed'}
                
    #         # Get completion status
    #         processed_files = job_state.get('processed_files', [])
    #         # Ensure processed_files is a list
    #         if not isinstance(processed_files, list):
    #             processed_files = []
    #             job_state['processed_files'] = []
    #             self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                
    #         prev_processed_count = len(processed_files)
            
    #         # Get files to process - with consistent handling
    #         all_files = job_state.get('all_files', [])
    #         csv_files = job_state.get('csv_files', [])
            
    #         # If all_files is missing but csv_files exists, use csv_files
    #         if not all_files and csv_files:
    #             all_files = csv_files
    #             job_state['all_files'] = csv_files
    #             self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
    #         # If csv_files is missing but all_files exists, update csv_files
    #         elif not csv_files and all_files:
    #             job_state['csv_files'] = all_files
    #             self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
            
    #         # Ensure all files actually exist
    #         valid_files = [f for f in all_files if os.path.exists(f)]
    #         if len(valid_files) < len(all_files):
    #             _logger.warning(f"Some CSV files are missing: {len(all_files) - len(valid_files)} out of {len(all_files)}")
    #             all_files = valid_files
    #             job_state['all_files'] = valid_files
    #             job_state['csv_files'] = valid_files
    #             self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
            
    #         # Get next file to queue
    #         next_file_to_queue = job_state.get('next_file_to_queue', 0)
    #         # Fix if next_file_to_queue is beyond range
    #         if next_file_to_queue > len(all_files):
    #             next_file_to_queue = len(processed_files)
    #             job_state['next_file_to_queue'] = next_file_to_queue
    #             self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
            
    #         # Calculate progress
    #         total_files = len(all_files)
    #         completion_percentage = (len(processed_files) / total_files) * 100 if total_files > 0 else 0
    #         _logger.info(f"Loading progress: {len(processed_files)}/{total_files} files ({completion_percentage:.1f}%)")
            
    #         # Update table status with current progress
    #         table_config.write({
    #             'progress_percentage': 50 + (completion_percentage / 2),  # 50-100% for loading phase
    #             'last_sync_message': f'Loading in progress: {len(processed_files)}/{total_files} files ({completion_percentage:.1f}%)'
    #         })
            
    #         # Check if we're making progress - use sorted lists for comparison
    #         sorted_processed = sorted(processed_files) if processed_files else []
    #         if len(sorted_processed) == prev_processed_count:
    #             stalled_count += 1
    #             _logger.info(f"No progress detected for {stalled_count} checks")
                
    #             # If no progress for a while, try recovery actions
    #             if stalled_count >= 3:
    #                 _logger.warning(f"No loading progress detected for {stalled_count} checks")
                    
    #                 # Check for failed files
    #                 failed_files = job_state.get('failed_files', [])
                    
    #                 # Get number of currently running loading jobs
    #                 env.cr.execute("""
    #                     SELECT COUNT(*) as count FROM queue_job
    #                     WHERE state IN ('pending', 'enqueued', 'started') 
    #                     AND name LIKE %s
    #                     AND channel = 'etl_loader'
    #                 """, (f"%{table_config.name}%",))
    #                 pending_jobs = env.cr.fetchone()[0]
                    
    #                 _logger.info(f"Found {pending_jobs} pending/running loading jobs")
                    
    #                 # Only queue more jobs if fewer pending than max_concurrent
    #                 if pending_jobs < max_concurrent_loading:
    #                     # Calculate how many more jobs to queue
    #                     additional_jobs = max_concurrent_loading - pending_jobs
                        
    #                     # First, requeue any failed files up to the additional job limit
    #                     if failed_files:
    #                         files_to_requeue = failed_files[:additional_jobs]
                            
    #                         _logger.info(f"Requeuing {len(files_to_requeue)} failed files")
                            
    #                         for i, file_path in enumerate(files_to_requeue):
    #                             # Skip if file doesn't exist
    #                             if not os.path.exists(file_path):
    #                                 _logger.warning(f"Failed file not found, skipping: {file_path}")
    #                                 continue
                                    
    #                             # Add delay between requeues
    #                             delay_seconds = i * 10
                                
    #                             # Generate unique identity key for this file with timestamp to ensure uniqueness
    #                             file_id = os.path.basename(file_path).replace('.csv', '')
    #                             identity_key = f"load_file_{table_config_id}_{file_id}_retry_{int(time.time())}"
                                
    #                             _logger.info(f"Requeuing failed file {i+1}/{len(files_to_requeue)}: {file_path}")
                                
    #                             env['etl.fast.sync.postgres'].with_delay(
    #                                 description=f"CSV Loading File (Retry): {table_config.name}",
    #                                 channel="etl_loader",
    #                                 priority=10,
    #                                 identity_key=identity_key,
    #                                 eta=datetime.now() + timedelta(seconds=delay_seconds)
    #                             )._load_csv_file_to_db_independent(table_config_id, file_path)
                            
    #                         # Remove requeued files from failed list
    #                         new_failed_files = [f for f in failed_files if f not in files_to_requeue]
    #                         job_state['failed_files'] = new_failed_files
    #                         additional_jobs -= len(files_to_requeue)
                            
    #                         # Save updated job state
    #                         self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                        
    #                     # Queue additional new files if needed and available
    #                     if additional_jobs > 0 and next_file_to_queue < len(all_files):
    #                         files_to_queue = min(additional_jobs, len(all_files) - next_file_to_queue)
                            
    #                         if files_to_queue > 0:
    #                             _logger.info(f"Queueing {files_to_queue} additional file loading jobs")
                                
    #                             for i in range(files_to_queue):
    #                                 file_index = next_file_to_queue + i
    #                                 if file_index < len(all_files):
    #                                     csv_file = all_files[file_index]
                                        
    #                                     # Skip if file doesn't exist
    #                                     if not os.path.exists(csv_file):
    #                                         _logger.warning(f"File not found, skipping: {csv_file}")
    #                                         continue
                                        
    #                                     # Generate unique identity key for this file
    #                                     file_id = os.path.basename(csv_file).replace('.csv', '')
    #                                     identity_key = f"load_file_{table_config_id}_{file_id}"
                                        
    #                                     # Add delay between jobs to stagger them
    #                                     delay_seconds = i * 10  # 10 seconds between jobs
                                        
    #                                     _logger.info(f"Queueing loading job for file {file_index+1}/{len(all_files)}: {csv_file}")
                                        
    #                                     env['etl.fast.sync.postgres'].with_delay(
    #                                         description=f"CSV Loading File {file_index+1}/{len(all_files)}: {table_config.name}",
    #                                         channel="etl_loader",
    #                                         priority=10,
    #                                         identity_key=identity_key,
    #                                         eta=datetime.now() + timedelta(seconds=delay_seconds)
    #                                     )._load_csv_file_to_db_independent(table_config_id, csv_file)
                                
    #                             # Update next file to queue
    #                             job_state['next_file_to_queue'] = next_file_to_queue + files_to_queue
    #                             self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                    
    #                 # Try to force any stuck jobs to requeue
    #                 self._force_unstick_jobs(table_config_id)
                    
    #                 # Reset stalled count after recovery attempts
    #                 stalled_count = 0
    #         else:
    #             # We made progress, reset stalled count
    #             stalled_count = 0
                
    #             # Queue additional files if needed - fix to ensure we maintain max_concurrent_loading
    #             files_in_progress = next_file_to_queue - len(processed_files)
                
    #             if files_in_progress < max_concurrent_loading and next_file_to_queue < len(all_files):
    #                 # Calculate how many new files to queue
    #                 files_to_queue = min(max_concurrent_loading - files_in_progress, len(all_files) - next_file_to_queue)
                    
    #                 if files_to_queue > 0:
    #                     _logger.info(f"Queueing {files_to_queue} more loading jobs")
                        
    #                     for i in range(files_to_queue):
    #                         file_index = next_file_to_queue + i
    #                         if file_index < len(all_files):
    #                             csv_file = all_files[file_index]
                                
    #                             # Skip if file doesn't exist
    #                             if not os.path.exists(csv_file):
    #                                 _logger.warning(f"File not found, skipping: {csv_file}")
    #                                 continue
                                
    #                             # Generate unique identity key for this file
    #                             file_id = os.path.basename(csv_file).replace('.csv', '')
    #                             identity_key = f"load_file_{table_config_id}_{file_id}"
                                
    #                             # Add delay between jobs to stagger them
    #                             delay_seconds = i * 10  # 10 seconds between jobs
                                
    #                             _logger.info(f"Queueing loading job for file {file_index+1}/{len(all_files)}: {csv_file}")
                                
    #                             env['etl.fast.sync.postgres'].with_delay(
    #                                 description=f"CSV Loading File {file_index+1}/{len(all_files)}: {table_config.name}",
    #                                 channel="etl_loader",
    #                                 priority=10,
    #                                 identity_key=identity_key,
    #                                 eta=datetime.now() + timedelta(seconds=delay_seconds)
    #                             )._load_csv_file_to_db_independent(table_config_id, csv_file)
                        
    #                     # Update next file to queue
    #                     job_state['next_file_to_queue'] = next_file_to_queue + files_to_queue
    #                     self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
            
    #         # Check if all files are processed - fix to properly detect completion
    #         all_processed = len(processed_files) >= len(all_files)
            
    #         # Additional verification: check if every file in all_files is in processed_files
    #         if all_processed:
    #             processed_set = set(processed_files)
    #             all_files_set = set(all_files)
                
    #             # Check if all required files are processed
    #             missing_files = all_files_set - processed_set
                
    #             if missing_files:
    #                 all_processed = False
    #                 _logger.warning(f"Found {len(missing_files)} unprocessed files")
                    
    #                 # Add to failed_files list for retry
    #                 failed_files = job_state.get('failed_files', [])
    #                 for file_path in missing_files:
    #                     if file_path not in failed_files:
    #                         failed_files.append(file_path)
                    
    #                 job_state['failed_files'] = failed_files
    #                 self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
            
    #         if all_processed:
    #             _logger.info(f"All {len(all_files)} CSV files loaded. Finalizing ETL process.")
                
    #             # Get current stats
    #             stats = job_state.get('current_stats', {})
    #             if not isinstance(stats, dict):
    #                 stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
                
    #             # Calculate total time
    #             total_time = time.time() - job_state.get('start_time', time.time())
    #             if job_state.get('extract_time'):
    #                 total_time += job_state.get('extract_time')
                
    #             # Update table status
    #             table_config.write({
    #                 'job_status': 'done',
    #                 'last_sync_status': 'success',
    #                 'last_sync_message': f'ETL complete: {stats.get("total_rows", 0)} rows processed '
    #                                     f'({stats.get("new_rows", 0)} new, {stats.get("updated_rows", 0)} updated) '
    #                                     f'in {total_time:.1f}s',
    #                 'progress_percentage': 100,
    #                 'last_sync_time': fields.Datetime.now(),
    #                 'total_records_synced': stats.get('total_rows', 0)
    #             })
                
    #             # Update sync log
    #             sync_log = env['etl.sync.log'].search([
    #                 ('table_id', '=', table_config.id),
    #                 ('status', '=', 'running')
    #             ], limit=1)
                
    #             if sync_log:
    #                 sync_log.write({
    #                     'end_time': fields.Datetime.now(),
    #                     'status': 'success',
    #                     'total_records': stats.get('total_rows', 0),
    #                     'new_records': stats.get('new_rows', 0),
    #                     'updated_records': stats.get('updated_rows', 0)
    #                 })
                
    #             # Clean up job state
    #             self._clear_job_state(table_config)
                
    #             # Commit changes
    #             new_cr.commit()
                
    #             return {
    #                 'status': 'complete',
    #                 'stats': stats,
    #                 'total_time': total_time
    #             }
            
    #         # Not complete yet, create a new monitoring job
    #         env['etl.fast.sync.postgres'].with_delay(
    #             description=f"CSV Loading Monitor: {table_config.name}",
    #             channel="etl_loader_monitor",
    #             priority=5,
    #             eta=datetime.now() + timedelta(seconds=check_interval_seconds)
    #         )._monitor_loading_progress(table_config_id)
            
    #         # Commit changes to ensure the new job is created
    #         new_cr.commit()
            
    #         return {
    #             'status': 'monitoring_continued',
    #             'completed': len(processed_files),
    #             'total': total_files,
    #             'progress': completion_percentage
    #         }


    @api.model
    def _extract_transform_to_csv(self, connector_service, source_db, config, primary_key, 
                            offset, batch_size, csv_filename):
        """
        Extract and transform a batch of data, saving it to a CSV file
        with proper lookup metadata preservation.
        
        Args:
            connector_service: Database connector service
            source_db: Source database connection
            config: Configuration dictionary
            primary_key: Primary key field name
            offset: Starting offset
            batch_size: Size of the batch to extract
            csv_filename: Path to save the CSV file
            
        Returns:
            int: Number of rows processed
        """
        # Get source columns and mappings
        source_table = config['source_table']
        
        try:
            # Get source columns
            source_columns = connector_service.get_columns(source_db, source_table)
            
            # Prepare column mappings
            column_map = {}
            included_columns = []
            lookup_columns = {}  # Track which columns need lookups
            
            for source_col, mapping in config['mappings'].items():
                original_source_col = source_columns.get(source_col.lower())
                if original_source_col and mapping.get('target'):
                    column_map[original_source_col] = mapping['target'].lower()
                    included_columns.append(original_source_col)
                    
                    # Save lookup configurations if needed
                    if mapping.get('type') == 'lookup':
                        lookup_columns[original_source_col] = {
                            'target': mapping['target'].lower(),
                            'lookup_table': mapping.get('lookup_table'),
                            'lookup_key': mapping.get('lookup_key'),
                            'lookup_value': mapping.get('lookup_value')
                        }
            
            if not included_columns:
                _logger.error("No valid columns found in mappings")
                return 0
            
            # Build query to get data
            query = f"""
                SELECT {', '.join([f'"{col}"' for col in included_columns])}
                FROM "{source_table}"
                ORDER BY "{primary_key}"
                LIMIT {batch_size} OFFSET {offset}
            """
            
            _logger.info(f"Executing extraction query with limit {batch_size} offset {offset}")
            
            # Execute query
            result = connector_service.execute_query(source_db, query)
            
            if not result:
                _logger.warning(f"No data returned from extraction query")
                return 0
                
            row_count = len(result)
            _logger.info(f"Retrieved {row_count} rows for CSV extraction")
            
            if row_count == 0:
                return 0
            
            # Make sure directory exists
            os.makedirs(os.path.dirname(csv_filename), exist_ok=True)
            
            # Save lookup configuration in a metadata file
            if lookup_columns:
                metadata_filename = csv_filename + '.meta'
                with open(metadata_filename, 'w') as metafile:
                    json.dump(lookup_columns, metafile)
                _logger.info(f"Saved lookup configuration to {metadata_filename}")
            
            # Transform and write to CSV
            with open(csv_filename, 'w', newline='') as csvfile:
                # Get header from first row's mapped column names
                header = [column_map[col] for col in included_columns]
                
                writer = csv.DictWriter(csvfile, fieldnames=header)
                writer.writeheader()
                
                # Process each row
                for row in result:
                    # Transform row using column mapping
                    transformed_row = {}
                    for src_col, tgt_col in column_map.items():
                        if src_col in row:
                            value = row[src_col]
                            
                            # Handle special data types for CSV
                            if isinstance(value, (datetime, date)):
                                value = value.isoformat()
                            elif value is None:
                                value = ''
                            
                            transformed_row[tgt_col] = value
                    
                    # Write transformed row to CSV
                    writer.writerow(transformed_row)
            
            _logger.info(f"Wrote {row_count} rows to CSV file {csv_filename}")
            return row_count
            
        except Exception as e:
            _logger.error(f"Error extracting data to CSV: {str(e)}")
            raise

    @api.model
    def _load_csvs_to_database(self, table_config, csv_files):
        """Load CSV files into the target database"""
        start_time = time.time()
        
        # Get connector service
        connector_service = self.env['etl.database.connector.service']
        target_db = table_config.target_db_connection_id
        
        # Get configuration
        config = table_config.get_config_json()
        target_table = config['target_table']
        primary_key = config['primary_key']
        
        job_state = self._get_job_state(table_config)
        processed_files = job_state.get('processed_files', [])
        
        # Get sync log
        sync_log = self.env['etl.sync.log'].search([
            ('table_id', '=', table_config.id),
            ('status', '=', 'running')
        ], limit=1)
        
        if not sync_log:
            sync_log = self.env['etl.sync.log'].create({
                'table_id': table_config.id,
                'start_time': fields.Datetime.now(),
                'status': 'running'
            })
        
        # Initialize stats
        stats = {
            'total_rows': 0,
            'new_rows': 0,
            'updated_rows': 0,
            'errors': 0
        }
        
        try:
            _logger.info(f"Phase 2: Loading {len(csv_files)} CSV files into {target_table}")
            
            # Create a transaction manager
            tx_manager = self.env['etl.transaction']
            
            # Load files in order
            for i, csv_file in enumerate(csv_files):
                # Skip already processed files
                if csv_file in processed_files:
                    _logger.info(f"Skipping already processed file: {csv_file}")
                    continue
                
                _logger.info(f"Loading CSV file {i+1}/{len(csv_files)}: {csv_file}")
                
                # Check if the file exists
                if not os.path.exists(csv_file):
                    _logger.warning(f"CSV file not found: {csv_file}")
                    continue
                
                # Load CSV in smaller chunks to manage memory
                batch_stats = self._load_csv_file_to_database(
                    connector_service, target_db, target_table, primary_key, csv_file, tx_manager
                )
                
                # Update stats
                for key in batch_stats:
                    if key in stats:
                        stats[key] += batch_stats[key]
                
                # Mark file as processed
                processed_files.append(csv_file)
                
                # Update job state
                self._save_job_state(table_config, {
                    'phase': 'load',
                    'csv_files': csv_files,
                    'processed_files': processed_files,
                    'current_stats': stats
                })
                
                # Update progress
                progress = 50 + ((i + 1) / len(csv_files) * 50)  # 50-100% for loading phase
                table_config.write({
                    'progress_percentage': progress,
                    'last_sync_message': f'Loaded CSV file {i+1}/{len(csv_files)}: {batch_stats["total_rows"]} rows'
                })
                
                # Commit transaction after each file to release memory
                self.env.cr.commit()
            
            # Update table status on completion
            execution_time = time.time() - start_time
            combined_time = execution_time
            if job_state.get('extract_time'):
                combined_time += job_state.get('extract_time')
            
            table_config.write({
                'job_status': 'done',
                'last_sync_status': 'success',
                'last_sync_message': f'Processed {stats["total_rows"]} records ({stats["new_rows"]} new, {stats["updated_rows"]} updated) in {combined_time:.1f}s',
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
            
            # Clear job state
            self._clear_job_state(table_config)
            
            return {
                'status': 'success',
                'total_rows': stats['total_rows'],
                'new_rows': stats['new_rows'],
                'updated_rows': stats['updated_rows'],
                'execution_time': combined_time
            }
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in CSV loading phase: {error_message}")
            _logger.error(traceback.format_exc())
            
            # Update table status
            table_config.write({
                'job_status': 'failed',
                'last_sync_status': 'failed',
                'last_sync_message': f'Error in loading phase: {error_message}'
            })
            
            # Update sync log
            sync_log.write({
                'end_time': fields.Datetime.now(),
                'status': 'failed',
                'error_message': error_message
            })
            
            return {
                'status': 'failed',
                'error': error_message,
                'execution_time': time.time() - start_time
            }
    
    @api.model
    def _load_csv_file_to_database(self, connector_service, target_db, target_table, primary_key, csv_file, tx_manager):
        """
        Load a single CSV file into the database with optimized batch processing and lookup support.
        
        Args:
            connector_service: Database connector service
            target_db: Target database connection
            target_table: Target table name
            primary_key: Primary key field name
            csv_file: Path to the CSV file
            tx_manager: Transaction manager for safe operations
            
        Returns:
            dict: Statistics about the loading operation
        """
        stats = {
            'total_rows': 0,
            'new_rows': 0,
            'updated_rows': 0,
            'errors': 0
        }
        
        try:
            # Get target column types
            with connector_service.cursor(target_db) as cursor:
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = '{target_table}'
                """)
                
                target_columns_info = {}
                for row in cursor.fetchall():
                    col_name = row['column_name'].lower()
                    target_columns_info[col_name] = {
                        'type': row['data_type'].lower(),
                        'nullable': row['is_nullable'] == 'YES'
                    }
            
            # Check for lookup metadata file
            lookup_configs = {}
            metadata_file = csv_file + '.meta'
            if os.path.exists(metadata_file):
                try:
                    with open(metadata_file, 'r') as metafile:
                        lookup_metadata = json.load(metafile)
                        # Transform metadata into a format for the target columns
                        for src_col, config in lookup_metadata.items():
                            if 'target' in config:
                                lookup_configs[config['target']] = config
                        _logger.info(f"Loaded lookup configuration from {metadata_file}")
                except Exception as e:
                    _logger.warning(f"Error loading lookup metadata: {str(e)}")
            
            # Open CSV file and process in batches
            with open(csv_file, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Process in small batches to manage memory
                batch_size = 500
                batch = []
                
                for row in reader:
                    # Add row to batch
                    batch.append(row)
                    stats['total_rows'] += 1
                    
                    # Process batch when it reaches the size limit
                    if len(batch) >= batch_size:
                        batch_stats = self._process_csv_batch(
                            connector_service, target_db, target_table, 
                            primary_key, batch, target_columns_info, tx_manager, 
                            lookup_configs  # Pass lookup configurations
                        )
                        
                        # Update stats
                        for key in batch_stats:
                            if key in stats:
                                stats[key] += batch_stats[key]
                        
                        # Clear batch
                        batch = []
                
                # Process any remaining rows
                if batch:
                    batch_stats = self._process_csv_batch(
                        connector_service, target_db, target_table, 
                        primary_key, batch, target_columns_info, tx_manager,
                        lookup_configs  # Pass lookup configurations
                    )
                    
                    # Update stats
                    for key in batch_stats:
                        if key in stats:
                            stats[key] += batch_stats[key]
            
            return stats
            
        except Exception as e:
            _logger.error(f"Error loading CSV to database: {str(e)}")
            stats['errors'] += 1
            return stats

    @api.model
    def _process_csv_batch(self, connector_service, target_db, target_table, 
                    primary_key, batch, target_columns_info, tx_manager, lookup_configs=None):
        """Process a batch of CSV rows and insert/update in the database with lookup support"""
        stats = {'new_rows': 0, 'updated_rows': 0}
        
        # Use transaction context for safety
        with tx_manager.transaction_context(
            name=f"csv_batch_{int(time.time()*1000)}",
            retry_count=3
        ) as tx:
            # Convert CSV strings to appropriate types
            converted_batch = []
            
            for row in batch:
                # Convert values based on target column types
                converted_row = {}
                
                for col, val in row.items():
                    col_lower = col.lower()
                    
                    # Skip empty values
                    if val == '':
                        converted_row[col_lower] = None
                        continue
                    
                    # Check if this column has a lookup configuration
                    if lookup_configs and col_lower in lookup_configs:
                        # Perform lookup
                        lookup_config = lookup_configs[col_lower]
                        lookup_value = self._lookup_value(
                            connector_service,
                            target_db,
                            lookup_config['lookup_table'],
                            lookup_config['lookup_key'],
                            lookup_config['lookup_value'],
                            str(val)
                        )
                        
                        # Use the lookup value instead of the original value
                        val = lookup_value
                    
                    # Convert based on target type
                    if col_lower in target_columns_info:
                        target_type = target_columns_info[col_lower]['type']
                        
                        if 'int' in target_type:
                            try:
                                converted_row[col_lower] = int(float(val)) if val is not None else None
                            except (ValueError, TypeError):
                                converted_row[col_lower] = None
                        
                        elif any(t in target_type for t in ('float', 'numeric', 'decimal', 'double')):
                            try:
                                converted_row[col_lower] = float(val) if val is not None else None
                            except (ValueError, TypeError):
                                converted_row[col_lower] = None
                        
                        elif 'date' in target_type and 'timestamp' not in target_type:
                            try:
                                # Handle ISO format date strings
                                if val and 'T' in val:
                                    dt = datetime.fromisoformat(val.replace('Z', '+00:00'))
                                    converted_row[col_lower] = dt.date()
                                elif val:
                                    converted_row[col_lower] = date.fromisoformat(val)
                                else:
                                    converted_row[col_lower] = None
                            except (ValueError, TypeError):
                                converted_row[col_lower] = None
                        
                        elif 'timestamp' in target_type:
                            try:
                                # Handle ISO format datetime strings
                                if val:
                                    converted_row[col_lower] = datetime.fromisoformat(val.replace('Z', '+00:00'))
                                else:
                                    converted_row[col_lower] = None
                            except (ValueError, TypeError):
                                converted_row[col_lower] = None
                        
                        elif 'bool' in target_type:
                            if val is None:
                                converted_row[col_lower] = None
                            else:
                                val_lower = str(val).lower()
                                if val_lower in ('true', 't', 'yes', 'y', '1'):
                                    converted_row[col_lower] = True
                                elif val_lower in ('false', 'f', 'no', 'n', '0'):
                                    converted_row[col_lower] = False
                                else:
                                    converted_row[col_lower] = None
                        
                        else:
                            # String or other types
                            converted_row[col_lower] = val
                    else:
                        # Column not in target table - include as is
                        converted_row[col_lower] = val
                
                converted_batch.append(converted_row)
            
            # Process the batch with efficient bulk operations
            with connector_service.cursor(target_db) as cursor:
                # Group by insert vs update
                to_insert = []
                to_update = []
                
                # Check which records exist
                if converted_batch:
                    # Extract primary keys
                    primary_keys = [row.get(primary_key.lower()) for row in converted_batch 
                                if primary_key.lower() in row and row.get(primary_key.lower()) is not None]
                    
                    # Check which ones exist
                    if primary_keys:
                        placeholders = ', '.join(['%s'] * len(primary_keys))
                        query = f'SELECT "{primary_key}" FROM "{target_table}" WHERE "{primary_key}" IN ({placeholders})'
                        cursor.execute(query, primary_keys)
                        existing_keys = [row[primary_key] for row in cursor.fetchall()]
                        
                        # Separate into insert vs update
                        for row in converted_batch:
                            pk_value = row.get(primary_key.lower())
                            if pk_value is None:
                                continue
                                
                            if pk_value in existing_keys:
                                to_update.append(row)
                            else:
                                to_insert.append(row)
                    else:
                        # No primary keys - treat all as inserts
                        to_insert = converted_batch
                
                # Process inserts
                if to_insert:
                    # Get columns from first row
                    columns = list(to_insert[0].keys())
                    
                    # Create SQL for bulk insert
                    columns_str = ', '.join([f'"{col}"' for col in columns])
                    placeholders = ', '.join(['%s'] * len(columns))
                    insert_sql = f'INSERT INTO "{target_table}" ({columns_str}) VALUES ({placeholders})'
                    
                    # Create values list
                    values = []
                    for row in to_insert:
                        row_values = [row.get(col) for col in columns]
                        values.append(tuple(row_values))
                    
                    # Execute bulk insert
                    cursor.executemany(insert_sql, values)
                    stats['new_rows'] += len(to_insert)
                
                # Process updates
                for row in to_update:
                    pk_value = row.get(primary_key.lower())
                    
                    # Skip if no primary key
                    if pk_value is None:
                        continue
                    
                    # Create update statement
                    update_parts = []
                    update_values = []
                    
                    for col, val in row.items():
                        if col != primary_key.lower():
                            update_parts.append(f'"{col}" = %s')
                            update_values.append(val)
                    
                    if update_parts:
                        update_sql = f'UPDATE "{target_table}" SET {", ".join(update_parts)} WHERE "{primary_key}" = %s'
                        update_values.append(pk_value)
                        cursor.execute(update_sql, update_values)
                        stats['updated_rows'] += 1
        
        return stats
    
    @api.model
    def _get_csv_directory(self, table_config):
        """Get directory path for storing CSV files within the module base directory"""
        base_dir = os.path.dirname(os.path.abspath(__file__))

        root = os.path.dirname(base_dir)

        table_dir = f"{table_config.id}_{table_config.name}"

        csv_dir = os.path.join(root, 'etl_csv_files', table_dir)

        os.makedirs(csv_dir, exist_ok=True)

        return csv_dir
    
    @api.model
    def _process_full_extraction(self, table_config_id, batch_size, total_batches):
        """Process all extraction batches sequentially with improved error handling"""
        table_config = self.env['etl.source.table'].browse(table_config_id)
        if not table_config.exists():
            return {'error': 'Table config not found'}
        
        _logger.info(f"Starting full extraction process for {table_config.name} with {total_batches} batches")
        
        try:
            # Get resources needed for processing
            connector_service = self.env['etl.database.connector.service']
            source_db = table_config.source_db_connection_id
            config = table_config.get_config_json()
            source_table = config['source_table']
            primary_key = config['primary_key']
            
            # Create directory for CSV files
            csv_dir = self._get_csv_directory(table_config)
            os.makedirs(csv_dir, exist_ok=True)
            
            # Get job state
            job_state = self._get_job_state(table_config)
            completed_batches = job_state.get('completed_batches', [])
            csv_files = job_state.get('csv_files', [])
            
            # Process each batch sequentially with better error handling
            for batch_num in range(total_batches):
                # Skip if already processed
                if batch_num in completed_batches:
                    _logger.info(f"Batch {batch_num+1}/{total_batches} already processed, skipping")
                    continue
                
                # Calculate offset
                offset = batch_num * batch_size
                
                # Generate CSV filename
                csv_filename = os.path.join(csv_dir, f"{source_table}_batch_{batch_num}.csv")
                
                # Create savepoint for this batch
                savepoint_name = f"batch_{batch_num}_{int(time.time())}"
                self.env.cr.execute(f"SAVEPOINT {savepoint_name}")
                
                try:
                    _logger.info(f"Processing extraction batch {batch_num+1}/{total_batches} at offset {offset}")
                    
                    # Extract and transform batch to CSV
                    rows_processed = self._extract_transform_to_csv(
                        connector_service, source_db, config, primary_key,
                        offset, batch_size, csv_filename
                    )
                    
                    if rows_processed > 0:
                        _logger.info(f"Extracted {rows_processed} rows to {csv_filename}")
                        
                        # Update job state
                        completed_batches.append(batch_num)
                        if csv_filename not in csv_files:
                            csv_files.append(csv_filename)
                            
                        self._save_job_state(table_config, {
                            'phase': 'extract',
                            'total_rows': job_state.get('total_rows', 0),
                            'total_batches': total_batches,
                            'batch_size': batch_size,
                            'csv_files': csv_files,
                            'completed_batches': completed_batches
                        })
                        
                        # Update progress
                        progress = ((batch_num + 1) / total_batches) * 50  # 50% for extraction phase
                        table_config.write({
                            'progress_percentage': progress,
                            'last_sync_message': f'Extracted batch {batch_num+1}/{total_batches} to CSV ({progress:.1f}%)'
                        })
                    else:
                        _logger.info(f"No rows processed for batch {batch_num+1}/{total_batches}")
                        
                        # Mark as complete anyway
                        completed_batches.append(batch_num)
                        self._save_job_state(table_config, {
                            'phase': 'extract',
                            'total_rows': job_state.get('total_rows', 0),
                            'total_batches': total_batches,
                            'batch_size': batch_size,
                            'csv_files': csv_files,
                            'completed_batches': completed_batches
                        })
                    
                    # Release savepoint
                    self.env.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    
                    # Commit to save progress after each batch - CRITICAL for avoiding long transactions
                    self.env.cr.commit()
                    
                    # Force garbage collection to manage memory
                    gc.collect()
                    
                except Exception as batch_error:
                    # Rollback to savepoint for this batch
                    try:
                        self.env.cr.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                        _logger.error(f"Error processing batch {batch_num+1}: {str(batch_error)}. Rolled back to savepoint.")
                    except Exception as rollback_error:
                        _logger.error(f"Failed to rollback to savepoint: {str(rollback_error)}")
                        # Try a full rollback
                        try:
                            self.env.cr.rollback()
                            _logger.warning("Performed full rollback after savepoint rollback failure")
                        except:
                            pass
                    
                    # Mark batch as failed in job state
                    failed_batches = job_state.get('failed_batches', [])
                    failed_batches.append(batch_num)
                    self._save_job_state(table_config, {
                        'phase': 'extract',
                        'total_rows': job_state.get('total_rows', 0),
                        'total_batches': total_batches,
                        'batch_size': batch_size,
                        'csv_files': csv_files,
                        'completed_batches': completed_batches,
                        'failed_batches': failed_batches
                    })
                    
                    # Commit the job state update to ensure we don't lose track of progress
                    self.env.cr.commit()
                    
                    # Skip to next batch instead of stopping the whole process
                    continue
            
            # Extraction complete - start loading phase
            _logger.info(f"Extraction phase complete with {len(csv_files)} CSV files. Starting loading phase.")
            
            # Update job state for loading phase
            job_state = self._get_job_state(table_config)
            self._save_job_state(table_config, {
                'phase': 'load',
                'csv_files': csv_files,
                'extract_time': time.time() - job_state.get('start_time', time.time()),
                'start_time': time.time(),
                'total_rows': job_state.get('total_rows', 0)
            })
            
            # Start loading phase with a single job
            loading_job = self.with_delay(
                description=f"CSV Loading Master: {table_config.name}",
                channel="root",  # Use the default channel
                priority=10
            )._process_full_loading(table_config.id)
            
            return {
                'status': 'extraction_complete',
                'csv_files': len(csv_files),
                'loading_job_uuid': loading_job.uuid
            }
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in full extraction process: {error_message}")
            _logger.error(traceback.format_exc())
            
            # Update table status
            table_config.write({
                'job_status': 'failed',
                'last_sync_status': 'failed',
                'last_sync_message': f'Error in extraction phase: {error_message}'
            })
            
            return {
                'status': 'failed',
                'error': error_message
            }


    @api.model
    def _process_full_loading(self, table_config_id):
        """
        Process all loading tasks sequentially in a single job.
        This avoids the need for multiple parallel jobs.
        """
        table_config = self.env['etl.source.table'].browse(table_config_id)
        if not table_config.exists():
            return {'error': 'Table config not found'}
        
        _logger.info(f"Starting full loading process for {table_config.name}")
        
        try:
            # Get job state
            job_state = self._get_job_state(table_config)
            csv_files = job_state.get('csv_files', [])
            processed_files = job_state.get('processed_files', [])
            
            if not csv_files:
                _logger.warning(f"No CSV files to load for {table_config.name}")
                
                # Update table status
                table_config.write({
                    'job_status': 'done',
                    'last_sync_status': 'warning',
                    'last_sync_message': 'No CSV files to load',
                    'progress_percentage': 100,
                    'last_sync_time': fields.Datetime.now()
                })
                
                return {'status': 'no_files'}
            
            # Get resources needed for processing
            connector_service = self.env['etl.database.connector.service']
            target_db = table_config.target_db_connection_id
            config = table_config.get_config_json()
            target_table = config['target_table']
            primary_key = config['primary_key']
            
            # Create a transaction manager
            tx_manager = self.env['etl.transaction']
            
            # Initialize stats
            stats = {
                'total_rows': 0,
                'new_rows': 0,
                'updated_rows': 0,
                'errors': 0
            }
            
            # Process each CSV file sequentially
            for i, csv_file in enumerate(csv_files):
                # Skip if already processed
                if csv_file in processed_files:
                    _logger.info(f"CSV file {i+1}/{len(csv_files)} already processed, skipping")
                    continue
                
                _logger.info(f"Loading CSV file {i+1}/{len(csv_files)}: {csv_file}")
                
                # Check if file exists
                if not os.path.exists(csv_file):
                    _logger.warning(f"CSV file not found: {csv_file}")
                    continue
                
                # Load the CSV file
                file_stats = self._load_csv_file_to_database(
                    connector_service, target_db, target_table, primary_key, csv_file, tx_manager
                )
                
                # Update stats
                for key in file_stats:
                    if key in stats:
                        stats[key] += file_stats[key]
                
                # Mark file as processed
                processed_files.append(csv_file)
                self._save_job_state(table_config, {
                    'phase': 'load',
                    'csv_files': csv_files,
                    'processed_files': processed_files,
                    'current_stats': stats
                })
                
                # Update progress
                progress = 50 + ((i + 1) / len(csv_files) * 50)  # 50-100% for loading phase
                table_config.write({
                    'progress_percentage': progress,
                    'last_sync_message': f'Loaded CSV file {i+1}/{len(csv_files)}: {file_stats["total_rows"]} rows ({progress:.1f}%)'
                })
                
                # Commit after each file to save progress
                self.env.cr.commit()
            
            # Calculate total time
            total_time = time.time() - job_state.get('start_time', time.time())
            if job_state.get('extract_time'):
                total_time += job_state.get('extract_time')
            
            # Update table status
            table_config.write({
                'job_status': 'done',
                'last_sync_status': 'success',
                'last_sync_message': f'ETL complete: {stats["total_rows"]} rows processed '
                                    f'({stats["new_rows"]} new, {stats["updated_rows"]} updated) '
                                    f'in {total_time:.1f}s',
                'progress_percentage': 100,
                'last_sync_time': fields.Datetime.now(),
                'total_records_synced': stats["total_rows"]
            })
            
            # Update sync log
            sync_log = self.env['etl.sync.log'].search([
                ('table_id', '=', table_config.id),
                ('status', '=', 'running')
            ], limit=1)
            
            if sync_log:
                sync_log.write({
                    'end_time': fields.Datetime.now(),
                    'status': 'success',
                    'total_records': stats["total_rows"],
                    'new_records': stats["new_rows"],
                    'updated_records': stats["updated_rows"]
                })
            
            # Clear job state
            self._clear_job_state(table_config)
            
            return {
                'status': 'complete',
                'stats': stats,
                'total_time': total_time
            }
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in full loading process: {error_message}")
            _logger.error(traceback.format_exc())
            
            # Update table status
            table_config.write({
                'job_status': 'failed',
                'last_sync_status': 'failed',
                'last_sync_message': f'Error in loading phase: {error_message}'
            })
            
            return {
                'status': 'failed',
                'error': error_message
            }
    
    @api.model
    def _force_unstick_jobs(self, table_config_id):
        """
        Simple function to force stuck jobs to be requeued.
        
        Args:
            table_config_id: ID of the ETL table config
            
        Returns:
            bool: True if any jobs were unstuck, False otherwise
        """
        try:
            # Find jobs that might be stuck (running for more than 5 minutes)
            five_mins_ago = fields.Datetime.now() - timedelta(minutes=5)
            
            self.env.cr.execute("""
                SELECT id, uuid, date_created
                FROM queue_job
                WHERE state IN ('pending', 'enqueued', 'started') 
                AND date_created < %s
                AND name LIKE %s
                ORDER BY date_created ASC
                LIMIT 10
            """, (five_mins_ago, f"%{table_config_id}%"))
            
            stuck_jobs = self.env.cr.dictfetchall()
            
            if not stuck_jobs:
                return False
            
            _logger.warning(f"Found {len(stuck_jobs)} potentially stuck jobs")
            
            # For each stuck job, try to force it to be retried
            for job in stuck_jobs:
                _logger.warning(f"Forcing requeue of stuck job {job['uuid']} created at {job['date_created']}")
                
                # Update job state to be picked up again
                self.env.cr.execute("""
                    UPDATE queue_job
                    SET state = 'pending',
                        date_enqueued = NULL,
                        date_started = NULL,
                        retry = retry + 1
                    WHERE id = %s
                """, (job['id'],))
            
            # Commit changes
            self.env.cr.commit()
            
            return True
            
        except Exception as e:
            _logger.error(f"Error in force_unstick_jobs: {str(e)}")
            return False
    
    # @api.model
    # def _force_unstick_jobs(self, table_config_id):
    #     """Force unstick jobs that seem to be hanging"""
    #     _logger.info(f"Attempting to unstick jobs for table {table_config_id}")
        
    #     # Find jobs that might be stuck (running for more than 5 minutes)
    #     five_mins_ago = fields.Datetime.now() - timedelta(minutes=5)
        
    #     self.env.cr.execute("""
    #         SELECT id, uuid, date_created, eta
    #         FROM queue_job
    #         WHERE state IN ('pending', 'enqueued', 'started')
    #         AND date_created < %s
    #         AND name LIKE %s
    #         LIMIT 10
    #     """, (five_mins_ago, f"%{table_config_id}%"))
        
    #     stuck_jobs = self.env.cr.dictfetchall()
        
    #     if not stuck_jobs:
    #         _logger.info("No stuck jobs found")
    #         return False
        
    #     _logger.warning(f"Found {len(stuck_jobs)} potentially stuck jobs")
        
    #     # For each stuck job, try to force it to be retried
    #     for job in stuck_jobs:
    #         _logger.warning(f"Forcing requeue of stuck job {job['uuid']} created at {job['date_created']}")
            
    #         # Update job state to be picked up again
    #         self.env.cr.execute("""
    #             UPDATE queue_job
    #             SET state = 'pending',
    #                 date_enqueued = NULL,
    #                 date_started = NULL,
    #                 retry = retry + 1
    #             WHERE id = %s
    #         """, (job['id'],))
        
    #     # Commit changes
    #     self.env.cr.commit()
        
    #     return True
    
    
    @api.model
    def _extract_batch_to_csv_independent(self, table_config_id, sync_log_id, batch_num, offset, batch_size):
        """
        Extract a batch of data to CSV file as an independent job with its own transaction.
        This method is designed to be called as a queue job with its own transaction.
        
        Args:
            table_config_id: ID of the ETL source table
            sync_log_id: ID of the sync log (unused but kept for compatibility)
            batch_num: Batch number to extract
            offset: Offset in the source table
            batch_size: Number of records to extract
        
        Returns:
            dict: Status information about the extraction
        """
        # Create a new cursor to ensure this job has its own transaction
        with self.env.registry.cursor() as new_cr:
            env = api.Environment(new_cr, self.env.uid, self.env.context)
            
            # Get table config using new environment
            table_config = env['etl.source.table'].browse(table_config_id)
            if not table_config.exists():
                return {'error': 'Table config not found', 'batch': batch_num}
            
            # Get job state
            job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
            completed_batches = job_state.get('completed_batches', [])
            
            # Ensure completed_batches is a list
            if not isinstance(completed_batches, list):
                completed_batches = []
                job_state['completed_batches'] = []
                self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
            
            # Skip if already processed
            if batch_num in completed_batches:
                _logger.info(f"Batch {batch_num} already processed, skipping")
                return {'status': 'already_processed', 'batch_num': batch_num}
            
            try:
                _logger.info(f"STARTING extraction batch {batch_num} at offset {offset}")
                
                # Get connector service and config in the new environment
                connector_service = env['etl.database.connector.service']
                source_db = table_config.source_db_connection_id
                config = table_config.get_config_json()
                source_table = config['source_table']
                primary_key = config['primary_key']
                
                # Generate CSV filename
                csv_dir = self._get_csv_directory(table_config)
                csv_filename = os.path.join(csv_dir, f"{source_table}_batch_{batch_num}.csv")
                
                # Make sure directory exists
                os.makedirs(csv_dir, exist_ok=True)
                
                # Extract and transform batch to CSV
                _logger.info(f"Processing extraction of batch {batch_num} at offset {offset}")
                rows_processed = self._extract_transform_to_csv(
                    connector_service, source_db, config, primary_key,
                    offset, batch_size, csv_filename
                )
                
                if rows_processed > 0:
                    _logger.info(f"Extracted {rows_processed} rows to {csv_filename}")
                    
                    # CRITICAL FIX: Use a direct approach to get and update job state
                    # to avoid race conditions or missed updates
                    
                    # 1. Get a fresh copy of the job state
                    job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
                    
                    # 2. Ensure we have valid lists
                    if 'completed_batches' not in job_state or not isinstance(job_state['completed_batches'], list):
                        job_state['completed_batches'] = []
                    
                    if 'csv_files' not in job_state or not isinstance(job_state['csv_files'], list):
                        job_state['csv_files'] = []
                    
                    # 3. Add current batch to completed list if not already there
                    if batch_num not in job_state['completed_batches']:
                        job_state['completed_batches'].append(batch_num)
                    
                    # 4. Add CSV file to the list if not already there
                    if csv_filename not in job_state['csv_files']:
                        job_state['csv_files'].append(csv_filename)
                    
                    # 5. Save the updated state
                    self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                    
                    # Commit to ensure changes are saved
                    new_cr.commit()
                    
                    _logger.info(f"COMPLETED extraction batch {batch_num} with {rows_processed} rows")
                    
                    return {
                        'status': 'success',
                        'batch_num': batch_num,
                        'rows_processed': rows_processed,
                        'csv_filename': csv_filename
                    }
                else:
                    _logger.info(f"No rows processed for batch {batch_num}")
                    
                    # Even with no rows, mark as complete to avoid stuck job
                    job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
                    
                    if 'completed_batches' not in job_state or not isinstance(job_state['completed_batches'], list):
                        job_state['completed_batches'] = []
                    
                    if batch_num not in job_state['completed_batches']:
                        job_state['completed_batches'].append(batch_num)
                        self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                    
                    # Commit to ensure changes are saved
                    new_cr.commit()
                    
                    return {
                        'status': 'no_data',
                        'batch_num': batch_num
                    }
            except Exception as e:
                error_message = str(e)
                _logger.error(f"Error extracting batch {batch_num} to CSV: {error_message}")
                
                # Try to mark batch as failed in job state
                try:
                    job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
                    
                    if 'failed_batches' not in job_state or not isinstance(job_state['failed_batches'], list):
                        job_state['failed_batches'] = []
                    
                    if batch_num not in job_state['failed_batches']:
                        job_state['failed_batches'].append(batch_num)
                        self._save_job_state_direct_sql(new_cr, table_config_id, job_state)
                    
                    # Commit to ensure changes are saved
                    new_cr.commit()
                except Exception as inner_e:
                    _logger.error(f"Error updating job state after batch failure: {str(inner_e)}")
                
                return {
                    'status': 'failed',
                    'batch_num': batch_num,
                    'error': error_message
                }

    # @api.model
    # def _extract_batch_to_csv_independent(self, table_config_id, sync_log_id, batch_num, offset, batch_size):
    #     """
    #     Extract a batch of data to CSV file as an independent job with its own transaction.
    #     This method is designed to be called as a queue job, one per batch.
    #     """
    #     # Create a new cursor to ensure this job has its own transaction
    #     with self.env.registry.cursor() as new_cr:
    #         env = api.Environment(new_cr, self.env.uid, self.env.context)
            
    #         # Get table config using new environment
    #         table_config = env['etl.source.table'].browse(table_config_id)
    #         if not table_config.exists():
    #             return {'error': 'Table config not found', 'batch': batch_num}
            
    #         # Get job state
    #         job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
    #         completed_batches = job_state.get('completed_batches', [])
            
    #         # Skip if already processed
    #         if batch_num in completed_batches:
    #             _logger.info(f"Batch {batch_num+1} already processed, skipping")
    #             return {'status': 'already_processed', 'batch_num': batch_num}
            
    #         try:
    #             _logger.info(f"Starting independent extraction of batch {batch_num+1} at offset {offset}")
                
    #             # Get connector service and config in the new environment
    #             connector_service = env['etl.database.connector.service']
    #             source_db = table_config.source_db_connection_id
    #             config = table_config.get_config_json()
    #             source_table = config['source_table']
    #             primary_key = config['primary_key']
                
    #             # Generate CSV filename
    #             csv_dir = self._get_csv_directory(table_config)
    #             csv_filename = os.path.join(csv_dir, f"{source_table}_batch_{batch_num}.csv")
                
    #             # Make sure directory exists
    #             os.makedirs(csv_dir, exist_ok=True)
                
    #             # Extract and transform batch to CSV
    #             rows_processed = self._extract_transform_to_csv(
    #                 connector_service, source_db, config, primary_key,
    #                 offset, batch_size, csv_filename
    #             )
                
    #             if rows_processed > 0:
    #                 _logger.info(f"Extracted {rows_processed} rows to {csv_filename}")
                    
    #                 # Update job state
    #                 # NOTE: We need to get a fresh copy of the job state since it might have changed
    #                 job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
    #                 completed_batches = job_state.get('completed_batches', [])
    #                 csv_files = job_state.get('csv_files', [])
                    
    #                 if batch_num not in completed_batches:
    #                     completed_batches.append(batch_num)
    #                 if csv_filename not in csv_files:
    #                     csv_files.append(csv_filename)
                    
    #                 # Update with a direct SQL query to avoid ORM issues
    #                 self._save_job_state_direct_sql(
    #                     new_cr, 
    #                     table_config_id,
    #                     {
    #                         'phase': 'extract',
    #                         'total_rows': job_state.get('total_rows', 0),
    #                         'total_batches': job_state.get('total_batches', 0),
    #                         'batch_size': job_state.get('batch_size', batch_size),
    #                         'start_time': job_state.get('start_time', time.time()),
    #                         'next_batch_to_queue': job_state.get('next_batch_to_queue', 0),
    #                         'csv_files': csv_files,
    #                         'completed_batches': completed_batches
    #                     }
    #                 )
                    
    #                 # Commit to ensure progress is saved
    #                 new_cr.commit()
                    
    #                 return {
    #                     'status': 'success',
    #                     'batch_num': batch_num,
    #                     'rows_processed': rows_processed,
    #                     'csv_filename': csv_filename
    #                 }
    #             else:
    #                 _logger.info(f"No rows processed for batch {batch_num+1}")
                    
    #                 # Even with no rows, mark as complete to avoid stuck job
    #                 job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
    #                 completed_batches = job_state.get('completed_batches', [])
                    
    #                 if batch_num not in completed_batches:
    #                     completed_batches.append(batch_num)
                    
    #                 # Update job state directly
    #                 self._save_job_state_direct_sql(
    #                     new_cr,
    #                     table_config_id,
    #                     {
    #                         'phase': 'extract',
    #                         'total_rows': job_state.get('total_rows', 0),
    #                         'total_batches': job_state.get('total_batches', 0),
    #                         'batch_size': job_state.get('batch_size', batch_size),
    #                         'start_time': job_state.get('start_time', time.time()),
    #                         'next_batch_to_queue': job_state.get('next_batch_to_queue', 0),
    #                         'csv_files': job_state.get('csv_files', []),
    #                         'completed_batches': completed_batches
    #                     }
    #                 )
                    
    #                 # Commit to ensure progress is saved
    #                 new_cr.commit()
                    
    #                 return {
    #                     'status': 'no_data',
    #                     'batch_num': batch_num
    #                 }
    #         except Exception as e:
    #             error_message = str(e)
    #             _logger.error(f"Error extracting batch {batch_num+1} to CSV: {error_message}")
    #             _logger.error(traceback.format_exc())
                
    #             # Try to mark batch as failed in job state
    #             try:
    #                 job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
    #                 failed_batches = job_state.get('failed_batches', [])
                    
    #                 if batch_num not in failed_batches:
    #                     failed_batches.append(batch_num)
                    
    #                 self._save_job_state_direct_sql(
    #                     new_cr,
    #                     table_config_id,
    #                     {
    #                         'phase': 'extract',
    #                         'total_rows': job_state.get('total_rows', 0),
    #                         'total_batches': job_state.get('total_batches', 0),
    #                         'batch_size': job_state.get('batch_size', batch_size),
    #                         'start_time': job_state.get('start_time', time.time()),
    #                         'next_batch_to_queue': job_state.get('next_batch_to_queue', 0),
    #                         'csv_files': job_state.get('csv_files', []),
    #                         'completed_batches': job_state.get('completed_batches', []),
    #                         'failed_batches': failed_batches
    #                     }
    #                 )
                    
    #                 # Commit to ensure progress is saved
    #                 new_cr.commit()
    #             except Exception as inner_e:
    #                 _logger.error(f"Error updating job state after batch failure: {str(inner_e)}")
                
    #             return {
    #                 'status': 'failed',
    #                 'batch_num': batch_num,
    #                 'error': error_message
    #             }
    
    @api.model
    def _get_job_state_with_cursor(self, cursor, table_config_id):
        """
        Get job state using direct SQL with minimal processing.
        
        Args:
            cursor: Database cursor
            table_config_id: ID of the ETL table config
            
        Returns:
            dict: Job state information
        """
        try:
            # Retrieve job state
            cursor.execute("""
                SELECT value FROM ir_config_parameter 
                WHERE key = %s
            """, (f"etl.job_state.{table_config_id}",))
            
            result = cursor.fetchone()
            
            if result and result[0]:
                try:
                    state = json.loads(result[0])
                    
                    # Ensure critical lists exist
                    for key in ['completed_batches', 'csv_files', 'failed_batches']:
                        if key in state and not isinstance(state[key], list):
                            state[key] = []
                    
                    return state
                except json.JSONDecodeError:
                    _logger.error(f"Invalid JSON in job state for table {table_config_id}")
                    return {'phase': 'extract'}
            else:
                # No state found
                return {
                    'phase': 'extract',
                    'start_time': time.time(),
                    'completed_batches': [],
                    'csv_files': [],
                    'failed_batches': []
                }
        except Exception as e:
            _logger.error(f"Error retrieving job state: {str(e)}")
            return {'phase': 'extract'}

    # @api.model
    # def _save_job_state_direct_sql(self, cursor, table_config_id, state):
    #     """
    #     Save job state using direct SQL with type validation
    #     to prevent serialization issues.
        
    #     Args:
    #         cursor: Database cursor
    #         table_config_id: ID of the ETL table config
    #         state: Job state dictionary to save
            
    #     Returns:
    #         bool: Success status
    #     """
    #     try:
    #         # Validate critical lists exist and are actually lists
    #         for key in ['completed_batches', 'csv_files', 'all_files', 'processed_files', 'failed_batches']:
    #             if key in state and not isinstance(state[key], list):
    #                 _logger.warning(f"Converting non-list {key} to empty list")
    #                 state[key] = []
            
    #         # SPECIAL FIX: If we have a weird value in total_batches, fix it
    #         if 'total_batches' in state and (not isinstance(state['total_batches'], int) or state['total_batches'] <= 0):
    #             if 'total_rows' in state and isinstance(state['total_rows'], int) and state['total_rows'] > 0:
    #                 batch_size = state.get('batch_size', 10000)
    #                 state['total_batches'] = (state['total_rows'] + batch_size - 1) // batch_size
    #                 _logger.info(f"Fixed total_batches to {state['total_batches']} based on total_rows")
    #             else:
    #                 # Set a reasonable default
    #                 state['total_batches'] = 507  # Use the value from logs
    #                 _logger.warning(f"Setting total_batches explicitly to {state['total_batches']}")
            
    #         # Convert state to JSON
    #         try:
    #             state_json = json.dumps(state)
    #         except Exception as json_error:
    #             _logger.error(f"Error serializing job state to JSON: {str(json_error)}")
    #             return False
            
    #         state_key = f"etl.job_state.{table_config_id}"
            
    #         # Check if the record exists
    #         cursor.execute("SELECT 1 FROM ir_config_parameter WHERE key = %s", (state_key,))
    #         exists = cursor.fetchone() is not None
            
    #         if exists:
    #             # Update existing record
    #             cursor.execute("""
    #                 UPDATE ir_config_parameter
    #                 SET value = %s,
    #                     write_date = NOW(),
    #                     write_uid = %s
    #                 WHERE key = %s
    #             """, (state_json, self.env.uid, state_key))
    #         else:
    #             # Insert new record
    #             cursor.execute("""
    #                 INSERT INTO ir_config_parameter (key, value, create_date, write_date, create_uid, write_uid)
    #                 VALUES (%s, %s, NOW(), NOW(), %s, %s)
    #             """, (state_key, state_json, self.env.uid, self.env.uid))
            
    #         return True
    #     except Exception as e:
    #         _logger.error(f"Error in _save_job_state_direct_sql: {str(e)}")
    #         return False

    # def _get_job_state_with_cursor(self, cursor, table_config_id):
    #     """Get job state using direct SQL to avoid ORM conflicts"""
    #     cursor.execute("""
    #         SELECT value FROM ir_config_parameter 
    #         WHERE key = %s
    #     """, (f"etl.job_state.{table_config_id}",))
        
    #     result = cursor.fetchone()
    #     if result and result[0]:
    #         try:
    #             return json.loads(result[0])
    #         except:
    #             return {}
    #     return {}
    
    @api.model
    def _save_job_state_direct_sql(self, cursor, table_config_id, state):
        """
        Save job state using direct SQL with minimal processing.
        
        Args:
            cursor: Database cursor
            table_config_id: ID of the ETL table config
            state: Job state dictionary to save
            
        Returns:
            bool: Success status
        """
        try:
            # Ensure critical lists are actually lists
            for key in ['completed_batches', 'csv_files', 'failed_batches']:
                if key in state and not isinstance(state[key], list):
                    state[key] = []
            
            # Convert state to JSON
            try:
                state_json = json.dumps(state)
            except Exception as json_error:
                _logger.error(f"Error serializing job state to JSON: {str(json_error)}")
                return False
            
            state_key = f"etl.job_state.{table_config_id}"
            
            # Check if the record exists
            cursor.execute("SELECT 1 FROM ir_config_parameter WHERE key = %s", (state_key,))
            exists = cursor.fetchone() is not None
            
            if exists:
                # Update existing record
                cursor.execute("""
                    UPDATE ir_config_parameter
                    SET value = %s,
                        write_date = NOW(),
                        write_uid = %s
                    WHERE key = %s
                """, (state_json, self.env.uid, state_key))
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO ir_config_parameter (key, value, create_date, write_date, create_uid, write_uid)
                    VALUES (%s, %s, NOW(), NOW(), %s, %s)
                """, (state_key, state_json, self.env.uid, self.env.uid))
            
            return True
        except Exception as e:
            _logger.error(f"Error saving job state: {str(e)}")
            return False

    # def _save_job_state_direct_sql(self, cursor, table_config_id, state):
    #     """Save job state using direct SQL to avoid ORM conflicts"""
    #     state_json = json.dumps(state)
    #     state_key = f"etl.job_state.{table_config_id}"
        
    #     # Try to update existing record
    #     cursor.execute("""
    #         UPDATE ir_config_parameter
    #         SET value = %s,
    #             write_date = NOW()
    #         WHERE key = %s
    #     """, (state_json, state_key))
        
    #     # If no record was updated, insert a new one
    #     if cursor.rowcount == 0:
    #         cursor.execute("""
    #             INSERT INTO ir_config_parameter (key, value, create_date, write_date, create_uid, write_uid)
    #             VALUES (%s, %s, NOW(), NOW(), %s, %s)
    #         """, (state_key, state_json, self.env.uid, self.env.uid))
        
    #     return True
    
    # @api.model
    # def _start_csv_loading_jobs_with_cursor(self, cursor, table_config_id, csv_files):
    #     """
    #     Start loading jobs with a given cursor to maintain transaction context.
    #     This method safely initiates the loading phase after extraction is complete.
        
    #     Args:
    #         cursor: Database cursor
    #         table_config_id: ID of the ETL table config
    #         csv_files: List of CSV files to load
            
    #     Returns:
    #         dict: Status information
    #     """
    #     env = api.Environment(cursor, self.env.uid, self.env.context)
    #     table_config = env['etl.source.table'].browse(table_config_id)
        
    #     if not table_config.exists():
    #         _logger.error(f"Table config {table_config_id} not found")
    #         return {'error': 'Table config not found'}
        
    #     # CRITICAL FIX: Make sure we have valid CSV files
    #     valid_files = [f for f in csv_files if os.path.exists(f)]
    #     if len(valid_files) < len(csv_files):
    #         _logger.warning(f"Some CSV files are missing: {len(csv_files) - len(valid_files)} out of {len(csv_files)}")
    #         csv_files = valid_files
        
    #     if not csv_files:
    #         _logger.warning(f"No CSV files to load for {table_config.name}")
    #         # Return properly formatted stats
    #         return {
    #             'total_rows': 0,
    #             'new_rows': 0,
    #             'updated_rows': 0,
    #             'unchanged_rows': 0,
    #             'error_rows': 0,
    #             'execution_time': 0,
    #             'status': 'no_files'
    #         }
        
    #     _logger.info(f"Starting loading phase with {len(csv_files)} CSV files for {table_config.name}")
        
    #     # Get job state
    #     job_state = self._get_job_state_with_cursor(cursor, table_config_id)
        
    #     # CRITICAL FIX: Set both all_files and csv_files to the same list
    #     job_state['all_files'] = csv_files
    #     job_state['csv_files'] = csv_files
        
    #     # Initialize processed_files as an empty list
    #     processed_files = []
    #     job_state['processed_files'] = processed_files
        
    #     # Save the synchronized state
    #     self._save_job_state_direct_sql(cursor, table_config_id, job_state)
        
    #     # Create coordinator job to monitor loading progress
    #     monitor_job = env['etl.fast.sync.postgres'].with_delay(
    #         description=f"CSV Loading Coordinator: {table_config.name}",
    #         channel="etl_loader_monitor",
    #         priority=5
    #     )._monitor_loading_progress(table_config_id)
        
    #     # Update table status
    #     table_config.write({
    #         'progress_percentage': 50,  # Start at 50% since extraction is complete
    #         'last_sync_message': f'Starting loading phase with {len(csv_files)} CSV files'
    #     })
        
    #     # Queue loading jobs with staggered start
    #     # Start with 5 concurrent files as specified
    #     max_concurrent = 5
    #     initial_files = csv_files[:max_concurrent]
        
    #     for i, csv_file in enumerate(initial_files):
    #         # Skip if file doesn't exist
    #         if not os.path.exists(csv_file):
    #             _logger.warning(f"File not found, skipping: {csv_file}")
    #             continue
                
    #         # Start jobs with a delay based on position
    #         delay_seconds = i * 10  # 10 seconds between jobs
            
    #         # Generate unique identity key for this file
    #         file_id = os.path.basename(csv_file).replace('.csv', '')
    #         identity_key = f"load_file_{table_config_id}_{file_id}"
            
    #         env['etl.fast.sync.postgres'].with_delay(
    #             description=f"CSV Loading File {i+1}/{len(csv_files)}: {table_config.name}",
    #             channel="etl_loader",
    #             priority=10,
    #             identity_key=identity_key,
    #             eta=datetime.now() + timedelta(seconds=delay_seconds)
    #         )._load_csv_file_to_db_independent(table_config_id, csv_file)
        
    #     # Update job state with initial files queued
    #     job_state['next_file_to_queue'] = len(initial_files)
    #     self._save_job_state_direct_sql(cursor, table_config_id, job_state)
        
    #     # Commit to ensure changes are saved
    #     cursor.commit()
        
    #     # Return properly formatted stats
    #     return {
    #         'status': 'loading_started',
    #         'files_to_process': len(csv_files),
    #         'coordinator_job': monitor_job.uuid
    #     }
    
    @api.model
    def _start_csv_loading_jobs_with_cursor(self, cursor, table_config_id, csv_files):
        """
        Start loading jobs with a given cursor to maintain transaction context.
        This method safely initiates the loading phase after extraction is complete.
        
        Args:
            cursor: Database cursor
            table_config_id: ID of the ETL table config
            csv_files: List of CSV files to load
            
        Returns:
            dict: Status information
        """
        env = api.Environment(cursor, self.env.uid, self.env.context)
        table_config = env['etl.source.table'].browse(table_config_id)
        
        if not table_config.exists():
            _logger.error(f"Table config {table_config_id} not found")
            return {'error': 'Table config not found'}
        
        # CRITICAL FIX: Make sure we have valid CSV files
        valid_files = [f for f in csv_files if os.path.exists(f)]
        if len(valid_files) < len(csv_files):
            _logger.warning(f"Some CSV files are missing: {len(csv_files) - len(valid_files)} out of {len(csv_files)}")
            csv_files = valid_files
        
        if not csv_files:
            _logger.warning(f"No CSV files to load for {table_config.name}")
            # Return properly formatted stats
            return {
                'total_rows': 0,
                'new_rows': 0,
                'updated_rows': 0,
                'unchanged_rows': 0,
                'error_rows': 0,
                'execution_time': 0,
                'status': 'no_files'
            }
        
        _logger.info(f"Starting loading phase with {len(csv_files)} CSV files for {table_config.name}")
        
        # Get job state
        job_state = self._get_job_state_with_cursor(cursor, table_config_id)
        
        # CRITICAL FIX: Set both all_files and csv_files to the same list
        job_state['all_files'] = csv_files
        job_state['csv_files'] = csv_files
        
        # Initialize processed_files as an empty list
        processed_files = []
        job_state['processed_files'] = processed_files
        
        # Save the synchronized state
        self._save_job_state_direct_sql(cursor, table_config_id, job_state)
        
        # Create coordinator job to monitor loading progress
        monitor_job = env['etl.fast.sync.postgres'].with_delay(
            description=f"CSV Loading Coordinator: {table_config.name}",
            channel="etl_loader_monitor",
            priority=5
        )._monitor_loading_progress(table_config_id)
        
        # Update table status
        table_config.write({
            'progress_percentage': 50,  # Start at 50% since extraction is complete
            'last_sync_message': f'Starting loading phase with {len(csv_files)} CSV files'
        })
        
        # Queue loading jobs with staggered start
        # Start with 5 concurrent files as configured
        max_concurrent = 10
        initial_files = csv_files[:max_concurrent]
        
        for i, csv_file in enumerate(initial_files):
            # Skip if file doesn't exist
            if not os.path.exists(csv_file):
                _logger.warning(f"File not found, skipping: {csv_file}")
                continue
                
            # Start jobs with a delay based on position
            delay_seconds = i * 5  # 5 seconds between jobs
            
            # Generate unique identity key for this file with timestamp
            file_id = os.path.basename(csv_file).replace('.csv', '')
            identity_key = f"load_file_{table_config_id}_{file_id}_{int(time.time())}"
            
            env['etl.fast.sync.postgres'].with_delay(
                description=f"CSV Loading File {i+1}/{len(csv_files)}: {table_config.name}",
                channel="etl_loader",
                priority=10,
                identity_key=identity_key,
                eta=datetime.now() + timedelta(seconds=delay_seconds)
            )._load_csv_file_to_db_independent(table_config_id, csv_file)
        
        # Update job state with initial files queued
        job_state['next_file_to_queue'] = len(initial_files)
        self._save_job_state_direct_sql(cursor, table_config_id, job_state)
        
        # Commit to ensure changes are saved
        cursor.commit()
        
        # Return properly formatted stats
        return {
            'status': 'loading_started',
            'files_to_process': len(csv_files),
            'coordinator_job': monitor_job.uuid
        }
    
    @api.model
    def _load_csv_file_to_db_independent(self, table_config_id, csv_file):
        """
        Independent job to load a CSV file into the database.
        Uses its own transaction for reliability.
        
        Args:
            table_config_id: ID of the ETL table configuration
            csv_file: Path to the CSV file to load
            
        Returns:
            dict: Status information about the loading operation
        """
        # Create a new cursor to ensure this job has its own transaction
        with self.env.registry.cursor() as new_cr:
            env = api.Environment(new_cr, self.env.uid, self.env.context)
            
            # Get table config using new environment
            table_config = env['etl.source.table'].browse(table_config_id)
            if not table_config.exists():
                return {'error': 'Table config not found'}
            
            # Get job state
            job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
            processed_files = job_state.get('processed_files', [])
            
            # Skip if already processed
            if csv_file in processed_files:
                _logger.info(f"File {csv_file} already processed, skipping")
                return {'status': 'already_processed', 'file': csv_file}
            
            try:
                start_time = time.time()
                
                # Get connector service and config
                connector_service = env['etl.database.connector.service']
                target_db = table_config.target_db_connection_id
                config = table_config.get_config_json()
                target_table = config['target_table']
                primary_key = config['primary_key']
                
                # Check if the file exists
                if not os.path.exists(csv_file):
                    _logger.warning(f"CSV file not found: {csv_file}")
                    return {'status': 'file_not_found', 'file': csv_file}
                
                # Create transaction manager
                tx_manager = env['etl.transaction']
                
                # Load the CSV file
                stats = self._load_csv_file_to_database(
                    connector_service, target_db, target_table, primary_key, csv_file, tx_manager
                )
                
                # CRITICAL FIX: Use direct approach to update job state to avoid race conditions
                
                # 1. Get a fresh copy of the job state
                job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
                
                # 2. Ensure we have valid lists
                if 'processed_files' not in job_state or not isinstance(job_state['processed_files'], list):
                    job_state['processed_files'] = []
                    
                if 'current_stats' not in job_state or not isinstance(job_state['current_stats'], dict):
                    job_state['current_stats'] = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
                
                # 3. Update processed files list if not already there
                if csv_file not in job_state['processed_files']:
                    job_state['processed_files'].append(csv_file)
                
                # 4. Update cumulative stats
                job_state['current_stats']['total_rows'] = job_state['current_stats'].get('total_rows', 0) + stats.get('total_rows', 0)
                job_state['current_stats']['new_rows'] = job_state['current_stats'].get('new_rows', 0) + stats.get('new_rows', 0)
                job_state['current_stats']['updated_rows'] = job_state['current_stats'].get('updated_rows', 0) + stats.get('updated_rows', 0)
                
                # 5. Save the updated state with all necessary fields preserved
                self._save_job_state_direct_sql(
                    new_cr,
                    table_config_id,
                    {
                        'phase': 'load',
                        'csv_files': job_state.get('csv_files', []),
                        'all_files': job_state.get('all_files', job_state.get('csv_files', [])),
                        'processed_files': job_state['processed_files'],
                        'next_file_to_queue': job_state.get('next_file_to_queue', 0),
                        'current_stats': job_state['current_stats'],
                        'start_time': job_state.get('start_time', time.time())
                    }
                )
                
                # Commit to ensure changes are saved
                new_cr.commit()
                
                # Log results
                _logger.info(f"Loaded CSV file {csv_file}: {stats['total_rows']} rows "
                            f"({stats['new_rows']} new, {stats['updated_rows']} updated)")
                
                return {
                    'status': 'success',
                    'file': csv_file,
                    'stats': stats,
                    'execution_time': time.time() - start_time
                }
                
            except Exception as e:
                error_message = str(e)
                _logger.error(f"Error loading CSV file {csv_file}: {error_message}")
                
                # Try to mark the file as failed
                try:
                    job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
                    
                    if 'failed_files' not in job_state or not isinstance(job_state['failed_files'], list):
                        job_state['failed_files'] = []
                    
                    if csv_file not in job_state['failed_files']:
                        job_state['failed_files'].append(csv_file)
                    
                    self._save_job_state_direct_sql(
                        new_cr,
                        table_config_id,
                        {
                            'phase': 'load',
                            'csv_files': job_state.get('csv_files', []),
                            'all_files': job_state.get('all_files', job_state.get('csv_files', [])),
                            'processed_files': job_state.get('processed_files', []),
                            'failed_files': job_state['failed_files'],
                            'next_file_to_queue': job_state.get('next_file_to_queue', 0),
                            'current_stats': job_state.get('current_stats', {}),
                            'start_time': job_state.get('start_time', time.time()),
                            'last_error': error_message
                        }
                    )
                    
                    # Commit to ensure changes are saved
                    new_cr.commit()
                except Exception as inner_e:
                    _logger.error(f"Error updating job state after file loading failure: {str(inner_e)}")
                
                return {
                    'status': 'failed',
                    'file': csv_file,
                    'error': error_message
                }

    # @api.model
    # def _load_csv_file_to_db_independent(self, table_config_id, csv_file):
    #     """
    #     Independent job to load a CSV file into the database.
    #     Uses its own transaction for reliability.
        
    #     Args:
    #         table_config_id: ID of the ETL table configuration
    #         csv_file: Path to the CSV file to load
            
    #     Returns:
    #         dict: Status information about the loading operation
    #     """
    #     # Create a new cursor to ensure this job has its own transaction
    #     with self.env.registry.cursor() as new_cr:
    #         env = api.Environment(new_cr, self.env.uid, self.env.context)
            
    #         # Get table config using new environment
    #         table_config = env['etl.source.table'].browse(table_config_id)
    #         if not table_config.exists():
    #             return {'error': 'Table config not found'}
            
    #         # Get job state
    #         job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
    #         processed_files = job_state.get('processed_files', [])
            
    #         # Skip if already processed
    #         if csv_file in processed_files:
    #             _logger.info(f"File {csv_file} already processed, skipping")
    #             return {'status': 'already_processed', 'file': csv_file}
            
    #         try:
    #             start_time = time.time()
                
    #             # Get connector service and config
    #             connector_service = env['etl.database.connector.service']
    #             target_db = table_config.target_db_connection_id
    #             config = table_config.get_config_json()
    #             target_table = config['target_table']
    #             primary_key = config['primary_key']
                
    #             # Check if the file exists
    #             if not os.path.exists(csv_file):
    #                 _logger.warning(f"CSV file not found: {csv_file}")
    #                 return {'status': 'file_not_found', 'file': csv_file}
                
    #             # Create transaction manager
    #             tx_manager = env['etl.transaction']
                
    #             # Load the CSV file
    #             stats = self._load_csv_file_to_database(
    #                 connector_service, target_db, target_table, primary_key, csv_file, tx_manager
    #             )
                
    #             # Update job state with processed file
    #             job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
    #             processed_files = job_state.get('processed_files', [])
    #             current_stats = job_state.get('current_stats', {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0})
                
    #             # Update processed files list
    #             if csv_file not in processed_files:
    #                 processed_files.append(csv_file)
                
    #             # Update cumulative stats
    #             current_stats['total_rows'] = current_stats.get('total_rows', 0) + stats.get('total_rows', 0)
    #             current_stats['new_rows'] = current_stats.get('new_rows', 0) + stats.get('new_rows', 0)
    #             current_stats['updated_rows'] = current_stats.get('updated_rows', 0) + stats.get('updated_rows', 0)
                
    #             # Save updated job state
    #             self._save_job_state_direct_sql(
    #                 new_cr,
    #                 table_config_id,
    #                 {
    #                     'phase': 'load',
    #                     'csv_files': job_state.get('csv_files', []),
    #                     'all_files': job_state.get('all_files', job_state.get('csv_files', [])),
    #                     'processed_files': processed_files,
    #                     'next_file_to_queue': job_state.get('next_file_to_queue', 0),
    #                     'current_stats': current_stats,
    #                     'start_time': job_state.get('start_time', time.time())
    #                 }
    #             )
                
    #             # Commit to ensure changes are saved
    #             new_cr.commit()
                
    #             # Log results
    #             _logger.info(f"Loaded CSV file {csv_file}: {stats['total_rows']} rows "
    #                         f"({stats['new_rows']} new, {stats['updated_rows']} updated)")
                
    #             return {
    #                 'status': 'success',
    #                 'file': csv_file,
    #                 'stats': stats,
    #                 'execution_time': time.time() - start_time
    #             }
                
    #         except Exception as e:
    #             error_message = str(e)
    #             _logger.error(f"Error loading CSV file {csv_file}: {error_message}")
                
    #             # Try to mark the file as failed
    #             try:
    #                 job_state = self._get_job_state_with_cursor(new_cr, table_config_id)
    #                 failed_files = job_state.get('failed_files', [])
                    
    #                 if csv_file not in failed_files:
    #                     failed_files.append(csv_file)
                    
    #                 self._save_job_state_direct_sql(
    #                     new_cr,
    #                     table_config_id,
    #                     {
    #                         'phase': 'load',
    #                         'csv_files': job_state.get('csv_files', []),
    #                         'all_files': job_state.get('all_files', job_state.get('csv_files', [])),
    #                         'processed_files': job_state.get('processed_files', []),
    #                         'failed_files': failed_files,
    #                         'next_file_to_queue': job_state.get('next_file_to_queue', 0),
    #                         'current_stats': job_state.get('current_stats', {}),
    #                         'start_time': job_state.get('start_time', time.time()),
    #                         'last_error': error_message
    #                     }
    #                 )
                    
    #                 # Commit to ensure changes are saved
    #                 new_cr.commit()
    #             except Exception as inner_e:
    #                 _logger.error(f"Error updating job state after file loading failure: {str(inner_e)}")
                
    #             return {
    #                 'status': 'failed',
    #                 'file': csv_file,
    #                 'error': error_message
    #             }
            
    def _postgresql_transaction_settings(self, cursor):
        """Configure optimal PostgreSQL transaction settings for ETL operations"""
        try:
            # Use READ COMMITTED isolation level which allows more concurrency while
            # still providing appropriate transaction isolation for ETL
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            
            # Increase statement timeout for long-running operations (if supported)
            try:
                cursor.execute("SET statement_timeout = '600000'") # 10 minutes
            except:
                _logger.info("Could not set statement_timeout, ignoring")
            
            # Set lock_timeout to prevent indefinite waiting on locks (if supported)
            try:
                cursor.execute("SET lock_timeout = '60000'")  # 1 minute
            except:
                _logger.info("Could not set lock_timeout, ignoring")
            
            # NOTE: Removed vacuum_cleanup_index_scale_factor as it's not supported
            
            return True
        except Exception as e:
            _logger.warning(f"Could not configure PostgreSQL transaction settings: {str(e)}")
            return False
    
    def _get_advisory_lock(self, cursor, table_config_id, lock_type='state'):
        """Try to get a PostgreSQL advisory lock without blocking"""
        # Generate a unique lock ID for this table and lock type
        # We need different lock IDs for different lock types to avoid conflicts
        lock_base = abs(hash(f"etl_{lock_type}_{table_config_id}")) % 2147483647
        
        # Try to acquire the lock
        cursor.execute("SELECT pg_try_advisory_xact_lock(%s)", (lock_base,))
        lock_acquired = cursor.fetchone()[0]
        
        if not lock_acquired:
            _logger.info(f"Could not acquire advisory lock for table {table_config_id}, type {lock_type}")
        
        return lock_acquired

    def _with_savepoint(self, cursor, operation_name, callback, *args, **kwargs):
        """
        Safely execute an operation within a savepoint with proper error handling.
        
        Args:
            cursor: Database cursor
            operation_name: Name for the operation (used in savepoint name)
            callback: Function to call within the savepoint
            *args, **kwargs: Arguments to pass to the callback
            
        Returns:
            Result from the callback, or None if an error occurred
        """
        # Create a unique savepoint name
        unique_id = int(time.time() * 1000) % 10000
        savepoint_name = f"sp_{operation_name}_{unique_id}"
        
        try:
            # Create savepoint
            cursor.execute(f"SAVEPOINT {savepoint_name}")
            
            # Call the callback
            result = callback(*args, **kwargs)
            
            # Release savepoint on success
            cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            
            return result
        except Exception as e:
            # Try to rollback to savepoint
            try:
                cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                _logger.warning(f"Rolled back to savepoint {savepoint_name} due to error: {str(e)}")
            except Exception as rollback_e:
                _logger.error(f"Failed to rollback to savepoint: {str(rollback_e)}")
                # Continue with the outer exception
            
            # Re-raise the original exception
            raise

    # Improved transaction retry logic for critical database operations

    def _with_retry(self, operation, max_retries=3, retry_on=None):
        """
        Execute an operation with automatic retries on specific exceptions.
        
        Args:
            operation: Function to execute
            max_retries: Maximum number of retry attempts
            retry_on: List of exception types to retry on, defaults to serialization failures
            
        Returns:
            Result from the operation
        """
        if retry_on is None:
            # Default to retrying on serialization failures
            import psycopg2.errors
            retry_on = [psycopg2.errors.SerializationFailure]
        
        retry_count = 0
        last_error = None
        
        while retry_count < max_retries:
            try:
                return operation()
            except Exception as e:
                retry_count += 1
                last_error = e
                
                # Check if this exception should trigger a retry
                should_retry = any(isinstance(e, exc_type) for exc_type in retry_on)
                
                if not should_retry or retry_count >= max_retries:
                    # Don't retry or out of retries
                    raise
                
                # Calculate backoff time with jitter
                backoff = (2 ** retry_count) * (0.1 + random.random() * 0.1)
                
                _logger.warning(f"Operation failed with {type(e).__name__}, "
                            f"retry {retry_count}/{max_retries} after {backoff:.2f}s: {str(e)}")
                
                # Wait before retrying
                time.sleep(backoff)
        
        # If we get here, we've exhausted retries
        raise last_error

    # Safe IR parameter access with locking 

    def _get_param_with_retry(self, cr, key, default=None):
        """Get ir.config_parameter with retry logic for concurrent access"""
        def _get_param():
            cr.execute("SELECT value FROM ir_config_parameter WHERE key = %s", (key,))
            result = cr.fetchone()
            return result[0] if result else default
        
        return self._with_retry(_get_param, max_retries=5)

    def _set_param_with_retry(self, cr, key, value, uid=None):
        """Set ir.config_parameter with retry logic for concurrent access"""
        if uid is None:
            uid = self.env.uid
        
        def _set_param():
            # Try to update existing record
            cr.execute("""
                UPDATE ir_config_parameter
                SET value = %s,
                    write_date = NOW(),
                    write_uid = %s
                WHERE key = %s
            """, (value, uid, key))
            
            # If no record was updated, insert a new one
            if cr.rowcount == 0:
                cr.execute("""
                    INSERT INTO ir_config_parameter (key, value, create_date, write_date, create_uid, write_uid)
                    VALUES (%s, %s, NOW(), NOW(), %s, %s)
                """, (key, value, uid, uid))
            
            return True
        
        return self._with_retry(_set_param, max_retries=5)