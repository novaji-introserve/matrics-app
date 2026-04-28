# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import logging
import time
import threading
import math
import gc
import json
import os
import psutil
from datetime import datetime, timedelta

_logger = logging.getLogger(__name__)

class ETLJobDistribution(models.AbstractModel):
    _name = 'etl.job.distribution'
    _description = 'ETL Job Distribution and Parallelization'
    
    _max_workers = min(os.cpu_count() or 4, 8)  # Limit to CPU count or 8, whichever is smaller
    _job_semaphore = threading.Semaphore(_max_workers)
    _active_jobs = {}
    _jobs_lock = threading.Lock()
    
    @api.model
    def distribute_table_sync(self, table_config):
        """Create multiple jobs to process a large table in parallel"""
        try:
            # Get connectors
            connector_service = self.env['etl.database.connector.service']
            source_db = table_config.source_db_connection_id
            
            # Get table count
            table_name = table_config.name.lower()
            total_count = connector_service.get_table_count(source_db, table_name)
            
            if not total_count:
                _logger.warning(f"Could not determine size of table {table_name}")
                # Fall back to standard processing
                return table_config.sync_table_job()
            
            _logger.info(f"Distributing ETL for table {table_name} with {total_count} rows")
            
            # For small tables, don't bother with distribution
            if total_count < 5000:
                _logger.info(f"Table {table_name} is small ({total_count} rows), using standard processing")
                return table_config.sync_table_job()
            
            # For medium tables (5k-500k), use a few chunks
            elif total_count < 500000:
                num_chunks = min(4, self._max_workers)
                
            # For large tables (500k-5M), use more chunks
            elif total_count < 5000000:
                num_chunks = min(8, self._max_workers)
                
            # For very large tables (5M+), use even more chunks
            else:
                num_chunks = self._max_workers
            
            # Adjust chunk size based on available memory
            available_memory = self._get_available_memory()
            if available_memory < 1024:  # Less than 1GB
                _logger.warning(f"Low memory available ({available_memory}MB), reducing chunk count")
                num_chunks = max(2, num_chunks // 2)
            
            # Create main coordinator job
            coordinator_job = table_config.with_delay(
                description=f"ETL Coordinator: {table_config.name}",
                channel="etl_coordinator",
                priority=10
            ).distributed_etl_coordinator(num_chunks, total_count)
            
            # Update table status
            table_config.write({
                'job_uuid': coordinator_job.uuid,
                'job_status': 'pending',
                'last_sync_status': 'running',
                'last_sync_message': f'Distributed ETL started with {num_chunks} chunks',
                'progress_percentage': 0
            })
            
            # Create sync log
            sync_log = self.env['etl.sync.log'].create({
                'table_id': table_config.id,
                'start_time': fields.Datetime.now(),
                'status': 'running'
            })
            
            # Get source columns to determine primary key
            source_columns = connector_service.get_columns(source_db, table_name)
            config = table_config.get_config_json()
            primary_key_original = source_columns.get(config['primary_key'].lower())
            
            if not primary_key_original:
                raise ValueError(f"Primary key {config['primary_key']} not found in table {table_name}")
            
            # Get primary key range for numerical keys
            try:
                # Try to get min/max values
                min_query = f"SELECT MIN({primary_key_original}) AS min_id FROM {table_name}"
                max_query = f"SELECT MAX({primary_key_original}) AS max_id FROM {table_name}"
                
                min_result = connector_service.execute_query(source_db, min_query)
                max_result = connector_service.execute_query(source_db, max_query)
                
                min_id = min_result[0]['min_id']
                max_id = max_result[0]['max_id']
                
                # For numeric IDs, distribute by ID range
                if isinstance(min_id, (int, float)) and isinstance(max_id, (int, float)):
                    # Distribute by ID range
                    return self._distribute_by_id_range(
                        table_config, sync_log.id, min_id, max_id, num_chunks, primary_key_original
                    )
            except Exception as e:
                _logger.warning(f"Could not determine ID range: {str(e)}")
            
            # Fall back to distributing by offset/limit
            return self._distribute_by_offset(
                table_config, sync_log.id, total_count, num_chunks
            )
            
        except Exception as e:
            _logger.error(f"Error distributing ETL jobs: {str(e)}")
            # Fall back to standard processing
            return table_config.sync_table_job()
    
    def _get_available_memory(self):
        """Get available system memory in MB"""
        try:
            memory = psutil.virtual_memory()
            return memory.available / (1024 * 1024)  # Convert to MB
        except:
            # Default to 1GB if we can't determine
            return 1024
    
    def _distribute_by_id_range(self, table_config, sync_log_id, min_id, max_id, num_chunks, primary_key):
        """Distribute ETL jobs by ID range for numeric primary keys"""
        _logger.info(f"Distributing ETL for {table_config.name} by ID range: {min_id} to {max_id}")
        
        # Calculate chunk boundaries
        id_range = max_id - min_id
        chunk_size = id_range / num_chunks
        
        # Create a job for each chunk
        worker_jobs = []
        for i in range(num_chunks):
            chunk_min = min_id + (i * chunk_size)
            chunk_max = min_id + ((i + 1) * chunk_size)
            
            # For the last chunk, ensure we include the max_id
            if i == num_chunks - 1:
                chunk_max = max_id + 0.1  # Add small buffer
            
            # Create job for this chunk
            job = table_config.with_delay(
                description=f"ETL Worker: {table_config.name} (Chunk {i+1}/{num_chunks})",
                channel="etl_worker",
                identity_key=f"etl_worker_{table_config.id}_{sync_log_id}_{i}"
            ).distributed_etl_worker_id_range(
                sync_log_id, i, num_chunks, chunk_min, chunk_max, primary_key
            )
            
            worker_jobs.append(job.uuid)
        
        # Store worker job UUIDs in the sync log for tracking
        log_data = {
            'coordinator_uuid': table_config.job_uuid,
            'worker_uuids': worker_jobs,
            'chunks': num_chunks,
            'distribution_method': 'id_range'
        }
        
        # Use a JSON field to store metadata
        self.env['etl.sync.log'].browse(sync_log_id).write({
            'error_message': json.dumps(log_data)  # Use error_message as temp storage
        })
        
        _logger.info(f"Created {num_chunks} ETL worker jobs for {table_config.name}")
        return {'table': table_config.name, 'jobs': num_chunks, 'method': 'id_range'}
    
    def _distribute_by_offset(self, table_config, sync_log_id, total_count, num_chunks):
        """Distribute ETL jobs by offset/limit for non-numeric or unknown primary keys"""
        _logger.info(f"Distributing ETL for {table_config.name} by offset: {total_count} rows")
        
        # Calculate chunk sizes
        chunk_size = math.ceil(total_count / num_chunks)
        
        # Create a job for each chunk
        worker_jobs = []
        for i in range(num_chunks):
            offset = i * chunk_size
            # For the last chunk, adjust limit to make sure we get all rows
            if i == num_chunks - 1:
                limit = total_count - offset
            else:
                limit = chunk_size
            
            # Create job for this chunk
            job = table_config.with_delay(
                description=f"ETL Worker: {table_config.name} (Chunk {i+1}/{num_chunks})",
                channel="etl_worker",
                identity_key=f"etl_worker_{table_config.id}_{sync_log_id}_{i}"
            ).distributed_etl_worker_offset(
                sync_log_id, i, num_chunks, offset, limit
            )
            
            worker_jobs.append(job.uuid)
        
        # Store worker job UUIDs in the sync log for tracking
        log_data = {
            'coordinator_uuid': table_config.job_uuid,
            'worker_uuids': worker_jobs,
            'chunks': num_chunks,
            'distribution_method': 'offset'
        }
        
        # Use a JSON field to store metadata
        self.env['etl.sync.log'].browse(sync_log_id).write({
            'error_message': json.dumps(log_data)  # Use error_message as temp storage
        })
        
        _logger.info(f"Created {num_chunks} ETL worker jobs for {table_config.name}")
        return {'table': table_config.name, 'jobs': num_chunks, 'method': 'offset'}
    
    @api.model
    def track_job_completion(self, table_config, sync_log_id):
        """Track completion of distributed ETL jobs"""
        try:
            # Get sync log
            sync_log = self.env['etl.sync.log'].browse(sync_log_id)
            if not sync_log.exists():
                _logger.warning(f"Sync log {sync_log_id} not found")
                return False
            
            # Parse job metadata
            try:
                job_data = json.loads(sync_log.error_message or '{}')
            except json.JSONDecodeError:
                _logger.error(f"Could not parse job metadata for sync log {sync_log_id}")
                return False
            
            if not job_data or 'worker_uuids' not in job_data:
                _logger.warning(f"No worker jobs found in sync log {sync_log_id}")
                return False
            
            # Check status of all worker jobs
            worker_uuids = job_data.get('worker_uuids', [])
            total_workers = len(worker_uuids)
            completed = 0
            failed = 0
            
            # Get job stats
            total_records = 0
            new_records = 0
            updated_records = 0
            
            for uuid in worker_uuids:
                job = self.env['queue.job'].search([('uuid', '=', uuid)], limit=1)
                if not job.exists():
                    continue
                
                if job.state == 'done':
                    completed += 1
                    # Extract stats from job result
                    if job.result:
                        try:
                            result = json.loads(job.result) if isinstance(job.result, str) else job.result
                            total_records += result.get('total_rows', 0)
                            new_records += result.get('new_rows', 0)
                            updated_records += result.get('updated_rows', 0)
                        except:
                            pass
                elif job.state == 'failed':
                    failed += 1
            
            # Calculate progress
            if total_workers > 0:
                completed_pct = (completed / total_workers) * 100
                
                # Update table status
                table_config.write({
                    'progress_percentage': completed_pct,
                    'last_sync_message': f'Progress: {completed}/{total_workers} chunks complete ({completed_pct:.1f}%)'
                })
                
                # If all workers are done (success or failure)
                if completed + failed >= total_workers:
                    if failed > 0:
                        # Some jobs failed
                        table_config.write({
                            'job_status': 'failed',
                            'last_sync_status': 'failed',
                            'last_sync_message': f'ETL failed: {failed}/{total_workers} chunks failed'
                        })
                        
                        sync_log.write({
                            'end_time': fields.Datetime.now(),
                            'status': 'failed',
                            'error_message': f'ETL failed: {failed}/{total_workers} chunks failed'
                        })
                    else:
                        # All jobs succeeded
                        table_config.write({
                            'job_status': 'done',
                            'last_sync_status': 'success',
                            'last_sync_message': f'ETL completed successfully: {total_records} records processed',
                            'progress_percentage': 100,
                            'last_sync_time': fields.Datetime.now(),
                            'total_records_synced': total_records
                        })
                        
                        sync_log.write({
                            'end_time': fields.Datetime.now(),
                            'status': 'success',
                            'total_records': total_records,
                            'new_records': new_records,
                            'updated_records': updated_records
                        })
                    
                    # We're done tracking
                    return True
                
                # Still have jobs running
                return False
            
            return False
            
        except Exception as e:
            _logger.error(f"Error tracking job completion: {str(e)}")
            return False

class ETLSourceTableJobExtensions(models.Model):
    _inherit = 'etl.source.table'
    
    def distributed_etl_coordinator(self, num_chunks, total_count):
        """Coordinator job for distributed ETL"""
        _logger.info(f"Starting ETL coordinator for {self.name} with {num_chunks} chunks")
        
        self.write({
            'job_status': 'started',
            'progress_percentage': 0,
            'last_sync_message': f'Starting ETL with {num_chunks} parallel chunks'
        })
        
        # Poll for job completion every 10 seconds
        check_interval = 10
        max_checks = 24 * 60 * 60 // check_interval  # Up to 24 hours
        
        for i in range(max_checks):
            # Check if all worker jobs are complete
            is_complete = self.env['etl.job.distribution'].track_job_completion(self, int(self._context.get('sync_log_id', 0)))
            
            if is_complete:
                _logger.info(f"ETL coordinator for {self.name} completed")
                return {
                    'table': self.name,
                    'status': 'completed',
                    'chunks': num_chunks
                }
            
            # Yield to allow other jobs to run and resume after interval
            return {'yield': True, 'countdown': check_interval}
        
        # If we reach here, we've timed out
        _logger.warning(f"ETL coordinator for {self.name} timed out after 24 hours")
        return {
            'table': self.name,
            'status': 'timeout',
            'chunks': num_chunks
        }
    
    def distributed_etl_worker_id_range(self, sync_log_id, chunk_num, total_chunks, min_id, max_id, primary_key):
        """Worker job for processing a chunk by ID range"""
        try:
            _logger.info(f"Starting ETL worker for {self.name} (chunk {chunk_num+1}/{total_chunks}, IDs {min_id}-{max_id})")
            
            # Process the chunk using ETL manager
            etl_manager = self.env['etl.manager']
            stats = etl_manager.process_table_chunk(self, min_id, max_id)
            
            # Log completion
            _logger.info(f"Completed ETL worker for {self.name} (chunk {chunk_num+1}/{total_chunks}): "
                        f"{stats.get('total_rows', 0)} rows processed")
            
            # Return statistics
            return stats
        except Exception as e:
            _logger.error(f"Error in ETL worker for {self.name} (chunk {chunk_num+1}/{total_chunks}): {str(e)}")
            raise
    
    def distributed_etl_worker_offset(self, sync_log_id, chunk_num, total_chunks, offset, limit):
        """Worker job for processing a chunk by offset/limit"""
        try:
            _logger.info(f"Starting ETL worker for {self.name} (chunk {chunk_num+1}/{total_chunks}, offset {offset}, limit {limit})")
            
            # Get connectors
            connector_service = self.env['etl.database.connector.service']
            source_db = self.source_db_connection_id
            target_db = self.target_db_connection_id
            
            # Get config
            config = self.get_config_json()
            
            # Get source columns
            source_columns = connector_service.get_columns(source_db, config['source_table'])
            
            # Prepare query columns
            etl_manager = self.env['etl.manager']
            query_columns, column_map, primary_key_original = etl_manager._prepare_columns(config, source_columns)
            
            # Get last sync info
            last_sync_time, last_hashes = etl_manager._get_last_sync_info(self.id)
            
            # Stats to track progress
            stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
            current_hashes = {}
            
            # Create temporary hash table
            temp_hash_table = f"tmp_hash_{self.id}_{chunk_num}_{int(time.time())}"
            etl_manager._create_temp_hash_table(connector_service, target_db, temp_hash_table)
            etl_manager._populate_temp_hash_table(connector_service, target_db, temp_hash_table, last_hashes)
            
            # Process chunk using offset/limit
            chunk_stats = etl_manager._process_table_chunk_with_offset(
                connector_service, source_db, target_db,
                config, query_columns, column_map,
                primary_key_original, offset, limit,
                temp_hash_table, current_hashes
            )
            
            # Clean up temporary hash table
            etl_manager._drop_temp_hash_table(connector_service, target_db, temp_hash_table)
            
            # Update stats
            stats = chunk_stats
            
            # Log completion
            _logger.info(f"Completed ETL worker for {self.name} (chunk {chunk_num+1}/{total_chunks}): "
                        f"{stats.get('total_rows', 0)} rows processed")
            
            # Return statistics
            return stats
        except Exception as e:
            _logger.error(f"Error in ETL worker for {self.name} (chunk {chunk_num+1}/{total_chunks}): {str(e)}")
            raise
      