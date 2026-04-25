# -*- coding: utf-8 -*-
import json
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import logging
import time
import gc
import psutil
import os

_logger = logging.getLogger(__name__)

class ETLFastSync(models.AbstractModel):
    _name = 'etl.fast.sync'
    _description = 'High Performance ETL Sync'
    
    @api.model
    def init_sync_progress(self, table_config, total_chunks):
        """Initialize progress tracking for a sync operation"""
        try:
            # Create a savepoint - allows rollback without affecting outer transaction
            self.env.cr.execute("SAVEPOINT progress_init")
            
            # Reset progress tracking 
            self.env.cr.execute("""
                UPDATE etl_source_table
                SET processed_chunks = %s,
                    progress_percentage = 0,
                    job_status = 'started',
                    last_sync_status = 'running',
                    last_sync_message = %s
                WHERE id = %s
            """, (
                json.dumps([]),
                f"Starting sync with {total_chunks} chunks",
                table_config.id
            ))
            
            # Create sync log
            self.env.cr.execute("""
                INSERT INTO etl_sync_log
                (table_id, start_time, status, create_date, create_uid)
                VALUES (%s, %s, 'running', %s, %s)
                RETURNING id
            """, (
                table_config.id,
                fields.Datetime.now(),
                fields.Datetime.now(),
                self.env.uid
            ))
            
            log_id = self.env.cr.fetchone()[0]
            
            # Store metadata about chunks
            metadata = {
                'total_chunks': total_chunks,
                'start_time': time.time(),
                'chunk_size': 0  # Will be updated by first chunk
            }
            
            self.env.cr.execute("""
                UPDATE etl_sync_log
                SET error_message = %s
                WHERE id = %s
            """, (
                json.dumps(metadata),
                log_id
            ))
            
            # Commit the transaction to make progress visible
            self.env.cr.commit()
            
            return log_id
            
        except Exception as e:
            # Rollback to savepoint
            self.env.cr.execute("ROLLBACK TO SAVEPOINT progress_init")
            _logger.error(f"Error initializing progress tracking: {str(e)}")
            return None
    
    @api.model
    def finalize_sync_progress(self, table_config, sync_log_id, final_stats):
        """Finalize progress tracking after sync is complete"""
        try:
            now = fields.Datetime.now()
            
            # Update sync log
            self.env.cr.execute("""
                UPDATE etl_sync_log
                SET end_time = %s,
                    status = %s,
                    total_records = %s,
                    new_records = %s,
                    updated_records = %s
                WHERE id = %s
            """, (
                now,
                'success',
                final_stats.get('total_rows', 0),
                final_stats.get('new_rows', 0),
                final_stats.get('updated_rows', 0),
                sync_log_id
            ))
            
            # Update table status
            self.env.cr.execute("""
                UPDATE etl_source_table
                SET last_sync_time = %s,
                    job_status = 'done',
                    progress_percentage = 100,
                    last_sync_status = 'success',
                    last_sync_message = %s,
                    total_records_synced = %s
                WHERE id = %s
            """, (
                now,
                f"Successfully synced {final_stats.get('total_rows', 0)} records "
                f"({final_stats.get('new_rows', 0)} new, {final_stats.get('updated_rows', 0)} updated)",
                final_stats.get('total_rows', 0),
                table_config.id
            ))
            
            # Commit the transaction
            self.env.cr.commit()
            return True
            
        except Exception as e:
            _logger.error(f"Error finalizing progress tracking: {str(e)}")
            # Don't rollback - this is not a critical operation
            return False
    
    @api.model
    def sync_table(self, table_config):
        """
        Table synchronization main entry point
        
        Args:
            table_config: ETL table configuration record
                
        Returns:
            dict: Statistics about the sync operation
        """
        start_time = time.time()
        process = psutil.Process(os.getpid())
        mem_before = process.memory_info().rss / 1024 / 1024  # MB
        
        _logger.info(f"Starting ETL sync for table {table_config.name}")
        
        # Create sync log
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
            'unchanged_rows': 0,
            'error_rows': 0,
            'execution_time': 0
        }
        
        try:
            # Determine if CSV-based approach is requested
            use_csv = table_config.use_csv_mode if hasattr(table_config, 'use_csv_mode') else False
            
            # Process table using the appropriate approach
            db_adapter = self._get_db_adapter(table_config)
            
            if use_csv:
                # Use the two-phase CSV approach
                stats = db_adapter.sync_data_with_csv(table_config)
                
                # For CSV-based mode, we're done here - don't update job state
                # The background jobs will handle all updates from here
                
                # Map CSV-based sync statuses to valid sync log statuses
                raw_status = stats.get('status', '')
                if raw_status in ['extraction_started', 'loading_started', 'monitoring']:
                    log_status = 'running'
                elif raw_status == 'complete':
                    log_status = 'success'
                elif raw_status == 'failed':
                    log_status = 'failed'
                else:
                    log_status = 'running'  # Default to running for any other status
                    
                # Only update sync log with status - don't update any job state
                sync_log.write({
                    'status': log_status
                })
                
                # Don't mark the job as complete yet
                table_config.write({
                    'job_status': 'started',
                    'last_sync_message': f"Started {stats.get('status', 'async')} process with {stats.get('total_batches', 0)} batches"
                })
                
                # Print performance metrics for the initial setup only
                execution_time = time.time() - start_time
                mem_after = process.memory_info().rss / 1024 / 1024  # MB
                
                _logger.info(f"ETL sync completed for {table_config.name} in {execution_time:.2f}s")
                _logger.info(f"Memory usage: {mem_before:.1f}MB → {mem_after:.1f}MB, diff: {mem_after - mem_before:+.1f}MB")
                
                # Add execution time to stats
                stats['execution_time'] = execution_time
                
                return stats
            else:
                # Use the standard approach
                stats = db_adapter.sync_data(table_config)
            
            # Handle the case when stats doesn't include expected keys
            # This happens with CSV-based sync which returns initial status
            if 'total_rows' in stats:
                # Update sync log with complete stats
                sync_log.write({
                    'end_time': fields.Datetime.now(),
                    'status': 'success',
                    'total_records': stats['total_rows'],
                    'new_records': stats.get('new_rows', 0),
                    'updated_records': stats.get('updated_rows', 0)
                })
                
                # Update table status
                table_config.write({
                    'last_sync_time': fields.Datetime.now(),
                    'last_sync_status': 'success',
                    'last_sync_message': f"Successfully synced {stats['total_rows']} records ({stats.get('new_rows', 0)} new, {stats.get('updated_rows', 0)} updated)",
                    'total_records_synced': stats['total_rows']
                })
            else:
                # For async jobs like CSV-based sync, the immediate stats might not be available
                # The final update will be done by the background jobs
                
                # Map CSV-based sync statuses to valid sync log statuses
                # Valid sync log statuses are likely: 'running', 'success', 'failed'
                raw_status = stats.get('status', '')
                if raw_status in ['extraction_started', 'loading_started', 'monitoring']:
                    log_status = 'running'
                elif raw_status == 'complete':
                    log_status = 'success'
                elif raw_status == 'failed':
                    log_status = 'failed'
                else:
                    log_status = 'running'  # Default to running for any other status
                    
                sync_log.write({
                    'status': log_status
                })
                
                # Don't mark the job as complete yet
                table_config.write({
                    'job_status': 'started',
                    'last_sync_message': f"Started {stats.get('status', 'async')} process with {stats.get('total_batches', 0)} batches"
                })
            
            # Print performance metrics
            execution_time = time.time() - start_time
            mem_after = process.memory_info().rss / 1024 / 1024  # MB
            
            _logger.info(f"ETL sync completed for {table_config.name} in {execution_time:.2f}s")
            _logger.info(f"Memory usage: {mem_before:.1f}MB → {mem_after:.1f}MB, diff: {mem_after - mem_before:+.1f}MB")
            
            # Add execution time to stats
            stats['execution_time'] = execution_time
            
            return stats
        
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in ETL sync for table {table_config.name}: {error_message}")
            
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
    
    def _get_db_adapter(self, table_config):
        """Get the appropriate database adapter for the table configuration"""
        # Get source and target database types
        source_db_type = table_config.source_db_connection_id.db_type_code
        target_db_type = table_config.target_db_connection_id.db_type_code
        
        # PostgreSQL to PostgreSQL - use native features
        if source_db_type == 'postgresql' and target_db_type == 'postgresql':
            return self.env['etl.fast.sync.postgres']
        
        # MSSQL to MSSQL - use native features
        elif source_db_type == 'mssql' and target_db_type == 'mssql':
            return self.env['etl.fast.sync.mssql']
        
        # MySQL to MySQL - use native features
        elif source_db_type == 'mysql' and target_db_type == 'mysql':
            return self.env['etl.fast.sync.mysql']
        
        # For cross-database transfers, use generic adapter
        else:
            return self.env['etl.fast.sync.generic']
