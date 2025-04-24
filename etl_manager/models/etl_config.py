# -*- coding: utf-8 -*-
from datetime import date, datetime
from decimal import Decimal
import time
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import json
import logging
import math
import gc
from odoo.addons.queue.queue_job.job import Job

_logger = logging.getLogger(__name__)

class ETLSourceTable(models.Model):
    _name = 'etl.source.table'
    _description = 'ETL Source Table Configuration'
    _order = 'sequence, name'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char('Table Name', required=True, help="Source table name (e.g., tbl_customer)")
    sequence = fields.Integer('Sequence', default=10)
    target_table = fields.Char('Target Table', required=True, help="Target table name (e.g., res_partner)")
    source_db_connection_id = fields.Many2one('etl.database.connection', string='Source Database', 
                                           required=True, ondelete='restrict', tracking=True)
    target_db_connection_id = fields.Many2one('etl.database.connection', string='Target Database', 
                                           required=True, ondelete='restrict', tracking=True)
    primary_key = fields.Char('Primary Key', required=True)
    batch_size = fields.Integer('Batch Size', default=2000)
    is_base_table = fields.Boolean('Is Base Table', help="Tables with no dependencies")
    active = fields.Boolean(default=True)
    progress_percentage = fields.Float('Progress', readonly=True, default=0)

    category_id = fields.Many2one('etl.category', string='Category', required=True)
    frequency_id = fields.Many2one('etl.frequency', string='Frequency', required=True)

    job_uuid = fields.Char('Job UUID', readonly=True, copy=False)
    job_status = fields.Selection([
        ('pending', 'Pending'),
        ('started', 'Started'),
        ('done', 'Done'),
        ('failed', 'Failed'),
        ('canceled', 'Canceled'),
    ], string='Job Status', readonly=True, copy=False)
    
    dependency_ids = fields.Many2many(
        'etl.source.table', 
        'etl_table_dependencies', 
        'table_id', 
        'dependency_id', 
        string='Dependencies'
    )
    
    mapping_ids = fields.One2many('etl.column.mapping', 'table_id', string='Column Mappings')
    sync_log_ids = fields.One2many('etl.sync.log', 'table_id', string='Sync Logs')
    
    last_sync_time = fields.Datetime('Last Sync Time', readonly=True)
    last_sync_status = fields.Selection([
        ('success', 'Success'),
        ('failed', 'Failed'),
        ('running', 'Running'),
    ], string='Last Sync Status', readonly=True)
    last_sync_message = fields.Text('Last Sync Message', readonly=True)
    
    total_records_synced = fields.Integer('Total Records Synced', readonly=True)
    sync_method = fields.Selection([
        ('standard', 'Standard Method'),
        ('fast', 'Fast Sync')
    ], string='Sync Method', default='fast', required=True)

    processed_chunks = fields.Text('Processed Chunks', 
                                 help="JSON-encoded list of processed chunk IDs for resumable operations")
    
    use_csv_mode = fields.Boolean(
        string='Use CSV Mode', 
        default=True,
        help='If enabled, ETL will first extract to CSV files, then load from CSV to target database'
    )
    
    def get_processed_chunks(self):
        """Safely retrieve processed chunks as a list, handles backward compatibility"""
        try:
            if not self.processed_chunks:
                return []
                
            # Try to parse as JSON
            chunks = json.loads(self.processed_chunks)
            if isinstance(chunks, list):
                return chunks
            return []
        except (ValueError, json.JSONDecodeError):
            _logger.warning(f"Invalid processed_chunks format for table {self.name}")
            return []
    
    def set_processed_chunks(self, chunks):
        """Safely store processed chunks list as JSON"""
        if not isinstance(chunks, list):
            _logger.warning(f"Invalid chunks format for table {self.name}: {type(chunks)}")
            chunks = []
            
        self.write({'processed_chunks': json.dumps(chunks)})
        
    @api.model
    def default_get(self, fields_list):
        defaults = super(ETLSourceTable, self).default_get(fields_list)
        
        # Set default source connection if exists
        if 'source_db_connection_id' in fields_list:
            default_source = self.env['etl.database.connection'].search([
                ('is_default_source', '=', True),
                ('active', '=', True)
            ], limit=1)
            if default_source:
                defaults['source_db_connection_id'] = default_source.id
        
        # Set default target connection if exists
        if 'target_db_connection_id' in fields_list:
            default_target = self.env['etl.database.connection'].search([
                ('is_default_target', '=', True),
                ('active', '=', True)
            ], limit=1)
            if default_target:
                defaults['target_db_connection_id'] = default_target.id
        
        return defaults
    
    @api.model
    def update_sync_progress(self, chunk_num, total_chunks, chunk_stats):
        """
        Thread-safe method to update sync progress 
        Uses row-level locking and advisory locks to prevent concurrent updates
        """
        try:
            # Try to acquire an advisory lock first to reduce contention
            lock_id = abs(hash(f"etl_progress_{self.id}")) % 2147483647
            self.env.cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (lock_id,))
            lock_acquired = self.env.cr.fetchone()[0]
            
            if not lock_acquired:
                _logger.info(f"Skipping progress update for chunk {chunk_num} - couldn't acquire lock")
                return False
                
            # Get current state with FOR UPDATE to lock the row
            self.env.cr.execute("""
                SELECT processed_chunks, progress_percentage 
                FROM etl_source_table 
                WHERE id = %s 
                FOR UPDATE NOWAIT
            """, (self.id,))
            
            result = self.env.cr.fetchone()
            if not result:
                _logger.warning(f"Could not update progress for table {self.id} - record not found")
                return False
                
            processed_chunks_json, current_progress = result
            
            # Parse processed chunks
            try:
                processed_chunks = json.loads(processed_chunks_json or '[]')
                if not isinstance(processed_chunks, list):
                    processed_chunks = []
            except (ValueError, json.JSONDecodeError):
                processed_chunks = []
            
            # Add the current chunk if not already processed
            if chunk_num not in processed_chunks:
                processed_chunks.append(chunk_num)
            
            # Calculate new progress percentage
            if total_chunks > 0:
                new_progress = (len(processed_chunks) / total_chunks) * 100
            else:
                new_progress = current_progress  # Keep current if no total chunks
            
            # Update message based on chunk stats
            message = f"Processed {len(processed_chunks)}/{total_chunks} chunks ({new_progress:.1f}%)"
            if chunk_stats:
                message += f" - Last chunk: {chunk_stats.get('total_rows', 0)} rows processed"
            
            # Update directly with SQL to avoid ORM overhead
            self.env.cr.execute("""
                UPDATE etl_source_table 
                SET processed_chunks = %s,
                    progress_percentage = %s,
                    last_sync_message = %s
                WHERE id = %s
            """, (
                json.dumps(processed_chunks),
                new_progress,
                message,
                self.id
            ))
            
            # Check if this was the last chunk
            if len(processed_chunks) >= total_chunks:
                # Update final status
                self.env.cr.execute("""
                    UPDATE etl_source_table
                    SET job_status = 'done',
                        progress_percentage = 100,
                        last_sync_status = 'success',
                        last_sync_message = %s
                    WHERE id = %s
                """, (
                    f"Completed processing {total_chunks} chunks - ETL sync finished",
                    self.id
                ))
            
            # Commit the transaction to make the update visible to other processes
            self.env.cr.commit()
            return True
            
        except Exception as e:
            _logger.error(f"Error updating progress: {str(e)}")
            # Rollback this transaction - don't let progress updates affect main processing
            self.env.cr.rollback()
            return False
    
    @api.constrains('dependency_ids')
    def _check_dependencies(self):
        for table in self:
            if table in table.dependency_ids:
                raise ValidationError(_("A table cannot depend on itself!"))
            
            # Check for circular dependencies
            visited = set()
            to_visit = [(table, visited.copy())]
            
            while to_visit:
                current, path = to_visit.pop(0)
                if current.id in path:
                    raise ValidationError(_("Circular dependency detected!"))
                
                path.add(current.id)
                for dep in current.dependency_ids:
                    to_visit.append((dep, path.copy()))
    
    def get_config_json(self):
        self.ensure_one()
        _logger.debug(f"Generating config for table {self.name}")
        normalized_mappings = {}
        for mapping in self.mapping_ids:
            mapping_dict = {
                'target': mapping.target_column.lower(),
                'type': mapping.mapping_type,
            }
            if mapping.mapping_type == 'lookup':
                mapping_dict.update({
                    'lookup_table': mapping.lookup_table.lower() if mapping.lookup_table else None,
                    'lookup_key': mapping.lookup_key.lower() if mapping.lookup_key else None,
                    'lookup_value': mapping.lookup_value.lower() if mapping.lookup_value else None
                })
            normalized_mappings[mapping.source_column.lower()] = mapping_dict
        config = {
            'source_table': self.name.lower(),
            'target_table': self.target_table.lower(),
            'primary_key': self.primary_key.lower(),
            'batch_size': self.batch_size,
            'dependencies': [dep.name.lower() for dep in self.dependency_ids],
            'mappings': normalized_mappings,
            'source_db_id': self.source_db_connection_id.id,
            'target_db_id': self.target_db_connection_id.id
        }
        _logger.debug(f"Config generated for {self.name}: {config}")
        if not normalized_mappings:
            _logger.warning(f"No mappings defined for table {self.name}")
        return config
    
    def action_test_connection(self):
        """Test database connections without recursion issues"""
        self.ensure_one()
        try:
            # Get connector service
            connector_service = self.env['etl.database.connector.service']
            
            # Test source connection
            try:
                source_result = connector_service.test_connection(self.source_db_connection_id)
                if not source_result:
                    return {
                        'type': 'ir.actions.client',
                        'tag': 'display_notification',
                        'params': {
                            'title': _('Error'),
                            'message': _('Could not connect to source database'),
                            'type': 'danger',
                            'sticky': True,
                        }
                    }
            except Exception as e:
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': _('Source Connection Error'),
                        'message': str(e),
                        'type': 'danger',
                        'sticky': True,
                    }
                }
                
            # Test target connection
            try:
                target_result = connector_service.test_connection(self.target_db_connection_id)
                if not target_result:
                    return {
                        'type': 'ir.actions.client',
                        'tag': 'display_notification',
                        'params': {
                            'title': _('Error'),
                            'message': _('Could not connect to target database'),
                            'type': 'danger',
                            'sticky': True,
                        }
                    }
            except Exception as e:
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': _('Target Connection Error'),
                        'message': str(e),
                        'type': 'danger',
                        'sticky': True,
                    }
                }
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Success'),
                    'message': _('Successfully connected to both source and target databases!'),
                    'type': 'success',
                    'sticky': False,
                }
            }
        except Exception as e:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Error'),
                    'message': str(e),
                    'type': 'danger',
                    'sticky': True,
                }
            }
    
    def action_sync_table(self):
        """Override the sync action to use the selected sync method"""
        self.ensure_one()
        
        # Check if already running
        if self.job_status in ('pending', 'started'):
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Warning'),
                    'message': _('A sync job is already running for this table.'),
                    'type': 'warning',
                    'sticky': False,
                }
            }
        
        try:
            if self.sync_method == 'fast':
                # Use the Fast Sync method
                job = self.with_delay(
                    description=f"Fast Sync ETL table: {self.name}",
                    channel="etl"
                ).fast_sync_table_job()
            else:
                # Use the original method
                job = self.with_delay(
                    description=f"Sync ETL table: {self.name}",
                    channel="etl"
                ).sync_table_job()
            
            self.write({
                'job_uuid': job.uuid,
                'job_status': 'pending',
                'last_sync_status': 'running',
                'last_sync_message': f'Sync job queued (method: {self.sync_method})',
                'progress_percentage': 0
            })
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Job Queued'),
                    'message': _('Table synchronization job has been queued.'),
                    'type': 'success',
                    'sticky': False,
                }
            }
            
        except Exception as e:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Error'),
                    'message': str(e),
                    'type': 'danger',
                    'sticky': True,
                }
            }
  
    def action_sync_table_csv(self):
        """Start ETL sync with CSV-based approach as a queue job"""
        self.ensure_one()
        
        # Check if already running
        if self.job_status in ('pending', 'started'):
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Warning'),
                    'message': _('A sync job is already running for this table.'),
                    'type': 'warning',
                    'sticky': False,
                }
            }
        
        # Enable CSV mode
        self.write({'use_csv_mode': True})
        
        try:
            # Create job with proper delay
            fast_sync = self.env['etl.fast.sync']
            job = fast_sync.with_delay(
                description=f"CSV ETL Process: {self.name}",
                channel="etl_csv",
                priority=5
            ).sync_table(self)
            
            # Update table with job info
            self.write({
                'job_uuid': job.uuid,
                'job_status': 'pending',
                'last_sync_status': 'running',
                'last_sync_message': f'CSV-based ETL job queued (Job: {job.uuid})',
                'progress_percentage': 0
            })
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Job Queued'),
                    'message': _(f'CSV-based ETL job has been queued (Job ID: {job.uuid})'),
                    'type': 'success',
                    'sticky': False,
                }
            }
        except Exception as e:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Error'),
                    'message': str(e),
                    'type': 'danger',
                    'sticky': True,
                }
            }

    def fast_sync_table_job(self):
        """Job for running the fast sync process"""
        try:
            start_time = time.time()
            
            # Update job status
            self.write({
                'job_status': 'started',
                'progress_percentage': 0,
                'last_sync_message': 'Fast sync started'
            })
            
            # Use the ETL Fast Sync service
            stats = self.env['etl.fast.sync'].sync_table(self)
            
            # Update final status
            execution_time = time.time() - start_time
            self.write({
                'job_status': 'done',
                'progress_percentage': 100,
                'last_sync_status': 'success',
                'last_sync_message': f'Fast sync completed in {execution_time:.2f}s with {stats["total_rows"]} records'
            })
            
            return {
                'table': self.name,
                'status': 'success',
                'method': 'fast_sync',
                'execution_time': execution_time,
                'records': stats
            }
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Fast sync job failed for table {self.name}: {error_message}")
            
            self.write({
                'job_status': 'failed',
                'last_sync_status': 'failed',
                'last_sync_message': error_message
            })
            
            return {
                'table': self.name,
                'status': 'failed',
                'method': 'fast_sync',
                'message': error_message
            }
        
    # def action_sync_table(self):
    #     """Queue a sync job with optimized chunking for very large tables"""
    #     self.ensure_one()
        
    #     # Check if already running
    #     if self.job_status in ('pending', 'started'):
    #         return {
    #             'type': 'ir.actions.client',
    #             'tag': 'display_notification',
    #             'params': {
    #                 'title': _('Warning'),
    #                 'message': _('A sync job is already running for this table.'),
    #                 'type': 'warning',
    #                 'sticky': False,
    #             }
    #         }
        
    #     try:
    #         # Check table size to determine chunking strategy
    #         connector_service = self.env['etl.database.connector.service']
            
    #         try:
    #             # Get count of records in source table
    #             total_count = connector_service.get_table_count(
    #                 self.source_db_connection_id, 
    #                 self.name.lower()
    #             )
                
    #             _logger.info(f"Table {self.name} has {total_count} records")
                    
    #             # For small tables (< 20,000 rows), run synchronously
    #             if total_count < 5000:
    #                 self.env['etl.manager'].process_table(self)
    #                 self.env.cr.commit()
    #                 return {
    #                     'type': 'ir.actions.client',
    #                     'tag': 'display_notification',
    #                     'params': {
    #                         'title': _('Success'),
    #                         'message': _('Table synchronization completed successfully.'),
    #                         'type': 'success',
    #                         'sticky': False,
    #                     }
    #                 }
                
    #             # For large tables, determine ID range
    #             source_columns = connector_service.get_columns(
    #                 self.source_db_connection_id, 
    #                 self.name.lower()
    #             )
                
    #             primary_key = self.primary_key.lower()
    #             primary_key_original = source_columns.get(primary_key)
                
    #             if not primary_key_original:
    #                 raise ValueError(f"Could not find primary key {primary_key} in table")
                
    #             # For very large tables, use optimal chunk size calculation
    #             if total_count > 500000:
    #                 # Determine optimal chunk size based on record count
    #                 # For extremely large tables, use smaller chunks
    #                 if total_count > 5000000:  # 5M+ records
    #                     chunk_size = 20000
    #                 elif total_count > 2000000:  # 2M-5M records
    #                     chunk_size = 10000
    #                 elif total_count > 1000000:  # 1M-2M records
    #                     chunk_size = 20000
    #                 else:  # 500K-1M records
    #                     chunk_size = 30000
                        
    #                 # Calculate number of chunks needed
    #                 chunks = math.ceil(total_count / chunk_size)
    #                 _logger.info(f"Splitting table {self.name} into {chunks} chunks of ~{chunk_size} records each")
                    
    #                 # Get primary key range
    #                 min_query = f"SELECT MIN({primary_key_original}) AS min_id FROM {self.name.lower()}"
    #                 max_query = f"SELECT MAX({primary_key_original}) AS max_id FROM {self.name.lower()}"
                    
    #                 min_result = connector_service.execute_query(self.source_db_connection_id, min_query)
    #                 max_result = connector_service.execute_query(self.source_db_connection_id, max_query)
                    
    #                 min_id = min_result[0]['min_id']
    #                 max_id = max_result[0]['max_id']
                    
    #                 _logger.info(f"ID range for {self.name}: {min_id} to {max_id}")
                    
    #                 # Create main job
    #                 main_job = self.with_delay(
    #                     description=f"Main sync job for table: {self.name}",
    #                     channel="etl"
    #                 ).sync_table_job_main(chunks, total_count)
                    
    #                 # Update table status
    #                 self.write({
    #                     'job_uuid': main_job.uuid,
    #                     'job_status': 'pending',
    #                     'last_sync_status': 'running',
    #                     'last_sync_message': f'Sync job queued in {chunks} chunks',
    #                     'progress_percentage': 0
    #                 })
                    
    #                 # Calculate chunk boundaries
    #                 if isinstance(min_id, (int, float)) and isinstance(max_id, (int, float)):
    #                     # Numeric IDs
    #                     step = (max_id - min_id) / chunks
    #                     for i in range(chunks):
    #                         chunk_min = min_id + (i * step)
    #                         chunk_max = min_id + ((i + 1) * step)
    #                         if i == chunks - 1:
    #                             chunk_max = max_id + 0.1  # Add small buffer to include the max value
                                
    #                         self.with_delay(
    #                             description=f"Sync ETL table: {self.name} (chunk {i+1}/{chunks})",
    #                             channel="etl",
    #                             priority=10
    #                         ).sync_table_job_chunk(chunk_min, chunk_max, i+1, chunks)
    #                 else:
    #                     # String IDs - use optimized row ranges
    #                     rows_per_chunk = math.ceil(total_count / chunks)
                        
    #                     # Use a more efficient approach for string IDs
    #                     # Instead of using simple OFFSET which is slow for large offsets,
    #                     # we can use a cursor-based approach or batched fetching
                        
    #                     for i in range(chunks):
    #                         self.with_delay(
    #                             description=f"Sync ETL table: {self.name} (chunk {i+1}/{chunks})",
    #                             channel="etl",
    #                             priority=10
    #                         ).sync_table_job_chunk_by_offset(
    #                             offset=i * rows_per_chunk,
    #                             limit=rows_per_chunk,
    #                             chunk_num=i+1,
    #                             total_chunks=chunks
    #                         )
                    
    #                 return {
    #                     'type': 'ir.actions.client',
    #                     'tag': 'display_notification',
    #                     'params': {
    #                         'title': _('Jobs Queued'),
    #                         'message': _(f'Table synchronization split into {chunks} jobs and queued.'),
    #                         'type': 'success',
    #                         'sticky': False,
    #                     }
    #                 }
                
    #             # For medium-size tables, use a single job
    #             job = self.with_delay(
    #                 description=f"Sync ETL table: {self.name}",
    #                 channel="etl"
    #             ).sync_table_job()
                
    #             self.write({
    #                 'job_uuid': job.uuid,
    #                 'job_status': 'pending',
    #                 'last_sync_status': 'running',
    #                 'last_sync_message': 'Sync job queued',
    #                 'progress_percentage': 0
    #             })
                
    #             return {
    #                 'type': 'ir.actions.client',
    #                 'tag': 'display_notification',
    #                 'params': {
    #                     'title': _('Job Queued'),
    #                     'message': _('Table synchronization job has been queued.'),
    #                     'type': 'success',
    #                     'sticky': False,
    #                 }
    #             }
    #         except Exception as e:
    #             _logger.warning(f"Could not determine table size: {str(e)}")
    #             # Fall back to standard job
    #             job = self.with_delay(
    #                 description=f"Sync ETL table: {self.name}",
    #                 channel="etl"
    #             ).sync_table_job()
                
    #             self.write({
    #                 'job_uuid': job.uuid,
    #                 'job_status': 'pending',
    #                 'last_sync_status': 'running',
    #                 'last_sync_message': 'Sync job queued (fallback method)',
    #                 'progress_percentage': 0
    #             })
                
    #             return {
    #                 'type': 'ir.actions.client',
    #                 'tag': 'display_notification',
    #                 'params': {
    #                     'title': _('Job Queued'),
    #                     'message': _('Table synchronization job has been queued.'),
    #                     'type': 'success',
    #                     'sticky': False,
    #                 }
    #             }
    #     except Exception as e:
    #         return {
    #             'type': 'ir.actions.client',
    #             'tag': 'display_notification',
    #             'params': {
    #                 'title': _('Error'),
    #                 'message': str(e),
    #                 'type': 'danger',
    #                 'sticky': True,
    #             }
    #         }

    def sync_table_job_main(self, total_chunks, total_records=0):
        """Main job for coordinating chunks"""
        try:
            self.write({
                'job_status': 'started',
                'progress_percentage': 0
            })
            
            _logger.info(f"Main sync job started for table {self.name} with {total_chunks} chunks")
            
            # Create a sync log
            sync_log = self.env['etl.sync.log'].create({
                'table_id': self.id,
                'start_time': fields.Datetime.now(),
                'status': 'running',
                'total_records': total_records
            })
            
            # Store sync log ID in context for chunk jobs
            self = self.with_context(main_sync_log_id=sync_log.id)
            
            return {
                'table': self.name,
                'status': 'started',
                'message': f'Main sync job started with {total_chunks} chunks'
            }
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in main sync job for table {self.name}: {error_message}")
            
            self.write({
                'job_status': 'failed',
                'last_sync_status': 'failed',
                'last_sync_message': f'Main job error: {error_message}'
            })
            
            return {
                'table': self.name,
                'status': 'failed',
                'message': error_message
            }

    def sync_table_job_chunk(self, min_id, max_id, chunk_num, total_chunks):
        """Process a chunk of a table based on ID range"""
        try:
            _logger.info(f"Starting chunk {chunk_num}/{total_chunks} for table {self.name}")
            
            etl_manager = self.env['etl.manager']
            stats = etl_manager.process_table_chunk(self, min_id, max_id)
            
            # Update progress
            current_progress = (chunk_num / total_chunks) * 100
            self.write({
                'progress_percentage': current_progress,
                'last_sync_message': f'Processed chunk {chunk_num}/{total_chunks} ({current_progress:.1f}%)'
            })
            
            # Update the main sync log if it exists
            if self.env.context.get('main_sync_log_id'):
                main_log = self.env['etl.sync.log'].browse(self.env.context['main_sync_log_id'])
                if main_log.exists():
                    main_log.write({
                        'total_records': (main_log.total_records or 0) + stats.get('total_rows', 0),
                        'new_records': (main_log.new_records or 0) + stats.get('new_rows', 0),
                        'updated_records': (main_log.updated_records or 0) + stats.get('updated_rows', 0)
                    })
            
            # If this is the last chunk, mark job as done
            if chunk_num == total_chunks:
                self.write({
                    'job_status': 'done',
                    'progress_percentage': 100,
                    'last_sync_status': 'success',
                    'last_sync_message': f'All {total_chunks} chunks completed successfully'
                })
                
                # Complete main log
                if self.env.context.get('main_sync_log_id'):
                    main_log = self.env['etl.sync.log'].browse(self.env.context['main_sync_log_id'])
                    if main_log.exists():
                        main_log.write({
                            'end_time': fields.Datetime.now(),
                            'status': 'success'
                        })
            
            # Commit transaction
            self.env.cr.commit()
            
            # Force garbage collection
            gc.collect()
            
            return {
                'table': self.name,
                'status': 'success',
                'chunk': chunk_num,
                'total_chunks': total_chunks,
                'message': f'Chunk from {min_id} to {max_id} completed successfully',
                'stats': stats
            }
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Sync job failed for table {self.name} chunk {chunk_num}: {error_message}")
            
            # Don't mark the main job as failed, just update the message
            self.write({
                'last_sync_message': f'Error in chunk {chunk_num}/{total_chunks}: {error_message}'
            })
            
            # Commit transaction to prevent locks
            self.env.cr.commit()
            
            return {
                'table': self.name,
                'status': 'failed',
                'chunk': chunk_num,
                'total_chunks': total_chunks,
                'message': error_message
            }    
    
    def sync_table_job_chunk_by_offset(self, offset, limit, chunk_num, total_chunks):
        """Process a chunk of a table based on offset/limit with optimized query execution"""
        try:
            _logger.info(f"Starting offset chunk {chunk_num}/{total_chunks} for table {self.name}")
            
            etl_manager = self.env['etl.manager']
            connector_service, source_db, target_db = etl_manager._get_connectors(self)
            
            # Get config and columns
            config = self.get_config_json()
            source_columns = connector_service.get_columns(source_db, config['source_table'])
            query_columns, column_map, primary_key_original = etl_manager._prepare_columns(config, source_columns)
            
            # Get last sync info
            last_sync_time, last_hashes = etl_manager._get_last_sync_info(self.id)
            
            # Stats to track progress
            current_hashes = {}
            stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
            
            # Create sync log entry
            sync_log = self.env['etl.sync.log'].create({
                'table_id': self.id,
                'start_time': fields.Datetime.now(),
                'status': 'running'
            })
            
            # Find primary key in mappings for target table
            target_primary_key = None
            for source_col, mapping in config['mappings'].items():
                if source_col.lower() == config['primary_key'].lower():
                    target_primary_key = mapping['target'].lower()
                    break
            
            if not target_primary_key:
                raise ValueError(f"Primary key mapping not found for {config['primary_key']}")
            
            # Create a temporary table for batch processing
            temp_table_name = f"tmp_chunk_{self.id}_{chunk_num}_{int(time.time())}"
            self._create_temp_table_for_batch(connector_service, target_db, temp_table_name)
            
            # Process chunk using optimized strategy - fetch data in smaller sub-chunks
            # This is much more efficient than a single large OFFSET/LIMIT query
            sub_limit = min(20000, limit)  # Use smaller chunks for fetching
            current_offset = offset
            rows_processed = 0
            max_offset = offset + limit
            
            while current_offset < max_offset and rows_processed < limit:
                # Calculate remaining rows to process
                remaining = min(sub_limit, limit - rows_processed)
                
                # Process a sub-chunk of data
                batch_stats = self._process_sub_chunk(
                    connector_service,
                    source_db,
                    target_db,
                    config,
                    query_columns,
                    column_map,
                    primary_key_original,
                    current_offset,
                    remaining,
                    last_hashes,
                    current_hashes,
                    temp_table_name,
                    chunk_num,
                    total_chunks
                )
                
                # Update statistics
                stats['total_rows'] += batch_stats.get('batch_size', 0)
                stats['new_rows'] += batch_stats.get('new_rows', 0)
                stats['updated_rows'] += batch_stats.get('updated_rows', 0)
                
                # Update offset and processed count
                rows_processed += batch_stats.get('batch_size', 0)
                current_offset += remaining
                
                # Break if we received fewer rows than requested (end of data)
                if batch_stats.get('batch_size', 0) < remaining:
                    break
                    
                # Update progress after each sub-chunk
                progress = (chunk_num - 1) / total_chunks * 100
                progress += (rows_processed / limit) / total_chunks * 100
                self.write({
                    'progress_percentage': min(99, progress),
                    'last_sync_message': (f'Processing chunk {chunk_num}/{total_chunks}: '
                                        f'{rows_processed}/{limit} records')
                })
                
                # Periodically commit to release locks
                if rows_processed % 10000 == 0:
                    self.env.cr.commit()
            
            # Now we have the properly defined target_primary_key to pass
            # Load all records from the temp table to the target table
            self._load_from_temp_table(
                connector_service, 
                target_db, 
                temp_table_name, 
                config['target_table'], 
                primary_key_original, 
                target_primary_key  # Using the properly defined target primary key
            )
            
            # Clean up the temporary table
            self._drop_temp_table(connector_service, target_db, temp_table_name)
            
            # Update sync log
            sync_log.write({
                'end_time': fields.Datetime.now(),
                'status': 'success',
                'total_records': stats['total_rows'],
                'new_records': stats['new_rows'],
                'updated_records': stats['updated_rows'],
                'row_hashes': json.dumps(current_hashes)
            })
            
            # Update progress
            current_progress = (chunk_num / total_chunks) * 100
            self.write({
                'progress_percentage': current_progress,
                'last_sync_message': f'Processed chunk {chunk_num}/{total_chunks} ({current_progress:.1f}%)'
            })
            
            # If this is the last chunk, mark job as done
            if chunk_num == total_chunks:
                self.write({
                    'job_status': 'done',
                    'progress_percentage': 100,
                    'last_sync_status': 'success',
                    'last_sync_message': f'All {total_chunks} chunks completed successfully'
                })
                
                # Update the main sync log if it exists
                if self.env.context.get('main_sync_log_id'):
                    main_log = self.env['etl.sync.log'].browse(self.env.context['main_sync_log_id'])
                    if main_log.exists():
                        main_log.write({
                            'end_time': fields.Datetime.now(),
                            'status': 'success',
                            'total_records': (main_log.total_records or 0) + stats['total_rows'],
                            'new_records': (main_log.new_records or 0) + stats['new_rows'],
                            'updated_records': (main_log.updated_records or 0) + stats['updated_rows']
                        })
            
            # Commit transaction
            self.env.cr.commit()
            
            # Force garbage collection
            gc.collect()
            
            return {
                'table': self.name,
                'status': 'success',
                'chunk': chunk_num,
                'total_chunks': total_chunks,
                'message': f'Chunk with offset {offset} and limit {limit} completed successfully',
                'stats': stats
            }
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Sync job failed for table {self.name} chunk {chunk_num}: {error_message}")
            
            # Don't mark the main job as failed, just update the message
            self.write({
                'last_sync_message': f'Error in chunk {chunk_num}/{total_chunks}: {error_message}'
            })
            
            # Update sync log
            if 'sync_log' in locals() and sync_log.exists():
                sync_log.write({
                    'end_time': fields.Datetime.now(),
                    'status': 'failed',
                    'error_message': error_message
                })
            
            # Clean up temp table if it exists
            if 'temp_table_name' in locals():
                try:
                    self._drop_temp_table(connector_service, target_db, temp_table_name)
                except Exception:
                    pass
            
            # Commit transaction to prevent locks
            self.env.cr.commit()
            
            return {
                'table': self.name,
                'status': 'failed',
                'chunk': chunk_num,
                'total_chunks': total_chunks,
                'message': error_message
            }

    def _create_temp_table_for_batch(self, connector_service, target_db, temp_table_name):
        """Create a temporary table for batch processing"""
        # Different approach based on database type
        db_type = target_db.db_type_code
        
        if db_type == 'postgresql':
            create_sql = f"""
                CREATE TEMPORARY TABLE "{temp_table_name}" (
                    record_key VARCHAR(255),
                    record_data JSONB,
                    is_new BOOLEAN DEFAULT FALSE,
                    is_updated BOOLEAN DEFAULT FALSE,
                    PRIMARY KEY (record_key)
                )
            """
        elif db_type == 'mssql':
            create_sql = f"""
                CREATE TABLE #{temp_table_name} (
                    record_key VARCHAR(255) PRIMARY KEY,
                    record_data NVARCHAR(MAX),
                    is_new BIT DEFAULT 0,
                    is_updated BIT DEFAULT 0
                )
            """
        elif db_type == 'mysql':
            create_sql = f"""
                CREATE TEMPORARY TABLE `{temp_table_name}` (
                    record_key VARCHAR(255) PRIMARY KEY,
                    record_data JSON,
                    is_new BOOLEAN DEFAULT FALSE,
                    is_updated BOOLEAN DEFAULT FALSE
                )
            """
        elif db_type == 'oracle':
            create_sql = f"""
                CREATE GLOBAL TEMPORARY TABLE "{temp_table_name}" (
                    record_key VARCHAR2(255) PRIMARY KEY,
                    record_data CLOB,
                    is_new NUMBER(1) DEFAULT 0,
                    is_updated NUMBER(1) DEFAULT 0
                ) ON COMMIT PRESERVE ROWS
            """
        else:
            # Generic fallback
            create_sql = f"""
                CREATE TEMPORARY TABLE {temp_table_name} (
                    record_key VARCHAR(255) PRIMARY KEY,
                    record_data TEXT,
                    is_new BOOLEAN DEFAULT FALSE,
                    is_updated BOOLEAN DEFAULT FALSE
                )
            """
        
        connector_service.execute_query(target_db, create_sql)
        _logger.info(f"Created temporary table {temp_table_name}")

    def _drop_temp_table(self, connector_service, target_db, temp_table_name):
        """Drop the temporary table"""
        db_type = target_db.db_type_code
        
        try:
            if db_type == 'postgresql':
                drop_sql = f'DROP TABLE IF EXISTS "{temp_table_name}"'
            elif db_type == 'mssql':
                drop_sql = f"IF OBJECT_ID('tempdb..#{temp_table_name}') IS NOT NULL DROP TABLE #{temp_table_name}"
            elif db_type == 'mysql':
                drop_sql = f"DROP TEMPORARY TABLE IF EXISTS `{temp_table_name}`"
            elif db_type == 'oracle':
                drop_sql = f'DROP TABLE "{temp_table_name}" PURGE'
            else:
                drop_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
            
            connector_service.execute_query(target_db, drop_sql)
            _logger.info(f"Dropped temporary table {temp_table_name}")
        except Exception as e:
            _logger.warning(f"Failed to drop temporary table {temp_table_name}: {str(e)}")

    def _process_sub_chunk(self, connector_service, source_db, target_db, config, 
                      query_columns, column_map, primary_key_original,
                      offset, limit, last_hashes, current_hashes, 
                      temp_table_name, chunk_num, total_chunks):
        """Process a sub-chunk of data with optimized query"""
        etl_manager = self.env['etl.manager']
        formatted_columns = ', '.join(query_columns)
        db_type = source_db.db_type_code
        
        # Construct query - optimize based on database type
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
        elif db_type == 'oracle':
            # Oracle requires different pagination syntax
            query = f"""
                SELECT {formatted_columns}
                FROM (
                    SELECT {formatted_columns}, ROWNUM AS rn
                    FROM {config['source_table']}
                    WHERE ROWNUM <= {offset + limit}
                    ORDER BY {primary_key_original}
                )
                WHERE rn > {offset}
            """
        else:
            # Generic fallback
            query = f"""
                SELECT {formatted_columns} FROM {config['source_table']} 
                ORDER BY {primary_key_original} 
                LIMIT {limit} OFFSET {offset}
            """
        
        # For very large offsets, this query can be slow, so log start time
        _logger.info(f"Executing sub-chunk query at offset {offset}, limit {limit} for chunk {chunk_num}/{total_chunks}")
        query_start_time = time.time()
        result_rows = connector_service.execute_query(source_db, query)
        
        query_time = time.time() - query_start_time
        if query_time > 5:
            _logger.warning(f"Slow query ({query_time:.2f}s) at offset {offset}")
        
        batch_stats = {
            'batch_size': len(result_rows) if result_rows else 0,
            'new_rows': 0,
            'updated_rows': 0
        }
        
        if not result_rows:
            _logger.info(f"No rows found at offset {offset}")
            return batch_stats
        
        transformed_rows = []
        for row in result_rows:
            pk_value = str(row.get(primary_key_original))
            if not pk_value:
                continue
            transformed_row = {}
            for original_col, source_col in column_map.items():
                source_value = row.get(original_col)
                if source_value is not None:
                    mapping = config['mappings'].get(source_col.lower())
                    if mapping:
                        if mapping['type'] == 'direct':
                            transformed_row[mapping['target'].lower()] = source_value
                        elif mapping['type'] == 'lookup':
                            lookup_value = etl_manager._lookup_value(
                                connector_service, target_db, mapping['lookup_table'],
                                mapping['lookup_key'], mapping['lookup_value'], str(source_value)
                            )
                            transformed_row[mapping['target'].lower()] = lookup_value
            if not transformed_row:
                _logger.warning(f"No fields transformed for row with pk {pk_value}")
                continue
            _logger.debug(f"Transformed row for pk {pk_value}: {transformed_row}")
            json_safe_data = {}
            for key, value in transformed_row.items():
                if isinstance(value, datetime):
                    json_safe_data[key] = value.isoformat()
                elif isinstance(value, date):
                    json_safe_data[key] = value.isoformat()
                elif isinstance(value, Decimal):
                    json_safe_data[key] = str(value)
                else:
                    json_safe_data[key] = value
            row_hash = etl_manager._calculate_row_hash(json_safe_data)
            current_hashes[pk_value] = row_hash
            is_new = pk_value not in last_hashes
            is_updated = not is_new and last_hashes.get(pk_value) != row_hash
            if is_new:
                batch_stats['new_rows'] += 1
                transformed_rows.append((pk_value, json_safe_data, True, False))
            elif is_updated:
                batch_stats['updated_rows'] += 1
                transformed_rows.append((pk_value, json_safe_data, False, True))
        
        if transformed_rows:
            self._batch_insert_to_temp_table(
                connector_service, target_db, temp_table_name, transformed_rows
            )
        _logger.info(f"Processed sub-chunk of {len(result_rows)} rows: {batch_stats['new_rows']} new, {batch_stats['updated_rows']} updated")
        return batch_stats

    def _batch_insert_to_temp_table(self, connector_service, target_db, temp_table_name, transformed_rows):
        """Insert transformed rows into the temporary table in a batch"""
        if not transformed_rows:
            return
            
        db_type = target_db.db_type_code
        batch_size = 1000  # Smaller batches for inserts
        
        for i in range(0, len(transformed_rows), batch_size):
            batch = transformed_rows[i:i + batch_size]
            
            if db_type == 'postgresql':
                # Build VALUES clause for PostgreSQL
                values_parts = []
                params = []
                
                for pk, row_data, is_new, is_updated in batch:
                    values_parts.append("(%s, %s, %s, %s)")
                    params.extend([
                        pk, 
                        json.dumps(row_data), 
                        is_new, 
                        is_updated
                    ])
                # for pk, row_data, is_new, is_updated in batch:
                #     # Convert date objects before JSON serialization
                #     safe_row_data = {}
                #     for key, value in row_data.items():
                #         if isinstance(value, datetime) or isinstance(value, date):
                #             safe_row_data[key] = value.isoformat()
                #         elif isinstance(value, Decimal):
                #             safe_row_data[key] = str(value)
                #         else:
                #             safe_row_data[key] = value
                    
                #     values_parts.append("(%s, %s, %s, %s)")
                #     params.extend([
                #         pk, 
                #         json.dumps(safe_row_data),  # Using pre-processed data
                #         is_new, 
                #         is_updated
                #     ])
                
                values_clause = ", ".join(values_parts)
                sql = f"""
                    INSERT INTO "{temp_table_name}" (record_key, record_data, is_new, is_updated)
                    VALUES {values_clause}
                    ON CONFLICT (record_key) DO UPDATE SET 
                    record_data = EXCLUDED.record_data,
                    is_new = EXCLUDED.is_new,
                    is_updated = EXCLUDED.is_updated
                """
                
                connector_service.execute_query(target_db, sql, params)
                
            elif db_type == 'mssql':
                # For SQL Server, use a table variable
                for pk, row_data, is_new, is_updated in batch:
                    sql = f"""
                        IF EXISTS (SELECT 1 FROM #{temp_table_name} WHERE record_key = ?)
                        UPDATE #{temp_table_name} 
                        SET record_data = ?, is_new = ?, is_updated = ?
                        WHERE record_key = ?
                        ELSE
                        INSERT INTO #{temp_table_name} (record_key, record_data, is_new, is_updated)
                        VALUES (?, ?, ?, ?)
                    """
                    params = [
                        pk, 
                        json.dumps(row_data), 
                        1 if is_new else 0, 
                        1 if is_updated else 0,
                        pk,
                        pk,
                        json.dumps(row_data),
                        1 if is_new else 0,
                        1 if is_updated else 0
                    ]
                    connector_service.execute_query(target_db, sql, params)
                    
            elif db_type == 'mysql':
                # Build VALUES clause for MySQL
                values_parts = []
                params = []
                
                # for pk, row_data, is_new, is_updated in batch:
                for pk, row_data_json, is_new, is_updated in batch:
                    values_parts.append("(%s, %s, %s, %s)")
                    params.extend([
                        pk, 
                        row_data_json, 
                        # json.dumps(row_data), 
                        is_new, 
                        is_updated
                    ])
                
                values_clause = ", ".join(values_parts)
                sql = f"""
                    INSERT INTO `{temp_table_name}` (record_key, record_data, is_new, is_updated)
                    VALUES {values_clause}
                    ON DUPLICATE KEY UPDATE 
                    record_data = VALUES(record_data),
                    is_new = VALUES(is_new),
                    is_updated = VALUES(is_updated)
                """
                
                connector_service.execute_query(target_db, sql, params)
                
            elif db_type == 'oracle':
                # For Oracle, use MERGE
                for pk, row_data, is_new, is_updated in batch:
                    sql = f"""
                        MERGE INTO "{temp_table_name}" t
                        USING (SELECT :1 as record_key FROM DUAL) s
                        ON (t.record_key = s.record_key)
                        WHEN MATCHED THEN
                            UPDATE SET record_data = :2, is_new = :3, is_updated = :4
                        WHEN NOT MATCHED THEN
                            INSERT (record_key, record_data, is_new, is_updated)
                            VALUES (:5, :6, :7, :8)
                    """
                    params = [
                        pk, 
                        json.dumps(row_data), 
                        1 if is_new else 0, 
                        1 if is_updated else 0,
                        pk,
                        json.dumps(row_data),
                        1 if is_new else 0,
                        1 if is_updated else 0
                    ]
                    connector_service.execute_query(target_db, sql, params)
            
            else:
                # Generic approach
                for pk, row_data, is_new, is_updated in batch:
                    sql = f"""
                        INSERT INTO {temp_table_name} (record_key, record_data, is_new, is_updated)
                        VALUES (%s, %s, %s, %s)
                    """
                    params = [
                        pk, 
                        json.dumps(row_data), 
                        is_new, 
                        is_updated
                    ]
                    try:
                        connector_service.execute_query(target_db, sql, params)
                    except Exception:
                        # Try update if insert fails
                        update_sql = f"""
                            UPDATE {temp_table_name}
                            SET record_data = %s, is_new = %s, is_updated = %s
                            WHERE record_key = %s
                        """
                        update_params = [
                            json.dumps(row_data), 
                            is_new, 
                            is_updated,
                            pk
                        ]
                        connector_service.execute_query(target_db, update_sql, update_params)
                        
    def _load_from_temp_table(self, connector_service, target_db, temp_table_name, target_table, primary_key_original, primary_key):
        db_type = target_db.db_type_code
        count_sql = f"SELECT COUNT(*) as count FROM {temp_table_name} WHERE is_new = TRUE OR is_updated = TRUE"
        result = connector_service.execute_query(target_db, count_sql)
        count = result[0]['count'] if result else 0
        
        if count == 0:
            _logger.info(f"No changes to apply from temp table {temp_table_name}")
            return
        
        _logger.info(f"Loading {count} changes from temp table {temp_table_name} to {target_table}")
        
        select_sql = f"""
            SELECT record_key, record_data
            FROM {temp_table_name}
            WHERE is_new = TRUE OR is_updated = TRUE
        """
        rows = connector_service.execute_query(target_db, select_sql)
        if not rows:
            return
        
        batch_size = 500
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            target_rows = []
            for row in batch:
                record_data = row['record_data']
                if isinstance(record_data, str):
                    record_data = json.loads(record_data)
                elif not isinstance(record_data, dict):
                    raise ValueError(f"Unexpected type for record_data: {type(record_data)}")
                target_rows.append(record_data)
            
            # Use the full config from get_config_json
            config = self.get_config_json()
            etl_manager = self.env['etl.manager']
            etl_manager._batch_update_rows(
                connector_service, 
                target_db, 
                config, 
                target_rows
            )
        
        _logger.info(f"Successfully loaded {count} records from temp table to {target_table}")

    def _process_chunk_by_offset(self, connector_service, source_db, target_db, config, 
                           query_columns, column_map, primary_key_original,
                           offset, limit, last_hashes, current_hashes, stats, sync_log):
        """Process a chunk of data using OFFSET/LIMIT pagination"""
        etl_manager = self.env['etl.manager']
        formatted_columns = ', '.join(query_columns)
        batch_size = min(config['batch_size'], 20000)
        
        # Build query with OFFSET/LIMIT
        query = f"SELECT {formatted_columns} FROM {config['source_table']} " \
                f"ORDER BY {primary_key_original} " \
                f"OFFSET {offset} LIMIT {limit}"
        
        _logger.info(f"Executing offset chunk query: {query}")
        result_rows = connector_service.execute_query(source_db, query)
        
        if not result_rows:
            _logger.info(f"No rows found in chunk at offset {offset}")
            return
        
        # Create a temporary hash table
        temp_table_name = f"tmp_hash_{self.id}_{int(time.time())}"
        create_temp_table_sql = f"""
            CREATE TEMPORARY TABLE {temp_table_name} (
                record_key VARCHAR(255) PRIMARY KEY,
                record_hash VARCHAR(64) NOT NULL,
                is_processed BOOLEAN DEFAULT FALSE
            )
        """
        connector_service.execute_query(target_db, create_temp_table_sql)
        
        # Process rows in batches for memory efficiency
        rows_to_update = []
        
        for row in result_rows:
            # Process row - IMPORTANT: Make sure the parameter count matches the method definition
            transformed_row = etl_manager._transform_row(
                row, column_map, config, primary_key_original, 
                connector_service, target_db, temp_table_name, stats
            )
            
            if transformed_row:
                rows_to_update.append(transformed_row)
            
            # Update in batches
            if len(rows_to_update) >= 1000:
                etl_manager._batch_update_rows(connector_service, target_db, config, rows_to_update)
                rows_to_update = []
        
        # Final batch update
        if rows_to_update:
            etl_manager._batch_update_rows(connector_service, target_db, config, rows_to_update)
        
        # Update progress
        _logger.info(f"Processed {len(result_rows)} rows in chunk at offset {offset}")
        
        # Update sync log periodically
        sync_log.write({
            'total_records': stats['total_rows'],
            'new_records': stats['new_rows'],
            'updated_records': stats['updated_rows']
        })
        
        # Cleanup hash table
        connector_service.execute_query(target_db, f"DROP TABLE IF EXISTS {temp_table_name}")
        
        # Release memory
        result_rows = None
        rows_to_update = None
        gc.collect()
    
    def sync_table_job(self):
        """Single job for processing a table"""
        try:
            # Update job status
            self.write({
                'job_status': 'started',
                'progress_percentage': 0
            })
            
            # Create sync log
            sync_log = self.env['etl.sync.log'].create({
                'table_id': self.id,
                'start_time': fields.Datetime.now(),
                'status': 'running'
            })
            
            try:
                # Set up progress tracking
                def update_progress(progress, message=None):
                    """Update progress information"""
                    self.write({
                        'progress_percentage': progress,
                        'last_sync_message': message or f'Progress: {progress:.1f}%'
                    })
                    # Commit to release transaction locks
                    self.env.cr.commit()
                
                # Process table with progress tracking
                etl_manager = self.env['etl.manager'].with_context(
                    progress_tracker=update_progress
                )
                stats = etl_manager.process_table(self)
                
                # Update final status
                self.write({
                    'job_status': 'done',
                    'progress_percentage': 100,
                    'last_sync_status': 'success',
                    'last_sync_message': f'Sync completed successfully with {stats["total_rows"]} records'
                })
                
                # Update sync log
                sync_log.write({
                    'end_time': fields.Datetime.now(),
                    'status': 'success',
                    'total_records': stats.get('total_rows', 0),
                    'new_records': stats.get('new_rows', 0),
                    'updated_records': stats.get('updated_rows', 0)
                })
                
                return {
                    'table': self.name,
                    'status': 'success',
                    'message': f'Sync completed successfully with {stats["total_rows"]} records'
                }
                
            except Exception as e:
                raise e
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Sync job failed for table {self.name}: {error_message}")
            
            self.write({
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
            
            return {
                'table': self.name,
                'status': 'failed',
                'message': error_message
            }

    def action_view_jobs(self):
        """View jobs related to this table"""
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': _('Jobs'),
            'res_model': 'queue.job',
            'domain': [('uuid', '=', self.job_uuid)],
            'view_mode': 'tree,form',
            'context': self.env.context,
        }
        
    def action_cancel_job(self):
        """Cancel running job"""
        self.ensure_one()
        
        if not self.job_uuid or self.job_status not in ('pending', 'started'):
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Warning'),
                    'message': _('No active job to cancel.'),
                    'type': 'warning',
                    'sticky': False,
                }
            }
            
        try:
            # Get the job
            job = self.env['queue.job'].search([('uuid', '=', self.job_uuid)], limit=1)
            if job:
                # Cancel the job
                job.button_cancelled()
                
                # Update table status
                self.write({
                    'job_status': 'canceled',
                    'last_sync_status': 'failed',
                    'last_sync_message': 'Job canceled by user'
                })
                
                # Update any running sync logs
                sync_logs = self.env['etl.sync.log'].search([
                    ('table_id', '=', self.id),
                    ('status', '=', 'running')
                ])
                for log in sync_logs:
                    log.write({
                        'end_time': fields.Datetime.now(),
                        'status': 'failed',
                        'error_message': 'Job canceled by user'
                    })
                
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': _('Success'),
                        'message': _('Job has been canceled.'),
                        'type': 'success',
                        'sticky': False,
                    }
                }
            else:
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': _('Warning'),
                        'message': _('Job not found in queue.'),
                        'type': 'warning',
                        'sticky': False,
                    }
                }
        except Exception as e:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Error'),
                    'message': str(e),
                    'type': 'danger',
                    'sticky': True,
                }
            }

class ETLCategory(models.Model):
    _name = 'etl.category'
    _description = 'ETL Table Category'
    _order = 'sequence, name'
    
    name = fields.Char('Category Name', required=True)
    code = fields.Char('Category Code', required=True)
    sequence = fields.Integer('Sequence', default=10)
    active = fields.Boolean(default=True)
    
    _sql_constraints = [
        ('code_uniq', 'unique (code)', 'Category code must be unique!')
    ]

class ETLFrequency(models.Model):
    _name = 'etl.frequency'
    _description = 'ETL Sync Frequency'
    _order = 'sequence, name'
    
    name = fields.Char('Frequency Name', required=True)
    code = fields.Char('Frequency Code', required=True)
    interval_number = fields.Integer('Interval Number', default=1, required=True)
    interval_type = fields.Selection([
        ('minutes', 'Minutes'),
        ('hours', 'Hours'),
        ('days', 'Days'),
        ('weeks', 'Weeks'),
        ('months', 'Months')
    ], string='Interval Type', required=True)
    sequence = fields.Integer('Sequence', default=10)
    active = fields.Boolean(default=True)
    
    _sql_constraints = [
        ('code_uniq', 'unique (code)', 'Frequency code must be unique!')
    ]

class ETLColumnMapping(models.Model):
    _name = 'etl.column.mapping'
    _description = 'ETL Column Mapping'
    _order = 'sequence, id'

    sequence = fields.Integer('Sequence', default=10)
    table_id = fields.Many2one('etl.source.table', required=True, ondelete='cascade')
    source_column = fields.Char('Source Column', required=True)
    target_column = fields.Char('Target Column', required=True)
    mapping_type = fields.Selection([
        ('direct', 'Direct'),
        ('lookup', 'Lookup')
    ], required=True, default='direct')
    
    # For lookup mappings
    lookup_table = fields.Char('Lookup Table')
    lookup_key = fields.Char('Lookup Key')
    lookup_value = fields.Char('Lookup Value')
    
    active = fields.Boolean(default=True)

    @api.model
    def create(self, vals):
        """Override create to handle case normalization"""
        if vals.get('target_column'):
            vals['target_column'] = vals['target_column'].lower()
        if vals.get('lookup_table'):
            vals['lookup_table'] = vals['lookup_table'].lower()
        if vals.get('lookup_key'):
            vals['lookup_key'] = vals['lookup_key'].lower()
        if vals.get('lookup_value'):
            vals['lookup_value'] = vals['lookup_value'].lower()
        return super().create(vals)

    def write(self, vals):
        """Override write to handle case normalization"""
        if vals.get('target_column'):
            vals['target_column'] = vals['target_column'].lower()
        if vals.get('lookup_table'):
            vals['lookup_table'] = vals['lookup_table'].lower()
        if vals.get('lookup_key'):
            vals['lookup_key'] = vals['lookup_key'].lower()
        if vals.get('lookup_value'):
            vals['lookup_value'] = vals['lookup_value'].lower()
        return super().write(vals)
    
    @api.constrains('mapping_type', 'lookup_table', 'lookup_key', 'lookup_value')
    def _check_lookup_fields(self):
        for mapping in self:
            if mapping.mapping_type == 'lookup':
                if not (mapping.lookup_table and mapping.lookup_key and mapping.lookup_value):
                    raise ValidationError(_("Lookup mappings require lookup table, key, and value!"))

class ETLSyncLog(models.Model):
    _name = 'etl.sync.log'
    _description = 'ETL Synchronization Log'
    _order = 'create_date desc'

    table_id = fields.Many2one('etl.source.table', string='Table', required=True)
    start_time = fields.Datetime('Start Time', required=True)
    end_time = fields.Datetime('End Time')
    status = fields.Selection([
        ('success', 'Success'),
        ('failed', 'Failed'),
        ('running', 'Running'),
    ], required=True)
    total_records = fields.Integer('Total Records')
    new_records = fields.Integer('New Records')
    updated_records = fields.Integer('Updated Records')
    error_message = fields.Text('Error Message')
    row_hashes = fields.Text('Row Hashes', help="JSON string storing the row hashes for change detection")
    
    def name_get(self):
        return [(log.id, f"{log.table_id.name} - {log.start_time}") for log in self]

    @api.model
    def create(self, vals):
        """Override create to ensure row_hashes is properly formatted"""
        if 'row_hashes' in vals and isinstance(vals['row_hashes'], dict):
            vals['row_hashes'] = json.dumps(vals['row_hashes'])
        return super().create(vals)

    def write(self, vals):
        """Override write to ensure row_hashes is properly formatted"""
        if 'row_hashes' in vals and isinstance(vals['row_hashes'], dict):
            vals['row_hashes'] = json.dumps(vals['row_hashes'])
        return super().write(vals)
    