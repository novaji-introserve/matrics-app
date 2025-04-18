# -*- coding: utf-8 -*-
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
        """Generate JSON configuration for ETL process"""
        self.ensure_one()
        
        # Normalize all mappings to lowercase
        normalized_mappings = {}
        for mapping in self.mapping_ids:
            mapping_dict = {
                'target': mapping.target_column.lower(),
                'type': mapping.mapping_type,
            }
            
            if mapping.mapping_type == 'lookup':
                mapping_dict.update({
                    'lookup_table': mapping.lookup_table.lower(),
                    'lookup_key': mapping.lookup_key.lower(),
                    'lookup_value': mapping.lookup_value.lower()
                })
            
            # Store with lowercase source column as key
            normalized_mappings[mapping.source_column.lower()] = mapping_dict
            
        return {
            'source_table': self.name.lower(),
            'target_table': self.target_table.lower(),
            'primary_key': self.primary_key.lower(),
            'batch_size': self.batch_size,
            'dependencies': [dep.name.lower() for dep in self.dependency_ids],
            'mappings': normalized_mappings,
            'source_db_id': self.source_db_connection_id.id,
            'target_db_id': self.target_db_connection_id.id
        }
    
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
        """Queue a sync job with chunking for very large tables"""
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
            # Check table size to determine chunking strategy
            connector_service = self.env['etl.database.connector.service']
            
            try:
                # Get count of records in source table
                total_count = connector_service.get_table_count(
                    self.source_db_connection_id, 
                    self.name.lower()
                )
                
                _logger.info(f"Table {self.name} has {total_count} records")
                    
                # For small tables (< 20,000 rows), run synchronously
                if total_count < 20000:
                    self.env['etl.manager'].process_table(self)
                    self.env.cr.commit()
                    return {
                        'type': 'ir.actions.client',
                        'tag': 'display_notification',
                        'params': {
                            'title': _('Success'),
                            'message': _('Table synchronization completed successfully.'),
                            'type': 'success',
                            'sticky': False,
                        }
                    }
                
                # For large tables, determine ID range
                source_columns = connector_service.get_columns(
                    self.source_db_connection_id, 
                    self.name.lower()
                )
                
                primary_key = self.primary_key.lower()
                primary_key_original = source_columns.get(primary_key)
                
                if not primary_key_original:
                    raise ValueError(f"Could not find primary key {primary_key} in table")
                
                # For very large tables (> 500,000 rows), split into chunks
                if total_count > 500000:
                    # Calculate chunk boundaries
                    chunks = math.ceil(total_count / 150000)
                    
                    # Get primary key range
                    min_query = f"SELECT MIN({primary_key_original}) AS min_id FROM {self.name.lower()}"
                    max_query = f"SELECT MAX({primary_key_original}) AS max_id FROM {self.name.lower()}"
                    
                    min_result = connector_service.execute_query(self.source_db_connection_id, min_query)
                    max_result = connector_service.execute_query(self.source_db_connection_id, max_query)
                    
                    min_id = min_result[0]['min_id']
                    max_id = max_result[0]['max_id']
                    
                    _logger.info(f"ID range for {self.name}: {min_id} to {max_id}")
                    
                    # Create main job
                    main_job = self.with_delay(
                        description=f"Main sync job for table: {self.name}",
                        channel="etl"
                    ).sync_table_job_main(chunks, total_count)
                    
                    # Update table status
                    self.write({
                        'job_uuid': main_job.uuid,
                        'job_status': 'pending',
                        'last_sync_status': 'running',
                        'last_sync_message': f'Sync job queued in {chunks} chunks',
                        'progress_percentage': 0
                    })
                    
                    # Calculate chunk boundaries
                    if isinstance(min_id, (int, float)) and isinstance(max_id, (int, float)):
                        # Numeric IDs
                        step = (max_id - min_id) / chunks
                        for i in range(chunks):
                            chunk_min = min_id + (i * step)
                            chunk_max = min_id + ((i + 1) * step)
                            if i == chunks - 1:
                                chunk_max = max_id
                                
                            self.with_delay(
                                description=f"Sync ETL table: {self.name} (chunk {i+1}/{chunks})",
                                channel="etl",
                                priority=10
                            ).sync_table_job_chunk(chunk_min, chunk_max, i+1, chunks)
                    else:
                        # String IDs - just use row ranges
                        rows_per_chunk = math.ceil(total_count / chunks)
                        for i in range(chunks):
                            self.with_delay(
                                description=f"Sync ETL table: {self.name} (chunk {i+1}/{chunks})",
                                channel="etl",
                                priority=10
                            ).sync_table_job_chunk_by_offset(i * rows_per_chunk, rows_per_chunk, i+1, chunks)
                    
                    return {
                        'type': 'ir.actions.client',
                        'tag': 'display_notification',
                        'params': {
                            'title': _('Jobs Queued'),
                            'message': _(f'Table synchronization split into {chunks} jobs and queued.'),
                            'type': 'success',
                            'sticky': False,
                        }
                    }
                
                # For medium-size tables, use a single job
                job = self.with_delay(
                    description=f"Sync ETL table: {self.name}",
                    channel="etl"
                ).sync_table_job()
                
                self.write({
                    'job_uuid': job.uuid,
                    'job_status': 'pending',
                    'last_sync_status': 'running',
                    'last_sync_message': 'Sync job queued',
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
                _logger.warning(f"Could not determine table size: {str(e)}")
                # Fall back to standard job
                job = self.with_delay(
                    description=f"Sync ETL table: {self.name}",
                    channel="etl"
                ).sync_table_job()
                
                self.write({
                    'job_uuid': job.uuid,
                    'job_status': 'pending',
                    'last_sync_status': 'running',
                    'last_sync_message': 'Sync job queued (fallback method)',
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
        """Process a chunk of a table based on offset/limit (for string IDs)"""
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
            
            # Process chunk using OFFSET/LIMIT pagination
            self._process_chunk_by_offset(
                connector_service, 
                source_db, 
                target_db, 
                config, 
                query_columns, 
                column_map, 
                primary_key_original,
                offset, 
                limit, 
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
            
            # Commit to release locks
            self.env.cr.commit()
            
            # Release memory
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
            
            # Commit to release locks
            self.env.cr.commit()
            
            return {
                'table': self.name,
                'status': 'failed',
                'chunk': chunk_num,
                'total_chunks': total_chunks,
                'message': error_message
            }
    
    def _process_chunk_by_offset(self, connector_service, source_db, target_db, config, 
                                query_columns, column_map, primary_key_original,
                                offset, limit, last_hashes, current_hashes, stats, sync_log):
        """Process a chunk of data using OFFSET/LIMIT pagination"""
        etl_manager = self.env['etl.manager']
        formatted_columns = ', '.join(query_columns)
        batch_size = min(config['batch_size'], 5000)
        
        # Build query with OFFSET/LIMIT
        query = f"SELECT {formatted_columns} FROM {config['source_table']} " \
                f"ORDER BY {primary_key_original} " \
                f"OFFSET {offset} LIMIT {limit}"
        
        _logger.info(f"Executing offset chunk query: {query}")
        result_rows = connector_service.execute_query(source_db, query)
        
        if not result_rows:
            _logger.info(f"No rows found in chunk at offset {offset}")
            return
        
        # Process rows in batches for memory efficiency
        rows_to_update = []
        
        for row in result_rows:
            # Process row
            transformed_row = etl_manager._transform_row(
                row, column_map, config, primary_key_original, 
                connector_service, target_db, last_hashes, 
                current_hashes, stats
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
    