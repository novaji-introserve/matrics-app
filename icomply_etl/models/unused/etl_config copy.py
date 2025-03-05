# # -*- coding: utf-8 -*-
# from odoo import models, fields, api, _
# from odoo.exceptions import ValidationError
# import json
# import logging
# from odoo.addons.queue.queue_job.job import Job
# import math
# _logger = logging.getLogger(__name__)

# class ETLSourceTable(models.Model):
#     _name = 'etl.source.table'
#     _description = 'ETL Source Table Configuration'
#     _order = 'sequence, name'
#     _inherit = ['mail.thread', 'mail.activity.mixin']

#     name = fields.Char('Table Name', required=True, help="Source table name (e.g., tbl_customer)")
#     sequence = fields.Integer('Sequence', default=10)
#     target_table = fields.Char('Target Table', required=True, help="Target table name (e.g., res_partner)")
#     primary_key = fields.Char('Primary Key', required=True)
#     batch_size = fields.Integer('Batch Size', default=2000)
#     is_base_table = fields.Boolean('Is Base Table', help="Tables with no dependencies")
#     active = fields.Boolean(default=True)
#     progress_percentage = fields.Float('Progress', readonly=True, default=0)

#     category_id = fields.Many2one('etl.category', string='Category', required=True)
#     frequency_id = fields.Many2one('etl.frequency', string='Frequency', required=True)


#     job_uuid = fields.Char('Job UUID', readonly=True, copy=False)
#     job_status = fields.Selection([
#         ('pending', 'Pending'),
#         ('started', 'Started'),
#         ('done', 'Done'),
#         ('failed', 'Failed'),
#         ('canceled', 'Canceled'),
#     ], string='Job Status', readonly=True, copy=False)
    
    
#     dependency_ids = fields.Many2many(
#         'etl.source.table', 
#         'etl_table_dependencies', 
#         'table_id', 
#         'dependency_id', 
#         string='Dependencies'
#     )
    
#     mapping_ids = fields.One2many('etl.column.mapping', 'table_id', string='Column Mappings')
#     sync_log_ids = fields.One2many('etl.sync.log', 'table_id', string='Sync Logs')
    
#     last_sync_time = fields.Datetime('Last Sync Time', readonly=True)
#     last_sync_status = fields.Selection([
#         ('success', 'Success'),
#         ('failed', 'Failed'),
#         ('running', 'Running'),
#     ], string='Last Sync Status', readonly=True)
#     last_sync_message = fields.Text('Last Sync Message', readonly=True)
    
#     total_records_synced = fields.Integer('Total Records Synced', readonly=True)
    
#     @api.constrains('dependency_ids')
#     def _check_dependencies(self):
#         for table in self:
#             if table in table.dependency_ids:
#                 raise ValidationError(_("A table cannot depend on itself!"))
    
#     def get_config_json(self):
#         """Generate JSON configuration for ETL process"""
#         self.ensure_one()
        
#         # Normalize all mappings to lowercase
#         normalized_mappings = {}
#         for mapping in self.mapping_ids:
#             mapping_dict = {
#                 'target': mapping.target_column.lower(),
#                 'type': mapping.mapping_type,
#             }
            
#             if mapping.mapping_type == 'lookup':
#                 mapping_dict.update({
#                     'lookup_table': mapping.lookup_table.lower(),
#                     'lookup_key': mapping.lookup_key.lower(),
#                     'lookup_value': mapping.lookup_value.lower()
#                 })
            
#             # Store with lowercase source column as key
#             normalized_mappings[mapping.source_column.lower()] = mapping_dict
            
#         return {
#             'source_table': self.name.lower(),
#             'target_table': self.target_table.lower(),
#             'primary_key': self.primary_key.lower(),
#             'batch_size': self.batch_size,
#             'dependencies': [dep.name.lower() for dep in self.dependency_ids],
#             'mappings': normalized_mappings
#         }
    


#     def action_test_connection(self):
#         """Test database connections"""
#         self.ensure_one()
#         try:
#             etl_manager = self.env['etl.manager']
#             with etl_manager.get_connections() as (mssql_conn, pg_conn):
#                 mssql_cursor = mssql_conn.cursor()
#                 pg_cursor = pg_conn.cursor()
                
#                 return {
#                     'type': 'ir.actions.client',
#                     'tag': 'display_notification',
#                     'params': {
#                         'title': _('Success'),
#                         'message': _('Successfully connected to both databases!'),
#                         'type': 'success',
#                         'sticky': False,
#                     }
#                 }
#         except Exception as e:
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _('Error'),
#                     'message': str(e),
#                     'type': 'danger',
#                     'sticky': True,
#                 }
#             }
        

#     def action_sync_table(self):
#         """Queue a sync job with chunking for very large tables"""
#         self.ensure_one()
        
#         try:
#             # Check if table is large to determine if queue_job should be used
#             try:
#                 etl_manager = self.env['etl.manager']
#                 with etl_manager.get_connections() as (mssql_conn, pg_conn):
#                     config = self.get_config_json()
#                     mssql_cursor = mssql_conn.cursor()
#                     count_query = f"SELECT COUNT(*) FROM [{config['source_table']}]"
#                     mssql_cursor.execute(count_query)
#                     total_count = mssql_cursor.fetchone()[0]
                    
#                     # For small tables (< 20,000 rows), run synchronously
#                     if total_count < 20000:
#                         etl_manager.process_table(self)
#                         return {
#                             'type': 'ir.actions.client',
#                             'tag': 'display_notification',
#                             'params': {
#                                 'title': _('Success'),
#                                 'message': _('Table synchronization completed successfully.'),
#                                 'type': 'success',
#                                 'sticky': False,
#                             }
#                         }
                    
#                     # Get primary key for chunking
#                     primary_key = config['primary_key']
#                     primary_key_original = None
                    
#                     # Get original column name with proper case
#                     query = f"SELECT TOP 1 * FROM [{config['source_table']}]"
#                     mssql_cursor.execute(query)
#                     for col in mssql_cursor.description:
#                         if col[0].lower() == primary_key.lower():
#                             primary_key_original = col[0]
#                             break
                    
#                     if not primary_key_original:
#                         raise ValueError(f"Could not find primary key {primary_key} in table")
                    
#                     # For very large tables (> 500,000 rows), split into chunks
#                     if total_count > 500000:
#                         chunk_size = 150000  # Process 150k records per job
#                         chunks = math.ceil(total_count / chunk_size)
                        
#                         # Get range of primary key values
#                         min_query = f"SELECT MIN([{primary_key_original}]) FROM [{config['source_table']}]"
#                         max_query = f"SELECT MAX([{primary_key_original}]) FROM [{config['source_table']}]"
#                         mssql_cursor.execute(min_query)
#                         min_id = mssql_cursor.fetchone()[0]
#                         mssql_cursor.execute(max_query)
#                         max_id = mssql_cursor.fetchone()[0]
                        
#                         _logger.info(f"Splitting table {self.name} into {chunks} chunks (min_id={min_id}, max_id={max_id})")
                        
#                         # Create a main job record for tracking overall progress
#                         main_job = self.with_delay(
#                             description=f"Main sync job for table: {self.name}"
#                         ).sync_table_job_main(chunks)
                        
#                         # Update the main job UUID
#                         self.write({
#                             'job_uuid': main_job.uuid,
#                             'job_status': 'pending',
#                             'last_sync_status': 'running',
#                             'last_sync_message': f'Sync job queued in {chunks} chunks',
#                             'progress_percentage': 0
#                         })
                        
#                         # Queue multiple jobs with ID ranges
#                         for i in range(chunks):
#                             # Calculate chunk boundaries
#                             if isinstance(min_id, str) and isinstance(max_id, str):
#                                 # For string IDs, divide the chunks evenly based on index
#                                 chunk_min = min_id if i == 0 else f"{self.name}_chunk_{i}"
#                                 chunk_max = max_id if i == chunks - 1 else f"{self.name}_chunk_{i+1}"
#                             else:
#                                 # For numeric IDs, calculate range
#                                 chunk_min = min_id + (i * (max_id - min_id) // chunks)
#                                 chunk_max = min_id + ((i + 1) * (max_id - min_id) // chunks)
#                                 if i == chunks - 1:
#                                     chunk_max = max_id  # Ensure we include the max value
                            
#                             # Queue this chunk's job
#                             self.with_delay(
#                                 description=f"Sync ETL table: {self.name} (chunk {i+1}/{chunks})"
#                             ).sync_table_job_chunk(chunk_min, chunk_max, i+1, chunks)
                        
#                         return {
#                             'type': 'ir.actions.client',
#                             'tag': 'display_notification',
#                             'params': {
#                                 'title': _('Jobs Queued'),
#                                 'message': _(f'Table synchronization split into {chunks} jobs and queued.'),
#                                 'type': 'success',
#                                 'sticky': False,
#                             }
#                         }
                    
#                     # For medium-size tables, use a single job
#                     job = self.with_delay(
#                         description=f"Sync ETL table: {self.name}"
#                     ).sync_table_job()
                    
#                     self.write({
#                         'job_uuid': job.uuid,
#                         'job_status': 'pending',
#                         'last_sync_status': 'running',
#                         'last_sync_message': 'Sync job queued',
#                         'progress_percentage': 0
#                     })
                    
#                     return {
#                         'type': 'ir.actions.client',
#                         'tag': 'display_notification',
#                         'params': {
#                             'title': _('Job Queued'),
#                             'message': _('Table synchronization job has been queued.'),
#                             'type': 'success',
#                             'sticky': False,
#                         }
#                     }
#             except Exception as e:
#                 _logger.warning(f"Could not determine table size: {str(e)}")
#                 # Fall back to standard job if we can't determine size
#                 job = self.with_delay(
#                     description=f"Sync ETL table: {self.name}"
#                 ).sync_table_job()
                
#                 self.write({
#                     'job_uuid': job.uuid,
#                     'job_status': 'pending',
#                     'last_sync_status': 'running',
#                     'last_sync_message': 'Sync job queued (fallback method)',
#                     'progress_percentage': 0
#                 })
                
#                 return {
#                     'type': 'ir.actions.client',
#                     'tag': 'display_notification',
#                     'params': {
#                         'title': _('Job Queued'),
#                         'message': _('Table synchronization job has been queued.'),
#                         'type': 'success',
#                         'sticky': False,
#                     }
#                 }
#         except Exception as e:
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _('Error'),
#                     'message': str(e),
#                     'type': 'danger',
#                     'sticky': True,
#                 }
#             }

#     def sync_table_job_main(self, total_chunks):
#         """Main job that coordinates multiple chunk jobs"""
#         try:
#             self.write({
#                 'job_status': 'started',
#                 'progress_percentage': 0
#             })
            
#             # This job doesn't do processing itself - it just waits for all chunks
#             # to complete and then updates the final status
            
#             _logger.info(f"Main sync job started for table {self.name} with {total_chunks} chunks")
            
#             return {
#                 'table': self.name,
#                 'status': 'started',
#                 'message': f'Main sync job started with {total_chunks} chunks'
#             }
#         except Exception as e:
#             error_message = str(e)
#             _logger.error(f"Error in main sync job for table {self.name}: {error_message}")
            
#             self.write({
#                 'job_status': 'failed',
#                 'last_sync_status': 'failed',
#                 'last_sync_message': f'Main job error: {error_message}'
#             })
            
#             return {
#                 'table': self.name,
#                 'status': 'failed',
#                 'message': error_message
#             }

#     def sync_table_job_chunk(self, min_id, max_id, chunk_num, total_chunks):
#         """Process a chunk of a large table"""
#         try:
#             # We don't update the main job status since this is just a chunk
#             _logger.info(f"Starting chunk {chunk_num}/{total_chunks} for table {self.name}")
            
#             etl_manager = self.env['etl.manager']
#             stats = etl_manager.process_table_chunk(self, min_id, max_id)
            
#             # Update progress in main job
#             current_progress = (chunk_num / total_chunks) * 100
#             self.write({
#                 'progress_percentage': current_progress,
#                 'last_sync_message': f'Processed chunk {chunk_num}/{total_chunks} ({current_progress:.1f}%)'
#             })
            
#             # If this is the last chunk, mark the main job as complete
#             if chunk_num == total_chunks:
#                 self.write({
#                     'job_status': 'done',
#                     'progress_percentage': 100,
#                     'last_sync_message': f'All {total_chunks} chunks completed successfully'
#                 })
            
#             return {
#                 'table': self.name,
#                 'status': 'success',
#                 'chunk': chunk_num,
#                 'total_chunks': total_chunks,
#                 'message': f'Chunk from {min_id} to {max_id} completed successfully',
#                 'stats': stats
#             }
#         except Exception as e:
#             error_message = str(e)
#             _logger.error(f"Sync job failed for table {self.name} chunk {chunk_num}: {error_message}")
            
#             # Don't mark the main job as failed, just update the message
#             self.write({
#                 'last_sync_message': f'Error in chunk {chunk_num}/{total_chunks}: {error_message}'
#             })
            
#             return {
#                 'table': self.name,
#                 'status': 'failed',
#                 'chunk': chunk_num,
#                 'total_chunks': total_chunks,
#                 'message': error_message
#             }

#     # @Job(default_channel='root', default_priority=10)
#     def sync_table_job(self):
#         """Background job to sync a table"""
#         try:
#             # Update job status to started
#             self.write({
#                 'job_status': 'started',
#                 'progress_percentage': 0  # Initialize progress to 0%
#             })
            
#             # Wrap process_table in a custom handler to update progress
#             original_logger_info = _logger.info
            
#             def custom_logger_info(message):
#                 # Intercept progress log messages to update the progress bar
#                 original_logger_info(message)
#                 if "Progress:" in message:
#                     try:
#                         # Extract progress percentage from the log message
#                         progress_str = message.split("Progress:")[1].split("%")[0].strip()
#                         progress = float(progress_str)
                        
#                         # Update progress on the record
#                         self.write({
#                             'progress_percentage': progress,
#                             'last_sync_message': message
#                         })
#                     except Exception:
#                         pass  # If parsing fails, just continue
            
#             # Replace logger temporarily
#             _logger.info = custom_logger_info
            
#             try:
#                 # Use your existing process_table method
#                 etl_manager = self.env['etl.manager']
#                 etl_manager.process_table(self)
                
#                 # Restore original logger
#                 _logger.info = original_logger_info
                
#                 # Update final status
#                 self.write({
#                     'job_status': 'done',
#                     'progress_percentage': 100,  # Set to 100% when complete
#                     'last_sync_message': f'Sync completed successfully at {fields.Datetime.now()}'
#                 })
                
#                 return {
#                     'table': self.name,
#                     'status': 'success',
#                     'message': 'Sync completed successfully'
#                 }
#             finally:
#                 # Ensure logger is restored even if there's an exception
#                 _logger.info = original_logger_info
            
#         except Exception as e:
#             error_message = str(e)
#             _logger.error(f"Sync job failed for table {self.name}: {error_message}")
            
#             self.write({
#                 'job_status': 'failed',
#                 'last_sync_status': 'failed',
#                 'last_sync_message': error_message
#             })
            
#             return {
#                 'table': self.name,
#                 'status': 'failed',
#                 'message': error_message
#             }

#     def action_view_jobs(self):
#         """View jobs related to this table"""
#         self.ensure_one()
#         return {
#             'type': 'ir.actions.act_window',
#             'name': _('Jobs'),
#             'res_model': 'queue.job',
#             'domain': [('uuid', '=', self.job_uuid)],
#             'view_mode': 'tree,form',
#             'context': self.env.context,
#         }

# class ETLCategory(models.Model):
#     _name = 'etl.category'
#     _description = 'ETL Table Category'
#     _order = 'sequence, name'
    
#     name = fields.Char('Category Name', required=True)
#     code = fields.Char('Category Code', required=True)
#     sequence = fields.Integer('Sequence', default=10)
#     active = fields.Boolean(default=True)
    
#     _sql_constraints = [
#         ('code_uniq', 'unique (code)', 'Category code must be unique!')
#     ]

# class ETLFrequency(models.Model):
#     _name = 'etl.frequency'
#     _description = 'ETL Sync Frequency'
#     _order = 'sequence, name'
    
#     name = fields.Char('Frequency Name', required=True)
#     code = fields.Char('Frequency Code', required=True)
#     interval_number = fields.Integer('Interval Number', default=1, required=True)
#     interval_type = fields.Selection([
#         ('minutes', 'Minutes'),
#         ('hours', 'Hours'),
#         ('days', 'Days'),
#         ('weeks', 'Weeks'),
#         ('months', 'Months')
#     ], string='Interval Type', required=True)
#     sequence = fields.Integer('Sequence', default=10)
#     active = fields.Boolean(default=True)
    
#     _sql_constraints = [
#         ('code_uniq', 'unique (code)', 'Frequency code must be unique!')
#     ]

# class ETLColumnMapping(models.Model):
#     _name = 'etl.column.mapping'
#     _description = 'ETL Column Mapping'
#     _order = 'sequence, id'

#     sequence = fields.Integer('Sequence', default=10)
#     table_id = fields.Many2one('etl.source.table', required=True, ondelete='cascade')
#     source_column = fields.Char('Source Column', required=True)
#     target_column = fields.Char('Target Column', required=True)
#     mapping_type = fields.Selection([
#         ('direct', 'Direct'),
#         ('lookup', 'Lookup')
#     ], required=True, default='direct')
    
#     # For lookup mappings
#     lookup_table = fields.Char('Lookup Table')
#     lookup_key = fields.Char('Lookup Key')
#     lookup_value = fields.Char('Lookup Value')
    
#     active = fields.Boolean(default=True)

#     @api.model
#     def create(self, vals):
#         """Override create to handle case normalization"""
#         if vals.get('target_column'):
#             vals['target_column'] = vals['target_column'].lower()
#         if vals.get('lookup_table'):
#             vals['lookup_table'] = vals['lookup_table'].lower()
#         if vals.get('lookup_key'):
#             vals['lookup_key'] = vals['lookup_key'].lower()
#         if vals.get('lookup_value'):
#             vals['lookup_value'] = vals['lookup_value'].lower()
#         return super().create(vals)

#     def write(self, vals):
#         """Override write to handle case normalization"""
#         if vals.get('target_column'):
#             vals['target_column'] = vals['target_column'].lower()
#         if vals.get('lookup_table'):
#             vals['lookup_table'] = vals['lookup_table'].lower()
#         if vals.get('lookup_key'):
#             vals['lookup_key'] = vals['lookup_key'].lower()
#         if vals.get('lookup_value'):
#             vals['lookup_value'] = vals['lookup_value'].lower()
#         return super().write(vals)
    
#     @api.constrains('mapping_type', 'lookup_table', 'lookup_key', 'lookup_value')
#     def _check_lookup_fields(self):
#         for mapping in self:
#             if mapping.mapping_type == 'lookup':
#                 if not (mapping.lookup_table and mapping.lookup_key and mapping.lookup_value):
#                     raise ValidationError(_("Lookup mappings require lookup table, key, and value!"))

# class ETLSyncLog(models.Model):
#     _name = 'etl.sync.log'
#     _description = 'ETL Synchronization Log'
#     _order = 'create_date desc'

#     table_id = fields.Many2one('etl.source.table', string='Table', required=True)
#     start_time = fields.Datetime('Start Time', required=True)
#     end_time = fields.Datetime('End Time')
#     status = fields.Selection([
#         ('success', 'Success'),
#         ('failed', 'Failed'),
#         ('running', 'Running'),
#     ], required=True)
#     total_records = fields.Integer('Total Records')
#     new_records = fields.Integer('New Records')
#     updated_records = fields.Integer('Updated Records')
#     error_message = fields.Text('Error Message')
#     row_hashes = fields.Text('Row Hashes', help="JSON string storing the row hashes for change detection")
    
#     def name_get(self):
#         return [(log.id, f"{log.table_id.name} - {log.start_time}") for log in self]

#     @api.model
#     def create(self, vals):
#         """Override create to ensure row_hashes is properly formatted"""
#         if 'row_hashes' in vals and isinstance(vals['row_hashes'], dict):
#             vals['row_hashes'] = json.dumps(vals['row_hashes'])
#         return super().create(vals)

#     def write(self, vals):
#         """Override write to ensure row_hashes is properly formatted"""
#         if 'row_hashes' in vals and isinstance(vals['row_hashes'], dict):
#             vals['row_hashes'] = json.dumps(vals['row_hashes'])
#         return super().write(vals)