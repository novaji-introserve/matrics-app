# import logging
# import time
# import os
# import traceback
# from datetime import datetime, timedelta
# from odoo import api, fields, models, tools, SUPERUSER_ID, _

# _logger = logging.getLogger(__name__)

# class OpenSanctionsJobQueue(models.Model):
#     """
#     Model to manage OpenSanctions data import jobs with chunking and auto-population
#     """
#     _name = 'opensanctions.job.queue'
#     _description = 'OpenSanctions Job Queue'
#     _order = 'priority desc, create_date desc'
    
#     name = fields.Char('Job Name', required=True)
#     source_id = fields.Many2one('pep.source', string='Data Source', required=True, ondelete='cascade')
#     job_type = fields.Selection([
#         ('csv', 'CSV Import'),
#         ('api', 'API Import')
#     ], string='Job Type', required=True, default='csv')
#     state = fields.Selection([
#         ('pending', 'Pending'),
#         ('running', 'Running'),
#         ('done', 'Completed'),
#         ('failed', 'Failed'),
#         ('cancelled', 'Cancelled')
#     ], string='Status', default='pending', required=True)
#     priority = fields.Integer('Priority', default=10, 
#                              help="Higher number means higher priority")
#     user_id = fields.Many2one('res.users', string='Created By', default=lambda self: self.env.user)
#     create_date = fields.Datetime('Created On', readonly=True)
#     start_date = fields.Datetime('Started On', readonly=True)
#     end_date = fields.Datetime('Completed On', readonly=True)
#     duration = fields.Float('Duration (seconds)', readonly=True)
#     result = fields.Text('Job Result', readonly=True)
#     log = fields.Text('Job Log', readonly=True)
#     records_created = fields.Integer('Records Created', readonly=True, default=0)
#     records_updated = fields.Integer('Records Updated', readonly=True, default=0)
#     records_failed = fields.Integer('Records Failed', readonly=True, default=0)
#     next_run = fields.Datetime('Next Scheduled Run', 
#                               help="Schedule this job to run at a specific time")
#     run_as_user_id = fields.Many2one('res.users', string='Run As User', 
#                                     default=lambda self: self.env.user,
#                                     help="The user whose permissions will be used to run this job")
#     api_limit = fields.Integer('API Result Limit', default=1000,
#                               help="Maximum number of results to fetch from API")
#     file_path = fields.Char('CSV File Path', readonly=True,
#                            help="Path to the downloaded CSV file")
#     batch_size = fields.Integer('Batch Size', default=500,
#                               help="Number of records to process in each batch")
#     debug_mode = fields.Boolean('Debug Mode', default=False,
#                                help="Enable verbose logging for debugging")

#     base_url = fields.Char(related='source_id.base_url', string='Base URL', readonly=True)
#     csv_path = fields.Char(related='source_id.csv_path', string='CSV Path', readonly=True)
#     csv_delimiter = fields.Char(related='source_id.csv_delimiter', string='CSV Delimiter', readonly=True)
#     use_api = fields.Boolean(related='source_id.use_api', string='Use API', readonly=True)
#     api_url = fields.Char(related='source_id.api_url', string='API URL', readonly=True)
#     api_endpoint = fields.Char(related='source_id.api_endpoint', string='API Endpoint', readonly=True)
#     is_opensanctions = fields.Boolean(related='source_id.is_opensanctions', string='Is OpenSanctions', readonly=True)
    
#     # Chunking fields
#     is_chunk = fields.Boolean('Is Chunk Job', default=False, 
#                              help="Indicates this job is a chunk of a larger job")
#     parent_job_id = fields.Many2one('opensanctions.job.queue', string='Parent Job',
#                                    help="Parent job that created this chunk")
#     chunk_index = fields.Integer('Chunk Index', default=0,
#                                help="Starting index for this chunk")
#     chunk_size = fields.Integer('Chunk Size', default=5000,
#                               help="Maximum number of records to process in this chunk")
#     total_chunks = fields.Integer('Total Chunks', default=0,
#                                 help="Total number of chunks for the parent job")
#     chunks_completed = fields.Integer('Chunks Completed', default=0,
#                                     help="Number of chunks completed for the parent job")
#     total_records = fields.Integer('Total Records', default=0,
#                                  help="Total number of records in the source file")
    
#     @api.onchange('source_id')
#     def _onchange_source_id(self):
#         """
#         Auto-populate fields when source is selected
#         """
#         if self.source_id:
#             # Set job name based on source
#             self.name = f"Import - {self.source_id.name}" 
            
#             # Set job type based on source configuration
#             if self.source_id.source_format == 'csv':
#                 self.job_type = 'csv'
#             elif self.source_id.source_format == 'api' and self.source_id.use_api and self.source_id.api_key:
#                 self.job_type = 'api'
#             else:
#                 self.job_type = 'csv'  # Default to CSV if nothing else is configured
                
#             # Set priority higher for OpenSanctions
#             if self.source_id.is_opensanctions:
#                 self.priority = 15
#             else:
#                 self.priority = 10
                
#             # Set limits based on params
#             params = self.env['ir.config_parameter'].sudo()
#             self.batch_size = int(params.get_param('compliance_management.import_batch_size', '500'))
            
#             if self.source_id.use_api and self.job_type == 'api':
#                 self.api_limit = 1000  # Default API limit

#     def name_get(self):
#         result = []
#         for job in self:
#             source_name = job.source_id.name or 'Unknown Source'
#             state_label = dict(self._fields['state'].selection).get(job.state)
            
#             # For chunk jobs, show chunk information
#             if job.is_chunk:
#                 result.append((job.id, f"{job.name} - Chunk {job.chunk_index}/{job.total_records} ({state_label})"))
#             else:
#                 result.append((job.id, f"{job.name} ({source_name}) - {state_label}"))
                
#         return result

#     @api.model
#     def create(self, vals):
#         """Override create to set a default name if not provided"""
#         if not vals.get('name') and vals.get('source_id'):
#             source = self.env['pep.source'].browse(vals.get('source_id'))
#             job_type = vals.get('job_type', 'csv')
            
#             # For chunk jobs, include chunk information in name
#             if vals.get('is_chunk', False):
#                 start_index = vals.get('chunk_index', 0)
#                 chunk_size = vals.get('chunk_size', 5000)
#                 end_index = start_index + chunk_size
#                 vals['name'] = f"Chunk {start_index}-{end_index} - {source.name} ({job_type})"
#             else:
#                 vals['name'] = f"Import - {source.name} ({job_type})"
                
#         return super(OpenSanctionsJobQueue, self).create(vals)
    
#     def action_run_job(self):
#         """Manually trigger job execution"""
#         self.ensure_one()
        
#         if self.state in ['running']:
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("Cannot Run Job"),
#                     'message': _("This job is already running."),
#                     'sticky': False,
#                     'type': 'warning',
#                 }
#             }
            
#         # Allow rerunning completed jobs
#         if self.state in ['done']:
#             # Ask for confirmation
#             return {
#                 'type': 'ir.actions.act_window',
#                 'name': _('Rerun Completed Job?'),
#                 'res_model': 'opensanctions.job.confirm',
#                 'view_mode': 'form',
#                 'target': 'new',
#                 'context': {'default_job_id': self.id, 'default_action': 'rerun'},
#             }
            
#         # Check if queue_job is installed
#         queue_job = self.env['ir.module.module'].sudo().search(
#             [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
            
#         if queue_job:
#             # Use unique identity key to prevent duplicate jobs
#             identity_key = f"opensanctions_job_{self.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
#             self.with_delay(priority=self.priority, identity_key=identity_key).process_job()
            
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("Job Queued"),
#                     'message': _("The job has been queued for processing."),
#                     'sticky': False,
#                     'type': 'success',
#                 }
#             }
#         else:
#             # Direct execution for non-queue environment
#             self.process_job()
            
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("Job Started"),
#                     'message': _("The job has been started."),
#                     'sticky': False,
#                     'type': 'success',
#                 }
#             }
    
#     def action_cancel_job(self):
#         """Cancel a pending job"""
#         for job in self:
#             if job.state == 'pending':
#                 job.write({
#                     'state': 'cancelled',
#                     'end_date': fields.Datetime.now(),
#                     'result': 'Job was cancelled by user'
#                 })
                
#         return {
#             'type': 'ir.actions.client',
#             'tag': 'display_notification',
#             'params': {
#                 'title': _("Job Cancelled"),
#                 'message': _("The selected jobs have been cancelled."),
#                 'sticky': False,
#                 'type': 'info',
#             }
#         }
    
#     def action_reset_job(self):
#         """Reset a failed or cancelled job to pending"""
#         for job in self:
#             if job.state in ['failed', 'cancelled', 'done']:
#                 job.write({
#                     'state': 'pending',
#                     'start_date': False,
#                     'end_date': False,
#                     'duration': 0,
#                     'result': False,
#                     'log': job.log + '\n\n' + f"--- Job reset on {fields.Datetime.now()} ---" if job.log else ''
#                 })
                
#         return {
#             'type': 'ir.actions.client',
#             'tag': 'display_notification',
#             'params': {
#                 'title': _("Job Reset"),
#                 'message': _("The selected jobs have been reset to pending status."),
#                 'sticky': False,
#                 'type': 'info',
#             }
#         }
        
#     def process_job(self):
#         """
#         Process a job based on its type
#         This is the main job execution method
#         """
#         self.ensure_one()
        
#         if self.state == 'running':
#             _logger.warning(f"Job {self.name} (ID: {self.id}) is already running")
#             return False
            
#         # Mark job as running
#         start_time = time.time()
#         self.write({
#             'state': 'running',
#             'start_date': fields.Datetime.now(),
#             'log': f"Job started at {fields.Datetime.now()}\n"
#         })
        
#         try:
#             # Run job as specified user if different from current user
#             if self.run_as_user_id and self.run_as_user_id.id != self.env.uid:
#                 # Switch to a new environment with the specified user
#                 env = api.Environment(self.env.cr, self.run_as_user_id.id, self.env.context.copy())
#                 job = env['opensanctions.job.queue'].browse(self.id)
#                 source = env['pep.source'].browse(self.source_id.id)
#             else:
#                 job = self
#                 source = self.source_id
                
#             # Enable debug logging if debug mode is on
#             if self.debug_mode:
#                 self._append_log("Debug mode enabled - verbose logging will be used")
#                 logging.getLogger('odoo.addons.compliance_management').setLevel(logging.DEBUG)
                
#             # Initialize services
#             from ..services.open_sanctions import OpenSanctions
#             from ..services.open_sanctions_importer import OpenSanctionsImporter
            
#             service = OpenSanctions(self.env)
#             importer = OpenSanctionsImporter(self.env)
            
#             # Set batch size for importer
#             importer.batch_size = self.batch_size or 500
            
#             # Ensure the storage directory exists
#             storage_dir = self.env['ir.config_parameter'].sudo().get_param(
#                 'compliance_management.pep_storage_dir', '/media/pep_list_data')
                
#             if not os.path.exists(storage_dir):
#                 try:
#                     os.makedirs(storage_dir, exist_ok=True)
#                     self._append_log(f"Created storage directory: {storage_dir}")
#                 except Exception as e:
#                     self._append_log(f"Error creating storage directory: {str(e)}")
            
#             # Process based on job type
#             if self.is_chunk:
#                 # This is a chunk job - process only the specified chunk
#                 self._append_log(f"Processing chunk {self.chunk_index} for parent job {self.parent_job_id.id}")
#                 self._process_csv_chunk(service, importer, source)
#             elif self.job_type == 'csv':
#                 # CSV-only job
#                 self._process_csv_job(service, importer, source)
#             elif self.job_type == 'api':
#                 # API-only job
#                 self._process_api_job(service, importer, source)
                
#             # Calculate duration
#             end_time = time.time()
#             duration = end_time - start_time
            
#             # Determine overall job status
#             if self.is_chunk:
#                 if self.parent_job_id:
#                     # Update parent job with atomic operation to avoid race conditions
#                     self.env.cr.execute("""
#                         UPDATE opensanctions_job_queue 
#                         SET chunks_completed = chunks_completed + 1,
#                             records_created = records_created + %s,
#                             records_updated = records_updated + %s,
#                             records_failed = records_failed + %s
#                         WHERE id = %s
#                     """, (self.records_created, self.records_updated, self.records_failed, self.parent_job_id.id))
                    
#                     # Refresh parent job data
#                     self.parent_job_id.refresh()
                    
#                     # Check if this is the last chunk
#                     if self.parent_job_id.chunks_completed >= self.parent_job_id.total_chunks:
#                         self.parent_job_id.write({
#                             'state': 'done',
#                             'end_date': fields.Datetime.now(),
#                             'result': f"All chunks completed: {self.parent_job_id.records_created} records created, {self.parent_job_id.records_updated} updated"
#                         })
#                         self._append_log(f"All chunks completed. Updated parent job {self.parent_job_id.id} status to 'done'.")
                    
#                 status = 'done'
#                 message = f"Chunk job completed: {self.records_created} records created, {self.records_updated} updated in {duration:.2f} seconds"
#             else:
#                 if self.records_created > 0 or self.records_updated > 0:
#                     status = 'done'
#                     message = f"Job completed successfully: {self.records_created} records created, {self.records_updated} updated in {duration:.2f} seconds"
#                 else:
#                     status = 'done'
#                     message = f"Job completed with no changes: {duration:.2f} seconds"
            
#             # Update last_update on source
#             source.write({
#                 'last_update': fields.Datetime.now()
#             })
            
#             # Complete job
#             self._append_log(f"Job completed with status: {status}")
#             self._append_log(message)
            
#             self.write({
#                 'state': status,
#                 'end_date': fields.Datetime.now(),
#                 'duration': duration,
#                 'result': message
#             })
            
#             # Reset debug logging if it was enabled
#             if self.debug_mode:
#                 logging.getLogger('odoo.addons.compliance_management').setLevel(logging.INFO)
            
#             # Commit to ensure changes are saved before next job runs
#             self.env.cr.commit()
            
#             # Clean up old chunk jobs if this was a parent job and is now done
#             if not self.is_chunk and self.total_chunks > 0 and status == 'done':
#                 self._cleanup_chunk_jobs()
            
#             return {
#                 'status': status,
#                 'message': message,
#                 'records_created': self.records_created,
#                 'records_updated': self.records_updated
#             }
            
#         except Exception as e:
#             error_trace = traceback.format_exc()
#             _logger.error(f"Error in job: {str(e)}\n{error_trace}")
            
#             # Calculate duration
#             end_time = time.time()
#             duration = end_time - start_time
            
#             # Complete job with error
#             self._append_log(f"Job failed with error: {str(e)}")
#             self._append_log(error_trace)
            
#             self.write({
#                 'state': 'failed',
#                 'end_date': fields.Datetime.now(),
#                 'duration': duration,
#                 'result': f"Error: {str(e)}"
#             })
            
#             # Reset debug logging if it was enabled
#             if self.debug_mode:
#                 logging.getLogger('odoo.addons.compliance_management').setLevel(logging.INFO)
            
#             return {
#                 'status': 'error',
#                 'message': str(e)
#             }
            
#     def _cleanup_chunk_jobs(self):
#         """Clean up completed chunk jobs to prevent database bloat"""
#         self.ensure_one()
        
#         if not self.is_chunk and self.total_chunks > 0:
#             chunk_jobs = self.env['opensanctions.job.queue'].search([
#                 ('parent_job_id', '=', self.id),
#                 ('is_chunk', '=', True),
#                 ('state', 'in', ['done', 'failed', 'cancelled'])
#             ])
            
#             if chunk_jobs:
#                 chunk_count = len(chunk_jobs)
#                 self._append_log(f"Cleaning up {chunk_count} completed chunk jobs")
#                 chunk_jobs.unlink()
#                 return chunk_count
            
#         return 0

#     def _process_csv_job(self, service, importer, source):
#         """Process CSV data fetching and importing with chunking"""
#         self._append_log("Fetching CSV data...")
        
#         # Use dynamic discovery for OpenSanctions sources
#         if hasattr(source, 'is_opensanctions') and source.is_opensanctions:
#             self._append_log("Using dynamic URL discovery for OpenSanctions...")
#             csv_result = service.fetch_latest_opensanctions_csv(source)
#         else:
#             # Standard fetch for other sources
#             csv_result = service.fetch_csv_file(source)
        
#         if csv_result.get('status') == 'success':
#             file_path = csv_result.get('path')
#             self._append_log(f"CSV file downloaded to: {file_path}")
            
#             # Store file path in job record
#             self.write({'file_path': file_path})
            
#             # Verify file exists and is readable
#             if not os.path.exists(file_path):
#                 self._append_log(f"Error: Downloaded file does not exist at path: {file_path}")
#                 return
                
#             if not os.access(file_path, os.R_OK):
#                 self._append_log(f"Error: Downloaded file is not readable: {file_path}")
#                 return
                
#             # Check file size
#             file_size = os.path.getsize(file_path)
#             self._append_log(f"File size: {file_size / (1024*1024):.2f} MB")
                
#             if file_size == 0:
#                 self._append_log("Error: Downloaded file is empty")
#                 return
            
#             # Get the maximum records per chunk from parameters
#             params = self.env['ir.config_parameter'].sudo()
#             max_records_per_chunk = int(params.get_param('compliance_management.max_records_per_job', '5000'))
            
#             # Count total records to determine chunking
#             total_records = importer._count_csv_lines(file_path)
#             self._append_log(f"Total records in file: {total_records}")
            
#             # For small files, process directly
#             if total_records <= max_records_per_chunk:
#                 self._append_log("File is small enough to process without chunking")
#                 self._process_csv_directly(importer, source, file_path)
#             else:
#                 self._append_log("File is large, creating chunks for processing")
#                 self._create_csv_chunks(file_path, source, total_records, max_records_per_chunk)
#         else:
#             self._append_log(f"CSV fetch failed: {csv_result.get('message')}")
    
#     def _process_csv_directly(self, importer, source, file_path):
#         """Process a CSV file directly without chunking"""
#         self._append_log("Processing CSV data...")
        
#         # Process CSV file
#         csv_process_result = importer.process_csv_file(file_path, source)
        
#         if csv_process_result.get('status') == 'success':
#             self._append_log(f"CSV processing completed: {csv_process_result.get('records_created', 0)} created, "
#                         f"{csv_process_result.get('records_updated', 0)} updated")
            
#             # Update job counters
#             self.write({
#                 'records_created': self.records_created + csv_process_result.get('records_created', 0),
#                 'records_updated': self.records_updated + csv_process_result.get('records_updated', 0),
#                 'records_failed': self.records_failed + csv_process_result.get('records_errored', 0)
#             })
#         else:
#             self._append_log(f"CSV processing failed: {csv_process_result.get('message')}")
    
#     def _create_csv_chunks(self, file_path, source, total_records, chunk_size):
#         """Create chunk jobs for processing a large CSV file"""
#         # Calculate number of chunks needed
#         total_chunks = (total_records + chunk_size - 1) // chunk_size  # Ceiling division
        
#         self._append_log(f"Creating {total_chunks} chunk jobs with size {chunk_size}")
        
#         # Update parent job with chunk information
#         self.write({
#             'total_chunks': total_chunks,
#             'chunks_completed': 0,
#             'total_records': total_records
#         })
        
#         # Check for existing chunk jobs and delete them
#         existing_chunks = self.env['opensanctions.job.queue'].search([
#             ('parent_job_id', '=', self.id),
#             ('is_chunk', '=', True)
#         ])
        
#         if existing_chunks:
#             self._append_log(f"Cleaning up {len(existing_chunks)} existing chunk jobs")
#             existing_chunks.unlink()
        
#         # Create a job for each chunk
#         chunk_jobs = []
#         for i in range(total_chunks):
#             start_index = i * chunk_size
            
#             # Create chunk job
#             chunk_job = self.create({
#                 'name': f"Chunk {i+1}/{total_chunks} - {source.name}",
#                 'source_id': source.id,
#                 'job_type': 'csv',
#                 'priority': self.priority + 5,  # Higher priority than parent to ensure they run next
#                 'state': 'pending',
#                 'is_chunk': True,
#                 'parent_job_id': self.id,
#                 'chunk_index': start_index,
#                 'chunk_size': chunk_size,
#                 'total_chunks': total_chunks,
#                 'file_path': file_path,
#                 'batch_size': self.batch_size,
#                 'debug_mode': self.debug_mode,
#                 'total_records': total_records
#             })
            
#             chunk_jobs.append(chunk_job)
            
#         self._append_log(f"Created {len(chunk_jobs)} chunk jobs")
        
#         # For safety, sort the chunks by index before starting
#         sorted_chunks = sorted(chunk_jobs, key=lambda job: job.chunk_index)
        
#         # Queue or start the first chunk
#         if sorted_chunks:
#             # Check if queue_job is installed
#             queue_job = self.env['ir.module.module'].sudo().search(
#                 [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
                
#             if queue_job:
#                 # Queue the first chunk only - others will be queued by the completion handler
#                 first_chunk = sorted_chunks[0]
#                 identity_key = f"opensanctions_chunk_{first_chunk.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
#                 # Even higher priority for first chunk to ensure it runs immediately
#                 first_chunk.write({'priority': self.priority + 10})
#                 first_chunk.with_delay(priority=first_chunk.priority, identity_key=identity_key).process_job()
#                 self._append_log(f"Queued first chunk job with index {first_chunk.chunk_index} and priority {first_chunk.priority}")
#             else:
#                 # Start the first chunk directly
#                 sorted_chunks[0].process_job()
#                 self._append_log(f"Started first chunk job with index {sorted_chunks[0].chunk_index} directly")
    
#     def _process_csv_chunk(self, service, importer, source):
#         """Process a single chunk of a CSV file"""
#         file_path = self.file_path
        
#         if not file_path or not os.path.exists(file_path):
#             self._append_log(f"Error: File not found at path: {file_path}")
#             return
            
#         self._append_log(f"Processing chunk starting at index {self.chunk_index}")
        
#         # Process chunk
#         csv_process_result = importer.process_csv_file(
#             file_path, 
#             source, 
#             start_index=self.chunk_index, 
#             max_records=self.chunk_size
#         )
        
#         if csv_process_result.get('status') == 'success':
#             self._append_log(f"Chunk processing completed: {csv_process_result.get('records_created', 0)} created, "
#                         f"{csv_process_result.get('records_updated', 0)} updated")
            
#             # Update job counters
#             self.write({
#                 'records_created': self.records_created + csv_process_result.get('records_created', 0),
#                 'records_updated': self.records_updated + csv_process_result.get('records_updated', 0),
#                 'records_failed': self.records_failed + csv_process_result.get('records_errored', 0)
#             })
            
#             # Queue the next chunk
#             self._queue_next_chunk()
#         else:
#             self._append_log(f"Chunk processing failed: {csv_process_result.get('message')}")
#             # Even if this chunk failed, still try to continue with next chunks
#             self._queue_next_chunk(even_if_failed=True)
            
#     def _queue_next_chunk(self, even_if_failed=False):
#         """Queue the next chunk job in sequence"""
#         # Only if this is a chunk job
#         if not self.is_chunk or not self.parent_job_id:
#             return False
            
#         # Force update parent job status to ensure it's seen as running
#         if self.parent_job_id.state != 'running':
#             self.parent_job_id.write({'state': 'running'})
            
#         # Find the next chunk by index
#         next_chunk = self.env['opensanctions.job.queue'].search([
#             ('parent_job_id', '=', self.parent_job_id.id),
#             ('is_chunk', '=', True),
#             ('state', '=', 'pending'),
#             ('chunk_index', '>', self.chunk_index)  # Force the next chunk by index
#         ], order='chunk_index', limit=1)
        
#         if not next_chunk:
#             # Double check with a broader search in case the ordering got mixed up
#             next_chunk = self.env['opensanctions.job.queue'].search([
#                 ('parent_job_id', '=', self.parent_job_id.id),
#                 ('is_chunk', '=', True),
#                 ('state', '=', 'pending')
#             ], order='chunk_index', limit=1)
            
#         if next_chunk:
#             # Check if queue_job is installed
#             queue_job = self.env['ir.module.module'].sudo().search(
#                 [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
                
#             if queue_job:
#                 # Queue the next chunk with higher priority to ensure it runs next
#                 priority = self.priority + 1  # Higher priority than the parent job
#                 identity_key = f"opensanctions_chunk_{next_chunk.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
#                 next_chunk.write({'priority': priority})  # Update priority to be higher
#                 next_chunk.with_delay(priority=priority, identity_key=identity_key).process_job()
#                 self._append_log(f"Queued next chunk job at index {next_chunk.chunk_index} with priority {priority}")
#                 return True
#             else:
#                 # Start the next chunk directly
#                 next_chunk.process_job()
#                 self._append_log(f"Started next chunk job at index {next_chunk.chunk_index}")
#                 return True
#         else:
#             self._append_log("No more pending chunks found. This might be the last chunk.")
#             # Notify parent that we're done with all chunks
#             if self.parent_job_id.chunks_completed >= self.parent_job_id.total_chunks - 1:
#                 self._append_log("All chunks appear to be completed. Finalizing parent job.")
#                 self.parent_job_id.write({
#                     'state': 'done',
#                     'end_date': fields.Datetime.now(),
#                     'result': f"All chunks completed: {self.parent_job_id.records_created + self.records_created} records created, {self.parent_job_id.records_updated + self.records_updated} updated"
#                 })
#             return False
    
#     def _process_api_job(self, service, importer, source):
#         """Process API data fetching and importing"""
#         if not hasattr(source, 'use_api') or not source.use_api:
#             self._append_log("API not enabled for this source, skipping")
#             return
            
#         if not hasattr(source, 'api_key') or not source.api_key:
#             self._append_log("API key not configured for this source, skipping")
#             return
            
#         self._append_log("Querying API...")
#         entity_type = source.default_entity_type if hasattr(source, 'default_entity_type') else 'person'
        
#         api_result = service.query_api(
#             entity_type=entity_type,
#             limit=self.api_limit,
#             source_record=source
#         )
        
#         if api_result.get('status') == 'success':
#             self._append_log("API data received, processing...")
            
#             # Process API results
#             api_process_result = importer.process_api_results(api_result, source)
            
#             if api_process_result.get('status') == 'success':
#                 self._append_log(f"API processing completed: {api_process_result.get('records_created', 0)} created, "
#                                f"{api_process_result.get('records_updated', 0)} updated")
                
#                 # Update job counters
#                 self.write({
#                     'records_created': self.records_created + api_process_result.get('records_created', 0),
#                     'records_updated': self.records_updated + api_process_result.get('records_updated', 0),
#                     'records_failed': self.records_failed + api_process_result.get('records_errored', 0)
#                 })
#             else:
#                 self._append_log(f"API processing failed: {api_process_result.get('message')}")
#         else:
#             self._append_log(f"API fetch failed: {api_result.get('message')}")
    
#     def _append_log(self, message):
#         """
#         Append a message to the job log
        
#         Args:
#             message: Message to append
#         """
#         timestamp = fields.Datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#         current_log = self.log or ""
#         self.write({
#             'log': current_log + f"\n[{timestamp}] {message}"
#         })
        
#     @api.model
#     def _process_scheduled_jobs(self):
#         """
#         Process any scheduled jobs that are due to run
#         This method is meant to be called by a cron job
#         """
#         now = fields.Datetime.now()
#         jobs_to_run = self.search([
#             ('state', '=', 'pending'),
#             ('next_run', '<=', now),
#             ('is_chunk', '=', False)  # Don't schedule chunks directly
#         ])
        
#         _logger.info(f"Running {len(jobs_to_run)} scheduled OpenSanctions jobs")
        
#         for job in jobs_to_run:
#             try:
#                 # Check if queue_job is installed
#                 queue_job = self.env['ir.module.module'].sudo().search(
#                     [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
                    
#                 if queue_job:
#                     # Use unique identity key to prevent duplicate jobs
#                     identity_key = f"opensanctions_scheduled_job_{job.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
#                     job.with_delay(priority=job.priority, identity_key=identity_key).process_job()
#                     _logger.info(f"Queued scheduled job: {job.name}")
#                 else:
#                     job.process_job()
#                     _logger.info(f"Ran scheduled job directly: {job.name}")
#             except Exception as e:
#                 _logger.error(f"Error scheduling job {job.name}: {str(e)}")
                
#     @api.model
#     def clean_old_jobs(self, days=30):
#         """
#         Clean up old completed jobs to prevent database bloat
        
#         Args:
#             days: Number of days to keep jobs (default: 30)
#         """
#         cutoff_date = fields.Datetime.now() - timedelta(days=days)
#         old_jobs = self.search([
#             ('state', 'in', ['done', 'cancelled']),
#             ('end_date', '<', cutoff_date),
#             ('is_chunk', '=', False)  # Only clean up parent jobs, chunks will be cleaned separately
#         ])
        
#         # For each parent job, also clean up its chunks
#         for job in old_jobs:
#             chunk_jobs = self.search([
#                 ('parent_job_id', '=', job.id),
#                 ('is_chunk', '=', True)
#             ])
            
#             if chunk_jobs:
#                 _logger.info(f"Cleaning up {len(chunk_jobs)} chunk jobs for job {job.name}")
#                 chunk_jobs.unlink()
        
#         if old_jobs:
#             _logger.info(f"Cleaning up {len(old_jobs)} old jobs")
#             old_jobs.unlink()
            
# class OpenSanctionsJobConfirm(models.TransientModel):
#     """Confirmation wizard for OpenSanctions jobs"""
#     _name = 'opensanctions.job.confirm'
#     _description = 'OpenSanctions Job Confirmation'
    
#     job_id = fields.Many2one('opensanctions.job.queue', string='Job', required=True)
#     action = fields.Selection([
#         ('rerun', 'Rerun Job'),
#         ('cancel', 'Cancel Job'),
#     ], string='Action', required=True)
    
#     def confirm_action(self):
#         """Confirm the selected action"""
#         self.ensure_one()
        
#         if self.action == 'rerun':
#             # Reset the job first
#             self.job_id.action_reset_job()
#             # Then run it
#             return self.job_id.action_run_job()
#         elif self.action == 'cancel':
#             return self.job_id.action_cancel_job()








import logging
import time
import os
import traceback
from datetime import datetime, timedelta
from odoo import api, fields, models, tools, SUPERUSER_ID, _

_logger = logging.getLogger(__name__)

class OpenSanctionsJobQueue(models.Model):
    """
    Model to manage OpenSanctions data import jobs with chunking and auto-population
    """
    _name = 'opensanctions.job.queue'
    _description = 'OpenSanctions Job Queue'
    _order = 'priority desc, create_date desc'
    
    name = fields.Char('Job Name', required=True)
    source_id = fields.Many2one('pep.source', string='Data Source', required=True, ondelete='cascade')
    job_type = fields.Selection([
        ('csv', 'CSV Import'),
        ('api', 'API Import'),
        # ('both', 'CSV and API Import')
    ], string='Job Type', required=True, default='both')
    state = fields.Selection([
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('done', 'Completed'),
        ('failed', 'Failed'),
        ('cancelled', 'Cancelled')
    ], string='Status', default='pending', required=True)
    priority = fields.Integer('Priority', default=10, 
                             help="Higher number means higher priority")
    user_id = fields.Many2one('res.users', string='Created By', default=lambda self: self.env.user)
    create_date = fields.Datetime('Created On', readonly=True)
    start_date = fields.Datetime('Started On', readonly=True)
    end_date = fields.Datetime('Completed On', readonly=True)
    duration = fields.Float('Duration (seconds)', readonly=True)
    result = fields.Text('Job Result', readonly=True)
    log = fields.Text('Job Log', readonly=True)
    records_created = fields.Integer('Records Created', readonly=True, default=0)
    records_updated = fields.Integer('Records Updated', readonly=True, default=0)
    records_failed = fields.Integer('Records Failed', readonly=True, default=0)
    next_run = fields.Datetime('Next Scheduled Run', 
                              help="Schedule this job to run at a specific time")
    run_as_user_id = fields.Many2one('res.users', string='Run As User', 
                                    default=lambda self: self.env.user,
                                    help="The user whose permissions will be used to run this job")
    api_limit = fields.Integer('API Result Limit', default=500,
                              help="Maximum number of results to fetch from API")
    file_path = fields.Char('CSV File Path', readonly=True,
                           help="Path to the downloaded CSV file")
    batch_size = fields.Integer('Batch Size', default=500,
                              help="Number of records to process in each batch")
    debug_mode = fields.Boolean('Debug Mode', default=False,
                               help="Enable verbose logging for debugging")

    base_url = fields.Char(related='source_id.base_url', string='Base URL', readonly=True)
    csv_path = fields.Char(related='source_id.csv_path', string='CSV Path', readonly=True)
    csv_delimiter = fields.Char(related='source_id.csv_delimiter', string='CSV Delimiter', readonly=True)
    use_api = fields.Boolean(related='source_id.use_api', string='Use API', readonly=True)
    api_url = fields.Char(related='source_id.api_url', string='API URL', readonly=True)
    api_endpoint = fields.Char(related='source_id.api_endpoint', string='API Endpoint', readonly=True)
    is_opensanctions = fields.Boolean(related='source_id.is_opensanctions', string='Is OpenSanctions', readonly=True)
    
    # Chunking fields
    is_chunk = fields.Boolean('Is Chunk Job', default=False, 
                             help="Indicates this job is a chunk of a larger job")
    parent_job_id = fields.Many2one('opensanctions.job.queue', string='Parent Job',
                                   help="Parent job that created this chunk")
    chunk_index = fields.Integer('Chunk Index', default=0,
                               help="Starting index for this chunk")
    chunk_size = fields.Integer('Chunk Size', default=5000,
                              help="Maximum number of records to process in this chunk")
    total_chunks = fields.Integer('Total Chunks', default=0,
                                help="Total number of chunks for the parent job")
    chunks_completed = fields.Integer('Chunks Completed', default=0,
                                    help="Number of chunks completed for the parent job")
    total_records = fields.Integer('Total Records', default=0,
                                 help="Total number of records in the source file")
    
    @api.onchange('source_id')
    def _onchange_source_id(self):
        """
        Auto-populate fields when source is selected
        """
        if self.source_id:
            # Set job name based on source
            self.name = f"Import - {self.source_id.name}" 
            
            # Set job type based on source configuration
            # if self.source_id.source_format == 'both':
            #     self.job_type = 'both'
            if self.source_id.source_format == 'csv':
                self.job_type = 'csv'
            elif self.source_id.source_format == 'api' and self.source_id.use_api and self.source_id.api_key:
                self.job_type = 'api'
            else:
                self.job_type = 'csv'  # Default to CSV if nothing else is configured
                
            # Set priority higher for OpenSanctions
            if self.source_id.is_opensanctions:
                self.priority = 15
            else:
                self.priority = 10
                
            # Set limits based on params
            params = self.env['ir.config_parameter'].sudo()
            self.batch_size = int(params.get_param('compliance_management.import_batch_size', '500'))
            
            if self.source_id.use_api and self.job_type in ['api', 'both']:
                self.api_limit = 1000  # Default API limit

    def name_get(self):
        result = []
        for job in self:
            source_name = job.source_id.name or 'Unknown Source'
            state_label = dict(self._fields['state'].selection).get(job.state)
            
            # For chunk jobs, show chunk information
            if job.is_chunk:
                result.append((job.id, f"{job.name} - Chunk {job.chunk_index}/{job.total_records} ({state_label})"))
            else:
                result.append((job.id, f"{job.name} ({source_name}) - {state_label}"))
                
        return result

    @api.model
    def create(self, vals):
        """Override create to set a default name if not provided"""
        if not vals.get('name') and vals.get('source_id'):
            source = self.env['pep.source'].browse(vals.get('source_id'))
            job_type = vals.get('job_type', 'both')
            
            # For chunk jobs, include chunk information in name
            if vals.get('is_chunk', False):
                start_index = vals.get('chunk_index', 0)
                chunk_size = vals.get('chunk_size', 5000)
                end_index = start_index + chunk_size
                vals['name'] = f"Chunk {start_index}-{end_index} - {source.name} ({job_type})"
            else:
                vals['name'] = f"Import - {source.name} ({job_type})"
                
        return super(OpenSanctionsJobQueue, self).create(vals)
    
    def action_run_job(self):
        """Manually trigger job execution"""
        self.ensure_one()
        
        if self.state in ['running']:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("Cannot Run Job"),
                    'message': _("This job is already running."),
                    'sticky': False,
                    'type': 'warning',
                }
            }
            
        # Allow rerunning completed jobs
        if self.state in ['done']:
            # Ask for confirmation
            return {
                'type': 'ir.actions.act_window',
                'name': _('Rerun Completed Job?'),
                'res_model': 'opensanctions.job.confirm',
                'view_mode': 'form',
                'target': 'new',
                'context': {'default_job_id': self.id, 'default_action': 'rerun'},
            }
            
        # Check if queue_job is installed
        queue_job = self.env['ir.module.module'].sudo().search(
            [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
            
        if queue_job:
            # Use unique identity key to prevent duplicate jobs
            identity_key = f"opensanctions_job_{self.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            self.with_delay(priority=self.priority, identity_key=identity_key).process_job()
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("Job Queued"),
                    'message': _("The job has been queued for processing."),
                    'sticky': False,
                    'type': 'success',
                }
            }
        else:
            # Direct execution for non-queue environment
            self.process_job()
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("Job Started"),
                    'message': _("The job has been started."),
                    'sticky': False,
                    'type': 'success',
                }
            }
    
    def action_cancel_job(self):
        """Cancel a pending job"""
        for job in self:
            if job.state == 'pending':
                job.write({
                    'state': 'cancelled',
                    'end_date': fields.Datetime.now(),
                    'result': 'Job was cancelled by user'
                })
                
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': _("Job Cancelled"),
                'message': _("The selected jobs have been cancelled."),
                'sticky': False,
                'type': 'info',
            }
        }
    
    def action_reset_job(self):
        """Reset a failed or cancelled job to pending"""
        for job in self:
            if job.state in ['failed', 'cancelled', 'done']:
                job.write({
                    'state': 'pending',
                    'start_date': False,
                    'end_date': False,
                    'duration': 0,
                    'result': False,
                    'log': job.log + '\n\n' + f"--- Job reset on {fields.Datetime.now()} ---" if job.log else ''
                })
                
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': _("Job Reset"),
                'message': _("The selected jobs have been reset to pending status."),
                'sticky': False,
                'type': 'info',
            }
        }
        
    def process_job(self):
        """
        Process a job based on its type
        This is the main job execution method
        """
        self.ensure_one()
        
        if self.state == 'running':
            _logger.warning(f"Job {self.name} (ID: {self.id}) is already running")
            return False
            
        # Mark job as running
        start_time = time.time()
        self.write({
            'state': 'running',
            'start_date': fields.Datetime.now(),
            'log': f"Job started at {fields.Datetime.now()}\n"
        })
        
        try:
            # Run job as specified user if different from current user
            if self.run_as_user_id and self.run_as_user_id.id != self.env.uid:
                # Switch to a new environment with the specified user
                env = api.Environment(self.env.cr, self.run_as_user_id.id, self.env.context.copy())
                job = env['opensanctions.job.queue'].browse(self.id)
                source = env['pep.source'].browse(self.source_id.id)
            else:
                job = self
                source = self.source_id
                
            # Enable debug logging if debug mode is on
            if self.debug_mode:
                self._append_log("Debug mode enabled - verbose logging will be used")
                logging.getLogger('odoo.addons.compliance_management').setLevel(logging.DEBUG)
                
            # Initialize services
            from ..services.open_sanctions import OpenSanctions
            from ..services.open_sanctions_importer import OpenSanctionsImporter
            
            service = OpenSanctions(self.env)
            importer = OpenSanctionsImporter(self.env)
            
            # Set batch size for importer
            importer.batch_size = self.batch_size or 500
            
            # # Ensure the storage directory exists
            # storage_dir = self.env['ir.config_parameter'].sudo().get_param(
            #     'compliance_management.pep_storage_dir', '/media/pep_list_data')
                
            # if not os.path.exists(storage_dir):
            #     try:
            #         os.makedirs(storage_dir, exist_ok=True)
            #         self._append_log(f"Created storage directory: {storage_dir}")
            #     except Exception as e:
            #         self._append_log(f"Error creating storage directory: {str(e)}")

            # Ensure the storage directory exists
            storage_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "media", "pep_list_data"
            )
            self._append_log(f"Using storage directory: {storage_dir}")

            # Create the directory with proper error handling and fallback
            try:
                os.makedirs(storage_dir, exist_ok=True)
            except Exception as e:
                self._append_log(f"Failed to create storage directory {storage_dir}: {str(e)}")
                # Fallback to temporary directory
                import tempfile
                storage_dir = tempfile.mkdtemp(prefix="pep_list_")
                self._append_log(f"Using temporary directory as fallback: {storage_dir}")
            
            # Process based on job type
            if self.is_chunk:
                # This is a chunk job - process only the specified chunk
                self._process_csv_chunk(service, importer, source)
            elif self.job_type == 'csv':
                # CSV-only job
                self._process_csv_job(service, importer, source)
            elif self.job_type == 'api':
                # API-only job
                self._process_api_job(service, importer, source)
            # elif self.job_type == 'both':
            #     # Process both CSV and API
            #     self._process_csv_job(service, importer, source)
            #     self._process_api_job(service, importer, source)
                
            # Calculate duration
            end_time = time.time()
            duration = end_time - start_time
            
            # Determine overall job status
            if self.is_chunk:
                if self.parent_job_id:
                    # Update parent job
                    parent_job = self.parent_job_id
                    parent_job.chunks_completed += 1
                    
                    # Check if this is the last chunk
                    if parent_job.chunks_completed >= parent_job.total_chunks:
                        parent_job.write({
                            'state': 'done',
                            'end_date': fields.Datetime.now(),
                            'result': f"All chunks completed: {parent_job.records_created} records created, {parent_job.records_updated} updated",
                            'records_created': parent_job.records_created + self.records_created,
                            'records_updated': parent_job.records_updated + self.records_updated,
                            'records_failed': parent_job.records_failed + self.records_failed
                        })
                    else:
                        # Update parent job stats but keep it running
                        parent_job.write({
                            'records_created': parent_job.records_created + self.records_created,
                            'records_updated': parent_job.records_updated + self.records_updated,
                            'records_failed': parent_job.records_failed + self.records_failed
                        })
                    
                status = 'done'
                message = f"Chunk job completed: {self.records_created} records created, {self.records_updated} updated in {duration:.2f} seconds"
            else:
                if self.records_created > 0 or self.records_updated > 0:
                    status = 'done'
                    message = f"Job completed successfully: {self.records_created} records created, {self.records_updated} updated in {duration:.2f} seconds"
                else:
                    status = 'done'
                    message = f"Job completed with no changes: {duration:.2f} seconds"
            
            # Update last_update on source
            source.write({
                'last_update': fields.Datetime.now()
            })
            
            # Complete job
            self._append_log(f"Job completed with status: {status}")
            self._append_log(message)
            
            self.write({
                'state': status,
                'end_date': fields.Datetime.now(),
                'duration': duration,
                'result': message
            })
            
            # Reset debug logging if it was enabled
            if self.debug_mode:
                logging.getLogger('odoo.addons.compliance_management').setLevel(logging.INFO)
            
            return {
                'status': status,
                'message': message,
                'records_created': self.records_created,
                'records_updated': self.records_updated
            }
            
        except Exception as e:
            error_trace = traceback.format_exc()
            _logger.error(f"Error in job: {str(e)}\n{error_trace}")
            
            # Calculate duration
            end_time = time.time()
            duration = end_time - start_time
            
            # Complete job with error
            self._append_log(f"Job failed with error: {str(e)}")
            self._append_log(error_trace)
            
            self.write({
                'state': 'failed',
                'end_date': fields.Datetime.now(),
                'duration': duration,
                'result': f"Error: {str(e)}"
            })
            
            # Reset debug logging if it was enabled
            if self.debug_mode:
                logging.getLogger('odoo.addons.compliance_management').setLevel(logging.INFO)
            
            return {
                'status': 'error',
                'message': str(e)
            }

    def _process_csv_job(self, service, importer, source):
        """Process CSV data fetching and importing with chunking"""
        self._append_log("Fetching CSV data...")
        
        # Use dynamic discovery for OpenSanctions sources
        if hasattr(source, 'is_opensanctions') and source.is_opensanctions:
            self._append_log("Using dynamic URL discovery for OpenSanctions...")
            csv_result = service.fetch_latest_opensanctions_csv(source)
        else:
            # Standard fetch for other sources
            csv_result = service.fetch_csv_file(source)
        
        if csv_result.get('status') == 'success':
            file_path = csv_result.get('path')
            self._append_log(f"CSV file downloaded to: {file_path}")
            
            # Store file path in job record
            self.write({'file_path': file_path})
            
            # Verify file exists and is readable
            if not os.path.exists(file_path):
                self._append_log(f"Error: Downloaded file does not exist at path: {file_path}")
                return
                
            if not os.access(file_path, os.R_OK):
                self._append_log(f"Error: Downloaded file is not readable: {file_path}")
                return
                
            # Check file size
            file_size = os.path.getsize(file_path)
            self._append_log(f"File size: {file_size / (1024*1024):.2f} MB")
                
            if file_size == 0:
                self._append_log("Error: Downloaded file is empty")
                return
            
            # Get the maximum records per chunk from parameters
            params = self.env['ir.config_parameter'].sudo()
            max_records_per_chunk = int(params.get_param('compliance_management.max_records_per_job', '5000'))
            
            # Count total records to determine chunking
            total_records = importer._count_csv_lines(file_path)
            self._append_log(f"Total records in file: {total_records}")
            
            # For small files, process directly
            if total_records <= max_records_per_chunk:
                self._append_log("File is small enough to process without chunking")
                self._process_csv_directly(importer, source, file_path)
            else:
                self._append_log("File is large, creating chunks for processing")
                self._create_csv_chunks(file_path, source, total_records, max_records_per_chunk)
        else:
            self._append_log(f"CSV fetch failed: {csv_result.get('message')}")
    
    def _process_csv_directly(self, importer, source, file_path):
        """Process a CSV file directly without chunking"""
        self._append_log("Processing CSV data...")
        
        # Process CSV file
        csv_process_result = importer.process_csv_file(file_path, source)
        
        if csv_process_result.get('status') == 'success':
            self._append_log(f"CSV processing completed: {csv_process_result.get('records_created', 0)} created, "
                        f"{csv_process_result.get('records_updated', 0)} updated")
            
            # Update job counters
            self.write({
                'records_created': self.records_created + csv_process_result.get('records_created', 0),
                'records_updated': self.records_updated + csv_process_result.get('records_updated', 0),
                'records_failed': self.records_failed + csv_process_result.get('records_errored', 0)
            })
        else:
            self._append_log(f"CSV processing failed: {csv_process_result.get('message')}")
    
    def _create_csv_chunks(self, file_path, source, total_records, chunk_size):
        """Create chunk jobs for processing a large CSV file"""
        # Calculate number of chunks needed
        total_chunks = (total_records + chunk_size - 1) // chunk_size  # Ceiling division
        
        self._append_log(f"Creating {total_chunks} chunk jobs with size {chunk_size}")
        
        # Update parent job with chunk information
        self.write({
            'total_chunks': total_chunks,
            'chunks_completed': 0,
            'total_records': total_records
        })
        
        # Create a job for each chunk
        chunk_jobs = []
        for i in range(total_chunks):
            start_index = i * chunk_size
            
            # Create chunk job
            chunk_job = self.create({
                'name': f"Chunk {i+1}/{total_chunks} - {source.name}",
                'source_id': source.id,
                'job_type': 'csv',
                'priority': self.priority,
                'state': 'pending',
                'is_chunk': True,
                'parent_job_id': self.id,
                'chunk_index': start_index,
                'chunk_size': chunk_size,
                'total_chunks': total_chunks,
                'file_path': file_path,
                'batch_size': self.batch_size,
                'debug_mode': self.debug_mode,
                'total_records': total_records
            })
            
            chunk_jobs.append(chunk_job)
            
        self._append_log(f"Created {len(chunk_jobs)} chunk jobs")
        
        # Queue or start the first chunk
        if chunk_jobs:
            # Check if queue_job is installed
            queue_job = self.env['ir.module.module'].sudo().search(
                [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
                
            if queue_job:
                # Queue all chunks
                for job in chunk_jobs:
                    identity_key = f"opensanctions_chunk_{job.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    job.with_delay(priority=job.priority, identity_key=identity_key).process_job()
                    
                self._append_log(f"Queued {len(chunk_jobs)} chunk jobs")
            else:
                # Start the first chunk directly
                chunk_jobs[0].process_job()
                self._append_log(f"Started first chunk job directly")
    
    def _process_csv_chunk(self, service, importer, source):
        """Process a single chunk of a CSV file"""
        file_path = self.file_path
        
        if not file_path or not os.path.exists(file_path):
            self._append_log(f"Error: File not found at path: {file_path}")
            return
            
        self._append_log(f"Processing chunk starting at index {self.chunk_index}")
        
        # Process chunk
        csv_process_result = importer.process_csv_file(
            file_path, 
            source, 
            start_index=self.chunk_index, 
            max_records=self.chunk_size
        )
        
        if csv_process_result.get('status') == 'success':
            self._append_log(f"Chunk processing completed: {csv_process_result.get('records_created', 0)} created, "
                        f"{csv_process_result.get('records_updated', 0)} updated")
            
            # Update job counters
            self.write({
                'records_created': self.records_created + csv_process_result.get('records_created', 0),
                'records_updated': self.records_updated + csv_process_result.get('records_updated', 0),
                'records_failed': self.records_failed + csv_process_result.get('records_errored', 0)
            })
            
            # If this is part of a parent job, queue the next chunk if needed
            if self.parent_job_id and csv_process_result.get('more_records', False):
                next_index = csv_process_result.get('next_index', 0)
                
                # Find the next chunk job
                next_chunk = self.search([
                    ('parent_job_id', '=', self.parent_job_id.id),
                    ('chunk_index', '=', next_index),
                    ('state', '=', 'pending')
                ], limit=1)
                
                if next_chunk:
                    # Check if queue_job is installed
                    queue_job = self.env['ir.module.module'].sudo().search(
                        [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
                        
                    if queue_job:
                        # Queue the next chunk
                        identity_key = f"opensanctions_chunk_{next_chunk.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                        next_chunk.with_delay(priority=next_chunk.priority, identity_key=identity_key).process_job()
                        self._append_log(f"Queued next chunk job at index {next_index}")
                    else:
                        # Start the next chunk directly
                        next_chunk.process_job()
                        self._append_log(f"Started next chunk job at index {next_index}")
        else:
            self._append_log(f"Chunk processing failed: {csv_process_result.get('message')}")
    
    def _process_api_job(self, service, importer, source):
        """Process API data fetching and importing"""
        if not hasattr(source, 'use_api') or not source.use_api:
            self._append_log("API not enabled for this source, skipping")
            return
            
        # if not hasattr(source, 'api_key') or not source.api_key:
        #     self._append_log("API key not configured for this source, skipping")
        #     return

        # This will check both the source record and the system parameter
        api_key = service.get_api_key(source)
        
        if not api_key:
            self._append_log("API key not configured, skipping")
            return
            
        self._append_log("Querying API...")
        entity_type = None
        if hasattr(source, 'default_entity_type') and source.default_entity_type:
            # Capitalize the first letter as the API might expect proper casing
            entity_type = source.default_entity_type.capitalize()
        else:
            entity_type = "Person"  # Default with proper capitalization
        # entity_type = source.default_entity_type if hasattr(source, 'default_entity_type') else 'person'
        
        api_result = service.query_api(
            entity_type=entity_type,
            limit=self.api_limit,
            source_record=source
        )
        
        if api_result.get('status') == 'success':
            self._append_log("API data received, processing...")
            
            # Process API results
            api_process_result = importer.process_api_results(api_result, source)
            
            if api_process_result.get('status') == 'success':
                self._append_log(f"API processing completed: {api_process_result.get('records_created', 0)} created, "
                               f"{api_process_result.get('records_updated', 0)} updated")
                
                # Update job counters
                self.write({
                    'records_created': self.records_created + api_process_result.get('records_created', 0),
                    'records_updated': self.records_updated + api_process_result.get('records_updated', 0),
                    'records_failed': self.records_failed + api_process_result.get('records_errored', 0)
                })
            else:
                self._append_log(f"API processing failed: {api_process_result.get('message')}")
        else:
            self._append_log(f"API fetch failed: {api_result.get('message')}")
    
    def _append_log(self, message):
        """
        Append a message to the job log
        
        Args:
            message: Message to append
        """
        timestamp = fields.Datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_log = self.log or ""
        self.write({
            'log': current_log + f"\n[{timestamp}] {message}"
        })
        
    # @api.model
    # def _process_scheduled_jobs(self):
    #     """
    #     Process any scheduled jobs that are due to run
    #     This method is meant to be called by a cron job
    #     """
    #     now = fields.Datetime.now()
    #     jobs_to_run = self.search([
    #         ('state', '=', 'pending'),
    #         ('next_run', '<=', now),
    #         ('is_chunk', '=', False)  # Don't schedule chunks directly
    #     ])
        
    #     _logger.info(f"Running {len(jobs_to_run)} scheduled OpenSanctions jobs")
        
    #     for job in jobs_to_run:
    #         try:
    #             # Check if queue_job is installed
    #             queue_job = self.env['ir.module.module'].sudo().search(
    #                 [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
                    
    #             if queue_job:
    #                 # Use unique identity key to prevent duplicate jobs
    #                 identity_key = f"opensanctions_scheduled_job_{job.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    #                 job.with_delay(priority=job.priority, identity_key=identity_key).process_job()
    #                 _logger.info(f"Queued scheduled job: {job.name}")
    #             else:
    #                 job.process_job()
    #                 _logger.info(f"Ran scheduled job directly: {job.name}")
    #         except Exception as e:
    #             _logger.error(f"Error scheduling job {job.name}: {str(e)}")
                
    @api.model
    def clean_old_jobs(self, days=30):
        """
        Clean up old completed jobs to prevent database bloat
        
        Args:
            days: Number of days to keep jobs (default: 30)
        """
        cutoff_date = fields.Datetime.now() - timedelta(days=days)
        old_jobs = self.search([
            ('state', 'in', ['done', 'cancelled']),
            ('end_date', '<', cutoff_date)
        ])
        
        if old_jobs:
            _logger.info(f"Cleaning up {len(old_jobs)} old jobs")
            old_jobs.unlink()
            
class OpenSanctionsJobConfirm(models.TransientModel):
    """Confirmation wizard for OpenSanctions jobs"""
    _name = 'opensanctions.job.confirm'
    _description = 'OpenSanctions Job Confirmation'
    
    job_id = fields.Many2one('opensanctions.job.queue', string='Job', required=True)
    action = fields.Selection([
        ('rerun', 'Rerun Job'),
        ('cancel', 'Cancel Job'),
    ], string='Action', required=True)
    
    def confirm_action(self):
        """Confirm the selected action"""
        self.ensure_one()
        
        if self.action == 'rerun':
            # Reset the job first
            self.job_id.action_reset_job()
            # Then run it
            return self.job_id.action_run_job()
        elif self.action == 'cancel':
            return self.job_id.action_cancel_job()
