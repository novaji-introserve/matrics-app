import logging
import time
import os
from datetime import datetime, timedelta
from odoo import api, fields, models, tools, SUPERUSER_ID, _

_logger = logging.getLogger(__name__)

class OpenSanctionsJobQueue(models.Model):
    """
    Model to manage OpenSanctions data import jobs with improved storage handling
    """
    _name = 'opensanctions.job.queue'
    _description = 'OpenSanctions Job Queue'
    _order = 'priority desc, create_date desc'
    
    name = fields.Char('Job Name', required=True)
    source_id = fields.Many2one('pep.source', string='Data Source', required=True, ondelete='cascade')
    job_type = fields.Selection([
        ('csv', 'CSV Import'),
        ('api', 'API Import'),
        ('both', 'CSV and API Import')
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
    api_limit = fields.Integer('API Result Limit', default=1000,
                              help="Maximum number of results to fetch from API")
    file_path = fields.Char('CSV File Path', readonly=True,
                           help="Path to the downloaded CSV file")
    batch_size = fields.Integer('Batch Size', default=500,
                              help="Number of records to process in each batch")
    
    def name_get(self):
        result = []
        for job in self:
            source_name = job.source_id.name or 'Unknown Source'
            state_label = dict(self._fields['state'].selection).get(job.state)
            result.append((job.id, f"{job.name} ({source_name}) - {state_label}"))
        return result
    
    @api.model
    def create(self, vals):
        """Override create to set a default name if not provided"""
        if not vals.get('name'):
            source = self.env['pep.source'].browse(vals.get('source_id'))
            job_type = vals.get('job_type', 'both')
            vals['name'] = f"OpenSanctions Import - {source.name} ({job_type})"
        return super(OpenSanctionsJobQueue, self).create(vals)
    
    def action_run_job(self):
        """Manually trigger job execution"""
        self.ensure_one()
        
        if self.state in ['running', 'done']:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("Cannot Run Job"),
                    'message': _("This job is already running or completed."),
                    'sticky': False,
                    'type': 'warning',
                }
            }
            
        # Check if queue_job is installed
        queue_job = self.env['ir.module.module'].sudo().search(
            [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
            
        if queue_job:
            # Use unique identity key to prevent duplicate jobs
            identity_key = f"opensanctions_job_{self.source_id.id}_{self.job_type}_{datetime.now().strftime('%Y%m%d')}"
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
                
            # Initialize services
            from ..services.open_sanctions import OpenSanctions
            from ..services.open_sanctions_importer import OpenSanctionsImporter
            
            service = OpenSanctions(self.env)
            importer = OpenSanctionsImporter(self.env)
            
            # Set batch size for importer
            importer.batch_size = self.batch_size or 500
            
            # Ensure the storage directory exists
            storage_dir = self.env['ir.config_parameter'].sudo().get_param(
                'compliance_management.pep_storage_dir', '/media/pep_list_data')
                
            if not os.path.exists(storage_dir):
                try:
                    os.makedirs(storage_dir, exist_ok=True)
                    self._append_log(f"Created storage directory: {storage_dir}")
                except Exception as e:
                    self._append_log(f"Error creating storage directory: {str(e)}")
            
            results = {}
            
            # Process based on job type
            if self.job_type == 'csv':
                # CSV-only job
                self._process_csv_job(service, importer, source)
                
            elif self.job_type == 'api':
                # API-only job
                self._process_api_job(service, importer, source)
                
            elif self.job_type == 'both':
                # Process both CSV and API
                self._process_csv_job(service, importer, source)
                self._process_api_job(service, importer, source)
                
            # Calculate duration
            end_time = time.time()
            duration = end_time - start_time
            
            # Determine overall job status
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
            
            return {
                'status': status,
                'message': message,
                'records_created': self.records_created,
                'records_updated': self.records_updated
            }
            
        except Exception as e:
            import traceback
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
            
            return {
                'status': 'error',
                'message': str(e)
            }

    def _process_csv_job(self, service, importer, source):
        """Process CSV data fetching and importing with dynamic URL discovery"""
        self._append_log("Fetching CSV data...")
        
        # Use dynamic discovery for OpenSanctions sources
        if source.is_opensanctions:
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
        else:
            self._append_log(f"CSV fetch failed: {csv_result.get('message')}")
    
    def _process_api_job(self, service, importer, source):
        """Process API data fetching and importing"""
        if not source.use_api or not source.api_key:
            self._append_log("API not configured for this source, skipping")
            return
            
        self._append_log("Querying API...")
        api_result = service.query_api(
            entity_type=source.default_entity_type,
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
    
    def _process_api_job(self, service, importer, source):
        """Process API data fetching and importing"""
        if not source.use_api or not source.api_key:
            self._append_log("API not configured for this source, skipping")
            return
            
        self._append_log("Querying API...")
        api_result = service.query_api(
            entity_type=source.default_entity_type,
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
        
    @api.model
    def _process_scheduled_jobs(self):
        """
        Process any scheduled jobs that are due to run
        This method is meant to be called by a cron job
        """
        now = fields.Datetime.now()
        jobs_to_run = self.search([
            ('state', '=', 'pending'),
            ('next_run', '<=', now)
        ])
        
        _logger.info(f"Running {len(jobs_to_run)} scheduled OpenSanctions jobs")
        
        for job in jobs_to_run:
            try:
                # Check if queue_job is installed
                queue_job = self.env['ir.module.module'].sudo().search(
                    [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
                    
                if queue_job:
                    # Use unique identity key to prevent duplicate jobs
                    identity_key = f"opensanctions_scheduled_job_{job.id}_{datetime.now().strftime('%Y%m%d')}"
                    job.with_delay(priority=job.priority, identity_key=identity_key).process_job()
                    _logger.info(f"Queued scheduled job: {job.name}")
                else:
                    job.process_job()
                    _logger.info(f"Ran scheduled job directly: {job.name}")
            except Exception as e:
                _logger.error(f"Error scheduling job {job.name}: {str(e)}")

# import logging
# import time
# from datetime import datetime, timedelta
# from odoo import api, fields, models, tools, SUPERUSER_ID, _

# _logger = logging.getLogger(__name__)

# class OpenSanctionsJobQueue(models.Model):
#     """
#     Model to manage OpenSanctions data import jobs
#     """
#     _name = 'opensanctions.job.queue'
#     _description = 'OpenSanctions Job Queue'
#     _order = 'priority desc, create_date desc'
    
#     name = fields.Char('Job Name', required=True)
#     source_id = fields.Many2one('pep.source', string='Data Source', required=True, ondelete='cascade')
#     job_type = fields.Selection([
#         ('csv', 'CSV Import'),
#         ('api', 'API Import'),
#         ('both', 'CSV and API Import')
#     ], string='Job Type', required=True, default='both')
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
    
#     def name_get(self):
#         result = []
#         for job in self:
#             source_name = job.source_id.name or 'Unknown Source'
#             state_label = dict(self._fields['state'].selection).get(job.state)
#             result.append((job.id, f"{job.name} ({source_name}) - {state_label}"))
#         return result
    
#     @api.model
#     def create(self, vals):
#         """Override create to set a default name if not provided"""
#         if not vals.get('name'):
#             source = self.env['pep.source'].browse(vals.get('source_id'))
#             job_type = vals.get('job_type', 'both')
#             vals['name'] = f"OpenSanctions Import - {source.name} ({job_type})"
#         return super(OpenSanctionsJobQueue, self).create(vals)
    
#     def action_run_job(self):
#         """Manually trigger job execution"""
#         self.ensure_one()
        
#         if self.state in ['running', 'done']:
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("Cannot Run Job"),
#                     'message': _("This job is already running or completed."),
#                     'sticky': False,
#                     'type': 'warning',
#                 }
#             }
            
#         # Check if queue_job is installed
#         queue_job = self.env['ir.module.module'].sudo().search(
#             [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
            
#         if queue_job:
#             self.with_delay(priority=self.priority).process_job()
            
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
                
#             # Initialize services
#             from ..services.open_sanctions import OpenSanctions
#             from ..services.open_sanctions_importer import OpenSanctionsImporter
            
#             service = OpenSanctions(self.env)
#             importer = OpenSanctionsImporter(self.env)
            
#             results = {}
            
#             # Process based on job type
#             if self.job_type == 'csv':
#                 # CSV-only job
#                 self._process_csv_job(service, importer, source)
                
#             elif self.job_type == 'api':
#                 # API-only job
#                 self._process_api_job(service, importer, source)
                
#             elif self.job_type == 'both':
#                 # Process both CSV and API
#                 self._process_csv_job(service, importer, source)
#                 self._process_api_job(service, importer, source)
                
#             # Calculate duration
#             end_time = time.time()
#             duration = end_time - start_time
            
#             # Determine overall job status
#             if self.records_created > 0 or self.records_updated > 0:
#                 status = 'done'
#                 message = f"Job completed successfully: {self.records_created} records created, {self.records_updated} updated in {duration:.2f} seconds"
#             else:
#                 status = 'done'
#                 message = f"Job completed with no changes: {duration:.2f} seconds"
            
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
            
#             return {
#                 'status': status,
#                 'message': message,
#                 'records_created': self.records_created,
#                 'records_updated': self.records_updated
#             }
            
#         except Exception as e:
#             import traceback
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
            
#             return {
#                 'status': 'error',
#                 'message': str(e)
#             }

#     def _process_csv_job(self, service, importer, source):
#         """Process CSV data fetching and importing with dynamic URL discovery"""
#         self._append_log("Fetching CSV data...")
        
#         # Use dynamic discovery for OpenSanctions sources
#         if source.is_opensanctions:
#             self._append_log("Using dynamic URL discovery for OpenSanctions...")
#             csv_result = service.fetch_latest_opensanctions_csv(source)
#         else:
#             # Standard fetch for other sources
#             csv_result = service.fetch_csv_file(source)
        
#         if csv_result.get('status') == 'success':
#             self._append_log(f"CSV file downloaded: {csv_result.get('path')}")
#             self._append_log("Processing CSV data...")
            
#             # Process CSV file
#             csv_process_result = importer.process_csv_file(csv_result.get('path'), source)
            
#             if csv_process_result.get('status') == 'success':
#                 self._append_log(f"CSV processing completed: {csv_process_result.get('records_created', 0)} created, "
#                             f"{csv_process_result.get('records_updated', 0)} updated")
                
#                 # Update job counters
#                 self.write({
#                     'records_created': self.records_created + csv_process_result.get('records_created', 0),
#                     'records_updated': self.records_updated + csv_process_result.get('records_updated', 0),
#                     'records_failed': self.records_failed + csv_process_result.get('records_errored', 0)
#                 })
#             else:
#                 self._append_log(f"CSV processing failed: {csv_process_result.get('message')}")
#         else:
#             self._append_log(f"CSV fetch failed: {csv_result.get('message')}")
    
#     # def _process_csv_job(self, service, importer, source):
#     #     """Process CSV data fetching and importing"""
#     #     self._append_log("Fetching CSV data...")
#     #     csv_result = service.fetch_csv_file(source)
        
#     #     if csv_result.get('status') == 'success':
#     #         self._append_log(f"CSV file downloaded: {csv_result.get('path')}")
#     #         self._append_log("Processing CSV data...")
            
#     #         # Process CSV file
#     #         csv_process_result = importer.process_csv_file(csv_result.get('path'), source)
            
#     #         if csv_process_result.get('status') == 'success':
#     #             self._append_log(f"CSV processing completed: {csv_process_result.get('records_created', 0)} created, "
#     #                            f"{csv_process_result.get('records_updated', 0)} updated")
                
#     #             # Update job counters
#     #             self.write({
#     #                 'records_created': self.records_created + csv_process_result.get('records_created', 0),
#     #                 'records_updated': self.records_updated + csv_process_result.get('records_updated', 0),
#     #                 'records_failed': self.records_failed + csv_process_result.get('records_errored', 0)
#     #             })
#     #         else:
#     #             self._append_log(f"CSV processing failed: {csv_process_result.get('message')}")
#     #     else:
#     #         self._append_log(f"CSV fetch failed: {csv_result.get('message')}")
    
#     def _process_api_job(self, service, importer, source):
#         """Process API data fetching and importing"""
#         if not source.use_api or not source.api_key:
#             self._append_log("API not configured for this source, skipping")
#             return
            
#         self._append_log("Querying API...")
#         api_result = service.query_api(
#             entity_type=source.default_entity_type,
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
#             ('next_run', '<=', now)
#         ])
        
#         _logger.info(f"Running {len(jobs_to_run)} scheduled OpenSanctions jobs")
        
#         for job in jobs_to_run:
#             try:
#                 # Check if queue_job is installed
#                 queue_job = self.env['ir.module.module'].sudo().search(
#                     [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
                    
#                 if queue_job:
#                     job.with_delay(priority=job.priority).process_job()
#                     _logger.info(f"Queued scheduled job: {job.name}")
#                 else:
#                     job.process_job()
#                     _logger.info(f"Ran scheduled job directly: {job.name}")
#             except Exception as e:
#                 _logger.error(f"Error scheduling job {job.name}: {str(e)}")