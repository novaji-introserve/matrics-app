# -*- coding: utf-8 -*-
from odoo import api, fields, models, tools, SUPERUSER_ID, _
import logging
import threading
import traceback
from datetime import datetime, timedelta

_logger = logging.getLogger(__name__)

class PepScrapingCron(models.Model):
    """
    Cron job handlers for PEP web scraping and API fetching
    """
    _name = 'pep.scraping.cron'
    _description = 'PEP Scraping Cron Jobs'
    
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    @api.model
    def cron_fetch_pep_csv_data(self):
        """
        Cron job to fetch CSV data from all active sources
        This function will be called by the scheduled action
        """
        _logger.info("Starting cron job for PEP CSV data scraping")
        
        # Clean up old jobs first
        self._clean_old_jobs()
        
        # Get all active sources with CSV format
        sources = self.env['pep.source'].search([
            ('active', '=', True),
            ('source_format', '=', 'csv')
        ])
        
        if not sources:
            _logger.info("No active CSV sources found, skipping job")
            return True
            
        # Split between OpenSanctions and regular sources
        opensanctions_sources = sources.filtered(lambda s: s.is_opensanctions)
        regular_sources = sources - opensanctions_sources
        
        # Log source counts
        _logger.info(f"Found {len(opensanctions_sources)} OpenSanctions sources and {len(regular_sources)} regular sources")
        
        # Check if queue_job is installed
        queue_job = self.env['ir.module.module'].sudo().search(
            [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
        
        # Process OpenSanctions sources first to ensure they use dynamic URL discovery
        for source in opensanctions_sources:
            try:
                # Check if a job is already running for this source
                running_jobs = self.env['opensanctions.job.queue'].search([
                    ('source_id', '=', source.id),
                    ('state', 'in', ['running', 'pending']),
                    ('job_type', '=', 'csv'),
                    ('is_chunk', '=', False)
                ], limit=1)
                
                if running_jobs:
                    _logger.info(f"Skipping OpenSanctions source {source.name} - a job is already running")
                    continue
                
                _logger.info(f"Processing OpenSanctions source: {source.name}")
                job = self._create_csv_job(source)
                
                # Queue the job or run directly based on availability
                if queue_job:
                    # Use a unique identity key to prevent duplicate jobs
                    identity_key = f"opensanctions_csv_{source.id}_{datetime.now().strftime('%Y%m%d')}"
                    job.with_delay(priority=job.priority, identity_key=identity_key).process_job()
                    _logger.info(f"Queued OpenSanctions job for source: {source.name}")
                else:
                    # Run in a separate thread
                    thread = threading.Thread(
                        target=self._run_job_thread,
                        args=(self.env.cr.dbname, self.env.uid, job.id)
                    )
                    thread.daemon = True
                    thread.start()
                    _logger.info(f"Started OpenSanctions thread for source: {source.name}")
                    
            except Exception as e:
                _logger.error(f"Error processing OpenSanctions source {source.name}: {str(e)}")
                _logger.error(traceback.format_exc())
        
        # Process other sources
        for source in regular_sources:
            try:
                # Check if a job is already running for this source
                running_jobs = self.env['opensanctions.job.queue'].search([
                    ('source_id', '=', source.id),
                    ('state', 'in', ['running', 'pending']),
                    ('job_type', '=', 'csv'),
                    ('is_chunk', '=', False)
                ], limit=1)
                
                if running_jobs:
                    _logger.info(f"Skipping regular source {source.name} - a job is already running")
                    continue
                
                _logger.info(f"Processing regular CSV source: {source.name}")
                job = self._create_csv_job(source)
                
                # Queue the job or run directly based on availability
                if queue_job:
                    # Use a unique identity key to prevent duplicate jobs
                    identity_key = f"reg_csv_{source.id}_{datetime.now().strftime('%Y%m%d')}"
                    job.with_delay(priority=job.priority, identity_key=identity_key).process_job()
                    _logger.info(f"Queued CSV job for source: {source.name}")
                else:
                    # Run in a separate thread
                    thread = threading.Thread(
                        target=self._run_job_thread,
                        args=(self.env.cr.dbname, self.env.uid, job.id)
                    )
                    thread.daemon = True
                    thread.start()
                    _logger.info(f"Started CSV thread for source: {source.name}")
                    
            except Exception as e:
                _logger.error(f"Error processing CSV source {source.name}: {str(e)}")
                _logger.error(traceback.format_exc())
                
        return True
        
    @api.model
    def cron_fetch_pep_api_data(self):
        """
        Cron job to fetch API data from all active sources
        This function will be called by the scheduled action
        """
        _logger.info("Starting cron job for PEP API data fetching")
        
        # Clean up old jobs first
        self._clean_old_jobs()
        
        # Get all active sources with API format and API key
        sources = self.env['pep.source'].search([
            ('active', '=', True),
            ('source_format', '=', 'api'),
            ('use_api', '=', True),
            ('api_key', '!=', False)
        ])
        
        if not sources:
            _logger.info("No active API sources found, skipping job")
            return True
            
        _logger.info(f"Found {len(sources)} active API sources to process")
        
        # Split between OpenSanctions and regular sources
        opensanctions_sources = sources.filtered(lambda s: s.is_opensanctions)
        regular_sources = sources - opensanctions_sources
        
        # Log source counts
        _logger.info(f"Found {len(opensanctions_sources)} OpenSanctions API sources and {len(regular_sources)} regular API sources")
        
        # Check if queue_job is installed
        queue_job = self.env['ir.module.module'].sudo().search(
            [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
            
        # Process OpenSanctions API sources first
        for source in opensanctions_sources:
            try:
                # Check if a job is already running for this source
                running_jobs = self.env['opensanctions.job.queue'].search([
                    ('source_id', '=', source.id),
                    ('state', 'in', ['running', 'pending']),
                    ('job_type', '=', 'api'),
                    ('is_chunk', '=', False)
                ], limit=1)
                
                if running_jobs:
                    _logger.info(f"Skipping OpenSanctions API source {source.name} - a job is already running")
                    continue
                
                _logger.info(f"Processing OpenSanctions API source: {source.name}")
                job = self._create_api_job(source)
                
                # Queue the job or run directly based on availability
                if queue_job:
                    # Use a unique identity key to prevent duplicate jobs
                    identity_key = f"opensanctions_api_{source.id}_{datetime.now().strftime('%Y%m%d')}"
                    job.with_delay(priority=job.priority, identity_key=identity_key).process_job()
                    _logger.info(f"Queued OpenSanctions API job for source: {source.name}")
                else:
                    # Run in a separate thread
                    thread = threading.Thread(
                        target=self._run_job_thread,
                        args=(self.env.cr.dbname, self.env.uid, job.id)
                    )
                    thread.daemon = True
                    thread.start()
                    _logger.info(f"Started OpenSanctions API thread for source: {source.name}")
                    
            except Exception as e:
                _logger.error(f"Error processing OpenSanctions API source {source.name}: {str(e)}")
                _logger.error(traceback.format_exc())
        
        # Process other API sources
        for source in regular_sources:
            try:
                # Check if a job is already running for this source
                running_jobs = self.env['opensanctions.job.queue'].search([
                    ('source_id', '=', source.id),
                    ('state', 'in', ['running', 'pending']),
                    ('job_type', '=', 'api'),
                    ('is_chunk', '=', False)
                ], limit=1)
                
                if running_jobs:
                    _logger.info(f"Skipping regular API source {source.name} - a job is already running")
                    continue
                
                _logger.info(f"Processing regular API source: {source.name}")
                job = self._create_api_job(source)
                
                # Queue the job or run directly based on availability
                if queue_job:
                    # Use a unique identity key to prevent duplicate jobs
                    identity_key = f"reg_api_{source.id}_{datetime.now().strftime('%Y%m%d')}"
                    job.with_delay(priority=job.priority, identity_key=identity_key).process_job()
                    _logger.info(f"Queued API job for source: {source.name}")
                else:
                    # Run in a separate thread
                    thread = threading.Thread(
                        target=self._run_job_thread,
                        args=(self.env.cr.dbname, self.env.uid, job.id)
                    )
                    thread.daemon = True
                    thread.start()
                    _logger.info(f"Started API thread for source: {source.name}")
                    
            except Exception as e:
                _logger.error(f"Error processing API source {source.name}: {str(e)}")
                _logger.error(traceback.format_exc())
                
        return True
    
    def _run_job_thread(self, dbname, uid, job_id):
        """
        Run a job in a separate thread
        
        Args:
            dbname: Database name
            uid: User ID
            job_id: Job ID to run
        """
        with api.Environment.manage():
            # Get a new cursor for this thread
            registry = tools.registry(dbname)
            with registry.cursor() as cr:
                env = api.Environment(cr, uid, {})
                
                # Get the job and process it
                try:
                    job = env['opensanctions.job.queue'].browse(job_id)
                    if job.exists():
                        job.process_job()
                        cr.commit()
                    else:
                        _logger.error(f"Job {job_id} does not exist")
                except Exception as e:
                    _logger.error(f"Error in job thread: {str(e)}")
                    _logger.error(traceback.format_exc())

    def _clean_old_jobs(self, days=7):
        """
        Clean up old jobs before starting new ones
        
        Args:
            days: Number of days to keep completed jobs
        """
        try:
            # Call the cleanup method from the job queue model
            job_queue = self.env['opensanctions.job.queue']
            job_queue.clean_old_jobs(days=days)
            
            # Also clean up stalled jobs
            stalled_cutoff = datetime.now() - timedelta(hours=24)
            stalled_jobs = job_queue.search([
                ('state', '=', 'running'),
                ('start_date', '<', stalled_cutoff)
            ])
            
            if stalled_jobs:
                _logger.info(f"Resetting {len(stalled_jobs)} stalled jobs")
                stalled_jobs.write({
                    'state': 'failed',
                    'end_date': fields.Datetime.now(),
                    'result': 'Job timed out after 24 hours'
                })
                
            # Cleanup chunk jobs without parents (orphaned chunks)
            orphaned_chunks = job_queue.search([
                ('is_chunk', '=', True),
                '|',
                ('parent_job_id', '=', False),
                ('parent_job_id.state', 'in', ['done', 'failed', 'cancelled'])
            ])
            
            if orphaned_chunks:
                _logger.info(f"Cleaning up {len(orphaned_chunks)} orphaned chunk jobs")
                orphaned_chunks.unlink()
                
            # Find jobs that are stuck in "running" state but without chunks making progress
            stuck_parent_jobs = job_queue.search([
                ('state', '=', 'running'),
                ('is_chunk', '=', False),
                ('total_chunks', '>', 0),
                ('start_date', '<', stalled_cutoff)
            ])
            
            for job in stuck_parent_jobs:
                # Check if any chunks are still running
                running_chunks = job_queue.search_count([
                    ('parent_job_id', '=', job.id),
                    ('state', '=', 'running')
                ])
                
                # Check if there are still pending chunks
                pending_chunks = job_queue.search_count([
                    ('parent_job_id', '=', job.id),
                    ('state', '=', 'pending')
                ])
                
                if running_chunks == 0 and pending_chunks > 0:
                    # Job is stuck - no chunks are running but there are pending chunks
                    _logger.info(f"Found stuck parent job {job.id} with no running chunks but {pending_chunks} pending chunks")
                    
                    # Get the next pending chunk
                    next_chunk = job_queue.search([
                        ('parent_job_id', '=', job.id),
                        ('state', '=', 'pending')
                    ], order='chunk_index', limit=1)
                    
                    if next_chunk:
                        # Check if queue_job is installed
                        queue_job = self.env['ir.module.module'].sudo().search(
                            [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
                            
                        if queue_job:
                            # Force restart the next chunk
                            _logger.info(f"Force starting next chunk {next_chunk.id} with index {next_chunk.chunk_index}")
                            identity_key = f"force_restart_chunk_{next_chunk.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                            next_chunk.with_delay(priority=100, identity_key=identity_key).process_job()
                
        except Exception as e:
            _logger.error(f"Error cleaning up old jobs: {str(e)}")

    def _create_csv_job(self, source):
        """Create a CSV import job for a source"""
        # Check for existing pending jobs to avoid duplicates
        existing_jobs = self.env['opensanctions.job.queue'].search([
            ('source_id', '=', source.id),
            ('state', '=', 'pending'),
            ('job_type', '=', 'csv'),
            ('is_chunk', '=', False)
        ], limit=1)
        
        if existing_jobs:
            _logger.info(f"Using existing pending job for source {source.name}")
            return existing_jobs[0]
        
        job_vals = {
            'name': f"CSV Import - {source.name}",
            'source_id': source.id,
            'job_type': 'csv',
            'priority': 10,
            'state': 'pending'
        }
        
        # Add extra info for OpenSanctions sources
        if source.is_opensanctions:
            job_vals['name'] = f"OpenSanctions CSV Import - {source.name}"
            job_vals['priority'] = 15  # Higher priority for OpenSanctions
        
        return self.env['opensanctions.job.queue'].create(job_vals)
        
    def _create_api_job(self, source):
        """Create an API import job for a source"""
        # Check for existing pending jobs to avoid duplicates
        existing_jobs = self.env['opensanctions.job.queue'].search([
            ('source_id', '=', source.id),
            ('state', '=', 'pending'),
            ('job_type', '=', 'api'),
            ('is_chunk', '=', False)
        ], limit=1)
        
        if existing_jobs:
            _logger.info(f"Using existing pending job for source {source.name}")
            return existing_jobs[0]
            
        job_vals = {
            'name': f"API Import - {source.name}",
            'source_id': source.id,
            'job_type': 'api',
            'priority': 10,
            'state': 'pending'
        }
        
        # Add extra info for OpenSanctions sources
        if source.is_opensanctions:
            job_vals['name'] = f"OpenSanctions API Import - {source.name}"
            job_vals['priority'] = 15  # Higher priority for OpenSanctions
        
        return self.env['opensanctions.job.queue'].create(job_vals)
   
    @api.model 
    def manual_fetch_for_source(self, source_id, job_type=None):
        """
        Manually trigger a fetch job for a specific source
        
        Args:
            source_id: ID of the source to fetch
            job_type: Type of job to run (if None, use source default)
        
        Returns:
            dict: Status message
        """
        source = self.env['pep.source'].browse(source_id)
        
        if not source.exists():
            return {
                'status': 'error',
                'message': f"Source with ID {source_id} not found"
            }
        
        # If job_type isn't specified, use the source's format
        if not job_type:
            job_type = source.source_format
            
        # Check if the job type is valid for this source
        if job_type == 'api' and (not source.use_api or not source.api_key):
            return {
                'status': 'error',
                'message': f"API not properly configured for source {source.name}"
            }
            
        # Create job based on type
        if job_type == 'csv':
            job = self._create_csv_job(source)
        elif job_type == 'api':
            job = self._create_api_job(source)
        else:
            return {
                'status': 'error',
                'message': f"Invalid job type: {job_type}"
            }
            
        # Run the job
        job.action_run_job()
        
        return {
            'status': 'success',
            'message': f"Import job created for {source.name}",
            'job_id': job.id
        }
        