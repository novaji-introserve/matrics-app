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
    
    @api.model
    def cron_fetch_pep_csv_data(self):
        """
        Cron job to fetch CSV data from all active sources
        This function will be called by the scheduled action
        """
        _logger.info("Starting cron job for PEP CSV data scraping")
        
        # Get all active sources with CSV format
        sources = self.env['pep.source'].search([
            ('active', '=', True),
            ('source_format', 'in', ['csv', 'both'])
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
                _logger.info(f"Processing OpenSanctions source: {source.name}")
                job = self._create_csv_job(source)
                
                # Queue the job or run directly based on availability
                if queue_job:
                    job.with_delay(priority=job.priority).process_job()
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
                _logger.info(f"Processing regular CSV source: {source.name}")
                job = self._create_csv_job(source)
                
                # Queue the job or run directly based on availability
                if queue_job:
                    job.with_delay(priority=job.priority).process_job()
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
        
        # Get all active sources with API format and API key
        sources = self.env['pep.source'].search([
            ('active', '=', True),
            ('source_format', 'in', ['api', 'both']),
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
                _logger.info(f"Processing OpenSanctions API source: {source.name}")
                job = self._create_api_job(source)
                
                # Queue the job or run directly based on availability
                if queue_job:
                    job.with_delay(priority=job.priority).process_job()
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
                _logger.info(f"Processing regular API source: {source.name}")
                job = self._create_api_job(source)
                
                # Queue the job or run directly based on availability
                if queue_job:
                    job.with_delay(priority=job.priority).process_job()
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
                except Exception as e:
                    _logger.error(f"Error in job thread: {str(e)}")
                    _logger.error(traceback.format_exc())

    def _create_csv_job(self, source):
        """Create a CSV import job for a source"""
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


# from odoo import api, fields, models, tools, SUPERUSER_ID, _
# import logging
# import threading
# from datetime import datetime, timedelta

# _logger = logging.getLogger(__name__)

# class PepScrapingCron(models.Model):
#     """
#     Cron job handlers for PEP web scraping and API fetching
#     """
#     _name = 'pep.scraping.cron'
#     _description = 'PEP Scraping Cron Jobs'
    
#     @api.model
#     def cron_fetch_pep_csv_data(self):
#         """
#         Cron job to fetch CSV data from all active sources
#         This function will be called by the scheduled action
#         """
#         _logger.info("Starting cron job for PEP CSV data scraping")
        
#         # Get all active sources with CSV format
#         sources = self.env['pep.source'].search([
#             ('active', '=', True),
#             ('source_format', 'in', ['csv', 'both'])
#         ])
        
#         if not sources:
#             _logger.info("No active CSV sources found, skipping job")
#             return True
            
#         # Split between OpenSanctions and regular sources
#         opensanctions_sources = sources.filtered(lambda s: s.is_opensanctions)
#         regular_sources = sources - opensanctions_sources
        
#         # Log source counts
#         _logger.info(f"Found {len(opensanctions_sources)} OpenSanctions sources and {len(regular_sources)} regular sources")
        
#         # Check if queue_job is installed
#         queue_job = self.env['ir.module.module'].sudo().search(
#             [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
        
#         # Process OpenSanctions sources first to ensure they use dynamic URL discovery
#         for source in opensanctions_sources:
#             try:
#                 _logger.info(f"Processing OpenSanctions source: {source.name}")
#                 job = self._create_csv_job(source)
                
#                 # Queue the job or run directly based on availability
#                 if queue_job:
#                     job.with_delay(priority=job.priority).process_job()
#                     _logger.info(f"Queued OpenSanctions job for source: {source.name}")
#                 else:
#                     # Run in a separate thread
#                     thread = threading.Thread(
#                         target=self._run_job_thread,
#                         args=(self.env.cr.dbname, self.env.uid, job.id)
#                     )
#                     thread.daemon = True
#                     thread.start()
#                     _logger.info(f"Started OpenSanctions thread for source: {source.name}")
                    
#             except Exception as e:
#                 _logger.error(f"Error processing OpenSanctions source {source.name}: {str(e)}")
        
#         # Process other sources
#         for source in regular_sources:
#             try:
#                 _logger.info(f"Processing regular CSV source: {source.name}")
#                 job = self._create_csv_job(source)
                
#                 # Queue the job or run directly based on availability
#                 if queue_job:
#                     job.with_delay(priority=job.priority).process_job()
#                     _logger.info(f"Queued CSV job for source: {source.name}")
#                 else:
#                     # Run in a separate thread
#                     thread = threading.Thread(
#                         target=self._run_job_thread,
#                         args=(self.env.cr.dbname, self.env.uid, job.id)
#                     )
#                     thread.daemon = True
#                     thread.start()
#                     _logger.info(f"Started CSV thread for source: {source.name}")
                    
#             except Exception as e:
#                 _logger.error(f"Error processing CSV source {source.name}: {str(e)}")
                
#         return True

#     # def cron_fetch_pep_csv_data(self):
#     #     """
#     #     Cron job to fetch CSV data from all active sources
#     #     This function will be called by the scheduled action
#     #     """
#     #     _logger.info("Starting cron job for PEP CSV data scraping")
        
#     #     # Get all active sources with CSV format
#     #     sources = self.env['pep.source'].search([
#     #         ('active', '=', True),
#     #         ('source_format', 'in', ['csv', 'both'])
#     #     ])
        
#     #     if not sources:
#     #         _logger.info("No active CSV sources found, skipping job")
#     #         return True
            
#     #     _logger.info(f"Found {len(sources)} active CSV sources to process")
        
#     #     # Check if queue_job is installed
#     #     queue_job = self.env['ir.module.module'].sudo().search(
#     #         [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
            
#     #     # Process each source
#     #     for source in sources:
#     #         try:
#     #             _logger.info(f"Processing CSV source: {source.name}")
                
#     #             # Create a job for this source
#     #             job_vals = {
#     #                 'name': f"CSV Import - {source.name}",
#     #                 'source_id': source.id,
#     #                 'job_type': 'csv',
#     #                 'priority': 10,
#     #                 'state': 'pending'
#     #             }
                
#     #             job = self.env['opensanctions.job.queue'].create(job_vals)
                
#     #             # Queue the job or run directly based on availability
#     #             if queue_job:
#     #                 job.with_delay(priority=job.priority).process_job()
#     #                 _logger.info(f"Queued CSV job for source: {source.name}")
#     #             else:
#     #                 # Run in a separate thread
#     #                 thread = threading.Thread(
#     #                     target=self._run_job_thread,
#     #                     args=(self.env.cr.dbname, self.env.uid, job.id)
#     #                 )
#     #                 thread.daemon = True
#     #                 thread.start()
#     #                 _logger.info(f"Started CSV thread for source: {source.name}")
                    
#     #         except Exception as e:
#     #             _logger.error(f"Error processing CSV source {source.name}: {str(e)}")
                
#     #     return True
        
#     @api.model
#     def cron_fetch_pep_api_data(self):
#         """
#         Cron job to fetch API data from all active sources
#         This function will be called by the scheduled action
#         """
#         _logger.info("Starting cron job for PEP API data fetching")
        
#         # Get all active sources with API format and API key
#         sources = self.env['pep.source'].search([
#             ('active', '=', True),
#             ('source_format', 'in', ['api', 'both']),
#             ('use_api', '=', True),
#             ('api_key', '!=', False)
#         ])
        
#         if not sources:
#             _logger.info("No active API sources found, skipping job")
#             return True
            
#         _logger.info(f"Found {len(sources)} active API sources to process")
        
#         # Check if queue_job is installed
#         queue_job = self.env['ir.module.module'].sudo().search(
#             [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
            
#         # Process each source
#         for source in sources:
#             try:
#                 _logger.info(f"Processing API source: {source.name}")
                
#                 # Create a job for this source
#                 job_vals = {
#                     'name': f"API Import - {source.name}",
#                     'source_id': source.id,
#                     'job_type': 'api',
#                     'priority': 10,
#                     'state': 'pending'
#                 }
                
#                 job = self.env['opensanctions.job.queue'].create(job_vals)
                
#                 # Queue the job or run directly based on availability
#                 if queue_job:
#                     job.with_delay(priority=job.priority).process_job()
#                     _logger.info(f"Queued API job for source: {source.name}")
#                 else:
#                     # Run in a separate thread
#                     thread = threading.Thread(
#                         target=self._run_job_thread,
#                         args=(self.env.cr.dbname, self.env.uid, job.id)
#                     )
#                     thread.daemon = True
#                     thread.start()
#                     _logger.info(f"Started API thread for source: {source.name}")
                    
#             except Exception as e:
#                 _logger.error(f"Error processing API source {source.name}: {str(e)}")
                
#         return True
    
#     def _run_job_thread(self, dbname, uid, job_id):
#         """
#         Run a job in a separate thread
        
#         Args:
#             dbname: Database name
#             uid: User ID
#             job_id: Job ID to run
#         """
#         with api.Environment.manage():
#             # Get a new cursor for this thread
#             registry = tools.registry(dbname)
#             with registry.cursor() as cr:
#                 env = api.Environment(cr, uid, {})
                
#                 # Get the job and process it
#                 try:
#                     job = env['opensanctions.job.queue'].browse(job_id)
#                     if job.exists():
#                         job.process_job()
#                         cr.commit()
#                 except Exception as e:
#                     _logger.error(f"Error in job thread: {str(e)}")

    
#     @api.model
#     def cron_fetch_pep_api_data(self):
#         """
#         Cron job to fetch API data from all active sources
#         This function will be called by the scheduled action
#         """
#         _logger.info("Starting cron job for PEP API data fetching")
        
#         # Get all active sources with API format and API key
#         sources = self.env['pep.source'].search([
#             ('active', '=', True),
#             ('source_format', 'in', ['api', 'both']),
#             ('use_api', '=', True),
#             ('api_key', '!=', False)
#         ])
        
#         if not sources:
#             _logger.info("No active API sources found, skipping job")
#             return True
            
#         _logger.info(f"Found {len(sources)} active API sources to process")
        
#         # Check if queue_job is installed
#         queue_job = self.env['ir.module.module'].sudo().search(
#             [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
            
#         # Process each source
#         for source in sources:
#             try:
#                 _logger.info(f"Processing API source: {source.name}")
                
#                 # Create a job for this source
#                 job_vals = {
#                     'name': f"API Import - {source.name}",
#                     'source_id': source.id,
#                     'job_type': 'api',
#                     'priority': 10,
#                     'state': 'pending'
#                 }
                
#                 job = self.env['opensanctions.job.queue'].create(job_vals)
                
#                 # Queue the job or run directly based on availability
#                 if queue_job:
#                     job.with_delay(priority=job.priority).process_job()
#                     _logger.info(f"Queued API job for source: {source.name}")
#                 else:
#                     # Run in a separate thread
#                     thread = threading.Thread(
#                         target=self._run_job_thread,
#                         args=(self.env.cr.dbname, self.env.uid, job.id)
#                     )
#                     thread.daemon = True
#                     thread.start()
#                     _logger.info(f"Started API thread for source: {source.name}")
                    
#             except Exception as e:
#                 _logger.error(f"Error processing API source {source.name}: {str(e)}")
                
#         return True
    
#     def _run_job_thread(self, dbname, uid, job_id):
#         """
#         Run a job in a separate thread
        
#         Args:
#             dbname: Database name
#             uid: User ID
#             job_id: Job ID to run
#         """
#         with api.Environment.manage():
#             # Get a new cursor for this thread
#             registry = tools.registry(dbname)
#             with registry.cursor() as cr:
#                 env = api.Environment(cr, uid, {})
                
#                 # Get the job and process it
#                 try:
#                     job = env['opensanctions.job.queue'].browse(job_id)
#                     if job.exists():
#                         job.process_job()
#                         cr.commit()
#                 except Exception as e:
#                     _logger.error(f"Error in job thread: {str(e)}")

#     def _create_csv_job(self, source):
#         """Create a CSV import job for a source"""
#         job_vals = {
#             'name': f"CSV Import - {source.name}",
#             'source_id': source.id,
#             'job_type': 'csv',
#             'priority': 10,
#             'state': 'pending'
#         }
        
#         # Add extra info for OpenSanctions sources
#         if source.is_opensanctions:
#             job_vals['name'] = f"OpenSanctions CSV Import - {source.name}"
#             job_vals['priority'] = 15  # Higher priority for OpenSanctions
        
#         return self.env['opensanctions.job.queue'].create(job_vals)
