from odoo import _, api, fields, models
import logging
import threading
import uuid
from datetime import datetime, timedelta

from ..services.open_sanctions import OpenSanctions
from ..services.open_sanctions_importer import OpenSanctionsImporter

_logger = logging.getLogger(__name__)

class PepSource(models.Model):
    """
    Extension of the PEP Source model with additional fields for web scraping and API configuration
    """
    _inherit = 'pep.source'
    
    # General configuration
    source_format = fields.Selection([
        ('csv', 'CSV File'),
        ('api', 'API'),
        ('both', 'Both CSV and API')
    ], string='Source Format', default='csv', required=True)
    
    # CSV file configuration
    base_url = fields.Char('Base URL', help="Base URL for the sanctions data source")
    csv_path = fields.Char('CSV Path', help="Path to the CSV file, relative to the base URL")
    request_headers = fields.Text('Request Headers', help="JSON dictionary of HTTP headers to send with requests")
    csv_delimiter = fields.Char('CSV Delimiter', default=',', help="Delimiter character for CSV files")
    
    # API configuration
    use_api = fields.Boolean('Use API', default=False, help="Use API for data retrieval")
    api_url = fields.Char('API URL', help="Base URL for the API")
    api_endpoint = fields.Char('API Endpoint', help="API endpoint path, relative to the API URL")
    api_auth_format = fields.Char('Auth Format', default='ApiKey {}', 
                                 help="Format string for API authorization header, use {} for API key placement")
    api_headers = fields.Text('API Headers', help="JSON dictionary of HTTP headers to send with API requests")
    api_params = fields.Text('API Parameters', help="JSON dictionary of default query parameters to send with API requests")
    default_entity_type = fields.Char('Default Entity Type', default='person',
                                     help="Default entity type to query in the API")
    api_results_path = fields.Char('Results Path', help="Path to results array in API response, using dot notation")
    api_entity_filter = fields.Char('Entity Filter', 
                                   help="Filter for entity types in format 'field:value', e.g. 'schema:Person'")
    
    # Field mapping
    field_mapping = fields.Text('Field Mapping', 
                               help="JSON dictionary mapping source fields to PEP model fields")
    
    is_opensanctions = fields.Boolean('Is OpenSanctions', help="Check if this source is from OpenSanctions.org")

    job_id = fields.Char(string="Current Job ID", readonly=True)
    job_status = fields.Selection(
        [
            ("pending", "Pending"),
            ("running", "Running"),
            ("completed", "Completed"),
            ("failed", "Failed"),
        ],
        string="Job Status",
        readonly=True,
    )
    job_started = fields.Datetime(string="Job Started", readonly=True)
    job_completed = fields.Datetime(string="Job Completed", readonly=True)
    import_message = fields.Text(string="Import Message", readonly=True)

    def action_test_csv_fetch(self):
        """Test the CSV fetch configuration with dynamic URL discovery for OpenSanctions"""
        self.ensure_one()
        
        from ..services.open_sanctions import OpenSanctions
        service = OpenSanctions(self.env)
        
        # Use dynamic discovery if this is an OpenSanctions source
        if self.is_opensanctions:
            result = service.fetch_latest_opensanctions_csv(self)
        else:
            result = service.fetch_csv_file(self)
        
        if result.get('status') == 'success':
            # Delete the temporary file
            if 'path' in result:
                try:
                    import os
                    os.unlink(result['path'])
                except:
                    pass
                    
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("CSV Fetch Successful"),
                    'message': _("Successfully downloaded CSV file from %s") % result.get('url'),
                    'sticky': False,
                    'type': 'success',
                }
            }
        else:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("CSV Fetch Failed"),
                    'message': _(result.get('message')),
                    'sticky': False,
                    'type': 'warning',
                }
            }
            
    def action_test_api(self):
        """Test the API configuration"""
        self.ensure_one()
        
        if not self.use_api:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("API Not Enabled"),
                    'message': _("Please enable API usage for this source first."),
                    'sticky': False,
                    'type': 'warning',
                }
            }
            
        if not self.api_key:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("API Key Missing"),
                    'message': _("Please configure an API key for this source."),
                    'sticky': False,
                    'type': 'warning',
                }
            }
            
        from ..services.open_sanctions import OpenSanctions
        service = OpenSanctions(self.env)
        
        result = service.query_api(
            limit=1,  # Just get one record for testing
            source_record=self
        )
        
        if result.get('status') == 'success':
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("API Test Successful"),
                    'message': _("Successfully connected to API at %s") % (self.api_url or 'API endpoint'),
                    'sticky': False,
                    'type': 'success',
                }
            }
        else:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("API Test Failed"),
                    'message': _(result.get('message')),
                    'sticky': False,
                    'type': 'warning',
                }
            }
    
    @api.onchange('domain')
    def _onchange_domain(self):
        """
        Auto-detect OpenSanctions domain and configure defaults
        """
        if self.domain and ('opensanctions.org' in self.domain or 'data.opensanctions.org' in self.domain):
            self.is_opensanctions = True
            
            if not self.name:
                self.name = 'OpenSanctions'
                
            if not self.source_type:
                self.source_type = 'regulatory'
                
            # Set default URL values for OpenSanctions
            if not self.base_url:
                self.base_url = 'https://data.opensanctions.org'
                
            if not self.csv_path:
                self.csv_path = '/datasets/latest/peps/targets.simple.csv'
                
            # Default API settings if using API
            if not self.api_url and self.use_api:
                self.api_url = 'https://api.opensanctions.org'
                
            if not self.api_endpoint and self.use_api:
                self.api_endpoint = '/search/default'
            
    def action_fetch_opensanctions(self):
        """
        Manually trigger OpenSanctions data fetch
        """
        self.ensure_one()
        
        if not self.is_opensanctions:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("Not OpenSanctions"),
                    'message': _("This source is not configured for OpenSanctions."),
                    'sticky': False,
                    'type': 'warning',
                }
            }
            
        # Check if queue_job is installed
        queue_job = self.env['ir.module.module'].sudo().search(
            [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
            
        if queue_job:
            # Queue a job for data fetching
            self.with_delay(priority=10).fetch_opensanctions_job()
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("OpenSanctions Import Started"),
                    'message': _("OpenSanctions data fetch has been queued as a background job."),
                    'sticky': False,
                    'type': 'success',
                }
            }
        else:
            # Use threading as fallback
            thread = threading.Thread(
                target=self._fetch_opensanctions_thread,
                args=(self.id,)
            )
            thread.daemon = True
            thread.start()
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("OpenSanctions Import Started"),
                    'message': _("OpenSanctions data fetch has been started in a background thread."),
                    'sticky': False,
                    'type': 'info',
                }
            }
            
    def fetch_opensanctions_job(self):
        """
        Background job for fetching OpenSanctions data
        Used with queue_job module
        """
        self.ensure_one()
        
        job_id = str(uuid.uuid4())
        
        try:
            # Start job tracking
            self._start_opensanctions_job(job_id)
            
            _logger.info(f"Starting OpenSanctions import job {job_id} for source {self.name}")
            
            # Initialize services
            service = OpenSanctions(self.env)
            importer = OpenSanctionsImporter(self.env)
            
            results = {
                'csv': None,
                'api': None
            }
            
            # Always fetch CSV for initial data
            csv_result = service.fetch_latest_opensanctions_csv(self)
            
            if csv_result.get('status') == 'success':
                # Process CSV file
                csv_process_result = importer.process_csv_file(csv_result.get('path'), self)
                results['csv'] = csv_process_result
            else:
                results['csv'] = csv_result
                
            # Use API if configured
            if self.use_api and self.api_key:
                api_result = service.query_api(
                    entity_type="person", 
                    limit=1000,  # Increase if needed
                    source_record=self
                )
                
                if api_result.get('status') == 'success':
                    # Process API results
                    api_process_result = importer.process_api_results(api_result, self)
                    results['api'] = api_process_result
                else:
                    results['api'] = api_result
            
            # Determine overall status
            if (results['csv'] and results['csv'].get('status') == 'success') or \
               (results['api'] and results['api'].get('status') == 'success'):
                status = 'success'
                message = "OpenSanctions data import completed successfully"
            else:
                status = 'error'
                message = "OpenSanctions data import failed"
                
            # Calculate total records
            records_created = 0
            records_updated = 0
            
            for result_type, result in results.items():
                if result and result.get('status') == 'success':
                    records_created += result.get('records_created', 0)
                    records_updated += result.get('records_updated', 0)
                    
            # Update message with stats
            message = f"{message}: {records_created} records created, {records_updated} updated"
            
            # Update last_update on source
            self.write({
                'last_update': datetime.now()
            })
            
            # Complete job
            self._complete_opensanctions_job(job_id, 'completed', message)
            
            return {
                'status': status,
                'message': message,
                'records_created': records_created,
                'records_updated': records_updated,
                'results': results
            }
            
        except Exception as e:
            _logger.error(f"Error in OpenSanctions job: {str(e)}")
            self._complete_opensanctions_job(job_id, 'failed', str(e))
            
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def _fetch_opensanctions_thread(self, source_id):
        """
        Thread function for fetching OpenSanctions data
        Used when queue_job is not available
        
        Args:
            source_id: ID of the source record
        """
        try:
            # Get a new environment for the thread
            with api.Environment.manage():
                new_cr = self.pool.cursor()
                env = api.Environment(new_cr, self.env.uid, self.env.context)
                
                # Get the source record in the new environment
                source = env['pep.source'].browse(source_id)
                
                # Call the job function
                result = source.fetch_opensanctions_job()
                
                # Log the result
                if result.get('status') == 'success':
                    _logger.info(f"OpenSanctions import completed: {result.get('records_created', 0)} created, {result.get('records_updated', 0)} updated")
                else:
                    _logger.error(f"OpenSanctions import failed: {result.get('message')}")
                    
                # Commit changes
                new_cr.commit()
                new_cr.close()
                
        except Exception as e:
            _logger.error(f"Error in OpenSanctions thread: {str(e)}")
            try:
                # Try to close the cursor
                new_cr.close()
            except:
                pass
                
    def _start_opensanctions_job(self, job_id):
        """
        Start tracking an OpenSanctions job
        
        Args:
            job_id: Unique job ID
        """
        self.ensure_one()
        
        # Store job info in source record
        self.write({
            'job_id': job_id,
            'job_status': 'running',
            'job_started': datetime.now()
        })
        
    def _complete_opensanctions_job(self, job_id, status, message):
        """
        Complete job tracking for an OpenSanctions job
        
        Args:
            job_id: Job ID to complete
            status: Final status (completed or failed)
            message: Result message
        """
        self.ensure_one()
        
        # Only update if this is the current job
        if self.job_id == job_id:
            self.write({
                'job_status': status,
                'job_completed': datetime.now(),
                'import_message': message
            })

# from odoo import _, api, fields, models
# import logging
# import threading
# import uuid
# from datetime import datetime, timedelta

# from ..services.open_sanctions import OpenSanctions
# from ..services.open_sanctions_importer import OpenSanctionsImporter

# _logger = logging.getLogger(__name__)

# class PepSource(models.Model):
#     """
#     Extension of the PEP Source model with additional fields for web scraping and API configuration
#     """
#     _inherit = 'pep.source'
    
#     # General configuration
#     source_format = fields.Selection([
#         ('csv', 'CSV File'),
#         ('api', 'API'),
#         ('both', 'Both CSV and API')
#     ], string='Source Format', default='csv', required=True)
    
#     # CSV file configuration
#     base_url = fields.Char('Base URL', help="Base URL for the sanctions data source")
#     csv_path = fields.Char('CSV Path', help="Path to the CSV file, relative to the base URL")
#     request_headers = fields.Text('Request Headers', help="JSON dictionary of HTTP headers to send with requests")
#     csv_delimiter = fields.Char('CSV Delimiter', default=',', help="Delimiter character for CSV files")
    
#     # API configuration
#     use_api = fields.Boolean('Use API', default=False, help="Use API for data retrieval")
#     api_url = fields.Char('API URL', help="Base URL for the API")
#     api_endpoint = fields.Char('API Endpoint', help="API endpoint path, relative to the API URL")
#     api_auth_format = fields.Char('Auth Format', default='ApiKey {}', 
#                                  help="Format string for API authorization header, use {} for API key placement")
#     api_headers = fields.Text('API Headers', help="JSON dictionary of HTTP headers to send with API requests")
#     api_params = fields.Text('API Parameters', help="JSON dictionary of default query parameters to send with API requests")
#     default_entity_type = fields.Char('Default Entity Type', default='person',
#                                      help="Default entity type to query in the API")
#     api_results_path = fields.Char('Results Path', help="Path to results array in API response, using dot notation")
#     api_entity_filter = fields.Char('Entity Filter', 
#                                    help="Filter for entity types in format 'field:value', e.g. 'schema:Person'")
    
#     # Field mapping
#     field_mapping = fields.Text('Field Mapping', 
#                                help="JSON dictionary mapping source fields to PEP model fields")
    
#     is_opensanctions = fields.Boolean('Is OpenSanctions', help="Check if this source is from OpenSanctions.org")

#     source_format = fields.Selection([
#         ('csv', 'CSV File'),
#         ('api', 'API'),
#         ('both', 'Both CSV and API')
#     ], string='Source Format', default='csv', required=True)

#     job_id = fields.Char(string="Current Job ID", readonly=True)
#     job_status = fields.Selection(
#         [
#             ("pending", "Pending"),
#             ("running", "Running"),
#             ("completed", "Completed"),
#             ("failed", "Failed"),
#         ],
#         string="Job Status",
#         readonly=True,
#     )
#     job_started = fields.Datetime(string="Job Started", readonly=True)
#     job_completed = fields.Datetime(string="Job Completed", readonly=True)
#     import_message = fields.Text(string="Import Message", readonly=True)

#     def action_test_csv_fetch(self):
#         """Test the CSV fetch configuration with dynamic URL discovery for OpenSanctions"""
#         self.ensure_one()
        
#         from ..services.open_sanctions import OpenSanctions
#         service = OpenSanctions(self.env)
        
#         # Use dynamic discovery if this is an OpenSanctions source
#         if self.is_opensanctions:
#             result = service.fetch_latest_opensanctions_csv(self)
#         else:
#             result = service.fetch_csv_file(self)
        
#         if result.get('status') == 'success':
#             # Delete the temporary file
#             if 'path' in result:
#                 try:
#                     import os
#                     os.unlink(result['path'])
#                 except:
#                     pass
                    
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("CSV Fetch Successful"),
#                     'message': _("Successfully downloaded CSV file from %s") % result.get('url'),
#                     'sticky': False,
#                     'type': 'success',
#                 }
#             }
#         else:
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("CSV Fetch Failed"),
#                     'message': _(result.get('message')),
#                     'sticky': False,
#                     'type': 'warning',
#                 }
#             }
    
#     # def action_test_csv_fetch(self):
#     #     """Test the CSV fetch configuration"""
#     #     self.ensure_one()
        
#     #     from ..services.open_sanctions import OpenSanctions
#     #     service = OpenSanctions(self.env)
        
#     #     result = service.fetch_csv_file(self)
        
#     #     if result.get('status') == 'success':
#     #         # Delete the temporary file
#     #         if 'path' in result:
#     #             try:
#     #                 import os
#     #                 os.unlink(result['path'])
#     #             except:
#     #                 pass
                    
#     #         return {
#     #             'type': 'ir.actions.client',
#     #             'tag': 'display_notification',
#     #             'params': {
#     #                 'title': _("CSV Fetch Successful"),
#     #                 'message': _("Successfully downloaded CSV file from %s") % result.get('url'),
#     #                 'sticky': False,
#     #                 'type': 'success',
#     #             }
#     #         }
#     #     else:
#     #         return {
#     #             'type': 'ir.actions.client',
#     #             'tag': 'display_notification',
#     #             'params': {
#     #                 'title': _("CSV Fetch Failed"),
#     #                 'message': _(result.get('message')),
#     #                 'sticky': False,
#     #                 'type': 'warning',
#     #             }
#     #         }
            
#     def action_test_api(self):
#         """Test the API configuration"""
#         self.ensure_one()
        
#         if not self.use_api:
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("API Not Enabled"),
#                     'message': _("Please enable API usage for this source first."),
#                     'sticky': False,
#                     'type': 'warning',
#                 }
#             }
            
#         if not self.api_key:
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("API Key Missing"),
#                     'message': _("Please configure an API key for this source."),
#                     'sticky': False,
#                     'type': 'warning',
#                 }
#             }
            
#         from ..services.open_sanctions import OpenSanctions
#         service = OpenSanctions(self.env)
        
#         result = service.query_api(
#             limit=1,  # Just get one record for testing
#             source_record=self
#         )
        
#         if result.get('status') == 'success':
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("API Test Successful"),
#                     'message': _("Successfully connected to API at %s") % (self.api_url or 'API endpoint'),
#                     'sticky': False,
#                     'type': 'success',
#                 }
#             }
#         else:
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("API Test Failed"),
#                     'message': _(result.get('message')),
#                     'sticky': False,
#                     'type': 'warning',
#                 }
#             }
    
#     @api.onchange('domain')
#     def _onchange_domain(self):
#         """
#         Auto-detect OpenSanctions domain
#         """
#         if self.domain and 'opensanctions.org' in self.domain:
#             self.is_opensanctions = True
#             self.name = self.name or 'OpenSanctions'
#             self.source_type = self.source_type or 'regulatory'
            
#     def action_fetch_opensanctions(self):
#         """
#         Manually trigger OpenSanctions data fetch
#         """
#         self.ensure_one()
        
#         if not self.is_opensanctions:
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("Not OpenSanctions"),
#                     'message': _("This source is not configured for OpenSanctions."),
#                     'sticky': False,
#                     'type': 'warning',
#                 }
#             }
            
#         # Check if queue_job is installed
#         queue_job = self.env['ir.module.module'].sudo().search(
#             [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
            
#         if queue_job:
#             # Queue a job for data fetching
#             self.with_delay(priority=10).fetch_opensanctions_job()
            
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("OpenSanctions Import Started"),
#                     'message': _("OpenSanctions data fetch has been queued as a background job."),
#                     'sticky': False,
#                     'type': 'success',
#                 }
#             }
#         else:
#             # Use threading as fallback
#             thread = threading.Thread(
#                 target=self._fetch_opensanctions_thread,
#                 args=(self.id,)
#             )
#             thread.daemon = True
#             thread.start()
            
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("OpenSanctions Import Started"),
#                     'message': _("OpenSanctions data fetch has been started in a background thread."),
#                     'sticky': False,
#                     'type': 'info',
#                 }
#             }
            
#     def fetch_opensanctions_job(self):
#         """
#         Background job for fetching OpenSanctions data
#         Used with queue_job module
#         """
#         self.ensure_one()
        
#         job_id = str(uuid.uuid4())
        
#         try:
#             # Start job tracking
#             self._start_opensanctions_job(job_id)
            
#             _logger.info(f"Starting OpenSanctions import job {job_id} for source {self.name}")
            
#             # Initialize services
#             service = OpenSanctions(self.env)
#             importer = OpenSanctionsImporter(self.env)
            
#             results = {
#                 'csv': None,
#                 'api': None
#             }
            
#             # Always fetch CSV for initial data
#             csv_result = service.fetch_csv_file(self)
            
#             if csv_result.get('status') == 'success':
#                 # Process CSV file
#                 csv_process_result = importer.process_csv_file(csv_result.get('path'))
#                 results['csv'] = csv_process_result
#             else:
#                 results['csv'] = csv_result
                
#             # Use API if configured
#             if self.use_api and self.api_key:
#                 api_result = service.query_api(
#                     entity_type="person", 
#                     limit=1000,  # Increase if needed
#                     source_record=self
#                 )
                
#                 if api_result.get('status') == 'success':
#                     # Process API results
#                     api_process_result = importer.process_api_results(api_result)
#                     results['api'] = api_process_result
#                 else:
#                     results['api'] = api_result
            
#             # Determine overall status
#             if (results['csv'] and results['csv'].get('status') == 'success') or \
#                (results['api'] and results['api'].get('status') == 'success'):
#                 status = 'success'
#                 message = "OpenSanctions data import completed successfully"
#             else:
#                 status = 'error'
#                 message = "OpenSanctions data import failed"
                
#             # Calculate total records
#             records_created = 0
#             records_updated = 0
            
#             for result_type, result in results.items():
#                 if result and result.get('status') == 'success':
#                     records_created += result.get('records_created', 0)
#                     records_updated += result.get('records_updated', 0)
                    
#             # Update message with stats
#             message = f"{message}: {records_created} records created, {records_updated} updated"
            
#             # Update last_update on source
#             self.write({
#                 'last_update': datetime.now()
#             })
            
#             # Complete job
#             self._complete_opensanctions_job(job_id, 'completed', message)
            
#             return {
#                 'status': status,
#                 'message': message,
#                 'records_created': records_created,
#                 'records_updated': records_updated,
#                 'results': results
#             }
            
#         except Exception as e:
#             _logger.error(f"Error in OpenSanctions job: {str(e)}")
#             self._complete_opensanctions_job(job_id, 'failed', str(e))
            
#             return {
#                 'status': 'error',
#                 'message': str(e)
#             }
    
#     def _fetch_opensanctions_thread(self, source_id):
#         """
#         Thread function for fetching OpenSanctions data
#         Used when queue_job is not available
        
#         Args:
#             source_id: ID of the source record
#         """
#         try:
#             # Get a new environment for the thread
#             with api.Environment.manage():
#                 new_cr = self.pool.cursor()
#                 env = api.Environment(new_cr, self.env.uid, self.env.context)
                
#                 # Get the source record in the new environment
#                 source = env['pep.source'].browse(source_id)
                
#                 # Call the job function
#                 result = source.fetch_opensanctions_job()
                
#                 # Log the result
#                 if result.get('status') == 'success':
#                     _logger.info(f"OpenSanctions import completed: {result.get('records_created', 0)} created, {result.get('records_updated', 0)} updated")
#                 else:
#                     _logger.error(f"OpenSanctions import failed: {result.get('message')}")
                    
#                 # Commit changes
#                 new_cr.commit()
#                 new_cr.close()
                
#         except Exception as e:
#             _logger.error(f"Error in OpenSanctions thread: {str(e)}")
#             try:
#                 # Try to close the cursor
#                 new_cr.close()
#             except:
#                 pass
                
#     def _start_opensanctions_job(self, job_id):
#         """
#         Start tracking an OpenSanctions job
        
#         Args:
#             job_id: Unique job ID
#         """
#         self.ensure_one()
        
#         # Store job info in source record
#         self.write({
#             'job_id': job_id,
#             'job_status': 'running',
#             'job_started': datetime.now()
#         })
        
#     def _complete_opensanctions_job(self, job_id, status, message):
#         """
#         Complete job tracking for an OpenSanctions job
        
#         Args:
#             job_id: Job ID to complete
#             status: Final status (completed or failed)
#             message: Result message
#         """
#         self.ensure_one()
        
#         # Only update if this is the current job
#         if self.job_id == job_id:
#             self.write({
#                 'job_status': status,
#                 'job_completed': datetime.now(),
#                 'import_message': message
#             })


class PepOpenSanctions(models.Model):
    """
    Extension of the PEP model for OpenSanctions integration
    """
    _inherit = 'res.pep'
    
    # Add OpenSanctions specific fields
    opensanctions_id = fields.Char('OpenSanctions ID', index=True)
    opensanctions_url = fields.Char('OpenSanctions URL')
    opensanctions_schema = fields.Char('OpenSanctions Schema')
    opensanctions_dataset = fields.Char('OpenSanctions Dataset')
    opensanctions_last_change = fields.Datetime('Last Changed in OpenSanctions')
    
    def fetch_opensanctions_data(self):
        """
        Button action to fetch OpenSanctions data for all sources
        """
        # Get all active OpenSanctions sources
        sources = self.env['pep.source'].search([
            ('is_opensanctions', '=', True),
            ('active', '=', True)
        ])
        
        if not sources:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("No OpenSanctions Sources"),
                    'message': _("No active OpenSanctions sources found. Please configure at least one source."),
                    'sticky': False,
                    'type': 'warning',
                }
            }
            
        # Check if a job is already running
        running_job = self.env['pep.source'].search([
            ('is_opensanctions', '=', True),
            ('job_status', '=', 'running')
        ], limit=1)
        
        if running_job:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("Job Already Running"),
                    'message': _("An OpenSanctions import job is already running for source '%s'") % running_job.name,
                    'sticky': False,
                    'type': 'warning',
                }
            }
            
        # Check if queue_job is installed
        queue_job = self.env['ir.module.module'].sudo().search(
            [('name', '=', 'queue_job'), ('state', '=', 'installed')], limit=1)
            
        # Start jobs for each source
        for source in sources:
            if queue_job:
                source.with_delay(priority=10).fetch_opensanctions_job()
            else:
                thread = threading.Thread(
                    target=source._fetch_opensanctions_thread,
                    args=(source.id,)
                )
                thread.daemon = True
                thread.start()
                
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': _("OpenSanctions Import Started"),
                'message': _("OpenSanctions data fetch has been started for %d sources.") % len(sources),
                'sticky': False,
                'type': 'success',
            }
        }