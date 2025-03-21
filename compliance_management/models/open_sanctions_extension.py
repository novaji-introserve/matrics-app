# -*- coding: utf-8 -*-
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
        ('api', 'API')
    ], string='Source Format', default='csv', required=True, 
       help="Choose one source format - either CSV or API. Both formats cannot be used simultaneously.")
    
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
    last_update = fields.Datetime(string="Last Update", readonly=True)

    @api.onchange('source_format')
    def _onchange_source_format(self):
        """Handle source format change to ensure CSV and API aren't used together"""
        if self.source_format == 'csv':
            self.use_api = False
        elif self.source_format == 'api':
            self.use_api = True

    @api.onchange('use_api')
    def _onchange_use_api(self):
        """Handle API usage change to update source format"""
        if self.use_api:
            self.source_format = 'api'
        else:
            self.source_format = 'csv'

    def action_test_csv_fetch(self):
        """Test the CSV fetch configuration with dynamic URL discovery for OpenSanctions"""
        self.ensure_one()
        
        if self.source_format != 'csv':
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("Invalid Source Format"),
                    'message': _("This source is not configured for CSV import."),
                    'sticky': False,
                    'type': 'warning',
                }
            }
        
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
                except Exception as e:
                    _logger.error(f"Error deleting temporary file: {str(e)}")
                    
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
    
    def action_fetch_csv(self):
        """
        Manually fetch CSV data from source and create an import job
        """
        self.ensure_one()
        
        if self.source_format != 'csv':
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("Invalid Source Format"),
                    'message': _("This source is not configured for CSV import."),
                    'sticky': False,
                    'type': 'warning',
                }
            }
        
        try:
            # Check for existing running jobs
            existing_jobs = self.env['opensanctions.job.queue'].search([
                ('source_id', '=', self.id),
                ('state', 'in', ['running', 'pending']),
                ('job_type', '=', 'csv'),
                ('is_chunk', '=', False)
            ])
            
            if existing_jobs:
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': _("Job Already Running"),
                        'message': _("There is already a CSV import job running or pending for this source."),
                        'sticky': False,
                        'type': 'warning',
                    }
                }
            
            # Initialize OpenSanctions service
            from ..services.open_sanctions import OpenSanctions
            service = OpenSanctions(self.env)
            
            # Get the CSV file - with dynamic discovery for OpenSanctions
            if self.is_opensanctions:
                _logger.info(f"Fetching OpenSanctions CSV for {self.name}")
                result = service.fetch_latest_opensanctions_csv(self)
            else:
                _logger.info(f"Fetching CSV for {self.name}")
                result = service.fetch_csv_file(self)
            
            if result.get('status') == 'success':
                file_path = result.get('path')
                _logger.info(f"CSV file downloaded to: {file_path}")
                
                # Create a job to process the file
                job_queue = self.env['opensanctions.job.queue']
                job = job_queue.create({
                    'name': f"CSV Import - {self.name}",
                    'source_id': self.id,
                    'job_type': 'csv',
                    'state': 'pending',
                    'file_path': file_path,
                    'priority': 15 if self.is_opensanctions else 10,
                    'batch_size': 500  # Default batch size
                })
                
                # Start job immediately
                job.action_run_job()
                
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': _("CSV Import Started"),
                        'message': _("CSV file downloaded and import job started."),
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
        
        except Exception as e:
            _logger.error(f"Error in action_fetch_csv: {str(e)}")
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("CSV Import Failed"),
                    'message': _(f"Error: {str(e)}"),
                    'sticky': False,
                    'type': 'error',
                }
            }

    def action_test_api(self):
        """Test the API configuration"""
        self.ensure_one()
        
        if self.source_format != 'api':
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("Invalid Source Format"),
                    'message': _("This source is not configured for API usage."),
                    'sticky': False,
                    'type': 'warning',
                }
            }
            
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
    
    def action_fetch_api(self):
        """
        Manually fetch data from API and create an import job
        """
        self.ensure_one()
        
        if self.source_format != 'api':
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("Invalid Source Format"),
                    'message': _("This source is not configured for API usage."),
                    'sticky': False,
                    'type': 'warning',
                }
            }
            
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
        
        try:
            # Check for existing running jobs
            existing_jobs = self.env['opensanctions.job.queue'].search([
                ('source_id', '=', self.id),
                ('state', 'in', ['running', 'pending']),
                ('job_type', '=', 'api'),
                ('is_chunk', '=', False)
            ])
            
            if existing_jobs:
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': _("Job Already Running"),
                        'message': _("There is already an API import job running or pending for this source."),
                        'sticky': False,
                        'type': 'warning',
                    }
                }
            
            # Create a job for API import
            job_queue = self.env['opensanctions.job.queue']
            job = job_queue.create({
                'name': f"API Import - {self.name}",
                'source_id': self.id,
                'job_type': 'api',
                'state': 'pending',
                'priority': 15 if self.is_opensanctions else 10,
                'api_limit': 1000  # Default API limit
            })
            
            # Start job immediately
            job.action_run_job()
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("API Import Started"),
                    'message': _("API import job started."),
                    'sticky': False,
                    'type': 'success',
                }
            }
        
        except Exception as e:
            _logger.error(f"Error in action_fetch_api: {str(e)}")
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("API Import Failed"),
                    'message': _(f"Error: {str(e)}"),
                    'sticky': False,
                    'type': 'error',
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
            
        # Check which method to use based on source format
        if self.source_format == 'csv':
            return self.action_fetch_csv()
        elif self.source_format == 'api' and self.use_api and self.api_key:
            return self.action_fetch_api()
        else:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("No Data Source"),
                    'message': _("No valid data source (CSV or API) is configured for this source."),
                    'sticky': False,
                    'type': 'warning',
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
            
            # Create a job in the job queue based on format
            job_queue = self.env['opensanctions.job.queue']
            
            if self.source_format == 'csv':
                job = job_queue.create({
                    'name': f"OpenSanctions CSV Import - {self.name}",
                    'source_id': self.id,
                    'job_type': 'csv',
                    'state': 'pending',
                    'priority': 20,  # High priority for OpenSanctions jobs
                    'batch_size': 500  # Default batch size
                })
            elif self.source_format == 'api' and self.use_api and self.api_key:
                job = job_queue.create({
                    'name': f"OpenSanctions API Import - {self.name}",
                    'source_id': self.id,
                    'job_type': 'api',
                    'state': 'pending',
                    'priority': 20,  # High priority for OpenSanctions jobs
                    'api_limit': 1000  # Default API limit
                })
            else:
                self._complete_opensanctions_job(job_id, 'failed', 'No valid import method configured')
                return {
                    'status': 'error',
                    'message': 'No valid import method configured'
                }
            
            # Run the job
            result = job.process_job()
            
            # Complete job tracking
            status = 'completed' if result.get('status') != 'error' else 'failed'
            message = result.get('message', 'Import completed')
            
            self._complete_opensanctions_job(job_id, status, message)
            
            return result
            
        except Exception as e:
            _logger.error(f"Error in OpenSanctions job: {str(e)}")
            self._complete_opensanctions_job(job_id, 'failed', str(e))
            
            return {
                'status': 'error',
                'message': str(e)
            }
    
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
                'import_message': message,
                'last_update': datetime.now() if status == 'completed' else self.last_update
            })

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
#                 except Exception as e:
#                     _logger.error(f"Error deleting temporary file: {str(e)}")
                    
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
    
#     def action_fetch_csv(self):
#         """
#         Manually fetch CSV data from source and create an import job
#         """
#         self.ensure_one()
        
#         try:
#             # Initialize OpenSanctions service
#             from ..services.open_sanctions import OpenSanctions
#             service = OpenSanctions(self.env)
            
#             # Get the CSV file - with dynamic discovery for OpenSanctions
#             if self.is_opensanctions:
#                 _logger.info(f"Fetching OpenSanctions CSV for {self.name}")
#                 result = service.fetch_latest_opensanctions_csv(self)
#             else:
#                 _logger.info(f"Fetching CSV for {self.name}")
#                 result = service.fetch_csv_file(self)
            
#             if result.get('status') == 'success':
#                 file_path = result.get('path')
#                 _logger.info(f"CSV file downloaded to: {file_path}")
                
#                 # Create a job to process the file
#                 job_queue = self.env['opensanctions.job.queue']
#                 job = job_queue.create({
#                     'name': f"CSV Import - {self.name}",
#                     'source_id': self.id,
#                     'job_type': 'csv',
#                     'state': 'pending',
#                     'file_path': file_path,
#                     'priority': 15 if self.is_opensanctions else 10,
#                     'batch_size': 500  # Default batch size
#                 })
                
#                 # Start job immediately
#                 job.action_run_job()
                
#                 return {
#                     'type': 'ir.actions.client',
#                     'tag': 'display_notification',
#                     'params': {
#                         'title': _("CSV Import Started"),
#                         'message': _("CSV file downloaded and import job started."),
#                         'sticky': False,
#                         'type': 'success',
#                     }
#                 }
#             else:
#                 return {
#                     'type': 'ir.actions.client',
#                     'tag': 'display_notification',
#                     'params': {
#                         'title': _("CSV Fetch Failed"),
#                         'message': _(result.get('message')),
#                         'sticky': False,
#                         'type': 'warning',
#                     }
#                 }
        
#         except Exception as e:
#             _logger.error(f"Error in action_fetch_csv: {str(e)}")
            
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("CSV Import Failed"),
#                     'message': _(f"Error: {str(e)}"),
#                     'sticky': False,
#                     'type': 'error',
#                 }
#             }

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
    
#     def action_fetch_api(self):
#         """
#         Manually fetch data from API and create an import job
#         """
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
        
#         try:
#             # Create a job for API import
#             job_queue = self.env['opensanctions.job.queue']
#             job = job_queue.create({
#                 'name': f"API Import - {self.name}",
#                 'source_id': self.id,
#                 'job_type': 'api',
#                 'state': 'pending',
#                 'priority': 15 if self.is_opensanctions else 10,
#                 'api_limit': 1000  # Default API limit
#             })
            
#             # Start job immediately
#             job.action_run_job()
            
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("API Import Started"),
#                     'message': _("API import job started."),
#                     'sticky': False,
#                     'type': 'success',
#                 }
#             }
        
#         except Exception as e:
#             _logger.error(f"Error in action_fetch_api: {str(e)}")
            
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("API Import Failed"),
#                     'message': _(f"Error: {str(e)}"),
#                     'sticky': False,
#                     'type': 'error',
#                 }
#             }
    
#     @api.onchange('domain')
#     def _onchange_domain(self):
#         """
#         Auto-detect OpenSanctions domain and configure defaults
#         """
#         if self.domain and ('opensanctions.org' in self.domain or 'data.opensanctions.org' in self.domain):
#             self.is_opensanctions = True
            
#             if not self.name:
#                 self.name = 'OpenSanctions'
                
#             if not self.source_type:
#                 self.source_type = 'regulatory'
                
#             # Set default URL values for OpenSanctions
#             if not self.base_url:
#                 self.base_url = 'https://data.opensanctions.org'
                
#             if not self.csv_path:
#                 self.csv_path = '/datasets/latest/peps/targets.simple.csv'
                
#             # Default API settings if using API
#             if not self.api_url and self.use_api:
#                 self.api_url = 'https://api.opensanctions.org'
                
#             if not self.api_endpoint and self.use_api:
#                 self.api_endpoint = '/search/default'
            
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
            
#         # Check if both CSV and API should be fetched
#         fetch_csv = self.source_format in ['csv', 'both']
#         fetch_api = self.source_format in ['api', 'both'] and self.use_api and self.api_key
        
#         if not fetch_csv and not fetch_api:
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _("No Data Source"),
#                     'message': _("No data source (CSV or API) is configured for this source."),
#                     'sticky': False,
#                     'type': 'warning',
#                 }
#             }
        
#         # Create a job for the requested data sources
#         job_type = 'both' if fetch_csv and fetch_api else 'csv' if fetch_csv else 'api'
        
#         job_queue = self.env['opensanctions.job.queue']
#         job = job_queue.create({
#             'name': f"OpenSanctions Import - {self.name}",
#             'source_id': self.id,
#             'job_type': job_type,
#             'state': 'pending',
#             'priority': 20,  # High priority for manual OpenSanctions jobs
#             'batch_size': 500,  # Default batch size
#             'api_limit': 1000  # Default API limit
#         })
        
#         # Start job immediately
#         job.action_run_job()
        
#         return {
#             'type': 'ir.actions.client',
#             'tag': 'display_notification',
#             'params': {
#                 'title': _("OpenSanctions Import Started"),
#                 'message': _("OpenSanctions import job started."),
#                 'sticky': False,
#                 'type': 'success',
#             }
#         }

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
            
#             # Create a job in the job queue
#             job_queue = self.env['opensanctions.job.queue']
#             job = job_queue.create({
#                 'name': f"OpenSanctions Import - {self.name}",
#                 'source_id': self.id,
#                 'job_type': 'both',
#                 'state': 'pending',
#                 'priority': 20,  # High priority for OpenSanctions jobs
#                 'batch_size': 500,  # Default batch size
#                 'api_limit': 1000  # Default API limit
#             })
            
#             # Run the job
#             result = job.process_job()
            
#             # Complete job tracking
#             status = 'completed' if result.get('status') == 'success' else 'failed'
#             message = result.get('message', 'Import completed')
            
#             self._complete_opensanctions_job(job_id, status, message)
            
#             return result
            
#         except Exception as e:
#             _logger.error(f"Error in OpenSanctions job: {str(e)}")
#             self._complete_opensanctions_job(job_id, 'failed', str(e))
            
#             return {
#                 'status': 'error',
#                 'message': str(e)
#             }
    
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