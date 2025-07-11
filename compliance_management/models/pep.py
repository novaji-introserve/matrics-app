from odoo import _, api, fields, models
import threading
import logging
import traceback
import json
from datetime import datetime, timedelta
import uuid

from ..services.data_processor import DataProcessor
from ..services.pep_importer import PepImporter
from ..services.pep_service import PepService

_logger = logging.getLogger(__name__)

class Pep(models.Model):
    _name = 'res.pep'
    _description = 'PEP List'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    _sql_constraints = [
        ('uniq_pep_identifier', 'unique(unique_identifier)',
         "Unique Identifier already exists. Value must be unique!"),
    ]
    _order="surname,first_name"
    
    narration = fields.Html(string='Narration')
    lastmodifiedon = fields.Char(string="Last Modified On")
    lastmodifiedbyemail = fields.Char(string="Last Modified by Email")
    unique_identifier = fields.Char(string="Unique Identifier", index=True,required=True)
    surname = fields.Char(string="Surname",tracking=True,required=True,index=True)
    first_name = fields.Char(string="First Name",tracking=True,required=True,index=True)
    middle_name = fields.Char(string="Middle Name")
    title = fields.Char(string="Title")
    aka = fields.Char(string="Aka")
    sex = fields.Char(string="Sex")
    date_of_birth = fields.Char(string="Date Of Birth")
    present_position = fields.Char(string="Present Position")
    previous_position = fields.Char(string="Previous Position")
    pep_classification = fields.Char(string="Pep Classification")
    official_address = fields.Char(string="Official Address")
    profession = fields.Char(string="Profession")
    residential_address = fields.Char(string="Residential Address")
    state_of_origin = fields.Char(string="State Of Origin")
    spouse = fields.Char(string="Spouse")
    children = fields.Char(string="Children", index=True)
    sibling = fields.Char(string="Sibling")
    parents = fields.Char(string="Parents")
    mothers_maden_name = fields.Char(string="Mothers Maiden Name")
    associates__business_political_social_ = fields.Char(string="Associates  Business Political Social")
    bankers = fields.Char(string="Bankers")
    account_details = fields.Char(string="Account Details")
    place_of_birth = fields.Char(string="Place Of Birth")
    press_report = fields.Text(string="Press Report")
    date_report = fields.Char(string="Date Report")
    additional_info = fields.Html(string="Additional Info")
    email = fields.Char(string="Email", index=True)
    remarks = fields.Char(string="Remarks")
    name = fields.Char(string="Name", index=True)
    status = fields.Char(string="Status")
    business_interest = fields.Char(string="Business Interest")
    age = fields.Char(string="Age")
    associate_business_politics = fields.Char(string="Associate Business Politics")
    pob = fields.Char(string="Place of Birth")
    createdby = fields.Char(string="Created By")
    createdon = fields.Char(string="Created On")
    createdbyemail = fields.Char(string="Created by Email")
    lastmodifiedby = fields.Char(string="Last Modified By")
    religion = fields.Text(string='Religion')
    citizenship = fields.Char(string='Citizenship')
    education = fields.Text(string='Education')
    career_history = fields.Text(string='Career History')
    source = fields.Char(string="Source", help="Source of the PEP information")

    # Internal tracking fields
    last_fetch_date = fields.Datetime(string="Last Fetch Date", readonly=True)
    import_status = fields.Selection(
        [
            ("new", "New"),
            ("imported", "Imported"),
            ("updated", "Updated"),
            ("error", "Error"),
        ],
        string="Import Status",
        default="new",
        readonly=True,
    )
    import_message = fields.Text(string="Import Message", readonly=True)

    # Job tracking fields
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
    
    biography_added = fields.Boolean(string="Biography Added", default=False, 
                               help="Indicates if biography data was successfully retrieved")
    sanctions_added = fields.Boolean(string="Sanctions Added", default=False,
                                help="Indicates if sanctions data was successfully retrieved")
    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')


    def init(self):
        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS res_pep_id_idx ON res_pep (id)")

    @api.model
    def create(self, vals):
        if "first_name" in vals and "surname" in vals:
            vals["name"] = self._get_name(vals)
        record = super(Pep, self).create(vals)
        return record

    def write(self, vals):
        if "first_name" in vals or "surname" in vals:
            if "first_name" not in vals or "surname" not in vals:
                # Get missing values from existing record
                for rec in self:
                    if "first_name" not in vals:
                        vals["first_name"] = rec.first_name
                    if "surname" not in vals:
                        vals["surname"] = rec.surname
                    break
            vals["name"] = self._get_name(vals)
        record = super(Pep, self).write(vals)
        return record

    def _get_name(self, vals):
        """Get full name from first name and surname"""
        return f"{vals['first_name']} {vals['surname']}".strip()
    
    def _find_person_thread(self, job_id):
        """
        Thread implementation for finding person information
        This is used when queue_job is not installed
        """
        try:
            # Get a new environment for the thread
            with api.Environment.manage():
                new_cr = self.pool.cursor()
                env = api.Environment(new_cr, self.env.uid, self.env.context)
                
                # Get the record in the new environment
                record = env['res.pep'].browse(self.id)
                
                try:
                    # Execute the search
                    record._execute_find_person(job_id)
                    # Commit changes
                    new_cr.commit()
                except Exception as e:
                    new_cr.rollback()
                    # Log error and update status
                    _logger.error(f"Error in find person thread: {str(e)}")
                    _logger.error(traceback.format_exc())
                    record.write({
                        'job_status': 'failed',
                        'job_completed': fields.Datetime.now(),
                        'import_status': 'error',
                        'import_message': str(e)
                    })
                    new_cr.commit()
                finally:
                    new_cr.close()
        except Exception as e:
            _logger.error(f"Thread error in find person: {str(e)}")
            _logger.error(traceback.format_exc())
    
    def find_person_background(self, job_id):
        """
        Background job to find person information
        This is called by the job queue
        """
        try:
            return self._execute_find_person(job_id)
        except Exception as e:
            _logger.error(f"Error in find person job: {str(e)}")
            _logger.error(traceback.format_exc())
            
            # Update job status on error
            self.write({
                'job_status': 'failed',
                'job_completed': fields.Datetime.now(),
                'import_status': 'error',
                'import_message': str(e)
            })
            
            return {"error": str(e)}
    
    @api.model
    def check_job_status(self, record_id):
        """
        Check job status method for proper Odoo module
        
        Args:
            record_id: ID of the record to check
            
        Returns:
            dict: Current job status information
        """
        try:
            # Ensure record_id is an integer
            if isinstance(record_id, str):
                try:
                    record_id = int(record_id)
                except ValueError:
                    return {"status": "error", "message": "Invalid record ID"}
            
            record = self.browse(record_id)
            if not record.exists():
                return {"status": "error", "message": "Record not found"}
                
            # Get current status
            job_status = record.job_status or "none"
            
            # Check for stuck jobs
            if job_status == "running":
                # Check if the job has completed data
                if record.narration and record.last_fetch_date:
                    _logger.info(f"Job for {record.name} appears to be completed based on data")
                    
                    # Force update the status
                    record.write({
                        'job_status': 'completed',
                        'job_completed': fields.Datetime.now(),
                        'import_message': "Biography lookup completed successfully"
                    })
                    
                    job_status = "completed"
                
                # Check if job has been running for too long (more than 30 seconds)
                elif record.job_started:
                    started = fields.Datetime.from_string(record.job_started)
                    now = datetime.now()
                    
                    if (now - started).total_seconds() > 30:
                        _logger.info(f"Job for {record.name} has been running for over 30 seconds")
                        
                        # Check for data that would indicate completion
                        if record.narration or record.last_fetch_date:
                            record.write({
                                'job_status': 'completed',
                                'job_completed': fields.Datetime.now(),
                                'import_message': "Biography lookup completed successfully"
                            })
                            
                            job_status = "completed"
            
            # Get the message based on status
            message = record.import_message or ""
            if not message:
                if job_status == "completed":
                    message = "Biography lookup completed successfully"
                elif job_status == "failed":
                    message = "Biography lookup failed"
            
            return {
                "status": job_status,
                "message": message,
                "record_id": record_id
            }
            
        except Exception as e:
            _logger.error(f"Error in check_job_status: {str(e)}")
            _logger.error(traceback.format_exc())
            return {
                "status": "error",
                "message": str(e)
            }

    @api.depends("first_name", "surname")
    def action_find_person(self):
        """
        Queue a background job to find information about a person using OpenSanctions Entity API
        """
        self.ensure_one()

        # Generate a unique job ID
        job_id = str(uuid.uuid4())
        
        # Update job status to indicate processing has started
        self.write({
            'job_id': job_id,
            'job_status': 'running',
            'job_started': fields.Datetime.now(),
            'job_completed': False
        })
        
        # Check if queue_job is installed
        queue_job = self.env['ir.module.module'].sudo().search(
            [('name', '=', 'queue_job'), ('state', '=', 'installed')])
        
        if queue_job:
            # Queue the job with the job_id
            self.with_delay(priority=10, identity_key=f"find_person_{self.id}").find_person_background(job_id)
            
            # Return client action that doesn't refresh the page
            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": _("Biography Lookup"),
                    "message": _("Person biography lookup has started and will update shortly."),
                    "sticky": False,
                    "type": "info",
                    "next": {"type": "ir.actions.act_window_close"},
                },
            }
        else:
            # Fallback to threading if queue_job is not installed
            thread = threading.Thread(
                target=self._find_person_thread, 
                args=(job_id,)
            )
            thread.daemon = True
            thread.start()
            
            # Return client action that doesn't refresh the page
            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": _("Biography Lookup"),
                    "message": _("Person biography lookup has been started in the background."),
                    "sticky": False,
                    "type": "info",
                    "next": {"type": "ir.actions.act_window_close"},
                },
            }
            
    def _execute_find_person(self, job_id):
        """
        Execute the person information lookup with enhanced status handling
        This is used by both the thread and job queue implementations
        """
        try:
            # Check if this is still the current job
            if self.job_id != job_id:
                _logger.warning(f"Job {job_id} is no longer current for {self.name}")
                return {"status": "cancelled"}
            
            # Create a PepService instance
            service = PepService(self.env)
            person_data = {}
            biography_found = False
            sanctions_found = False
            
            # Step 1: Find biography using Gemini
            try:
                narration_html = service.find_person_biography(self.first_name, self.surname)
                if narration_html:
                    self.write({"narration": narration_html, "biography_added": True})
                    biography_found = True
            except Exception as bio_error:
                _logger.error(f"Error fetching biography: {str(bio_error)}")
                _logger.error(traceback.format_exc())
                # Continue to sanctions lookup even if biography fails
            
            # Step 2: Query OpenSanctions API with enhanced entity lookup
            try:
                result = service.query_sanctions_service(self.first_name, self.surname)
                
                if result:
                    # Format the data with relationship information
                    person_data = service.format_person_data(result)
                    _logger.info(f"Formatted person data from OpenSanctions: {json.dumps(person_data, indent=2)}")
                    
                    # Update the record
                    if person_data:
                        person_data['sanctions_added'] = True
                        self.write(person_data)
                        sanctions_found = True
            except Exception as sanctions_error:
                _logger.error(f"Error fetching sanctions: {str(sanctions_error)}")
                _logger.error(traceback.format_exc())
                
                # If neither biography nor sanctions were found, raise the error
                if not biography_found:
                    raise sanctions_error
            
            # Generate appropriate message based on what was found
            message = "Information lookup completed"
            if biography_found and sanctions_found:
                message = "Successfully retrieved biography and sanctions information"
            elif biography_found:
                message = "Retrieved biography information only"
            elif sanctions_found:
                message = "Retrieved sanctions information only"
            
            # Commit CR before making notifications
            self.env.cr.commit()
            
            # Update job status on success - ensure job_completed is set
            completion_time = fields.Datetime.now()
            self.write({
                'job_status': 'completed',
                'job_completed': completion_time,
                'import_status': 'updated',
                'import_message': message,
                'last_fetch_date': completion_time
            })
            
            # Force another commit to ensure status is saved
            self.env.cr.commit()
            
            # Log the action
            _logger.info(f"Person information lookup completed for {self.name}: {message}")
            
            # Wait briefly to ensure status is saved
            import time
            time.sleep(0.5)
            
            # Trigger UI update via bus.bus if available - try multiple times
            for attempt in range(1):
                try:
                    self._notify_person_update(True, message)
                    _logger.info(f"Notification attempt {attempt+1} completed")
                    time.sleep(0.5)  # Wait between attempts
                except Exception as e:
                    _logger.error(f"Notification attempt {attempt+1} failed: {str(e)}")
            
            return {
                "status": "success",
                "message": message,
                "biography_found": biography_found,
                "sanctions_found": sanctions_found,
                "data": person_data
            }
            
        except Exception as e:
            _logger.error(f"Error executing find person: {str(e)}")
            _logger.error(traceback.format_exc())
            
            # Update job status on error
            self.write({
                'job_status': 'failed',
                'job_completed': fields.Datetime.now(),
                'import_status': 'error',
                'import_message': str(e)
            })
            
            # Trigger UI update via bus.bus if available
            self._notify_person_update(False, f"Error finding person information: {str(e)}")
            
            return {"status": "error", "error": str(e)}

    def _notify_person_update(self, success, message):
        """
        Send a notification via the bus system to update the UI without refresh
        Uses multiple methods to ensure the notification is delivered
        """
        try:
            # Check if the bus module is installed and available
            if 'bus.bus' in self.env:
                # Get all active users that might be viewing this record
                active_users = self.env['res.users'].search([('active', '=', True)])
                
                # Prepare the notification payload
                notification_payload = {
                    'record_id': self.id,
                    'success': success,
                    'message': message
                }
                
                # Log what we're about to do
                _logger.info(f"Sending update notification for {self.name} to {len(active_users)} users")
                
                bus = self.env['bus.bus']
                sent = False
                
                # Try multiple notification methods, from newest to oldest Odoo versions
                
                # Method 1: Odoo 15+ with _sendone (partner id)
                if hasattr(bus, '_sendone'):
                    try:
                        # Send to current user's partner channel
                        channel = f"res.partner/{self.env.user.partner_id.id}"
                        bus._sendone(channel, 'person_update', notification_payload)
                        _logger.info(f"Sent bus notification via _sendone to {channel}")
                        
                        # Also broadcast to all potential users viewing this record
                        for user in active_users:
                            if user.partner_id and user.id != self.env.user.id:
                                channel = f"res.partner/{user.partner_id.id}"
                                bus._sendone(channel, 'person_update', notification_payload)
                        sent = True
                    except Exception as e:
                        _logger.warning(f"_sendone notification failed: {str(e)}")
                
                # Method 2: Classic sendone method
                if not sent and hasattr(bus, 'sendone'):
                    try:
                        # Send to current user
                        bus.sendone(
                            f"res.partner_{self.env.user.partner_id.id}",
                            {'type': 'person_update', 'payload': notification_payload}
                        )
                        _logger.info(f"Sent bus notification via sendone")
                        
                        # Also broadcast to all potential users viewing this record
                        for user in active_users:
                            if user.partner_id and user.id != self.env.user.id:
                                bus.sendone(
                                    f"res.partner_{user.partner_id.id}",
                                    {'type': 'person_update', 'payload': notification_payload}
                                )
                        sent = True
                    except Exception as e:
                        _logger.warning(f"sendone notification failed: {str(e)}")
                
                # Method 3: Using sendmany array format
                if not sent and hasattr(bus, 'sendmany'):
                    try:
                        # Prepare channels list for all users
                        channels = []
                        # Current user first
                        channels.append([
                            (self._cr.dbname, 'res.partner', self.env.user.partner_id.id), 
                            {'type': 'person_update', 'payload': notification_payload}
                        ])
                        
                        # Other active users
                        for user in active_users:
                            if user.partner_id and user.id != self.env.user.id:
                                channels.append([
                                    (self._cr.dbname, 'res.partner', user.partner_id.id),
                                    {'type': 'person_update', 'payload': notification_payload}
                                ])
                        
                        bus.sendmany(channels)
                        _logger.info(f"Sent bus notification via sendmany to {len(channels)} channels")
                        sent = True
                    except Exception as e:
                        _logger.warning(f"sendmany notification failed: {str(e)}")
                
                # If all bus methods failed, fall back to message posting
                if not sent:
                    _logger.warning("All bus notification methods failed, using message posting instead")
                    self._post_message_fallback(success, message)
                    
            else:
                # Fallback if bus module is not available
                _logger.warning("Bus module not available, using message posting instead")
                self._post_message_fallback(success, message)
                
        except Exception as e:
            _logger.error(f"Error sending notification: {str(e)}")
            # Fall back to message posting on error
            self._post_message_fallback(success, message)

    def _post_message_fallback(self, success, message):
        """Post a message as a fallback when bus notifications fail"""
        try:
            self.message_post(
                body=f"Biography lookup {'completed successfully' if success else 'failed'}: {message}",
                subject="Person Information Update"
            )
            _logger.info(f"Posted message for {self.name} as fallback notification")
        except Exception as e:
            _logger.error(f"Failed to post message fallback: {str(e)}")

    def _is_job_running(self):
        """
        Check if a PEP import job is currently running

        Returns:
            bool: True if a job is running, False otherwise
        """
        config = self.env["ir.config_parameter"].sudo()
        job_id = config.get_param("compliance_management.pep_import_job_id")
        job_status = config.get_param("compliance_management.pep_import_job_status")
        job_started = config.get_param("compliance_management.pep_import_job_started")

        if job_id and job_status == "running" and job_started:
            # Check if job started less than 1 hour ago (to handle stalled jobs)
            try:
                started_dt = fields.Datetime.from_string(job_started)
                now = fields.Datetime.now()
                if now - started_dt < timedelta(hours=1):
                    return True
            except:
                pass

        return False

    def _start_job_tracking(self):
        """
        Start tracking a new PEP import job

        Returns:
            str: Job ID
        """
        config = self.env["ir.config_parameter"].sudo()

        # Generate a unique job ID
        job_id = str(uuid.uuid4())

        # Store job info
        config.set_param("compliance_management.pep_import_job_id", job_id)
        config.set_param("compliance_management.pep_import_job_status", "running")
        config.set_param(
            "compliance_management.pep_import_job_started", fields.Datetime.now()
        )
        config.set_param("compliance_management.pep_import_job_message", "Job started")

        return job_id

    def _complete_job_tracking(self, job_id, status, message):
        """
        Complete job tracking for a PEP import job

        Args:
            job_id: Job ID to complete
            status: Final status (completed or failed)
            message: Result message
        """
        config = self.env["ir.config_parameter"].sudo()
        current_job_id = config.get_param("compliance_management.pep_import_job_id")

        # Only update if this is the current job
        if current_job_id == job_id:
            config.set_param("compliance_management.pep_import_job_status", status)
            config.set_param(
                "compliance_management.pep_import_job_completed", fields.Datetime.now()
            )
            config.set_param("compliance_management.pep_import_job_message", message)

    def fetch_global_pep_list(self):
        """
        Start background jobs to fetch PEP data with job tracking
        """
        # Check if a job is already running
        if self._is_job_running():
            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": _("PEP Import Already Running"),
                    "message": _(
                        "A PEP data import job is already in progress. Please wait for it to complete."
                    ),
                    "sticky": False,
                    "type": "warning",
                },
            }

        # Start job tracking
        job_id = self._start_job_tracking()

        # Check if queue_job is installed
        queue_job = (
            self.env["ir.module.module"]
            .sudo()
            .search([("name", "=", "queue_job"), ("state", "=", "installed")])
        )

        if queue_job:
            # Queue a single job for the entire import process
            self.with_delay(priority=10).fetch_global_pep_list_job(job_id)

            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": _("PEP Import Started"),
                    "message": _(
                        "PEP data fetch has been started as a background job."
                    ),
                    "sticky": False,
                    "type": "success",
                },
            }
        else:
            # Fallback to threading
            thread = threading.Thread(
                target=self._fetch_global_pep_list_thread, args=(job_id,)
            )
            thread.daemon = True
            thread.start()

            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": _("PEP Import Started"),
                    "message": _(
                        "PEP data fetch has been started in a background thread."
                    ),
                    "sticky": False,
                    "type": "info",
                },
            }

    def fetch_source_pep_list(self, source_name, job_id):
        """
        Fetch PEP data for a specific source

        Args:
            source_name: Name of the source to fetch
            job_id: Job ID for tracking

        Returns:
            dict: Results of the operation
        """
        service = PepService(self.env)

        # Map source names to scraper methods
        source_methods = {
            "eu_sanctions": service.scraper.fetch_eu_sanctions,
            "un_sanctions": service.scraper.fetch_un_sanctions,
            "ofac_sanctions": service.scraper.fetch_ofac_sanctions,
            "uk_sanctions": service.scraper.fetch_uk_sanctions,
        }

        if source_name not in source_methods:
            _logger.error(f"Unknown source: {source_name}")
            return {
                "status": "error",
                "message": f"Unknown source: {source_name}",
                "files_processed": 0,
            }

        try:
            # Get files for this source
            _logger.info(f"Fetching files for {source_name}")

            # Use a longer timeout for fetching
            old_timeout = service.scraper.timeout
            service.scraper.timeout = 60

            files = source_methods[source_name]()

            # Restore original timeout
            service.scraper.timeout = old_timeout

            if not files:
                _logger.warning(f"No files found for {source_name}")
                return {
                    "status": "warning",
                    "message": f"No files found for {source_name}",
                    "files_processed": 0,
                }

            _logger.info(f"Found {len(files)} files for {source_name}")

            # Filter files by priority
            selected_files = []

            # Group files by type
            files_by_type = {}
            for file_info in files:
                file_type = file_info["type"]
                if file_type not in files_by_type:
                    files_by_type[file_type] = []
                files_by_type[file_type].append(file_info)

            # File type priority
            file_type_priority = ["csv", "xlsx", "xls", "ods", "pdf", "xml", "txt"]

            # Select files by priority
            for file_type in file_type_priority:
                if file_type in files_by_type and files_by_type[file_type]:
                    # Use the first file of highest priority type
                    selected_files.append(files_by_type[file_type][0])
                    _logger.info(f"Selected {file_type} file for {source_name}")
                    break

            # If no files were selected by priority, use the first file
            if not selected_files and files:
                selected_files = [files[0]]
                _logger.info(
                    f"Selected {files[0]['type']} file for {source_name} (fallback)"
                )

            # Process each file
            processed_count = 0
            results = []

            for file_info in selected_files:
                try:
                    # Process file
                    _logger.info(f"Processing file: {file_info['path']}")

                    # Create new importer and processor for this file
                    importer = PepImporter(self.env)
                    processor = DataProcessor(self.env)

                    # Process the file
                    result = importer.process_file(file_info, processor)
                    result["file"] = file_info["path"]
                    result["source"] = source_name

                    results.append(result)
                    processed_count += 1
                except Exception as e:
                    _logger.error(
                        f"Error processing file {file_info['path']}: {str(e)}"
                    )
                    results.append(
                        {
                            "status": "error",
                            "message": str(e),
                            "file": file_info["path"],
                            "source": source_name,
                        }
                    )

            _logger.info(f"Processed {processed_count} files for {source_name}")

            return {
                "status": "success" if processed_count > 0 else "warning",
                "message": f"Processed {processed_count} files for {source_name}",
                "files_processed": processed_count,
                "results": results,
            }

        except Exception as e:
            _logger.error(f"Error fetching data for {source_name}: {str(e)}")
            _logger.error(traceback.format_exc())

            return {
                "status": "error",
                "message": f"Error fetching data for {source_name}: {str(e)}",
                "files_processed": 0,
            }

    def process_pep_file(self, file_info, job_id):
        """
        Process a single PEP file

        Args:
            file_info: Dictionary with file information
            job_id: Job ID for tracking

        Returns:
            dict: Processing results
        """
        _logger.info(f"Processing file: {file_info['path']}")

        try:
            # Create new processor instances to avoid thread conflicts
            importer = PepImporter(self.env)
            processor = DataProcessor(self.env)

            # Process the file
            result = importer.process_file(file_info, processor)

            _logger.info(
                f"Processed file {file_info['path']}: created {result.get('records_created', 0)}, updated {result.get('records_updated', 0)}"
            )
            return result
        except Exception as e:
            _logger.error(f"Error processing file {file_info['path']}: {str(e)}")
            _logger.error(traceback.format_exc())
            return {"status": "error", "message": str(e)}

    def fetch_global_pep_list_job(self, job_id):
        """
        Queue job implementation for fetching global PEP list
        This is used when the queue_job module is installed

        Args:
            job_id: Job ID for tracking
        """
        try:
            _logger.info(f"Starting PEP import job {job_id}")

            # Process sources in parallel using queue_job
            sources = ["uk_sanctions", "eu_sanctions", "un_sanctions", "ofac_sanctions"]
            source_jobs = []

            # Queue separate jobs for each source
            for source_name in sources:
                source_job = self.with_delay(
                    priority=20,
                    identity_key=f"fetch_source_pep_list_{source_name}_{job_id}",
                ).fetch_source_pep_list(source_name, job_id)
                source_jobs.append((source_name, source_job))
                _logger.info(f"Queued job for {source_name}")

            # Prepare the final result
            result = {
                "status": "success",
                "message": "PEP data import completed successfully",
                "sources_processed": len(sources),
                "records_created": 0,
                "records_updated": 0,
                "records_errored": 0,
                "records_skipped": 0,
                "source_results": {},
            }

            # Complete job tracking
            self._complete_job_tracking(job_id, "completed", result["message"])

            # Update last fetch date
            config = self.env["ir.config_parameter"].sudo()
            config.set_param(
                "compliance_management.last_pep_fetch", fields.Datetime.now()
            )

            return result

        except Exception as e:
            _logger.error(f"Error in background PEP import job: {str(e)}")
            _logger.error(traceback.format_exc())

            # Complete job tracking with error
            self._complete_job_tracking(job_id, "failed", str(e))

            return {"status": "error", "message": str(e)}

    def _fetch_global_pep_list_thread(self, job_id):
        """
        Thread implementation for fetching global PEP list
        This is used when the queue_job module is not installed

        Args:
            job_id: Job ID for tracking
        """
        try:
            # Get a new environment for the thread
            with api.Environment.manage():
                new_cr = self.pool.cursor()
                env = api.Environment(new_cr, self.env.uid, self.env.context)

                # Create a PepService instance with the new env
                service = PepService(env)

                # Fetch and import data from all sources
                result = service.fetch_and_import_pep_data()

                # Log the result
                if result["status"] == "success":
                    _logger.info(
                        f"PEP data import completed: {result['records_created']} records created, {result['records_updated']} updated"
                    )
                    status = "completed"
                elif result["status"] == "warning":
                    _logger.warning(f"PEP data import warning: {result['message']}")
                    status = "completed"
                else:
                    _logger.error(f"PEP data import error: {result['message']}")
                    status = "failed"

                # Complete job tracking
                model = env["res.pep"]
                model._complete_job_tracking(job_id, status, result["message"])

                # Update last fetch date
                config = env["ir.config_parameter"].sudo()
                config.set_param(
                    "compliance_management.last_pep_fetch", fields.Datetime.now()
                )

                # Commit changes
                new_cr.commit()
                new_cr.close()

        except Exception as e:
            _logger.error(f"Error in background PEP import thread: {str(e)}")
            _logger.error(traceback.format_exc())

            try:
                # Try to update job status
                with api.Environment.manage():
                    new_cr = self.pool.cursor()
                    env = api.Environment(new_cr, self.env.uid, self.env.context)
                    model = env["res.pep"]
                    model._complete_job_tracking(job_id, "failed", str(e))
                    new_cr.commit()
                    new_cr.close()
            except:
                pass
            