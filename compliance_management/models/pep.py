from odoo import _, api, fields, models
import threading
import logging
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
    children = fields.Char(string="Children")
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
    email = fields.Char(string="Email")
    remarks = fields.Char(string="Remarks")
    name = fields.Char(string="Name")
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

    @api.depends("first_name", "surname")
    def action_find_person(self):
        """
        Find information about a person using external services
        """
        self.ensure_one()

        # Create a PepService instance
        service = PepService(self.env)

        # Find biography using Gemini
        narration_html = service.find_person_biography(self.first_name, self.surname)
        if narration_html:
            self.write({"narration": narration_html})

        # Query sanctions service
        result = service.query_sanctions_service(self.first_name, self.surname)
        if result:
            # Format the data
            person_data = service.format_person_data(result)

            # Update the record
            if person_data:
                self.write(person_data)

        # Log the action
        _logger.info(f"Person information lookup completed for {self.name}")

        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": _("Information Lookup"),
                "message": _("Person information lookup completed."),
                "sticky": False,
                "type": "success",
            },
        }

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


# import traceback
# from odoo import _, api, fields, models
# import threading
# import logging

# from ..services.data_processor import DataProcessor
# from ..services.pep_importer import PepImporter

# from ..services.pep_service import PepService

# _logger = logging.getLogger(__name__)


# class Pep(models.Model):
#     _name = "res.pep"
#     _description = "PEP List"
#     _inherit = ["mail.thread", "mail.activity.mixin"]

#     _sql_constraints = [
#         (
#             "uniq_pep_identifier",
#             "unique(unique_identifier)",
#             "Unique Identifier already exists. Value must be unique!",
#         ),
#     ]
#     _order = "surname,first_name"

#     # Basic information
#     name = fields.Char(string="Name", index=True)
#     unique_identifier = fields.Char(
#         string="Unique Identifier", index=True, required=True
#     )

#     # Personal details
#     first_name = fields.Char(
#         string="First Name", tracking=True, required=True, index=True
#     )
#     middle_name = fields.Char(string="Middle Name")
#     surname = fields.Char(string="Surname", tracking=True, required=True, index=True)
#     title = fields.Char(string="Title")
#     aka = fields.Char(string="Aka")
#     sex = fields.Char(string="Sex")
#     date_of_birth = fields.Char(string="Date Of Birth")
#     age = fields.Char(string="Age")
#     place_of_birth = fields.Char(string="Place Of Birth")
#     pob = fields.Char(string="Place of Birth")
#     state_of_origin = fields.Char(string="State Of Origin")

#     # Professional information
#     present_position = fields.Char(string="Present Position")
#     previous_position = fields.Char(string="Previous Position")
#     pep_classification = fields.Char(string="Pep Classification")
#     profession = fields.Char(string="Profession")
#     status = fields.Char(string="Status")
#     business_interest = fields.Char(string="Business Interest")
#     associate_business_politics = fields.Char(string="Associate Business Politics")

#     # Contact information
#     official_address = fields.Char(string="Official Address")
#     residential_address = fields.Char(string="Residential Address")
#     email = fields.Char(string="Email")

#     # Family information
#     spouse = fields.Char(string="Spouse")
#     children = fields.Char(string="Children")
#     sibling = fields.Char(string="Sibling")
#     parents = fields.Char(string="Parents")
#     mothers_maden_name = fields.Char(string="Mothers Maiden Name")

#     # Additional information
#     narration = fields.Html(string="Narration")
#     associates__business_political_social_ = fields.Char(
#         string="Associates  Business Political Social"
#     )
#     bankers = fields.Char(string="Bankers")
#     account_details = fields.Char(string="Account Details")
#     press_report = fields.Text(string="Press Report")
#     date_report = fields.Char(string="Date Report")
#     additional_info = fields.Html(string="Additional Info")
#     remarks = fields.Char(string="Remarks")
#     religion = fields.Text(string="Religion")
#     citizenship = fields.Char(string="Citizenship")
#     education = fields.Text(string="Education")
#     career_history = fields.Text(string="Career History")

#     # Sourcing information
#     source = fields.Text(string="Source")
#     createdby = fields.Char(string="Created By")
#     createdon = fields.Char(string="Created On")
#     createdbyemail = fields.Char(string="Created by Email")
#     lastmodifiedby = fields.Char(string="Last Modified By")
#     lastmodifiedon = fields.Char(string="Last Modified On")
#     lastmodifiedbyemail = fields.Char(string="Last Modified by Email")

#     # Internal tracking fields
#     last_fetch_date = fields.Datetime(string="Last Fetch Date", readonly=True)
#     import_status = fields.Selection(
#         [
#             ("new", "New"),
#             ("imported", "Imported"),
#             ("updated", "Updated"),
#             ("error", "Error"),
#         ],
#         string="Import Status",
#         default="new",
#         readonly=True,
#     )
#     import_message = fields.Text(string="Import Message", readonly=True)

#     @api.model
#     def create(self, vals):
#         if "first_name" in vals:
#             vals["name"] = self.get_name(vals)
#         record = super(Pep, self).create(vals)
#         return record

#     def write(self, vals):
#         if "first_name" in vals:
#             vals["name"] = self.get_name(vals)
#         record = super(Pep, self).write(vals)
#         return record

#     def get_name(self, vals):
#         return f"%s %s" % (vals["first_name"], vals["surname"])

#     @api.depends("first_name", "surname")
#     def action_find_person(self):
#         """
#         Find information about a person using external services
#         """
#         self.ensure_one()

#         # Create a PepService instance
#         service = PepService(self.env)

#         # Find biography using Gemini
#         narration_html = service.find_person_biography(self.first_name, self.surname)
#         if narration_html:
#             self.write({"narration": narration_html})

#         # Query sanctions service
#         result = service.query_sanctions_service(self.first_name, self.surname)
#         if result:
#             # Format the data
#             person_data = service.format_person_data(result)

#             # Update the record
#             if person_data:
#                 self.write(person_data)

#         # Log the action
#         _logger.info(f"Person information lookup completed for {self.name}")

#         return {
#             "type": "ir.actions.client",
#             "tag": "display_notification",
#             "params": {
#                 "title": _("Information Lookup"),
#                 "message": _("Person information lookup completed."),
#                 "sticky": False,
#                 "type": "success",
#             },
#         }

#     def fetch_global_pep_list(self):
#         """
#         Start background jobs to fetch PEP data by source
#         """
#         # Define sources
#         sources = ["eu_sanctions", "un_sanctions", "ofac_sanctions", "uk_sanctions"]

#         # Check if queue_job is installed
#         queue_job = self.env['ir.module.module'].sudo().search(
#             [('name', '=', 'queue_job'), ('state', '=', 'installed')]
#         )

#         if queue_job:
#             # Queue separate jobs for each source
#             for idx, source in enumerate(sources):
#                 self.with_delay(priority=10+idx).fetch_source_pep_list(source)

#             return {
#                 "type": "ir.actions.client",
#                 "tag": "display_notification",
#                 "params": {
#                     "title": _("PEP Import Started"),
#                     "message": _("PEP data fetch has been started in background jobs."),
#                     "sticky": False,
#                     "type": "success",
#                 },
#             }
#         else:
#             # Fallback to threading
#             thread = threading.Thread(target=self._fetch_global_pep_list_thread)
#             thread.daemon = True
#             thread.start()

#             return {
#                 "type": "ir.actions.client",
#                 "tag": "display_notification",
#                 "params": {
#                     "title": _("PEP Import Started"),
#                     "message": _("PEP data fetch has been started in a background thread."),
#                     "sticky": False,
#                     "type": "info",
#                 },
#             }

#     def fetch_source_pep_list(self, source_name):
#         """
#         Fetch PEP data for a specific source
#         """
#         service = PepService(self.env)

#         # Map source names to scraper methods
#         source_methods = {
#             "eu_sanctions": service.scraper.fetch_eu_sanctions,
#             "un_sanctions": service.scraper.fetch_un_sanctions,
#             "ofac_sanctions": service.scraper.fetch_ofac_sanctions,
#             "uk_sanctions": service.scraper.fetch_uk_sanctions
#         }

#         if source_name not in source_methods:
#             _logger.error(f"Unknown source: {source_name}")
#             return

#         # Get files for this source
#         _logger.info(f"Fetching files for {source_name}")
#         files = source_methods[source_name]()

#         if not files:
#             _logger.warning(f"No files found for {source_name}")
#             return

#         _logger.info(f"Found {len(files)} files for {source_name}")

#         # Process each file in a separate job
#         processed_count = 0
#         for file_info in files:
#             # Queue the file processing as a separate job
#             self.with_delay().process_pep_file(file_info)
#             processed_count += 1

#         _logger.info(f"Queued {processed_count} file processing jobs for {source_name}")
#         return processed_count

#     def process_pep_file(self, file_info):
#         """
#         Process a single PEP file
#         """
#         _logger.info(f"Processing file: {file_info['path']}")

#         service = PepService(self.env)
#         importer = PepImporter(self.env)
#         processor = DataProcessor(self.env)

#         try:
#             # Process the file
#             result = importer.process_file(file_info, processor)

#             _logger.info(f"Processed file {file_info['path']}: created {result.get('records_created', 0)}, updated {result.get('records_updated', 0)}")
#             return result
#         except Exception as e:
#             _logger.error(f"Error processing file {file_info['path']}: {e}")
#             return {
#                 "status": "error",
#                 "message": str(e)
#             }

#     def fetch_global_pep_list_job(self):
#         """
#         Queue job implementation for fetching global PEP list
#         This is used when the queue_job module is installed
#         """
#         try:
#             # Create a PepService instance
#             service = PepService(self.env)

#             # Fetch and import data from all sources
#             result = service.fetch_and_import_pep_data()

#             # Log the result
#             if result["status"] == "success":
#                 _logger.info(
#                     f"PEP data import completed: {result['records_created']} records created, {result['records_updated']} updated"
#                 )
#             elif result["status"] == "warning":
#                 _logger.warning(f"PEP data import warning: {result['message']}")
#             else:
#                 _logger.error(f"PEP data import error: {result['message']}")

#             return result

#         except Exception as e:
#             _logger.error(f"Error in background PEP import job: {str(e)}")
#             _logger.error(traceback.format_exc())
#             return {
#                 "status": "error",
#                 "message": str(e)
#             }

#     def _fetch_global_pep_list_thread(self):
#         """
#         Thread implementation for fetching global PEP list
#         This is used when the queue_job module is not installed
#         """
#         try:
#             # Get a new environment for the thread
#             with api.Environment.manage():
#                 new_cr = self.pool.cursor()
#                 env = api.Environment(new_cr, self.env.uid, self.env.context)

#                 # Create a PepService instance with the new env
#                 service = PepService(env)

#                 # Fetch and import data from all sources
#                 result = service.fetch_and_import_pep_data()

#                 # Log the result
#                 if result["status"] == "success":
#                     _logger.info(
#                         f"PEP data import completed: {result['records_created']} records created, {result['records_updated']} updated"
#                     )
#                 elif result["status"] == "warning":
#                     _logger.warning(f"PEP data import warning: {result['message']}")
#                 else:
#                     _logger.error(f"PEP data import error: {result['message']}")

#                 # Update status in config params
#                 config = env["ir.config_parameter"].sudo()
#                 config.set_param(
#                     "compliance_management.pep_import_thread_end",
#                     fields.Datetime.now()
#                 )
#                 config.set_param(
#                     "compliance_management.pep_import_thread_result",
#                     result["status"]
#                 )
#                 config.set_param(
#                     "compliance_management.pep_import_thread_message",
#                     result["message"]
#                 )

#                 # Commit changes
#                 new_cr.commit()
#                 new_cr.close()

#         except Exception as e:
#             _logger.error(f"Error in background PEP import thread: {str(e)}")
#             _logger.error(traceback.format_exc())
