import traceback
from odoo import _, api, fields, models
import threading
import logging

from ..services.data_processor import DataProcessor
from ..services.pep_importer import PepImporter

from ..services.pep_service import PepService

_logger = logging.getLogger(__name__)


class Pep(models.Model):
    _name = "res.pep"
    _description = "PEP List"
    _inherit = ["mail.thread", "mail.activity.mixin"]

    _sql_constraints = [
        (
            "uniq_pep_identifier",
            "unique(unique_identifier)",
            "Unique Identifier already exists. Value must be unique!",
        ),
    ]
    _order = "surname,first_name"

    # Basic information
    name = fields.Char(string="Name", index=True)
    unique_identifier = fields.Char(
        string="Unique Identifier", index=True, required=True
    )

    # Personal details
    first_name = fields.Char(
        string="First Name", tracking=True, required=True, index=True
    )
    middle_name = fields.Char(string="Middle Name")
    surname = fields.Char(string="Surname", tracking=True, required=True, index=True)
    title = fields.Char(string="Title")
    aka = fields.Char(string="Aka")
    sex = fields.Char(string="Sex")
    date_of_birth = fields.Char(string="Date Of Birth")
    age = fields.Char(string="Age")
    place_of_birth = fields.Char(string="Place Of Birth")
    pob = fields.Char(string="Place of Birth")
    state_of_origin = fields.Char(string="State Of Origin")

    # Professional information
    present_position = fields.Char(string="Present Position")
    previous_position = fields.Char(string="Previous Position")
    pep_classification = fields.Char(string="Pep Classification")
    profession = fields.Char(string="Profession")
    status = fields.Char(string="Status")
    business_interest = fields.Char(string="Business Interest")
    associate_business_politics = fields.Char(string="Associate Business Politics")

    # Contact information
    official_address = fields.Char(string="Official Address")
    residential_address = fields.Char(string="Residential Address")
    email = fields.Char(string="Email")

    # Family information
    spouse = fields.Char(string="Spouse")
    children = fields.Char(string="Children")
    sibling = fields.Char(string="Sibling")
    parents = fields.Char(string="Parents")
    mothers_maden_name = fields.Char(string="Mothers Maiden Name")

    # Additional information
    narration = fields.Html(string="Narration")
    associates__business_political_social_ = fields.Char(
        string="Associates  Business Political Social"
    )
    bankers = fields.Char(string="Bankers")
    account_details = fields.Char(string="Account Details")
    press_report = fields.Text(string="Press Report")
    date_report = fields.Char(string="Date Report")
    additional_info = fields.Html(string="Additional Info")
    remarks = fields.Char(string="Remarks")
    religion = fields.Text(string="Religion")
    citizenship = fields.Char(string="Citizenship")
    education = fields.Text(string="Education")
    career_history = fields.Text(string="Career History")

    # Sourcing information
    source = fields.Text(string="Source")
    createdby = fields.Char(string="Created By")
    createdon = fields.Char(string="Created On")
    createdbyemail = fields.Char(string="Created by Email")
    lastmodifiedby = fields.Char(string="Last Modified By")
    lastmodifiedon = fields.Char(string="Last Modified On")
    lastmodifiedbyemail = fields.Char(string="Last Modified by Email")

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

    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')
    @api.model
    def create(self, vals):
        if "first_name" in vals:
            vals["name"] = self.get_name(vals)
        record = super(Pep, self).create(vals)
        return record

    def write(self, vals):
        if "first_name" in vals:
            vals["name"] = self.get_name(vals)
        record = super(Pep, self).write(vals)
        return record

    def get_name(self, vals):
        return f"%s %s" % (vals["first_name"], vals["surname"])

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

    def fetch_global_pep_list(self):
        """
        Start background jobs to fetch PEP data by source
        """
        # Define sources
        sources = ["eu_sanctions", "un_sanctions", "ofac_sanctions", "uk_sanctions"]

        # Check if queue_job is installed
        queue_job = self.env['ir.module.module'].sudo().search(
            [('name', '=', 'queue_job'), ('state', '=', 'installed')]
        )

        if queue_job:
            # Queue separate jobs for each source
            for idx, source in enumerate(sources):
                self.with_delay(priority=10+idx).fetch_source_pep_list(source)

            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": _("PEP Import Started"),
                    "message": _("PEP data fetch has been started in background jobs."),
                    "sticky": False,
                    "type": "success",
                },
            }
        else:
            # Fallback to threading
            thread = threading.Thread(target=self._fetch_global_pep_list_thread)
            thread.daemon = True
            thread.start()

            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": _("PEP Import Started"),
                    "message": _("PEP data fetch has been started in a background thread."),
                    "sticky": False,
                    "type": "info",
                },
            }

    def fetch_source_pep_list(self, source_name):
        """
        Fetch PEP data for a specific source
        """
        service = PepService(self.env)

        # Map source names to scraper methods
        source_methods = {
            "eu_sanctions": service.scraper.fetch_eu_sanctions,
            "un_sanctions": service.scraper.fetch_un_sanctions,
            "ofac_sanctions": service.scraper.fetch_ofac_sanctions,
            "uk_sanctions": service.scraper.fetch_uk_sanctions
        }

        if source_name not in source_methods:
            _logger.error(f"Unknown source: {source_name}")
            return

        # Get files for this source
        _logger.info(f"Fetching files for {source_name}")
        files = source_methods[source_name]()

        if not files:
            _logger.warning(f"No files found for {source_name}")
            return

        _logger.info(f"Found {len(files)} files for {source_name}")

        # Process each file in a separate job
        processed_count = 0
        for file_info in files:
            # Queue the file processing as a separate job
            self.with_delay().process_pep_file(file_info)
            processed_count += 1

        _logger.info(f"Queued {processed_count} file processing jobs for {source_name}")
        return processed_count

    def process_pep_file(self, file_info):
        """
        Process a single PEP file
        """
        _logger.info(f"Processing file: {file_info['path']}")

        service = PepService(self.env)
        importer = PepImporter(self.env)
        processor = DataProcessor(self.env)

        try:
            # Process the file
            result = importer.process_file(file_info, processor)

            _logger.info(f"Processed file {file_info['path']}: created {result.get('records_created', 0)}, updated {result.get('records_updated', 0)}")
            return result
        except Exception as e:
            _logger.error(f"Error processing file {file_info['path']}: {e}")
            return {
                "status": "error",
                "message": str(e)
            }

    def fetch_global_pep_list_job(self):
        """
        Queue job implementation for fetching global PEP list
        This is used when the queue_job module is installed
        """
        try:
            # Create a PepService instance
            service = PepService(self.env)

            # Fetch and import data from all sources
            result = service.fetch_and_import_pep_data()

            # Log the result
            if result["status"] == "success":
                _logger.info(
                    f"PEP data import completed: {result['records_created']} records created, {result['records_updated']} updated"
                )
            elif result["status"] == "warning":
                _logger.warning(f"PEP data import warning: {result['message']}")
            else:
                _logger.error(f"PEP data import error: {result['message']}")

            return result

        except Exception as e:
            _logger.error(f"Error in background PEP import job: {str(e)}")
            _logger.error(traceback.format_exc())
            return {
                "status": "error",
                "message": str(e)
            }

    def _fetch_global_pep_list_thread(self):
        """
        Thread implementation for fetching global PEP list
        This is used when the queue_job module is not installed
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
                elif result["status"] == "warning":
                    _logger.warning(f"PEP data import warning: {result['message']}")
                else:
                    _logger.error(f"PEP data import error: {result['message']}")

                # Update status in config params
                config = env["ir.config_parameter"].sudo()
                config.set_param(
                    "compliance_management.pep_import_thread_end",
                    fields.Datetime.now()
                )
                config.set_param(
                    "compliance_management.pep_import_thread_result",
                    result["status"]
                )
                config.set_param(
                    "compliance_management.pep_import_thread_message",
                    result["message"]
                )

                # Commit changes
                new_cr.commit()
                new_cr.close()

        except Exception as e:
            _logger.error(f"Error in background PEP import thread: {str(e)}")
            _logger.error(traceback.format_exc())