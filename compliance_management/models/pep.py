import traceback
from odoo import _, api, fields, models
import threading
import logging

from ..services.data_processor import DataProcessor
from ..services.pep_importer import PepImporter

from ..services.pep_service import PepService
from ..services.sanction_list_data import sources

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

        # Check if queue_job is installed
        queue_job = (
            self.env["ir.module.module"]
            .sudo()
            .search([("name", "=", "queue_job"), ("state", "=", "installed")])
        )

        if queue_job:
            # Queue separate jobs for each source
            for idx, source in enumerate(sources):
                self.with_delay(priority=10 + idx).fetch_source_pep_list(source['url'], source['country'])

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
            pass
        # Fallback to threading
        # thread = threading.Thread(target=self._fetch_global_pep_list_thread)
        # thread.daemon = True
        # thread.start()

        # return {
        #     "type": "ir.actions.client",
        #     "tag": "display_notification",
        #     "params": {
        #         "title": _("PEP Import Started"),
        #         "message": _(
        #             "PEP data fetch has been started in a background thread."
        #         ),
        #         "sticky": False,
        #         "type": "info",
        #     },
        # }

    def fetch_source_pep_list(self, source_url, source_name):
        """
        Fetch PEP data for a specific source
        """

        service = PepService(self.env)

        # Map source names to scraper methods
        # source_methods = {
        #     "eu_sanctions": service.scraper.fetch_eu_sanctions,
        #     "un_sanctions": service.scraper.fetch_un_sanctions,
        #     "ofac_sanctions": service.scraper.fetch_ofac_sanctions,
        #     "uk_sanctions": service.scraper.fetch_uk_sanctions,
        # }

        # if source_name not in source_methods:
        #     _logger.error(f"Unknown source: {source_name}")
        #     return

        files = service.scraper.scrapeAndDownload(
            source_url, source_name
        )


        # Get files for this source
        _logger.info(f"Fetching files for {source_name}")

        if not files:
            _logger.warning(f"No files found for {source_name}")
            return

        _logger.info(f"Found {len(files)} files for {source_name}")
        print(files)
        # Process each file in a separate job
        processed_count = 0
        for file_info in files:
            # Queue the file processing as a separate job
            self.with_delay().process_pep_file(file_info)
            processed_count += 1

        # _logger.info(f"Queued {processed_count} file processing jobs for {source_name}")
        # return processed_count

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
            
            _logger.info(
                f"Processed file {file_info['path']}: created {result.get('records_created', 0)}, updated {result.get('records_updated', 0)}"
            )
            return result
        except Exception as e:
            _logger.error(f"Error processing file {file_info['path']}: {e}")
            return {"status": "error", "message": str(e)}

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
            return {"status": "error", "message": str(e)}

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
                    "compliance_management.pep_import_thread_end", fields.Datetime.now()
                )
                config.set_param(
                    "compliance_management.pep_import_thread_result", result["status"]
                )
                config.set_param(
                    "compliance_management.pep_import_thread_message", result["message"]
                )

                # Commit changes
                new_cr.commit()
                new_cr.close()

        except Exception as e:
            _logger.error(f"Error in background PEP import thread: {str(e)}")
            _logger.error(traceback.format_exc())


# from odoo import _, api, fields, models
# import requests
# import json
# import markdown
# from bs4 import BeautifulSoup
# import io
# import pandas as pd
# import tabula
# import os
# from .web_scraping import WebScraper


# # from queue import Queue
# # from threading import Thread
# from io import BytesIO

# # from odoo.addons.queue.queue_job.job import Job
# # from file_reader import FileReader


# class Pep(models.Model):
#     _name = 'res.pep'
#     _description = 'PEP List'
#     _inherit = ['mail.thread', 'mail.activity.mixin']

#     _sql_constraints = [
#         ('uniq_pep_identifier', 'unique(unique_identifier)',
#          "Unique Identifier already exists. Value must be unique!"),
#     ]
#     _order="surname,first_name"

#     narration = fields.Html(string="Narration")
#     lastmodifiedon = fields.Char(string="Last Modified On")
#     lastmodifiedbyemail = fields.Char(string="Last Modified by Email")
#     unique_identifier = fields.Char(
#         string="Unique Identifier", index=True, required=True
#     )
#     surname = fields.Char(string="Surname", tracking=True, required=True, index=True)
#     first_name = fields.Char(
#         string="First Name", tracking=True, required=True, index=True
#     )
#     middle_name = fields.Char(string="Middle Name")
#     title = fields.Char(string="Title")
#     aka = fields.Char(string="Aka")
#     sex = fields.Char(string="Sex")
#     date_of_birth = fields.Char(string="Date Of Birth")
#     present_position = fields.Char(string="Present Position")
#     previous_position = fields.Char(string="Previous Position")
#     pep_classification = fields.Char(string="Pep Classification")
#     official_address = fields.Char(string="Official Address")
#     profession = fields.Char(string="Profession")
#     residential_address = fields.Char(string="Residential Address")
#     state_of_origin = fields.Char(string="State Of Origin")
#     spouse = fields.Char(string="Spouse")
#     children = fields.Char(string="Children")
#     sibling = fields.Char(string="Sibling")
#     parents = fields.Char(string="Parents")
#     mothers_maden_name = fields.Char(string="Mothers Maiden Name")
#     associates__business_political_social_ = fields.Char(
#         string="Associates  Business Political Social"
#     )
#     bankers = fields.Char(string="Bankers")
#     account_details = fields.Char(string="Account Details")
#     place_of_birth = fields.Char(string="Place Of Birth")
#     press_report = fields.Text(string="Press Report")
#     date_report = fields.Char(string="Date Report")
#     additional_info = fields.Html(string="Additional Info")
#     email = fields.Char(string="Email")
#     remarks = fields.Char(string="Remarks")
#     name = fields.Char(string="Name")
#     status = fields.Char(string="Status")
#     business_interest = fields.Char(string="Business Interest")
#     age = fields.Char(string="Age")
#     associate_business_politics = fields.Char(string="Associate Business Politics")
#     pob = fields.Char(string="Place of Birth")
#     createdby = fields.Char(string="Created By")
#     createdon = fields.Char(string="Created On")
#     createdbyemail = fields.Char(string="Created by Email")
#     lastmodifiedby = fields.Char(string="Last Modified By")
#     religion = fields.Text(string="Religion")
#     citizenship = fields.Char(string="Citizenship")
#     education = fields.Text(string="Education")
#     career_history = fields.Text(string="Career History")
#     source = fields.Text(string="Source")

#     @api.model
#     def create(self,vals):
#         if 'first_name' in vals:
#             vals['name'] = self.get_name(vals)
#         record = super(Pep, self).create(vals)
#         return record

#     def write(self,vals):
#         if 'first_name' in vals:
#             vals['name'] = self.get_name(vals)
#         record = super(Pep, self).write(vals)
#         return record

#     def get_name(self,vals):
#         return f"%s %s"%(vals['first_name'],vals['surname'])

#     @api.depends('first_name','surname')
#     def action_find_person(self):
#         name = f"Who is %s %s"%(self.first_name,self.surname)
#         headers = {"Content-Type": "application/json", "Accept": "application/json", "Catch-Control": "no-cache"}
#         json_data = {"contents":[{"parts":[{"text":f"{name}"}]}]}
#         config = self.env['ir.config_parameter'].sudo()
#         api_key = config.get_param('gemini_api_key')
#         try:
#             if api_key:
#                 url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={api_key}"
#                 response = requests.post(url, data=json.dumps(json_data), headers=headers)
#                 data = json.loads(response.text)
#                 # Extract the 'text' tag
#                 text_value = data['candidates'][0]['content']['parts'][0]['text']
#                 self.write({'narration':markdown.markdown(text_value)})
#         except:
#             pass

#         self.query_sanctions_service(self.first_name,self.surname)

#     def query_sanctions_service(self,firstname,lastname):
#         config = self.env['ir.config_parameter'].sudo()
#         API_KEY = config.get_param('opensanctions_api_key')
#         if API_KEY is not None:
#             try:
#                 headers = {
#                     "Authorization": API_KEY,
#                 }
#                 # Prepare a query to match on schema and the name property
#                 query = {
#                     "queries": {
#                         "q1": {"schema": "Person", "properties": {"name": [f"{firstname} {lastname}"]}}
#                     }
#                 }
#                 # Make the request
#                 response = requests.post(
#                     "https://api.opensanctions.org/match/default", headers=headers, json=query
#                 )
#                 # Check for HTTP errors
#                 response.raise_for_status()
#                 # Get the results for our query
#                 data = response.json()["responses"]["q1"]["results"]
#                 person = data[0]
#                 metadata = data[1]
#                 properties = person['properties']
#                 position = "\n".join(properties['position']) if 'position' in properties else "\n".join(metadata['properties']['position'])
#                 education = "\n".join(metadata['properties']['education']) if 'education' in metadata['properties'] else None
#                 notes = "\n".join(properties['notes']) if 'notes' in properties else None
#                 birth_place = "\n".join(properties['birthPlace']) if 'birthPlace' in properties else None
#                 religion = "\n".join(properties['religion']) if 'religion' in properties else ''
#                 middle_name = metadata['properties']['middleName'][0] if 'middleName' in metadata['properties'] else ''
#                 first_name = metadata['properties']['firstName'][0] if 'firstName' in  metadata['properties'] else " ".join(person['caption'])
#                 last_name = metadata['properties']['lastName'][0] if 'lastName' in metadata['properties'] else None
#                 title = metadata['properties']['title'][0] if 'title' in metadata['properties'] else ''
#                 gender = person['properties']['gender'][0].capitalize()
#                 citizenship = person['properties']['citizenship'][0].upper()
#                 birth_date = person['properties']['birthDate'][0]
#                 unique_id = person['id']
#                 # Now 'data' is a dictionary
#                 self.write({
#                         'sex': gender,
#                         'date_of_birth': birth_date,
#                         'title': title,
#                         'education': education,
#                         'religion': religion,
#                         'citizenship': citizenship,
#                         'middle_name': middle_name,
#                         'place_of_birth': birth_place,
#                         'career_history':position})

#             except:
#                 None

#     def fetch_global_pep_list(self):
#         """
#         Fetch global PEP list from multiple sources
#         """
#         # Create a WebScraper instance
#         scraper = WebScraper(self.env)

#         # Set up URL collections
#         url_collections = [
#             # "https://webgate.ec.europa.eu/fsd/fsf#!/files",
#             "https://www.gov.uk/government/publications/the-uk-sanctions-list",
#             # "https://www.un.org/securitycouncil/content/un-sc-consolidated-list",
#             # "https://sanctionssearch.ofac.treas.gov/"
#         ]

#         # Set up credentials for authenticated URLs
#         credentials = {
#             "username": "kayode.o@novajii.com",
#             "password": "2virusFOUND",
#             "login_url": "https://ecas.ec.europa.eu/cas/login",
#         }

#         results = {}

#         try:
#             for url in url_collections:
#                 # Call process_url on the scraper instance, not on the class
#                 links = scraper.process_url(url, credentials)

#                 csv_links = []
#                 xlsx_links = []
#                 pdf_links = []

#                 for link in links:
#                     href = link["href"]

#                     if href.endswith(".csv"):
#                         csv_links.append(href)
#                     elif href.endswith(".xlsx") or href.endswith(".ods"):
#                         xlsx_links.append(href)
#                     elif href.endswith(".pdf"):
#                         pdf_links.append(href)

#                 url_results = {}

#                 if csv_links:
#                     print(f"CSV Links: {csv_links}")
#                     # Call read_csv_links on the scraper instance
#                     csv_data = scraper.read_csv_links(url, csv_links)
#                     url_results["csv"] = csv_data

#                 if xlsx_links:
#                     # Call read_xlsx_links on the scraper instance
#                     xlsx_data = scraper.read_xlsx_links(url, xlsx_links)
#                     url_results["xlsx"] = xlsx_data

#                 if pdf_links:
#                     print(f"PDF Links: {pdf_links}")
#                     # Call read_pdf_links on the scraper instance
#                     pdf_data = scraper.read_pdf_links(url, pdf_links)
#                     url_results["pdf"] = pdf_data

#                 if not csv_links and not xlsx_links and not pdf_links:
#                     print("No CSV, XLSX, or PDF files found.")

#                 results[url] = url_results

#         except Exception as e:
#             print(f"An unexpected error occurred: {e}")

#         # Process the results (if needed)
#         self._process_global_pep_results(results)

#         return True

#     def _process_global_pep_results(self, results):
#         """
#         Process the results from fetch_global_pep_list

#         Args:
#             results: Dictionary containing all extracted data
#         """
#         # This method would contain your business logic for processing the fetched data
#         # For example, creating PEP records from the CSV/XLSX data
#         for url, url_results in results.items():
#             # Process CSV data
#             if "csv" in url_results:
#                 for csv_link, df in url_results["csv"].items():
#                     self._process_dataframe(df, source=f"CSV: {csv_link}")

#             # Process XLSX data
#             if "xlsx" in url_results:
#                 for xlsx_link, df in url_results["xlsx"].items():
#                     self._process_dataframe(df, source=f"XLSX: {xlsx_link}")

#             # Process PDF data
#             if "pdf" in url_results:
#                 for pdf_link, dfs in url_results["pdf"].items():
#                     for df in dfs:
#                         self._process_dataframe(df, source=f"PDF: {pdf_link}")

#     def _process_dataframe(self, df, source=None, batch_size=200):
#         """
#         Process a DataFrame and create PEP records

#         Args:
#             df: Pandas DataFrame to process
#             source: Source of the DataFrame
#             batch_size: Number of rows to process at once
#         """
#         # This is a placeholder method - you would implement your specific logic here
#         # For example, mapping DataFrame columns to PEP fields and creating records

#         # Sample implementation (modify as needed):
#         try:
#             print(f"Processing DataFrame from {source}")
#             print(f"Columns: {df.columns.tolist()}")
#             print(f"Sample data: {df.head(2).to_dict(orient='records')}")

#             # Your actual processing logic would go here
#             # For example:
#             # total_rows = len(df)
#             # start_row = 0
#             # while start_row < total_rows:
#             #    end_row = min(start_row + batch_size, total_rows)
#             #    batch_df = df.iloc[start_row:end_row]
#             #    # Process batch...
#             #    start_row = end_row

#         except Exception as e:
#             print(f"Error processing DataFrame: {e}")
