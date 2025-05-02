from odoo import models, fields, api
import os
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from dotenv import load_dotenv
import base64
from urllib.parse import unquote, urlparse
from os.path import basename, join
import json
import time
import threading
from datetime import datetime, timedelta
from odoo.tools import html_escape
import logging
from odoo.exceptions import AccessError
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin
import hashlib
import psycopg2

import io
from collections import Counter
import binascii  # Make sure this import is added
import PyPDF2
from odoo.exceptions import ValidationError, UserError
from PIL import Image
import pytesseract
from pdf2image import convert_from_bytes


_logger = logging.getLogger(__name__)


load_dotenv()


class RulebookTitle(models.Model):
    _name = 'rulebook.title'
    _description = 'Rulebook Sources'
    _rec_name = 'name'
    _order = "id desc"

    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Title', required=True, index=True)
    file = fields.Binary(string='File', attachment=True,
                         required=False, index=True, tracking=True,)
    file_name = fields.Char(string='File Name', index=True, tracking=True,)
    ref_number = fields.Char(string='Reference Number', required=False)
    released_date = fields.Date(
        string='Released Date', required=False, tracking=True,)
    status = fields.Selection([
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('deleted', 'Deleted')
    ], string='Status', default='active', required=True, tracking=True,)
    source_id = fields.Many2one(
        'rulebook.sources', string='Source', required=True, index=True)
    created_on = fields.Datetime(
        string='Created On', default=fields.Datetime.now, required=True)
    created_by = fields.Many2one(
        'res.users', string='Created By', default=lambda self: self.env.user, readonly=True)
    input_type = fields.Selection(
        [('manual', 'Manual Input'), ('ai', 'AI Generated')],
        string='Source',
        default='manual',
        tracking=True,
    )
    active = fields.Boolean(string='Active', default=True)

    # Add the external_resource_url field if it's not already defined
    external_resource_url = fields.Char("External Resource URL")
    # Add email-related fields
    email_recipient_ids = fields.Many2many('res.users', 'rulebook_email_recipient_rel',
                                           string='Email Recipients')
    email_cc_ids = fields.Many2many('res.users', 'rulebook_email_cc_rel',
                                    string='CC Recipients')
    email_subject = fields.Char('Email Subject')
    email_body = fields.Html('Email Body')

    _email_data = {}

    # to send the data to the template
    def data(self):
        """Return the email data for templates to use"""
        return self._email_data.get(self.id, {})

    def set_email_data(self, data):
        """Set email data for this specific record"""
        self._email_data[self.id] = data

    def action_send_email(self):
        self.ensure_one()
        if not self.email_recipient_ids:
            raise AccessError("Please select at least one recipient.")

        if not self.email_body:
            raise AccessError("email body is required.")

        if not self.file:
            raise AccessError("No file attached to send.")

        # Create attachment from the current file
        attachment = self.env['ir.attachment'].create({
            'name': self.file_name or 'Rulebook',
            'datas': self.file,
            'res_model': 'rulebook.title',
            'res_id': self.id,
        })

        # Replace with the actual template ID
        template = self.env.ref('rule_book.email_template_share_document_')
        now = datetime.now()
        now_without_microseconds = now.replace(microsecond=0)
        timestamp = self.env["reply.log"]._compute_formatted_date(
            now_without_microseconds)

        # Prepare email data
        email_data = {
            "email_from": os.getenv("EMAIL_FROM"),
            'subject': self.email_subject or f"Rulebook: {self.name}",
            'body_html': self.email_body,
            'email_to': ','.join(self.email_recipient_ids.mapped('email')),
            'email_cc': ','.join(self.email_cc_ids.mapped('email')),
            'datetime': timestamp,
            "current_year": datetime.now().year,
            'attachment_ids': [(4, attachment.id)],
        }

        # Store the data for this specific record
        self.set_email_data(email_data)

        try:
            # Send the email with attachments
            template.send_mail(self.id, force_send=True, email_values={
                'attachment_ids': email_data['attachment_ids']
            })

            # Show success message
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Success',
                    'message': 'Email sent successfully',
                    'type': 'success',
                    'sticky': False,
                    'next': {
                        'type': 'ir.actions.client',
                        'tag': 'reload',
                        'params': {
                            'model': 'rulebook.title',
                            'id': self.id,
                            'values': {
                                'email_recipient_ids': [(5, 0, 0)],
                                'email_cc_ids': [(5, 0, 0)],
                                'email_subject': False,
                                'email_body': False,
                            },
                            'context': self.env.context,
                        }
                    }
                }
            }

        except Exception as e:
            # Show error message if email sending fails
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Error',
                    'message': str(e),
                    'type': 'danger',
                    'sticky': True,
                }
            }

        finally:
            # Update the record to clear fields
            self.write({
                'email_recipient_ids': [(5, 0, 0)],
                'email_cc_ids': [(5, 0, 0)],
                'email_subject': False,
                'email_body': False,
            })

            # Clean up the stored email data after sending
            if self.id in self._email_data:
                del self._email_data[self.id]

    @api.model
    def fetch_new_ai_titles(self):
        # Get today's date for filtering
        today = datetime.now()
        three_days_ago = today - timedelta(days=3)

        # Fetch titles created within the last 7 days
        titles = self.env['rulebook.title'].search([
            ('create_date', '>=', three_days_ago),
            ('create_date', '<=', today),
            ('input_type', '=', 'ai'),
            ('status', '=', 'active')
        ], order='source_id asc')
        # ], order='source_id desc, name like "%%CBN%%" desc, create_date desc')

        # Prepare the results
        results = []
        base_url = self.env['ir.config_parameter'].sudo(
        ).get_param('web.base.url')

        for title in titles:
            # Construct the URL to the form view of each record
            # Ensure base_url ends with a slash for proper URL construction
            if not base_url.endswith('/'):
                base_url += '/'
            form_url = f"{base_url}web#id={title.id}&model=rulebook.title&view_type=form"

            results.append({
                'id': title.id,
                'name': title.name,
                'source': title.source_id.name if title.source_id else 'N/A',
                'created_on': title.created_on,
                'created_by': title.created_by.name if title.created_by else 'N/A',
                'form_link': form_url,  # HTML escaped link to form view
            })

        # _logger.info(
        #     f"Retrieved {len(results)} AI titles from the last 7 days: {results}")
        return results

    def action_ndic_scrapper(self):
        """Method to run the NDIC Scrapper when button is clicked"""
        success = self.NDICScrapper()
        if success:
            return {
                "type": "ir.actions.client",
                "tag": "reload",  # Reload the view to reflect any changes
            }
        else:
            return {
                "type": "ir.actions.client",
                "tag": "display_notification",
                "params": {
                    "title": "Error",
                    "message": "NDIC Scrapper failed to create records.",
                    "type": "danger",
                },
            }

    # @api.model
    @api.model
    def create(self, vals):
        # Check if the 'created_by' field is present, otherwise assign the current user
        if not vals.get('created_by'):
            vals['created_by'] = self.env.user.id

        # Print/log the user creating the record and other details
        print(
            f"A new record is being created by user: {self.env.user.name} (ID: {self.env.user.id})")
        # print(f"Values used for creation: {vals}")

        # Call the super method to actually create the record
        record = super(RulebookTitle, self).create(vals)

        # Print/log the newly created record details
        print(f"New record created with ID: {record.id}")

        return record


    @api.model
    def SECScraper(self):
        sec_files = [
            {
                "name": os.getenv("SEC_FILES_LAWS_AND_RULES_NAME"),
                "urls": json.loads(os.getenv("SEC_FILES_LAWS_AND_RULES_URLS")),
                "storage_path": os.getenv("SEC_FILES_LAWS_AND_RULES_STORAGE_PATH"),
                "html_tags": json.loads(os.getenv("SEC_FILES_LAWS_AND_RULES_HTML_TAGS")),
            },
            {
                "name": os.getenv("SEC_FILES_CODES_NAME"),
                "urls": json.loads(os.getenv("SEC_FILES_CODES_URLS")),
                "storage_path": os.getenv("SEC_FILES_CODES_STORAGE_PATH"),
                "html_tags": json.loads(os.getenv("SEC_FILES_CODES_HTML_TAGS")),
            },
            {
                "name": os.getenv("SEC_FILES_GUIDELINES_NAME"),
                "urls": json.loads(os.getenv("SEC_FILES_GUIDELINES_URLS")),
                "storage_path": os.getenv("SEC_FILES_GUIDELINES_STORAGE_PATH"),
                "html_tags": json.loads(os.getenv("SEC_FILES_GUIDELINES_HTML_TAGS")),
            },
        ]

        # Use ThreadPoolExecutor for better thread management
        with ThreadPoolExecutor(max_workers=5) as executor:
            for file_type in sec_files:
                _logger.info(f"Starting scrape for {file_type['name']}")
                for url, html_tag in zip(file_type["urls"], file_type["html_tags"]):
                    try:
                        response = requests.get(url, timeout=10)
                        response.raise_for_status()
                        soup = BeautifulSoup(response.text, "html.parser")
                        pdf_links = soup.select(html_tag)

                        for link in pdf_links:
                            pdf_url = link.get('href', '')
                            if not pdf_url.startswith('http'):
                                pdf_url = urljoin(url, pdf_url)

                            if pdf_url.endswith(".pdf"):
                                filename = basename(urlparse(pdf_url).path)
                                filename_title = unquote(
                                    filename.split('.')[0]).replace(' ', '_')
                                date_string = self._get_date_from_url(pdf_url)

                                try:
                                    pdf_response = requests.get(
                                        pdf_url, timeout=10)
                                    pdf_response.raise_for_status()
                                    content_hash = hashlib.md5(
                                        pdf_response.content).hexdigest()
                                    filename = f"{filename_title}_{content_hash[:8]}{date_string}.pdf"
                                except requests.RequestException as e:
                                    _logger.error(
                                        f"Failed to download {pdf_url} for hashing: {e}")
                                    continue  # Skip to next PDF if download fails

                                file_binary_data = base64.b64encode(
                                    pdf_response.content).decode('utf-8')

                                # Check if record already exists
                                existing = self.env["rulebook.title"].sudo().search([
                                    "|",
                                    ("file_name", "=", filename),
                                    ("name", "=", filename_title)
                                ], limit=1)

                                if existing:
                                    _logger.info(
                                        f"Skipping, document '{filename}' or '{filename_title}' already exists.")
                                    continue

                                # Fetch the source_id
                                source = self.env["rulebook.sources"].sudo().search(
                                    [("name", "ilike", "SEC")], limit=1)
                                if not source:
                                    _logger.error(
                                        "Source 'SEC' not found in the rulebook.sources.")
                                    continue

                                try:
                                    new_record = self.env["rulebook.title"].sudo().create({
                                        "name": filename_title,
                                        "file": file_binary_data,
                                        "file_name": filename,
                                        "ref_number": None,
                                        "released_date": date_string or fields.Datetime.today(),
                                        "status": "active",
                                        "source_id": source.id,
                                        "created_on": fields.Datetime.now(),
                                        "created_by": self.env.user.id,
                                        "external_resource_url": pdf_url,
                                        "input_type": "ai",
                                    })

                                    app_base_url = self.env['ir.config_parameter'].sudo(
                                    ).get_param('web.base.url')
                                    document_url = f"{app_base_url}/web#id={new_record.id}&model=rulebook.title&view_type=form"

                                    _logger.info(
                                        f"Created SEC record for: {new_record}")
                                    self.env.cr.commit()  # Explicitly commit the transaction

                                    self._notify_reg_officers(
                                        document_url, source.name, new_record.name)

                                except Exception as e:
                                    _logger.error(
                                        f"Failed to create record for {filename_title}: {str(e)}")

                    except requests.RequestException as e:
                        _logger.error(f"Failed to process URL {url}: {e}")

        return "Scrape successful for Sec!"

    def _get_date_from_url(self, url):
        date_patterns = [
            # Add re.IGNORECASE for month names with day and year
            (re.compile(r"[A-Za-z]{1,15}-\d{1,2}-\d{4}",
                        re.IGNORECASE), "%B-%d-%Y"),
            (re.compile(r"[A-Za-z]{1,15}-\d{1,2}-\d{4}",
                        re.IGNORECASE), "%b-%d-%Y"),
            (re.compile(r"\d{1,2}-[A-Za-z]{1,15}-\d{4}",
                        re.IGNORECASE), "%d-%B-%Y"),
            (re.compile(r"[A-Za-z]{1,15}-\d{1,2}-\d{4}",
                        re.IGNORECASE), "%B-%d-%Y"),
            (re.compile(r"\d{1,2}[A-Za-z]{3}\d{4}", re.IGNORECASE), "%d%b%Y"),
            # Defaults to day = 01
            (re.compile(r"[A-Za-z]{3,10}-\d{4}", re.IGNORECASE), "%b-%Y"),
            (re.compile(r"\d{4}/\d{2}", re.IGNORECASE),
             "%Y/%m"),           # Defaults to day = 01
            (re.compile(r"\([A-Za-z]{3,10}\d{4}\)"),
             "(%B%Y)"),  # Defaults to day = 01
            (re.compile(r"\b\d{4}\b"), "%Y")  # Defaults to month/day = 01

        ]

        if re.search(r"(ISA-|ACT-|SEC-Guideline)", url, re.IGNORECASE):
            _logger.warning(f"Non-date pattern found (ignoring): {url}")
            return None

        for pattern, date_format in date_patterns:
            match = pattern.search(url)
            if match:
                date_str = match.group(0)
                _logger.debug(f"Matched date: '{date_str}' from URL: {url}")
                try:
                    if date_format == "%b-%Y":
                        return datetime.strptime(date_str + "-01", "%b-%Y-%d").strftime("%Y-%m-%d")
                    return datetime.strptime(date_str, date_format).strftime("%Y-%m-%d")
                except ValueError:
                    _logger.warning(
                        f"Failed to parse date '{date_str}' with format '{date_format}' from URL: {url}")
        return None

    def _filter_unique_records(self, new_records, existing_records):
        unique = []
        for record in new_records:
            if not any(
                (r.file_name == record['file_name'] and r.released_date == record['released_date']) or
                (r.name == record['name'] and r.external_resource_url ==
                 record['external_resource_url'])
                for r in existing_records
            ):
                unique.append(record)
        return unique

    @api.model
    def CBNScraper(self):
        _logger.critical("CBN Scraper processing")
        api_url = os.getenv("CBN_API")
        base_url = os.getenv("CBN_BASE_URL")

        current_year = datetime.now().year

        # Fetch data from the API
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            documents = response.json()
        except requests.RequestException as e:
            _logger.error(f"Failed to retrieve documents from API: {e}")
            return "Failed to fetch documents"

        for doc in documents:
            # Check if the document is from the current year
            doc_date = datetime.strptime(doc['documentDate'], "%d/%m/%Y")
            if doc_date.year != current_year:
                continue

            # Check if the document already exists in the database
            existing_title = self.env["rulebook.title"].search(
                [("file_name", "=", doc['refNo'])], limit=1
            )
            existing_name = self.env["rulebook.title"].search(
                [("name", "=", doc['title'])], limit=1
            )

            if existing_title or existing_name:
                _logger.info(
                    f"Skipping, document '{doc['refNo']}' or '{doc['title']}' already exists.")
                continue

            # Construct the full URL for the document
            resource_url = base_url + doc['link']

            try:
                # Download the document
                download = requests.get(resource_url, timeout=10)
                download.raise_for_status()
                file_binary_data = base64.b64encode(
                    download.content).decode('utf-8')

                # Fetch the source_id
                source = self.env["rulebook.sources"].search(
                    [("name", "ilike", "CBN")], limit=1
                )
                if not source:
                    raise ValueError(
                        "Source 'CBN' not found in the rulebook.sources."
                    )

                # Create the record
                new_record = self.env["rulebook.title"].create({
                    "name": doc['title'],
                    "file": file_binary_data,
                    "file_name": doc['refNo'],
                    "ref_number": doc['refNo'],
                    "released_date": doc_date.date(),
                    "status": "active",
                    "source_id": source.id,
                    "created_on": fields.Datetime.now(),
                    "created_by": self.env.user.id,
                    "external_resource_url": resource_url,
                    "input_type": "ai",
                    # "document_size": doc['filesize']
                })
                _logger.info(f"Created CBN record for: {doc['title']}")

                app_base_url = self.env['ir.config_parameter'].sudo(
                ).get_param('web.base.url')
                document_url = f"{app_base_url}/web#id={new_record.id}&model=rulebook.title&view_type=form"

                self._notify_reg_officers(
                    document_url, source.name, new_record.name)
            except requests.RequestException as e:
                _logger.error(f"Failed to download {doc['title']}: {e}")

        return "CBN Scrape was successful!"

    def NDICScrapper(self):
        # Add a 60-second delay before starting the scraping
        _logger.critical(
            "Waiting for 60 seconds before starting NDIC Scraper...")
        time.sleep(60)  # Sleep for 60 seconds

        _logger.critical("Running NDIC Scraper")
        url = os.getenv("NDIC_SCRAPE_URL")
        base_url = os.getenv("NDIC_BASE_URL")
        storage_path = os.getenv("NDIC_STORAGE_DIR")
        source = self.env["rulebook.sources"].search(
            [("name", "ilike", "NDIC")], limit=1)

        user_agent = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0"
        }

        try:
            response = requests.get(url, headers=user_agent, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            _logger.critical(f"Error fetching data from {url}: {str(e)}")
            _logger.critical("Error fetching data from %s: %s", url, str(e))

            return False
        _logger.critical(f"{response.text}")
        soup = BeautifulSoup(response.text, "html.parser")
        pdf_links = [
            link for link in soup.select("ul li a[href]")
        ]
        file_names = soup.select("li a")
        _logger.critical(f"{len(pdf_links)}")

        batch_size = 5
        num_batches = len(pdf_links) // batch_size + \
            (1 if len(pdf_links) % batch_size else 0)

        success_count = 0
        error_count = 0
        threads = []

        for batch_index in range(num_batches):
            start_index = batch_index * batch_size
            end_index = start_index + batch_size

            # Create and start a thread for each batch
            thread = threading.Thread(target=self._process_batch, args=(
                pdf_links[start_index:end_index],
                file_names[start_index:end_index],
                base_url,
                storage_path,
                source
            ))
            threads.append(thread)
            thread.start()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

        _logger.debug(
            f"NDIC Scraper finished: {success_count} records created, {error_count} errors.")

        return success_count > 0

    def _process_batch(self, pdf_links, file_names, base_url, storage_path, source):
        """ Process a batch of PDFs and save to Odoo """
        success_count = 0
        error_count = 0
        records_to_create = []

        for link, file_name in zip(pdf_links, file_names):
            pdf_url = link["href"]
            file_name = self.clean_filename(file_name.text.strip())
            if not pdf_url.startswith(base_url):
                pdf_url = base_url + pdf_url

            filename = basename(urlparse(pdf_url).path)[:-4]
            date_string = self.get_date_from_url(unquote(pdf_url))
            spaced_name = file_name.replace("_", " ")
            filename = f"{unquote(file_name)[:-len(date_string) - 1]}_{date_string}.pdf"

            for attempt in range(3):
                try:
                    pdf_response = requests.get(pdf_url, timeout=10)
                    pdf_response.raise_for_status()

                    os.makedirs(storage_path, exist_ok=True)
                    filepath = os.path.join(storage_path, filename)

                    if not os.path.exists(filepath):
                        with open(filepath, "wb") as f:
                            f.write(pdf_response.content)

                    # existing_title = self.env["rulebook.title"].search(
                    #     [("file_name", "=", spaced_name)], limit=1
                    # )
                    existing_title = self.env["rulebook.title"].search(
                        [("file_name", "=", filename)], limit=1)

                    existing_name = self.env["rulebook.title"].search(
                        [("name", "=", spaced_name)], limit=1)

                    if existing_title or existing_name:
                        _logger.critical(
                            f"Skipping, file '{spaced_name}' already exists.")
                    # if existing_title:
                    #     print(f"Skipping, file '{spaced_name}' already exists.")
                    #     _logger.debug(f"Skipping, file '{spaced_name}' already exists.")
                    else:

                        file_binary_data = self.download_file_as_binary(
                            pdf_url)
                        released_date = self.parse_date(date_string)
                        record_data = {
                            "name": spaced_name,
                            "file": file_binary_data,
                            "file_name": filename,
                            "ref_number": None,
                            "status": "active",
                            "source_id": source.id,
                            "created_on": fields.Datetime.now(),
                            "created_by": self.env.user.id,
                            "external_resource_url": pdf_url,
                            "input_type": "ai"

                        }

                        records_to_create.append(record_data)
                        success_count += 1
                        break  # Exit the retry loop after success

                except requests.RequestException as e:
                    # print(f"Attempt {attempt + 1} failed for {pdf_url}: {str(e)}")
                    _logger.critical(
                        f"Attempt {attempt + 1} failed for {pdf_url}: {str(e)}")
                    time.sleep(2)
            else:
                error_count += 1

        # Commit the records for this batch
        if records_to_create:
            try:
                new_record = self.sudo().create(records_to_create)

                app_base_url = self.env['ir.config_parameter'].sudo(
                ).get_param('web.base.url')
                document_url = f"{app_base_url}/web#id={new_record.id}&model=rulebook.title&view_type=form"

                self._notify_reg_officers(
                    document_url, source.name, new_record.name)
            except Exception as e:
                # print(f"Error creating records for batch: {str(e)}")
                _logger.critical(f"Error creating records for batch: {str(e)}")
                error_count += len(records_to_create)

        _logger.critical(
            f"Batch processed: {success_count} records created, {error_count} errors.")

    @api.model
    def NFIUScraper(self):
        # Get the scrape configuration
        scrape_config_string = os.getenv("NFUI_SCRAPE_CONFIG")
        if not scrape_config_string:
            _logger.error("NFUI_SCRAPE_CONFIG environment variable not set")
            return "Failed: Configuration not found"

        scrape_config = json.loads(scrape_config_string)
        NfiuBaseUrl = os.getenv("NFIU_BASE_URL")

        all_records_to_create = []  # Collect records from all threads

        # Find the source record once
        source = self.env["rulebook.sources"].search(
            [("name", "ilike", "NFIU")], limit=1)
        if not source:
            _logger.error("Source 'NFIU' not found in the rulebook.sources.")
            return "Failed: Source not found"

        def process_batch(pdf_links, file_names, storage_path, base_url, source_id, start, end):
            records = []
            for pdf_link, file_name in zip(pdf_links[start:end], file_names[start:end]):
                full_url = base_url + \
                    pdf_link['href'] if not pdf_link['href'].startswith(
                        'http') else pdf_link['href']
                filename = file_name.text.strip() + '.pdf'

                # Check if file exists before downloading
                file_path = os.path.join(storage_path, filename)
                if os.path.exists(file_path):
                    _logger.info(f"Skipping, file already exists: {filename}")
                    continue

                try:
                    response = requests.get(
                        full_url, timeout=10)  # Set a timeout
                    response.raise_for_status()

                    # Convert file content to base64
                    file_content = base64.b64encode(
                        response.content).decode('utf-8')

                    records.append({
                        'name': filename,
                        'source_id': source_id,
                        'file': file_content,
                        # Add other necessary fields
                    })
                except requests.RequestException as e:
                    _logger.error(f"Failed to download {filename}: {e}")
            return {"records": True}

        for file_type in scrape_config:
            urls = file_type["urls"]
            storage_path = file_type["storage_path"]
            html_tags = file_type["html_tags"]
            nameTag = html_tags[1]

            for url, html_tag in zip(urls, html_tags):
                # Use a timeout for each request
                response = requests.get(url, timeout=10)
                soup = BeautifulSoup(response.text, "html.parser")
                pdf_links = soup.select(html_tag)
                file_names = soup.select(nameTag)

                if not pdf_links or not file_names:
                    _logger.warning(f"No links or names found for URL: {url}")
                    continue

                # Number of workers can be adjusted based on server capability
                with ThreadPoolExecutor(max_workers=5) as executor:
                    batch_size = 5
                    num_batches = len(pdf_links) // batch_size + \
                        (1 if len(pdf_links) % batch_size else 0)

                    futures = []
                    for batch_index in range(num_batches):
                        start_index = batch_index * batch_size
                        end_index = min(
                            start_index + batch_size, len(pdf_links))
                        futures.append(executor.submit(process_batch, pdf_links, file_names,
                                       storage_path, NfiuBaseUrl, source.id, start_index, end_index))

                    for future in futures:
                        try:
                            result = future.result()
                            all_records_to_create.extend(result["records"])
                        except Exception as e:
                            _logger.error(f"Error processing batch: {e}")

        # Create all records in a single batch outside of threading
        if all_records_to_create:
            try:
                # Logging each record's data for debugging
                for record in all_records_to_create:
                    _logger.debug(
                        f"Creating new NFIU record with data: {True}")

                new_record = self.sudo().create(all_records_to_create)
                _logger.info(
                    f"Successfully created {len(all_records_to_create)} records")

                app_base_url = self.env['ir.config_parameter'].sudo(
                ).get_param('web.base.url')
                document_url = f"{app_base_url}/web#id={new_record.id}&model=rulebook.title&view_type=form"

                self._notify_reg_officers(
                    document_url, source.name, new_record.name)

            except Exception as e:
                _logger.critical(f"Error creating records in bulk: {str(e)}")
                return "Failed: Error creating records"

        return "Nfiu Scrape was successful!"

    def clean_filename(self, filename):
        # Replace invalid characters with underscores
        filename = (
            filename.replace("&amp", "and").replace(
                "&#8211;", "_").replace(" ", "_")
        )
        # Remove any other invalid characters
        filename = re.sub(r"[^a-zA-Z0-9_\.]", "", filename)
        return filename

    def store(self, reference, resource_url, download):
        # Method to store the file and return the download URL
        dir_path = self.get_directory("pdf")
        filename = self.make_file_name(reference) + ".pdf"
        filepath = os.path.join(dir_path.get("dir_path"), filename)

        # Save the PDF file to the file system
        if "pdf" in resource_url and not os.path.exists(filepath):
            with open(filepath, "wb") as pdf:
                pdf.write(download.content)

        # Retrieve the base URL for the document download
        base_url = os.getenv("DOCUMENT_DOWNLOAD_BASE_URL")

        if base_url is None:
            raise ValueError(
                "Environment variable DOCUMENT_DOWNLOAD_BASE_URL is not set."
            )

        # Return the download URL if the file exists
        if os.path.exists(filepath):
            return base_url + "/pdf/" + dir_path.get("base_dir") + "/" + filename
        return None

    def get_directory(self, subfolder):
        # Assuming there's a predefined directory for storing PDF files
        base_dir = os.getenv(
            "CBN_DOCUMENT_PATH"
        )  # Example base folder, adjust based on actual config
        dir_path = os.path.join(base_dir, subfolder)

        # Create the directory if it doesn't exist
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        return {
            "base_dir": base_dir,
            "dir_path": dir_path,
        }

    def make_file_name(self, reference):
        # Create a valid file name from the reference by removing special characters
        return re.sub(r"[^a-zA-Z0-9_]", "_", reference)

    def download_file_as_binary(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Convert the file content to base64-encoded binary data
            file_binary_data = base64.b64encode(response.content)
            return file_binary_data
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Failed to download file from {url}: {str(e)}")

    def convert_date_format(self, date_str):
        try:
            # Convert the incoming date from 'MM/DD/YYYY' to 'YYYY-MM-DD'
            parsed_date = datetime.strptime(date_str, "%m/%d/%Y")
            formatted_date = parsed_date.strftime("%Y-%m-%d")
            return formatted_date
        except ValueError as e:
            raise ValueError(f"Error processing date '{date_str}': {e}")

    def get_date_from_url(self, url):
        date_patterns = [
            # Add re.IGNORECASE for month names with day and year
            (re.compile(r"[A-Za-z]{1,15}-\d{1,2}-\d{4}",
                        re.IGNORECASE), "%B-%d-%Y"),
            (re.compile(r"[A-Za-z]{1,15}-\d{1,2}-\d{4}",
                        re.IGNORECASE), "%b-%d-%Y"),
            (re.compile(r"\d{1,2}-[A-Za-z]{1,15}-\d{4}",
                        re.IGNORECASE), "%d-%B-%Y"),
            (re.compile(r"[A-Za-z]{1,15}-\d{1,2}-\d{4}",
                        re.IGNORECASE), "%B-%d-%Y"),
            (re.compile(r"\d{1,2}[A-Za-z]{3}\d{4}", re.IGNORECASE), "%d%b%Y"),
            # Defaults to day = 01
            (re.compile(r"[A-Za-z]{3,10}-\d{4}", re.IGNORECASE), "%b-%Y"),
            (re.compile(r"\d{4}/\d{2}", re.IGNORECASE),
             "%Y/%m"),           # Defaults to day = 01
            (re.compile(r"\([A-Za-z]{3,10}\d{4}\)"),
             "(%B%Y)"),  # Defaults to day = 01
            (re.compile(r"\b\d{4}\b"), "%Y")  # Defaults to month/day = 01

        ]

        # Exclude known non-date patterns
        if re.search(r"(ISA-|ACT-|SEC-Guideline)", url, re.IGNORECASE):
            _logger.warning(f"Non-date pattern found (ignoring): {url}")
            return ""

        # Loop over the patterns and check for matches
        for pattern, date_format in date_patterns:
            match = pattern.search(url)
            if match:
                # Get the matched date string
                date = match.group(0)
                # Log the matched date
                _logger.debug(f"Matched date: '{date}' from URL: {url}")

                try:
                    # Handle Month-Year format by appending '-01' for the day
                    if date_format == "%b-%Y":
                        date_obj = datetime.strptime(date + "-01", "%b-%Y-%d")
                    elif date_format == "%d-%B-%Y":
                        date_obj = datetime.strptime(date, date_format)
                    elif date_format == "%d%b%Y":
                        date_obj = datetime.strptime(date, date_format)
                    else:
                        date_obj = datetime.strptime(date, date_format)

                    # Return the formatted date as YYYY-MM-DD
                    formatted_date = date_obj.strftime("%Y-%m-%d")
                    return formatted_date
                except ValueError:
                    _logger.warning(
                        f"Failed to parse date '{date}' with format '{date_format}' from URL: {url}")
            else:
                _logger.debug(
                    f"No match found for pattern '{pattern}' in URL: {url}")
                return ""

        # If no pattern matches, log and return an empty string
        _logger.warning(f"No date pattern matched for URL: {url}")
        return ""

    def get_filename(self, string):
        parsed_url = urlparse(string)
        filename = os.path.basename(parsed_url.path)
        filename_without_extension = os.path.splitext(filename)[0]
        # Normalize filename by replacing spaces with underscores and removing special characters
        return filename_without_extension

    @api.model
    def clean_duplicate_records(self):
        batch_size = 200  # Adjust based on server capacity
        offset = 0

        # Exclude records where source_id is 11 or 'Bank of Industry'
        excluded_sources = [
            11] + self.env['rulebook.sources'].search([('name', '=ilike', 'Bank of Industry')]).ids

        while True:
            try:
                # Set savepoint before starting batch processing
                self.env.cr.execute('SAVEPOINT clean_duplicate_records_batch')

                records = self.with_context(prefetch_fields=False).search(
                    [('source_id', 'not in', excluded_sources)], limit=batch_size, offset=offset)

                if not records:
                    break  # No more records to process

                grouped_records = {}
                for record in records:
                    key = (record.name, record.file_name, record.released_date)
                    if key not in grouped_records:
                        grouped_records[key] = []
                    grouped_records[key].append(record)

                for key, duplicates in grouped_records.items():
                    if len(duplicates) > 1:
                        # Sort by whether the file field is set (not loading the file itself)
                        sorted_duplicates = sorted(duplicates,
                                                   key=lambda r: (
                                                       1 if bool(r.file) else 0, r.create_date),
                                                   reverse=True)

                        keep_record = sorted_duplicates[0]

                        # Delete duplicates, handling potential errors
                        for duplicate in sorted_duplicates[1:]:
                            try:
                                duplicate.unlink()
                                # self.env.cr.commit()
                            except Exception as e:
                                _logger.error(
                                    f"Error deleting duplicate {duplicate.id}: {str(e)}")
                                # Optionally, rollback to savepoint if critical
                                # self.env.cr.execute('ROLLBACK TO SAVEPOINT clean_duplicate_records_batch')
                                # break

                # Handle records without files
                records_without_files = self.search(
                    [('file', '=', False), ('source_id', 'not in', excluded_sources)],
                    limit=batch_size, offset=0)
                for record in records_without_files:
                    try:
                        record.unlink()
                    except Exception as e:
                        _logger.error(
                            f"Error deleting record without file {record.id}: {str(e)}")

                # If batch processed successfully, commit
                self.env.cr.execute(
                    'RELEASE SAVEPOINT clean_duplicate_records_batch')
                self.env.cr.commit()  # Commit after each batch if successful

            except Exception as e:
                _logger.error(
                    f"Error processing batch at offset {offset}: {e}")
                self.env.cr.rollback()  # Rollback the transaction for this batch
            finally:
                offset += batch_size  # Move to the next batch

        return True

    def parse_date(self, date_string):
        _logger.critical(f"{date_string}")

        # List of date formats to check
        formats = ["%Y-%m-%d", "%Y-%m", "%Y"]

        # Try each format one by one
        for fmt in formats:
            try:
                return datetime.strptime(date_string, fmt)
            except ValueError:
                continue  # If format doesn't match, try the next one

        # If none of the formats work, raise an error
        # raise ValueError(f"Date format for '{date_string}' is not supported")
        return ""

    def action_scan_keywords(self):
        """Button action to scan the document for keywords and alert officers if found."""
        self.ensure_one()
        if not self.file:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': ('Warning'),
                    'message': ('No file attached to scan.'),
                    'type': 'warning',
                    'sticky': False,
                }
            }

        # Get the PDF content
        pdf_content = self._get_pdf_content(self)

        if not pdf_content:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': ('Error'),
                    'message': ('Could not retrieve PDF content.'),
                    'type': 'danger',
                    'sticky': False,
                }
            }

        # Extract text from the PDF
        extracted_text = self._extract_text_from_pdf(pdf_content)

        if not extracted_text or extracted_text.startswith("Error:"):
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': ('Error'),
                    'message': ('Failed to extract text from PDF: %s') % extracted_text,
                    'type': 'danger',
                    'sticky': False,
                }
            }

        # Get all active keywords
        keywords = self.env['keyword.tracking'].search([('active', '=', True)])

        if not keywords:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': ('Information'),
                    'message': ('No active keywords to scan for.'),
                    'type': 'info',
                    'sticky': False,
                }
            }

        # Search for keywords in the text
        found_keywords = []

        for keyword in keywords:
            # Use regex to find the keyword with word boundaries
            pattern = r'\b' + re.escape(keyword.name) + r'\b'
            matches = re.finditer(pattern, extracted_text, re.IGNORECASE)

            for match in matches:
                # Get surrounding text (100 characters before and after)
                start = max(0, match.start() - 100)
                end = min(len(extracted_text), match.end() + 100)
                surrounding_text = extracted_text[start:end]

                # Format the text to highlight the keyword
                matched_word = extracted_text[match.start():match.end()]
                highlighted_text = surrounding_text.replace(
                    matched_word,
                    f"**{matched_word}**"  # Using markdown for highlighting
                )

                found_keywords.append({
                    'keyword': keyword,
                    'matched_text': highlighted_text,
                    'risk_level': keyword.risk_level,
                })

                # Only process the first occurrence of each keyword
                break

        if not found_keywords:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': ('Information'),
                    'message': ('No keywords found in the document.'),
                    'type': 'info',
                    'sticky': False,
                }
            }

        # Process the found keywords
        self._process_found_keywords(found_keywords)

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': ('Success'),
                'message': ('%s keywords found and alerts sent.') % len(found_keywords),
                'type': 'success',
                'sticky': False,
            }
        }

    def _process_found_keywords(self, found_keywords):
        """Process the found keywords and consolidate into a single alert."""
        # Initialize variables for consolidated alert
        keyword_ids = []
        matched_texts = []
        risk_levels = []
        all_keywords = []

        # Collect data from all found keywords
        for found in found_keywords:
            keyword = found['keyword']
            matched_text = found['matched_text']
            risk_level = found['risk_level']

            keyword_ids.append(keyword.id)
            matched_texts.append(
                f"Keyword '{keyword.name}':\n{matched_text}\n")
            risk_levels.append(risk_level)
            all_keywords.append(keyword.name)

        # Determine the highest risk level
        highest_risk = 'low'
        if 'high' in risk_levels:
            highest_risk = 'high'
        elif 'medium' in risk_levels:
            highest_risk = 'medium'

        # Combine all matched texts
        combined_text = "\n---\n".join(matched_texts)

        # Create a single keyword alert log
        alert_log = self.env['keyword.alert.log'].create({
            'name': self.env['ir.sequence'].next_by_code('keyword.alert.log') or 'New',
            # Replace with all found keyword IDs
            'keyword_id': [(6, 0, keyword_ids)],
            'document_id': self.id,
            'risk_level': highest_risk,
            'match_text': combined_text,
        })

        # Send AI analysis to add context for all keywords
        ai_analysis = self._analyze_keyword_context(
            ", ".join(all_keywords), combined_text)

        alert_log.sudo().write({
            'ai_analysis': ai_analysis,
        })

        # Send a single consolidated email to all assigned officers
        self._send_consolidated_alert_email(alert_log, ai_analysis)

    def _analyze_keyword_context(self, keywords, context_text):
        """Send the matched text to AI for analysis."""
        prompt = f"""
        Analyze the following text where these keywords were found: {keywords}
        
        {context_text}
        
        Please provide a brief analysis of:
        1. The context in which each keyword appears
        2. Potential implications or concerns
        3. Recommended actions if any
        
        IMPORTANT FORMATTING INSTRUCTIONS:
        - Format your response with clean section headers (do NOT use asterisks for emphasis)
        - Use "Section 1: Context" format for main sections
        - For bullet points, use simple dash (-) without asterisks
        - If you need to emphasize text, use standard formatting without special characters
        
        Keep your response concise and focused on the regulatory or compliance implications.
        """

        return self.query_gemini_api(prompt)

    def _send_consolidated_alert_email(self, alert_log, ai_analysis):
        """Send a consolidated alert email to all assigned officers."""
        try:
            # Load template
            template = self.env.ref(
                'rule_book.email_template_keyword_alert', raise_if_not_found=False)
            if not template:
                _logger.error("Email template for keyword alerts not found!")
                return

            # Get officers with valid emails
            officers = alert_log.officers_alerted.filtered('email')
            if not officers:
                _logger.warning(
                    "No officers with valid email addresses found!")
                return

            # Generate document URL
            base_url = self.env['ir.config_parameter'].sudo(
            ).get_param('web.base.url')
            document_url = f"{base_url}/web#id={self.id}&model=rulebook.title&view_type=form"

            formatted_analysis = self._format_email_analysis(ai_analysis)

            # Prepare context with all required data
            ctx = {
                # 'ai_analysis': ai_analysis,
                'ai_analysis': formatted_analysis,
                'document_name': self.name,
                'document_id': self.id,
                'document_url': document_url,
                'keywords': ", ".join(alert_log.keyword_id.mapped('name')),
                'risk_level': alert_log.risk_level,
                # 'officer_name': officers[0].name if len(officers) == 1 else "Officer",
                'officer_name': ", ".join(officers.mapped('name')),
                'email_from': os.getenv("EMAIL_FROM"),
            }

            # Prepare email values
            email_values = {
                'email_to': ", ".join(officers.mapped('email')),
                'email_from': os.getenv("EMAIL_FROM"),
                'subject': f"ALERT: Keywords Found in Document '{self.name}'",
            }

            # Send email using template
            try:
                template_id = template.with_context(**ctx)
                template_id.send_mail(
                    alert_log.id,
                    force_send=True,
                    email_values=email_values
                )

                _logger.info(
                    f"Consolidated alert email sent to {email_values['email_to']} for keywords: {ctx['keywords']}")
            except Exception as e:
                _logger.error(
                    f"Failed to send consolidated alert email: {str(e)}")
                raise

        except Exception as e:
            _logger.error(
                f"Error in sending consolidated alert email: {str(e)}", exc_info=True)
            return

    def _get_pdf_content(self, rulebook):
        """Retrieve the actual PDF content from the rulebook."""
        if not rulebook.file:
            return None

        # Try to get the attachment directly
        attachment = self.env['ir.attachment'].search([
            ('res_model', '=', 'rulebook.title'),
            ('res_id', '=', rulebook.id),
            ('res_field', '=', 'file')
        ], limit=1)

        if attachment:
            _logger.info(
                f"Found attachment: {attachment.name}, size: {attachment.file_size}")

            # Try a direct approach to get the file contents
            try:
                # For Odoo v14+ with attachment store
                if hasattr(attachment, '_full_path'):
                    store_fname = attachment.store_fname
                    if store_fname:
                        full_path = attachment._full_path(store_fname)
                        _logger.info(
                            f"Reading file directly from: {full_path}")
                        with open(full_path, 'rb') as f:
                            file_content = f.read()
                            return file_content

                # Another approach for getting the binary data
                if hasattr(attachment, 'raw'):
                    _logger.info("Using attachment.raw to get data")
                    return attachment.raw

                # For database storage
                _logger.info("Using standard datas field")
                raw_datas = attachment.datas

                # If it's a string (base64), decode it
                if isinstance(raw_datas, str):
                    # Remove any padding issues
                    padding = len(raw_datas) % 4
                    if padding:
                        raw_datas += '=' * (4 - padding)

                    try:
                        return base64.b64decode(raw_datas)
                    except Exception as e:
                        _logger.error(
                            f"Failed to decode attachment.datas: {e}")

                return raw_datas
            except Exception as e:
                _logger.error(f"Error accessing attachment data: {e}")

        # If all else fails, try the original file
        try:
            return base64.b64decode(rulebook.file)
        except Exception as e:
            _logger.error(f"Failed to decode rulebook.file: {e}")
            return None

    def _extract_text_from_pdf(self, pdf_file):
        """Extract text from the provided binary PDF file."""
        if not pdf_file:
            _logger.error("No PDF file provided")
            return "No PDF content available"

        # Make sure we're working with binary data
        if isinstance(pdf_file, str):
            try:
                pdf_file = base64.b64decode(pdf_file)
            except Exception as e:
                _logger.error(f"Failed to decode base64 string: {e}")
                return "Error: Could not decode PDF data"

        # Safety check - is this actually a PDF?
        if not pdf_file.startswith(b'%PDF'):
            _logger.error(
                "Data does not appear to be a valid PDF (missing PDF header)")
            # Log a sample of the data for debugging
            sample = pdf_file[:100].hex()
            # _logger.info(f"First 100 bytes (hex): {sample}")
            return "Error: File does not appear to be a valid PDF"

        # Get a file-like object
        pdf_stream = io.BytesIO(pdf_file)

        try:
            # Try PyPDF2
            reader = PyPDF2.PdfReader(pdf_stream, strict=False)
            text = ""
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"

            if text.strip():
                return text

            # If no text extracted, fallback to OCR
            return self._extract_text_from_image_pdf(pdf_file)
        except Exception as e:
            _logger.error(f"PyPDF2 error: {e}")
            # Fallback to OCR
            return self._extract_text_from_image_pdf(pdf_file)

    def _extract_text_from_image_pdf(self, pdf_data):
        """Convert PDF pages to images and use OCR."""
        try:
            _logger.info("Attempting OCR conversion of PDF")
            pages = convert_from_bytes(pdf_data)
            text = ""

            for i, page in enumerate(pages):
                _logger.info(f"Processing page {i+1} with OCR")
                page_text = pytesseract.image_to_string(page)
                text += page_text + "\n"

            if not text.strip():
                return "OCR processing did not extract any text"

            return text
        except Exception as e:
            _logger.error(f"OCR processing error: {e}")
            return f"Error during OCR processing: {str(e)}"

    def query_gemini_api(self, prompt):
        apikey = os.getenv("GEMINI_API")
        """Function to send the prompt to the Gemini API and return the response."""
        api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={apikey}"
        headers = {
            'Content-Type': 'application/json',
        }

        payload = {
            "contents": [
                {
                    "parts": [
                        {"text": prompt}
                    ]
                }
            ]
        }

        try:
            response = requests.post(api_url, headers=headers, json=payload)
            response.raise_for_status()  # Raise an error for bad responses

            # Extract the text from the response
            candidates = response.json().get('candidates', [])
            if candidates:
                # Assuming you want the text from the first candidate
                text_parts = candidates[0].get('content', {}).get('parts', [])
                if text_parts:
                    return ''.join(part['text'] for part in text_parts)

        except requests.exceptions.HTTPError as http_err:
            # Handle HTTP errors
            _logger.info(f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            # Handle connection errors
            _logger.info(f"Connection error occurred: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            # Handle timeout errors
            _logger.info(f"Timeout error occurred: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            # Handle any other request errors
            _logger.info(f"An error occurred: {req_err}")
        except Exception as e:
            # Handle unexpected errors
            _logger.info(f"An unexpected error occurred: {e}")

        return "An error occurred while analyzing the keyword context"

    def open_keyword_log(self):
        # Define your action here
        action = self.env.ref(
            "rule_book.action_keyword_alert_log").sudo().read()[0]

        # Set the default domain to show tickets with matching issue
        id = self.id
        action["domain"] = [("document_id", "=", id)]

        return action

    def action_view_rulebooks(self):
        self.ensure_one()
        return {
            'name': 'Rulebooks',
            'type': 'ir.actions.act_window',
            'res_model': 'rulebook',
            'view_mode': 'form',
            'domain': [('name', '=', self.id)],
            # Pre-fill the title when creating new rulebook
            'context': {'default_name': self.id},
        }

    def copy_email_to_alert_log(self, alert_log_id=None):
        """Copy email messages from rulebook.title to keyword.alert.log"""
        self.ensure_one()

        # Find alert logs to copy to
        if not alert_log_id:
            alert_logs = self.env['keyword.alert.log'].search(
                [('document_id', '=', self.id)])
        else:
            alert_logs = self.env['keyword.alert.log'].browse([alert_log_id])

        if not alert_logs:
            return False

        # Get email messages
        messages = self.env['mail.message'].search([
            ('res_id', '=', self.id),
            ('model', '=', 'rulebook.title'),
            ('message_type', 'in', ['email', 'notification'])
        ], order='create_date asc')

        if not messages:
            return False

        # Copy messages to each alert log
        for alert_log in alert_logs:
            for message in messages:
                # Skip if already copied
                existing = self.env['mail.message'].search([
                    ('res_id', '=', alert_log.id),
                    ('model', '=', 'keyword.alert.log'),
                    ('parent_id', '=', message.id)
                ], limit=1)

                if not existing:
                    # Copy message
                    message.copy({
                        'model': 'keyword.alert.log',
                        'res_id': alert_log.id,
                        'parent_id': message.id
                    })

        return alert_logs

    # Override message_post for this specific model only
    @api.returns('mail.message', lambda value: value.id)
    def message_post(self, *args, **kwargs):
        """Copy emails to alert logs when posted from rulebook.title"""
        message = super(RulebookTitle, self).message_post(*args, **kwargs)

        # Only process email/notification messages
        if message.message_type in ['email', 'notification']:
            self.copy_email_to_alert_log()

        return message

    def _format_email_analysis(self, analysis_text):
        """Format AI analysis text for proper display in email."""
        # Convert plain text newlines to HTML paragraphs
        paragraphs = analysis_text.split('\n\n')

        # Process each paragraph
        formatted_paragraphs = []
        for para in paragraphs:
            if para.strip():
                # Check if it's a list item
                if para.strip().startswith('-'):
                    # Format list items
                    list_items = para.split('\n-')
                    # Use div instead of ul/li for better control
                    list_html = '<div style="margin-left: 20px;">'
                    for i, item in enumerate(list_items):
                        if i == 0:  # First item may not start with dash
                            item = item.lstrip('- ')
                        # Using div with dash prefix instead of li
                        list_html += f'<div style="margin-bottom: 8px; text-indent: -12px; padding-left: 12px;">- {item.strip()}</div>'
                    list_html += '</div>'
                    formatted_paragraphs.append(list_html)
                # Check if it's a section header
                elif any(section in para for section in ["Section", "Context:", "Implications:", "Actions:"]):
                    # Format as a heading
                    formatted_paragraphs.append(
                        f'<h4 style="margin-top: 16px; margin-bottom: 8px; font-weight: normal;">{para}</h4>')
                else:
                    # Regular paragraph
                    formatted_paragraphs.append(
                        f'<p style="margin-bottom: 12px;">{para}</p>')

        # Join all formatted paragraphs
        return ''.join(formatted_paragraphs)

    def _notify_reg_officers(self, action_url, regulator, name):
        """Send consolidated email notification to responsible officers"""
        try:
            template = self.env.ref(
                'rule_book.email_template_reg_document_alert')
            if not template:
                _logger.error(
                    "Email template not found")
                raise ValidationError("Email template not found")
            # Search for all records in reg.model
            reg_records = self.env['regulatory.alert'].search([])
            # Collect all users from the alert_officers Many2many field
            officers = reg_records.mapped('alert_officers')
            if not officers:
                _logger.warning("No officers configured for alerts")
                return
            # Get email from address
            email_from = os.getenv("EMAIL_FROM")
            if not email_from:
                _logger.error("EmailFrom environment variable not configured")
                raise ValidationError("Email sender address not configured")
            # Prepare email values
            officers_name = ", ".join(officers.mapped('name')) or ""
            officers_email = ", ".join(officers.mapped('email')) or ""

            # Store the data in the record for template access
            now = datetime.now()
            now_without_microseconds = now.replace(microsecond=0)
            timestamp = self.env["reply.log"]._compute_formatted_date(
                now_without_microseconds) if hasattr(self.env["reply.log"], "_compute_formatted_date") else str(now_without_microseconds)


            email_data = {
                'regulator': regulator,
                'record_link': action_url,
                'email_from': email_from,
                'officers_name': officers_name,
                'datetime': timestamp,
                'title': name,
                "current_year": datetime.now().year,
                'email_to': officers_email,
                'email_cc': '',  # Add an empty email_cc to prevent KeyError
                'subject': 'New Regulatory Document Detected',
                'body_html': f'A new regulatory document has been retrieved by the system and is now available for your review.'
            }

            # Store the data for this specific record
            self.set_email_data(email_data)

            try:
                # Render the template with context
                template_id = template.with_context(
                    regulator=regulator,
                    record_link=action_url,
                    email_from=email_from,
                    officers_name=officers_name,
                    datetime=timestamp,
                    title= name,
                    current_year=datetime.now().year,
                    email_to=officers_email
                )

                email_result = template_id.send_mail(
                    self.id,
                    force_send=True,
                    raise_exception=True,
                    email_values={
                        'email_to': officers_email,
                        'email_from': email_from,
                        'email_cc': '',
                    }
                )

                mail = self.env['mail.mail'].browse(email_result)
                if mail.state == 'sent':
                    # insert into alert history table
                    _logger.info(
                        f"Consolidated email sent to regulatory alert officers ({officers_email})")

                # Clean up the stored email data after sending
                if hasattr(self, '_email_data') and self.id in self._email_data:
                    del self._email_data[self.id]

            except Exception as e:
                _logger.error(
                    f"Failed to send regulatory alert email: {str(e)}")
                raise
        except Exception as e:
            _logger.error(
                f"Error in regulatory alert process: {str(e)}", exc_info=True)
            raise ValidationError(
                f"Failed to send regulatory alert email: {str(e)}")
