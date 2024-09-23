from odoo import models, fields, api
import os
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from odoo import fields, models, api
from dotenv import load_dotenv
import base64
from urllib.parse import unquote, urlparse
from os.path import basename
import json
import time
import threading

load_dotenv()

class RulebookTitle(models.Model):
    _name = 'rulebook.title'
    _description = 'Rulebook Titles'
    _rec_name = 'name'

    name = fields.Char(string='Title', required=True)
    file = fields.Binary(string='File', attachment=True, required=False)
    file_name = fields.Char(string='File Name')
    ref_number = fields.Char(string='Reference Number', required=False)
    released_date = fields.Date(string='Released Date', required=False)
    status = fields.Selection([
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('deleted', 'Deleted')
    ], string='Status', default='active', required=True)
    source_id = fields.Many2one('rulebook.sources', string='Source', required=True)
    created_on = fields.Datetime(string='Created On', default=fields.Datetime.now, required=True)
    created_by = fields.Many2one('res.users', string='Created By', default=lambda self: self.env.user, readonly=True)
    # Add the external_resource_url field if it's not already defined
    external_resource_url = fields.Char("External Resource URL")

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
        print(f"A new record is being created by user: {self.env.user.name} (ID: {self.env.user.id})")
        # print(f"Values used for creation: {vals}")

        # Call the super method to actually create the record
        record = super(RulebookTitle, self).create(vals)

        # Print/log the newly created record details
        print(f"New record created with ID: {record.id}")

        return record

    # general webscrapping functino
    def run_webscrapping(self):
        print("web scrapper run sucessfully")
        # self.CBNScrapper()
        # self.NDICScrapper()
        self.NFIUScraper()

    @api.model
    def CBNScrapper(self):
        print("CBN Scraper processing")
        url = os.getenv("CBN_URL")

        # Send a GET request to the URL
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Find the table with the specified class
            table = soup.find("table", class_="dbasetable")

            if table:
                # Iterate over each row (tr) in the table
                for row in table.find_all("tr"):
                    # Extract data from each cell (td) in the row
                    cells = row.find_all("td")
                    # Extract the download link (href) from the second cell
                    download_link = (
                        cells[1].find("a")["href"]
                        if cells and cells[1].find("a")
                        else ""
                    )
                    resource_url = (
                        "https://www.cbn.gov.ng"
                        + requests.utils.requote_uri(download_link)
                    )
                    reference = cells[0].text.strip() if cells else ""
                    size = cells[2].text.strip() if cells and len(cells) > 2 else ""
                    # Extract and parse the title and publication date
                    title_text = (
                        cells[1].text.strip() if cells and len(cells) > 1 else ""
                    )
                    title = title_text
                    published = ""
                    if "Published" in title_text:
                        title_tokens = re.split("Published", title_text)
                        title = title_tokens[0].strip() if title_tokens[0] else ""
                        published = title_tokens[1].strip() if title_tokens[1] else ""

                    # Parse the published date if available
                    published_date = ""
                    if len(published) > 1:
                        published_tokens = re.split(" ", published)
                        published_date = published_tokens[0].strip()

                    # Download the resource
                    download = requests.get(resource_url)
                    print(download)
                    print(len(reference.strip()))
                    download_url = self.store(reference, resource_url, download)
                    if len(reference.strip()) > 0:
                        # Fetch the source_id where the rulebook.sources.name is like 'CBN'
                        source = self.env["rulebook.sources"].search(
                            [("name", "ilike", "CBN")], limit=1
                        )
                        print(source.id)

                        if not source:
                            raise ValueError(
                                "Source 'CBN' not found in the rulebook.sources."
                            )

                        # Download the file from resource_url and convert it to binary
                        file_binary_data = self.download_file_as_binary(resource_url)

                        # Create the record in rulebook.title
                        self.env["rulebook.title"].create(
                            {
                                "name": title,  # Corresponds to the 'Title' field
                                # "file": file_binary_data,
                                "file_name": reference,  # Use reference as the file name
                                "ref_number": reference,  # Reference number
                                "released_date": (
                                    self.convert_date_format(published_date)
                                    if published_date
                                    else None
                                ),  # Released Date
                                "status": "active",  # Default status to 'active'
                                "source_id": source.id,  # Source ID (from rulebook.sources where name like 'CBN')
                                "created_on": fields.Datetime.now(),  # Current timestamp
                                "created_by": self.env.user.id,  # Created by the current user
                                "external_resource_url": resource_url,  # URL where the file was downloaded from
                            }
                        )

        else:
            print(f"Failed to retrieve the page. Status code: {response.status_code}")

    def NDICScrapper(self):
        print("Running NDIC Scraper")
        url = os.getenv("NDIC_SCRAPE_URL")
        base_url = os.getenv("NDIC_BASE_URL")
        storage_path = os.getenv("NDIC_STORAGE_DIR")
        source = self.env["rulebook.sources"].search([("name", "ilike", "NDIC")], limit=1)

        user_agent = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0"
        }

        try:
            response = requests.get(url, headers=user_agent, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching data from {url}: {str(e)}")
            return False
        print(response.text)
        soup = BeautifulSoup(response.text, "html.parser")
        pdf_links = [
            link for link in soup.select("ul li a[href]")
        ]
        file_names = soup.select("li a")
        print(len(pdf_links))

        return len(pdf_links)

        batch_size = 5
        num_batches = len(pdf_links) // batch_size + (1 if len(pdf_links) % batch_size else 0)

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

        print(f"NDIC Scraper finished: {success_count} records created, {error_count} errors.")
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

                    file_binary_data = self.download_file_as_binary(pdf_url)
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
                    }

                    records_to_create.append(record_data)
                    success_count += 1
                    break  # Exit the retry loop after success

                except requests.RequestException as e:
                    print(f"Attempt {attempt + 1} failed for {pdf_url}: {str(e)}")
                    time.sleep(2)
            else:
                error_count += 1

        # Commit the records for this batch
        if records_to_create:
            try:
                self.sudo().create(records_to_create)
            except Exception as e:
                print(f"Error creating records for batch: {str(e)}")
                error_count += len(records_to_create)

        print(f"Batch processed: {success_count} records created, {error_count} errors.")

    def NFIUScraper(self):
        # Get the scrape configuration
        scrape_config_string = os.getenv("NFUI_SCRAPE_CONFIG")
        scrape_config = json.loads(scrape_config_string)

        file_types = scrape_config
        NfiuBaseUrl = os.getenv("NFIU_BASE_URL")

        all_records_to_create = []  # Collect records from all threads

        for file_type in file_types:
            urls = file_type["urls"]
            storage_path = file_type["storage_path"]
            html_tags = file_type["html_tags"]
            nameTag = html_tags[1]

            for url, html_tag in zip(urls, html_tags):
                response = requests.get(url)
                soup = BeautifulSoup(response.text, "html.parser")
                pdf_links = soup.select(html_tag)
                file_names = soup.select(nameTag)

                source = self.env["rulebook.sources"].search([("name", "ilike", "NFIU")], limit=1)
                if not source:
                    raise ValueError("Source 'NFIU' not found in the rulebook.sources.")

                batch_size = 5
                num_batches = len(pdf_links) // batch_size + (1 if len(pdf_links) % batch_size else 0)

                threads = []
                thread_results = []

                for batch_index in range(num_batches):
                    start_index = batch_index * batch_size
                    end_index = min(start_index + batch_size, len(pdf_links))

                    # Create and start a thread for each batch
                    thread = threading.Thread(
                        target=self.process_nfiu_batch,
                        args=(
                            pdf_links,
                            file_names,
                            storage_path,
                            NfiuBaseUrl,
                            source.id,
                            start_index,
                            end_index,
                            thread_results,
                        ),
                    )
                    threads.append(thread)
                    thread.start()

                # Wait for all threads to finish
                for thread in threads:
                    thread.join()

                # Collect records from all threads
                for result in thread_results:
                    all_records_to_create.extend(result["records"])

        # Create all records in a single batch outside of threading
        if all_records_to_create:
            try:
                self.sudo().create(all_records_to_create)
            except Exception as e:
                print(f"Error creating records in bulk: {str(e)}")

        return "Nfiu Scrape was successful!"    

    def process_nfiu_batch(
        self,
        pdf_links,
        file_names,
        storage_path,
        NfiuBaseUrl,
        source_id,
        batch_start,
        batch_end,
        thread_results,  # List to store results
    ):
        success_count = 0
        error_count = 0
        records_to_create = []

        for link, file_name in zip(
            pdf_links[batch_start:batch_end], file_names[batch_start:batch_end]
        ):
            pdf_url = link["href"]
            title = file_name.get("title", "")

            if title:
                file_name = title
            else:
                file_name = file_name.get_text().strip()
            file_name = file_name.replace("Download", "").replace(".pdf", "")

            if not pdf_url.startswith(NfiuBaseUrl):
                pdf_url = NfiuBaseUrl + pdf_url

            filename = basename(urlparse(pdf_url).path)[:-4]
            date_string = self.get_date_from_url(unquote(pdf_url))
            noSpaceFileName = (
                self.clean_filename(file_name).removeprefix("_").removesuffix("_")
            )
            filename = (
                f"{unquote(noSpaceFileName)[:-len(date_string)-1]}_{date_string}.pdf"
            )

            for attempt in range(3):
                try:
                    pdf_response = requests.get(pdf_url, timeout=10)
                    pdf_response.raise_for_status()

                    if not os.path.exists(storage_path):
                        os.makedirs(storage_path)

                    filepath = os.path.join(storage_path, filename)

                    if not os.path.exists(filepath):
                        with open(filepath, "wb") as f:
                            f.write(pdf_response.content)

                    file_binary_data = self.download_file_as_binary(pdf_url)
                    released_date = self.parse_date(date_string)

                    record_data = {
                        "name": file_name.replace("_", " "),
                        "file": file_binary_data,
                        "file_name": filename,
                        "ref_number": None,
                        "released_date": fields.Datetime.now(),
                        "status": "active",
                        "source_id": source_id,
                        "created_on": fields.Datetime.now(),
                        "created_by": self.env.user.id,
                        "external_resource_url": pdf_url,
                    }

                    records_to_create.append(record_data)
                    success_count += 1
                    break

                except requests.RequestException as e:
                    print(f"Attempt {attempt + 1} failed for {pdf_url}: {str(e)}")
                    time.sleep(2)
            else:
                error_count += 1

        # Append the results to the shared thread_results list
        thread_results.append({"records": records_to_create, "success": success_count, "errors": error_count})

        return success_count, error_count

    def clean_filename(self, filename):
        # Replace invalid characters with underscores
        filename = (
            filename.replace("&amp", "and").replace("&#8211;", "_").replace(" ", "_")
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
        date_pattern1 = re.compile(r"[A-Za-z]{1,15}-\d{1,2}-\d{4}")
        date_pattern2 = re.compile(r"\d{1,2}-[A-Za-z]{1,15}-\d{4}")
        date_pattern3 = re.compile(r"[A-Za-z]{1,15}-\d{1,2}-\d{4}")
        date_pattern4 = re.compile(r"\d{1,2}[A-Za-z]{3}\d{4}")
        date_pattern5 = re.compile(r"[A-Za-z]{3,10}-\d{4}")
        date_pattern6 = re.compile(r"\d{4}/\d{2}")
        date_pattern7 = re.compile(r"\([A-Za-z]{3,10}\d{4}\)")
        date_pattern8 = re.compile(r"\b\d{4}\b")

        match1 = date_pattern1.search(url)
        match2 = date_pattern2.search(url)
        match3 = date_pattern3.search(url)
        match4 = date_pattern4.search(url)
        match5 = date_pattern5.search(url)
        match6 = date_pattern6.search(url)
        match7 = date_pattern7.search(url)
        match8 = date_pattern8.search(url)

        if match1:
            date = match1.group(0)
            # return date
            try:
                date_obj = datetime.strptime(date, "%b-%d-%Y")
                formatted_date = date_obj.strftime("%d-%m-%Y")
                return formatted_date
            except ValueError:
                pass  # Invalid date format, continue to the next pattern

        if match2:
            date = match2.group(0)
            # return date
            try:
                date_obj = datetime.strptime(date, "%d-%B-%Y")
                formatted_date = date_obj.strftime("%d-%m-%Y")
                return formatted_date
            except ValueError:
                pass  # Invalid date format
        if match3:
            date = match3.group(0)
            # return date
            try:
                date_obj = datetime.strptime(date, "%B-%d-%Y")
                formatted_date = date_obj.strftime("%d-%m-%Y")
                return formatted_date
            except ValueError:
                pass  # Invalid date format
        if match4:
            date = match4.group(0)
            # return date
            try:
                date_obj = datetime.strptime(date, "%d%b%Y")
                formatted_date = date_obj.strftime("%d-%m-%Y")

                return formatted_date
            except ValueError:
                pass  # Invalid date format, continue to the next pattern
        if match5:
            date = match5.group(0)
            # return date
            try:
                date_obj = datetime.strptime(date, "%b-%Y")
                formatted_date = date_obj.strftime("%m-%Y")

                return formatted_date
            except ValueError:
                pass  # Invalid date format, continue to the next pattern
        if match6:
            date = match6.group(0)
            # return date
            try:
                # date_obj = datetime.strptime(date, "%b-%Y")
                # formatted_date = date_obj.strftime("%m-%Y")
                date_obj = datetime.strptime(date, "%Y/%m")
                formatted_date = date_obj.strftime("%Y-%m")

                return formatted_date
            except ValueError:
                pass  # Invalid date format, continue to the next pattern
        if match7:
            date = match7.group(0)
            # return date
            try:

                date_obj = datetime.strptime(date, "(%B%Y)")
                formatted_date = date_obj.strftime("%m-%Y")

                return formatted_date
            except ValueError:
                pass  # Invalid date format, continue to the next pattern
        if match8:
            try:

                extracted_year = match8.group()
                return extracted_year
            except ValueError:
                pass

        return ""

    def parse_date(self, date_string):
        print(date_string)

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
