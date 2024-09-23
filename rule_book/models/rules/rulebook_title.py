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
        created_record = self.env['rulebook.title'].sudo().search([('id', '=', 59)])
        print(f"Record found: {created_record}")

        return record

    # general webscrapping functino
    def run_webscrapping(self):
        print("web scrapper run sucessfully")
        # self.CBNScrapper()
        self.NDICScrapper()
        # self.NFIUScraper()

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
        print("running ndic")
        url = os.getenv("NDIC_SCRAPE_URL")
        base_url = os.getenv("NDIC_BASE_URL")
        html_tags = os.getenv("NDIC_HTML_TAGS").split(",")
        storage_path = os.getenv("NDIC_STORAGE_DIR")
        source = self.env["rulebook.sources"].search([("name", "ilike", "NDIC")], limit=1)

        user_agent = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0"
        }

        response = requests.get(url, headers=user_agent)
        soup = BeautifulSoup(response.text, "html.parser")

        pdf_links = soup.select("ul li a[href]")  # Assuming the first tag selects the PDF link
        file_names = soup.select("li a")  # Assuming the second tag selects the file names

        success_count = 0  # To track successful inserts
        error_count = 0  # To track any errors

        for link, file_name in zip(pdf_links, file_names):
            try:
                pdf_url = link["href"]
                file_name = self.clean_filename(file_name.text.strip())
                if not pdf_url.startswith(base_url):
                    pdf_url = base_url + pdf_url

                if pdf_url.endswith(".pdf"):
                    filename = basename(urlparse(pdf_url).path)[:-4]  # Strip '.pdf' extension
                    date_string = self.get_date_from_url(unquote(pdf_url))

                    spaced_name = file_name.replace("_", " ")
                    filename = f"{unquote(file_name)[:-len(date_string)-1]}_{date_string}.pdf"

                    pdf_response = requests.get(pdf_url)
                    if not os.path.exists(storage_path):
                        os.makedirs(storage_path)

                    filepath = os.path.join(storage_path, filename)

                    # Save the PDF file locally if it doesn't already exist
                    if not os.path.exists(filepath):
                        with open(filepath, "wb") as f:
                            f.write(pdf_response.content)

                    # Convert the downloaded file to base64 binary
                    file_binary_data = self.download_file_as_binary(pdf_url)

                    # Parse the date from the filename (or elsewhere)
                    released_date = self.parse_date(date_string)

                    # Prepare data for storing in the rulebook.title model
                    record_data = {
                        "name": spaced_name,  # Title
                        "file": file_binary_data,  # Binary data
                        "file_name": filename,  # File name for saving
                        "ref_number": None,  # Reference number (optional)
                        "status": "active",  # Status
                        "source_id": source.id,  # Source ID (from rulebook.sources)
                        "created_on": fields.Datetime.now(),  # Timestamp
                        "created_by": self.env.user.id,  # Created by user
                        "external_resource_url": pdf_url,  # URL of file
                    }

                    # Create the record in Odoo
                    self.sudo().create(record_data)
                    success_count += 1  # Increment success counter

            except Exception as e:
                error_count += 1  # Increment error counter
                print(f"Error processing file '{filename}': {str(e)}")
                # Optionally, log the error or handle it differently depending on your use case

        # Final return after the loop completes
        print(f"NDIC Scrapper finished: {success_count} records created, {error_count} errors.")
        return True if success_count > 0 else False

    @api.model
    def NFIUScraper(self):

        # Get the string from the .env file
        scrape_config_string = os.getenv("NFUI_SCRAPE_CONFIG")

        # Parse the JSON string back into a Python list of dictionaries
        scrape_config = json.loads(scrape_config_string)

        file_types = scrape_config
        NfiuBaseUrl = os.getenv("NFIU_BASE_URL")
        # pdfLinks = "CBN (AML CFT) (Amendment) Regulation, 2019"
        # return file_types

        # hekp=helper.get_date_from_url(unquote(pdfLinks))
        # return hekp
        fileNmaes = []

        for file_type in file_types:
            urls = file_type["urls"]
            storage_path = file_type["storage_path"]
            html_tags = file_type["html_tags"]
            nameTag = html_tags[1]

            for url, html_tag in zip(urls, html_tags):
                response = requests.get(url)
                # return response.text

                soup = BeautifulSoup(response.text, "html.parser")
                pdf_links = soup.select(html_tag)
                file_names = soup.select(nameTag)

                # return pdf_links

                for link, file_name in zip(pdf_links, file_names):
                    pdf_url = link["href"]

                    title = file_name.get("title", "")
                    if title:
                        # continue
                        file_name = title

                    else:
                        # return "a tag entered"
                        file_name = file_name.get_text().strip()
                    file_name = file_name.replace("Download", "").replace(".pdf", "")

                    # fileNmaes.append(pdf_url)

                    # pdfLinks.append(pdf_url)

                    if not pdf_url.startswith(NfiuBaseUrl):
                        pdf_url = NfiuBaseUrl + pdf_url

                    if pdf_url:
                        filename = basename(urlparse(pdf_url).path)
                        filename = filename[:-4]

                        date_string = self.get_date_from_url(unquote(pdf_url))
                        noSpaceFileName = (
                            self.clean_filename(file_name)
                            .removeprefix("_")
                            .removesuffix("_")
                        )
                        filename = f"{unquote(noSpaceFileName)[:-len(date_string)-1]}{date_string}.pdf"
                        # return filename
                        pdf_response = requests.get(pdf_url)

                        if not os.path.exists(storage_path):
                            os.makedirs(storage_path)

                        filepath = os.path.join(storage_path, filename)

                        # Save the PDF file locally
                        if not os.path.exists(filepath):
                            with open(filepath, "wb") as f:
                                f.write(pdf_response.content)

                        # Prepare data for storing in the rulebook.title model
                        try:
                            # Fetch the source_id where rulebook.sources.name is like 'CBN'
                            source = self.env["rulebook.sources"].search(
                                [("name", "ilike", "NFIU")], limit=1
                            )
                            print(source)
                            if not source:
                                raise ValueError(
                                    "Source 'CBN' not found in the rulebook.sources."
                                )

                                # Convert the downloaded file to base64 binary
                                # with open(filepath, "rb") as pdf_file:
                                #     file_binary_data = base64.b64encode(pdf_file.read())
                                # Download the file from resource_url and convert it to binary
                            file_binary_data = self.download_file_as_binary(pdf_url)
                            date_string = self.get_date_from_url(unquote(pdf_url))
                            released_date = (
                                self.parse_date(date_string) if date_string else None
                            )

                            # Create record in Odoo model 'rulebook.title'
                            spaced_name = file_name.replace("_", " ")
                            self.sudo().create(
                                {
                                    "name": spaced_name,  # Corresponds to the 'Title' field
                                    "file": file_binary_data,  # Binary data from the downloaded file
                                    "file_name": filename,  # File name used for saving the file
                                    "ref_number": None,  # You can set the reference number later if needed
                                    "released_date": fields.Datetime.now(),  # Release date from the filename
                                    "status": "active",  # Default status to 'active'
                                    "source_id": source.id,  # Source ID (rulebook.sources where name like 'CBN')
                                    "created_on": fields.Datetime.now(),  # Current timestamp
                                    "created_by": self.env.user.id,  # Created by the current user
                                    "external_resource_url": pdf_url,  # URL where the file was downloaded from
                                }
                            )

                        except Exception as e:
                            raise ValueError(f"Error : {str(e)}")

                    else:
                        return "something went wrong"

        # return fileNmaes

        return "Nfiu Scrape was successful!"

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
        raise ValueError(f"Date format for '{date_string}' is not supported")
