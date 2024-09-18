import os
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from odoo import fields, models, api
from dotenv import load_dotenv
import base64

load_dotenv()


class ExternalResource(models.Model):
    _name = "external.resource"
    _description = "External Resource Model"

    mime_type = fields.Char(
        string="MIME Type", help="The MIME type of the external resource."
    )
    name = fields.Char(
        string="Name", required=True, help="The name of the external resource."
    )
    filename = fields.Char(
        string="Filename", help="The original filename of the resource."
    )
    ref_number = fields.Char(
        string="Reference Number", help="A unique reference number for this resource."
    )
    release_date = fields.Date(
        string="Release Date", help="The date when the resource was released."
    )
    source_id = fields.Many2one(
        "res.partner",
        string="Source",
        help="The source or origin of the external resource.",
    )
    created_by = fields.Many2one(
        "res.users",
        string="Created By",
        default=lambda self: self.env.user,
        help="The user who created the resource.",
    )
    channel = fields.Char(
        string="Channel", help="The channel through which the resource is available."
    )
    external_resource_url = fields.Char(
        string="External Resource URL", help="URL linking to the external resource."
    )

    # Optional: Adding auto-generated timestamp fields
    create_date = fields.Datetime(string="Created On", readonly=True)
    write_date = fields.Datetime(string="Last Updated On", readonly=True)

    # general webscrapping functino
    def run_webscrapping(self):
        print('web scrapper run sucessfully')
        self.process()


    @api.model
    def process(self):
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
                    download_url = self.store(reference, resource_url, download)

                    # Create the record in the external.resource model
                    # if len(reference.strip()) > 0:
                        # self.env["external.resource"].create(
                        #     {
                        #         "mime_type": "application/pdf",
                        #         "name": title,
                        #         "filename": reference,
                        #         "ref_number": reference,
                        #         "release_date": (
                        #             self.convert_date_format(published_date)
                        #             if published_date
                        #             else None
                        #         ),
                        #         "source_id": None,  # Set to the appropriate partner ID if needed
                        #         "created_by": self.env.user.id,
                        #         "channel": "CBN Website",
                        #         "external_resource_url": resource_url,
                        #     }
                        # )
                    # Create the record for rulebook title
                    if len(reference.strip()) > 0:
                        # Fetch the source_id where the rulebook.sources.name is like 'CBN'
                            source = self.env['rulebook.sources'].search([('name', 'ilike', 'CBN')], limit=1)

                            if not source:
                                raise ValueError("Source 'CBN' not found in the rulebook.sources.")

                            
                            # Download the file from resource_url and convert it to binary
                            file_binary_data = self.download_file_as_binary(resource_url)    

                            # Create the record in rulebook.title
                            self.env["rulebook.title"].create({
                                "name": title,  # Corresponds to the 'Title' field
                                "file": file_binary_data,  # Assuming file_binary_data is the file content in binary
                                "file_name": reference,  # Use reference as the file name
                                "ref_number": reference,  # Reference number
                                "released_date": (
                                    self.convert_date_format(published_date) if published_date else None
                                ),  # Released Date
                                "status": "active",  # Default status to 'active'
                                "source_id": source.id,  # Source ID (from rulebook.sources where name like 'CBN')
                                "created_on": fields.Datetime.now(),  # Current timestamp
                                "created_by": self.env.user.id,  # Created by the current user
                            })

        else:
            print(f"Failed to retrieve the page. Status code: {response.status_code}")

    def store(self, reference, resource_url, download):
        # Method to store the file and return the download URL
        dir_path = self.get_directory('pdf')
        filename = self.make_file_name(reference) + ".pdf"
        filepath = os.path.join(dir_path.get("dir_path"), filename)

        # Save the PDF file to the file system
        if "pdf" in resource_url and not os.path.exists(filepath):
            with open(filepath, "wb") as pdf:
                pdf.write(download.content)

        # Retrieve the base URL for the document download
        base_url = os.getenv("DOCUMENT_DOWNLOAD_BASE_URL")

        if base_url is None:
            raise ValueError("Environment variable DOCUMENT_DOWNLOAD_BASE_URL is not set.")

        # Return the download URL if the file exists
        if os.path.exists(filepath):
            return (
                base_url
                + "/pdf/"
                + dir_path.get("base_dir")
                + "/"
                + filename
            )
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

    def download_file_as_binary(self,url):
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Convert the file content to base64-encoded binary data
            file_binary_data = base64.b64encode(response.content)
            return file_binary_data
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Failed to download file from {url}: {str(e)}")


    def convert_date_format(self,date_str):
        try:
            # Convert the incoming date from 'MM/DD/YYYY' to 'YYYY-MM-DD'
            parsed_date = datetime.strptime(date_str, '%m/%d/%Y')
            formatted_date = parsed_date.strftime('%Y-%m-%d')
            return formatted_date
        except ValueError as e:
            raise ValueError(f"Error processing date '{date_str}': {e}")            
