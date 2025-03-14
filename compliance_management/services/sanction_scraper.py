import requests
import os
import re
import logging
import time
from datetime import datetime
import hashlib
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import traceback
import os
from requests.auth import HTTPBasicAuth
import random
from dotenv import load_dotenv


_logger = logging.getLogger(__name__)


class SanctionScraper:
    """
    Handles scraping of sanctions data from various government sources
    """

    def __init__(self, env=None, media_dir=None):
        """
        Initialize the sanctions scraper

        Args:
            env: Odoo environment
            media_dir: Directory for storing downloaded files
        """
        self.env = env

        # Set up the base media directory
        if media_dir:
            self.media_dir = media_dir
        # elif env:
        #     # Use Odoo's filestore if available
        #     self.media_dir = os.path.join(
        #         env["ir.attachment"]._filestore(),
        #         "compliance_management",
        #         "sanctions_data",
        #     )
        else:
            # Fallback to a local directory
            self.media_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "media", "sanctions_data"
            )

        # Create the media directory if it doesn't exist
        os.makedirs(self.media_dir, exist_ok=True)

        # Configure a request session with proper headers
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }
        )

        # Retry configuration
        self.max_retries = 3
        self.retry_delay = 2  # seconds

        # Configurable timeout
        self.timeout = 30  # seconds

    def _get_source_directory(self, source_name):
        """
        Get the directory for a specific source

        Args:
            source_name: Name of the source

        Returns:
            str: Path to the source directory
        """
        source_dir = os.path.join(self.media_dir, source_name)
        os.makedirs(source_dir, exist_ok=True)
        return source_dir

    def _generate_filename(self, url, source_name):
        """
        Generate a unique filename for a URL

        Args:
            url: URL of the file
            source_name: Name of the source

        Returns:
            str: A unique filename
        """
        # Get the original filename from the URL
        parsed_url = urlparse(url)
        original_filename = os.path.basename(parsed_url.path)

        # If no filename could be extracted, use the hash of the URL
        if not original_filename:
            original_filename = hashlib.md5(url.encode()).hexdigest()

        # Extract the file extension
        _, file_extension = os.path.splitext(original_filename)
        if not file_extension:
            # Try to determine extension from the URL
            if "?" in original_filename:
                original_filename = original_filename.split("?")[0]
                _, file_extension = os.path.splitext(original_filename)

        # If still no extension, try to infer it from the URL or Content-Type
        if not file_extension:
            file_extension = self._infer_extension(url)

        # Generate a timestamp prefix
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create a unique filename
        filename = f"{source_name}_{timestamp}_{original_filename}"

        return filename

    def _infer_extension(self, url):
        """
        Infer file extension from URL or by making a HEAD request

        Args:
            url: URL of the file

        Returns:
            str: File extension with leading dot
        """
        # First, try to infer from URL patterns
        if "csv" in url.lower():
            return ".csv"
        elif "excel" in url.lower() or "xlsx" in url.lower():
            return ".xlsx"
        elif "pdf" in url.lower():
            return ".pdf"
        elif "ods" in url.lower():
            return ".ods"
        elif "xls" in url.lower():
            return ".xls"

        # If not obvious from URL, make a HEAD request
        try:
            response = self.session.head(url, timeout=self.timeout)
            content_type = response.headers.get("Content-Type", "")

            if "csv" in content_type:
                return ".csv"
            elif "excel" in content_type or "xlsx" in content_type:
                return ".xlsx"
            elif "pdf" in content_type:
                return ".pdf"
            elif "opendocument.spreadsheet" in content_type:
                return ".ods"
            elif "xls" in content_type:
                return ".xls"
            else:
                # Default to binary file
                return ".bin"
        except:
            # If HEAD request fails, default to binary
            return ".bin"

    def _download_file(self, url, source_name, retries=None):
        """
        Download a file with reliable duplicate prevention
        """

        if retries is None:
            retries = self.max_retries

        try:
            # Get the source directory
            source_dir = self._get_source_directory(source_name)

            # get neccessary links
            urls = self._extract_links(url)


            for url in urls:
                try:

                    # Generate a consistent filename based on URL
                    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
                    parsed_url = urlparse(url)
                    filename_base = os.path.basename(parsed_url.path)

                    if not filename_base or filename_base == '/' or '?' in filename_base:
                        filename_base = f"{source_name}_{url_hash}"

                    # Create the filename - don't use timestamps to avoid duplicates
                    filename = f"{filename_base}"
                    file_path = os.path.join(source_dir, filename)

                    # Check if this file exists and is recent (less than 24 hours old)
                    if os.path.exists(file_path) and os.path.getsize(file_path) > 100:
                        file_age = datetime.now().timestamp() - os.path.getmtime(file_path)
                        if file_age < 86400:  # 24 hours
                            _logger.info(f"Using existing file (less than 24h old): {file_path}")

                            # Determine file type from extension
                            _, extension = os.path.splitext(file_path)
                            file_type = extension.lstrip(".").lower()
                            if not file_type:
                                file_type = "csv"  # Default assumption

                            return file_path, file_type

                    # Download the file - use a temporary file first
                    _logger.info(f"Downloading {url} to {file_path}")

                    # Use HEAD request to get file metadata
                    try:
                        head_response = self.session.head(url, timeout=10)

                        content_length = int(head_response.headers.get('content-length', 0))

                        _logger.info(f"File size: {content_length} bytes")
                    except:
                        content_length = 0

                    # Set reasonable timeouts based on file size
                    timeout = min(60, max(10, content_length / 100000))  # 10-60 seconds

                    # Get file with proper timeout
                    response = self.session.get(url, stream=True, timeout=timeout)
                    response.raise_for_status()

                    # Create temp file path
                    temp_file_path = f"{file_path}.tmp"

                    # Download file in chunks to avoid memory issues
                    with open(temp_file_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                    # Move temp file to final location only if the download was successful
                    if os.path.exists(temp_file_path) and os.path.getsize(temp_file_path) > 0:
                        # If we already have a file, only replace if newer file is larger or old file is incomplete
                        if not os.path.exists(file_path) or \
                        os.path.getsize(temp_file_path) > os.path.getsize(file_path) or \
                        os.path.getsize(file_path) < 1000:
                            os.replace(temp_file_path, file_path)
                            _logger.info(f"Downloaded file saved to {file_path}")
                        else:
                            os.remove(temp_file_path)
                            _logger.info(f"Keeping existing file (larger than new download)")

                    # Determine file type from extension or content-type
                    _, extension = os.path.splitext(file_path)
                    file_type = extension.lstrip(".").lower()

                    if not file_type:
                        content_type = response.headers.get('Content-Type', '').lower()
                        if 'csv' in content_type:
                            file_type = 'csv'
                        elif 'excel' in content_type or 'xlsx' in content_type:
                            file_type = 'xlsx'
                        elif 'pdf' in content_type:
                            file_type = 'pdf'
                        elif "opendocument.spreadsheet" in content_type:
                            file_type = "ods"
                        elif 'xml' in content_type:
                            file_type = 'xml'
                        else:
                            file_type = 'txt'  # Default to text

                    return file_path, file_type

                except requests.exceptions.RequestException as e:
                    if retries > 0:
                        # Use a short delay and retry
                        _logger.warning(f"Download failed, retrying ({retries} retries left): {url}")
                        time.sleep(2)  # Short delay to avoid timeouts
                        return self._download_file(url, source_name, retries - 1)
                    else:
                        _logger.error(f"Failed to download file after {self.max_retries} attempts: {url}")
                        _logger.error(f"Error: {str(e)}")
                        return None, None
        except Exception as e:
            _logger.error(f"Error downloading file: {url}")
            _logger.error(f"Error: {str(e)}")
            return None, None

    def _download_file_with_timeout(self, url, source_name, max_size_mb=10, timeout=30):
        """
        Download a file with size checking and strict timeout controls
        
        Args:
            url: URL to download
            source_name: Name of the source
            max_size_mb: Maximum file size in MB to download
            timeout: Timeout in seconds
            
        Returns:
            tuple: (file_path, file_type) or (None, None) if file is too large or timeout occurs
        """
        try:
            # Get source directory
            source_dir = self._get_source_directory(source_name)

            # Generate filename from URL
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            filename = os.path.basename(url) or f"{source_name}_{url_hash}"
            file_path = os.path.join(source_dir, filename)

            # Check if file exists and is recent (less than 24 hours old)
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                file_age = datetime.now().timestamp() - os.path.getmtime(file_path)
                if file_age < 86400:  # 24 hours
                    _logger.info(f"Using existing file (less than 24h old): {file_path}")

                    # Get file type from extension
                    _, ext = os.path.splitext(file_path)
                    file_type = ext.lstrip('.').lower() or 'txt'
                    return file_path, file_type

            # Use a HEAD request to check file size first
            try:
                _logger.info(f"Checking file size: {url}")
                head_response = self.session.head(url, timeout=5)
                content_length = int(head_response.headers.get('content-length', 0))

                # Convert bytes to MB
                size_mb = content_length / (1024 * 1024)
                _logger.info(f"File size: {size_mb:.2f} MB")

                # Skip if file is too large
                if size_mb > max_size_mb:
                    _logger.warning(f"File is too large ({size_mb:.2f} MB > {max_size_mb} MB limit), skipping: {url}")
                    return None, None
            except Exception as e:
                _logger.warning(f"Could not check file size, proceeding with caution: {str(e)}")

            # Download with strict timeout
            _logger.info(f"Downloading file: {url}")
            temp_path = f"{file_path}.downloading"

            # Use stream=True for efficient memory usage
            with self.session.get(url, stream=True, timeout=timeout) as response:
                response.raise_for_status()

                # Get content type
                content_type = response.headers.get('Content-Type', '').lower()
                if 'csv' in content_type:
                    file_type = 'csv'
                elif 'xml' in content_type:
                    file_type = 'xml'
                elif 'text' in content_type:
                    file_type = 'txt'
                else:
                    # Get from extension
                    _, ext = os.path.splitext(url)
                    file_type = ext.lstrip('.').lower() or 'txt'

                # Download to temp file
                current_size = 0
                with open(temp_path, 'wb') as f:
                    # Use smaller chunks to allow more frequent timeout checks
                    for chunk in response.iter_content(chunk_size=4096):
                        if chunk:
                            f.write(chunk)
                            current_size += len(chunk)

                            # Check if exceeding max size
                            if current_size > max_size_mb * 1024 * 1024:
                                _logger.warning(f"File exceeded max size during download, aborting: {url}")
                                return None, None

            # Only move to final path if download completed successfully
            if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
                os.replace(temp_path, file_path)
                _logger.info(f"Download completed successfully: {file_path}")
                return file_path, file_type
            else:
                _logger.warning(f"Download failed (empty file): {url}")
                return None, None

        except requests.Timeout:
            _logger.warning(f"Timeout downloading file: {url}")
            return None, None
        except Exception as e:
            _logger.error(f"Error downloading file: {url}, {str(e)}")
            return None, None

    def _fetch_page(self, url, retries=None, auth_required=False):
        """
        Fetch a web page with improved error handling and auth support

        Args:
            url: URL to fetch
            retries: Number of retries, defaults to self.max_retries
            auth_required: Whether authentication is required

        Returns:
            tuple: (soup, response) or (None, None) on failure
        """
        if retries is None:
            retries = self.max_retries

        try:
            _logger.info(f"Fetching page: {url}")

            headers = self.session.headers.copy()

            # Add authentication if required
            if auth_required:
                if self.env:
                    config = self.env["ir.config_parameter"].sudo()
                    username = config.get_param("sanctions_scraper.username")
                    password = config.get_param("sanctions_scraper.password")
                    if username and password:
                        auth = HTTPBasicAuth(username, password)
                    else:
                        auth = None
                else:
                    auth = None
            else:
                auth = None

            # Add random delay to avoid rate limiting (1-3 seconds)
            time.sleep(random.uniform(1, 3))

            # Use a shorter timeout for initial connection
            response = self.session.get(
                url, 
                timeout=(5, self.timeout),  # (connect timeout, read timeout)
                auth=auth,
                headers=headers,
                allow_redirects=True
            )

            # Handle redirects explicitly
            if response.history:
                _logger.info(f"Request was redirected from {url} to {response.url}")

            response.raise_for_status()

            # Check for JavaScript redirects or challenges
            if "Checking your browser" in response.text or "Please enable JavaScript" in response.text:
                _logger.warning(f"Page {url} requires JavaScript or contains a browser check")

            soup = BeautifulSoup(response.content, "html.parser")
            return soup, response

        except requests.exceptions.Timeout:
            _logger.warning(f"Timeout error fetching {url}")
            if retries > 0:
                _logger.warning(f"Retrying with increased timeout ({retries} retries left)")
                # Increase timeout for retries
                old_timeout = self.timeout
                self.timeout = self.timeout * 1.5
                result = self._fetch_page(url, retries - 1, auth_required)
                self.timeout = old_timeout  # Reset timeout
                return result
            else:
                _logger.error(f"Failed to fetch page after {self.max_retries} attempts: {url}")
                _logger.error(f"Error: Timeout")
                return None, None

        except requests.exceptions.RequestException as e:
            if retries > 0:
                _logger.warning(
                    f"Fetch failed, retrying ({retries} retries left): {url}"
                )
                time.sleep(self.retry_delay)
                return self._fetch_page(url, retries - 1, auth_required)
            else:
                _logger.error(
                    f"Failed to fetch page after {self.max_retries} attempts: {url}"
                )
                _logger.error(f"Error: {str(e)}")
                return None, None
        except Exception as e:
            _logger.error(f"Unexpected error fetching page: {url}")
            _logger.error(f"Error: {str(e)}")
            _logger.error(traceback.format_exc())
            return None, None

    def _extract_links(self, url):
        """
        Extract links with specific file extensions from a web page.

        Args:
            url: The URL of the web page.

        Returns:
            list: A list of links with the desired file extensions, prioritized by CSV, XLSX/ODS, and then PDF.
                Returns an empty list if no matching links are found.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

            soup = BeautifulSoup(response.content, "html.parser")

            file_extensions = {
                "csv": [],
                "xlsx": [],
                "ods": [],
                "pdf": [],
            }

            for a_tag in soup.find_all("a", href=True, download=True):
                href = a_tag.get("href")

                # Split at '?' and take the first part (handles both URLs with and without queries)
                url_without_query = href.split('?')[0]

                _, extension = os.path.splitext(url_without_query)
                extension = extension.lstrip(".").lower()

                if extension in file_extensions:
                    file_extensions[extension].append(href)

            if file_extensions["csv"]:
                return file_extensions["csv"]
            elif file_extensions["xlsx"] or file_extensions["ods"]:
                return file_extensions["xlsx"] + file_extensions["ods"]
            elif file_extensions["pdf"]:
                return file_extensions["pdf"]
            else:
                return []

        except requests.exceptions.RequestException as e:
            print(f"Error fetching URL {url}: {e}")
            return []
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return []

    def _get_europa_credentials(self):
        """Get Europa website login credentials from .env file"""
        try:    
            # Load from the module directory
            base_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            env_path = os.path.join(base_path, '.env')

            if os.path.exists(env_path):
                load_dotenv(env_path)

                return {
                    'europa_username': os.getenv('EUROPA_USERNAME', ''),
                    'europa_password': os.getenv('EUROPA_PASSWORD', '')
                }
            return {}

        except ImportError:
            _logger.warning("python-dotenv not installed, cannot load Europa credentials")
            return {}
        except Exception as e:
            _logger.error(f"Error loading Europa credentials: {str(e)}")
            return {}

    def fetch_eu_sanctions(self):
        """
        Fetch sanctions data from the EU website with login support if needed
        """
        source_name = "eu_sanctions"

        # Updated URL for EU sanctions
        url = "https://finance.ec.europa.eu/eu-and-me/sanctions-restrictive-measures_en"

        # Alternative direct URLs that don't require login
        direct_urls = [
            "https://webgate.ec.europa.eu/fsd/fsf/public/files/csvFullSanctionsList_1_1/content?token=dG9rZW4tMjAxNw",
            "https://webgate.ec.europa.eu/fsd/fsf/public/files/csvFullSanctionsList/content?token=dG9rZW4tMjAxNw&checksum=E82F6DC542A1FC05A94773562BF07311D3114F4B",
            "https://webgate.ec.europa.eu/fsd/fsf/public/files/pdfFullSanctionsList/content?token=dG9rZW4tMjAxNw",
            "https://data.europa.eu/data/datasets/consolidated-list-of-persons-groups-and-entities-subject-to-eu-financial-sanctions?locale=en"
        ]

        files = []

        try:
            # First try direct download URLs
            for direct_url in direct_urls:
                _logger.info(f"Trying direct EU sanctions download: {direct_url}")

                file_path, file_type = self._download_file(direct_url, source_name)
                if file_path:
                    files.append({
                        "path": file_path,
                        "type": file_type,
                        "url": direct_url,
                        "source": source_name,
                        "description": "EU Sanctions Direct Download",
                    })

            # If direct downloads worked, return the files
            if files:
                _logger.info(f"Completed EU sanctions data fetch. Found {len(files)} files.")
                return files

            # Otherwise, try scraping the website
            _logger.info(f"No direct downloads found, scraping EU sanctions website: {url}")

            # Check if we need to login
            credentials = self._get_europa_credentials()

            # Fetch the page
            soup, response = self._fetch_page(url)
            if not soup:
                _logger.error("Failed to fetch EU sanctions page")
                return files

            # Look for download links
            links = []

            # Look for links with download-related text
            for a_tag in soup.find_all('a', href=True):
                href = a_tag.get('href')
                text = a_tag.get_text().lower()

                # Skip if not a potential download link
                if not href:
                    continue

                # Look for likely sanctions files
                if ('sanction' in href or 'restriction' in href or 
                    'consolidated' in href or 'financial' in href) and \
                any(ext in href.lower() for ext in ['.csv', '.xlsx', '.pdf', '.xml']):

                    # Make absolute URL if needed
                    if not href.startswith(('http://', 'https://')):
                        href = urljoin(url, href)

                    links.append({
                        'url': href,
                        'text': a_tag.get_text().strip()
                    })

            # Try to download each link
            for link in links:
                _logger.info(f"Trying to download EU sanctions link: {link['url']}")

                file_path, file_type = self._download_file(link['url'], source_name)
                if file_path:
                    files.append({
                        "path": file_path,
                        "type": file_type,
                        "url": link['url'],
                        "source": source_name,
                        "description": link['text'] or "EU Sanctions File",
                    })

            _logger.info(f"Completed EU sanctions data fetch. Found {len(files)} files.")
            return files

        except Exception as e:
            _logger.error(f"Error fetching EU sanctions data: {str(e)}")
            return files

    def fetch_ofac_sanctions(self):
        """
        Fetch sanctions data from OFAC with improved timeout handling

        Returns:
        list: List of dictionaries with file information
        """
        source_name = "ofac_sanctions"

        # Break OFAC downloads into very small groups to avoid timeout
        direct_urls = [
            # File 1: Small CSV (high priority)
            "https://www.treasury.gov/ofac/downloads/sdn.csv",
            
            # File 2: Small XML
            "https://www.treasury.gov/ofac/downloads/sdn_advanced.xml",
            
            # File 3: Medium TXT file
            "https://www.treasury.gov/ofac/downloads/sdnlist.txt"

            "https://www.treasury.gov/ofac/downloads/consolidated/consolidated.xml",
            "https://www.treasury.gov/ofac/downloads/sanctions/1.0/sdn_advanced.xml",
        ]

        # These are separate because they're larger files (process optionally)
        large_files = [
            "https://www.treasury.gov/ofac/downloads/consolidated/consolidated.xml",
            "https://www.treasury.gov/ofac/downloads/consolidated/consolidated.csv"
        ]

        files = []
        _logger.info(f"Fetching OFAC sanctions data (processing small files first)")

        # Process each small file individually with a timeout
        for direct_url in direct_urls:
            try:
                _logger.info(f"Downloading OFAC file: {direct_url}")
                file_path, file_type = self._download_file_with_timeout(direct_url, source_name, 
                                                                        max_size_mb=10, timeout=30)
                if file_path:
                    files.append({
                        "path": file_path,
                        "type": file_type,
                        "url": direct_url,
                        "source": source_name,
                        "description": os.path.basename(direct_url),
                    })
            except Exception as e:
                _logger.error(f"Error downloading {direct_url}: {str(e)}")

        # Process large files only if we have time (but don't block if they're too large)
        for large_file in large_files:
            try:
                _logger.info(f"Checking large OFAC file: {large_file}")
                # Only download if under 30MB, else create placeholder record
                file_path, file_type = self._download_file_with_timeout(large_file, source_name,
                                                                    max_size_mb=30, timeout=45)
                if file_path:
                    files.append({
                        "path": file_path,
                        "type": file_type,
                        "url": large_file,
                        "source": source_name, 
                        "description": os.path.basename(large_file),
                    })
                else:
                    _logger.warning(f"Skipped large file {large_file} to prevent timeout")
                    # Create a record about the file but don't include its content
                    source_dir = self._get_source_directory(source_name)
                    placeholder_path = os.path.join(source_dir, f"placeholder_{os.path.basename(large_file)}.txt")
                    with open(placeholder_path, 'w') as f:
                        f.write(f"Large file available at: {large_file}\n")
                        f.write(f"File was skipped to prevent timeouts. Download manually if needed.")

                    files.append({
                        "path": placeholder_path,
                        "type": "txt",
                        "url": large_file,
                        "source": source_name,
                        "description": f"PLACEHOLDER: {os.path.basename(large_file)} (too large)",
                    })
            except Exception as e:
                _logger.error(f"Error checking large file {large_file}: {str(e)}")

        _logger.info(f"OFAC sanctions data fetch completed. Found {len(files)} files.")
        return files

    def fetch_un_sanctions(self):
        """
        Fetch sanctions data from the UN consolidated list

        Returns:
            list: List of dictionaries with file information
        """
        source_name = "un_sanctions"
        url = "https://www.un.org/securitycouncil/content/un-sc-consolidated-list"

        files = []

        try:
            _logger.info(f"Fetching UN sanctions data from {url}")

            # Fetch the main page
            soup, response = self._fetch_page(url)
            if not soup:
                return files

            # Extract downloadable file links
            links = self._extract_links(soup, url)

            # UN site has some specific file formats, so look for XML files too
            xml_links = []
            for a_tag in soup.find_all("a", href=True):
                href = a_tag.get("href")
                if href.lower().endswith(".xml"):
                    if not href.startswith(("http://", "https://")):
                        href = urljoin(url, href)

                    xml_links.append(
                        {
                            "url": href,
                            "text": a_tag.get_text(strip=True)
                            or os.path.basename(href),
                            "type": "xml",
                        }
                    )

            # Combine regular links with XML links
            links.extend(xml_links)

            # Download each file
            for link in links:
                file_path, file_type = self._download_file(link["url"], source_name)
                if file_path:
                    files.append(
                        {
                            "path": file_path,
                            "type": file_type,
                            "url": link["url"],
                            "source": source_name,
                            "description": link["text"],
                        }
                    )

            _logger.info(
                f"Completed UN sanctions data fetch. Found {len(files)} files."
            )
            return files

        except Exception as e:
            _logger.error(f"Error fetching UN sanctions data")
            _logger.error(f"Error: {str(e)}")
            _logger.error(traceback.format_exc())
            return files

    def fetch_uk_sanctions(self):
        """
        Enhanced UK sanctions scraper with direct file downloads
        """
        source_name = "uk_sanctions"

        # Direct URLs for UK sanctions files (latest as of 2025)
        direct_urls = [
            # CSV format - most recent
            "https://assets.publishing.service.gov.uk/media/sanctions-list.csv",
            "https://ofsistorage.blob.core.windows.net/publishlive/ConList.csv",
            "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1183423/UK_Sanctions_List.csv",
            "https://assets.publishing.service.gov.uk/media/652e5c5df5762f001356c0e6/UK_Sanctions_List.csv",
            
            # Excel format
            "https://assets.publishing.service.gov.uk/media/sanctions-list.ods",
            "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1183425/UK_Sanctions_List.ods",
            "https://assets.publishing.service.gov.uk/media/67caf893a175f08d198d80f1/UK_Sanctions_List.ods",
            "https://assets.publishing.service.gov.uk/media/652e5c5e6a75b5001312d064/UK_Sanctions_List.ods",
            
            # XML format (if available)
            "https://assets.publishing.service.gov.uk/media/sanctions-list.xml",
            
            # PDF format
            "https://assets.publishing.service.gov.uk/media/sanctions-list.pdf",
            "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1183422/UK_Sanctions_List.pdf",
        ]

        # Alternative base URLs to try
        base_urls = [
            "https://www.gov.uk/government/publications/the-uk-sanctions-list",
            "https://www.gov.uk/government/publications/financial-sanctions-consolidated-list-of-targets",
            "https://sanctionssearchapp.ofsi.hmtreasury.gov.uk/",
            "https://www.gov.uk/government/collections/financial-sanctions-regime-specific-consolidated-lists-and-releases",
        ]

        files = []

        try:
            _logger.info("Attempting direct downloads for UK sanctions")

            # First try direct URLs
            for url in direct_urls:
                try:
                    _logger.info(f"Trying direct UK sanctions download: {url}")

                    # Use a longer timeout for UK files
                    self.timeout = 60

                    # Use a custom User-Agent to avoid blocks
                    old_headers = self.session.headers.copy()
                    self.session.headers.update({
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.5"
                    })

                    # Attempt to download with 3 retries
                    file_path, file_type = self._download_file(url, source_name, retries=3)

                    # Restore original headers
                    self.session.headers = old_headers

                    if file_path:
                        files.append({
                            "path": file_path,
                            "type": file_type,
                            "url": url,
                            "source": source_name,
                            "description": "UK Sanctions Direct Download",
                        })

                        # If we've found a CSV or Excel file, we can break early
                        if file_type in ['csv', 'xlsx', 'xls', 'ods']:
                            _logger.info(f"Successfully downloaded {file_type} file for UK sanctions")
                            break
                except Exception as e:
                    _logger.warning(f"Could not download {url}: {str(e)}")

            # If we got files from direct downloads, return them
            if files:
                _logger.info(f"Successfully downloaded {len(files)} UK sanctions files")
                return files

            # If direct downloads failed, try scraping each base URL
            _logger.info("Direct downloads failed, trying to scrape UK sanctions from base URLs")

            for base_url in base_urls:
                try:
                    _logger.info(f"Scraping UK sanctions from: {base_url}")

                    # Get the page content
                    soup, response = self._fetch_page(base_url)
                    if not soup:
                        _logger.warning(f"Could not fetch {base_url}")
                        continue

                    # Look for download links
                    for a_tag in soup.find_all('a', href=True):
                        href = a_tag.get('href', '')
                        text = a_tag.get_text().lower()

                        # Skip if not a potential download link
                        if not href:
                            continue

                        # Check if it looks like a sanctions file
                        if (('sanction' in href.lower() or 'consolidated' in href.lower()) and 
                            any(ext in href.lower() for ext in ['.csv', '.xlsx', '.xls', '.ods', '.pdf', '.xml'])):

                            # Make absolute URL if needed
                            if not href.startswith(('http://', 'https://')):
                                href = urljoin(base_url, href)

                            _logger.info(f"Found potential UK sanctions file: {href}")

                            try:
                                file_path, file_type = self._download_file(href, source_name)
                                if file_path:
                                    files.append({
                                        "path": file_path,
                                        "type": file_type,
                                        "url": href,
                                        "source": source_name,
                                        "description": text or "UK Sanctions File",
                                    })

                                    # If we've found a CSV or Excel file, we can break early
                                    if file_type in ['csv', 'xlsx', 'xls', 'ods']:
                                        _logger.info(f"Successfully downloaded {file_type} file for UK sanctions")
                                        break
                            except Exception as e:
                                _logger.warning(f"Could not download {href}: {str(e)}")

                    # If we found files from this base URL, break early
                    if files:
                        break

                except Exception as e:
                    _logger.warning(f"Error scraping {base_url}: {str(e)}")

            # If we still don't have files, try one more approach with a specific search
            if not files:
                _logger.info("Trying UK sanctions search page for files")
                try:
                    search_url = "https://www.gov.uk/search/all?keywords=sanctions+list&content_store_document_type=all&organisations%5B%5D=office-of-financial-sanctions-implementation&order=relevance"

                    soup, response = self._fetch_page(search_url)
                    if soup:
                        # Look for search results with files
                        for a_tag in soup.find_all('a', href=True):
                            href = a_tag.get('href', '')

                            if 'sanctions' in href.lower() and '/government/' in href.lower():
                                # Visit this page to look for files
                                _logger.info(f"Checking page for UK sanctions files: {href}")

                                result_soup, _ = self._fetch_page(href)
                                if not result_soup:
                                    continue

                                # Look for attachments with data formats
                                for file_link in result_soup.find_all('a', href=True):
                                    file_href = file_link.get('href', '')

                                    if any(ext in file_href.lower() for ext in ['.csv', '.xlsx', '.xls', '.ods', '.pdf', '.xml']):
                                        # Make absolute URL if needed
                                        if not file_href.startswith(('http://', 'https://')):
                                            file_href = urljoin(href, file_href)

                                        _logger.info(f"Found potential UK sanctions file: {file_href}")

                                        try:
                                            file_path, file_type = self._download_file(file_href, source_name)
                                            if file_path:
                                                files.append({
                                                    "path": file_path,
                                                    "type": file_type,
                                                    "url": file_href,
                                                    "source": source_name,
                                                    "description": file_link.get_text() or "UK Sanctions File",
                                                })
                                        except Exception as e:
                                            _logger.warning(f"Could not download {file_href}: {str(e)}")
                except Exception as e:
                    _logger.warning(f"Error with UK sanctions search: {str(e)}")

            # Final fallback - use a dummy file with a placeholder if no files were found
            if not files:
                _logger.warning("Could not find any UK sanctions files, creating placeholder")

                # Create a placeholder file
                source_dir = self._get_source_directory(source_name)
                placeholder_path = os.path.join(source_dir, f"uk_sanctions_placeholder.txt")

                with open(placeholder_path, 'w') as f:
                    f.write("# UK Sanctions List Placeholder\n\n")
                    f.write("No UK sanctions files could be downloaded at this time.\n")
                    f.write(f"Last attempt: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("Please check URLs and try again later.\n")

                files.append({
                    "path": placeholder_path,
                    "type": "txt",
                    "url": "placeholder",
                    "source": source_name,
                    "description": "UK Sanctions Placeholder",
                })

            _logger.info(f"Completed UK sanctions data fetch. Found {len(files)} files.")
            return files

        except Exception as e:
            _logger.error(f"Error fetching UK sanctions data: {str(e)}")
            _logger.error(traceback.format_exc())
            return files

    def scrapeAndDownload(self, url, source_name):
        """
        Enhanced sanctions scraper with direct file downloads
        """

        files = []

        try:
            _logger.info(f"downloading for: {source_name}")

            # Use a longer timeout for UK files
            self.timeout = 60

            # Use a custom User-Agent to avoid blocks
            old_headers = self.session.headers.copy()
            self.session.headers.update({
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5"
            })

            # scrape any link with download on the url

            # Attempt to download with 3 retries
            file_path, file_type = self._download_file(url, source_name, retries=3)

            # Restore original headers
        #     self.session.headers = old_headers

        #     if file_path:
        #         files.append({
        #             "path": file_path,
        #             "type": file_type,
        #             "url": url,
        #             "source": source_name,
        #             "description": "UK Sanctions Direct Download",
        #         })

        #         # If we've found a CSV or Excel file, we can break early
        #         if file_type in ['csv', 'xlsx', 'xls', 'ods']:
        #             _logger.info(f"Successfully downloaded {file_type} file for UK sanctions")
        #             break
        except Exception as e:
            _logger.warning(f"Could not download {url}: {str(e)}")

        # # If we got files from direct downloads, return them
        # if files:
        #         _logger.info(f"Successfully downloaded {len(files)} UK sanctions files")
        #         return files

        # # If direct downloads failed, try scraping each base URL
        # _logger.info("Direct downloads failed, trying to scrape UK sanctions from base URLs")

        # for base_url in base_urls:
        #         try:
        #             _logger.info(f"Scraping UK sanctions from: {base_url}")

        #             # Get the page content
        #             soup, response = self._fetch_page(base_url)
        #             if not soup:
        #                 _logger.warning(f"Could not fetch {base_url}")
        #                 continue

        #             # Look for download links
        #             for a_tag in soup.find_all('a', href=True):
        #                 href = a_tag.get('href', '')
        #                 text = a_tag.get_text().lower()

        #                 # Skip if not a potential download link
        #                 if not href:
        #                     continue

        #                 # Check if it looks like a sanctions file
        #                 if (('sanction' in href.lower() or 'consolidated' in href.lower()) and
        #                     any(ext in href.lower() for ext in ['.csv', '.xlsx', '.xls', '.ods', '.pdf', '.xml'])):

        #                     # Make absolute URL if needed
        #                     if not href.startswith(('http://', 'https://')):
        #                         href = urljoin(base_url, href)

        #                     _logger.info(f"Found potential UK sanctions file: {href}")

        #                     try:
        #                         file_path, file_type = self._download_file(href, source_name)
        #                         if file_path:
        #                             files.append({
        #                                 "path": file_path,
        #                                 "type": file_type,
        #                                 "url": href,
        #                                 "source": source_name,
        #                                 "description": text or "UK Sanctions File",
        #                             })

        #                             # If we've found a CSV or Excel file, we can break early
        #                             if file_type in ['csv', 'xlsx', 'xls', 'ods']:
        #                                 _logger.info(f"Successfully downloaded {file_type} file for UK sanctions")
        #                                 break
        #                     except Exception as e:
        #                         _logger.warning(f"Could not download {href}: {str(e)}")

        #             # If we found files from this base URL, break early
        #             if files:
        #                 break

        #         except Exception as e:
        #             _logger.warning(f"Error scraping {base_url}: {str(e)}")

        # # If we still don't have files, try one more approach with a specific search
        # if not files:
        #         _logger.info("Trying UK sanctions search page for files")
        #         try:
        #             search_url = "https://www.gov.uk/search/all?keywords=sanctions+list&content_store_document_type=all&organisations%5B%5D=office-of-financial-sanctions-implementation&order=relevance"

        #             soup, response = self._fetch_page(search_url)
        #             if soup:
        #                 # Look for search results with files
        #                 for a_tag in soup.find_all('a', href=True):
        #                     href = a_tag.get('href', '')

        #                     if 'sanctions' in href.lower() and '/government/' in href.lower():
        #                         # Visit this page to look for files
        #                         _logger.info(f"Checking page for UK sanctions files: {href}")

        #                         result_soup, _ = self._fetch_page(href)
        #                         if not result_soup:
        #                             continue

        #                         # Look for attachments with data formats
        #                         for file_link in result_soup.find_all('a', href=True):
        #                             file_href = file_link.get('href', '')

        #                             if any(ext in file_href.lower() for ext in ['.csv', '.xlsx', '.xls', '.ods', '.pdf', '.xml']):
        #                                 # Make absolute URL if needed
        #                                 if not file_href.startswith(('http://', 'https://')):
        #                                     file_href = urljoin(href, file_href)

        #                                 _logger.info(f"Found potential UK sanctions file: {file_href}")

        #                                 try:
        #                                     file_path, file_type = self._download_file(file_href, source_name)
        #                                     if file_path:
        #                                         files.append({
        #                                             "path": file_path,
        #                                             "type": file_type,
        #                                             "url": file_href,
        #                                             "source": source_name,
        #                                             "description": file_link.get_text() or "UK Sanctions File",
        #                                         })
        #                                 except Exception as e:
        #                                     _logger.warning(f"Could not download {file_href}: {str(e)}")
        #         except Exception as e:
        #             _logger.warning(f"Error with UK sanctions search: {str(e)}")

        # # Final fallback - use a dummy file with a placeholder if no files were found
        # if not files:
        #         _logger.warning("Could not find any UK sanctions files, creating placeholder")

        #         # Create a placeholder file
        #         source_dir = self._get_source_directory(source_name)
        #         placeholder_path = os.path.join(source_dir, f"uk_sanctions_placeholder.txt")

        #         with open(placeholder_path, 'w') as f:
        #             f.write("# UK Sanctions List Placeholder\n\n")
        #             f.write("No UK sanctions files could be downloaded at this time.\n")
        #             f.write(f"Last attempt: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        #             f.write("Please check URLs and try again later.\n")

        #         files.append({
        #             "path": placeholder_path,
        #             "type": "txt",
        #             "url": "placeholder",
        #             "source": source_name,
        #             "description": "UK Sanctions Placeholder",
        #         })

        # _logger.info(f"Completed UK sanctions data fetch. Found {len(files)} files.")
        # return files

    # def fetch_all_sanctions(self):
    #     """
    #     Fetch sanctions data from all sources, one at a time

    #     Returns:
    #         list: List of dictionaries with file information
    #     """
    #     all_files = []

    #     try:
    #         # Define sources with corresponding fetch methods
    #         sources = [
    #             ("eu_sanctions", self.fetch_eu_sanctions),
    #             ("un_sanctions", self.fetch_un_sanctions),
    #             ("ofac_sanctions", self.fetch_ofac_sanctions),
    #             ("uk_sanctions", self.fetch_uk_sanctions)
    #         ]

    #         # Process one source at a time to avoid timeout issues
    #         for source_name, fetch_method in sources:
    #             try:
    #                 _logger.info(f"Starting fetch from {source_name}")
    #                 files = fetch_method()
    #                 _logger.info(f"Completed fetch from {source_name}. Found {len(files)} files.")
    #                 all_files.extend(files)

    #                 # Add a short pause between sources to prevent overwhelming server
    #                 time.sleep(2)

    #             except Exception as e:
    #                 _logger.error(f"Error fetching from {source_name}: {str(e)}")
    #                 _logger.error(traceback.format_exc())
    #                 # Continue with next source even if this one fails
    #                 continue

    #         _logger.info(f"Completed fetch from all sources. Total files: {len(all_files)}")
    #         return all_files

    #     except Exception as e:
    #         _logger.error(f"Error in fetch_all_sanctions method")
    #         _logger.error(f"Error: {str(e)}")
    #         _logger.error(traceback.format_exc())
    #         return all_files
