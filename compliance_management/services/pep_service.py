import logging
import time
import traceback
from datetime import datetime
import requests
import json
import markdown
from odoo import fields
from .sanction_scraper import SanctionScraper
from .data_processor import DataProcessor
from .pep_importer import PepImporter
# from ..models.sanction_list_data import sources

_logger = logging.getLogger(__name__)


class PepService:
    """
    Service for PEP data management
    """

    def __init__(self, env):
        """
        Initialize the PEP service

        Args:
            env: Odoo environment
        """
        self.env = env

        # Initialize components
        self.scraper = SanctionScraper(env)
        self.processor = DataProcessor(env)
        self.importer = PepImporter(env)

    def find_person_biography(self, firstname, lastname):
        """
        Find biography information for a person using Gemini AI

        Args:
            firstname: First name of the person
            lastname: Last name of the person

        Returns:
            str: HTML-formatted biography or None on failure
        """
        try:
            config = self.env["ir.config_parameter"].sudo()
            api_key = config.get_param("gemini_api_key")

            if not api_key:
                _logger.error("Gemini API key not configured")
                return None

            _logger.info(
                f"Finding biography for {firstname} {lastname} using Gemini API"
            )

            name = f"Who is {firstname} {lastname}"
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Catch-Control": "no-cache",
            }

            json_data = {"contents": [{"parts": [{"text": name}]}]}

            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={api_key}"
            response = requests.post(
                url, data=json.dumps(json_data), headers=headers, timeout=30
            )
            response.raise_for_status()

            data = json.loads(response.text)
            text_value = data["candidates"][0]["content"]["parts"][0]["text"]

            # Convert markdown to HTML
            html_content = markdown.markdown(text_value)
            return html_content

        except requests.exceptions.RequestException as e:
            _logger.error(f"API request to Gemini failed: {str(e)}")
            return None
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            _logger.error(f"Error parsing Gemini response: {str(e)}")
            return None
        except Exception as e:
            _logger.error(f"Unexpected error finding person biography: {str(e)}")
            _logger.error(traceback.format_exc())
            return None

    def query_sanctions_service(self, firstname, lastname):
        """
        Query sanctions service for a person

        Args:
            firstname: First name of the person
            lastname: Last name of the person

        Returns:
            dict: Person data if found
        """
        try:
            config = self.env["ir.config_parameter"].sudo()
            API_KEY = config.get_param("opensanctions_api_key")

            if API_KEY is None:
                _logger.error("OpenSanctions API key not configured")
                return None

            _logger.info(f"Querying sanctions service for {firstname} {lastname}")

            headers = {
                "Authorization": API_KEY,
            }

            # Prepare a query to match on schema and the name property
            query = {
                "queries": {
                    "q1": {
                        "schema": "Person",
                        "properties": {"name": [f"{firstname} {lastname}"]},
                    }
                }
            }

            # Make the request
            response = requests.post(
                "https://api.opensanctions.org/match/default",
                headers=headers,
                json=query,
                timeout=30,
            )

            # Check for HTTP errors
            response.raise_for_status()

            # Get the results for our query
            data = response.json()["responses"]["q1"]["results"]

            if not data or len(data) < 2:
                _logger.info(f"No sanctions data found for {firstname} {lastname}")
                return None

            _logger.info(f"Found sanctions data for {firstname} {lastname}")

            return {"person": data[0], "metadata": data[1]}

        except requests.exceptions.RequestException as e:
            _logger.error(f"API request to OpenSanctions failed: {str(e)}")
            return None
        except (KeyError, IndexError) as e:
            _logger.error(f"Error parsing OpenSanctions response: {str(e)}")
            return None
        except Exception as e:
            _logger.error(f"Unexpected error querying sanctions service: {str(e)}")
            _logger.error(traceback.format_exc())
            return None

    def format_person_data(self, data):
        """
        Format person data from sanctions service

        Args:
            data: Data returned from sanctions service

        Returns:
            dict: Formatted person data for Odoo model
        """
        if not data:
            return {}

        try:
            person = data["person"]
            metadata = data["metadata"]
            properties = person["properties"]

            # Extract fields with error handling
            def get_property(source, field, default=""):
                try:
                    if field in source:
                        return (
                            "\n".join(source[field])
                            if isinstance(source[field], list)
                            else source[field]
                        )
                    return default
                except:
                    return default

            # Get properties from person object first, fall back to metadata
            position = get_property(properties, "position")
            if not position and "position" in metadata.get("properties", {}):
                position = get_property(metadata["properties"], "position")

            education = get_property(metadata.get("properties", {}), "education")
            notes = get_property(properties, "notes")
            birth_place = get_property(properties, "birthPlace")
            religion = get_property(properties, "religion")

            middle_name = ""
            if (
                "middleName" in metadata.get("properties", {})
                and metadata["properties"]["middleName"]
            ):
                middle_name = metadata["properties"]["middleName"][0]

            first_name = ""
            if (
                "firstName" in metadata.get("properties", {})
                and metadata["properties"]["firstName"]
            ):
                first_name = metadata["properties"]["firstName"][0]
            elif "caption" in person:
                first_name = " ".join(person["caption"])

            last_name = ""
            if (
                "lastName" in metadata.get("properties", {})
                and metadata["properties"]["lastName"]
            ):
                last_name = metadata["properties"]["lastName"][0]

            title = ""
            if (
                "title" in metadata.get("properties", {})
                and metadata["properties"]["title"]
            ):
                title = metadata["properties"]["title"][0]

            gender = ""
            if "gender" in properties and properties["gender"]:
                gender = properties["gender"][0].capitalize()

            citizenship = ""
            if "citizenship" in properties and properties["citizenship"]:
                citizenship = properties["citizenship"][0].upper()

            birth_date = ""
            if "birthDate" in properties and properties["birthDate"]:
                birth_date = properties["birthDate"][0]

            unique_id = person.get("id", "")

            return {
                "sex": gender,
                "date_of_birth": birth_date,
                "title": title,
                "education": education,
                "religion": religion,
                "citizenship": citizenship,
                "middle_name": middle_name,
                "place_of_birth": birth_place,
                "career_history": position,
                "remarks": notes,
                "source": "OpenSanctions API",
            }

        except Exception as e:
            _logger.error(f"Error formatting person data: {str(e)}")
            _logger.error(traceback.format_exc())
            return {}

    def fetch_and_import_pep_data(self):
        """
        Fetch and import PEP data with complete processing of all file types
        
        Returns:
            dict: Results of the operation
        """
        try:
            start_time = time.time()
            _logger.info("Starting PEP data fetch and import process with complete file processing")

            # Reset importer counters
            self.importer.reset_counters()
            
            # Define sources with corresponding fetch methods
            # sources = [
            #     ("uk_sanctions", self.scraper.fetch_uk_sanctions),  # Prioritize UK sanctions
            #     ("eu_sanctions", self.scraper.fetch_eu_sanctions),
            #     ("un_sanctions", self.scraper.fetch_un_sanctions),
            #     ("ofac_sanctions", self.scraper.fetch_ofac_sanctions)
            # ]

            sources = []
            
            all_files = []
            source_results = {}
            
            # Process one source at a time
            for source_name, fetch_method in sources:
                try:
                    _logger.info(f"Starting fetch from {source_name}")
                    source_start_time = time.time()
                    
                    # Use longer timeout for fetching
                    old_timeout = self.scraper.timeout
                    self.scraper.timeout = 120  # 2 minutes
                    
                    files = fetch_method()
                    
                    # Restore original timeout
                    self.scraper.timeout = old_timeout
                    
                    if files:
                        all_files.extend(files)
                        
                        source_duration = time.time() - source_start_time
                        source_results[source_name] = {
                            "status": "success",
                            "files_count": len(files),
                            "duration": source_duration,
                        }
                        
                        _logger.info(f"Completed fetch from {source_name}. Found {len(files)} files in {source_duration:.2f} seconds")
                    else:
                        source_results[source_name] = {
                            "status": "no_files",
                            "files_count": 0,
                            "duration": time.time() - source_start_time,
                        }
                        _logger.warning(f"No files found for {source_name}")
                    
                except Exception as e:
                    _logger.error(f"Error fetching from {source_name}: {str(e)}")
                    _logger.error(traceback.format_exc())
                    source_results[source_name] = {
                        "status": "error",
                        "error": str(e),
                        "duration": time.time() - source_start_time,
                    }
            
            if not all_files:
                _logger.warning("No files were fetched from any source")
                return {
                    "status": "warning",
                    "message": "No files were fetched from any source",
                    "files_processed": 0,
                    "records_processed": 0,
                    "records_created": 0,
                    "records_updated": 0,
                    "records_errored": 0,
                    "records_skipped": 0,
                    "duration": time.time() - start_time,
                    "source_results": source_results
                }

            _logger.info(f"Fetched {len(all_files)} files from all sources")
            
            # Organize files by source and type
            files_by_source = {}
            for file_info in all_files:
                source = file_info['source']
                if source not in files_by_source:
                    files_by_source[source] = {}
                    
                file_type = file_info['type']
                if file_type not in files_by_source[source]:
                    files_by_source[source][file_type] = []
                    
                files_by_source[source][file_type].append(file_info)
            
            # Define priority order of file types
            file_type_priority = ['csv', 'xlsx', 'xls', 'ods', 'pdf', 'xml', 'txt']
            
            # For each source, select at least one file of each type based on priority
            files_to_process = []
            for source, types in files_by_source.items():
                # First, select the highest priority file type available
                selected_priority_type = None
                for file_type in file_type_priority:
                    if file_type in types and types[file_type]:
                        selected_priority_type = file_type
                        selected_file = types[file_type][0]  # Take the first file of this type
                        files_to_process.append(selected_file)
                        _logger.info(f"Selected primary {file_type} file for {source}: {selected_file['path']}")
                        break
                
                # If no high-priority file was found, select the first available file
                if not selected_priority_type and types:
                    # Get the first available file type
                    first_available_type = list(types.keys())[0]
                    selected_file = types[first_available_type][0]
                    files_to_process.append(selected_file)
                    _logger.info(f"Selected fallback {first_available_type} file for {source}: {selected_file['path']}")
                
                # Now, also select one file of each remaining type for complete coverage
                for file_type in types:
                    if file_type != selected_priority_type and types[file_type]:
                        selected_file = types[file_type][0]  # Take the first file of this type
                        files_to_process.append(selected_file)
                        _logger.info(f"Selected additional {file_type} file for {source}: {selected_file['path']}")
            
            _logger.info(f"Selected {len(files_to_process)} files for processing")
            
            # Process and import each file
            file_results = []
            
            # Set batch size based on number of files
            batch_size = min(3, max(1, len(files_to_process)))
            
            # Process files in small batches to avoid timeouts
            for i in range(0, len(files_to_process), batch_size):
                batch = files_to_process[i:i+batch_size]
                _logger.info(f"Processing batch of {len(batch)} files ({i+1}-{i+len(batch)} of {len(files_to_process)})")
                
                for file_info in batch:
                    try:
                        _logger.info(f"Processing file: {file_info['path']} (type: {file_info['type']})")
                        file_start_time = time.time()
                        
                        # Process and import the file
                        result = self.importer.process_file(file_info, self.processor)
                        
                        file_duration = time.time() - file_start_time
                        
                        file_results.append({
                            "file": file_info['path'],
                            "source": file_info['source'],
                            "type": file_info['type'],
                            "status": result['status'],
                            "records_processed": result.get('records_processed', 0),
                            "records_created": result.get('records_created', 0),
                            "records_updated": result.get('records_updated', 0),
                            "records_errored": result.get('records_errored', 0),
                            "records_skipped": result.get('records_skipped', 0),
                            "duration": file_duration
                        })
                        
                        _logger.info(f"Completed processing file: {file_info['path']} in {file_duration:.2f} seconds")
                    except Exception as e:
                        _logger.error(f"Error processing file {file_info['path']}: {str(e)}")
                        _logger.error(traceback.format_exc())
                        file_results.append({
                            "file": file_info['path'],
                            "source": file_info['source'],
                            "type": file_info['type'],
                            "status": "error",
                            "error": str(e),
                        })
                
                # Add a short delay between batches
                if i + batch_size < len(files_to_process):
                    time.sleep(2)

            # Calculate total statistics
            total_processed = sum(r.get("records_processed", 0) for r in file_results)
            total_created = self.importer.created_count
            total_updated = self.importer.updated_count
            total_errored = self.importer.error_count
            total_skipped = self.importer.skipped_count

            # Calculate elapsed time
            elapsed_time = time.time() - start_time

            _logger.info(f"PEP data fetch and import completed in {elapsed_time:.2f} seconds")
            _logger.info(f"Files processed: {len(file_results)}")
            _logger.info(f"Records processed: {total_processed}")
            _logger.info(f"Records created: {total_created}")
            _logger.info(f"Records updated: {total_updated}")
            _logger.info(f"Records errored: {total_errored}")
            _logger.info(f"Records skipped: {total_skipped}")

            # Update last fetch date for PEP model
            config = self.env["ir.config_parameter"].sudo()
            config.set_param(
                "compliance_management.last_pep_fetch", fields.Datetime.now()
            )

            return {
                "status": "success",
                "message": f"Processed {len(file_results)} files, created {total_created} records, updated {total_updated} records",
                "files_processed": len(file_results),
                "records_processed": total_processed,
                "records_created": total_created,
                "records_updated": total_updated,
                "records_errored": total_errored,
                "records_skipped": total_skipped,
                "duration": elapsed_time,
                "file_results": file_results,
                "source_results": source_results
            }

        except Exception as e:
            _logger.error(f"Error in fetch and import operation: {str(e)}")
            _logger.error(traceback.format_exc())

            return {
                "status": "error",
                "message": f"Error in fetch and import operation: {str(e)}",
                "files_processed": 0,
                "records_processed": 0,
                "records_created": 0,
                "records_updated": 0,
                "records_errored": 0,
                "records_skipped": 0,
                "duration": time.time() - start_time,
            }

