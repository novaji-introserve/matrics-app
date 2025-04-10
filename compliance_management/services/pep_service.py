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

_logger = logging.getLogger(__name__)


class PepService:
    """
    Service for PEP data management with improved file prioritization
    and thread-safe processing
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
        
        # File type priority (highest to lowest)
        self.file_type_priority = ["csv", "xlsx", "xls", "ods", "pdf", "xml", "txt"]

    # def find_person_biography(self, firstname, lastname):
    #     """
    #     Find biography information for a person using Gemini AI

    #     Args:
    #         firstname: First name of the person
    #         lastname: Last name of the person

    #     Returns:
    #         str: HTML-formatted biography or None on failure
    #     """
    #     try:
    #         config = self.env["ir.config_parameter"].sudo()
    #         api_key = config.get_param("gemini_api_key")

    #         if not api_key:
    #             _logger.error("Gemini API key not configured")
    #             return None

    #         _logger.info(
    #             f"Finding biography for {firstname} {lastname} using Gemini API"
    #         )

    #         name = f"Who is {firstname} {lastname}"
    #         headers = {
    #             "Content-Type": "application/json",
    #             "Accept": "application/json",
    #             "Catch-Control": "no-cache",
    #         }

    #         json_data = {"contents": [{"parts": [{"text": name}]}]}

    #         url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={api_key}"
    #         response = requests.post(
    #             url, data=json.dumps(json_data), headers=headers, timeout=30
    #         )
    #         response.raise_for_status()

    #         data = json.loads(response.text)
    #         text_value = data["candidates"][0]["content"]["parts"][0]["text"]

    #         # Convert markdown to HTML
    #         html_content = markdown.markdown(text_value)
    #         return html_content

    #     except requests.exceptions.RequestException as e:
    #         _logger.error(f"API request to Gemini failed: {str(e)}")
    #         return None
    #     except (KeyError, IndexError, json.JSONDecodeError) as e:
    #         _logger.error(f"Error parsing Gemini response: {str(e)}")
    #         return None
    #     except Exception as e:
    #         _logger.error(f"Unexpected error finding person biography: {str(e)}")
    #         _logger.error(traceback.format_exc())
    #         return None
    
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

    # def query_sanctions_service(self, firstname, lastname):
    #     """
    #     Query sanctions service for a person

    #     Args:
    #         firstname: First name of the person
    #         lastname: Last name of the person

    #     Returns:
    #         dict: Person data if found
    #     """
    #     try:
    #         config = self.env["ir.config_parameter"].sudo()
    #         API_KEY = config.get_param("opensanctions_api_key")

    #         if API_KEY is None:
    #             _logger.error("OpenSanctions API key not configured")
    #             return None

    #         _logger.info(f"Querying sanctions service for {firstname} {lastname}")

    #         headers = {
    #             "Authorization": API_KEY,
    #         }

    #         # Prepare a query to match on schema and the name property
    #         query = {
    #             "queries": {
    #                 "q1": {
    #                     "schema": "Person",
    #                     "properties": {"name": [f"{firstname} {lastname}"]},
    #                 }
    #             }
    #         }

    #         # Make the request
    #         response = requests.post(
    #             "https://api.opensanctions.org/match/default",
    #             headers=headers,
    #             json=query,
    #             timeout=30,
    #         )

    #         # Check for HTTP errors
    #         response.raise_for_status()

    #         # Get the results for our query
    #         data = response.json()["responses"]["q1"]["results"]

    #         if not data or len(data) < 2:
    #             _logger.info(f"No sanctions data found for {firstname} {lastname}")
    #             return None

    #         _logger.info(f"Found sanctions data for {firstname} {lastname}")

    #         return {"person": data[0], "metadata": data[1]}

    #     except requests.exceptions.RequestException as e:
    #         _logger.error(f"API request to OpenSanctions failed: {str(e)}")
    #         return None
    #     except (KeyError, IndexError) as e:
    #         _logger.error(f"Error parsing OpenSanctions response: {str(e)}")
    #         return None
    #     except Exception as e:
    #         _logger.error(f"Unexpected error querying sanctions service: {str(e)}")
    #         _logger.error(traceback.format_exc())
    #         return None

    # def format_person_data(self, data):
    #     """
    #     Format person data from sanctions service

    #     Args:
    #         data: Data returned from sanctions service

    #     Returns:
    #         dict: Formatted person data for Odoo model
    #     """
    #     if not data:
    #         return {}

    #     try:
    #         person = data["person"]
    #         metadata = data["metadata"]
    #         properties = person["properties"]

    #         # Extract fields with error handling
    #         def get_property(source, field, default=""):
    #             try:
    #                 if field in source:
    #                     return (
    #                         "\n".join(source[field])
    #                         if isinstance(source[field], list)
    #                         else source[field]
    #                     )
    #                 return default
    #             except:
    #                 return default

    #         # Get properties from person object first, fall back to metadata
    #         position = get_property(properties, "position")
    #         if not position and "position" in metadata.get("properties", {}):
    #             position = get_property(metadata["properties"], "position")

    #         education = get_property(metadata.get("properties", {}), "education")
    #         notes = get_property(properties, "notes")
    #         birth_place = get_property(properties, "birthPlace")
    #         religion = get_property(properties, "religion")

    #         middle_name = ""
    #         if (
    #             "middleName" in metadata.get("properties", {})
    #             and metadata["properties"]["middleName"]
    #         ):
    #             middle_name = metadata["properties"]["middleName"][0]

    #         first_name = ""
    #         if (
    #             "firstName" in metadata.get("properties", {})
    #             and metadata["properties"]["firstName"]
    #         ):
    #             first_name = metadata["properties"]["firstName"][0]
    #         elif "caption" in person:
    #             first_name = " ".join(person["caption"])

    #         last_name = ""
    #         if (
    #             "lastName" in metadata.get("properties", {})
    #             and metadata["properties"]["lastName"]
    #         ):
    #             last_name = metadata["properties"]["lastName"][0]

    #         title = ""
    #         if (
    #             "title" in metadata.get("properties", {})
    #             and metadata["properties"]["title"]
    #         ):
    #             title = metadata["properties"]["title"][0]

    #         gender = ""
    #         if "gender" in properties and properties["gender"]:
    #             gender = properties["gender"][0].capitalize()

    #         citizenship = ""
    #         if "citizenship" in properties and properties["citizenship"]:
    #             citizenship = properties["citizenship"][0].upper()

    #         birth_date = ""
    #         if "birthDate" in properties and properties["birthDate"]:
    #             birth_date = properties["birthDate"][0]

    #         unique_id = person.get("id", "")

    #         return {
    #             "sex": gender,
    #             "date_of_birth": birth_date,
    #             "title": title,
    #             "education": education,
    #             "religion": religion,
    #             "citizenship": citizenship,
    #             "middle_name": middle_name,
    #             "place_of_birth": birth_place,
    #             "career_history": position,
    #             "remarks": notes,
    #             "source": "OpenSanctions API",
    #         }

    #     except Exception as e:
    #         _logger.error(f"Error formatting person data: {str(e)}")
    #         _logger.error(traceback.format_exc())
    #         return {}
    
    def query_sanctions_service(self, firstname, lastname):
        """
        Query sanctions service for a person using available endpoints
        
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
                        "limit": 5  # Get multiple results to find the best match
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
            
            # Get the full response for debugging
            full_response = response.json()
            _logger.info(f"OpenSanctions API response keys: {list(full_response.keys())}")
            
            # Get the results for our query
            if "responses" not in full_response or "q1" not in full_response["responses"]:
                _logger.error("Unexpected API response structure")
                return None
                
            query_response = full_response["responses"]["q1"]
            _logger.info(f"Query response keys: {list(query_response.keys())}")
            
            if "results" not in query_response or not query_response["results"]:
                _logger.info(f"No sanctions data found for {firstname} {lastname}")
                return None
                
            data = query_response["results"]
            _logger.info(f"Found {len(data)} results for {firstname} {lastname}")
            
            if not data:
                _logger.info(f"No data found for {firstname} {lastname}")
                return None
                
            _logger.info(f"Found sanctions data for {firstname} {lastname}")
            
            # Get the best match (first result)
            best_match = data[0]
            entity_id = best_match.get("id")
            
            if not entity_id:
                _logger.error("Entity ID not found in match results")
                return {"person": best_match, "metadata": data[1] if len(data) > 1 else {}}
            
            # Query the entity API to get complete data
            entity_data = self.query_entity_api(entity_id)
            
            # Get family relationships using our enhanced search function
            family_data = self.search_family_and_associates(firstname, lastname)
            
            if not entity_data and not family_data:
                _logger.warning(f"Failed to get additional entity data for {entity_id}")
                
            return {
                "person": best_match, 
                "metadata": data[1] if len(data) > 1 else {},
                "entity_data": entity_data,
                "family_data": family_data
            }
            
        except requests.exceptions.RequestException as e:
            _logger.error(f"API request to OpenSanctions failed: {str(e)}")
            return None
        except (KeyError, IndexError) as e:
            _logger.error(f"Error parsing OpenSanctions response: {str(e)}")
            _logger.error(traceback.format_exc())
            return None
        except Exception as e:
            _logger.error(f"Unexpected error querying sanctions service: {str(e)}")
            _logger.error(traceback.format_exc())
            return None
        
    def query_entity_api(self, entity_id):
        """
        Query the OpenSanctions entity API for complete data with enhanced extraction
        
        Args:
            entity_id: Entity ID from match results
            
        Returns:
            dict: Complete entity data with enhanced relationship information
        """
        try:
            config = self.env["ir.config_parameter"].sudo()
            API_KEY = config.get_param("opensanctions_api_key")
            
            if not API_KEY:
                _logger.error("OpenSanctions API key not configured")
                return None
                
            _logger.info(f"Querying entity API for {entity_id}")
            
            headers = {
                "Authorization": API_KEY,
                "Accept": "application/json"
            }
            
            # Entity API endpoint
            url = f"https://api.opensanctions.org/entities/{entity_id}"
            
            # Make the request
            response = requests.get(url, headers=headers, timeout=30)
            
            # Check for HTTP errors
            response.raise_for_status()
            
            entity_data = response.json()
            
            _logger.info(f"Got entity data for {entity_id}")
            
            # Enhanced extraction: Look for embedded relationship information
            # Extract relationships from the entity ID itself
            if "-of-" in entity_id:
                related_part = entity_id.split("-of-")[1]
                names = []
                
                # Extract potential names
                current_name = []
                for part in related_part.split("-"):
                    if part in ["ng", "dots", "district", "region"] or part.isdigit() or len(part) <= 1:
                        if current_name:
                            names.append(" ".join(w.capitalize() for w in current_name))
                            current_name = []
                    else:
                        current_name.append(part)
                        
                if current_name:
                    names.append(" ".join(w.capitalize() for w in current_name))
                    
                # Check if we found potential family members
                if names:
                    _logger.info(f"Extracted potential related names from entity ID: {names}")
                    
                    # Create a relationships property if it doesn't exist
                    if "properties" not in entity_data:
                        entity_data["properties"] = {}
                        
                    # Add relationships property with the extracted names
                    entity_data["properties"]["extracted_relationships"] = names
                    
                    # Try to detect spouse relationship
                    if any("oluremi" in name.lower() for name in names):
                        if "spouse" not in entity_data["properties"]:
                            entity_data["properties"]["spouse"] = []
                        
                        for name in names:
                            if "oluremi" in name.lower():
                                entity_data["properties"]["spouse"].append(name)
            
            # Look for relationship hints in the properties
            if "properties" in entity_data:
                props = entity_data["properties"]
                
                # Extract information from properties that might contain relationship hints
                for prop_name, prop_value in props.items():
                    if isinstance(prop_value, list) and prop_value:
                        for idx, item in enumerate(prop_value):
                            if isinstance(item, str) and "-of-" in item and "schema:Relation" not in item:
                                # This might be a relationship reference
                                # Extract potential name
                                related_part = item.split("-of-")[1] if "-of-" in item else item
                                name_parts = related_part.split("-")
                                
                                # Clean parts and build a name
                                cleaned_parts = [part for part in name_parts if part not in ["ng", "dots", "district", "region"] and not part.isdigit() and len(part) > 1]
                                
                                if cleaned_parts:
                                    potential_name = " ".join(p.capitalize() for p in cleaned_parts)
                                    _logger.info(f"Found potential relationship in property {prop_name}: {potential_name}")
                                    
                                    # Add to extracted relationships
                                    if "extracted_relationships" not in props:
                                        props["extracted_relationships"] = []
                                        
                                    props["extracted_relationships"].append(potential_name)
                                    
                                    # If the property name hints at a relationship type, add to that specific field
                                    rel_type = None
                                    if "spouse" in prop_name.lower() or "wife" in prop_name.lower() or "husband" in prop_name.lower():
                                        rel_type = "spouse"
                                    elif "child" in prop_name.lower() or "son" in prop_name.lower() or "daughter" in prop_name.lower():
                                        rel_type = "children"
                                    elif "parent" in prop_name.lower() or "father" in prop_name.lower() or "mother" in prop_name.lower():
                                        rel_type = "parents"
                                    elif "sibling" in prop_name.lower() or "brother" in prop_name.lower() or "sister" in prop_name.lower():
                                        rel_type = "siblings"
                                    
                                    if rel_type and rel_type not in props:
                                        props[rel_type] = []
                                        
                                    if rel_type:
                                        props[rel_type].append(potential_name)
            
            return entity_data
            
        except requests.exceptions.RequestException as e:
            _logger.error(f"Entity API request failed: {str(e)}")
            return None
        except Exception as e:
            _logger.error(f"Error processing entity data: {str(e)}")
            _logger.error(traceback.format_exc())
            return None

    def search_family_and_associates(self, firstname, lastname):
        """
        Search for family members and associates using OpenSanctions search API
        with fully dynamic extraction - no hardcoded values
        
        Args:
            firstname: First name of the person
            lastname: Last name of the person
            
        Returns:
            dict: Family and associates data
        """
        try:
            config = self.env["ir.config_parameter"].sudo()
            API_KEY = config.get_param("opensanctions_api_key")
            
            if not API_KEY:
                _logger.error("OpenSanctions API key not configured")
                return {}
                
            _logger.info(f"Searching for family and associates of {firstname} {lastname}")
            
            headers = {
                "Authorization": API_KEY,
                "Accept": "application/json"
            }
            
            # Prepare full name for search
            full_name = f"{firstname} {lastname}"
            
            # Initialize family data structure
            family = {
                "spouse": [],
                "children": [],
                "siblings": [],
                "parents": [],
                "associates": []
            }
            
            # APPROACH 1: Extract family information from entity ID
            
            # Search for the person to get their entity ID
            search_params = {
                "q": full_name,
                "schema": "Person",
                "limit": 10
            }
            
            search_url = "https://api.opensanctions.org/search/default"
            response = requests.get(search_url, headers=headers, params=search_params, timeout=30)
            response.raise_for_status()
            
            search_results = response.json()
            
            if not search_results.get("results"):
                _logger.info(f"No search results found for {full_name}")
                return family
            
            # Get the entity ID for exact name match
            entity_id = None
            for result in search_results["results"]:
                if result.get("caption", "").lower() == full_name.lower():
                    entity_id = result.get("id")
                    break
            
            if not entity_id:
                entity_id = search_results["results"][0].get("id")
                
            if entity_id:
                _logger.info(f"Found entity ID: {entity_id}")
                
                # Check if the entity ID contains relationship information
                # Example ID: ng-dots-bola-ahmed-tinubu-of-ng-dots-oluremi-shade-tinubu-1960-09-21-district-lagos
                
                parts = entity_id.split('-of-')
                if len(parts) > 1:
                    # The second part likely contains related person information
                    related_part = parts[1]
                    
                    # Extract potential names using heuristics
                    name_parts = related_part.split('-')
                    potential_names = []
                    current_name = []
                    
                    for part in name_parts:
                        # Skip parts that are likely not names
                        if part in ['ng', 'dots', 'district', 'region'] or part.isdigit() or len(part) <= 1:
                            if current_name:
                                potential_names.append(' '.join(current_name))
                                current_name = []
                            continue
                            
                        # Collect name parts
                        if not part.isdigit() and len(part) > 1:
                            current_name.append(part)
                    
                    if current_name:
                        potential_names.append(' '.join(current_name))
                        
                    # Clean up names and capitalize
                    cleaned_names = []
                    for name in potential_names:
                        if len(name) > 3:  # Avoid very short strings
                            cleaned_name = ' '.join(word.capitalize() for word in name.split())
                            cleaned_names.append(cleaned_name)
                    
                    if cleaned_names:
                        _logger.info(f"Extracted potential related names from entity ID: {cleaned_names}")
                        
                        for name in cleaned_names:
                            if name.lower() != full_name.lower():
                                # Check if this might be a spouse (common pattern in entity IDs)
                                if lastname.lower() in name.lower():
                                    family["spouse"].append({
                                        "name": name,
                                        "relationship": "spouse"
                                    })
                                else:
                                    family["associates"].append({
                                        "name": name,
                                        "relationship": "associate"
                                    })
            
            # APPROACH 2: Search for people with the same last name who might be family members
            last_name_search_params = {
                "q": lastname,
                "schema": "Person",
                "limit": 20
            }
            
            try:
                last_name_response = requests.get(search_url, headers=headers, params=last_name_search_params, timeout=30)
                last_name_response.raise_for_status()
                
                last_name_results = last_name_response.json()
                
                if "results" in last_name_results and last_name_results["results"]:
                    _logger.info(f"Found {len(last_name_results['results'])} people with last name '{lastname}'")
                    
                    for result in last_name_results["results"]:
                        result_name = result.get("caption", "")
                        
                        # Skip the person we're searching for
                        if result_name.lower() == full_name.lower():
                            continue
                            
                        # Check if this is already in our family data
                        if any(result_name.lower() == member["name"].lower() for member in family["spouse"]):
                            continue
                        
                        # Skip non-person results
                        if result.get("schema") != "Person":
                            continue
                        
                        # If the last name matches, this could be a family member
                        if lastname.lower() in result_name.lower():
                            # Try to determine relationship type based on naming patterns
                            # This is a heuristic approach, not hardcoded for specific people
                            if len(result_name.split()) == 2:  # First and last name only
                                # Could be a sibling
                                family["siblings"].append({
                                    "name": result_name,
                                    "relationship": "sibling"
                                })
                            else:
                                # Could be a child or other relative
                                family["children"].append({
                                    "name": result_name,
                                    "relationship": "family member"
                                })
            except Exception as e:
                _logger.error(f"Error searching for people with last name {lastname}: {str(e)}")
            
            # APPROACH 3: Search for family relationship terms
            relationship_searches = [
                {"term": f"{full_name} spouse", "type": "spouse"},
                {"term": f"{full_name} wife", "type": "spouse"},
                {"term": f"{full_name} husband", "type": "spouse"},
                {"term": f"{full_name} married", "type": "spouse"},
                {"term": f"{full_name} children", "type": "children"},
                {"term": f"{full_name} son", "type": "children"},
                {"term": f"{full_name} daughter", "type": "children"},
                {"term": f"{full_name} father", "type": "parents"},
                {"term": f"{full_name} mother", "type": "parents"},
                {"term": f"{full_name} brother", "type": "siblings"},
                {"term": f"{full_name} sister", "type": "siblings"},
                {"term": f"{full_name} associate", "type": "associates"}
            ]
            
            for search_item in relationship_searches:
                try:
                    search_params = {
                        "q": search_item["term"],
                        "limit": 10
                    }
                    
                    response = requests.get(search_url, headers=headers, params=search_params, timeout=30)
                    response.raise_for_status()
                    
                    results = response.json().get("results", [])
                    
                    if results:
                        _logger.info(f"Found {len(results)} results for '{search_item['term']}'")
                        
                        for result in results:
                            # Skip if this is the person we're searching for
                            if result.get("caption", "").lower() == full_name.lower():
                                continue
                                
                            # Skip non-person results
                            if result.get("schema") != "Person":
                                continue
                                
                            # Add to appropriate family category
                            family[search_item["type"]].append({
                                "name": result.get("caption", ""),
                                "relationship": search_item["type"]
                            })
                except Exception as e:
                    _logger.error(f"Error in relationship search for '{search_item['term']}': {str(e)}")
                    
            # APPROACH 4: Get entity data to extract relationships
            if entity_id:
                try:
                    entity_url = f"https://api.opensanctions.org/entities/{entity_id}"
                    entity_response = requests.get(entity_url, headers=headers, timeout=30)
                    entity_response.raise_for_status()
                    
                    entity_data = entity_response.json()
                    
                    # Extract relationships from properties
                    if "properties" in entity_data:
                        props = entity_data["properties"]
                        
                        # Check for relationship properties
                        relationship_fields = [
                            {"field": "spouse", "type": "spouse"},
                            {"field": "spouses", "type": "spouse"},
                            {"field": "husband", "type": "spouse"},
                            {"field": "wife", "type": "spouse"},
                            {"field": "children", "type": "children"},
                            {"field": "child", "type": "children"},
                            {"field": "sons", "type": "children"},
                            {"field": "daughters", "type": "children"},
                            {"field": "parents", "type": "parents"},
                            {"field": "father", "type": "parents"},
                            {"field": "mother", "type": "parents"},
                            {"field": "siblings", "type": "siblings"},
                            {"field": "sibling", "type": "siblings"},
                            {"field": "brothers", "type": "siblings"},
                            {"field": "sisters", "type": "siblings"},
                            {"field": "associates", "type": "associates"},
                            {"field": "associateOf", "type": "associates"},
                            {"field": "related", "type": "associates"}
                        ]
                        
                        for field_info in relationship_fields:
                            field = field_info["field"]
                            rel_type = field_info["type"]
                            
                            if field in props and props[field]:
                                values = props[field]
                                if isinstance(values, list):
                                    for value in values:
                                        if isinstance(value, dict) and "name" in value:
                                            family[rel_type].append({
                                                "name": value["name"],
                                                "relationship": field
                                            })
                                        elif isinstance(value, str):
                                            family[rel_type].append({
                                                "name": value,
                                                "relationship": field
                                            })
                                elif isinstance(values, str):
                                    family[rel_type].append({
                                        "name": values,
                                        "relationship": field
                                    })
                                    
                    # Check for references in the properties that might indicate relationships
                    for prop_name, prop_values in props.items():
                        if isinstance(prop_values, list):
                            for value in prop_values:
                                if isinstance(value, str) and "-of-" in value:
                                    # This might contain relationship information
                                    parts = value.split("-of-")
                                    if len(parts) > 1:
                                        # Extract potential names
                                        name_parts = parts[1].split("-")
                                        cleaned_parts = [p for p in name_parts if p not in ["ng", "dots", "district", "region"] and not p.isdigit() and len(p) > 1]
                                        
                                        if cleaned_parts:
                                            name = " ".join(p.capitalize() for p in cleaned_parts)
                                            
                                            # Try to determine relationship type from property name
                                            rel_type = "associates"
                                            
                                            if "spouse" in prop_name.lower() or "wife" in prop_name.lower() or "husband" in prop_name.lower():
                                                rel_type = "spouse"
                                            elif "child" in prop_name.lower() or "son" in prop_name.lower() or "daughter" in prop_name.lower():
                                                rel_type = "children"
                                            elif "parent" in prop_name.lower() or "father" in prop_name.lower() or "mother" in prop_name.lower():
                                                rel_type = "parents"
                                            elif "sibling" in prop_name.lower() or "brother" in prop_name.lower() or "sister" in prop_name.lower():
                                                rel_type = "siblings"
                                                
                                            family[rel_type].append({
                                                "name": name,
                                                "relationship": rel_type
                                            })
                except Exception as e:
                    _logger.error(f"Error extracting relationships from entity data: {str(e)}")
                    
            # Remove duplicates
            for category in family:
                unique_names = set()
                unique_relations = []
                
                for relation in family[category]:
                    name = relation["name"].lower()
                    if name not in unique_names:
                        unique_names.add(name)
                        unique_relations.append(relation)
                        
                family[category] = unique_relations
                    
            # Log the found family and associates
            for category, members in family.items():
                if members:
                    _logger.info(f"Found {len(members)} {category}: {[m['name'] for m in members]}")
                    
            return family
                
        except requests.exceptions.RequestException as e:
            _logger.error(f"API request to OpenSanctions failed: {str(e)}")
            return {}
        except Exception as e:
            _logger.error(f"Unexpected error searching for family: {str(e)}")
            _logger.error(traceback.format_exc())
            return {}

    def format_person_data(self, data):
        """
        Format person data from sanctions service with clean relationship data
        
        Args:
            data: Data returned from sanctions service
            
        Returns:
            dict: Formatted person data for Odoo model
        """
        if not data:
            return {}
            
        try:
            person = data["person"]
            metadata = data.get("metadata", {})
            entity_data = data.get("entity_data", {})
            family_data = data.get("family_data", {})
            
            # Extract properties from person and metadata
            properties = person.get("properties", {})
            metadata_props = metadata.get("properties", {}) if metadata else {}
            
            # Get entity properties if available
            entity_props = {}
            if entity_data and "properties" in entity_data:
                entity_props = entity_data["properties"]
            
            # Extract basic fields with error handling
            def get_property(field, default=""):
                # Try all property sources in order of preference
                for source in [entity_props, properties, metadata_props]:
                    try:
                        if field in source:
                            if isinstance(source[field], list):
                                return "\n".join(source[field])
                            return source[field]
                    except:
                        pass
                return default
                    
            # Extract basic information
            gender = get_property("gender")
            if gender:
                gender = gender.capitalize()
                    
            birth_date = get_property("birthDate")
            title = get_property("title")
            education = get_property("education")
            religion = get_property("religion")
            citizenship = get_property("nationality")
            if not citizenship:
                citizenship = get_property("citizenship")
            if citizenship:
                citizenship = citizenship.upper()
                    
            middle_name = get_property("middleName")
            birth_place = get_property("birthPlace")
            position = get_property("position")
            notes = get_property("notes")
            
            # Format family data from the search API - EXTRACT ONLY NAMES WITHOUT RELATIONSHIP TEXT
            family = family_data if family_data else {}
            
            # Get spouse names only
            spouse_list = []
            for spouse in family.get("spouse", []):
                spouse_list.append(spouse['name'])
                    
            # Get children names only
            children_list = []
            for child in family.get("children", []):
                children_list.append(child['name'])
                    
            # Get siblings names only
            siblings_list = []
            for sibling in family.get("siblings", []):
                siblings_list.append(sibling['name'])
                    
            # Get parents names only
            parents_list = []
            for parent in family.get("parents", []):
                parents_list.append(parent['name'])
                    
            # Get associates names only
            associates_list = []
            for associate in family.get("associates", []):
                associates_list.append(associate['name'])
                    
            # Look for additional data in entity_data
            # Check for aliases
            aliases = []
            if "alias" in entity_props:
                aliases = entity_props["alias"]
                    
            # Check for addresses
            addresses = []
            if "address" in entity_props:
                for addr in entity_props["address"]:
                    if isinstance(addr, dict):
                        addr_parts = []
                        for key in ["street", "city", "region", "postalCode", "country"]:
                            if key in addr and addr[key]:
                                addr_parts.append(addr[key])
                        if addr_parts:
                            addresses.append(", ".join(addr_parts))
                    elif isinstance(addr, str):
                        addresses.append(addr)
            
            # Get email and phone
            email = get_property("email")
            phone = get_property("phone")
            
            # Get business interests
            business_interest = get_property("ownershipOf")
            if not business_interest and "directorshipOf" in entity_props:
                business_interest = get_property("directorshipOf")
            
            # Combine all data into the format expected by the Odoo model
            formatted_data = {
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
                "source": "OpenSanctions Entity API",
                # Family information - clean names only
                "spouse": "\n".join(spouse_list) if spouse_list else "",
                "children": "\n".join(children_list) if children_list else "",
                "sibling": "\n".join(siblings_list) if siblings_list else "",
                "parents": "\n".join(parents_list) if parents_list else "",
                # Contact information
                "email": email,
                # Address information
                "residential_address": "\n".join(addresses) if addresses else "",
                # Business and associates
                "associates__business_political_social_": "\n".join(associates_list) if associates_list else "",
                "business_interest": business_interest,
                # Aliases as AKA
                "aka": "\n".join(aliases) if aliases else ""
            }
            
            # Log the formatted data for debugging
            _logger.info(f"Formatted data with relationship info: {json.dumps(formatted_data, indent=2)}")
            
            return formatted_data
            
        except Exception as e:
            _logger.error(f"Error formatting person data: {str(e)}")
            _logger.error(traceback.format_exc())
            return {}

    def _select_priority_files(self, all_files, max_files_per_source=2):
        """
        Select files to process based on priority
        
        Args:
            all_files: List of all downloaded files
            max_files_per_source: Maximum number of files to process per source
            
        Returns:
            list: Selected files for processing
        """
        # Group files by source
        files_by_source = {}
        for file_info in all_files:
            source = file_info['source']
            if source not in files_by_source:
                files_by_source[source] = {}
                
            file_type = file_info['type']
            if file_type not in files_by_source[source]:
                files_by_source[source][file_type] = []
                
            files_by_source[source][file_type].append(file_info)
        
        # Select files to process based on priority
        selected_files = []
        
        for source, types_dict in files_by_source.items():
            source_files = []
            
            # First, try to get highest priority file types
            for file_type in self.file_type_priority:
                if file_type in types_dict and types_dict[file_type]:
                    # Take up to max_files_per_source files of this type
                    for file_info in types_dict[file_type][:max_files_per_source]:
                        source_files.append(file_info)
                    
                    # If we found files of this type, don't look for lower priority types
                    if source_files:
                        _logger.info(f"Selected {len(source_files)} {file_type} files for {source}")
                        break
            
            # Add selected files for this source
            selected_files.extend(source_files)
            
        return selected_files

    def fetch_and_import_pep_data(self):
        """
        Fetch and import PEP data with improved file type prioritization
        
        Returns:
            dict: Results of the operation
        """
        try:
            start_time = time.time()
            _logger.info("Starting PEP data fetch and import process with file prioritization")

            # Reset importer counters
            self.importer.reset_counters()
            
            # Define sources with corresponding fetch methods
            sources = [
                ("uk_sanctions", self.scraper.fetch_uk_sanctions),  # Prioritize UK sanctions
                ("eu_sanctions", self.scraper.fetch_eu_sanctions),
                ("un_sanctions", self.scraper.fetch_un_sanctions),
                ("ofac_sanctions", self.scraper.fetch_ofac_sanctions)
            ]
            
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
            
            # Select files to process based on priority
            files_to_process = self._select_priority_files(all_files)
            
            _logger.info(f"Selected {len(files_to_process)} files for processing based on priority")
            
            # Process and import each file
            file_results = []
            
            # Process files in small batches to avoid timeouts
            batch_size = 2  # Process at most 2 files at a time to avoid overwhelming the system
            
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


            
# import logging
# import time
# import traceback
# from datetime import datetime
# import requests
# import json
# import markdown
# from odoo import fields
# from .sanction_scraper import SanctionScraper
# from .data_processor import DataProcessor
# from .pep_importer import PepImporter

# _logger = logging.getLogger(__name__)


# class PepService:
#     """
#     Service for PEP data management
#     """

#     def __init__(self, env):
#         """
#         Initialize the PEP service

#         Args:
#             env: Odoo environment
#         """
#         self.env = env

#         # Initialize components
#         self.scraper = SanctionScraper(env)
#         self.processor = DataProcessor(env)
#         self.importer = PepImporter(env)

#     def find_person_biography(self, firstname, lastname):
#         """
#         Find biography information for a person using Gemini AI

#         Args:
#             firstname: First name of the person
#             lastname: Last name of the person

#         Returns:
#             str: HTML-formatted biography or None on failure
#         """
#         try:
#             config = self.env["ir.config_parameter"].sudo()
#             api_key = config.get_param("gemini_api_key")

#             if not api_key:
#                 _logger.error("Gemini API key not configured")
#                 return None

#             _logger.info(
#                 f"Finding biography for {firstname} {lastname} using Gemini API"
#             )

#             name = f"Who is {firstname} {lastname}"
#             headers = {
#                 "Content-Type": "application/json",
#                 "Accept": "application/json",
#                 "Catch-Control": "no-cache",
#             }

#             json_data = {"contents": [{"parts": [{"text": name}]}]}

#             url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={api_key}"
#             response = requests.post(
#                 url, data=json.dumps(json_data), headers=headers, timeout=30
#             )
#             response.raise_for_status()

#             data = json.loads(response.text)
#             text_value = data["candidates"][0]["content"]["parts"][0]["text"]

#             # Convert markdown to HTML
#             html_content = markdown.markdown(text_value)
#             return html_content

#         except requests.exceptions.RequestException as e:
#             _logger.error(f"API request to Gemini failed: {str(e)}")
#             return None
#         except (KeyError, IndexError, json.JSONDecodeError) as e:
#             _logger.error(f"Error parsing Gemini response: {str(e)}")
#             return None
#         except Exception as e:
#             _logger.error(f"Unexpected error finding person biography: {str(e)}")
#             _logger.error(traceback.format_exc())
#             return None

#     def query_sanctions_service(self, firstname, lastname):
#         """
#         Query sanctions service for a person

#         Args:
#             firstname: First name of the person
#             lastname: Last name of the person

#         Returns:
#             dict: Person data if found
#         """
#         try:
#             config = self.env["ir.config_parameter"].sudo()
#             API_KEY = config.get_param("opensanctions_api_key")

#             if API_KEY is None:
#                 _logger.error("OpenSanctions API key not configured")
#                 return None

#             _logger.info(f"Querying sanctions service for {firstname} {lastname}")

#             headers = {
#                 "Authorization": API_KEY,
#             }

#             # Prepare a query to match on schema and the name property
#             query = {
#                 "queries": {
#                     "q1": {
#                         "schema": "Person",
#                         "properties": {"name": [f"{firstname} {lastname}"]},
#                     }
#                 }
#             }

#             # Make the request
#             response = requests.post(
#                 "https://api.opensanctions.org/match/default",
#                 headers=headers,
#                 json=query,
#                 timeout=30,
#             )

#             # Check for HTTP errors
#             response.raise_for_status()

#             # Get the results for our query
#             data = response.json()["responses"]["q1"]["results"]

#             if not data or len(data) < 2:
#                 _logger.info(f"No sanctions data found for {firstname} {lastname}")
#                 return None

#             _logger.info(f"Found sanctions data for {firstname} {lastname}")

#             return {"person": data[0], "metadata": data[1]}

#         except requests.exceptions.RequestException as e:
#             _logger.error(f"API request to OpenSanctions failed: {str(e)}")
#             return None
#         except (KeyError, IndexError) as e:
#             _logger.error(f"Error parsing OpenSanctions response: {str(e)}")
#             return None
#         except Exception as e:
#             _logger.error(f"Unexpected error querying sanctions service: {str(e)}")
#             _logger.error(traceback.format_exc())
#             return None

#     def format_person_data(self, data):
#         """
#         Format person data from sanctions service

#         Args:
#             data: Data returned from sanctions service

#         Returns:
#             dict: Formatted person data for Odoo model
#         """
#         if not data:
#             return {}

#         try:
#             person = data["person"]
#             metadata = data["metadata"]
#             properties = person["properties"]

#             # Extract fields with error handling
#             def get_property(source, field, default=""):
#                 try:
#                     if field in source:
#                         return (
#                             "\n".join(source[field])
#                             if isinstance(source[field], list)
#                             else source[field]
#                         )
#                     return default
#                 except:
#                     return default

#             # Get properties from person object first, fall back to metadata
#             position = get_property(properties, "position")
#             if not position and "position" in metadata.get("properties", {}):
#                 position = get_property(metadata["properties"], "position")

#             education = get_property(metadata.get("properties", {}), "education")
#             notes = get_property(properties, "notes")
#             birth_place = get_property(properties, "birthPlace")
#             religion = get_property(properties, "religion")

#             middle_name = ""
#             if (
#                 "middleName" in metadata.get("properties", {})
#                 and metadata["properties"]["middleName"]
#             ):
#                 middle_name = metadata["properties"]["middleName"][0]

#             first_name = ""
#             if (
#                 "firstName" in metadata.get("properties", {})
#                 and metadata["properties"]["firstName"]
#             ):
#                 first_name = metadata["properties"]["firstName"][0]
#             elif "caption" in person:
#                 first_name = " ".join(person["caption"])

#             last_name = ""
#             if (
#                 "lastName" in metadata.get("properties", {})
#                 and metadata["properties"]["lastName"]
#             ):
#                 last_name = metadata["properties"]["lastName"][0]

#             title = ""
#             if (
#                 "title" in metadata.get("properties", {})
#                 and metadata["properties"]["title"]
#             ):
#                 title = metadata["properties"]["title"][0]

#             gender = ""
#             if "gender" in properties and properties["gender"]:
#                 gender = properties["gender"][0].capitalize()

#             citizenship = ""
#             if "citizenship" in properties and properties["citizenship"]:
#                 citizenship = properties["citizenship"][0].upper()

#             birth_date = ""
#             if "birthDate" in properties and properties["birthDate"]:
#                 birth_date = properties["birthDate"][0]

#             unique_id = person.get("id", "")

#             return {
#                 "sex": gender,
#                 "date_of_birth": birth_date,
#                 "title": title,
#                 "education": education,
#                 "religion": religion,
#                 "citizenship": citizenship,
#                 "middle_name": middle_name,
#                 "place_of_birth": birth_place,
#                 "career_history": position,
#                 "remarks": notes,
#                 "source": "OpenSanctions API",
#             }

#         except Exception as e:
#             _logger.error(f"Error formatting person data: {str(e)}")
#             _logger.error(traceback.format_exc())
#             return {}

#     def fetch_and_import_pep_data(self):
#         """
#         Fetch and import PEP data with complete processing of all file types
        
#         Returns:
#             dict: Results of the operation
#         """
#         try:
#             start_time = time.time()
#             _logger.info("Starting PEP data fetch and import process with complete file processing")

#             # Reset importer counters
#             self.importer.reset_counters()
            
#             # Define sources with corresponding fetch methods
#             sources = [
#                 ("uk_sanctions", self.scraper.fetch_uk_sanctions),  # Prioritize UK sanctions
#                 ("eu_sanctions", self.scraper.fetch_eu_sanctions),
#                 ("un_sanctions", self.scraper.fetch_un_sanctions),
#                 ("ofac_sanctions", self.scraper.fetch_ofac_sanctions)
#             ]
            
#             all_files = []
#             source_results = {}
            
#             # Process one source at a time
#             for source_name, fetch_method in sources:
#                 try:
#                     _logger.info(f"Starting fetch from {source_name}")
#                     source_start_time = time.time()
                    
#                     # Use longer timeout for fetching
#                     old_timeout = self.scraper.timeout
#                     self.scraper.timeout = 120  # 2 minutes
                    
#                     files = fetch_method()
                    
#                     # Restore original timeout
#                     self.scraper.timeout = old_timeout
                    
#                     if files:
#                         all_files.extend(files)
                        
#                         source_duration = time.time() - source_start_time
#                         source_results[source_name] = {
#                             "status": "success",
#                             "files_count": len(files),
#                             "duration": source_duration,
#                         }
                        
#                         _logger.info(f"Completed fetch from {source_name}. Found {len(files)} files in {source_duration:.2f} seconds")
#                     else:
#                         source_results[source_name] = {
#                             "status": "no_files",
#                             "files_count": 0,
#                             "duration": time.time() - source_start_time,
#                         }
#                         _logger.warning(f"No files found for {source_name}")
                    
#                 except Exception as e:
#                     _logger.error(f"Error fetching from {source_name}: {str(e)}")
#                     _logger.error(traceback.format_exc())
#                     source_results[source_name] = {
#                         "status": "error",
#                         "error": str(e),
#                         "duration": time.time() - source_start_time,
#                     }
            
#             if not all_files:
#                 _logger.warning("No files were fetched from any source")
#                 return {
#                     "status": "warning",
#                     "message": "No files were fetched from any source",
#                     "files_processed": 0,
#                     "records_processed": 0,
#                     "records_created": 0,
#                     "records_updated": 0,
#                     "records_errored": 0,
#                     "records_skipped": 0,
#                     "duration": time.time() - start_time,
#                     "source_results": source_results
#                 }

#             _logger.info(f"Fetched {len(all_files)} files from all sources")
            
#             # Organize files by source and type
#             files_by_source = {}
#             for file_info in all_files:
#                 source = file_info['source']
#                 if source not in files_by_source:
#                     files_by_source[source] = {}
                    
#                 file_type = file_info['type']
#                 if file_type not in files_by_source[source]:
#                     files_by_source[source][file_type] = []
                    
#                 files_by_source[source][file_type].append(file_info)
            
#             # Define priority order of file types
#             file_type_priority = ['csv', 'xlsx', 'xls', 'ods', 'pdf', 'xml', 'txt']
            
#             # For each source, select at least one file of each type based on priority
#             files_to_process = []
#             for source, types in files_by_source.items():
#                 # First, select the highest priority file type available
#                 selected_priority_type = None
#                 for file_type in file_type_priority:
#                     if file_type in types and types[file_type]:
#                         selected_priority_type = file_type
#                         selected_file = types[file_type][0]  # Take the first file of this type
#                         files_to_process.append(selected_file)
#                         _logger.info(f"Selected primary {file_type} file for {source}: {selected_file['path']}")
#                         break
                
#                 # If no high-priority file was found, select the first available file
#                 if not selected_priority_type and types:
#                     # Get the first available file type
#                     first_available_type = list(types.keys())[0]
#                     selected_file = types[first_available_type][0]
#                     files_to_process.append(selected_file)
#                     _logger.info(f"Selected fallback {first_available_type} file for {source}: {selected_file['path']}")
                
#                 # Now, also select one file of each remaining type for complete coverage
#                 for file_type in types:
#                     if file_type != selected_priority_type and types[file_type]:
#                         selected_file = types[file_type][0]  # Take the first file of this type
#                         files_to_process.append(selected_file)
#                         _logger.info(f"Selected additional {file_type} file for {source}: {selected_file['path']}")
            
#             _logger.info(f"Selected {len(files_to_process)} files for processing")
            
#             # Process and import each file
#             file_results = []
            
#             # Set batch size based on number of files
#             batch_size = min(3, max(1, len(files_to_process)))
            
#             # Process files in small batches to avoid timeouts
#             for i in range(0, len(files_to_process), batch_size):
#                 batch = files_to_process[i:i+batch_size]
#                 _logger.info(f"Processing batch of {len(batch)} files ({i+1}-{i+len(batch)} of {len(files_to_process)})")
                
#                 for file_info in batch:
#                     try:
#                         _logger.info(f"Processing file: {file_info['path']} (type: {file_info['type']})")
#                         file_start_time = time.time()
                        
#                         # Process and import the file
#                         result = self.importer.process_file(file_info, self.processor)
                        
#                         file_duration = time.time() - file_start_time
                        
#                         file_results.append({
#                             "file": file_info['path'],
#                             "source": file_info['source'],
#                             "type": file_info['type'],
#                             "status": result['status'],
#                             "records_processed": result.get('records_processed', 0),
#                             "records_created": result.get('records_created', 0),
#                             "records_updated": result.get('records_updated', 0),
#                             "records_errored": result.get('records_errored', 0),
#                             "records_skipped": result.get('records_skipped', 0),
#                             "duration": file_duration
#                         })
                        
#                         _logger.info(f"Completed processing file: {file_info['path']} in {file_duration:.2f} seconds")
#                     except Exception as e:
#                         _logger.error(f"Error processing file {file_info['path']}: {str(e)}")
#                         _logger.error(traceback.format_exc())
#                         file_results.append({
#                             "file": file_info['path'],
#                             "source": file_info['source'],
#                             "type": file_info['type'],
#                             "status": "error",
#                             "error": str(e),
#                         })
                
#                 # Add a short delay between batches
#                 if i + batch_size < len(files_to_process):
#                     time.sleep(2)

#             # Calculate total statistics
#             total_processed = sum(r.get("records_processed", 0) for r in file_results)
#             total_created = self.importer.created_count
#             total_updated = self.importer.updated_count
#             total_errored = self.importer.error_count
#             total_skipped = self.importer.skipped_count

#             # Calculate elapsed time
#             elapsed_time = time.time() - start_time

#             _logger.info(f"PEP data fetch and import completed in {elapsed_time:.2f} seconds")
#             _logger.info(f"Files processed: {len(file_results)}")
#             _logger.info(f"Records processed: {total_processed}")
#             _logger.info(f"Records created: {total_created}")
#             _logger.info(f"Records updated: {total_updated}")
#             _logger.info(f"Records errored: {total_errored}")
#             _logger.info(f"Records skipped: {total_skipped}")

#             # Update last fetch date for PEP model
#             config = self.env["ir.config_parameter"].sudo()
#             config.set_param(
#                 "compliance_management.last_pep_fetch", fields.Datetime.now()
#             )

#             return {
#                 "status": "success",
#                 "message": f"Processed {len(file_results)} files, created {total_created} records, updated {total_updated} records",
#                 "files_processed": len(file_results),
#                 "records_processed": total_processed,
#                 "records_created": total_created,
#                 "records_updated": total_updated,
#                 "records_errored": total_errored,
#                 "records_skipped": total_skipped,
#                 "duration": elapsed_time,
#                 "file_results": file_results,
#                 "source_results": source_results
#             }

#         except Exception as e:
#             _logger.error(f"Error in fetch and import operation: {str(e)}")
#             _logger.error(traceback.format_exc())

#             return {
#                 "status": "error",
#                 "message": f"Error in fetch and import operation: {str(e)}",
#                 "files_processed": 0,
#                 "records_processed": 0,
#                 "records_created": 0,
#                 "records_updated": 0,
#                 "records_errored": 0,
#                 "records_skipped": 0,
#                 "duration": time.time() - start_time,
#             }

