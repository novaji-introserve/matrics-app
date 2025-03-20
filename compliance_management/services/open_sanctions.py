import requests
import logging
import os
import tempfile
import json
import traceback
from urllib.parse import urljoin, urlparse
from datetime import datetime

_logger = logging.getLogger(__name__)

class OpenSanctions:
    """
    Service for fetching data from OpenSanctions.org
    Handles both CSV file downloading and API interactions
    """
    
    def __init__(self, env, storage_dir=None):
        """
        Initialize the OpenSanctions service
        
        Args:
            env: Odoo environment
            storage_dir: Optional custom storage directory
        """
        self.env = env
        # Get configuration from system parameters
        params = self.env['ir.config_parameter'].sudo()
        self.base_url = params.get_param('compliance_management.opensanctions_base_url', 'https://data.opensanctions.org')
        self.api_endpoint = params.get_param('compliance_management.opensanctions_api_url', 'https://api.opensanctions.org')
        self.timeout = int(params.get_param('compliance_management.opensanctions_timeout', '30'))

        # Set up the storage directory
        if storage_dir:
            self.storage_dir = storage_dir
        else:
            # Try to get from system parameters
            self.storage_dir = params.get_param('compliance_management.pep_storage_dir')
            
            if not self.storage_dir:
                # Fallback to default path
                self.storage_dir = os.path.join(
                    os.path.dirname(os.path.dirname(__file__)), "media", "pep_list_data"
                )

        # Create the media directory if it doesn't exist
        try:
            os.makedirs(self.storage_dir, exist_ok=True)
            _logger.info(f"Using storage directory: {self.storage_dir}")
        except Exception as e:
            _logger.error(f"Failed to create storage directory {self.storage_dir}: {str(e)}")
            # Fallback to temporary directory if we can't create the storage dir
            self.storage_dir = tempfile.mkdtemp(prefix="opensanctions_")
            _logger.warning(f"Using temporary directory as fallback: {self.storage_dir}")
    
    def discover_latest_csv_url(self):
        """
        Discover the latest OpenSanctions CSV URL by querying their catalog
        
        Returns:
            str: Complete URL to the latest CSV, or None if discovery fails
        """
        try:
            _logger.info("Discovering latest OpenSanctions CSV URL")
            
            # First try the catalog API
            catalog_url = "https://data.opensanctions.org/datasets/catalog.json"
            
            headers = {
                'User-Agent': 'Odoo/Compliance Module'
            }
            
            response = requests.get(catalog_url, headers=headers, timeout=self.timeout)
            
            if response.status_code != 200:
                _logger.warning(f"Failed to fetch catalog: {response.status_code}")
                return "https://data.opensanctions.org/datasets/latest/peps/targets.simple.csv"
                
            catalog = response.json()
            
            # Find the latest PEP dataset
            pep_datasets = [d for d in catalog.get('datasets', []) if d.get('name') == 'peps']
            if not pep_datasets:
                _logger.warning("No PEP datasets found in catalog")
                return "https://data.opensanctions.org/datasets/latest/peps/targets.simple.csv"
                
            # Sort by date
            pep_datasets.sort(key=lambda x: x.get('updated', ''), reverse=True)
            latest_pep = pep_datasets[0]
            
            _logger.info(f"Found latest PEP dataset: {latest_pep.get('title')} (updated: {latest_pep.get('updated')})")
            
            # Get the resource URL
            resources = latest_pep.get('resources', {})
            
            # Try different resource types in order of preference
            resource_types = ['targets_simple', 'targets', 'entities']
            for resource_type in resource_types:
                if resource_type in resources:
                    target_resource = resources[resource_type]
                    path = target_resource.get('url')
                    if path:
                        full_url = urljoin("https://data.opensanctions.org", path)
                        _logger.info(f"Discovered URL: {full_url}")
                        return full_url
            
            # Fallback to default URL
            _logger.warning("No suitable resource found in catalog, using default URL")
            return "https://data.opensanctions.org/datasets/latest/peps/targets.simple.csv"
            
        except Exception as e:
            _logger.error(f"Error in discover_latest_csv_url: {str(e)}")
            _logger.error(traceback.format_exc())
            return "https://data.opensanctions.org/datasets/latest/peps/targets.simple.csv"
    
    def fetch_latest_opensanctions_csv(self, source_record):
        """
        Fetch the latest OpenSanctions CSV by first discovering the current URL
        
        Args:
            source_record: PEP source record to use and update
            
        Returns:
            dict: Result of the fetch operation
        """
        if not source_record:
            return {
                'status': 'error',
                'message': "Source record is required"
            }
            
        if not source_record.is_opensanctions:
            _logger.warning("Source is not configured as OpenSanctions, using standard fetch")
            return self.fetch_csv_file(source_record)
            
        # Try to discover the latest URL
        latest_url = self.discover_latest_csv_url()
        
        if not latest_url:
            _logger.warning("Failed to discover latest CSV URL, trying alternative discovery method")
            # Try alternative discovery method
            latest_url = self.discover_latest_csv_url_alternative()
            
            if not latest_url:
                _logger.warning("All discovery methods failed, falling back to configured URL")
                return self.fetch_csv_file(source_record)
            
        # Parse the URL to get base and path
        parsed = urlparse(latest_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        csv_path = parsed.path
        if parsed.query:
            csv_path += '?' + parsed.query
            
        # Store original values to restore if needed
        original_base = source_record.base_url
        original_path = source_record.csv_path
        
        _logger.info(f"Updating source with new URL - Base: {base_url}, Path: {csv_path}")
        
        try:
            # Update with discovered URL
            source_record.write({
                'base_url': base_url,
                'csv_path': csv_path
            })
            
            # Fetch using the updated URL
            result = self.fetch_csv_file(source_record)
            
            # If successful, keep the new URL
            if result.get('status') == 'success':
                _logger.info(f"Successfully updated OpenSanctions URL to: {latest_url}")
                return result
                
            # Otherwise restore original URL and try again
            _logger.warning("Failed to fetch with discovered URL, restoring original and retrying")
            source_record.write({
                'base_url': original_base,
                'csv_path': original_path
            })
            return self.fetch_csv_file(source_record)
            
        except Exception as e:
            # Restore original URL in case of error
            source_record.write({
                'base_url': original_base,
                'csv_path': original_path
            })
            _logger.error(f"Error fetching latest CSV: {str(e)}")
            _logger.error(traceback.format_exc())
            return {'status': 'error', 'message': f"Error fetching latest CSV: {str(e)}"}
    
    def discover_latest_csv_url_alternative(self):
        """
        Alternative method to discover the latest OpenSanctions CSV URL
        This is a fallback in case the primary method fails
        
        Returns:
            str: Complete URL to the latest CSV, or None if discovery fails
        """
        try:
            # Try to fetch the datasets page to find the latest version
            datasets_url = "https://data.opensanctions.org/datasets/latest/index.json"
            _logger.info(f"Using alternative discovery method with: {datasets_url}")
            
            headers = {
                'User-Agent': 'Odoo/Compliance Module'
            }
            
            response = requests.get(datasets_url, headers=headers, timeout=self.timeout)
            
            if response.status_code != 200:
                _logger.error(f"Failed to fetch latest index: {response.status_code}")
                # Hardcoded fallback to a known format
                return "https://data.opensanctions.org/datasets/latest/peps/targets.simple.csv"
            
            data = response.json()
            
            # Look for the PEP dataset in the collections
            collections = data.get('collections', [])
            for collection in collections:
                if collection.get('name') == 'peps':
                    # Found PEPs collection, now look for the targets.simple resource
                    resources = collection.get('resources', [])
                    for resource in resources:
                        if resource.get('name') == 'targets.simple':
                            path = resource.get('url')
                            if path:
                                full_url = urljoin("https://data.opensanctions.org", path)
                                _logger.info(f"Found alternative URL: {full_url}")
                                return full_url
            
            # If we couldn't find it in the JSON, use a hardcoded fallback path
            fallback_url = "https://data.opensanctions.org/datasets/latest/peps/targets.simple.csv"
            _logger.info(f"Using fallback URL: {fallback_url}")
            return fallback_url
            
        except Exception as e:
            _logger.error(f"Error in alternative discovery: {str(e)}")
            _logger.error(traceback.format_exc())
            # Hardcoded fallback to a known format
            return "https://data.opensanctions.org/datasets/latest/peps/targets.simple.csv"
    
    def get_api_key(self, source_record=None):
        """
        Get API key from source record or config parameter
        
        Args:
            source_record: Optional PEP source record
            
        Returns:
            str: API key if available, None otherwise
        """
        # Try to get API key from source record
        if source_record and hasattr(source_record, 'api_key') and source_record.api_key:
            return source_record.api_key
            
        # Fallback to system parameter
        return self.env['ir.config_parameter'].sudo().get_param(
            'compliance_management.opensanctions_api_key', False)
    
    def fetch_csv_file(self, source_record=None):
        """
        Fetch CSV file from a PEP source and save to the proper directory
        
        Args:
            source_record: PEP source record, required for this function
            
        Returns:
            dict: Dictionary with file information
        """
        if not source_record:
            return {
                'status': 'error',
                'message': "Source record is required for CSV fetch"
            }
        
        try:
            # Check source configuration
            if not source_record.domain and not source_record.base_url:
                return {
                    'status': 'error',
                    'message': "Source requires either domain or base_url to be configured"
                }
                
            # Force correct URL for OpenSanctions sources
            if source_record.is_opensanctions:
                # Always update the base_url to the correct value for OpenSanctions
                if source_record.base_url != 'https://data.opensanctions.org':
                    _logger.info(f"Updating base URL from {source_record.base_url} to https://data.opensanctions.org")
                    source_record.write({
                        'base_url': 'https://data.opensanctions.org'
                    })
                
                # For OpenSanctions sources, try using the dynamic discovery
                latest_url = self.discover_latest_csv_url()
                if latest_url:
                    # Parse the discovered URL
                    parsed = urlparse(latest_url)
                    source_record.write({
                        'base_url': f"{parsed.scheme}://{parsed.netloc}",
                        'csv_path': parsed.path
                    })
            
            # Get path to CSV file
            csv_path = source_record.csv_path or '/datasets/latest/peps/targets.simple.csv'
            
            # Build the complete URL
            base_url = source_record.base_url or self.base_url
            url = urljoin(base_url, csv_path)
            
            _logger.info(f"Fetching CSV file from: {url} for source: {source_record.name}")
            
            # Make request with proper headers
            headers = {
                'User-Agent': 'Odoo/Compliance Module'
            }
            
            # Add custom headers if configured
            if hasattr(source_record, 'request_headers') and source_record.request_headers:
                try:
                    custom_headers = json.loads(source_record.request_headers)
                    if isinstance(custom_headers, dict):
                        headers.update(custom_headers)
                except Exception as e:
                    _logger.warning(f"Could not parse custom headers: {str(e)}")
            
            # Make the request with a streaming response to handle large files
            with requests.get(url, headers=headers, timeout=self.timeout, stream=True) as response:
                # Check response
                if response.status_code != 200:
                    _logger.error(f"Failed to fetch CSV file: {response.status_code}")
                    
                    if source_record.is_opensanctions:
                        # Try fallback URL for OpenSanctions
                        fallback_url = "https://data.opensanctions.org/datasets/latest/peps/targets.simple.csv"
                        _logger.info(f"Trying fallback URL: {fallback_url}")
                        
                        with requests.get(fallback_url, headers=headers, timeout=self.timeout, stream=True) as fallback_response:
                            if fallback_response.status_code != 200:
                                return {
                                    'status': 'error',
                                    'message': f"Failed to fetch CSV file from fallback URL (status: {fallback_response.status_code})"
                                }
                            response = fallback_response
                            url = fallback_url
                            
                            # Update source with working URL
                            parsed = urlparse(fallback_url)
                            source_record.write({
                                'base_url': f"{parsed.scheme}://{parsed.netloc}",
                                'csv_path': parsed.path
                            })
                    else:
                        return {
                            'status': 'error',
                            'message': f"Failed to fetch CSV file (status: {response.status_code})"
                        }
                
                # Generate a filename based on source and date
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                source_name = source_record.name.replace(' ', '_').lower() if source_record.name else 'unknown'
                filename = f"{source_name}_{timestamp}.csv"
                
                # Save to the storage directory
                file_path = os.path.join(self.storage_dir, filename)
                
                # Ensure directory exists
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
                # Write file in chunks to handle large files
                try:
                    with open(file_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                except Exception as e:
                    _logger.error(f"Error writing to file {file_path}: {str(e)}")
                    _logger.error(traceback.format_exc())
                    return {
                        'status': 'error',
                        'message': f"Failed to save CSV file: {str(e)}"
                    }
                
                _logger.info(f"Successfully saved CSV file to {file_path}")
                
                # Update source record
                source_record.write({
                    'last_update': datetime.now()
                })
                
                # Return file info
                return {
                    'status': 'success',
                    'path': file_path,
                    'url': url,
                    'type': 'csv',
                    'source': source_record.id,
                    'source_name': source_record.name,
                    'timestamp': datetime.now()
                }
                
        except Exception as e:
            _logger.error(f"Error fetching CSV file: {str(e)}")
            _logger.error(traceback.format_exc())
            return {
                'status': 'error',
                'message': f"Error fetching CSV file: {str(e)}"
            }
    
    def query_api(self, query=None, entity_type=None, limit=100, source_record=None):
        """
        Query a sanctions API based on the source configuration
        
        Args:
            query: Optional search query
            entity_type: Entity type to search for (override from source config)
            limit: Maximum number of results (default: 100)
            source_record: PEP source record, required for this function
            
        Returns:
            dict: API response data
        """
        if not source_record:
            return {
                'status': 'error',
                'message': "Source record is required for API query"
            }
            
        api_key = self.get_api_key(source_record)
        
        if not api_key:
            _logger.warning(f"No API key found for source: {source_record.name}")
            return {
                'status': 'error',
                'message': "API key is required for API access"
            }
            
        try:
            # Get API settings from source record
            api_url = source_record.api_url or self.api_endpoint
            api_endpoint = source_record.api_endpoint or '/search/default'
            
            # Allow entity_type to be overridden from source config
            if not entity_type and hasattr(source_record, 'default_entity_type') and source_record.default_entity_type:
                entity_type = source_record.default_entity_type
            
            # Default to 'person' if not specified
            if not entity_type:
                entity_type = 'person'
                
            # Prepare request parameters
            params = {
                'limit': limit
            }
            
            # Add schema parameter if entity_type is provided
            if entity_type:
                params['schema'] = entity_type
                
            if query:
                params['q'] = query
                
            # Add custom parameters if configured
            if hasattr(source_record, 'api_params') and source_record.api_params:
                try:
                    custom_params = json.loads(source_record.api_params)
                    if isinstance(custom_params, dict):
                        params.update(custom_params)
                except Exception as e:
                    _logger.warning(f"Could not parse API params: {str(e)}")
                
            # Make request with proper headers
            headers = {
                'User-Agent': 'Odoo/Compliance Module'
            }
            
            # Add authorization header based on configuration
            auth_format = source_record.api_auth_format if hasattr(source_record, 'api_auth_format') else 'ApiKey {}'
            headers['Authorization'] = auth_format.format(api_key)
            
            # Add custom headers if configured
            if hasattr(source_record, 'api_headers') and source_record.api_headers:
                try:
                    custom_headers = json.loads(source_record.api_headers)
                    if isinstance(custom_headers, dict):
                        headers.update(custom_headers)
                except Exception as e:
                    _logger.warning(f"Could not parse API headers: {str(e)}")
            
            # Build the complete URL
            url = urljoin(api_url, api_endpoint)
            _logger.info(f"Querying API: {url} for source: {source_record.name}")
            
            response = requests.get(url, headers=headers, params=params, timeout=self.timeout)
            
            # Check response
            if response.status_code != 200:
                _logger.error(f"API request failed: {response.status_code}")
                return {
                    'status': 'error',
                    'message': f"API request failed (status code: {response.status_code}, response: {response.text[:200]})"
                }
                
            # Parse response
            data = response.json()
            
            # Log some info about the results
            if isinstance(data, dict):
                results_count = 0
                if 'results' in data and isinstance(data['results'], list):
                    results_count = len(data['results'])
                elif 'entities' in data and isinstance(data['entities'], list):
                    results_count = len(data['entities'])
                
                _logger.info(f"API returned {results_count} results")
            
            # Update source record
            source_record.write({
                'last_update': datetime.now()
            })
                
            return {
                'status': 'success',
                'data': data,
                'source': source_record.id,
                'source_name': source_record.name,
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            _logger.error(f"Error querying API: {str(e)}")
            _logger.error(traceback.format_exc())
            return {
                'status': 'error',
                'message': f"Error querying API: {str(e)}"
            }
