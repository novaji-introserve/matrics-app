import logging
import csv
import json
import os
import traceback
from datetime import datetime
import uuid

_logger = logging.getLogger(__name__)

class OpenSanctionsImporter:
    """
    Importer for OpenSanctions data with improved batch processing and error handling
    """
    
    def __init__(self, env):
        """
        Initialize the OpenSanctions importer
        
        Args:
            env: Odoo environment
        """
        self.env = env
        
        # Get batch size from system parameters
        params = self.env['ir.config_parameter'].sudo()
        self.batch_size = int(params.get_param('compliance_management.import_batch_size', '500'))
        self.max_records_per_job = int(params.get_param('compliance_management.max_records_per_job', '5000'))

    def process_csv_file(self, file_path, source=None, start_index=0, max_records=None):
        """
        Process CSV file with batch processing to avoid memory issues
        
        Args:
            file_path: Path to CSV file
            source: PEP source record for custom field mapping
            start_index: Index to start processing from (for chunking)
            max_records: Maximum number of records to process in this chunk
            
        Returns:
            dict: Processing results
        """
        if not os.path.exists(file_path):
            error_msg = f"File not found: {file_path}"
            _logger.error(error_msg)
            return {
                'status': 'error',
                'message': error_msg
            }
            
        try:
            total_records_created = 0
            total_records_updated = 0
            total_records_errored = 0
            
            source_name = source.name if source and hasattr(source, 'name') else 'Unknown'
            
            # First, count total records for progress reporting
            total_records_in_file = self._count_csv_lines(file_path)
            
            # If max_records is not set, use the default or all records
            if not max_records:
                max_records = self.max_records_per_job
                
            # Calculate end index
            end_index = min(start_index + max_records, total_records_in_file)
            
            _logger.info(f"Starting import of records {start_index}-{end_index} of {total_records_in_file} from {file_path}")
            
            # Detect CSV delimiter
            delimiter = ','
            if source and hasattr(source, 'csv_delimiter') and source.csv_delimiter:
                delimiter = source.csv_delimiter
            
            # Process in batches
            try:
                # Try UTF-8 first
                with open(file_path, 'r', encoding='utf-8') as f:
                    # Detect CSV format
                    sample = f.read(4096)
                    f.seek(0)
                    
                    # Check if the file has a BOM marker
                    has_bom = sample.startswith('\ufeff')
                    
                    # If the file has a BOM, reopen with utf-8-sig
                    if has_bom:
                        f.close()
                        with open(file_path, 'r', encoding='utf-8-sig') as f:
                            reader = csv.DictReader(f, delimiter=delimiter)
                            result = self._process_csv_reader(reader, source, total_records_in_file, start_index, end_index)
                    else:
                        reader = csv.DictReader(f, delimiter=delimiter)
                        result = self._process_csv_reader(reader, source, total_records_in_file, start_index, end_index)
                        
                    total_records_created = result['created']
                    total_records_updated = result['updated']
                    total_records_errored = result['errors']
                        
            except UnicodeDecodeError:
                # If UTF-8 fails, try Latin-1
                _logger.warning(f"UTF-8 decode failed, trying Latin-1 for {file_path}")
                with open(file_path, 'r', encoding='latin-1') as f:
                    reader = csv.DictReader(f, delimiter=delimiter)
                    result = self._process_csv_reader(reader, source, total_records_in_file, start_index, end_index)
                    
                    total_records_created = result['created']
                    total_records_updated = result['updated']
                    total_records_errored = result['errors']
            
            # Log final results
            _logger.info(f"Completed processing chunk {start_index}-{end_index} from {file_path}: {total_records_created} created, {total_records_updated} updated, {total_records_errored} errors")
                
            # Check if there are more records to process
            more_records = end_index < total_records_in_file
            
            return {
                'status': 'success',
                'message': f"Processed {source_name} CSV file chunk: {total_records_created} created, {total_records_updated} updated",
                'records_created': total_records_created,
                'records_updated': total_records_updated,
                'records_errored': total_records_errored,
                'more_records': more_records,
                'next_index': end_index,
                'total_records': total_records_in_file
            }
            
        except Exception as e:
            error_msg = f"Error processing CSV file: {str(e)}"
            _logger.error(error_msg)
            _logger.error(traceback.format_exc())
            return {
                'status': 'error',
                'message': error_msg
            }
    
    def _process_csv_reader(self, reader, source, total_records, start_index, end_index):
        """
        Process a CSV reader object with batch processing
        
        Args:
            reader: CSV DictReader object
            source: PEP source record
            total_records: Total number of records in file
            start_index: Index to start processing from
            end_index: Index to end processing at
            
        Returns:
            dict: Processing results
        """
        batch = []
        processed_count = 0
        total_records_created = 0
        total_records_updated = 0
        total_records_errored = 0
        
        # Pre-check if reader has data
        try:
            # Check CSV headers
            field_names = reader.fieldnames
            if not field_names:
                _logger.error("CSV file has no headers")
                return {
                    'created': 0, 
                    'updated': 0, 
                    'errors': 1
                }
                
            _logger.info(f"CSV headers: {field_names}")
        except Exception as e:
            _logger.error(f"Error reading CSV headers: {str(e)}")
            return {
                'created': 0, 
                'updated': 0, 
                'errors': 1
            }
            
        # Skip rows until we reach the start index
        for _ in range(start_index):
            try:
                next(reader)
            except StopIteration:
                _logger.error(f"CSV file has fewer rows than expected (tried to skip to row {start_index})")
                return {
                    'created': 0,
                    'updated': 0,
                    'errors': 1
                }
        
        # Process rows from start_index to end_index
        for current_index in range(start_index, end_index):
            try:
                try:
                    row = next(reader)
                except StopIteration:
                    _logger.info(f"Reached end of CSV file at row {current_index}")
                    break
                
                # Skip empty rows
                if not any(row.values()):
                    continue
                    
                # Map CSV data to PEP model
                pep_data = self.map_csv_to_pep(row, source)
                
                # Skip if essential data is missing
                if not pep_data.get('unique_identifier'):
                    # Generate one from name or other fields
                    name_parts = []
                    if pep_data.get('first_name'):
                        name_parts.append(pep_data['first_name'])
                    if pep_data.get('surname'):
                        name_parts.append(pep_data['surname'])
                        
                    if name_parts:
                        source_prefix = source.name if source and hasattr(source, 'name') else 'source'
                        source_prefix = source_prefix.replace(' ', '_').lower()
                        pep_data['unique_identifier'] = f"{source_prefix}_{'-'.join(name_parts)}".replace(' ', '_')
                    else:
                        # Fallback to a random unique ID
                        pep_data['unique_identifier'] = f"csv_import_{uuid.uuid4()}"
                
                # Handle HTML fields - sanitize content to prevent HTML errors
                self._sanitize_html_fields(pep_data)
                
                # Add to batch
                batch.append(pep_data)
                processed_count += 1
                
                # Process batch when it reaches batch_size
                if len(batch) >= self.batch_size:
                    batch_result = self._process_batch(batch, source)
                    total_records_created += batch_result['created']
                    total_records_updated += batch_result['updated']
                    total_records_errored += batch_result['errors']
                    
                    # Log progress
                    progress = ((current_index - start_index + 1) / (end_index - start_index)) * 100 if (end_index - start_index) > 0 else 0
                    _logger.info(f"Progress: {progress:.1f}% ({current_index - start_index + 1}/{end_index - start_index}) - Total: {current_index + 1}/{total_records}")
                    
                    # Clear batch
                    batch = []
                    
                    # Commit transaction after each batch to release memory
                    self.env.cr.commit()
                
            except Exception as e:
                _logger.error(f"Error processing row {current_index}: {str(e)}")
                total_records_errored += 1
        
        # Process any remaining records in the last batch
        if batch:
            batch_result = self._process_batch(batch, source)
            total_records_created += batch_result['created']
            total_records_updated += batch_result['updated']
            total_records_errored += batch_result['errors']
            
            # Final commit
            self.env.cr.commit()
            
        return {
            'created': total_records_created,
            'updated': total_records_updated,
            'errors': total_records_errored
        }

    def _sanitize_html_fields(self, data):
        """
        Sanitize HTML content in record data to prevent sanitization errors
        
        Args:
            data: Record data dictionary
        """
        html_fields = ['additional_info', 'narration', 'press_report']
        
        for field in html_fields:
            if field in data and data[field]:
                # Simple HTML sanitization - convert problematic characters
                if isinstance(data[field], str):
                    # Replace known problematic characters or sequences
                    data[field] = data[field].replace('<', '&lt;').replace('>', '&gt;')
                    # Escape quotes
                    data[field] = data[field].replace('"', '&quot;').replace("'", '&#39;')
    
    def _process_batch(self, batch, source):
        """
        Process a batch of records
        
        Args:
            batch: List of mapped PEP data dictionaries
            source: PEP source record
            
        Returns:
            dict: Batch processing results
        """
        created = 0
        updated = 0
        errors = 0
        
        # Early exit if batch is empty
        if not batch:
            return {'created': 0, 'updated': 0, 'errors': 0}
            
        # Get all unique identifiers in this batch
        identifiers = [record.get('unique_identifier') for record in batch if record.get('unique_identifier')]
        
        # Find existing records with these identifiers
        existing_records = {}
        if identifiers:
            try:
                existing = self.env['res.pep'].search([('unique_identifier', 'in', identifiers)])
                for record in existing:
                    existing_records[record.unique_identifier] = record
            except Exception as e:
                _logger.error(f"Error searching for existing records: {str(e)}")
        
        # Process each record in the batch
        for pep_data in batch:
            try:
                identifier = pep_data.get('unique_identifier')
                
                if not identifier:
                    _logger.warning("Record missing unique identifier, skipping")
                    errors += 1
                    continue
                    
                # Ensure we have a name field
                if 'name' not in pep_data and 'first_name' in pep_data and 'surname' in pep_data:
                    pep_data['name'] = f"{pep_data['first_name']} {pep_data['surname']}".strip()
                
                # Validate required fields
                if not pep_data.get('name'):
                    # Check if we can construct name from first_name and surname
                    if pep_data.get('first_name') or pep_data.get('surname'):
                        first = pep_data.get('first_name', '')
                        last = pep_data.get('surname', '')
                        pep_data['name'] = f"{first} {last}".strip()
                    else:
                        pep_data['name'] = f"Unknown-{identifier}"
                
                if not pep_data.get('first_name'):
                    pep_data['first_name'] = pep_data.get('name', '').split(' ')[0] if pep_data.get('name') else "Unknown"
                    
                if not pep_data.get('surname'):
                    name_parts = pep_data.get('name', '').split(' ')
                    pep_data['surname'] = name_parts[-1] if len(name_parts) > 1 else "Unknown"
                
                if identifier in existing_records:
                    # Update existing record
                    existing_records[identifier].write(pep_data)
                    updated += 1
                else:
                    # Create new record
                    self.env['res.pep'].create(pep_data)
                    created += 1
            except Exception as e:
                _logger.error(f"Error processing record: {str(e)}")
                _logger.error(traceback.format_exc())
                errors += 1
        
        return {
            'created': created,
            'updated': updated,
            'errors': errors
        }

    def _count_csv_lines(self, file_path):
        """Count the number of lines in a CSV file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Skip header
                next(f)
                count = sum(1 for _ in f)
            return count
        except UnicodeDecodeError:
            # If UTF-8 fails, try Latin-1
            try:
                with open(file_path, 'r', encoding='latin-1') as f:
                    # Skip header
                    next(f)
                    count = sum(1 for _ in f)
                return count
            except Exception as e:
                _logger.warning(f"Error counting CSV lines with Latin-1: {str(e)}")
                return 0
        except Exception as e:
            _logger.warning(f"Error counting CSV lines: {str(e)}")
            return 0
    
    def map_csv_to_pep(self, row, source=None):
        """
        Map CSV row to PEP record fields
        
        Args:
            row: CSV row data
            source: PEP source record for custom field mapping
            
        Returns:
            dict: Mapped PEP data
        """
        # Debug log for data mapping
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(f"Mapping row: {row}")
            
        # Check if source has a custom field mapping
        if source and hasattr(source, 'field_mapping') and source.field_mapping:
            try:
                # Parse custom field mapping
                field_map = json.loads(source.field_mapping)
                
                # Create mapping dictionary based on custom field mapping
                mapping = {
                    'import_status': 'imported',
                    'last_fetch_date': datetime.now(),
                    'source': source.name if hasattr(source, 'name') else 'Unknown'
                }
                
                # Apply field mapping
                for target_field, source_field in field_map.items():
                    # Skip id field for now, we'll handle it separately
                    if target_field == 'unique_identifier':
                        continue
                        
                    # Get value from the row
                    if source_field in row:
                        mapping[target_field] = row.get(source_field, '')
                
                # Handle unique identifier specially
                if 'unique_identifier' in field_map:
                    id_field = field_map['unique_identifier']
                    if id_field in row and row.get(id_field):
                        mapping['unique_identifier'] = row.get(id_field)
                    else:
                        # Generate one from name or other fields
                        name_parts = []
                        if 'first_name' in mapping and mapping['first_name']:
                            name_parts.append(mapping['first_name'])
                        if 'surname' in mapping and mapping['surname']:
                            name_parts.append(mapping['surname'])
                            
                        if name_parts:
                            source_prefix = source.name.replace(' ', '_').lower() if source and hasattr(source, 'name') else 'src'
                            mapping['unique_identifier'] = f"{source_prefix}_{'-'.join(name_parts)}".replace(' ', '_')
                        else:
                            # Fallback to a random unique ID
                            mapping['unique_identifier'] = f"{source_prefix}_{uuid.uuid4()}"
                            
                # Handle name field if not in mapping
                if 'name' not in mapping and 'first_name' in mapping and 'surname' in mapping:
                    mapping['name'] = f"{mapping.get('first_name', '')} {mapping.get('surname', '')}".strip()
                
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug(f"Mapped using custom field mapping: {mapping}")
                    
                return mapping
                    
            except Exception as e:
                _logger.error(f"Error processing custom field mapping: {str(e)}")
                if hasattr(source, 'field_mapping'):
                    _logger.error(f"Field mapping: {source.field_mapping}")
                # Fall through to default mapping
        
        # Default mapping optimized for OpenSanctions
        unique_id = None
        
        # Try to get a unique ID from various fields
        if 'id' in row and row['id']:
            unique_id = row['id']
        elif 'caption' in row and row['caption']:
            unique_id = f"opensanctions_{row.get('schema', 'entity')}_{row['caption']}".replace(' ', '_')
            
        # If still no ID, create one from name parts
        if not unique_id:
            name_parts = []
            if 'first_name' in row and row['first_name']:
                name_parts.append(row['first_name'])
            elif 'given_name' in row and row['given_name']:
                name_parts.append(row['given_name']) 
                
            if 'last_name' in row and row['last_name']:
                name_parts.append(row['last_name'])
            elif 'family_name' in row and row['family_name']:
                name_parts.append(row['family_name'])
                
            if name_parts:
                source_name = source.name.replace(' ', '_').lower() if source and hasattr(source, 'name') else 'opensanctions'
                unique_id = f"{source_name}_{'_'.join(name_parts)}".replace(' ', '_')
            else:
                # Last resort - random ID
                unique_id = f"opensanctions_{uuid.uuid4()}"
        
        # Get source name
        source_name = source.name if source and hasattr(source, 'name') else 'OpenSanctions'
        
        # Map fields from CSV to PEP model
        mapping = {
            'unique_identifier': unique_id,
            'source': source_name,
            'import_status': 'imported',
            'last_fetch_date': datetime.now(),
        }
        
        # Map name from OpenSanctions CSV structure
        if 'name' in row and row['name']:
            mapping['name'] = row['name']
            
        # Handle schema field for classification
        if 'schema' in row and row['schema']:
            mapping['pep_classification'] = row['schema']
            
        # Handle aliases
        if 'aliases' in row and row['aliases']:
            mapping['aka'] = row['aliases']
            
        # Handle dates
        if 'birth_date' in row and row['birth_date']:
            mapping['date_of_birth'] = row['birth_date']
            
        # Handle countries for citizenship
        if 'countries' in row and row['countries']:
            mapping['citizenship'] = row['countries']
        
        # Handle addresses
        if 'addresses' in row and row['addresses']:
            mapping['residential_address'] = row['addresses']
            
        # If we have name but no first/last name, try to split it
        if mapping.get('name') and not (mapping.get('first_name') and mapping.get('surname')):
            name_parts = mapping.get('name', '').split(' ', 1)
            if len(name_parts) >= 2:
                mapping['first_name'] = name_parts[0]
                mapping['surname'] = name_parts[1]
            elif len(name_parts) == 1:
                mapping['first_name'] = name_parts[0]
                mapping['surname'] = name_parts[0]  # Use same value as fallback
        
        # Add all available info as additional_info
        additional_info = []
        for key, value in row.items():
            if key not in ['id', 'name', 'schema'] and value:
                additional_info.append(f"{key}: {value}")
                
        if additional_info:
            mapping['additional_info'] = "\n".join(additional_info)
            
        # Remove any empty values
        mapping = {k: v for k, v in mapping.items() if v}
        
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(f"Mapped using default mapping: {mapping}")
            
        return mapping

    def process_api_results(self, api_data, source=None):
        """
        Process API results with batch processing
        
        Args:
            api_data: API response data
            source: PEP source record for configuration
            
        Returns:
            dict: Processing results
        """
        if not api_data or 'status' in api_data and api_data['status'] == 'error':
            return api_data
            
        try:
            total_records_created = 0
            total_records_updated = 0
            total_records_errored = 0
            
            source_name = source.name if source else 'Unknown API'
            
            # Extract entities from results based on source configuration
            results = api_data.get('data', {})
            
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(f"API results structure: {list(results.keys()) if isinstance(results, dict) else 'not a dict'}")
            
            # Get the entities path from source configuration or use default
            entities_path = 'results'
            if source and source.api_results_path:
                entities_path = source.api_results_path
                
            # Extract entities using the configured path
            entities = []
            if entities_path:
                path_parts = entities_path.split('.')
                current = results
                for part in path_parts:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        current = []
                        break
                
                if isinstance(current, list):
                    entities = current
            
            # If no entities found and results is a list, use that directly
            if not entities and isinstance(results, list):
                entities = results
                
            # If still no entities, look for common keys in results
            if not entities and isinstance(results, dict):
                for key in ['entities', 'results', 'data', 'items']:
                    if key in results and isinstance(results[key], list):
                        entities = results[key]
                        break
                        
            # Last resort - try to use the entire results as an entity if it has expected fields
            if not entities and isinstance(results, dict):
                if any(key in results for key in ['id', 'name', 'schema']):
                    entities = [results]
                
            _logger.info(f"Found {len(entities)} entities in API response")
            
            if _logger.isEnabledFor(logging.DEBUG) and entities:
                _logger.debug(f"Sample entity structure: {list(entities[0].keys()) if isinstance(entities[0], dict) else 'not a dict'}")
            
            # Process entities in batches
            batch = []
            processed_count = 0
            total_entities = len(entities)
            
            for entity in entities:
                try:
                    # Skip non-person entities if schema filtering is used
                    if source and source.api_entity_filter:
                        filter_parts = source.api_entity_filter.split(':')
                        if len(filter_parts) == 2:
                            filter_field, filter_value = filter_parts
                            entity_type = self._get_nested_value(entity, filter_field)
                            if entity_type != filter_value:
                                continue
                    
                    # Map API entity to PEP model using source-specific mapping
                    pep_data = self.map_api_entity_to_pep(entity, source)
                    
                    # Ensure we have a unique identifier
                    if not pep_data.get('unique_identifier'):
                        # Generate one from entity data
                        import uuid
                        pep_data['unique_identifier'] = f"api_import_{uuid.uuid4()}"
                    
                    # Add to batch
                    batch.append(pep_data)
                    processed_count += 1
                    
                    # Process batch when it reaches batch_size
                    if len(batch) >= self.batch_size:
                        batch_result = self._process_batch(batch, source)
                        total_records_created += batch_result['created']
                        total_records_updated += batch_result['updated']
                        total_records_errored += batch_result['errors']
                        
                        # Log progress
                        progress = (processed_count / total_entities) * 100 if total_entities > 0 else 0
                        _logger.info(f"API progress: {progress:.1f}% ({processed_count}/{total_entities})")
                        
                        # Clear batch
                        batch = []
                        
                        # Commit transaction after each batch if not in a queue job
                        self.env.cr.commit()
                    
                except Exception as e:
                    _logger.error(f"Error processing entity: {str(e)}")
                    _logger.error(traceback.format_exc())
                    total_records_errored += 1
            
            # Process any remaining records in the last batch
            if batch:
                batch_result = self._process_batch(batch, source)
                total_records_created += batch_result['created']
                total_records_updated += batch_result['updated']
                total_records_errored += batch_result['errors']
                
                # Final commit
                self.env.cr.commit()
            
            # Log final results
            _logger.info(f"Completed API processing: {total_records_created} created, {total_records_updated} updated, {total_records_errored} errors")
            
            return {
                'status': 'success',
                'message': f"Processed {source_name} API data: {total_records_created} created, {total_records_updated} updated",
                'records_created': total_records_created,
                'records_updated': total_records_updated,
                'records_errored': total_records_errored
            }
            
        except Exception as e:
            error_msg = f"Error processing API data: {str(e)}"
            _logger.error(error_msg)
            _logger.error(traceback.format_exc())
            return {
                'status': 'error',
                'message': error_msg
            }
    
    def map_api_entity_to_pep(self, entity, source=None):
        """
        Map API entity data to PEP record fields
        
        Args:
            entity: API entity data
            source: PEP source record for custom field mapping
            
        Returns:
            dict: Mapped PEP data
        """
        # Debug log for data mapping
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(f"Mapping API entity: {entity}")
            
        # Check if source has a custom field mapping
        if source and source.field_mapping:
            try:
                # Parse custom field mapping
                field_map = json.loads(source.field_mapping)
                
                # Create mapping dictionary based on custom field mapping
                mapping = {
                    'import_status': 'imported',
                    'last_fetch_date': datetime.now(),
                    'source': source.name
                }
                
                # Apply field mapping
                for target_field, source_field in field_map.items():
                    # Handle dot notation for nested fields
                    if '.' in source_field:
                        value = self._get_nested_value(entity, source_field)
                        if value is not None:
                            mapping[target_field] = value
                    elif source_field in entity:
                        mapping[target_field] = entity.get(source_field)
                
                # Handle unique identifier specially
                if 'unique_identifier' not in mapping or not mapping['unique_identifier']:
                    # Try to use id field if available
                    if 'id' in entity and entity['id']:
                        mapping['unique_identifier'] = entity.get('id')
                    else:
                        # Generate from name or other fields
                        name_parts = []
                        if 'first_name' in mapping and mapping['first_name']:
                            name_parts.append(mapping['first_name'])
                        if 'surname' in mapping and mapping['surname']:
                            name_parts.append(mapping['surname'])
                            
                        if name_parts:
                            source_prefix = source.name.replace(' ', '_').lower() if source and source.name else 'api'
                            mapping['unique_identifier'] = f"{source_prefix}_api_{'_'.join(name_parts)}".replace(' ', '_')
                        else:
                            # Fallback to a random unique ID
                            import uuid
                            mapping['unique_identifier'] = f"{source.name.replace(' ', '_').lower() if source and source.name else 'api'}_api_{uuid.uuid4()}"
                
                # Handle name field if not in mapping
                if 'name' not in mapping and 'first_name' in mapping and 'surname' in mapping:
                    mapping['name'] = f"{mapping['first_name']} {mapping['surname']}".strip()
                    
                return mapping
                
            except Exception as e:
                _logger.error(f"Error processing custom API field mapping: {str(e)}")
                _logger.error(traceback.format_exc())
                # Fall through to default mapping
                
        # Default mapping implementation for API entities
        properties = entity.get('properties', {})
        
        # Try to get a unique ID
        unique_id = None
        if 'id' in entity and entity['id']:
            unique_id = entity['id']
        elif 'caption' in entity and entity['caption']:
            schema = entity.get('schema', 'entity')
            unique_id = f"opensanctions_api_{schema}_{entity['caption']}".replace(' ', '_')
            
        # If still no unique ID, create one
        if not unique_id:
            # Fallback to a random ID
            import uuid
            unique_id = f"opensanctions_api_{uuid.uuid4()}"
        
        source_name = source.name if source else 'API import'
        
        # Extract standard fields
        mapping = {
            'unique_identifier': unique_id,
            'source': source_name,
            'import_status': 'imported',
            'last_fetch_date': datetime.now(),
        }
        
        # Extract names
        if 'name' in entity:
            mapping['name'] = entity.get('name')
            
        if 'caption' in entity and not mapping.get('name'):
            mapping['name'] = entity.get('caption')
            
        # Extract first name and surname if available
        if 'first_name' in entity:
            mapping['first_name'] = entity.get('first_name')
            
        if 'last_name' in entity:
            mapping['surname'] = entity.get('last_name')
            
        # Check for nested values common in OpenSanctions
        nested_fields = [
            ('schema', 'pep_classification'),
            ('properties.position.0.value', 'present_position'),
            ('properties.birthDate.0.value', 'date_of_birth'),
            ('properties.nationality.0.value', 'citizenship'),
            ('properties.gender.0.value', 'sex'),
            ('properties.alias.0.value', 'aka'),
            ('properties.title.0.value', 'title'),
            ('properties.address.0.value', 'residential_address'),
            ('properties.birthPlace.0.value', 'place_of_birth'),
            ('properties.summary.0.value', 'additional_info'),
            ('properties.notes.0.value', 'additional_info'),
            ('properties.status.0.value', 'status'),
            ('properties.publisher.0.value', 'source')
        ]
        
        for path, field in nested_fields:
            value = self._get_nested_value(entity, path)
            if value and not mapping.get(field):
                mapping[field] = value
        
        # If we have name but no first/last name, try to split it
        if mapping.get('name') and not (mapping.get('first_name') and mapping.get('surname')):
            name_parts = mapping.get('name', '').split(' ', 1)
            if len(name_parts) >= 2:
                mapping['first_name'] = name_parts[0]
                mapping['surname'] = name_parts[1]
            elif len(name_parts) == 1:
                mapping['first_name'] = name_parts[0]
                mapping['surname'] = ''
        
        # Ensure place_of_birth and pob are consistent
        if mapping.get('place_of_birth') and not mapping.get('pob'):
            mapping['pob'] = mapping['place_of_birth']
        elif mapping.get('pob') and not mapping.get('place_of_birth'):
            mapping['place_of_birth'] = mapping['pob']
        
        # Remove any empty values
        mapping = {k: v for k, v in mapping.items() if v}
        
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(f"Mapped API entity using default mapping: {mapping}")
            
        return mapping
            
    def _get_nested_value(self, data, path):
        """
        Get a value from a nested dictionary using dot notation path
        
        Args:
            data: Dictionary to extract value from
            path: Dot-notation path to the value
            
        Returns:
            object: The extracted value or None if not found
        """
        if not path or not data:
            return None
            
        parts = path.split('.')
        current = data
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                if 0 <= idx < len(current):
                    current = current[idx]
                else:
                    return None
            else:
                return None
                
        return current
