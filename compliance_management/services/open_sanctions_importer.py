import logging
import csv
import json
import os
from datetime import datetime
import traceback

_logger = logging.getLogger(__name__)

class OpenSanctionsImporter:
    """
    Importer for OpenSanctions data with batch processing
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
        
    def process_csv_file(self, file_path, source=None):
        """
        Process CSV file with batch processing to avoid memory issues
        
        Args:
            file_path: Path to CSV file
            source: PEP source record for custom field mapping
            
        Returns:
            dict: Processing results
        """
        if not os.path.exists(file_path):
            return {
                'status': 'error',
                'message': f"File not found: {file_path}"
            }
            
        try:
            total_records_created = 0
            total_records_updated = 0
            total_records_errored = 0
            
            source_name = source.name if source else 'Unknown'
            
            # First, count total records for progress reporting
            total_records = self._count_csv_lines(file_path)
            _logger.info(f"Starting import of {total_records} records from {file_path}")
            
            # Process in batches
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                batch = []
                processed_count = 0
                
                for row in reader:
                    try:
                        # Map CSV data to PEP model
                        pep_data = self.map_csv_to_pep(row, source)
                        
                        # Ensure we have a unique identifier
                        if not pep_data.get('unique_identifier'):
                            # Generate one from name or other fields
                            name_parts = []
                            if pep_data.get('first_name'):
                                name_parts.append(pep_data['first_name'])
                            if pep_data.get('surname'):
                                name_parts.append(pep_data['surname'])
                                
                            if name_parts:
                                source_prefix = source.name if source else 'source'
                                pep_data['unique_identifier'] = f"{source_prefix}_{'-'.join(name_parts)}"
                            else:
                                # Fallback to a random unique ID
                                import uuid
                                pep_data['unique_identifier'] = f"csv_import_{uuid.uuid4()}"
                        
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
                            progress = (processed_count / total_records) * 100 if total_records > 0 else 0
                            _logger.info(f"Progress: {progress:.1f}% ({processed_count}/{total_records})")
                            
                            # Clear batch
                            batch = []
                            
                            # Commit transaction after each batch if not in a queue job
                            self.env.cr.commit()
                        
                    except Exception as e:
                        _logger.error(f"Error processing row: {str(e)}")
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
            _logger.info(f"Completed processing {file_path}: {total_records_created} created, {total_records_updated} updated, {total_records_errored} errors")
            
            # Keep the CSV file for reference
            # You can uncomment this if you want to delete the file after processing
            #try:
            #    os.unlink(file_path)
            #except Exception as e:
            #    _logger.warning(f"Failed to delete temp file: {str(e)}")
                
            return {
                'status': 'success',
                'message': f"Processed {source_name} CSV file: {total_records_created} created, {total_records_updated} updated",
                'records_created': total_records_created,
                'records_updated': total_records_updated,
                'records_errored': total_records_errored
            }
            
        except Exception as e:
            _logger.error(f"Error processing CSV file: {str(e)}")
            _logger.error(traceback.format_exc())
            return {
                'status': 'error',
                'message': f"Error processing CSV file: {str(e)}"
            }
    
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
        
        # Get all unique identifiers in this batch
        identifiers = [record.get('unique_identifier') for record in batch if record.get('unique_identifier')]
        
        # Find existing records with these identifiers
        existing_records = {}
        if identifiers:
            existing = self.env['res.pep'].search([('unique_identifier', 'in', identifiers)])
            for record in existing:
                existing_records[record.unique_identifier] = record
        
        # Process each record in the batch
        for pep_data in batch:
            try:
                identifier = pep_data.get('unique_identifier')
                
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
                    # Skip id field for now, we'll handle it separately
                    if target_field == 'unique_identifier':
                        continue
                        
                    # Get value from the row
                    if source_field in row:
                        mapping[target_field] = row.get(source_field, '')
                
                # Handle unique identifier specially
                if 'unique_identifier' in field_map:
                    id_field = field_map['unique_identifier']
                    if id_field in row:
                        mapping['unique_identifier'] = row.get(id_field)
                    else:
                        # Generate one from name or other fields
                        name_parts = []
                        if 'first_name' in mapping:
                            name_parts.append(mapping['first_name'])
                        if 'surname' in mapping:
                            name_parts.append(mapping['surname'])
                            
                        if name_parts:
                            mapping['unique_identifier'] = f"{source.name}_{'-'.join(name_parts)}"
                        else:
                            # Fallback to a random unique ID
                            import uuid
                            mapping['unique_identifier'] = f"{source.name}_{uuid.uuid4()}"
                            
                # Handle name field if not in mapping
                if 'name' not in mapping and 'first_name' in mapping and 'surname' in mapping:
                    mapping['name'] = f"{mapping['first_name']} {mapping['surname']}".strip()
                    
                return mapping
                    
            except Exception as e:
                _logger.error(f"Error processing custom field mapping: {str(e)}")
                # Fall through to default mapping
        
        # Default mapping if no custom field mapping is available
        # Generate a unique identifier
        unique_id = row.get('id') or f"opensanctions_{row.get('schema')}_{row.get('caption')}"
        source_name = source.name if source else 'unknown'
        
        # Map fields from CSV to PEP model
        mapping = {
            'unique_identifier': unique_id,
            'surname': row.get('last_name', ''),
            'first_name': row.get('first_name', ''),
            'middle_name': row.get('middle_name', ''),
            'title': row.get('title', ''),
            'aka': row.get('alias', ''),
            'sex': row.get('gender', ''),
            'date_of_birth': row.get('birth_date', ''),
            'present_position': row.get('position', ''),
            'previous_position': row.get('previous_position', ''),
            'pep_classification': row.get('classification', ''),
            'profession': row.get('occupation', ''),
            'residential_address': row.get('address', ''),
            'state_of_origin': row.get('place', ''),
            'citizenship': row.get('nationality', ''),
            'place_of_birth': row.get('birth_place', ''),
            'additional_info': row.get('notes', ''),
            'email': row.get('email', ''),
            'status': row.get('status', ''),
            'pob': row.get('birth_place', ''),
            'source': source_name,
            'import_status': 'imported',
            'last_fetch_date': datetime.now(),
        }
        
        # Handle name field (either use the provided one or combine first+last)
        if row.get('name'):
            mapping['name'] = row.get('name')
        elif mapping['first_name'] and mapping['surname']:
            mapping['name'] = f"{mapping['first_name']} {mapping['surname']}".strip()
            
        # Add creation/modification info if available
        if row.get('created_at'):
            mapping['createdon'] = row.get('created_at')
            
        if row.get('updated_at'):
            mapping['lastmodifiedon'] = row.get('updated_at')
            
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
                
            _logger.info(f"Found {len(entities)} entities in API response")
            
            # Process entities in batches
            batch = []
            processed_count = 0
            total_entities = len(entities)
            
            for entity in entities:
                try:
                    # Skip non-person entities if schema filtering is used
                    if source and source.api_entity_filter:
                        filter_field, filter_value = source.api_entity_filter.split(':')
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
            _logger.error(f"Error processing API data: {str(e)}")
            _logger.error(traceback.format_exc())
            return {
                'status': 'error',
                'message': f"Error processing API data: {str(e)}"
            }
    
    def map_api_entity_to_pep(self, entity, source=None):
        """Map API entity data to PEP record fields"""
        # Add detailed implementation here similar to map_csv_to_pep
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
                if 'unique_identifier' not in mapping:
                    # Try to use id field if available
                    if 'id' in entity:
                        mapping['unique_identifier'] = entity.get('id')
                    else:
                        # Generate from name or other fields
                        name_parts = []
                        if 'first_name' in mapping:
                            name_parts.append(mapping['first_name'])
                        if 'surname' in mapping:
                            name_parts.append(mapping['surname'])
                            
                        if name_parts:
                            mapping['unique_identifier'] = f"{source.name}_api_{'-'.join(name_parts)}"
                        else:
                            # Fallback to a random unique ID
                            import uuid
                            mapping['unique_identifier'] = f"{source.name}_api_{uuid.uuid4()}"
                
                # Handle name field if not in mapping
                if 'name' not in mapping and 'first_name' in mapping and 'surname' in mapping:
                    mapping['name'] = f"{mapping['first_name']} {mapping['surname']}".strip()
                    
                return mapping
                
            except Exception as e:
                _logger.error(f"Error processing custom API field mapping: {str(e)}")
                # Fall through to default mapping
                
        # Default mapping implementation for API entities
        properties = entity.get('properties', {})
        unique_id = entity.get('id')
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
            
        # Extract first name and surname if available
        if 'first_name' in entity:
            mapping['first_name'] = entity.get('first_name')
            
        if 'last_name' in entity:
            mapping['surname'] = entity.get('last_name')
            
        # Add other fields from properties if available
        for prop, values in properties.items():
            if prop == 'name' and values and not mapping.get('name'):
                mapping['name'] = values[0].get('value', '')
                
            if prop == 'givenName' and values and not mapping.get('first_name'):
                mapping['first_name'] = values[0].get('value', '')
                
            if prop == 'familyName' and values and not mapping.get('surname'):
                mapping['surname'] = values[0].get('value', '')
                
            if prop == 'birthDate' and values:
                mapping['date_of_birth'] = values[0].get('value', '')
                
            if prop == 'position' and values:
                mapping['present_position'] = values[0].get('value', '')
                
            if prop == 'nationality' and values:
                mapping['citizenship'] = values[0].get('value', '')
                
            if prop == 'address' and values:
                mapping['residential_address'] = values[0].get('value', '')
        
        # If we have name but no first/last name, try to split it
        if mapping.get('name') and not (mapping.get('first_name') and mapping.get('surname')):
            name_parts = mapping.get('name', '').split(' ', 1)
            if len(name_parts) >= 2:
                mapping['first_name'] = name_parts[0]
                mapping['surname'] = name_parts[1]
            elif len(name_parts) == 1:
                mapping['first_name'] = name_parts[0]
                mapping['surname'] = ''
        
        # Add all properties as additional info
        mapping['additional_info'] = json.dumps(properties, indent=2)
        
        return mapping
            
    def _get_nested_value(self, data, path):
        """Get a value from a nested dictionary using dot notation path"""
        if not path:
            return None
            
        parts = path.split('.')
        current = data
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
                
        return current