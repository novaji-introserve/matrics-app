import logging
import time
import traceback
from datetime import datetime
from odoo import fields

_logger = logging.getLogger(__name__)


class PepImporter:
    """
    Imports PEP data into the database
    """

    def __init__(self, env):
        """
        Initialize the PEP importer

        Args:
            env: Odoo environment
        """
        self.env = env

        # Get the PEP model
        self.pep_model = self.env["res.pep"]

        # Batch size for database operations
        self.batch_size = 100

        # Counters for import statistics
        self.created_count = 0
        self.updated_count = 0
        self.error_count = 0
        self.skipped_count = 0

        # Error records for logging
        self.errors = []

    # def _clean_record(self, record):
    #     """
    #     Clean a record before inserting it into the database

    #     Args:
    #         record: Record to clean

    #     Returns:
    #         dict: Cleaned record
    #     """
    #     # Create a copy of the record to avoid modifying the original
    #     record = record.copy()

    #     # Remove fields that are not in the model
    #     model_fields = self.pep_model._fields
    #     invalid_fields = []

    #     for field in list(record.keys()):
    #         if field not in model_fields:
    #             invalid_fields.append(field)
    #             del record[field]

    #     if invalid_fields:
    #         _logger.debug(
    #             f"Removed invalid fields from record: {', '.join(invalid_fields)}"
    #         )

    #     return record

    def _clean_record(self, record):
        """
        Clean a record before inserting it into the database with improved validation
        
        Args:
            record: Record to clean
            
        Returns:
            dict: Cleaned record
        """
        # Create a copy of the record to avoid modifying the original
        record = record.copy()
        
        # Remove fields that are not in the model
        model_fields = self.pep_model._fields
        invalid_fields = []
        
        for field in list(record.keys()):
            if field not in model_fields:
                invalid_fields.append(field)
                del record[field]
        
        if invalid_fields:
            _logger.debug(f"Removed invalid fields from record: {', '.join(invalid_fields)}")
        
        # Ensure all string fields are strings and not other data types
        for field, value in record.items():
            field_obj = model_fields.get(field)
            if field_obj and field_obj.type == 'char' and value is not None:
                try:
                    record[field] = str(value)
                except Exception as e:
                    _logger.warning(f"Could not convert {field} value to string: {str(e)}")
                    record[field] = None
        
        # Validate required fields
        for field_name, field_obj in model_fields.items():
            if field_obj.required and field_name not in record:
                if field_name == "name":
                    # If name is missing but we have first_name and surname, create it
                    if "first_name" in record and "surname" in record:
                        record["name"] = f"{record['first_name']} {record['surname']}"
                    else:
                        # Default name if we can't create it
                        record["name"] = "Unknown Person"
                elif field_name == "first_name" and "name" in record:
                    # Extract first_name from name
                    name_parts = record["name"].split()
                    if name_parts:
                        record["first_name"] = name_parts[0]
                    else:
                        record["first_name"] = "Unknown"
                elif field_name == "surname" and "name" in record:
                    # Extract surname from name
                    name_parts = record["name"].split()
                    if len(name_parts) > 1:
                        record["surname"] = name_parts[-1]
                    else:
                        record["surname"] = "Unknown"
        
        return record

    def _batch_import(self, records):
        """
        Import a batch of records with improved error handling and data validation
        
        Args:
            records: List of records to import
            
        Returns:
            tuple: (records_created, records_updated, records_errored, records_skipped)
        """
        created_count = 0
        updated_count = 0
        error_count = 0
        skipped_count = 0
        
        # Start a transaction
        cr = self.env.cr
        
        try:
            for record in records:
                try:
                    # Clean the record
                    record = self._clean_record(record)
                    
                    # Skip if record doesn't have minimum required fields
                    if not record.get("unique_identifier"):
                        _logger.warning(f"Skipping record: Missing unique_identifier")
                        skipped_count += 1
                        continue
                    
                    if not record.get("first_name") or not record.get("surname"):
                        _logger.warning(f"Skipping record: Missing first_name or surname for {record.get('unique_identifier')}")
                        skipped_count += 1
                        continue
                    
                    # Debug: Print the record being processed
                    _logger.debug(f"Processing record: {record.get('unique_identifier')} - {record.get('first_name')} {record.get('surname')}")
                    
                    # Check if record already exists
                    existing = self.pep_model.search(
                        [("unique_identifier", "=", record["unique_identifier"])]
                    )
                    
                    if existing:
                        # Debug: Print existing record details
                        _logger.debug(f"Found existing record: {existing.unique_identifier} - {existing.first_name} {existing.surname}")
                        
                        # Don't update if no changes
                        changes = False
                        for field, value in record.items():
                            if field in existing and existing[field] != value:
                                _logger.debug(f"Field {field} changed: '{existing[field]}' -> '{value}'")
                                changes = True
                                break
                        
                        if changes:
                            # Update existing record
                            try:
                                existing.write(record)
                                updated_count += 1
                                _logger.debug(f"Updated record: {record['unique_identifier']}")
                            except Exception as e:
                                error_count += 1
                                self.errors.append((record, f"Update error: {str(e)}"))
                                _logger.error(f"Error updating record {record['unique_identifier']}: {str(e)}")
                        else:
                            skipped_count += 1
                            _logger.debug(f"Skipped record (no changes): {record['unique_identifier']}")
                    else:
                        # Create new record
                        try:
                            # Make one final check of required fields
                            if not record.get("first_name"):
                                record["first_name"] = "Unknown"
                            
                            if not record.get("surname"):
                                record["surname"] = "Unknown"
                            
                            # Set name field based on first_name and surname
                            record["name"] = f"{record['first_name']} {record['surname']}"
                            
                            self.pep_model.create(record)
                            created_count += 1
                            _logger.debug(f"Created record: {record['unique_identifier']} - {record['name']}")
                        except Exception as e:
                            error_count += 1
                            self.errors.append((record, f"Create error: {str(e)}"))
                            _logger.error(f"Error creating record {record.get('unique_identifier')}: {str(e)}")
                            _logger.error(f"Record data: {record}")
                
                except Exception as e:
                    error_count += 1
                    self.errors.append((record, str(e)))
                    _logger.error(f"Error importing record: {str(e)}")
                    traceback.print_exc()
            
            # Commit the transaction
            cr.commit()
            
            return created_count, updated_count, error_count, skipped_count
        
        except Exception as e:
            # Rollback on error
            cr.rollback()
            _logger.error(f"Error in batch import: {str(e)}")
            traceback.print_exc()
            
            # Return zero counts since we rolled back
            return 0, 0, len(records), 0

    def import_records(self, records):
        """
        Import records into the database

        Args:
            records: List of records to import

        Returns:
            tuple: (records_created, records_updated, records_errored, records_skipped)
        """
        # Import in batches
        batch_created = 0
        batch_updated = 0
        batch_error = 0
        batch_skipped = 0

        for i in range(0, len(records), self.batch_size):
            batch = records[i : i + self.batch_size]

            # Import batch
            created, updated, error, skipped = self._batch_import(batch)

            # Update batch counters
            batch_created += created
            batch_updated += updated
            batch_error += error
            batch_skipped += skipped

            # Update global counters
            self.created_count += created
            self.updated_count += updated
            self.error_count += error
            self.skipped_count += skipped

            # Log progress
            _logger.info(
                f"Imported batch of {len(batch)} records: {created} created, {updated} updated, {error} errors, {skipped} skipped"
            )

            # Small delay to avoid overwhelming the database
            if i + self.batch_size < len(records):
                time.sleep(0.1)

        return batch_created, batch_updated, batch_error, batch_skipped

    def process_file(self, file_info, processor):
        """
        Process and import data from a file

        Args:
            file_info: Dictionary with file information
            processor: DataProcessor instance

        Returns:
            dict: Import results
        """
        try:
            # Process the file
            results = processor.process_file(file_info, self.import_records)

            # Return combined results
            return {
                "status": results["status"],
                "message": results["message"],
                "records_processed": results["records_processed"],
                "records_valid": results["records_valid"],
                "records_invalid": results["records_invalid"],
                "records_created": self.created_count,
                "records_updated": self.updated_count,
                "records_errored": self.error_count,
                "records_skipped": self.skipped_count,
                "errors": self.errors[:10],  # Include first 10 errors for reference
            }

        except Exception as e:
            _logger.error(f"Error processing file {file_info['path']}")
            _logger.error(f"Error: {str(e)}")
            traceback.print_exc()

            return {
                "status": "error",
                "message": f"Error processing file: {str(e)}",
                "records_processed": 0,
                "records_valid": 0,
                "records_invalid": 0,
                "records_created": 0,
                "records_updated": 0,
                "records_errored": 0,
                "records_skipped": 0,
                "errors": [(None, str(e))],
            }

    def reset_counters(self):
        """
        Reset import counters
        """
        self.created_count = 0
        self.updated_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.errors = []
