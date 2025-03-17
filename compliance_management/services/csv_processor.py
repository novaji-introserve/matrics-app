import base64
import logging
import os
import re
import pandas as pd
import numpy as np
import chardet
import psycopg2
from io import BytesIO
from datetime import datetime
from odoo import _, fields
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

try:
    from ..services.websocket.connection import send_message
except ImportError:
    # Mock function if the real one can't be imported
    def send_message(env, message, message_type="info", user_id=None):
        log_level = {
            "info": _logger.info,
            "error": _logger.error,
            "success": _logger.info,
            "warning": _logger.warning,
        }.get(message_type, _logger.info)
        log_level(f"[{message_type.upper()}] {message}")


class CSVProcessor:
    """CSV Processor for dynamic model import with batch processing from file storage"""

    # Maximum records to insert in a single SQL statement
    MAX_BATCH_INSERT = 1000  # Maximum records to insert in a single SQL statement

    def __init__(self, import_log):
        """Initialize with import log record"""
        self.import_log = import_log
        self.env = import_log.env
        self.user_id = import_log.uploaded_by.id
        self.model_name = import_log.model_name
        self.model = self.env[self.model_name]
        self.file_path = import_log.file_path
        self.file_name = import_log.file_name
        self.results = {
            "success": False,
            "successful": 0,
            "failed": 0,
            "duplicates": 0,
            "error_message": "",
            "technical_details": "",
        }
        
        # Fields to validate and handle
        self.required_fields = []
        self.model_fields_info = {}

    def process_batch(self, start_position, end_position):
        """
        Process a specific batch of records from the file
        
        Args:
            start_position: Record index to start from (0-based)
            end_position: Record index to end at (exclusive)
        
        Returns:
            Dictionary with processing results
        """
        cr = self.env.cr
        
        try:
            # Validate inputs
            if start_position < 0 or end_position <= start_position:
                raise ValueError(f"Invalid positions: start={start_position}, end={end_position}")
                
            # Check if file exists
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"Import file not found at {self.file_path}")
            
            send_message(
                self.env, 
                f"Processing batch from position {start_position} to {end_position}", 
                "info", 
                self.user_id
            )
            
            # Read the specific chunk from the file
            df_chunk = self._read_file_chunk(start_position, end_position - start_position)
            if df_chunk is None or df_chunk.empty:
                raise ValueError(f"No data found in range {start_position}-{end_position}")
                
            # Get field mappings (only once if not already done)
            existing_mappings = self.env["import.field.mapping"].search([
                ("import_log_id", "=", self.import_log.id)
            ])
            
            if existing_mappings:
                # Use existing mappings
                field_mappings = {}
                for mapping in existing_mappings:
                    field_mappings[mapping.csv_field] = mapping.model_field
                    
                send_message(
                    self.env, 
                    f"Using {len(field_mappings)} existing field mappings", 
                    "info", 
                    self.user_id
                )
            else:
                # Create new mappings from scratch
                field_mappings = self._create_field_mappings(df_chunk)
                
            if not field_mappings:
                self.results["error_message"] = "Failed to map CSV columns to model fields"
                return self.results
                
            # Analyze required fields for validation
            self._analyze_required_fields()
            
            # Process the chunk
            result = self._process_records(df_chunk, field_mappings)
            
            # Update results
            self.results.update(result)
            self.results["success"] = True if result.get("successful", 0) > 0 else False
            
            send_message(
                self.env,
                f"Batch processed: {result.get('successful', 0)} successful, {result.get('failed', 0)} failed, {result.get('duplicates', 0)} duplicates",
                "success" if result.get("successful", 0) > 0 else "warning",
                self.user_id,
            )
            
            return self.results
            
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            error_message = f"Error processing batch: {str(e)}"
            _logger.error(error_message)
            _logger.error(error_trace)
            
            # Make sure to rollback the current transaction
            try:
                cr.rollback()
            except:
                pass
                
            send_message(self.env, error_message, "error", self.user_id)
            
            self.results.update({
                "success": False,
                "error_message": error_message,
                "technical_details": error_trace,
            })
            
            return self.results

    def _read_file_chunk(self, start_row, num_rows):
        """
        Read a specific chunk from the file
        
        Args:
            start_row: Row index to start from (0-based, skipping header)
            num_rows: Number of rows to read
            
        Returns:
            pandas DataFrame with the chunk data
        """
        try:
            # Determine file type
            file_ext = os.path.splitext(self.file_path)[1].lower()
            
            # For Excel files
            if file_ext in ('.xlsx', '.xls'):
                # Need to add 1 to start_row to account for header row in Excel
                df = pd.read_excel(
                    self.file_path,
                    dtype=str,
                    engine="openpyxl",
                    keep_default_na=False,
                    skiprows=start_row+1,  # +1 for header
                    nrows=num_rows
                )
                
                # If this is the first chunk, check if we have at least one valid row
                if start_row == 0 and (df.empty or df.shape[0] == 0):
                    # Try without skipping rows in case the file has no header
                    df = pd.read_excel(
                        self.file_path,
                        dtype=str,
                        engine="openpyxl",
                        keep_default_na=False,
                        nrows=num_rows
                    )
                    
                send_message(
                    self.env,
                    f"Excel chunk read successfully: {df.shape[0]} rows, {df.shape[1]} columns",
                    "success",
                    self.user_id,
                )
                return df
                
            # For CSV files
            else:
                # Detect encoding first
                with open(self.file_path, 'rb') as f:
                    sample = f.read(10000)
                    detection = chardet.detect(sample)
                    encoding = detection["encoding"] or "utf-8"
                
                # Try different separators if needed
                for sep in [',', ';', '\t', '|']:
                    try:
                        # Skip to the start_row (add 1 for header)
                        df = pd.read_csv(
                            self.file_path,
                            dtype=str,
                            encoding=encoding,
                            sep=sep,
                            keep_default_na=False,
                            skiprows=range(1, start_row+1),  # Skip header (row 0) and rows up to start_row
                            nrows=num_rows
                        )
                        
                        # Check if we have a valid dataframe
                        if df.shape[1] > 1:
                            send_message(
                                self.env,
                                f"CSV chunk read successfully: {df.shape[0]} rows, {df.shape[1]} columns",
                                "success",
                                self.user_id,
                            )
                            return df
                    except Exception as e:
                        _logger.debug(f"Failed to read CSV with separator '{sep}': {e}")
                        continue
                
                raise ValueError("Failed to read CSV file with any separator")
                
        except Exception as e:
            send_message(
                self.env, f"Error reading file chunk: {str(e)}", "error", self.user_id
            )
            raise

    def _create_field_mappings(self, df):
        """
        Create mappings between CSV columns and model fields
        
        Args:
            df: pandas DataFrame with the data
            
        Returns:
            Dictionary mapping CSV column names to model field names
        """
        try:
            # Get model fields
            model_fields = {}
            for field_name, field in self.model._fields.items():
                if not field.store or field.type in ["one2many", "reference"]:
                    continue

                model_fields[field_name] = {
                    "name": field_name,
                    "string": field.string,
                    "type": field.type,
                    "required": field.required,
                    "relation": (
                        field.comodel_name if hasattr(field, "comodel_name") else None
                    ),
                }

            # Get CSV columns
            csv_columns = list(df.columns)
            
            # Create mapping dictionary
            field_mappings = {}

            # Map exact matches first
            for col in csv_columns:
                col_lower = col.lower().strip()

                # Check for direct field name match
                if col_lower in model_fields:
                    field_mappings[col] = model_fields[col_lower]["name"]
                    continue

                # Try to match field name with spaces/underscores removed
                col_normalized = re.sub(r"[\s_-]", "", col_lower)
                for field_name, field_info in model_fields.items():
                    field_normalized = re.sub(r"[\s_-]", "", field_name.lower())
                    if col_normalized == field_normalized:
                        field_mappings[col] = field_name
                        break

                # Try to match field label
                for field_name, field_info in model_fields.items():
                    if col_lower == field_info["string"].lower().strip():
                        field_mappings[col] = field_name
                        break

            # Log mapping
            mapping_info = "Field mappings:\n"
            for csv_field, model_field in field_mappings.items():
                mapping_info += f"  - {csv_field} => {model_field}\n"

                # Store mapping in database
                self.env["import.field.mapping"].create(
                    {
                        "import_log_id": self.import_log.id,
                        "csv_field": csv_field,
                        "model_field": model_field,
                        "field_type": model_fields[model_field]["type"],
                        "required": model_fields[model_field]["required"],
                    }
                )
            
            # Commit mappings immediately to avoid losing them on transaction rollback
            self.env.cr.commit()

            send_message(self.env, mapping_info, "info", self.user_id)

            # Check for required fields
            required_fields = [
                f
                for f, info in model_fields.items()
                if info["required"] and f not in field_mappings.values()
            ]

            # Remove fields that have defaults or are computed
            for field_name in list(required_fields):
                field = self.model._fields[field_name]
                if field.default is not None or field.compute:
                    required_fields.remove(field_name)

            if required_fields:
                missing = ", ".join(required_fields)
                # Continue anyway with warning - we'll handle missing fields during preprocessing
                warning_msg = f"Required fields are missing in CSV: {missing}"
                send_message(self.env, warning_msg, "warning", self.user_id)
                send_message(
                    self.env, 
                    "Will attempt to derive missing required fields during import",
                    "info", 
                    self.user_id
                )

            return field_mappings

        except Exception as e:
            send_message(
                self.env, f"Error mapping fields: {str(e)}", "error", self.user_id
            )
            return None

    def _analyze_required_fields(self):
        """Analyze model fields to identify required fields and their properties"""
        self.required_fields = []
        self.model_fields_info = {}
        
        # Get all fields for the model
        for field_name, field in self.model._fields.items():
            # Skip non-storable fields
            if not field.store:
                continue
                
            field_info = {
                "name": field_name,
                "type": field.type,
                "required": field.required,
                "default": field.default,
                "compute": bool(field.compute),
                "relation": field.comodel_name if hasattr(field, "comodel_name") else None,
                "has_default": field.default is not None,
            }
            
            self.model_fields_info[field_name] = field_info
            
            # Track truly required fields (required, no default, not computed)
            if field.required and not field.default and not field.compute:
                self.required_fields.append(field_name)
                
        _logger.info(f"Required fields for {self.model_name}: {self.required_fields}")

    def _process_records(self, df, field_mappings):
        """
        Process records in the dataframe
        
        Args:
            df: pandas DataFrame with the data
            field_mappings: Dictionary mapping CSV column names to model field names
            
        Returns:
            Dictionary with processing results
        """
        results = {"successful": 0, "failed": 0, "duplicates": 0}
        cr = self.env.cr
        
        try:
            # Convert dataframe to list of dicts with appropriate field mapping
            records = []

            for _, row in df.iterrows():
                record = {}
                for csv_field, model_field in field_mappings.items():
                    # Skip empty values
                    if csv_field not in row or pd.isna(row[csv_field]) or row[csv_field] == "":
                        continue

                    # Get field type and convert value accordingly
                    field_type = self.model._fields[model_field].type
                    value = self._convert_value(row[csv_field], field_type, model_field)

                    if value is not None:
                        record[model_field] = value

                # Apply preprocessing to ensure required fields are handled
                record = self._preprocess_record(record, row)
                
                # Skip records that couldn't be properly validated
                if record is not None:
                    records.append(record)
                else:
                    results["failed"] += 1

            # Check for duplicates
            clean_records, duplicate_count = self._handle_duplicates(records)
            results["duplicates"] = duplicate_count

            # Process records in smaller batches
            if clean_records:
                successful = 0
                failed = 0
                
                # Set a savepoint before batch processing
                savepoint_name = f"csv_import_batch_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
                cr.execute(f"SAVEPOINT {savepoint_name}")
                
                try:
                    # Process in smaller batches to prevent oversized SQL queries
                    for i in range(0, len(clean_records), self.MAX_BATCH_INSERT):
                        batch = clean_records[i:i + self.MAX_BATCH_INSERT]
                        
                        # Set a savepoint for this mini-batch
                        mini_savepoint = f"{savepoint_name}_mini_{i}"
                        cr.execute(f"SAVEPOINT {mini_savepoint}")
                        
                        try:
                            created_records = self.model.with_context(
                                tracking_disable=True,
                                import_file=True,  # Custom context to optimize performance
                                # Disable mail thread features for imports
                                mail_create_nosubscribe=True,
                                mail_create_nolog=True,
                                mail_notrack=True,
                            ).create(batch)
                            successful += len(created_records)
                            
                            # Commit each successful batch immediately to avoid long transactions
                            cr.commit()
                            
                        except Exception as e:
                            _logger.warning(f"Error creating batch: {str(e)}. Rolling back to savepoint and trying individual records.")
                            # Rollback to the mini-batch savepoint
                            cr.execute(f"ROLLBACK TO SAVEPOINT {mini_savepoint}")
                            
                            # Try each record individually
                            for record in batch:
                                # Set a savepoint for each record
                                record_savepoint = f"{mini_savepoint}_record_{batch.index(record)}"
                                cr.execute(f"SAVEPOINT {record_savepoint}")
                                
                                try:
                                    self.model.with_context(
                                        tracking_disable=True,
                                        import_file=True,
                                        mail_create_nosubscribe=True,
                                        mail_create_nolog=True,
                                        mail_notrack=True,
                                    ).create([record])
                                    successful += 1
                                except Exception as e2:
                                    # Roll back to the record savepoint
                                    cr.execute(f"ROLLBACK TO SAVEPOINT {record_savepoint}")
                                    _logger.warning(f"Error creating individual record: {str(e2)}")
                                    failed += 1
                                    
                                # Commit each successful record to avoid long transactions
                                cr.commit()
                    
                    # If we got here, everything worked
                    results["successful"] = successful
                    results["failed"] = failed
                    
                except Exception as e:
                    # Roll back to the main batch savepoint
                    cr.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                    
                    send_message(
                        self.env,
                        f"Error in batch processing: {str(e)}. Falling back to very careful individual record creation.",
                        "warning",
                        self.user_id,
                    )
                    
                    # Last resort - try each record one by one with complete transaction isolation
                    successful = 0
                    failed = 0
                    
                    for record in clean_records:
                        try:
                            # Make sure we're in a good state
                            if cr._cnx.status != psycopg2.extensions.STATUS_READY:
                                cr.rollback()
                                
                            # Set a savepoint for this record
                            record_savepoint = f"record_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
                            cr.execute(f"SAVEPOINT {record_savepoint}")
                            
                            self.model.with_context(
                                tracking_disable=True,
                                import_file=True,
                                mail_create_nosubscribe=True,
                                mail_create_nolog=True,
                                mail_notrack=True,
                            ).create([record])
                            successful += 1
                            
                            # Release the savepoint
                            cr.execute(f"RELEASE SAVEPOINT {record_savepoint}")
                            
                            # Commit each successful record to avoid long transactions
                            cr.commit()
                        
                        except Exception as e:
                            # Roll back to the savepoint
                            try:
                                cr.execute(f"ROLLBACK TO SAVEPOINT {record_savepoint}")
                            except:
                                # If even this fails, do a full rollback
                                try:
                                    cr.rollback()
                                except:
                                    pass
                                    
                            _logger.warning(f"Error creating individual record: {str(e)}")
                            failed += 1

                    results["successful"] = successful
                    results["failed"] = failed

            # Explicit commit - although we've been committing along the way
            cr.commit()
            return results

        except Exception as e:
            # Make sure to rollback if there's an error
            try:
                cr.rollback()
            except:
                pass
                
            send_message(
                self.env, f"Error processing records: {str(e)}", "error", self.user_id
            )
            results["failed"] = len(df)
            return results

    def _preprocess_record(self, record, raw_row=None):
        """
        Preprocess record to fill in required fields and validate data
        
        Args:
            record: Dict of field values already mapped and converted
            raw_row: Original pandas row for additional data extraction if needed
            
        Returns:
            Processed record dict or None if record should be skipped
        """
        try:
            # Quick check if record is empty
            if not record:
                return None
                
            # Handle missing required fields
            for field_name in self.required_fields:
                if field_name not in record or record[field_name] is None or record[field_name] == '':
                    # Try to derive the value from other fields
                    derived_value = self._derive_field_value(field_name, record, raw_row)
                    if derived_value is not None:
                        record[field_name] = derived_value
                    else:
                        # Special case for specific models
                        if self.model_name == 'res.pep' and field_name == 'first_name':
                            # For PEP model, derive first_name from name or surname
                            if 'name' in record and record['name']:
                                # Extract first name from full name
                                name_parts = record['name'].strip().split()
                                if name_parts:
                                    record['first_name'] = name_parts[0]
                            elif 'surname' in record and record['surname']:
                                # Fall back to using surname as first name
                                record['first_name'] = record['surname']
                            else:
                                # Default value
                                record['first_name'] = "Unknown"
                        else:
                            # Log the missing required field
                            _logger.warning(f"Missing required field {field_name} in record: {record}")
                            
                            # Return None to skip this record
                            return None
                            
            return record
            
        except Exception as e:
            _logger.error(f"Error preprocessing record: {str(e)}")
            return None

    def _derive_field_value(self, field_name, record, raw_row=None):
        """
        Try to derive a value for a missing field based on rules specific to the model
        
        Args:
            field_name: Name of the field to derive
            record: Current record dict
            raw_row: Original pandas row for additional data extraction
            
        Returns:
            Derived value or None if can't be derived
        """
        # Model-specific field derivations
        if self.model_name == 'res.pep':
            if field_name == 'first_name' and 'name' in record:
                parts = record['name'].strip().split()
                if parts:
                    return parts[0]
                    
            if field_name == 'import_status':
                return 'new'
                
        # Add more model-specific rules as needed
                
        # Generic rules for common fields
        if field_name == 'name' and 'full_name' in record:
            return record['full_name']
            
        if field_name == 'active':
            return True
            
        # Try to check raw data if available
        if raw_row is not None:
            # Try different column names that might map to the field
            possible_columns = [
                field_name,
                field_name.title(),
                field_name.upper(),
                field_name.replace('_', ' '),
                ' '.join(word.capitalize() for word in field_name.split('_'))
            ]
            
            for col in possible_columns:
                if col in raw_row.index and not pd.isna(raw_row[col]) and raw_row[col] != '':
                    # We found a potential column in the raw data
                    field_type = self.model_fields_info.get(field_name, {}).get('type', 'char')
                    value = self._convert_value(raw_row[col], field_type, field_name)
                    if value is not None:
                        return value
                        
        # No value could be derived
        return None

    def _convert_value(self, value, field_type, field_name):
        """Convert value to appropriate type for Odoo field"""
        if value is None or pd.isna(value) or value == "":
            return None

        try:
            # Convert to string first
            if not isinstance(value, str):
                value = str(value)

            value = value.strip()
            
            # Special case for 'NULL' string values
            if value.upper() == 'NULL':
                return False  # False will be treated as NULL in SQL

            if field_type == "char" or field_type == "text":
                return value
            elif field_type == "integer":
                # Remove any non-numeric characters except negative sign
                clean_value = re.sub(r"[^0-9-]", "", value)
                return int(clean_value) if clean_value else 0
            elif field_type == "float" or field_type == "monetary":
                # Replace comma with dot for decimal separator
                clean_value = value.replace(",", ".")
                # Remove any non-numeric characters except decimal point and negative sign
                clean_value = re.sub(r"[^0-9.-]", "", clean_value)
                return float(clean_value) if clean_value else 0.0
            elif field_type == "boolean":
                return value.lower() in ("true", "yes", "y", "1", "x")
            elif field_type == "date":
                try:
                    parsed_date = fields.Date.from_string(value)
                    return fields.Date.to_string(parsed_date)
                except:
                    # Try pandas date parsing as fallback
                    return pd.to_datetime(value).strftime("%Y-%m-%d")
            elif field_type == "datetime":
                try:
                    parsed_datetime = fields.Datetime.from_string(value)
                    return fields.Datetime.to_string(parsed_datetime)
                except:
                    # Try pandas datetime parsing as fallback
                    return pd.to_datetime(value).strftime("%Y-%m-%d %H:%M:%S")
            elif field_type == "many2one":
                # Try to lookup the record by name
                relation = self.model._fields[field_name].comodel_name
                related_model = self.env[relation]

                # Try to find by ID first
                if value.isdigit():
                    record = related_model.browse(int(value)).exists()
                    if record:
                        return record.id

                # Then try to find by name
                record = related_model.search([("name", "=", value)], limit=1)
                if record:
                    return record.id

                # If still not found, try to find by name case insensitive
                record = related_model.search([("name", "=ilike", value)], limit=1)
                if record:
                    return record.id

                return None
            else:
                return value

        except Exception as e:
            _logger.warning(
                f"Error converting value '{value}' for field '{field_name}': {str(e)}"
            )
            # Return original value and let Odoo handle conversion errors
            return value

    def _handle_duplicates(self, records):
        """Check for and remove duplicate records"""
        if not records:
            return [], 0

        # Get model's unique fields
        unique_fields = []
        for constraint in self.model._sql_constraints:
            if "unique" in constraint[1].lower():
                fields_match = re.search(
                    r"unique\s*\(([^)]+)\)", constraint[1], re.IGNORECASE
                )
                if fields_match:
                    fields = [f.strip() for f in fields_match.group(1).split(",")]
                    unique_fields.extend(fields)

        # Handle _rec_name if no unique constraints found
        if not unique_fields and hasattr(self.model, "_rec_name"):
            unique_fields.append(self.model._rec_name)

        # Add 'name' field as fallback if still empty and it exists
        if not unique_fields and "name" in self.model._fields:
            unique_fields.append("name")

        # If still no unique fields, we can't check for duplicates
        if not unique_fields:
            send_message(
                self.env,
                "No unique fields found to check for duplicates",
                "warning",
                self.user_id,
            )
            return records, 0

        # Check each record against existing database records
        duplicates = []
        clean_records = []

        for record in records:
            is_duplicate = False

            # Build domain for potential duplicates
            domain = []
            for field in unique_fields:
                if field in record:
                    domain.append((field, "=", record[field]))

            if domain:
                # Combine with OR if multiple unique fields
                if len(domain) > 1:
                    domain = ["|"] * (len(domain) - 1) + domain

                # Search for existing record
                existing = self.model.search(domain, limit=1)
                if existing:
                    is_duplicate = True
                    duplicates.append(record)

            if not is_duplicate:
                clean_records.append(record)

        duplicate_count = len(duplicates)
        if duplicate_count > 0:
            send_message(
                self.env,
                f"Found {duplicate_count} duplicate records that will be skipped",
                "warning",
                self.user_id,
            )

        return clean_records, duplicate_count

# import base64
# import logging
# import tempfile
# import os
# import re
# import pandas as pd
# import numpy as np
# import chardet
# import psycopg2
# from io import BytesIO
# from datetime import datetime
# from odoo import _, fields
# from odoo.exceptions import UserError

# _logger = logging.getLogger(__name__)

# try:
#     from ..services.websocket.connection import send_message
# except ImportError:
#     # Mock function if the real one can't be imported
#     def send_message(env, message, message_type="info", user_id=None):
#         log_level = {
#             "info": _logger.info,
#             "error": _logger.error,
#             "success": _logger.info,
#             "warning": _logger.warning,
#         }.get(message_type, _logger.info)
#         log_level(f"[{message_type.upper()}] {message}")


# class CSVProcessor:
#     """CSV Processor for dynamic model import with optimized performance and resume capability"""

#     # Increase chunk size for better performance (adjust based on your system's memory)
#     CHUNK_SIZE = 5000  # Increased from 1000 to 5000 records per batch
    
#     # How often to commit to the database (in chunks)
#     COMMIT_EVERY = 2  # Commit every 2 chunks (10,000 records)
    
#     # Maximum records to insert in a single SQL statement
#     MAX_BATCH_INSERT = 1000  # Maximum records to insert in a single SQL statement

#     def __init__(self, import_log):
#         """Initialize with import log record"""
#         self.import_log = import_log
#         self.env = import_log.env
#         self.user_id = import_log.uploaded_by.id
#         self.model_name = import_log.model_name
#         self.model = self.env[self.model_name]
#         self.file_content = import_log.file
#         self.file_name = import_log.file_name
#         self.start_position = import_log.current_position or 0  # Resume position
#         self.results = {
#             "success": False,
#             "total_records": 0,
#             "successful_records": import_log.successful_records or 0,
#             "failed_records": import_log.failed_records or 0,
#             "duplicate_records": import_log.duplicate_records or 0,
#             "error_message": "",
#             "technical_details": "",
#         }

#     def process(self):
#         """Main processing method with resume capability"""
#         cr = self.env.cr
#         try:
#             # Update status to processing if not already
#             if self.import_log.status != 'processing':
#                 self.import_log.write({"status": "processing"})
#                 cr.commit()  # Commit status change immediately
                
#             send_message(
#                 self.env, f"Starting import for {self.model_name} (resuming from position {self.start_position})", 
#                 "info", self.user_id
#             )

#             # Read the file
#             df = self._read_file()
#             if df is None:
#                 return self.results

#             # Get total records
#             total_records = len(df)
#             self.results["total_records"] = total_records
#             send_message(
#                 self.env,
#                 f"Found {total_records} records to import (resuming from record {self.start_position})",
#                 "info",
#                 self.user_id,
#             )

#             # Skip if already completed
#             if self.start_position >= total_records:
#                 send_message(
#                     self.env,
#                     f"Import already completed (position {self.start_position} >= total {total_records})",
#                     "info",
#                     self.user_id,
#                 )
#                 self.import_log.write({"status": "completed"})
#                 cr.commit()
#                 return self.results

#             # Get field mappings
#             field_mappings = self._get_field_mappings(df)
#             if not field_mappings:
#                 self.results["error_message"] = (
#                     "Failed to map CSV columns to model fields"
#                 )
#                 return self.results

#             # Process in chunks, starting from the resume position
#             successful_records = self.results["successful_records"]
#             failed_records = self.results["failed_records"]
#             duplicate_records = self.results["duplicate_records"]
            
#             # Calculate which chunk we're starting from
#             start_chunk = self.start_position // self.CHUNK_SIZE
#             chunks_processed = 0

#             for chunk_index in range(start_chunk, (total_records + self.CHUNK_SIZE - 1) // self.CHUNK_SIZE):
#                 # Calculate chunk boundaries
#                 start_idx = chunk_index * self.CHUNK_SIZE
#                 end_idx = min(start_idx + self.CHUNK_SIZE, total_records)
                
#                 # Skip already processed records in the first chunk if resuming
#                 if chunk_index == start_chunk and self.start_position > start_idx:
#                     # Adjust the starting point within the first chunk
#                     actual_start = self.start_position
#                 else:
#                     actual_start = start_idx
                    
#                 # Skip if this chunk is already fully processed
#                 if actual_start >= end_idx:
#                     continue
                    
#                 # Get the relevant part of the chunk
#                 if actual_start > start_idx:
#                     # We need a partial chunk
#                     chunk = df.iloc[actual_start:end_idx]
#                 else:
#                     # Process the full chunk
#                     chunk = df.iloc[start_idx:end_idx]

#                 # Process each chunk in its own transaction
#                 try:
#                     # Try to process the chunk
#                     chunk_result = self._process_chunk(chunk, field_mappings)

#                     # Update counters
#                     successful_records += chunk_result.get("successful", 0)
#                     failed_records += chunk_result.get("failed", 0)
#                     duplicate_records += chunk_result.get("duplicates", 0)
                    
#                     # Save progress
#                     chunks_processed += 1
#                     current_position = min(end_idx, total_records)
                    
#                     # Always start a fresh transaction for status updates
#                     # This is critical - even if there was an issue in the chunk, 
#                     # we need to make sure we can still update the progress
#                     try:
#                         # Make sure the connection is in a good state
#                         if cr._cnx.status != psycopg2.extensions.STATUS_READY:
#                             cr.rollback()  # Roll back any failed transaction
                            
#                         # Update progress in import log
#                         progress_percent = round((current_position / total_records) * 100, 1)
#                         self.import_log.write({
#                             "successful_records": successful_records,
#                             "failed_records": failed_records,
#                             "duplicate_records": duplicate_records,
#                             "current_position": current_position,  # Track position for resume
#                         })
                        
#                         # Commit transaction to save progress
#                         cr.commit()
#                         send_message(
#                             self.env,
#                             f"Progress committed: {progress_percent}% ({current_position}/{total_records} records)",
#                             "info",
#                             self.user_id,
#                         )
#                     except Exception as e:
#                         # If we can't update the progress, log it but don't fail the whole import
#                         _logger.error(f"Error updating import progress: {str(e)}")
#                         # Try to rollback in case the transaction is aborted
#                         try:
#                             cr.rollback()
#                         except:
#                             pass
#                         send_message(
#                             self.env,
#                             f"Warning: Progress tracking temporarily unavailable. Import continues.",
#                             "warning",
#                             self.user_id,
#                         )
                        
#                 except Exception as e:
#                     import traceback
#                     error_msg = f"Error processing chunk {chunk_index}: {str(e)}"
#                     error_trace = traceback.format_exc()
#                     _logger.error(error_msg)
#                     _logger.error(error_trace)
                    
#                     # Make sure to rollback the current transaction
#                     try:
#                         cr.rollback()
#                     except:
#                         pass
                    
#                     # Try to update the import log with the error
#                     try:
#                         self.import_log.write({
#                             "successful_records": successful_records,
#                             "failed_records": failed_records + len(chunk),  # Count the whole chunk as failed
#                             "duplicate_records": duplicate_records,
#                             "current_position": actual_start,  # Go back to the start of this chunk
#                             "error_message": error_msg,
#                             "technical_details": error_trace,
#                         })
#                         cr.commit()
#                     except:
#                         # If we can't even update the status, we're in real trouble
#                         # Try another rollback and continue to the next chunk
#                         try:
#                             cr.rollback()
#                         except:
#                             pass
                    
#                     # Log the issue but continue with the next chunk
#                     send_message(
#                         self.env,
#                         f"Error processing chunk {chunk_index}: {str(e)}. Skipping to next chunk.",
#                         "error",
#                         self.user_id,
#                     )
                    
#                     # Increment the failed counter for the chunk
#                     failed_records += len(chunk)

#             # Update final results
#             self.results.update({
#                 "success": True if successful_records > 0 else False,
#                 "successful_records": successful_records,
#                 "failed_records": failed_records,
#                 "duplicate_records": duplicate_records,
#             })
            
#             # Ensure we're in a fresh transaction state
#             try:
#                 if cr._cnx.status != psycopg2.extensions.STATUS_READY:
#                     cr.rollback()
                
#                 # Mark import as completed
#                 self.import_log.write({
#                     "status": "completed",
#                     "completed_at": fields.Datetime.now(),
#                 })
#                 cr.commit()
#             except Exception as e:
#                 _logger.error(f"Error updating final status: {str(e)}")
#                 # Try one more rollback
#                 try:
#                     cr.rollback()
#                 except:
#                     pass

#             if successful_records > 0:
#                 send_message(
#                     self.env,
#                     f"Import completed successfully: {successful_records} records imported",
#                     "success",
#                     self.user_id,
#                 )
#             else:
#                 error_msg = f"Import failed: No records were imported. {failed_records} failed, {duplicate_records} duplicates"
#                 send_message(self.env, error_msg, "error", self.user_id)
#                 self.results["error_message"] = error_msg

#             if failed_records > 0:
#                 send_message(
#                     self.env,
#                     f"Warning: {failed_records} records failed to import",
#                     "warning",
#                     self.user_id,
#                 )

#             if duplicate_records > 0:
#                 send_message(
#                     self.env,
#                     f"Warning: {duplicate_records} duplicate records were found",
#                     "warning",
#                     self.user_id,
#                 )

#             return self.results

#         except Exception as e:
#             import traceback

#             error_trace = traceback.format_exc()
#             error_message = f"Error processing import: {str(e)}"
#             _logger.error(error_message)
#             _logger.error(error_trace)

#             send_message(self.env, error_message, "error", self.user_id)

#             # Ensure we roll back any open transaction
#             try:
#                 cr.rollback()
#             except:
#                 pass
                
#             # Try to update the import log status
#             try:
#                 # Don't mark as failed if we can resume
#                 if self.import_log.current_position and self.import_log.current_position > 0:
#                     status = "processing"  # Keep as processing so we can resume
#                 else:
#                     status = "failed"
                    
#                 self.import_log.write({
#                     "status": status,
#                     "error_message": error_message,
#                     "technical_details": error_trace,
#                     "completed_at": fields.Datetime.now() if status == "failed" else False,
#                 })
#                 cr.commit()
#             except:
#                 # If we can't update the status, just log it
#                 _logger.error("Failed to update import status")
#                 try:
#                     cr.rollback()
#                 except:
#                     pass
            
#             self.results.update({
#                 "success": False,
#                 "error_message": error_message,
#                 "technical_details": error_trace,
#             })
#             return self.results

#     def _process_chunk(self, chunk, field_mappings):
#         """Process a chunk of data with optimized batch processing"""
#         results = {"successful": 0, "failed": 0, "duplicates": 0}
#         cr = self.env.cr

#         try:
#             # Convert chunk to list of dicts with appropriate field mapping
#             records = []

#             for _, row in chunk.iterrows():
#                 record = {}
#                 for csv_field, model_field in field_mappings.items():
#                     # Skip empty values
#                     if pd.isna(row[csv_field]) or row[csv_field] == "":
#                         continue

#                     # Get field type and convert value accordingly
#                     field_type = self.model._fields[model_field].type
#                     value = self._convert_value(row[csv_field], field_type, model_field)

#                     if value is not None:
#                         record[model_field] = value

#                 records.append(record)

#             # Check for duplicates
#             clean_records, duplicate_count = self._handle_duplicates(records)
#             results["duplicates"] = duplicate_count

#             # Create records in smaller batches
#             if clean_records:
#                 successful = 0
#                 failed = 0
                
#                 # Set a savepoint before batch processing
#                 savepoint_name = f"csv_import_batch_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
#                 cr.execute(f"SAVEPOINT {savepoint_name}")
                
#                 try:
#                     # Process in smaller batches to prevent oversized SQL queries
#                     for i in range(0, len(clean_records), self.MAX_BATCH_INSERT):
#                         batch = clean_records[i:i + self.MAX_BATCH_INSERT]
                        
#                         # Set a savepoint for this mini-batch
#                         mini_savepoint = f"{savepoint_name}_mini_{i}"
#                         cr.execute(f"SAVEPOINT {mini_savepoint}")
                        
#                         try:
#                             created_records = self.model.with_context(
#                                 tracking_disable=True,
#                                 import_file=True,  # Custom context to optimize performance
#                                 # Disable mail thread features for imports
#                                 mail_create_nosubscribe=True,
#                                 mail_create_nolog=True,
#                                 mail_notrack=True,
#                             ).create(batch)
#                             successful += len(created_records)
#                         except Exception as e:
#                             _logger.warning(f"Error creating batch: {str(e)}. Rolling back to savepoint and trying individual records.")
#                             # Rollback to the mini-batch savepoint
#                             cr.execute(f"ROLLBACK TO SAVEPOINT {mini_savepoint}")
                            
#                             # Try each record individually
#                             for record in batch:
#                                 # Set a savepoint for each record
#                                 record_savepoint = f"{mini_savepoint}_record_{batch.index(record)}"
#                                 cr.execute(f"SAVEPOINT {record_savepoint}")
                                
#                                 try:
#                                     self.model.with_context(
#                                         tracking_disable=True,
#                                         import_file=True,
#                                         mail_create_nosubscribe=True,
#                                         mail_create_nolog=True,
#                                         mail_notrack=True,
#                                     ).create([record])
#                                     successful += 1
#                                 except Exception as e2:
#                                     # Roll back to the record savepoint
#                                     cr.execute(f"ROLLBACK TO SAVEPOINT {record_savepoint}")
#                                     _logger.warning(f"Error creating individual record: {str(e2)}")
#                                     failed += 1
                    
#                     # If we got here, everything worked
#                     results["successful"] = successful
#                     results["failed"] = failed
                    
#                 except Exception as e:
#                     # Roll back to the main batch savepoint
#                     cr.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                    
#                     send_message(
#                         self.env,
#                         f"Error in batch processing: {str(e)}. Falling back to very careful individual record creation.",
#                         "warning",
#                         self.user_id,
#                     )
                    
#                     # Last resort - try each record one by one with complete transaction isolation
#                     successful = 0
#                     failed = 0
                    
#                     for record in clean_records:
#                         # Each record gets its own transaction
#                         try:
#                             # Make sure we're in a good state
#                             if cr._cnx.status != psycopg2.extensions.STATUS_READY:
#                                 cr.rollback()
                                
#                             # Set a savepoint for this record
#                             record_savepoint = f"record_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
#                             cr.execute(f"SAVEPOINT {record_savepoint}")
                            
#                             self.model.with_context(
#                                 tracking_disable=True,
#                                 import_file=True,
#                                 mail_create_nosubscribe=True,
#                                 mail_create_nolog=True,
#                                 mail_notrack=True,
#                             ).create([record])
#                             successful += 1
                            
#                             # Release the savepoint
#                             cr.execute(f"RELEASE SAVEPOINT {record_savepoint}")
                        
#                         except Exception as e:
#                             # Roll back to the savepoint
#                             try:
#                                 cr.execute(f"ROLLBACK TO SAVEPOINT {record_savepoint}")
#                             except:
#                                 # If even this fails, do a full rollback
#                                 try:
#                                     cr.rollback()
#                                 except:
#                                     pass
                                    
#                             _logger.warning(f"Error creating individual record: {str(e)}")
#                             failed += 1

#                     results["successful"] = successful
#                     results["failed"] = failed

#             # Explicitly commit the transaction for this chunk
#             cr.commit()
#             return results

#         except Exception as e:
#             # Make sure to rollback if there's an error
#             try:
#                 cr.rollback()
#             except:
#                 pass
                
#             send_message(
#                 self.env, f"Error processing chunk: {str(e)}", "error", self.user_id
#             )
#             results["failed"] = len(chunk)
#             return results

#     def _read_file(self):
#         """Read CSV or Excel file with robust error handling"""
#         try:
#             if not self.file_content:
#                 self.results["error_message"] = "No file content found"
#                 return None

#             # Decode file content
#             file_data = base64.b64decode(self.file_content)

#             # Create temporary file
#             file_ext = os.path.splitext(self.file_name.lower())[1]

#             # Check if Excel file
#             if file_ext in (".xlsx", ".xls"):
#                 send_message(
#                     self.env, "Reading Excel file...", "info", self.user_id
#                 )
#                 try:
#                     df = pd.read_excel(
#                         BytesIO(file_data),
#                         dtype=str,
#                         engine="openpyxl",
#                         keep_default_na=False,
#                     )
#                     send_message(
#                         self.env,
#                         "Excel file read successfully",
#                         "success",
#                         self.user_id,
#                     )
#                     return df
#                 except Exception as e:
#                     send_message(
#                         self.env,
#                         f"Error reading Excel file: {str(e)}",
#                         "error",
#                         self.user_id,
#                     )
#                     self.results["error_message"] = (
#                         f"Error reading Excel file: {str(e)}"
#                     )
#                     return None

#             # Handle CSV with multiple approaches
#             return self._read_csv_with_fallbacks(file_data)

#         except Exception as e:
#             send_message(
#                 self.env, f"Error reading file: {str(e)}", "error", self.user_id
#             )
#             self.results["error_message"] = f"Error reading file: {str(e)}"
#             return None

#     def _read_csv_with_fallbacks(self, file_data):
#         """Try multiple approaches to read CSV data"""
#         # Detect encoding
#         detection = chardet.detect(file_data[:10000])
#         encoding = detection["encoding"] or "utf-8"
#         confidence = detection["confidence"]
#         send_message(
#             self.env,
#             f"Detected encoding: {encoding} (confidence: {confidence:.2f})",
#             "info",
#             self.user_id,
#         )

#         # List of encodings to try
#         encodings = [encoding, "utf-8", "latin1", "ISO-8859-1", "cp1252"]
#         # List of separators to try
#         separators = [",", ";", "\t", "|"]

#         for enc in encodings:
#             for sep in separators:
#                 try:
#                     df = pd.read_csv(
#                         BytesIO(file_data),
#                         encoding=enc,
#                         sep=sep,
#                         dtype=str,
#                         keep_default_na=False,
#                         on_bad_lines="warn",
#                         low_memory=True,  # Use less memory for large files
#                     )

#                     # Check if we got at least one column and one row
#                     if df.shape[1] > 1 and df.shape[0] > 0:
#                         send_message(
#                             self.env,
#                             f"CSV file read successfully with encoding={enc}, separator='{sep}'",
#                             "success",
#                             self.user_id,
#                         )
#                         return df
#                 except Exception as e:
#                     continue

#         send_message(
#             self.env,
#             "Failed to read CSV with any encoding or separator",
#             "error",
#             self.user_id,
#         )
#         self.results["error_message"] = (
#             "Failed to read CSV with any encoding or separator"
#         )
#         return None

#     def _get_field_mappings(self, df):
#         """Map CSV columns to model fields"""
#         try:
#             # Get model fields
#             model_fields = {}
#             for field_name, field in self.model._fields.items():
#                 if not field.store or field.type in ["one2many", "reference"]:
#                     continue

#                 model_fields[field_name] = {
#                     "name": field_name,
#                     "string": field.string,
#                     "type": field.type,
#                     "required": field.required,
#                     "relation": (
#                         field.comodel_name if hasattr(field, "comodel_name") else None
#                     ),
#                 }

#             # Get CSV columns
#             csv_columns = list(df.columns)

#             # Check if we already have mappings from a previous run
#             existing_mappings = self.env["import.field.mapping"].search([
#                 ("import_log_id", "=", self.import_log.id)
#             ])
            
#             if existing_mappings:
#                 # Use existing mappings
#                 field_mappings = {}
#                 for mapping in existing_mappings:
#                     field_mappings[mapping.csv_field] = mapping.model_field
                    
#                 send_message(
#                     self.env, 
#                     f"Using {len(field_mappings)} existing field mappings from previous run", 
#                     "info", 
#                     self.user_id
#                 )
#                 return field_mappings

#             # Create mapping dictionary
#             field_mappings = {}

#             # Map exact matches first
#             for col in csv_columns:
#                 col_lower = col.lower().strip()

#                 # Check for direct field name match
#                 if col_lower in model_fields:
#                     field_mappings[col] = model_fields[col_lower]["name"]
#                     continue

#                 # Try to match field name with spaces/underscores removed
#                 col_normalized = re.sub(r"[\s_-]", "", col_lower)
#                 for field_name, field_info in model_fields.items():
#                     field_normalized = re.sub(r"[\s_-]", "", field_name.lower())
#                     if col_normalized == field_normalized:
#                         field_mappings[col] = field_name
#                         break

#                 # Try to match field label
#                 for field_name, field_info in model_fields.items():
#                     if col_lower == field_info["string"].lower().strip():
#                         field_mappings[col] = field_name
#                         break

#             # Log mapping
#             mapping_info = "Field mappings:\n"
#             for csv_field, model_field in field_mappings.items():
#                 mapping_info += f"  - {csv_field} => {model_field}\n"

#                 # Store mapping in database
#                 self.env["import.field.mapping"].create(
#                     {
#                         "import_log_id": self.import_log.id,
#                         "csv_field": csv_field,
#                         "model_field": model_field,
#                         "field_type": model_fields[model_field]["type"],
#                         "required": model_fields[model_field]["required"],
#                     }
#                 )
            
#             # Commit mappings immediately to avoid losing them on transaction rollback
#             self.env.cr.commit()

#             send_message(self.env, mapping_info, "info", self.user_id)

#             # Check for required fields
#             required_fields = [
#                 f
#                 for f, info in model_fields.items()
#                 if info["required"] and f not in field_mappings.values()
#             ]

#             # Remove fields that have defaults or are computed
#             for field_name in list(required_fields):
#                 field = self.model._fields[field_name]
#                 if field.default is not None or field.compute:
#                     required_fields.remove(field_name)

#             if required_fields:
#                 missing = ", ".join(required_fields)
#                 error_msg = f"Required fields are missing in CSV: {missing}"
#                 send_message(self.env, error_msg, "error", self.user_id)
#                 self.results["error_message"] = error_msg
#                 return None

#             return field_mappings

#         except Exception as e:
#             send_message(
#                 self.env, f"Error mapping fields: {str(e)}", "error", self.user_id
#             )
#             self.results["error_message"] = f"Error mapping fields: {str(e)}"
#             return None

#     def _process_chunk(self, chunk, field_mappings):
#         """Process a chunk of data with optimized batch processing"""
#         results = {"successful": 0, "failed": 0, "duplicates": 0}

#         try:
#             # Convert chunk to list of dicts with appropriate field mapping
#             records = []

#             for _, row in chunk.iterrows():
#                 record = {}
#                 for csv_field, model_field in field_mappings.items():
#                     # Skip empty values
#                     if pd.isna(row[csv_field]) or row[csv_field] == "":
#                         continue

#                     # Get field type and convert value accordingly
#                     field_type = self.model._fields[model_field].type
#                     value = self._convert_value(row[csv_field], field_type, model_field)

#                     if value is not None:
#                         record[model_field] = value

#                 records.append(record)

#             # Check for duplicates
#             clean_records, duplicate_count = self._handle_duplicates(records)
#             results["duplicates"] = duplicate_count

#             # Create records in smaller batches
#             if clean_records:
#                 try:
#                     # Split records into smaller batches for better performance
#                     successful = 0
#                     failed = 0
                    
#                     # Process in smaller batches to prevent oversized SQL queries
#                     for i in range(0, len(clean_records), self.MAX_BATCH_INSERT):
#                         batch = clean_records[i:i + self.MAX_BATCH_INSERT]
#                         try:
#                             created_records = self.model.with_context(
#                                 tracking_disable=True,
#                                 import_file=True,  # Custom context to optimize performance
#                                 # Disable mail thread features for imports
#                                 mail_create_nosubscribe=True,
#                                 mail_create_nolog=True,
#                                 mail_notrack=True,
#                             ).create(batch)
#                             successful += len(created_records)
#                         except Exception as e:
#                             _logger.warning(f"Error creating batch: {str(e)}. Falling back to individual creation.")
#                             # Fallback to creating records individually
#                             for record in batch:
#                                 try:
#                                     self.model.with_context(
#                                         tracking_disable=True,
#                                         import_file=True,
#                                         mail_create_nosubscribe=True,
#                                         mail_create_nolog=True,
#                                         mail_notrack=True,
#                                     ).create([record])
#                                     successful += 1
#                                 except Exception as e2:
#                                     _logger.warning(f"Error creating individual record: {str(e2)}")
#                                     failed += 1
                    
#                     results["successful"] = successful
#                     results["failed"] = failed
                    
#                 except Exception as e:
#                     send_message(
#                         self.env,
#                         f"Error creating records: {str(e)}. Falling back to individual creation.",
#                         "warning",
#                         self.user_id,
#                     )
#                     # Fallback to creating records individually
#                     successful = 0
#                     failed = 0

#                     for record in clean_records:
#                         try:
#                             self.model.with_context(
#                                 tracking_disable=True,
#                                 import_file=True,
#                                 mail_create_nosubscribe=True,
#                                 mail_create_nolog=True,
#                                 mail_notrack=True,
#                             ).create([record])
#                             successful += 1
#                         except Exception as e:
#                             _logger.warning(f"Error creating record: {str(e)}")
#                             failed += 1

#                     results["successful"] = successful
#                     results["failed"] = failed

#             return results

#         except Exception as e:
#             send_message(
#                 self.env, f"Error processing chunk: {str(e)}", "error", self.user_id
#             )
#             results["failed"] = len(chunk)
#             return results

#     def _convert_value(self, value, field_type, field_name):
#         """Convert value to appropriate type for Odoo field"""
#         if value is None or pd.isna(value) or value == "":
#             return None

#         try:
#             # Convert to string first
#             if not isinstance(value, str):
#                 value = str(value)

#             value = value.strip()
            
#             # Special case for 'NULL' string values
#             if value.upper() == 'NULL':
#                 return False  # False will be treated as NULL in SQL

#             if field_type == "char" or field_type == "text":
#                 return value
#             elif field_type == "integer":
#                 # Remove any non-numeric characters except negative sign
#                 clean_value = re.sub(r"[^0-9-]", "", value)
#                 return int(clean_value) if clean_value else 0
#             elif field_type == "float" or field_type == "monetary":
#                 # Replace comma with dot for decimal separator
#                 clean_value = value.replace(",", ".")
#                 # Remove any non-numeric characters except decimal point and negative sign
#                 clean_value = re.sub(r"[^0-9.-]", "", clean_value)
#                 return float(clean_value) if clean_value else 0.0
#             elif field_type == "boolean":
#                 return value.lower() in ("true", "yes", "y", "1", "x")
#             elif field_type == "date":
#                 try:
#                     parsed_date = fields.Date.from_string(value)
#                     return fields.Date.to_string(parsed_date)
#                 except:
#                     # Try pandas date parsing as fallback
#                     return pd.to_datetime(value).strftime("%Y-%m-%d")
#             elif field_type == "datetime":
#                 try:
#                     parsed_datetime = fields.Datetime.from_string(value)
#                     return fields.Datetime.to_string(parsed_datetime)
#                 except:
#                     # Try pandas datetime parsing as fallback
#                     return pd.to_datetime(value).strftime("%Y-%m-%d %H:%M:%S")
#             elif field_type == "many2one":
#                 # Try to lookup the record by name
#                 relation = self.model._fields[field_name].comodel_name
#                 related_model = self.env[relation]

#                 # Try to find by ID first
#                 if value.isdigit():
#                     record = related_model.browse(int(value)).exists()
#                     if record:
#                         return record.id

#                 # Then try to find by name
#                 record = related_model.search([("name", "=", value)], limit=1)
#                 if record:
#                     return record.id

#                 # If still not found, try to find by name case insensitive
#                 record = related_model.search([("name", "=ilike", value)], limit=1)
#                 if record:
#                     return record.id

#                 return None
#             else:
#                 return value

#         except Exception as e:
#             _logger.warning(
#                 f"Error converting value '{value}' for field '{field_name}': {str(e)}"
#             )
#             # Return original value and let Odoo handle conversion errors
#             return value

#     def _handle_duplicates(self, records):
#         """Check for and remove duplicate records"""
#         if not records:
#             return [], 0

#         # Get model's unique fields
#         unique_fields = []
#         for constraint in self.model._sql_constraints:
#             if "unique" in constraint[1].lower():
#                 fields_match = re.search(
#                     r"unique\s*\(([^)]+)\)", constraint[1], re.IGNORECASE
#                 )
#                 if fields_match:
#                     fields = [f.strip() for f in fields_match.group(1).split(",")]
#                     unique_fields.extend(fields)

#         # Handle _rec_name if no unique constraints found
#         if not unique_fields and hasattr(self.model, "_rec_name"):
#             unique_fields.append(self.model._rec_name)

#         # Add 'name' field as fallback if still empty and it exists
#         if not unique_fields and "name" in self.model._fields:
#             unique_fields.append("name")

#         # If still no unique fields, we can't check for duplicates
#         if not unique_fields:
#             send_message(
#                 self.env,
#                 "No unique fields found to check for duplicates",
#                 "warning",
#                 self.user_id,
#             )
#             return records, 0

#         # Check each record against existing database records
#         duplicates = []
#         clean_records = []

#         for record in records:
#             is_duplicate = False

#             # Build domain for potential duplicates
#             domain = []
#             for field in unique_fields:
#                 if field in record:
#                     domain.append((field, "=", record[field]))

#             if domain:
#                 # Combine with OR if multiple unique fields
#                 if len(domain) > 1:
#                     domain = ["|"] * (len(domain) - 1) + domain

#                 # Search for existing record
#                 existing = self.model.search(domain, limit=1)
#                 if existing:
#                     is_duplicate = True
#                     duplicates.append(record)

#             if not is_duplicate:
#                 clean_records.append(record)

#         duplicate_count = len(duplicates)
#         if duplicate_count > 0:
#             send_message(
#                 self.env,
#                 f"Found {duplicate_count} duplicate records that will be skipped",
#                 "warning",
#                 self.user_id,
#             )

#         return clean_records, duplicate_count
