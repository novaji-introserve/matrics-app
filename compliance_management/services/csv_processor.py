import base64
import logging
import os
import re
import pandas as pd
import numpy as np
import chardet
import psycopg2
import random
import string
import time
import json
import math
from io import BytesIO, StringIO
from datetime import datetime
from odoo import _, fields, api
from odoo.exceptions import UserError
from contextlib import contextmanager

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
    """Production-grade CSV Processor with concurrency management and detailed reporting"""

    # Constants for performance tuning
    MAX_BATCH_SIZE = 50000
    SQL_CHUNK_SIZE = 5000  # Smaller chunks for more reliable processing
    TEMP_TABLE_PREFIX = "tmp_import_"
    MAX_RETRIES = 3
    RETRY_DELAY = 1  # seconds
    
    # Performance options
    DISABLE_TRIGGERS = True
    ANALYZE_AFTER_LOAD = True
    MAX_TRANSACTION_SIZE = 10000  # Maximum records per transaction
    
    # Import statistics tracking
    TRACK_FAILURE_REASONS = True
    
    def __init__(self, import_log):
        """Initialize with import log record"""
        self.import_log = import_log
        self.env = import_log.env
        self.cr = self.env.cr
        self.user_id = import_log.uploaded_by.id
        self.model_name = import_log.model_name
        self.model = self.env[self.model_name]
        self.file_path = import_log.file_path
        self.file_name = import_log.file_name
        self.job_id = self.env.context.get('job_uuid', 'unknown')
        
        # Create a job-specific table name to avoid conflicts
        self.table_name = self.model._table
        self.import_id = import_log.id
        clean_job_id = self.job_id.replace('-', '_')
        self.temp_table_name = f"{self.TEMP_TABLE_PREFIX}{self.table_name}_{self.import_id}_{clean_job_id}"[:63]
        # self.temp_table_name = f"{self.TEMP_TABLE_PREFIX}{self.table_name}_{self.import_id}_{self.job_id}"[:63]
        
        # Dynamic schema information
        self.table_columns = {}  # Table column name -> SQL type
        self.field_mappings = {}  # CSV column -> Odoo field
        self.inverse_mappings = {}  # Odoo field -> CSV column
        self.required_fields = []
        self.model_fields = {}  # Complete field definitions from the model
        self.unique_constraints = []
        self.csv_columns = []  # List of columns in the CSV
        
        # Error tracking
        self.failure_reasons = {}  # Category -> count
        self.missing_columns = set()  # Columns in DB that don't exist in CSV
        self.required_missing_columns = set()  # Required columns missing from CSV
        
        # Metrics and results
        self.start_time = None
        self.process_time = 0
        self.results = {
            "success": False,
            "successful": 0,
            "failed": 0,
            "duplicates": 0,
            "error_message": "",
            "technical_details": "",
            "failure_summary": {},
        }

        self.delete_mode = import_log.delete_mode
        self.unique_identifier_field = import_log.unique_identifier_field
        self.delete_progress = None
        if import_log.delete_progress:
            try:
                self.delete_progress = json.loads(import_log.delete_progress)
            except:
                _logger.warning(f"Failed to parse delete progress JSON: {import_log.delete_progress}")

    
    def process_batch(self, start_position, end_position):
        """Process a specific batch of records with proper transaction handling"""
        self.start_time = datetime.now()
        total_records = self.import_log.total_records
        
        try:
            # Validate inputs
            if start_position < 0 or end_position <= start_position:
                raise ValueError(f"Invalid positions: start={start_position}, end={end_position}")
                
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"Import file not found at {self.file_path}")
            
            # Calculate progress percentage
            progress = min(100, (start_position / total_records * 100)) if total_records > 0 else 0
            self._log_message(
                f"Processing records {start_position:,} to {end_position:,}", 
                "info"
            )

            if self.delete_mode:
                self._log_message("Processing in delete mode", "info")
                
                self.df = self._read_file_chunk(start_position, end_position - start_position)
                if self.df is None or self.df.empty:
                    self._log_message("No data found in specified range", "warning")
                    return self.results
                
                self.csv_columns = list(self.df.columns)
                
                self._initialize_dynamic_schema()
                
                self.df = self._preprocess_dataframe(self.df)
                
                deleted_count = self.process_delete_mode()
                
                self.results.update({
                    "success": True,
                    "deleted": deleted_count,
                    "mode": "delete"
                })
                
                self.process_time = (datetime.now() - self.start_time).total_seconds()
                
                return self.results
            
            # Initialize dynamic schema for mapping
            self._initialize_dynamic_schema()
            
            # Create temp table on first batch
            self._create_temp_table()
            
            # Read the specific chunk from the file and store it in instance variable
            self.df = self._read_file_chunk(start_position, end_position - start_position)
            if self.df is None or self.df.empty:
                self._log_message("No data found in specified range", "warning")
                return self.results
            
            # Save CSV columns for reference
            self.csv_columns = list(self.df.columns)
            
            # Log technical details server-side only
            self._log_message(
                f"Processing {len(self.df):,} records with {len(self.csv_columns)} columns", 
                "info", 
                send_to_websocket=False
            )
            
            # Process the data - update with animation and improved progress
            self.df = self._preprocess_dataframe(self.df)
            
            # Show a processing indicator before starting the chunked insert
            self._log_message_no_progress(
                f"⏳ Processing records {start_position:,} to {end_position:,} ({self._get_processing_indicator()})...", 
                "info"
            )
            
            self._chunked_insert_to_temp_table()  # This now uses self.df directly
            self._process_staged_data()
            
            # Calculate metrics
            self.process_time = (datetime.now() - self.start_time).total_seconds()
            
            # Clean up
            try:
                self._cleanup_temp_resources()
            except Exception as e:
                _logger.warning(f"Error cleaning up temp resources: {str(e)}")
            
            # Generate and include summary in results
            self._generate_failure_summary()
            
            # Send user-friendly summary
            self._send_batch_summary()
            
            # Clear the dataframe to free memory
            self.df = None
            
            return self.results
            
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            error_message = f"Error processing batch: {str(e)}"
            _logger.error(error_message)
            _logger.error(error_trace)
            
            try:
                if self._check_cursor_validity():
                    self.cr.rollback()
            except:
                pass
                
            try:
                self._cleanup_temp_resources()
            except:
                pass
            
            # Send user-friendly error message
            user_friendly_error = self._create_user_friendly_error(str(e))
            self._log_message(user_friendly_error, "error")
            
            self.results.update({
                "success": False,
                "error_message": error_message,
                "technical_details": error_trace,
            })
            
            # Clear the dataframe to free memory
            self.df = None
            
            return self.results

    def _initialize_dynamic_schema(self):
        """Initialize dynamic schema information by introspecting the database and model"""
        # Only initialize once
        if self.table_columns:
            return
        
        self._log_message("Analyzing database structure", "info")
        
        # 1. Get table column information from database
        self._introspect_table_schema()
        
        # 2. Get model field information
        self._introspect_model_fields()
        
        # 3. Load or create field mappings
        self._setup_field_mappings()
        
        # 4. Discover unique constraints
        self._discover_unique_constraints()
        
        # Technical log (server only)
        self._log_message(
            f"Schema analysis complete: {len(self.table_columns)} columns, {len(self.field_mappings)} mappings", 
            "info", 
            send_to_websocket=False
        )

    def _introspect_table_schema(self):
        """Dynamically introspect the database table schema"""
        # Get column information from information_schema
        self.cr.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (self.table_name,))
        
        for column_name, data_type, is_nullable, column_default in self.cr.fetchall():
            self.table_columns[column_name] = {
                'name': column_name,
                'type': data_type,
                'nullable': is_nullable == 'YES',
                'default': column_default,
            }
            
        _logger.debug(f"Discovered {len(self.table_columns)} columns in table {self.table_name}")
    
    def _introspect_model_fields(self):
        """Get field information from the Odoo model"""
        # Reset field lists
        self.model_fields = {}
        self.required_fields = []
        
        # Process all fields in the model
        for field_name, field in self.model._fields.items():
            # Skip non-storable or special fields
            if not field.store or field.type in ['one2many']:
                continue
                
            # Store field information
            self.model_fields[field_name] = {
                'name': field_name,
                'type': field.type,
                'required': field.required,
                'readonly': field.readonly,
                'relation': field.comodel_name if hasattr(field, 'comodel_name') else None,
                'column_name': field.name,  # Database column name
                'default': field.default,
                'compute': bool(field.compute),
            }
            
            # Track required fields
            if field.required and not field.default and not field.compute:
                self.required_fields.append(field_name)
    
    def _setup_field_mappings(self):
        """Load existing field mappings or create new ones by sampling the file"""
        # Try to load existing mappings first
        existing_mappings = self.env["import.field.mapping"].search([
            ("import_log_id", "=", self.import_log.id)
        ])
        
        if existing_mappings:
            # Use existing mappings
            self.field_mappings = {}
            self.inverse_mappings = {}
            
            for mapping in existing_mappings:
                self.field_mappings[mapping.csv_field] = mapping.model_field
                self.inverse_mappings[mapping.model_field] = mapping.csv_field
                
            self._log_message(f"Loaded {len(self.field_mappings)} existing field mappings", "info")
            
        else:
            # Create new mappings by sampling the file
            sample_df = self._read_file_chunk(0, 5)
            self._create_field_mappings(sample_df)
            
            if not self.field_mappings:
                raise ValueError("Failed to create field mappings from CSV columns")
    
    def _discover_unique_constraints(self):
        """Discover unique constraints from the database and model"""
        self.unique_constraints = []
        
        # Get constraints from model definition
        for constraint in self.model._sql_constraints:
            if "unique" in constraint[1].lower():
                fields_match = re.search(
                    r"unique\s*\(([^)]+)\)", constraint[1], re.IGNORECASE
                )
                if fields_match:
                    fields = [f.strip() for f in fields_match.group(1).split(",")]
                    self.unique_constraints.append(fields)
        
        # Get constraints from database
        self.cr.execute("""
            SELECT tc.constraint_name, array_agg(kcu.column_name ORDER BY kcu.ordinal_position)
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE'
              AND tc.table_name = %s
            GROUP BY tc.constraint_name
        """, (self.table_name,))
        
        for _, columns in self.cr.fetchall():
            if columns not in self.unique_constraints:
                self.unique_constraints.append(columns)
        
        # Use 'id' as fallback if no constraints found
        if not self.unique_constraints:
            self.unique_constraints.append(['id'])
            
        _logger.info(f"Discovered unique constraints for {self.model_name}: {self.unique_constraints}")
    
    def _create_field_mappings(self, df):
        """Create field mappings between CSV columns and model fields"""
        if df is None or df.empty:
            raise ValueError("Cannot create field mappings from empty DataFrame")
            
        # Get CSV columns
        csv_columns = list(df.columns)
        
        # Create mapping dictionaries
        self.field_mappings = {}
        self.inverse_mappings = {}
        
        # Try different matching strategies
        for csv_column in csv_columns:
            model_field = self._find_matching_field(csv_column)
            
            if model_field:
                self.field_mappings[csv_column] = model_field
                self.inverse_mappings[model_field] = csv_column
        
        # Store mappings in database
        mapping_info = "Field mappings:\n"
        for csv_field, model_field in self.field_mappings.items():
            mapping_info += f"  - {csv_field} => {model_field}\n"
            
            # Get field type
            field_type = self.model_fields.get(model_field, {}).get('type', 'char')
            field_required = model_field in self.required_fields
            
            # Create mapping record
            self.env["import.field.mapping"].create({
                "import_log_id": self.import_log.id,
                "csv_field": csv_field,
                "model_field": model_field,
                "field_type": field_type,
                "required": field_required,
            })
        
        # Commit mappings to database
        self.env.cr.commit()
        
        self._log_message(mapping_info, "info")
        
        # Check for missing required fields
        missing_required = [f for f in self.required_fields if f not in self.inverse_mappings]
        if missing_required:
            self._log_message(f"Required fields missing in CSV: {', '.join(missing_required)}", "warning")
            self.required_missing_columns = set(missing_required)
    
    def _find_matching_field(self, csv_column):
        """Find matching model field for a CSV column using multiple strategies"""
        # Clean the column name
        clean_column = csv_column.lower().strip()
        
        # Strategy 1: Direct match with field name
        if clean_column in self.model_fields:
            return clean_column
            
        # Strategy 2: Match with normalized field name (no spaces/underscores)
        normalized_column = re.sub(r'[\s_-]', '', clean_column)
        for field_name in self.model_fields:
            normalized_field = re.sub(r'[\s_-]', '', field_name.lower())
            if normalized_column == normalized_field:
                return field_name
                
        # Strategy 3: Match with field label (string)
        for field_name, field_info in self.model_fields.items():
            field_string = getattr(self.model._fields[field_name], 'string', '').lower()
            if clean_column == field_string.lower():
                return field_name
                
        # Strategy 4: Match with column name in database
        for field_name, field_info in self.model_fields.items():
            column_name = field_info.get('column_name', '').lower()
            if clean_column == column_name:
                return field_name
                
        # No match found
        return None

    def _quoted_temp_table(self):
        """Return properly quoted temp table name for SQL queries"""
        return self.cr.mogrify(f'"{self.temp_table_name}"', ()).decode('utf-8')
        
    def _create_temp_table(self):
        """Create a temporary table with appropriate schema for staging data"""
        unique_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        self.temp_table_name = f"{self.TEMP_TABLE_PREFIX}{self.table_name}_{self.import_id}_{unique_suffix}"[:63]
        
        for attempt in range(self.MAX_RETRIES):
            try:
                # First check if table exists
                self.cr.execute("""
                    SELECT EXISTS (
                        SELECT FROM pg_tables 
                        WHERE tablename = %s
                    )
                """, (self.temp_table_name,))
                
                if self.cr.fetchone()[0]:
                    # Table already exists (from a previous attempt)
                    return
                
                # Drop table if it exists anyway (belt and suspenders)
                self.cr.execute("DROP TABLE IF EXISTS %s" % self.cr.mogrify(f'"{self.temp_table_name}"', ()).decode('utf-8'))
                
                # Build column definitions based on target table
                # IMPORTANT: All columns explicitly allow NULL values
                column_defs = [
                    "tmp_id SERIAL PRIMARY KEY",
                    "odoo_status VARCHAR(20) DEFAULT 'pending'",
                    "odoo_error TEXT",
                    "odoo_row_index INTEGER"
                ]
                
                # Add all columns from the model, with explicit NULL allowed
                for field_name, field_info in self.model_fields.items():
                    # Skip computed fields without storage
                    if field_info.get('compute') and not field_info.get('store', True):
                        continue
                        
                    sql_type = self._get_sql_type_for_field(field_name)
                    column_defs.append(f'"{field_name}" {sql_type} NULL')  # Explicitly allow NULL
                
                # Create the table with quoted identifiers
                table_identifier = self.cr.mogrify(f'"{self.temp_table_name}"', ()).decode('utf-8')
                create_query = f"""
                    CREATE TABLE {table_identifier} (
                        {', '.join(column_defs)}
                    )
                """
                
                self.cr.execute(create_query)
                self.cr.commit()
                
                _logger.info(f"Created temporary table {self.temp_table_name} with explicit NULL support")
                break
                
            except Exception as e:
                _logger.warning(f"Attempt {attempt+1} to create temp table failed: {str(e)}")
                self.cr.rollback()
                
                if attempt == self.MAX_RETRIES - 1:
                    raise
                
                time.sleep(self.RETRY_DELAY)
    
    def _get_sql_type_for_field(self, field_name):
        """Get appropriate SQL type for an Odoo field"""
        field_info = self.model_fields.get(field_name, {})
        field_type = field_info.get('type', 'char')
        
        # Map Odoo field types to SQL types
        type_map = {
            'char': 'TEXT',
            'text': 'TEXT',
            'html': 'TEXT',
            'integer': 'INTEGER',
            'float': 'DOUBLE PRECISION',
            'monetary': 'NUMERIC',
            'date': 'DATE',
            'datetime': 'TIMESTAMP',
            'boolean': 'BOOLEAN',
            'many2one': 'INTEGER',
            'selection': 'TEXT',
            'binary': 'BYTEA',
        }
        
        return type_map.get(field_type, 'TEXT')
    
    def _read_file_chunk(self, start_row, num_rows):
        """Read a specific chunk from the file with progress indicators"""
        try:
            # Determine file type
            file_ext = os.path.splitext(self.file_path)[1].lower()
            
            # Get total records for progress
            total_records = self.import_log.total_records
            current_position = self.import_log.current_position
            
            # For Excel files
            if file_ext in ('.xlsx', '.xls'):
                engine = "openpyxl" if file_ext == '.xlsx' else "xlrd"
                
                # Need to add 1 to start_row to account for header row in Excel
                df = pd.read_excel(
                    self.file_path,
                    dtype=str,
                    engine=engine,
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
                        engine=engine,
                        keep_default_na=False,
                        nrows=num_rows
                    )
                    
                # Log with accurate progress and record count
                self._log_message_no_progress(
                    f"Read {df.shape[0]:,} rows from file ({start_row + df.shape[0]:,}/{total_records:,} - {((start_row + df.shape[0])/total_records*100):.1f}% complete). Processing...", 
                    "success"
                )
                return df
                
            # For CSV files
            else:
                # Detect encoding first
                with open(self.file_path, 'rb') as f:
                    sample = f.read(min(10000, os.path.getsize(self.file_path)))
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
                            skiprows=range(1, start_row+1),  # Skip header and rows up to start_row
                            nrows=num_rows,
                            low_memory=True,
                            on_bad_lines='skip'
                        )
                        
                        # Check if we have a valid dataframe
                        if df.shape[1] > 1:
                            # Log with accurate progress and record count
                            self._log_message_no_progress(
                                f"Read {df.shape[0]:,} rows from file ({start_row + df.shape[0]:,}/{total_records:,} - {((start_row + df.shape[0])/total_records*100):.1f}% complete). Processing...", 
                                "success"
                            )
                            return df
                    except Exception as e:
                        _logger.debug(f"Failed to read CSV with separator '{sep}': {e}")
                        continue
                
                raise ValueError("Failed to read CSV file with any separator")
                
        except Exception as e:
            self._log_message(f"Error reading file chunk: {str(e)}", "error")
            raise
    
    def _preprocess_dataframe(self, df):
        """Preprocess the dataframe for database insertion with improved required field handling"""
        # Create output dataframe
        result_df = pd.DataFrame()
        
        # Add row index for tracking
        result_df['odoo_row_index'] = df.index + 1
        
        # Step 1: Process columns found in the CSV
        for csv_field, model_field in self.field_mappings.items():
            if csv_field not in df.columns:
                continue
                
            # Get field type
            field_info = self.model_fields.get(model_field, {})
            field_type = field_info.get('type', 'char')
            
            # Transform based on field type
            result_df[model_field] = self._transform_field_values(df[csv_field], field_type)
        
        # Step 2: Track columns in the model but missing in the mapping
        missing_model_fields = set(self.model_fields.keys()) - set(self.field_mappings.values())
        self.missing_columns = missing_model_fields
        
        # Step 3: Add default values for all missing/unmapped columns
        for field_name in missing_model_fields:
            # Add default value for missing field (this ensures COPY will work)
            default_value = self._get_default_value_for_field(field_name)
            result_df[field_name] = default_value
        
        # Step 4: Handle required fields specifically (IMPROVED)
        missing_required_counts = {}
        for field_name in self.required_fields:
            field_info = self.model_fields.get(field_name, {})
            field_type = field_info.get('type', 'char')
            
            # Check if field is missing or has NA values
            if field_name not in result_df.columns:
                # Add entire column with appropriate default
                result_df[field_name] = self._get_default_value_for_field(field_name)
                missing_required_counts[field_name] = len(result_df)
            elif result_df[field_name].isna().any() or (result_df[field_name] == '').any():
                # Fill NA values with default, but keep track of count
                missing_count = result_df[field_name].isna().sum() + (result_df[field_name] == '').sum()
                missing_required_counts[field_name] = missing_count
                
                # For character fields, provide a descriptive default
                if field_type in ['char', 'text', 'html']:
                    # Use a helpful placeholder so users know it's a default
                    default_value = f"Required {field_name}"
                    # Fill missing values
                    result_df[field_name] = result_df[field_name].fillna(default_value)
                    result_df.loc[result_df[field_name] == '', field_name] = default_value
                else:
                    # Use standard default for non-character fields
                    default_value = self._get_default_value_for_field(field_name)
                    result_df[field_name] = result_df[field_name].fillna(default_value)
        
        # Log how many required fields were filled
        for field_name, count in missing_required_counts.items():
            if count > 0:
                self._log_message(f"Filled {count} missing values for required field '{field_name}'", "info")
        
        return result_df
    
    def _transform_field_values(self, series, field_type):
        """Transform a pandas Series according to field type"""
        if field_type == 'integer':
            # Extract digits only
            return series.str.extract(r'(-?\d+)', expand=False)
            
        elif field_type in ('float', 'monetary'):
            # Convert to float
            return series.str.replace(',', '.').str.extract(r'(-?\d+\.?\d*)', expand=False)
            
        elif field_type == 'boolean':
            # Convert various boolean representations
            return series.str.lower().isin(['true', 'yes', 'y', '1', 'x', 'on', 'checked'])
            
        elif field_type == 'date':
            # Convert to date
            try:
                return pd.to_datetime(series, errors='coerce').dt.strftime('%Y-%m-%d')
            except:
                return series
                
        elif field_type == 'datetime':
            # Convert to datetime
            try:
                return pd.to_datetime(series, errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                return series
                
        else:
            # Default: return as is
            return series
    
    def _get_default_value_for_field(self, field_name):
        """Get appropriate default value for a field"""
        field_info = self.model_fields.get(field_name, {})
        field_type = field_info.get('type', 'char')
        
        # Check if field has a defined default
        field_default = field_info.get('default')
        if field_default is not None:
            if callable(field_default):
                try:
                    return field_default()
                except:
                    pass
            else:
                return field_default
        
        # Fall back to type-specific defaults
        if field_type == 'boolean':
            return False
        elif field_type in ('integer', 'float', 'monetary'):
            return None  # Use NULL for numeric fields
        elif field_type == 'date':
            return datetime.now().strftime('%Y-%m-%d')
        elif field_type == 'datetime':
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        elif field_name == 'active':
            return True
        elif field_name == 'create_date' or field_name == 'write_date':
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        elif field_name == 'create_uid' or field_name == 'write_uid':
            return self.env.uid
        elif field_name == 'company_id' and 'company_id' in self.model_fields:
            return self.env.company.id
        elif field_type == 'many2one':
            # Return NULL for many2one fields
            return None
        else:
            # For char/text fields
            return ''
    
    def _stage_data_to_temp_table(self, df):
        """Stage data to temporary table with automatic recovery"""
        if df.empty:
            return
            
        # Make sure temp table exists
        self._create_temp_table()
        
        # Always use chunked insert - most reliable method
        self._chunked_insert_to_temp_table(df)
    
    def _chunked_insert_to_temp_table(self):
        """Insert data in chunks with robust connection management and progress updates"""
        if self.df.empty:
            return
        
        # Get column names
        columns = list(self.df.columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Prepare insert query
        insert_query = f"""
            INSERT INTO {self._quoted_temp_table()}
            ({', '.join([f'"{col}"' for col in columns])})
            VALUES ({placeholders})
        """
        
        # Process in chunks
        total_rows = len(self.df)
        chunk_size = min(self.SQL_CHUNK_SIZE, total_rows)
        
        for start_idx in range(0, total_rows, chunk_size):
            end_idx = min(start_idx + chunk_size, total_rows)
            chunk = self.df.iloc[start_idx:end_idx]
            
            # Replace any NaN/None values with SQL NULL
            clean_chunk = chunk.replace({pd.NA: None, np.nan: None})
            
            # Handle callable objects and None values
            rows = []
            for _, row in clean_chunk.iterrows():
                # Process each value in the row
                processed_row = []
                for val in row:
                    if callable(val):
                        try:
                            processed_row.append(val())  # Execute function
                        except:
                            processed_row.append(None)  # Use NULL if function fails
                    else:
                        processed_row.append(val)
                        
                rows.append(tuple(processed_row))
            
            # Use retries for transaction conflicts
            for retry in range(self.MAX_RETRIES):
                # Check if the cursor is still valid
                if self._check_cursor_validity() is False:
                    # Get a new cursor if needed
                    self._refresh_cursor()
                
                try:
                    # Execute the insert
                    self.cr.executemany(insert_query, rows)
                    
                    # Commit chunk
                    self.cr.commit()
                    break
                    
                except psycopg2.Error as e:
                    _logger.warning(f"Error in chunk insert (attempt {retry+1}): {str(e)}")
                    
                    # Handle connection issues
                    if "connection" in str(e).lower() or "cursor" in str(e).lower():
                        self._refresh_cursor()
                        
                        # If this is the last retry, propagate the error
                        if retry == self.MAX_RETRIES - 1:
                            raise
                    else:
                        # For non-connection errors, try standard rollback
                        try:
                            self.cr.rollback()
                        except Exception as rollback_error:
                            _logger.error(f"Rollback error: {str(rollback_error)}")
                            self._refresh_cursor()
                    
                    if retry == self.MAX_RETRIES - 1:
                        raise
                        
                    # Wait before retry with exponential backoff
                    time.sleep(self.RETRY_DELAY * (2 ** retry))
            
            # Log progress more frequently with processed rows
            total_records = self.import_log.total_records if self.import_log else 0
            current_position = self.import_log.current_position if self.import_log else 0
            
            # Only update on intervals to reduce log spam (every 5% or 10,000 records)
            is_log_interval = (
                end_idx % 10000 < chunk_size or  # Every 10,000 records
                int((end_idx / total_rows) * 20) > int((start_idx / total_rows) * 20)  # Every 5%
            )
            
            if is_log_interval:
                self._log_message(
                    f"Read {end_idx:,} rows from file ({(end_idx/total_rows*100):.1f}% complete). Currently at {current_position+end_idx:,}/{total_records:,} records.", 
                    "info", 
                    send_to_websocket=True
                )
        
        self._log_message(f"Staged {total_rows:,} records to temporary table", "info")
    
    def _check_cursor_validity(self):
        """Check if the current cursor is still valid"""
        if not self.cr:
            return False
            
        try:
            # Try a simple query to check cursor validity
            self.cr.execute("SELECT 1")
            return True
        except Exception as e:
            _logger.warning(f"Cursor validity check failed: {str(e)}")
            return False

    def _refresh_cursor(self):
        """Get a fresh cursor when the old one is invalid"""
        _logger.info("Getting a fresh database cursor")
        
        try:
            # Release savepoints if any are active
            try:
                self.cr.execute("RELEASE ALL")
            except:
                pass
                
            # Try to close the old cursor gracefully
            try:
                self.cr.close()
            except:
                pass
        except:
            # Ignore any errors when closing the old cursor
            pass
            
        # Get a fresh cursor from the registry
        self.cr = self.env.registry.cursor()
        
        # Ensure we have transaction isolation set properly
        try:
            self.cr.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        except:
            _logger.warning("Could not set isolation level on refreshed cursor")
            
        return self.cr

    def _process_staged_data(self):
        """Process staged data with full processing of all records"""
        results = {"successful": 0, "failed": 0, "duplicates": 0}
        
        # Initialize failure tracking
        if self.TRACK_FAILURE_REASONS:
            self.failure_reasons = {
                'missing_required': 0,
                'invalid_format': 0,
                'invalid_relation': 0,
                'duplicate': 0,
                'other': 0
            }
        
        try:
            # Check if the temporary table exists
            self.cr.execute(f"SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = %s)", 
                            (self.temp_table_name,))
            if not self.cr.fetchone()[0]:
                # Table disappeared, likely due to concurrent operation
                self._log_message("Recreation of temp table required", "warning")
                self._create_temp_table()
            
            # Process the data in smaller transaction units
            # 1. Resolve relations
            try:
                self._resolve_relations()
                self.cr.commit()
                _logger.info("Successfully resolved relations")
            except Exception as e:
                self.cr.rollback()
                _logger.error(f"Error resolving relations: {str(e)}")
                raise
                
            # 2. Validate data
            try:
                failed = self._validate_data()
                self.cr.commit()
                _logger.info(f"Data validation completed with {failed} failures")
            except Exception as e:
                self.cr.rollback()
                _logger.error(f"Error validating data: {str(e)}")
                raise
                
            # 3. Detect duplicates
            try:
                duplicates = self._detect_duplicates()
                self.cr.commit()
                _logger.info(f"Duplicate detection completed with {duplicates} duplicates")
            except Exception as e:
                self.cr.rollback()
                _logger.error(f"Error detecting duplicates: {str(e)}")
                raise
                
            # 4. Insert ALL valid records - IMPORTANT: This is where we ensure all records are processed
            try:
                # Double-check how many pending records we have
                self.cr.execute(f"""
                    SELECT COUNT(*) FROM {self._quoted_temp_table()}
                    WHERE odoo_status = 'pending'
                """)
                pending_count = self.cr.fetchone()[0]
                _logger.info(f"Found {pending_count} pending records to insert")
                
                # Insert all valid records
                inserted = self._insert_valid_records()
                
                # Ensure the count is correct by double-checking
                self.cr.execute(f"""
                    SELECT COUNT(*) FROM {self._quoted_temp_table()}
                    WHERE odoo_status = 'success'
                """)
                success_count = self.cr.fetchone()[0]
                
                # If there's a mismatch, log it and use the higher number
                if success_count != inserted:
                    _logger.warning(f"Insertion count mismatch: reported {inserted}, found {success_count} successful records")
                    inserted = max(inserted, success_count)
                
                self.cr.commit()
                _logger.info(f"Successfully inserted {inserted} records")
            except Exception as e:
                self.cr.rollback()
                _logger.error(f"Error inserting records: {str(e)}")
                raise
            
            # Update results
            results["duplicates"] = duplicates
            results["successful"] = inserted
            results["failed"] = failed
            
            # Update overall results
            self.results.update(results)
            self.results["failure_summary"] = self.failure_reasons
            
        except Exception as e:
            _logger.error(f"Error in staged data processing: {str(e)}")
            raise
        
        return results

    def _generate_random_id(self, length=10):
        """Generate a random identifier for savepoints"""
        import random
        import string
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
        
    def _resolve_relations(self):
        """Resolve relations (many2one fields) in the temporary table with proper quoting"""
        for field_name, field_info in self.model_fields.items():
            # Skip fields that aren't many2one or in our mapping
            if field_info.get('type') != 'many2one' or (field_name not in self.inverse_mappings and field_name not in self.missing_columns):
                continue
                
            relation_model = field_info.get('relation')
            if not relation_model:
                continue
                
            # Get related table name
            related_model = self.env[relation_model]
            related_table = related_model._table
            
            # Properly quote all identifiers
            quoted_temp_table = self._quoted_temp_table()
            quoted_field = self.cr.mogrify(f'"{field_name}"', ()).decode('utf-8')
            quoted_related_table = self.cr.mogrify(f'"{related_table}"', ()).decode('utf-8')
            
            # Special handling for system fields
            if field_name in ['create_uid', 'write_uid'] and relation_model == 'res.users':
                # For user fields, match against login (username) since 'name' doesn't exist
                self.cr.execute(f"""
                    UPDATE {quoted_temp_table} t
                    SET {quoted_field} = r.id
                    FROM {quoted_related_table} r
                    WHERE t.{quoted_field}::text = r.login
                    AND t.{quoted_field} IS NOT NULL
                    AND t.{quoted_field}::text != ''
                """)
                
                # Also match by ID for numeric values
                self.cr.execute(f"""
                    UPDATE {quoted_temp_table} t
                    SET {quoted_field} = r.id
                    FROM {quoted_related_table} r
                    WHERE t.{quoted_field}::text ~ '^[0-9]+$'
                    AND t.{quoted_field}::integer = r.id
                    AND t.{quoted_field} IS NOT NULL
                    AND t.{quoted_field}::text != ''
                """)
                
                # Default to current user if not resolved
                self.cr.execute(f"""
                    UPDATE {quoted_temp_table} t
                    SET {quoted_field} = %s
                    WHERE (t.{quoted_field} IS NULL OR t.{quoted_field}::text = '')
                    AND t.odoo_status = 'pending'
                """, (self.env.uid,))
                
                continue
            
            # Standard handling for regular many2one fields
            # Try to resolve by exact name
            self.cr.execute(f"""
                UPDATE {quoted_temp_table} t
                SET {quoted_field} = r.id
                FROM {quoted_related_table} r
                WHERE t.{quoted_field}::text = r.name
                AND t.{quoted_field} IS NOT NULL
                AND t.{quoted_field}::text != ''
            """)
            
            # Try to resolve by ID
            self.cr.execute(f"""
                UPDATE {quoted_temp_table} t
                SET {quoted_field} = r.id
                FROM {quoted_related_table} r
                WHERE t.{quoted_field}::text ~ '^[0-9]+$'
                AND t.{quoted_field}::integer = r.id
                AND t.{quoted_field} IS NOT NULL
                AND t.{quoted_field}::text != ''
            """)
            
            # Try to resolve by case-insensitive name
            self.cr.execute(f"""
                UPDATE {quoted_temp_table} t
                SET {quoted_field} = r.id
                FROM {quoted_related_table} r
                WHERE LOWER(t.{quoted_field}::text) = LOWER(r.name)
                AND t.{quoted_field} IS NOT NULL
                AND t.{quoted_field}::text != ''
                AND t.{quoted_field}::text !~ '^[0-9]+$'
            """)
            
            # Mark records where relation couldn't be resolved (but only for non-NULL values)
            # Use mogrify to safely handle the error message string
            error_message = self.cr.mogrify("Could not resolve relation for field %s: ", (field_name,)).decode('utf-8')
            self.cr.execute(f"""
                UPDATE {quoted_temp_table}
                SET odoo_status = 'error',
                    odoo_error = CONCAT(%s, {quoted_field})
                WHERE {quoted_field} IS NOT NULL
                AND {quoted_field}::text != ''
                AND {quoted_field}::text !~ '^[0-9]+$'
                AND odoo_status = 'pending'
                RETURNING tmp_id
            """, (error_message,))
            
            invalid_relations = len(self.cr.fetchall())
            if invalid_relations > 0 and self.TRACK_FAILURE_REASONS:
                self.failure_reasons['invalid_relation'] += invalid_relations

    def _validate_data(self):
        """Validate data before insertion with proper handling of required fields"""
        total_failures = 0
        quoted_temp_table = self._quoted_temp_table()
        
        # We'll still check for non-character fields that might need validation
        for field_name in self.required_fields:
            # Get field information
            field_info = self.model_fields.get(field_name, {})
            field_type = field_info.get('type', 'char')
            quoted_field = self.cr.mogrify(f'"{field_name}"', ()).decode('utf-8')
            
            # Skip character fields as they're already handled in preprocessing
            if field_type in ('char', 'text', 'html'):
                continue
                
            # For non-character fields (like numeric, date, etc.), validate
            if field_type in ('integer', 'float', 'monetary'):
                error_msg = f'Invalid numeric value for required field: {field_name}'
                
                self.cr.execute(f"""
                    UPDATE {quoted_temp_table}
                    SET odoo_status = 'error',
                        odoo_error = %s
                    WHERE {quoted_field} IS NULL 
                    AND odoo_status = 'pending'
                    RETURNING tmp_id
                """, (error_msg,))
                
                invalid_count = len(self.cr.fetchall())
                if invalid_count > 0:
                    total_failures += invalid_count
                    if self.TRACK_FAILURE_REASONS:
                        self.failure_reasons['missing_required'] += invalid_count
        
        # Validate data types for all fields
        invalid_fields = []
        for field_name, field_info in self.model_fields.items():
            field_type = field_info.get('type')
            
            # Skip fields not in the mapping and not required
            if field_name not in self.inverse_mappings and field_name not in self.required_fields:
                continue
                
            quoted_field = self.cr.mogrify(f'"{field_name}"', ()).decode('utf-8')
            
            if field_type == 'integer':
                error_msg = f'Invalid integer for field {field_name}'
                self.cr.execute(f"""
                    UPDATE {quoted_temp_table}
                    SET odoo_status = 'error',
                        odoo_error = %s
                    WHERE {quoted_field} IS NOT NULL
                    AND {quoted_field}::text != ''
                    AND {quoted_field}::text !~ '^-?[0-9]+$'
                    AND odoo_status = 'pending'
                    RETURNING tmp_id
                """, (error_msg,))
                
                invalid_count = len(self.cr.fetchall())
                if invalid_count > 0:
                    invalid_fields.append(f"{field_name} (should be whole numbers)")
                    total_failures += invalid_count
                    if self.TRACK_FAILURE_REASONS:
                        self.failure_reasons['invalid_format'] += invalid_count
                
            elif field_type in ('float', 'monetary'):
                error_msg = f'Invalid number for field {field_name}'
                self.cr.execute(f"""
                    UPDATE {quoted_temp_table}
                    SET odoo_status = 'error',
                        odoo_error = %s
                    WHERE {quoted_field} IS NOT NULL
                    AND {quoted_field}::text != ''
                    AND {quoted_field}::text !~ r'^-?[0-9]*\.[0-9]*$|^-?[0-9]+$'
                    AND odoo_status = 'pending'
                    RETURNING tmp_id
                """, (error_msg,))
                
                invalid_count = len(self.cr.fetchall())
                if invalid_count > 0:
                    invalid_fields.append(f"{field_name} (should be numbers)")
                    total_failures += invalid_count
                    if self.TRACK_FAILURE_REASONS:
                        self.failure_reasons['invalid_format'] += invalid_count
        
        # Report invalid field formats
        if invalid_fields:
            self._log_message(
                f"Found data format issues in: {', '.join(invalid_fields)}", 
                "warning",
                send_to_websocket=False
            )
            
        return total_failures

    def _detect_duplicates(self):
            """Detect and mark duplicate records using only unique_identifier field"""
            duplicate_count = 0
            quoted_temp_table = self._quoted_temp_table()
            
            try:
                # First check if unique_identifier field exists in model and mapping
                has_unique_identifier = 'unique_identifier' in self.model_fields
                unique_identifier_mapped = 'unique_identifier' in self.inverse_mappings
                
                if has_unique_identifier and unique_identifier_mapped:
                    # Only check for duplicates based on unique_identifier as requested
                    _logger.info("Checking duplicates using only unique_identifier field")
                    
                    # 1. Check for duplicates within the current batch
                    self.cr.execute(f"""
                        UPDATE {quoted_temp_table} t1
                        SET odoo_status = 'duplicate',
                            odoo_error = 'Duplicate unique_identifier in import batch'
                        FROM {quoted_temp_table} t2
                        WHERE t1.tmp_id > t2.tmp_id
                        AND t1."unique_identifier" = t2."unique_identifier"
                        AND t1."unique_identifier" IS NOT NULL
                        AND t1."unique_identifier" != ''
                        AND t1.odoo_status = 'pending'
                        RETURNING t1.tmp_id
                    """)
                    
                    batch_duplicates = len(self.cr.fetchall())
                    if batch_duplicates > 0 and self.TRACK_FAILURE_REASONS:
                        self.failure_reasons['duplicate'] = self.failure_reasons.get('duplicate', 0) + batch_duplicates
                        _logger.info(f"Found {batch_duplicates} duplicates within batch based on unique_identifier")
                    
                    # 2. Check against existing records in the database
                    quoted_table = self.cr.mogrify(f'"{self.table_name}"', ()).decode('utf-8')
                    
                    self.cr.execute(f"""
                        UPDATE {quoted_temp_table} t
                        SET odoo_status = 'duplicate',
                            odoo_error = 'Record with this unique_identifier already exists in database'
                        FROM {quoted_table} m
                        WHERE t."unique_identifier" = m."unique_identifier"
                        AND t."unique_identifier" IS NOT NULL
                        AND t."unique_identifier" != ''
                        AND t.odoo_status = 'pending'
                        RETURNING t.tmp_id
                    """)
                    
                    existing_duplicates = len(self.cr.fetchall())
                    if existing_duplicates > 0 and self.TRACK_FAILURE_REASONS:
                        self.failure_reasons['duplicate'] = self.failure_reasons.get('duplicate', 0) + existing_duplicates
                        _logger.info(f"Found {existing_duplicates} duplicates against database based on unique_identifier")
                    
                    duplicate_count = batch_duplicates + existing_duplicates
                else:
                    # If no unique_identifier field, check a fallback constraint
                    _logger.info("No unique_identifier field found, checking fallback constraints")
                    
                    # Find the shortest unique constraint that has mapped fields
                    best_constraint = None
                    for constraint_fields in self.unique_constraints:
                        mapped_fields = [f for f in constraint_fields if f in self.inverse_mappings.keys()]
                        
                        # Skip empty constraints
                        if not mapped_fields:
                            continue
                            
                        # Use the first valid constraint or the shortest one
                        if best_constraint is None or len(mapped_fields) < len(best_constraint):
                            best_constraint = mapped_fields
                    
                    # If we found a valid constraint, use it
                    if best_constraint:
                        _logger.info(f"Using constraint fields for duplicate detection: {best_constraint}")
                        
                        # 1. Check for duplicates within batch
                        fields_clause = " AND ".join([f't1."{f}" = t2."{f}"' for f in best_constraint])
                        not_null_clause = " AND ".join([f't1."{f}" IS NOT NULL' for f in best_constraint])
                        
                        self.cr.execute(f"""
                            UPDATE {quoted_temp_table} t1
                            SET odoo_status = 'duplicate',
                                odoo_error = 'Duplicate record in import batch'
                            FROM {quoted_temp_table} t2
                            WHERE t1.tmp_id > t2.tmp_id
                            AND {fields_clause}
                            AND {not_null_clause}
                            AND t1.odoo_status = 'pending'
                            RETURNING t1.tmp_id
                        """)
                        
                        batch_duplicates = len(self.cr.fetchall())
                        if batch_duplicates > 0 and self.TRACK_FAILURE_REASONS:
                            self.failure_reasons['duplicate'] = self.failure_reasons.get('duplicate', 0) + batch_duplicates
                        
                        # 2. Check against existing records
                        quoted_table = self.cr.mogrify(f'"{self.table_name}"', ()).decode('utf-8')
                        fields_clause = " AND ".join([f't."{f}" = m."{f}"' for f in best_constraint])
                        not_null_clause = " AND ".join([f't."{f}" IS NOT NULL' for f in best_constraint])
                        
                        self.cr.execute(f"""
                            UPDATE {quoted_temp_table} t
                            SET odoo_status = 'duplicate',
                                odoo_error = 'Record already exists in database'
                            FROM {quoted_table} m
                            WHERE {fields_clause}
                            AND {not_null_clause}
                            AND t.odoo_status = 'pending'
                            RETURNING t.tmp_id
                        """)
                        
                        existing_duplicates = len(self.cr.fetchall())
                        if existing_duplicates > 0 and self.TRACK_FAILURE_REASONS:
                            self.failure_reasons['duplicate'] = self.failure_reasons.get('duplicate', 0) + existing_duplicates
                        
                        duplicate_count = batch_duplicates + existing_duplicates
                    else:
                        _logger.warning("No suitable constraints found for duplicate detection")
                
                # Count total duplicates for reporting
                self.cr.execute(f"""
                    SELECT COUNT(*) FROM {quoted_temp_table}
                    WHERE odoo_status = 'duplicate'
                """)
                total_duplicates = self.cr.fetchone()[0] or 0
                
                if duplicate_count > 0 or total_duplicates > 0:
                    self._log_message(
                        f"Found {total_duplicates:,} duplicate records", 
                        "warning"
                    )
                
                return total_duplicates
                
            except Exception as e:
                _logger.error(f"Error detecting duplicates: {str(e)}")
                # Don't fail the entire process if duplicate detection fails
                self.cr.rollback()
                return 0

    def _check_constraint_duplicates(self, fields):
        """Check for duplicates based on constraint fields with proper quoting"""
        batch_duplicates = 0
        existing_duplicates = 0
        
        # Only proceed if we have fields to check
        if not fields:
            return 0, 0
        
        try:
            quoted_temp_table = self._quoted_temp_table()
            quoted_table = self.cr.mogrify(f'"{self.table_name}"', ()).decode('utf-8')
            
            # Check for duplicates within batch
            fields_clause = " AND ".join([f't1."{f}" = t2."{f}"' for f in fields])
            not_null_clause = " AND ".join([f't1."{f}" IS NOT NULL' for f in fields])
            
            self.cr.execute(f"""
                UPDATE {quoted_temp_table} t1
                SET odoo_status = 'duplicate',
                    odoo_error = 'Duplicate record in import batch'
                FROM {quoted_temp_table} t2
                WHERE t1.tmp_id > t2.tmp_id
                AND {fields_clause}
                AND {not_null_clause}
                AND t1.odoo_status = 'pending'
                RETURNING t1.tmp_id
            """)
            
            batch_duplicates = len(self.cr.fetchall())
            
            # Check against existing records in main table
            fields_clause = " AND ".join([f't."{f}" = m."{f}"' for f in fields])
            not_null_clause = " AND ".join([f't."{f}" IS NOT NULL' for f in fields])
            
            self.cr.execute(f"""
                UPDATE {quoted_temp_table} t
                SET odoo_status = 'duplicate',
                    odoo_error = 'Record already exists in database'
                FROM {quoted_table} m
                WHERE {fields_clause}
                AND {not_null_clause}
                AND t.odoo_status = 'pending'
                RETURNING t.tmp_id
            """)
            
            existing_duplicates = len(self.cr.fetchall())
            
        except Exception as e:
            _logger.error(f"Error checking constraint duplicates: {e}")
            # Don't fail the entire process for duplicate detection issues
            self.cr.rollback()
        
        return batch_duplicates, existing_duplicates

    def _insert_valid_records(self):
        """Insert all valid records without ANY artificial limits"""
        # Get valid fields from model
        valid_fields = set()
        
        # Add all fields from the mapping and required fields
        for field_name in self.model_fields:
            if field_name in self.inverse_mappings or field_name in self.required_fields:
                valid_fields.add(field_name)
        
        if not valid_fields:
            return 0
                
        # Build insert statement with proper quoting
        fields_str = ", ".join([f'"{f}"' for f in valid_fields])
        fields_src = ", ".join([f'src."{f}"' for f in valid_fields])
        
        # For transaction management, process in chunks
        inserted_count = 0
        chunk_size = min(self.MAX_TRANSACTION_SIZE, 5000)  # Use smaller chunks for reliability
        
        # Get count of pending records
        self.cr.execute(f"""
            SELECT COUNT(*) FROM {self._quoted_temp_table()}
            WHERE odoo_status = 'pending'
        """)
        pending_count = self.cr.fetchone()[0]
        
        if pending_count == 0:
            return 0
        
        _logger.info(f"Found {pending_count} pending records to insert")
        total_chunks = math.ceil(pending_count / chunk_size)
        
        # Process ALL pending records - no early termination
        chunk_counter = 0
        processed_count = 0
        
        while processed_count < pending_count:
            # Get next chunk of record IDs to process
            self.cr.execute(f"""
                SELECT tmp_id FROM {self._quoted_temp_table()}
                WHERE odoo_status = 'pending'
                ORDER BY tmp_id
                LIMIT %s
            """, (chunk_size,))
            
            record_ids = [r[0] for r in self.cr.fetchall()]
            
            if not record_ids:
                # No more pending records
                break
            
            chunk_counter += 1
            
            # Process this chunk with retries
            for retry in range(self.MAX_RETRIES):
                # Create a unique savepoint name for this chunk and retry
                import random
                import string
                sp_name = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
                savepoint_name = f"chunk_{chunk_counter}_{retry}_{sp_name}"
                savepoint_created = False
                
                try:
                    # Try to create a savepoint
                    try:
                        # Check if transaction is active first
                        self.cr.execute("SELECT pg_current_xact_id_if_assigned()")
                        if self.cr.fetchone()[0]:
                            self.cr.execute(f"SAVEPOINT {savepoint_name}")
                            savepoint_created = True
                    except Exception as sp_error:
                        _logger.warning(f"Could not create savepoint: {str(sp_error)}")
                    
                    # Direct SQL insert with RETURNING to get IDs
                    quoted_table = self.cr.mogrify(f'"{self.table_name}"', ()).decode('utf-8')
                    
                    # Use a more robust insert with explicit handling of duplicates
                    self.cr.execute(f"""
                        WITH inserted AS (
                            INSERT INTO {quoted_table}
                            ({fields_str})
                            SELECT {fields_src}
                            FROM {self._quoted_temp_table()} src
                            WHERE src.tmp_id IN %s
                            AND src.odoo_status = 'pending'
                            ON CONFLICT DO NOTHING  -- Skip duplicates instead of failing
                            RETURNING id
                        )
                        SELECT COUNT(*) FROM inserted
                    """, (tuple(record_ids),))
                    
                    # Count inserted records
                    chunk_inserted = self.cr.fetchone()[0]
                    inserted_count += chunk_inserted
                    processed_count += len(record_ids)
                    
                    # Detect duplicates that were skipped
                    if chunk_inserted < len(record_ids):
                        # Some records were skipped - update their status to 'duplicate'
                        skipped_count = len(record_ids) - chunk_inserted
                        if self.TRACK_FAILURE_REASONS:
                            self.failure_reasons['duplicate'] = self.failure_reasons.get('duplicate', 0) + skipped_count
                        
                        # Only mark those that are still pending as duplicates
                        self.cr.execute(f"""
                            UPDATE {self._quoted_temp_table()}
                            SET odoo_status = 'duplicate',
                                odoo_error = 'Duplicate record detected during insert'
                            WHERE tmp_id IN %s
                            AND odoo_status = 'pending'
                        """, (tuple(record_ids),))
                    
                    # Mark successful records
                    if chunk_inserted > 0:
                        # Mark explicitly by tmp_id
                        self.cr.execute(f"""
                            UPDATE {self._quoted_temp_table()}
                            SET odoo_status = 'success'
                            WHERE tmp_id IN %s
                            AND odoo_status = 'pending'
                        """, (tuple(record_ids),))
                    
                    # Release savepoint if created
                    if savepoint_created:
                        try:
                            self.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                        except Exception as release_error:
                            _logger.warning(f"Could not release savepoint: {str(release_error)}")
                    
                    # Commit after successful chunk to avoid long transactions
                    self.cr.commit()
                    
                    # Log progress for chunks
                    if chunk_counter % 5 == 0 or chunk_counter == total_chunks:
                        _logger.info(f"Inserted chunk {chunk_counter}/{total_chunks}: {chunk_inserted} records")
                    
                    break  # Successfully processed this chunk, move to next one
                    
                except Exception as e:
                    # Rollback to savepoint if it was created
                    if savepoint_created:
                        try:
                            self.cr.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                        except Exception as rollback_error:
                            _logger.error(f"Error rolling back to savepoint: {str(rollback_error)}")
                            # Do a full rollback if savepoint rollback fails
                            try:
                                self.cr.rollback()
                            except Exception as full_rollback_error:
                                _logger.error(f"Error in full rollback: {str(full_rollback_error)}")
                    else:
                        # No savepoint, do a full rollback
                        try:
                            self.cr.rollback()
                        except Exception as rollback_error:
                            _logger.error(f"Error in rollback: {str(rollback_error)}")
                    
                    # On final retry, mark as errors
                    if retry == self.MAX_RETRIES - 1:
                        error_msg = f"Database error: {str(e)}"
                        
                        # Check for duplicate key error
                        if "duplicate key value violates unique constraint" in str(e):
                            error_msg = "Duplicate record detected that wasn't caught by duplicate detection"
                            # Update statistics
                            if self.TRACK_FAILURE_REASONS:
                                self.failure_reasons['duplicate'] += len(record_ids)
                        
                        try:
                            self.cr.execute(f"""
                                UPDATE {self._quoted_temp_table()}
                                SET odoo_status = 'error',
                                    odoo_error = %s
                                WHERE tmp_id IN %s
                                AND odoo_status = 'pending'
                            """, (error_msg, tuple(record_ids)))
                            
                            self.cr.commit()
                        except Exception as update_error:
                            _logger.error(f"Error updating error status: {str(update_error)}")
                            
                        if self.TRACK_FAILURE_REASONS and "duplicate key" not in str(e):
                            self.failure_reasons['other'] += len(record_ids)
                        
                        # Count these as processed even if they failed
                        processed_count += len(record_ids)
                        break
                        
                    # Try again after delay
                    _logger.warning(f"Retry {retry+1} for insert due to: {str(e)}")
                    time.sleep(self.RETRY_DELAY)
        
        # Log the total records inserted
        _logger.info(f"Successfully inserted {inserted_count} records")
        
        # Double-check: Are there still pending records?
        self.cr.execute(f"""
            SELECT COUNT(*) FROM {self._quoted_temp_table()}
            WHERE odoo_status = 'pending'
        """)
        still_pending = self.cr.fetchone()[0]
        if still_pending > 0:
            _logger.warning(f"Warning: {still_pending} records still pending after processing all chunks")
        
        return inserted_count

    def _generate_failure_summary(self):
        """Generate a detailed summary of failures by category"""
        if not self.TRACK_FAILURE_REASONS:
            return
            
        # Calculate total records processed
        total_processed = (
            self.results.get('successful', 0) + 
            self.results.get('failed', 0) + 
            self.results.get('duplicates', 0)
        )
        
        # Calculate percentages
        if total_processed > 0:
            success_pct = self.results.get('successful', 0) / total_processed * 100
            
            # Add percentage to each failure category
            failure_details = {}
            for category, count in self.failure_reasons.items():
                if count > 0:
                    pct = count / total_processed * 100
                    failure_details[category] = {
                        'count': count,
                        'percentage': round(pct, 1)
                    }
                    
            # Store in results
            self.results['failure_summary'] = {
                'total_processed': total_processed,
                'success_count': self.results.get('successful', 0),
                'success_percentage': round(success_pct, 1),
                'failure_details': failure_details
            }

    def _send_batch_summary(self):
        """Send a user-friendly summary of the batch processing results with accurate counts"""
        # Get key metrics
        success_count = self.results.get('successful', 0)
        duplicate_count = self.results.get('duplicates', 0)
        failed_count = self.results.get('failed', 0)
        total_count = success_count + duplicate_count + failed_count
        process_time = self.process_time
        
        # Log the actual count for debugging
        _logger.info(f"Batch summary - Successful: {success_count}, Duplicates: {duplicate_count}, Failed: {failed_count}, Total: {total_count}")
        
        # Calculate records per second
        if process_time > 0:
            records_per_second = total_count / process_time
        else:
            records_per_second = 0
        
        # Get overall import progress from import log
        try:
            # Need to use a separate cursor to get the latest data
            with self.env.registry.cursor() as progress_cr:
                progress_cr.execute("""
                    SELECT current_position, total_records, 
                        completed_jobs, parallel_jobs,
                        status
                    FROM import_log
                    WHERE id = %s
                """, (self.import_log.id,))
                
                result = progress_cr.fetchone()
                if result:
                    current_position, total_records, completed_jobs, total_jobs, status = result
                    
                    # Check if this is the last job or if we're complete
                    is_complete = (status == 'completed' or completed_jobs + 1 >= total_jobs)
                    
                    # Calculate accurate progress percentage
                    if total_records > 0:
                        # If complete or last job, show 100%
                        if is_complete:
                            overall_progress = 100.0
                        else:
                            # Regular case - calculate actual percentage
                            raw_progress = (current_position / total_records * 100)
                            overall_progress = min(99.9, raw_progress)
                    else:
                        overall_progress = 0
                else:
                    # Fallback if query fails
                    current_position = self.import_log.current_position
                    total_records = self.import_log.total_records
                    overall_progress = (current_position / max(total_records, 1) * 100) if total_records > 0 else 0
        except Exception as e:
            _logger.error(f"Error getting latest progress: {str(e)}")
            # Fallback to basic calculation
            current_position = self.import_log.current_position
            total_records = self.import_log.total_records
            overall_progress = (current_position / max(total_records, 1) * 100) if total_records > 0 else 0
        
        # Prepare summary message
        summary_lines = []
        
        # Success information
        if success_count > 0:
            summary_lines.append(f"✅ Successfully imported {success_count:,} records")
        
        # Duplicate information if any
        if duplicate_count > 0:
            summary_lines.append(f"⚠️ Skipped {duplicate_count:,} duplicate records")
            
        # Failed information if any
        if failed_count > 0:
            summary_lines.append(f"❌ Failed to import {failed_count:,} records")
            
            # Add failure reasons if available
            if self.TRACK_FAILURE_REASONS and self.failure_reasons:
                failure_details = []
                for reason, count in self.failure_reasons.items():
                    if count > 0:
                        # Make the reason more readable
                        readable_reason = reason.replace('_', ' ').title()
                        failure_details.append(f"  • {readable_reason}: {count:,}")
                        
                if failure_details:
                    summary_lines.append("Failure reasons:")
                    summary_lines.extend(failure_details)
        
        # Performance metrics
        if total_count > 0:
            summary_lines.append(f"⏱️ Processed in {process_time:.1f} seconds ({records_per_second:.1f} records/sec)")
            
        # Progress information
        summary_lines.append(f"📊 Overall progress: {overall_progress:.1f}% complete")
            
        # Send the summary as a special type that won't get the percentage appended
        summary_message = "\n".join(summary_lines)
        self._log_message_no_progress(summary_message, "success")

    def _log_message_no_progress(self, message, message_type="info"):
        """Log a message without appending progress percentage"""
        # Log to server
        log_level = {
            "info": _logger.info,
            "error": _logger.error,
            "success": _logger.info,
            "warning": _logger.warning,
        }.get(message_type, _logger.info)
        
        log_message = f"{self.model_name} import (job {self.job_id}): {message}"
        log_level(log_message)
        
        # Send to websocket without appending progress
        try:
            send_message(self.env, message, message_type, self.user_id)
        except Exception as e:
            _logger.warning(f"Failed to send websocket message: {e}")

    def _cleanup_temp_resources(self):
        """Clean up temporary resources with proper quoting"""
        try:
            # Use proper parameter quoting to avoid SQL injection
            self.cr.execute("DROP TABLE IF EXISTS %s" % self.cr.mogrify(f'"{self.temp_table_name}"', ()).decode('utf-8'))
            self.cr.commit()
            _logger.info(f"Dropped temporary table {self.temp_table_name}")
        except Exception as e:
            _logger.warning(f"Error cleaning up temporary table: {str(e)}")

    def _log_message(self, message, message_type="info", send_to_websocket=True):
        """Log a message to both server log and websocket with improved progress information"""
        # Calculate progress
        current_position = self.import_log.current_position
        total_records = self.import_log.total_records
        
        # DON'T add progress info if the message already contains progress information
        has_progress_info = "% complete" in message or "Overall progress" in message
        
        if total_records > 0 and not has_progress_info:
            progress = min(100, (current_position / total_records * 100))
            progress_str = f" ({progress:.1f}% complete)"
        else:
            progress_str = ""
            
        # Create server log message
        log_message = f"{self.model_name} import (job {self.job_id}): {message}"
        
        # Add progress to websocket message if configured and message doesn't have progress
        if send_to_websocket and not has_progress_info:
            full_message = f"{message}{progress_str}"
        else:
            full_message = message if send_to_websocket else log_message
        
        # Always log to server
        log_level = {
            "info": _logger.info,
            "error": _logger.error,
            "success": _logger.info,
            "warning": _logger.warning,
        }.get(message_type, _logger.info)
        
        log_level(log_message)
        
        # Only send user-friendly messages to websocket
        if send_to_websocket:
            try:
                send_message(self.env, full_message, message_type, self.user_id)
            except Exception as e:
                _logger.warning(f"Failed to send websocket message: {e}")

    def _get_processing_indicator(self):
        """Return a visual indicator for ongoing processing"""
        # Simple spinner frames for visual feedback
        spinner_frames = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        
        # Use the current time to pick a frame (changes every 100ms)
        frame_index = int((time.time() * 10) % len(spinner_frames))
        return spinner_frames[frame_index]
    
    def _create_user_friendly_error(self, error_message):
        """Convert technical error messages to user-friendly ones"""
        # Check for common database errors and provide friendly explanations
        if "column r.name does not exist" in error_message:
            return "Unable to process user references in the data."
        
        if "could not serialize access due to concurrent update" in error_message:
            return "Database concurrency issue. Please try again."
            
        if "invalid input syntax for type" in error_message:
            return "The data contains values with incorrect format."
        
        if "duplicate key value violates unique constraint" in error_message:
            return "The data contains duplicate records that cannot be imported."
        
        if "null value in column" in error_message and "violates not-null constraint" in error_message:
            return "Some required fields are missing in the import data."
            
        if "relation" in error_message and "does not exist" in error_message:
            return "One or more related records referenced in the data could not be found."
        
        # For syntax errors in the database
        if "syntax error" in error_message:
            return "A database error occurred during import."
        
        # File related errors
        if "No such file or directory" in error_message:
            return "The import file could not be found."
            
        if "permission denied" in error_message:
            return "Permission denied while accessing the import file."
        
        # For corrupted files
        if "Unicode" in error_message or "codec" in error_message or "encoding" in error_message:
            return "The import file contains invalid characters or encoding issues."
        
        # Generic error message as fallback
        return "An error occurred during the import process. Please contact support for assistance."

    def process_delete_mode(self):
        """Delete records with matching unique identifiers with interruption recovery"""
        if not self.unique_identifier_field:
            raise ValueError("Unique identifier field is required for delete mode")
            
        self._log_message(f"Processing in delete mode with identifier field: {self.unique_identifier_field}", "info")
        
        # Ensure field exists in the model
        if self.unique_identifier_field not in self.model_fields:
            raise ValueError(f"Field '{self.unique_identifier_field}' does not exist in model {self.model_name}")
        
        # Find the CSV column mapped to this field - IMPROVED MAPPING LOGIC
        csv_column = None
        
        # Method 1: Check existing field mappings (most reliable)
        for csv_field, model_field in self.field_mappings.items():
            if model_field == self.unique_identifier_field:
                csv_column = csv_field
                self._log_message(f"Found column '{csv_column}' in field mappings for '{self.unique_identifier_field}'", "info")
                break
        
        # Method 2: If not found in mappings, try case-insensitive search on column names
        if not csv_column or csv_column not in self.df.columns:
            # Normalize field name (remove underscores, lowercase)
            normalized_field = self.unique_identifier_field.lower().replace('_', '')
            
            for col in self.df.columns:
                # Normalize column name the same way
                normalized_col = col.lower().replace('_', '')
                
                # Check if normalized names match
                if normalized_col == normalized_field:
                    csv_column = col
                    self._log_message(f"Found column '{csv_column}' via case-insensitive match for '{self.unique_identifier_field}'", "info")
                    break
                    
        # Method 3: Try partial matching as last resort
        if not csv_column or csv_column not in self.df.columns:
            best_match = None
            best_score = 0
            
            for col in self.df.columns:
                # Convert both to lowercase for comparison
                col_lower = col.lower()
                field_lower = self.unique_identifier_field.lower()
                
                # Check if one contains the other
                if field_lower in col_lower or col_lower in field_lower:
                    # Calculate a simple match score (length of overlap)
                    score = min(len(col_lower), len(field_lower))
                    if score > best_score:
                        best_score = score
                        best_match = col
            
            if best_match:
                csv_column = best_match
                self._log_message(f"Found column '{csv_column}' via partial match for '{self.unique_identifier_field}'", "info")
        
        # Final verification
        if not csv_column or csv_column not in self.df.columns:
            # Generate helpful error message with available columns
            available_columns = ", ".join(self.df.columns[:10])
            if len(self.df.columns) > 10:
                available_columns += f", ... and {len(self.df.columns) - 10} more"
                
            raise ValueError(
                f"Could not find a column matching unique identifier field '{self.unique_identifier_field}'. "
                f"Available columns: {available_columns}. "
                f"Please ensure the unique identifier exists in your CSV file."
            )
        
        self._log_message(f"Using column '{csv_column}' as the unique identifier source", "info")
        
        # Check for existing progress
        processed_values = set()
        if self.delete_progress and self.delete_progress.get('status') == 'in_progress':
            if 'processed_values' in self.delete_progress:
                try:
                    processed_values = set(self.delete_progress['processed_values'])
                    self._log_message(f"Resuming delete operation: {len(processed_values)} values already processed", "info")
                except Exception as e:
                    _logger.warning(f"Error parsing processed values: {str(e)}")
        
        # Get values from the dataframe
        values = self.df[csv_column].dropna().tolist()
        if not values:
            self._log_message("No values found for unique identifier field", "warning")
            return 0
            
        # Clean values and remove empty strings
        identifier_values = [str(v).strip() for v in values if str(v).strip()]
        if not identifier_values:
            self._log_message("No valid values found for unique identifier field", "warning")
            return 0
            
        # Remove already processed values
        if processed_values:
            identifier_values = [v for v in identifier_values if v not in processed_values]
            
        if not identifier_values:
            self._log_message("All values have already been processed", "info")
            return 0
        
        # Log the number of unique values
        self._log_message(f"Found {len(identifier_values)} unique values for deletion", "info")
        
        # Initialize progress tracking without processed values to avoid memory issues
        # We'll track processed values only in batches
        if not self.delete_progress:
            self.delete_progress = {
                'total': len(identifier_values),
                'processed': 0,
                'deleted': 0,
                'failed': 0,
                'status': 'in_progress',
                'processed_values': []
            }
        else:
            self.delete_progress['total'] = self.delete_progress.get('total', 0) + len(identifier_values)
            self.delete_progress['status'] = 'in_progress'
            if 'processed_values' not in self.delete_progress:
                self.delete_progress['processed_values'] = []
        
        # Store initial progress
        self._update_delete_progress()
        
        # Process in chunks to avoid memory issues
        chunk_size = 100  # Smaller chunks for better reliability
        deleted_count = self.delete_progress.get('deleted', 0)
        failed_count = self.delete_progress.get('failed', 0)
        
        # Process in chunks with independent transactions
        for i in range(0, len(identifier_values), chunk_size):
            chunk = identifier_values[i:i+chunk_size]
            chunk_processed = False
            retries = 0
            max_retries = 3
            
            # Retry logic for each chunk
            while not chunk_processed and retries < max_retries:
                # Use a new cursor for each chunk to isolate transactions
                with self.env.registry.cursor() as chunk_cr:
                    try:
                        # Create environment with new cursor
                        chunk_env = api.Environment(chunk_cr, self.env.uid, self.env.context)
                        model_obj = chunk_env[self.model_name]
                        
                        # Build the domain for deletion
                        domain = [(self.unique_identifier_field, 'in', chunk)]
                        
                        # Get records to delete
                        records = model_obj.search(domain)
                        record_count = len(records)
                        
                        if records:
                            # Store IDs before deletion for confirmation
                            record_ids = records.ids
                            
                            # Delete the records
                            records.unlink()
                            
                            # Verify deletion was successful
                            remaining = model_obj.search([('id', 'in', record_ids)])
                            if remaining:
                                # Some records weren't deleted
                                failed_ids = remaining.ids
                                failed_count += len(failed_ids)
                                deleted_count += (record_count - len(failed_ids))
                                self._log_message(f"Failed to delete {len(failed_ids)} records", "warning")
                            else:
                                # All records were deleted
                                deleted_count += record_count
                                self._log_message(f"Deleted {record_count} records", "success")
                        else:
                            self._log_message(f"No records found matching {len(chunk)} values", "info")
                        
                        # Track current processed batch separately from the main progress
                        # This avoids serialization errors by keeping batch updates small
                        batch_progress = {
                            'total': self.delete_progress['total'],
                            'processed': self.delete_progress.get('processed', 0) + len(chunk),
                            'deleted': deleted_count,
                            'failed': failed_count,
                            'status': 'in_progress',
                            'processed_values': chunk  # Only include current chunk
                        }
                        
                        # Store current batch progress
                        self.delete_progress = batch_progress
                        self._update_delete_progress()
                        
                        # Commit this chunk's transaction
                        chunk_cr.commit()
                        chunk_processed = True
                        
                        # Log progress
                        current_position = i + len(chunk)
                        progress_pct = round(current_position / len(identifier_values) * 100, 1)
                        self._log_message(f"Processed {current_position} of {len(identifier_values)} values ({progress_pct}% complete)", "info")
                        
                    except Exception as e:
                        # Rollback this chunk
                        chunk_cr.rollback()
                        
                        retries += 1
                        if retries >= max_retries:
                            # Mark as failed after max retries
                            failed_count += len(chunk)
                            error_msg = f"Error deleting records after {max_retries} attempts: {str(e)}"
                            self._log_message(error_msg, "error")
                        else:
                            # Log retry attempt
                            self._log_message(f"Retry {retries}/{max_retries} for chunk {i//chunk_size + 1}: {str(e)}", "warning")
                            time.sleep(1 * retries)  # Increasing delay between retries
        
        # Mark as completed with a final update
        final_progress = {
            'total': self.delete_progress.get('total', len(identifier_values)),
            'processed': len(identifier_values),
            'deleted': deleted_count,
            'failed': failed_count,
            'status': 'completed',
            'processed_values': []  # Don't store all values in the final summary
        }
        
        self.delete_progress = final_progress
        
        # Use a separate transaction for the final update
        with self.env.registry.cursor() as final_cr:
            try:
                # Create environment with new cursor
                final_env = api.Environment(final_cr, self.env.uid, self.env.context)
                
                # Store the final progress
                final_import_log = final_env['import.log'].browse(self.import_log.id)
                final_import_log.write({
                    'delete_progress': json.dumps(final_progress)
                })
                
                # Store a human-readable summary
                summary = {
                    "operation": "delete",
                    "identifier_field": self.unique_identifier_field,
                    "values_processed": len(identifier_values),
                    "records_deleted": deleted_count,
                    "records_failed": failed_count,
                    "completion_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                final_import_log.write({
                    'summary': json.dumps(summary)
                })
                
                final_cr.commit()
            except Exception as e:
                final_cr.rollback()
                _logger.error(f"Error updating final delete progress: {str(e)}")
        
        total_summary = f"Delete operation completed: {deleted_count} records deleted, {failed_count} failed"
        self._log_message(total_summary, "success")
        
        return deleted_count

    def _update_delete_progress(self):
        """Update the delete progress in the import log with robust concurrency handling"""
        if not self.delete_progress:
            return
            
        # Create a separate progress summary without the large processed_values list
        try:
            # Make a copy without the potentially large processed_values list for the summary
            progress_summary = dict(self.delete_progress)
            if 'processed_values' in progress_summary:
                # Just store the count in the summary to avoid huge JSON
                progress_summary['processed_value_count'] = len(progress_summary['processed_values'])
                del progress_summary['processed_values']
            
            # Use advisory locks and retries for atomic updates
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    # Create a new cursor for isolated transaction
                    with self.env.registry.cursor() as update_cr:
                        # Get a unique lock ID based on import log ID
                        lock_id = self.import_log.id + 50000000  # Different offset from other locks
                        
                        # Try to acquire advisory lock (non-blocking first attempt)
                        update_cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (lock_id,))
                        lock_acquired = update_cr.fetchone()[0]
                        
                        if not lock_acquired:
                            # If lock not acquired, try a blocking acquire with timeout
                            _logger.info(f"Waiting for lock to update delete progress (attempt {attempt+1})")
                            
                            # Set statement timeout to 2 seconds to avoid hanging
                            update_cr.execute("SET LOCAL statement_timeout = 2000")
                            
                            try:
                                # Try blocking lock with timeout
                                update_cr.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
                            except Exception as lock_error:
                                if "statement timeout" in str(lock_error):
                                    # Timeout is fine, we'll retry
                                    _logger.info("Lock acquisition timed out, will retry")
                                    time.sleep(0.5 * (attempt + 1))  # Backoff
                                    continue
                                else:
                                    # Other error, propagate
                                    raise
                        
                        # First check if record still exists and get current progress
                        update_cr.execute("""
                            SELECT id, delete_progress 
                            FROM import_log 
                            WHERE id = %s
                            FOR UPDATE
                        """, (self.import_log.id,))
                        
                        result = update_cr.fetchone()
                        if not result:
                            _logger.warning(f"Import log {self.import_log.id} not found during progress update")
                            return
                            
                        # If there's existing progress, merge it intelligently
                        current_id, current_progress_json = result
                        if current_progress_json:
                            try:
                                current_progress = json.loads(current_progress_json)
                                
                                # Keep processed values from current progress if our list is empty
                                if ('processed_values' not in self.delete_progress or 
                                    not self.delete_progress['processed_values']) and 'processed_values' in current_progress:
                                    self.delete_progress['processed_values'] = current_progress['processed_values']
                                    
                                # Add existing processed values to our list (avoid duplicates)
                                elif 'processed_values' in current_progress:
                                    current_set = set(current_progress['processed_values'])
                                    if 'processed_values' in self.delete_progress:
                                        new_set = set(self.delete_progress['processed_values'])
                                        combined = list(current_set.union(new_set))
                                        self.delete_progress['processed_values'] = combined
                            except Exception as e:
                                _logger.warning(f"Error merging progress data: {str(e)}")
                        
                        # Serialize delete progress for storage (with processed values)
                        progress_json = json.dumps(self.delete_progress)
                        
                        # Store delete progress with safe SQL update
                        update_cr.execute("""
                            UPDATE import_log
                            SET delete_progress = %s
                            WHERE id = %s
                        """, (progress_json, self.import_log.id))
                        
                        # Commit the transaction
                        update_cr.commit()
                        return
                except Exception as e:
                    _logger.warning(f"Error updating delete progress (attempt {attempt+1}/{max_retries}): {str(e)}")
                    time.sleep(0.5 * (2 ** attempt))  # Exponential backoff
                    
            # If all retries failed, log error but continue
            _logger.error(f"Failed to update delete progress after {max_retries} attempts")
        except Exception as e:
            _logger.error(f"Error in _update_delete_progress: {str(e)}")

