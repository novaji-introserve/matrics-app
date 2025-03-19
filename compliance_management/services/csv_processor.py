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

    def process_batch(self, start_position, end_position):
        """Process a specific batch of records with robust connection handling and visual indicators"""
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

    # def process_batch(self, start_position, end_position):
    #     """Process a specific batch of records with robust connection handling"""
    #     self.start_time = datetime.now()
    #     total_records = self.import_log.total_records
        
    #     try:
    #         # Validate inputs
    #         if start_position < 0 or end_position <= start_position:
    #             raise ValueError(f"Invalid positions: start={start_position}, end={end_position}")
                
    #         if not os.path.exists(self.file_path):
    #             raise FileNotFoundError(f"Import file not found at {self.file_path}")
            
    #         # Calculate progress percentage
    #         progress = min(100, (start_position / total_records * 100)) if total_records > 0 else 0
    #         self._log_message(
    #             f"Processing records {start_position:,} to {end_position:,}", 
    #             "info"
    #         )
            
    #         # Initialize dynamic schema for mapping
    #         self._initialize_dynamic_schema()
            
    #         # Create temp table on first batch
    #         self._create_temp_table()
            
    #         # Read the specific chunk from the file and store it in instance variable
    #         self.df = self._read_file_chunk(start_position, end_position - start_position)
    #         if self.df is None or self.df.empty:
    #             self._log_message("No data found in specified range", "warning")
    #             return self.results
            
    #         # Save CSV columns for reference
    #         self.csv_columns = list(self.df.columns)
            
    #         # Log technical details server-side only
    #         self._log_message(
    #             f"Processing {len(self.df)} records with {len(self.csv_columns)} columns", 
    #             "info", 
    #             send_to_websocket=False
    #         )
            
    #         # Process the data
    #         self.df = self._preprocess_dataframe(self.df)
    #         self._chunked_insert_to_temp_table()  # This now uses self.df directly
    #         self._process_staged_data()
            
    #         # Calculate metrics
    #         self.process_time = (datetime.now() - self.start_time).total_seconds()
            
    #         # Calculate new progress 
    #         new_progress = min(100, (end_position / total_records * 100)) if total_records > 0 else 0
            
    #         # Clean up
    #         try:
    #             self._cleanup_temp_resources()
    #         except Exception as e:
    #             _logger.warning(f"Error cleaning up temp resources: {str(e)}")
            
    #         # Generate and include summary in results
    #         self._generate_failure_summary()
            
    #         # Send user-friendly summary
    #         self._send_batch_summary()
            
    #         # Clear the dataframe to free memory
    #         self.df = None
            
    #         return self.results
            
    #     except Exception as e:
    #         import traceback
    #         error_trace = traceback.format_exc()
    #         error_message = f"Error processing batch: {str(e)}"
    #         _logger.error(error_message)
    #         _logger.error(error_trace)
            
    #         try:
    #             if self._check_cursor_validity():
    #                 self.cr.rollback()
    #         except:
    #             pass
                
    #         try:
    #             self._cleanup_temp_resources()
    #         except:
    #             pass
            
    #         # Send user-friendly error message
    #         user_friendly_error = self._create_user_friendly_error(str(e))
    #         self._log_message(user_friendly_error, "error")
            
    #         self.results.update({
    #             "success": False,
    #             "error_message": error_message,
    #             "technical_details": error_trace,
    #         })
            
    #         # Clear the dataframe to free memory
    #         self.df = None
            
    #         return self.results

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

    # def _read_file_chunk(self, start_row, num_rows):
    #     """Read a specific chunk from the file with optimized settings"""
    #     try:
    #         # Determine file type
    #         file_ext = os.path.splitext(self.file_path)[1].lower()
            
    #         # For Excel files
    #         if file_ext in ('.xlsx', '.xls'):
    #             engine = "openpyxl" if file_ext == '.xlsx' else "xlrd"
                
    #             # Need to add 1 to start_row to account for header row in Excel
    #             df = pd.read_excel(
    #                 self.file_path,
    #                 dtype=str,
    #                 engine=engine,
    #                 keep_default_na=False,
    #                 skiprows=start_row+1,  # +1 for header
    #                 nrows=num_rows
    #             )
                
    #             # If this is the first chunk, check if we have at least one valid row
    #             if start_row == 0 and (df.empty or df.shape[0] == 0):
    #                 # Try without skipping rows in case the file has no header
    #                 df = pd.read_excel(
    #                     self.file_path,
    #                     dtype=str,
    #                     engine=engine,
    #                     keep_default_na=False,
    #                     nrows=num_rows
    #                 )
                    
    #             self._log_message(f"Read {df.shape[0]} rows from file", "success")
    #             return df
                
    #         # For CSV files
    #         else:
    #             # Detect encoding first
    #             with open(self.file_path, 'rb') as f:
    #                 sample = f.read(min(10000, os.path.getsize(self.file_path)))
    #                 detection = chardet.detect(sample)
    #                 encoding = detection["encoding"] or "utf-8"
                
    #             # Try different separators if needed
    #             for sep in [',', ';', '\t', '|']:
    #                 try:
    #                     # Skip to the start_row (add 1 for header)
    #                     df = pd.read_csv(
    #                         self.file_path,
    #                         dtype=str,
    #                         encoding=encoding,
    #                         sep=sep,
    #                         keep_default_na=False,
    #                         skiprows=range(1, start_row+1),  # Skip header and rows up to start_row
    #                         nrows=num_rows,
    #                         low_memory=True,
    #                         on_bad_lines='skip'
    #                     )
                        
    #                     # Check if we have a valid dataframe
    #                     if df.shape[1] > 1:
    #                         self._log_message(f"Read {df.shape[0]} rows from file", "success")
    #                         return df
    #                 except Exception as e:
    #                     _logger.debug(f"Failed to read CSV with separator '{sep}': {e}")
    #                     continue
                
    #             raise ValueError("Failed to read CSV file with any separator")
                
    #     except Exception as e:
    #         self._log_message(f"Error reading file chunk: {str(e)}", "error")
    #         raise
    
    def _preprocess_dataframe(self, df):
        """Preprocess the dataframe for database insertion"""
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
        
        # Step 4: Handle required fields specifically
        for field_name in self.required_fields:
            if field_name not in result_df.columns or result_df[field_name].isna().any():
                # Add default value for required field
                default_value = self._get_default_value_for_field(field_name)
                result_df[field_name] = default_value
        
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

    # def _chunked_insert_to_temp_table(self):
    #     """Insert data in chunks with robust connection management"""
    #     if self.df.empty:
    #         return
        
    #     # Get column names
    #     columns = list(self.df.columns)
    #     placeholders = ', '.join(['%s'] * len(columns))
        
    #     # Prepare insert query
    #     insert_query = f"""
    #         INSERT INTO {self._quoted_temp_table()}
    #         ({', '.join([f'"{col}"' for col in columns])})
    #         VALUES ({placeholders})
    #     """
        
    #     # Process in chunks
    #     total_rows = len(self.df)
    #     chunk_size = min(self.SQL_CHUNK_SIZE, total_rows)
        
    #     for start_idx in range(0, total_rows, chunk_size):
    #         end_idx = min(start_idx + chunk_size, total_rows)
    #         chunk = self.df.iloc[start_idx:end_idx]
            
    #         # Replace any NaN/None values with SQL NULL
    #         clean_chunk = chunk.replace({pd.NA: None, np.nan: None})
            
    #         # Handle callable objects and None values
    #         rows = []
    #         for _, row in clean_chunk.iterrows():
    #             # Process each value in the row
    #             processed_row = []
    #             for val in row:
    #                 if callable(val):
    #                     try:
    #                         processed_row.append(val())  # Execute function
    #                     except:
    #                         processed_row.append(None)  # Use NULL if function fails
    #                 else:
    #                     processed_row.append(val)
                        
    #             rows.append(tuple(processed_row))
            
    #         # Use retries for transaction conflicts
    #         for retry in range(self.MAX_RETRIES):
    #             # Check if the cursor is still valid
    #             if self._check_cursor_validity() is False:
    #                 # Get a new cursor if needed
    #                 self._refresh_cursor()
                
    #             try:
    #                 # Execute the insert
    #                 self.cr.executemany(insert_query, rows)
                    
    #                 # Commit chunk
    #                 self.cr.commit()
    #                 break
                    
    #             except psycopg2.Error as e:
    #                 _logger.warning(f"Error in chunk insert (attempt {retry+1}): {str(e)}")
                    
    #                 # Handle connection issues
    #                 if "connection" in str(e).lower() or "cursor" in str(e).lower():
    #                     self._refresh_cursor()
                        
    #                     # If this is the last retry, propagate the error
    #                     if retry == self.MAX_RETRIES - 1:
    #                         raise
    #                 else:
    #                     # For non-connection errors, try standard rollback
    #                     try:
    #                         self.cr.rollback()
    #                     except Exception as rollback_error:
    #                         _logger.error(f"Rollback error: {str(rollback_error)}")
    #                         self._refresh_cursor()
                    
    #                 if retry == self.MAX_RETRIES - 1:
    #                     raise
                        
    #                 # Wait before retry with exponential backoff
    #                 time.sleep(self.RETRY_DELAY * (2 ** retry))
            
    #         # Log progress
    #         self._log_message(
    #             f"Processed chunk {start_idx+1:,}-{end_idx:,} of {total_rows:,} records", 
    #             "info", 
    #             send_to_websocket=False
    #         )
        
    #     self._log_message(f"Staged {total_rows:,} records to temporary table", "info")
    
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
        """Process staged data without using savepoints to avoid transaction issues"""
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
                
            # 4. Insert valid records
            try:
                inserted = self._insert_valid_records()
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

    # def _process_staged_data(self):
    #     """Process staged data with robust savepoint management"""
    #     results = {"successful": 0, "failed": 0, "duplicates": 0}
        
    #     # Initialize failure tracking
    #     if self.TRACK_FAILURE_REASONS:
    #         self.failure_reasons = {
    #             'missing_required': 0,
    #             'invalid_format': 0,
    #             'invalid_relation': 0,
    #             'duplicate': 0,
    #             'other': 0
    #         }
        
    #     # Generate a unique savepoint name but track it in the instance
    #     self.current_savepoint = f"csv_sp_{self._generate_random_id(10)}"
    #     savepoint_created = False
        
    #     try:
    #         # Check if transaction is active before creating savepoint
    #         self.cr.execute("SELECT pg_current_xact_id_if_assigned()")
    #         if self.cr.fetchone()[0]:
    #             self.cr.execute(f"SAVEPOINT {self.current_savepoint}")
    #             savepoint_created = True
    #             _logger.info(f"Created savepoint {self.current_savepoint}")
    #         else:
    #             _logger.warning("No active transaction, skipping savepoint creation")
            
    #         # Verify table exists after serialization conflicts
    #         self.cr.execute(f"SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = %s)", 
    #                         (self.temp_table_name,))
    #         if not self.cr.fetchone()[0]:
    #             # Table disappeared, likely due to concurrent operation
    #             self._log_message("Recreation of temp table required", "warning")
    #             self._create_temp_table()
            
    #         # Process the data
    #         self._resolve_relations()
    #         failed = self._validate_data()
    #         duplicates = self._detect_duplicates()
    #         inserted = self._insert_valid_records()
            
    #         # Update results
    #         results["duplicates"] = duplicates
    #         results["successful"] = inserted
    #         results["failed"] = failed
            
    #         # Release savepoint if we created one - using the instance variable
    #         if savepoint_created and self.current_savepoint:
    #             try:
    #                 self.cr.execute(f"RELEASE SAVEPOINT {self.current_savepoint}")
    #                 _logger.info(f"Released savepoint {self.current_savepoint}")
    #                 # Clear the savepoint name after successful release
    #                 self.current_savepoint = None
    #             except Exception as e:
    #                 _logger.warning(f"Could not release savepoint: {str(e)}")
            
    #         # Commit changes
    #         self.cr.commit()
            
    #         # Update overall results
    #         self.results.update(results)
    #         self.results["failure_summary"] = self.failure_reasons
            
    #     except Exception as e:
    #         # Try to rollback to savepoint if we created one
    #         if savepoint_created and self.current_savepoint:
    #             try:
    #                 self.cr.execute(f"ROLLBACK TO SAVEPOINT {self.current_savepoint}")
    #                 _logger.info(f"Rolled back to savepoint {self.current_savepoint}")
    #             except Exception as rollback_error:
    #                 _logger.error(f"Error rolling back to savepoint: {str(rollback_error)}")
    #                 # If rollback to savepoint fails, try a full rollback
    #                 try:
    #                     self.cr.rollback()
    #                 except:
    #                     pass
    #         else:
    #             # No savepoint, do a full rollback
    #             try:
    #                 self.cr.rollback()
    #             except Exception as rollback_error:
    #                 _logger.error(f"Error in rollback: {str(rollback_error)}")
            
    #         _logger.error(f"Error in staged data processing: {str(e)}")
    #         raise
        
    #     return results

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
    
    # def _resolve_relations(self):
    #     """Resolve relations (many2one fields) in the temporary table"""
    #     for field_name, field_info in self.model_fields.items():
    #         # Skip fields that aren't many2one or in our mapping
    #         if field_info.get('type') != 'many2one' or (field_name not in self.inverse_mappings and field_name not in self.missing_columns):
    #             continue
                
    #         relation_model = field_info.get('relation')
    #         if not relation_model:
    #             continue
                
    #         # Get related table name
    #         related_model = self.env[relation_model]
    #         related_table = related_model._table
            
    #         # Special handling for system fields
    #         if field_name in ['create_uid', 'write_uid'] and relation_model == 'res.users':
    #             # For user fields, match against login (username) since 'name' doesn't exist
    #             self.cr.execute(f"""
    #                 UPDATE {self._quoted_temp_table()} t
    #                 SET {field_name} = r.id
    #                 FROM {related_table} r
    #                 WHERE t.{field_name}::text = r.login
    #                   AND t.{field_name} IS NOT NULL
    #                   AND t.{field_name}::text != ''
    #             """)
                
    #             # Also match by ID for numeric values
    #             self.cr.execute(f"""
    #                 UPDATE {self._quoted_temp_table()} t
    #                 SET {field_name} = r.id
    #                 FROM {related_table} r
    #                 WHERE t.{field_name}::text ~ '^[0-9]+$'
    #                   AND t.{field_name}::integer = r.id
    #                   AND t.{field_name} IS NOT NULL
    #                   AND t.{field_name}::text != ''
    #             """)
                
    #             # Default to current user if not resolved
    #             self.cr.execute(f"""
    #                 UPDATE {self._quoted_temp_table()} t
    #                 SET {field_name} = %s
    #                 WHERE (t.{field_name} IS NULL OR t.{field_name}::text = '')
    #                   AND t.odoo_status = 'pending'
    #             """, (self.env.uid,))
                
    #             continue
            
    #         # Standard handling for regular many2one fields
    #         # Try to resolve by exact name
    #         self.cr.execute(f"""
    #             UPDATE {self._quoted_temp_table()} t
    #             SET {field_name} = r.id
    #             FROM {related_table} r
    #             WHERE t.{field_name}::text = r.name
    #               AND t.{field_name} IS NOT NULL
    #               AND t.{field_name}::text != ''
    #         """)
            
    #         # Try to resolve by ID
    #         self.cr.execute(f"""
    #             UPDATE {self._quoted_temp_table()} t
    #             SET {field_name} = r.id
    #             FROM {related_table} r
    #             WHERE t.{field_name}::text ~ '^[0-9]+$'
    #               AND t.{field_name}::integer = r.id
    #               AND t.{field_name} IS NOT NULL
    #               AND t.{field_name}::text != ''
    #         """)
            
    #         # Try to resolve by case-insensitive name
    #         self.cr.execute(f"""
    #             UPDATE {self._quoted_temp_table()} t
    #             SET {field_name} = r.id
    #             FROM {related_table} r
    #             WHERE LOWER(t.{field_name}::text) = LOWER(r.name)
    #               AND t.{field_name} IS NOT NULL
    #               AND t.{field_name}::text != ''
    #               AND t.{field_name}::text !~ '^[0-9]+$'
    #         """)
            
    #         # Mark records where relation couldn't be resolved (but only for non-NULL values)
    #         self.cr.execute(f"""
    #             UPDATE {self._quoted_temp_table()}
    #             SET odoo_status = 'error',
    #                 odoo_error = CONCAT('Could not resolve relation for field {field_name}: ', {field_name})
    #             WHERE {field_name} IS NOT NULL
    #               AND {field_name}::text != ''
    #               AND {field_name}::text !~ '^[0-9]+$'
    #               AND odoo_status = 'pending'
    #             RETURNING tmp_id
    #         """)
            
    #         invalid_relations = len(self.cr.fetchall())
    #         if invalid_relations > 0 and self.TRACK_FAILURE_REASONS:
    #             self.failure_reasons['invalid_relation'] += invalid_relations
    
    def _validate_data(self):
        """Validate data before insertion with proper quoting"""
        total_failures = 0
        quoted_temp_table = self._quoted_temp_table()
        
        # Check required fields
        missing_required = []
        for field_name in self.required_fields:
            quoted_field = self.cr.mogrify(f'"{field_name}"', ()).decode('utf-8')
            error_msg = f'Missing required field: {field_name}'
            
            self.cr.execute(f"""
                UPDATE {quoted_temp_table}
                SET odoo_status = 'error',
                    odoo_error = %s
                WHERE ({quoted_field} IS NULL OR {quoted_field} = '')
                  AND odoo_status = 'pending'
                RETURNING tmp_id
            """, (error_msg,))
            
            missing_count = len(self.cr.fetchall())
            if missing_count > 0:
                missing_required.append(field_name)
                total_failures += missing_count
                if self.TRACK_FAILURE_REASONS:
                    self.failure_reasons['missing_required'] += missing_count
        
        # Report missing required fields
        if missing_required:
            self._log_message(
                f"Some required fields are missing: {', '.join(missing_required)}", 
                "warning"
            )
        
        # Validate data types
        invalid_fields = []
        for field_name, field_info in self.model_fields.items():
            field_type = field_info.get('type')
            
            # Only validate fields from CSV or critical fields
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
                      AND {quoted_field}::text !~ '^-?[0-9]*\.?[0-9]*$'
                      AND odoo_status = 'pending'
                    RETURNING tmp_id
                """, (error_msg,))
                
                invalid_count = len(self.cr.fetchall())
                if invalid_count > 0:
                    invalid_fields.append(f"{field_name} (should be numbers)")
                    total_failures += invalid_count
                    if self.TRACK_FAILURE_REASONS:
                        self.failure_reasons['invalid_format'] += invalid_count
                
            # More validation code for date/datetime fields...
        
        # Report invalid field formats
        if invalid_fields:
            self._log_message(
                f"Found data format issues in: {', '.join(invalid_fields)}", 
                "warning",
                send_to_websocket=False
            )
            
        return total_failures
    
    # def _detect_duplicates(self):
    #     """Detect and mark duplicate records with user feedback"""
    #     if not self.unique_constraints:
    #         return 0
            
    #     duplicate_count = 0
    #     duplicate_constraints = []
        
    #     # Check against each unique constraint
    #     for constraint_fields in self.unique_constraints:
    #         # Filter to fields that are in our mapping
    #         fields = [f for f in constraint_fields if f in self.inverse_mappings.keys()]
            
    #         if not fields:
    #             continue
                
    #         # Check for duplicates within batch
    #         fields_clause = " AND ".join([f"t1.{f} = t2.{f}" for f in fields])
    #         not_null_clause = " AND ".join([f"t1.{f} IS NOT NULL" for f in fields])
            
    #         self.cr.execute(f"""
    #             UPDATE {self._quoted_temp_table()} t1
    #             SET odoo_status = 'duplicate',
    #                 odoo_error = 'Duplicate record in import batch'
    #             FROM {self._quoted_temp_table()} t2
    #             WHERE t1.tmp_id > t2.tmp_id
    #               AND {fields_clause}
    #               AND {not_null_clause}
    #               AND t1.odoo_status = 'pending'
    #             RETURNING t1.tmp_id
    #         """)
            
    #         batch_duplicates = len(self.cr.fetchall())
    #         if batch_duplicates > 0 and self.TRACK_FAILURE_REASONS:
    #             self.failure_reasons['duplicate'] += batch_duplicates
    #             if f"{', '.join(fields)}" not in duplicate_constraints:
    #                 duplicate_constraints.append(f"{', '.join(fields)}")
            
    #         # Check against existing records
    #         fields_clause = " AND ".join([f"t.{f} = m.{f}" for f in fields])
    #         not_null_clause = " AND ".join([f"t.{f} IS NOT NULL" for f in fields])
            
    #         self.cr.execute(f"""
    #             UPDATE {self._quoted_temp_table()} t
    #             SET odoo_status = 'duplicate',
    #                 odoo_error = 'Record already exists in database'
    #             FROM {self.table_name} m
    #             WHERE {fields_clause}
    #               AND {not_null_clause}
    #               AND t.odoo_status = 'pending'
    #             RETURNING t.tmp_id
    #         """)
            
    #         existing_duplicates = len(self.cr.fetchall())
    #         if existing_duplicates > 0 and self.TRACK_FAILURE_REASONS:
    #             self.failure_reasons['duplicate'] += existing_duplicates
    #             if f"{', '.join(fields)}" not in duplicate_constraints:
    #                 duplicate_constraints.append(f"{', '.join(fields)}")
        
    #     # Count duplicates
    #     self.cr.execute(f"""
    #         SELECT COUNT(*) FROM {self._quoted_temp_table()}
    #         WHERE odoo_status = 'duplicate'
    #     """)
    #     duplicate_count = self.cr.fetchone()[0]
        
    #     # Provide user feedback if duplicates found
    #     if duplicate_count > 0:
    #         if duplicate_constraints:
    #             self._log_message(
    #                 f"Found {duplicate_count:,} duplicate records based on unique constraints: {', '.join(duplicate_constraints)}",
    #                 "warning",
    #                 send_to_websocket=False
    #             )
    #         else:
    #             self._log_message(
    #                 f"Found {duplicate_count:,} duplicate records",
    #                 "warning",
    #                 send_to_websocket=False
    #             )
        
    #     return duplicate_count

    def _detect_duplicates(self):
        """Detect and mark duplicate records with user feedback"""
        if not self.unique_constraints:
            return 0
                
        duplicate_count = 0
        duplicate_constraints = []
        
        # Check against each unique constraint
        for constraint_fields in self.unique_constraints:
            # Filter to fields that are in our mapping
            fields = [f for f in constraint_fields if f in self.inverse_mappings.keys() or f in self.required_fields]
            
            if not fields:
                continue
            
            # Check for duplicates using the helper method
            batch_duplicates, existing_duplicates = self._check_constraint_duplicates(fields)
            
            # Track statistics
            duplicate_count += (batch_duplicates + existing_duplicates)
            
            if (batch_duplicates + existing_duplicates) > 0 and self.TRACK_FAILURE_REASONS:
                self.failure_reasons['duplicate'] += (batch_duplicates + existing_duplicates)
                if f"{', '.join(fields)}" not in duplicate_constraints:
                    duplicate_constraints.append(f"{', '.join(fields)}")
        
        # Provide user feedback if duplicates found
        if duplicate_count > 0:
            if duplicate_constraints:
                self._log_message(
                    f"Found {duplicate_count:,} duplicate records based on unique constraints: {', '.join(duplicate_constraints)}",
                    "warning"
                )
            else:
                self._log_message(
                    f"Found {duplicate_count:,} duplicate records",
                    "warning"
                )
        
        return duplicate_count

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

    # def _insert_valid_records(self):
    #     """Insert valid records into the target table with proper savepoint handling"""
    #     # Get valid fields from model
    #     valid_fields = set()
        
    #     # Add all fields from the mapping and required fields
    #     for field_name in self.model_fields:
    #         if field_name in self.inverse_mappings or field_name in self.required_fields:
    #             valid_fields.add(field_name)
        
    #     if not valid_fields:
    #         return 0
            
    #     # Build insert statement with proper quoting
    #     fields_str = ", ".join([f'"{f}"' for f in valid_fields])
    #     fields_src = ", ".join([f'src."{f}"' for f in valid_fields])
        
    #     # For transaction management, process in chunks
    #     inserted_count = 0
    #     chunk_size = self.MAX_TRANSACTION_SIZE
        
    #     # Get count of pending records
    #     self.cr.execute(f"""
    #         SELECT COUNT(*) FROM {self._quoted_temp_table()}
    #         WHERE odoo_status = 'pending'
    #     """)
    #     pending_count = self.cr.fetchone()[0]
        
    #     if pending_count == 0:
    #         return 0
            
    #     # Process in transaction-sized chunks
    #     for offset in range(0, pending_count, chunk_size):
    #         # Get chunk of record IDs to process
    #         self.cr.execute(f"""
    #             SELECT tmp_id FROM {self._quoted_temp_table()}
    #             WHERE odoo_status = 'pending'
    #             ORDER BY tmp_id
    #             LIMIT %s OFFSET %s
    #         """, (chunk_size, offset))
            
    #         record_ids = [r[0] for r in self.cr.fetchall()]
            
    #         if not record_ids:
    #             break
                
    #         # Insert this chunk with retries
    #         for retry in range(self.MAX_RETRIES):
    #             savepoint_created = False
    #             try:
    #                 # Create a unique savepoint name for this chunk and retry
    #                 savepoint_name = f"chunk_insert_{offset}_{retry}"
    #                 self.cr.execute(f"SAVEPOINT {savepoint_name}")
    #                 savepoint_created = True
                    
    #                 # Direct SQL insert with RETURNING to get IDs
    #                 quoted_table = self.cr.mogrify(f'"{self.table_name}"', ()).decode('utf-8')
    #                 self.cr.execute(f"""
    #                     WITH inserted AS (
    #                         INSERT INTO {quoted_table}
    #                         ({fields_str})
    #                         SELECT {fields_src}
    #                         FROM {self._quoted_temp_table()} src
    #                         WHERE src.tmp_id IN %s
    #                         AND src.odoo_status = 'pending'
    #                         RETURNING id
    #                     )
    #                     SELECT COUNT(*) FROM inserted
    #                 """, (tuple(record_ids),))
                    
    #                 # Count inserted records
    #                 chunk_inserted = self.cr.fetchone()[0]
    #                 inserted_count += chunk_inserted
                    
    #                 # Mark these records as successful
    #                 self.cr.execute(f"""
    #                     UPDATE {self._quoted_temp_table()}
    #                     SET odoo_status = 'success'
    #                     WHERE tmp_id IN %s
    #                     AND odoo_status = 'pending'
    #                 """, (tuple(record_ids),))
                    
    #                 # Release savepoint
    #                 if savepoint_created:
    #                     self.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    
    #                 # Commit after successful chunk
    #                 self.cr.commit()
    #                 break
                    
    #             except psycopg2.Error as e:
    #                 # Rollback to savepoint if it was created
    #                 if savepoint_created:
    #                     try:
    #                         self.cr.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
    #                     except Exception as rollback_error:
    #                         _logger.error(f"Error rolling back to savepoint: {str(rollback_error)}")
    #                         # Do a full rollback if savepoint rollback fails
    #                         self.cr.rollback()
    #                 else:
    #                     # No savepoint, do a full rollback
    #                     self.cr.rollback()
                    
    #                 # On final retry, mark as errors
    #                 if retry == self.MAX_RETRIES - 1:
    #                     error_msg = f"Database error: {str(e)}"
                        
    #                     # Check for duplicate key error
    #                     if "duplicate key value violates unique constraint" in str(e):
    #                         error_msg = "Duplicate record detected that wasn't caught by duplicate detection"
    #                         # Update statistics
    #                         if self.TRACK_FAILURE_REASONS:
    #                             self.failure_reasons['duplicate'] += len(record_ids)
                        
    #                     try:
    #                         self.cr.execute(f"""
    #                             UPDATE {self._quoted_temp_table()}
    #                             SET odoo_status = 'error',
    #                                 odoo_error = %s
    #                             WHERE tmp_id IN %s
    #                             AND odoo_status = 'pending'
    #                         """, (error_msg, tuple(record_ids)))
                            
    #                         self.cr.commit()
    #                     except Exception as update_error:
    #                         _logger.error(f"Error updating error status: {str(update_error)}")
                            
    #                     if self.TRACK_FAILURE_REASONS and "duplicate key" not in str(e):
    #                         self.failure_reasons['other'] += len(record_ids)
    #                     break
                        
    #                 # Try again after delay
    #                 _logger.warning(f"Retry {retry+1} for insert due to: {str(e)}")
    #                 time.sleep(self.RETRY_DELAY)
        
    #     return inserted_count

    def _insert_valid_records(self):
        """Insert valid records with improved savepoint management"""
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
        chunk_size = self.MAX_TRANSACTION_SIZE
        
        # Get count of pending records
        self.cr.execute(f"""
            SELECT COUNT(*) FROM {self._quoted_temp_table()}
            WHERE odoo_status = 'pending'
        """)
        pending_count = self.cr.fetchone()[0]
        
        if pending_count == 0:
            return 0
            
        # Process in transaction-sized chunks
        for offset in range(0, pending_count, chunk_size):
            # Get chunk of record IDs to process
            self.cr.execute(f"""
                SELECT tmp_id FROM {self._quoted_temp_table()}
                WHERE odoo_status = 'pending'
                ORDER BY tmp_id
                LIMIT %s OFFSET %s
            """, (chunk_size, offset))
            
            record_ids = [r[0] for r in self.cr.fetchall()]
            
            if not record_ids:
                break
                
            # Insert this chunk with retries
            for retry in range(self.MAX_RETRIES):
                # Create a unique savepoint name for this chunk and retry
                import random
                import string
                sp_name = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
                savepoint_name = f"chunk_{offset}_{retry}_{sp_name}"
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
                    self.cr.execute(f"""
                        WITH inserted AS (
                            INSERT INTO {quoted_table}
                            ({fields_str})
                            SELECT {fields_src}
                            FROM {self._quoted_temp_table()} src
                            WHERE src.tmp_id IN %s
                            AND src.odoo_status = 'pending'
                            RETURNING id
                        )
                        SELECT COUNT(*) FROM inserted
                    """, (tuple(record_ids),))
                    
                    # Count inserted records
                    chunk_inserted = self.cr.fetchone()[0]
                    inserted_count += chunk_inserted
                    
                    # Mark these records as successful
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
                    
                    # Commit after successful chunk
                    self.cr.commit()
                    break
                    
                except psycopg2.Error as e:
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
                        break
                        
                    # Try again after delay
                    _logger.warning(f"Retry {retry+1} for insert due to: {str(e)}")
                    time.sleep(self.RETRY_DELAY)
        
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
    
    # def _send_batch_summary(self):
    #     """Send a user-friendly summary of the batch processing results"""
    #     # Get key metrics
    #     success_count = self.results.get('successful', 0)
    #     duplicate_count = self.results.get('duplicates', 0)
    #     failed_count = self.results.get('failed', 0)
    #     total_count = success_count + duplicate_count + failed_count
    #     process_time = self.process_time
        
    #     # Calculate records per second
    #     if process_time > 0:
    #         records_per_second = total_count / process_time
    #     else:
    #         records_per_second = 0
            
    #     # Prepare summary message
    #     summary_lines = []
        
    #     # Success information
    #     if success_count > 0:
    #         summary_lines.append(f"✅ Successfully imported {success_count:,} records")
        
    #     # Duplicate information if any
    #     if duplicate_count > 0:
    #         summary_lines.append(f"⚠️ Skipped {duplicate_count:,} duplicate records")
            
    #     # Failed information if any
    #     if failed_count > 0:
    #         summary_lines.append(f"❌ Failed to import {failed_count:,} records")
            
    #         # Add failure reasons if available
    #         if self.TRACK_FAILURE_REASONS and self.failure_reasons:
    #             failure_details = []
    #             for reason, count in self.failure_reasons.items():
    #                 if count > 0:
    #                     # Make the reason more readable
    #                     readable_reason = reason.replace('_', ' ').title()
    #                     failure_details.append(f"  • {readable_reason}: {count:,}")
                        
    #             if failure_details:
    #                 summary_lines.append("Failure reasons:")
    #                 summary_lines.extend(failure_details)
        
    #     # Performance metrics
    #     if total_count > 0:
    #         summary_lines.append(f"⏱️ Processed in {process_time:.1f} seconds ({records_per_second:.1f} records/sec)")
            
    #     # Progress information
    #     total_records = self.import_log.total_records
    #     if total_records > 0:
    #         current_position = self.import_log.current_position + total_count
    #         progress = min(100, (current_position / total_records * 100))
    #         summary_lines.append(f"📊 Overall progress: {progress:.1f}% complete")
            
    #     # Send the summary
    #     summary_message = "\n".join(summary_lines)
    #     self._log_message(summary_message, "success" if success_count > 0 else "warning")

    def _send_batch_summary(self):
        """Send a user-friendly summary of the batch processing results with consistent progress calculation"""
        # Get key metrics
        success_count = self.results.get('successful', 0)
        duplicate_count = self.results.get('duplicates', 0)
        failed_count = self.results.get('failed', 0)
        total_count = success_count + duplicate_count + failed_count
        process_time = self.process_time
        
        # Calculate records per second
        if process_time > 0:
            records_per_second = total_count / process_time
        else:
            records_per_second = 0
        
        # Get overall import progress from import log
        current_position = self.import_log.current_position
        total_records = self.import_log.total_records
        
        # Calculate accurate progress percentage
        if total_records > 0:
            # Adjust current position to include all processed records in this batch
            adjusted_position = current_position + total_count
            # Ensure we don't exceed 100%
            overall_progress = min(100, (adjusted_position / total_records * 100))
        else:
            overall_progress = 0
        
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
            
        # Progress information - ensuring NO PERCENTAGE is added to the end of the message
        # by marking this as a special message
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
    
    # def _cleanup_temp_resources(self):
    #     """Clean up temporary resources"""
    #     try:
    #         # self.cr.execute(f"DROP TABLE IF EXISTS {self._quoted_temp_table()}")
    #         self.cr.execute(f'DROP TABLE IF EXISTS {self._quoted_temp_table()}')
    #         self.cr.commit()
    #         _logger.info(f"Dropped temporary table {self._quoted_temp_table()}")
    #     except Exception as e:
    #         _logger.warning(f"Error cleaning up temporary table: {str(e)}")

    def _cleanup_temp_resources(self):
        """Clean up temporary resources with proper quoting"""
        try:
            # Use proper parameter quoting to avoid SQL injection
            self.cr.execute("DROP TABLE IF EXISTS %s" % self.cr.mogrify(f'"{self.temp_table_name}"', ()).decode('utf-8'))
            self.cr.commit()
            _logger.info(f"Dropped temporary table {self.temp_table_name}")
        except Exception as e:
            _logger.warning(f"Error cleaning up temporary table: {str(e)}")
        
    # def _log_message(self, message, message_type="info", send_to_websocket=True):
    #     """Log a message to both server log and websocket"""
    #     # Calculate progress
    #     current_position = self.import_log.current_position
    #     total_records = self.import_log.total_records
        
    #     if total_records > 0:
    #         progress = min(100, (current_position / total_records * 100))
    #         progress_str = f" ({progress:.1f}% complete)"
    #     else:
    #         progress_str = ""
            
    #     # Create server log message
    #     log_message = f"{self.model_name} import (job {self.job_id}): {message}"
        
    #     # Add progress to websocket message if configured
    #     if send_to_websocket:
    #         full_message = f"{message}{progress_str}"
    #     else:
    #         full_message = log_message
        
    #     # Always log to server
    #     log_level = {
    #         "info": _logger.info,
    #         "error": _logger.error,
    #         "success": _logger.info,
    #         "warning": _logger.warning,
    #     }.get(message_type, _logger.info)
        
    #     log_level(log_message)
        
    #     # Only send user-friendly messages to websocket
    #     if send_to_websocket:
    #         try:
    #             send_message(self.env, full_message, message_type, self.user_id)
    #         except Exception as e:
    #             _logger.warning(f"Failed to send websocket message: {e}")

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
