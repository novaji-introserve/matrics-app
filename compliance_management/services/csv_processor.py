import base64
import logging
import tempfile
import os
import re
import pandas as pd
import numpy as np
import chardet
from io import BytesIO
from datetime import datetime
from odoo import _, fields

_logger = logging.getLogger(__name__)

try:
    from .websocket_helper import send_log_message
except ImportError:
    # Mock function if the real one can't be imported
    def send_log_message(env, message, message_type="info", user_id=None):
        log_level = {
            "info": _logger.info,
            "error": _logger.error,
            "success": _logger.info,
            "warning": _logger.warning,
        }.get(message_type, _logger.info)
        log_level(f"[{message_type.upper()}] {message}")


class CSVProcessor:
    """CSV Processor for dynamic model import"""

    CHUNK_SIZE = 1000  # Number of records to process in a batch

    def __init__(self, import_log):
        """Initialize with import log record"""
        self.import_log = import_log
        self.env = import_log.env
        self.user_id = import_log.uploaded_by.id
        self.model_name = import_log.model_name
        self.model = self.env[self.model_name]
        self.file_content = import_log.file
        self.file_name = import_log.file_name
        self.results = {
            "success": False,
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "duplicate_records": 0,
            "error_message": "",
            "technical_details": "",
        }

    def process(self):
        """Main processing method"""
        try:
            send_log_message(
                self.env, f"Starting import for {self.model_name}", "info", self.user_id
            )

            # Read the file
            df = self._read_file()
            if df is None:
                return self.results

            # Get total records
            total_records = len(df)
            self.results["total_records"] = total_records
            send_log_message(
                self.env,
                f"Found {total_records} records to import",
                "info",
                self.user_id,
            )

            # Get field mappings
            field_mappings = self._get_field_mappings(df)
            if not field_mappings:
                self.results["error_message"] = (
                    "Failed to map CSV columns to model fields"
                )
                return self.results

            # Process in chunks
            successful_records = 0
            failed_records = 0
            duplicate_records = 0

            for start_idx in range(0, total_records, self.CHUNK_SIZE):
                end_idx = min(start_idx + self.CHUNK_SIZE, total_records)
                chunk = df.iloc[start_idx:end_idx]

                # Process chunk
                chunk_result = self._process_chunk(chunk, field_mappings)

                # Update counters
                successful_records += chunk_result.get("successful", 0)
                failed_records += chunk_result.get("failed", 0)
                duplicate_records += chunk_result.get("duplicates", 0)

                # Update progress in import log
                progress_percent = round((end_idx / total_records) * 100, 1)
                self.import_log.write(
                    {
                        "successful_records": successful_records,
                        "failed_records": failed_records,
                        "duplicate_records": duplicate_records,
                    }
                )

                send_log_message(
                    self.env,
                    f"Progress: {progress_percent}% ({end_idx}/{total_records} records)",
                    "info",
                    self.user_id,
                )

            # Update final results
            self.results.update(
                {
                    "success": True if successful_records > 0 else False,
                    "successful_records": successful_records,
                    "failed_records": failed_records,
                    "duplicate_records": duplicate_records,
                }
            )

            if successful_records > 0:
                send_log_message(
                    self.env,
                    f"Import completed successfully: {successful_records} records imported",
                    "success",
                    self.user_id,
                )
            else:
                error_msg = f"Import failed: No records were imported. {failed_records} failed, {duplicate_records} duplicates"
                send_log_message(self.env, error_msg, "error", self.user_id)
                self.results["error_message"] = error_msg

            if failed_records > 0:
                send_log_message(
                    self.env,
                    f"Warning: {failed_records} records failed to import",
                    "warning",
                    self.user_id,
                )

            if duplicate_records > 0:
                send_log_message(
                    self.env,
                    f"Warning: {duplicate_records} duplicate records were found",
                    "warning",
                    self.user_id,
                )

            return self.results

        except Exception as e:
            import traceback

            error_trace = traceback.format_exc()
            error_message = f"Error processing import: {str(e)}"
            _logger.error(error_message)
            _logger.error(error_trace)

            send_log_message(self.env, error_message, "error", self.user_id)

            self.results.update(
                {
                    "success": False,
                    "error_message": error_message,
                    "technical_details": error_trace,
                }
            )
            return self.results

    def _read_file(self):
        """Read CSV or Excel file with robust error handling"""
        try:
            if not self.file_content:
                self.results["error_message"] = "No file content found"
                return None

            # Decode file content
            file_data = base64.b64decode(self.file_content)

            # Create temporary file
            file_ext = os.path.splitext(self.file_name.lower())[1]

            # Check if Excel file
            if file_ext in (".xlsx", ".xls"):
                send_log_message(
                    self.env, "Reading Excel file...", "info", self.user_id
                )
                try:
                    df = pd.read_excel(
                        BytesIO(file_data),
                        dtype=str,
                        engine="openpyxl",
                        keep_default_na=False,
                    )
                    send_log_message(
                        self.env,
                        "Excel file read successfully",
                        "success",
                        self.user_id,
                    )
                    return df
                except Exception as e:
                    send_log_message(
                        self.env,
                        f"Error reading Excel file: {str(e)}",
                        "error",
                        self.user_id,
                    )
                    self.results["error_message"] = (
                        f"Error reading Excel file: {str(e)}"
                    )
                    return None

            # Handle CSV with multiple approaches
            return self._read_csv_with_fallbacks(file_data)

        except Exception as e:
            send_log_message(
                self.env, f"Error reading file: {str(e)}", "error", self.user_id
            )
            self.results["error_message"] = f"Error reading file: {str(e)}"
            return None

    def _read_csv_with_fallbacks(self, file_data):
        """Try multiple approaches to read CSV data"""
        # Detect encoding
        detection = chardet.detect(file_data[:10000])
        encoding = detection["encoding"] or "utf-8"
        confidence = detection["confidence"]
        send_log_message(
            self.env,
            f"Detected encoding: {encoding} (confidence: {confidence:.2f})",
            "info",
            self.user_id,
        )

        # List of encodings to try
        encodings = [encoding, "utf-8", "latin1", "ISO-8859-1", "cp1252"]
        # List of separators to try
        separators = [",", ";", "\t", "|"]

        for enc in encodings:
            for sep in separators:
                try:
                    df = pd.read_csv(
                        BytesIO(file_data),
                        encoding=enc,
                        sep=sep,
                        dtype=str,
                        keep_default_na=False,
                        on_bad_lines="warn",
                    )

                    # Check if we got at least one column and one row
                    if df.shape[1] > 1 and df.shape[0] > 0:
                        send_log_message(
                            self.env,
                            f"CSV file read successfully with encoding={enc}, separator='{sep}'",
                            "success",
                            self.user_id,
                        )
                        return df
                except Exception as e:
                    continue

        send_log_message(
            self.env,
            "Failed to read CSV with any encoding or separator",
            "error",
            self.user_id,
        )
        self.results["error_message"] = (
            "Failed to read CSV with any encoding or separator"
        )
        return None

    def _get_field_mappings(self, df):
        """Map CSV columns to model fields"""
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

            send_log_message(self.env, mapping_info, "info", self.user_id)

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
                error_msg = f"Required fields are missing in CSV: {missing}"
                send_log_message(self.env, error_msg, "error", self.user_id)
                self.results["error_message"] = error_msg
                return None

            return field_mappings

        except Exception as e:
            send_log_message(
                self.env, f"Error mapping fields: {str(e)}", "error", self.user_id
            )
            self.results["error_message"] = f"Error mapping fields: {str(e)}"
            return None

    def _process_chunk(self, chunk, field_mappings):
        """Process a chunk of data"""
        results = {"successful": 0, "failed": 0, "duplicates": 0}

        try:
            # Convert chunk to list of dicts with appropriate field mapping
            records = []

            for _, row in chunk.iterrows():
                record = {}
                for csv_field, model_field in field_mappings.items():
                    # Skip empty values
                    if pd.isna(row[csv_field]) or row[csv_field] == "":
                        continue

                    # Get field type and convert value accordingly
                    field_type = self.model._fields[model_field].type
                    value = self._convert_value(row[csv_field], field_type, model_field)

                    if value is not None:
                        record[model_field] = value

                records.append(record)

            # Check for duplicates
            clean_records, duplicate_count = self._handle_duplicates(records)
            results["duplicates"] = duplicate_count

            # Create records in batches
            if clean_records:
                try:
                    created_records = self.model.with_context(
                        tracking_disable=True
                    ).create(clean_records)
                    results["successful"] = len(created_records)
                except Exception as e:
                    send_log_message(
                        self.env,
                        f"Error creating records: {str(e)}. Falling back to individual creation.",
                        "warning",
                        self.user_id,
                    )
                    # Fallback to creating records individually
                    successful = 0
                    failed = 0

                    for record in clean_records:
                        try:
                            self.model.with_context(tracking_disable=True).create(
                                [record]
                            )
                            successful += 1
                        except Exception as e:
                            failed += 1
                            continue

                    results["successful"] = successful
                    results["failed"] = failed

            return results

        except Exception as e:
            send_log_message(
                self.env, f"Error processing chunk: {str(e)}", "error", self.user_id
            )
            results["failed"] = len(chunk)
            return results

    def _convert_value(self, value, field_type, field_name):
        """Convert value to appropriate type for Odoo field"""
        if value is None or pd.isna(value) or value == "":
            return None

        try:
            # Convert to string first
            if not isinstance(value, str):
                value = str(value)

            value = value.strip()

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
            send_log_message(
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
            send_log_message(
                self.env,
                f"Found {duplicate_count} duplicate records that will be skipped",
                "warning",
                self.user_id,
            )

        return clean_records, duplicate_count
