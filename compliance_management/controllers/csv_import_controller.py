import json
import logging
import os
import tempfile
import base64
from datetime import datetime
import random
import string

from odoo import http, _
from odoo.http import request, Response
from werkzeug.exceptions import BadRequest
# from ..services.websocket.websocket_helper import send_log_message
from ..services.websocket.connection import send_message

_logger = logging.getLogger(__name__)

class CSVImportController(http.Controller):

    @http.route("/csv_import/upload", type="http", auth="user")
    def csv_upload_page(self):
        """Render the CSV upload page"""
        # No need to pass template_downloads - will be fetched dynamically via AJAX
        return request.render("csv_import.csv_import_upload_form", {})

    @http.route("/csv_import/get_import_models", type="json", auth="user")
    def get_import_models(self, search_term=None, limit=10, offset=0):
        """Get available import models"""
        domain = [("active", "=", True)]

        if search_term:
            domain += [
                "|",
                ("name", "ilike", search_term),
                ("model_id.model", "ilike", search_term),
            ]

        # Get total count for pagination
        total_count = request.env["csv.import.model"].sudo().search_count(domain)

        # Get records with pagination
        models = (
            request.env["csv.import.model"]
            .sudo()
            .search_read(
                domain=domain,
                fields=["id", "name", "model_name", "description", "template_filename"],
                limit=limit,
                offset=offset,
                order="name",
            )
        )

        return {"models": models, "total": total_count}

    @http.route("/csv_import/get_model_fields", type="json", auth="user")
    def get_model_fields(self, model_id):
        """Get fields for a specific model"""
        import_model = request.env["csv.import.model"].sudo().browse(int(model_id))
        if not import_model.exists():
            return {"error": "Model not found"}

        try:
            fields = import_model.get_importable_fields()
            return {
                "fields": fields,
                "required_fields": import_model.field_ids.mapped("name"),
            }
        except Exception as e:
            _logger.error(f"Error getting model fields: {str(e)}")
            return {"error": str(e)}

    @http.route(
        "/csv_import/upload_chunk",
        type="http",
        auth="user",
        methods=["POST"],
        csrf=True,
    )
    def upload_chunk(self, **post):
        """Handle chunked file uploads"""
        try:
            # Get headers and validate
            chunk_number = request.httprequest.headers.get("X-Chunk-Number")
            total_chunks = request.httprequest.headers.get("X-Total-Chunks")
            file_id = request.httprequest.headers.get("X-File-Id")
            original_filename = request.httprequest.headers.get("X-Original-Filename")
            model_id = request.httprequest.headers.get("X-Model-Id")

            # Log request info
            _logger.info(
                f"Processing chunk {chunk_number}/{total_chunks} for {original_filename}"
            )

            # Validate input
            if not all(
                [chunk_number, total_chunks, file_id, original_filename, model_id]
            ):
                return Response(
                    json.dumps({"error": "Missing required headers"}),
                    content_type="application/json",
                    status=400,
                )

            # Convert to correct types
            try:
                chunk_number = int(chunk_number)
                total_chunks = int(total_chunks)
                model_id = int(model_id)
            except ValueError:
                return Response(
                    json.dumps({"error": "Invalid number format in headers"}),
                    content_type="application/json",
                    status=400,
                )

            # Validate model ID
            import_model = request.env["csv.import.model"].sudo().browse(model_id)
            if not import_model.exists():
                return Response(
                    json.dumps({"error": f"Invalid model ID: {model_id}"}),
                    content_type="application/json",
                    status=400,
                )

            # Get the chunk file
            chunk_file = request.httprequest.files.get("chunk")
            if not chunk_file:
                return Response(
                    json.dumps({"error": "No chunk file provided"}),
                    content_type="application/json",
                    status=400,
                )

            # Create temp directory for chunks if not exists
            temp_dir = tempfile.gettempdir()
            chunk_dir = os.path.join(temp_dir, "odoo_csv_import", file_id)
            os.makedirs(chunk_dir, exist_ok=True)

            # Save the chunk
            chunk_path = os.path.join(chunk_dir, f"chunk_{chunk_number}")
            chunk_file.save(chunk_path)

            # Send log message
            self._send_log_message(
                f"Successfully saved chunk {chunk_number + 1} of {total_chunks}",
                "success",
            )

            # If this is the last chunk, process the complete file
            if chunk_number == total_chunks - 1:
                self._send_log_message(
                    "Final chunk received. Starting file reassembly...", "info"
                )
                return self._handle_final_chunk(
                    file_id, total_chunks, original_filename, import_model, chunk_dir
                )

            # Return success for intermediate chunks
            return Response(
                json.dumps(
                    {
                        "status": "success",
                        "message": "Chunk received successfully",
                        "chunk_number": chunk_number,
                    }
                ),
                content_type="application/json",
            )

        except Exception as e:
            _logger.error(f"Unexpected error in upload_chunk: {str(e)}")
            self._send_log_message(f"Error processing chunk: {str(e)}", "error")
            return Response(
                json.dumps({"error": f"Server error: {str(e)}"}),
                content_type="application/json",
                status=500,
            )

    def _handle_final_chunk(
        self, file_id, total_chunks, original_filename, import_model, chunk_dir
    ):
        """Process the final chunk and initiate file processing"""
        final_path = None
        try:
            # Generate unique filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            random_suffix = "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )
            name, ext = os.path.splitext(original_filename)
            unique_filename = f"{name}_{timestamp}_{random_suffix}{ext}"

            # Combine all chunks into a single file
            final_path = os.path.join(chunk_dir, unique_filename)
            with open(final_path, "wb") as outfile:
                for i in range(total_chunks):
                    chunk_path = os.path.join(chunk_dir, f"chunk_{i}")
                    if not os.path.exists(chunk_path):
                        self._send_log_message(f"Missing chunk {i}", "error")
                        return Response(
                            json.dumps({"error": f"Missing chunk {i}"}),
                            content_type="application/json",
                            status=500,
                        )

                    with open(chunk_path, "rb") as infile:
                        outfile.write(infile.read())

            # Read the file into memory
            with open(final_path, "rb") as infile:
                file_content = base64.b64encode(infile.read())

            # Create batch folder
            batch_folder = f"batch_{import_model.model_name}_{timestamp}"

            # Create import log
            content_type = "text/csv"
            if original_filename.lower().endswith((".xlsx", ".xls")):
                content_type = (
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            import_log = (
                request.env["import.log"]
                .sudo()
                .create(
                    {
                        "name": f"Import {unique_filename}",
                        "file_name": unique_filename,
                        "original_filename": original_filename,
                        "content_type": content_type,
                        "import_model_id": import_model.id,
                        "file": file_content,
                        "status": "pending",
                        "batch_folder": batch_folder,
                        "uploaded_by": request.env.user.id,
                    }
                )
            )

            # Queue the processing task (using Odoo queue.job if installed, otherwise direct process)
            if "queue.job" in request.env:
                self._send_log_message("Queueing file for processing...", "info")
                request.env["queue.job"].sudo().create(
                    {
                        "name": f"Process CSV Import {import_log.id}",
                        "model_name": "import.log",
                        "method_name": "process_file",
                        "args": str([import_log.id]),
                        "user_id": request.env.user.id,
                    }
                )
            else:
                self._send_log_message("Processing file immediately...", "info")
                import_log.sudo().with_context(async_mode=False).process_file()

            self._send_log_message(
                "File successfully uploaded and queued for processing", "success"
            )

            # Clean up chunks
            for i in range(total_chunks):
                chunk_path = os.path.join(chunk_dir, f"chunk_{i}")
                if os.path.exists(chunk_path):
                    os.unlink(chunk_path)

            # Clean up temporary directory
            try:
                os.rmdir(chunk_dir)
            except:
                pass

            # Clean up final file
            if os.path.exists(final_path):
                os.unlink(final_path)

            return Response(
                json.dumps(
                    {
                        "status": "success",
                        "import_id": import_log.id,
                        "message": "File upload complete, processing initiated",
                        "filename": unique_filename,
                    }
                ),
                content_type="application/json",
            )

        except Exception as e:
            _logger.error(f"Error handling final chunk: {str(e)}")
            self._send_log_message(f"Error processing file: {str(e)}", "error")

            # Clean up final file if it exists
            if final_path and os.path.exists(final_path):
                try:
                    os.unlink(final_path)
                except:
                    pass

            return Response(
                json.dumps({"error": f"Error handling final chunk: {str(e)}"}),
                content_type="application/json",
                status=500,
            )

    @http.route(
        "/csv_import/download_template/<int:model_id>", type="http", auth="user"
    )
    def download_template(self, model_id, **kw):
        """Download template file for a specific model"""
        try:
            import_model = request.env["csv.import.model"].sudo().browse(int(model_id))
            if not import_model.exists():
                return Response(
                    json.dumps({"error": "Model not found"}),
                    content_type="application/json",
                    status=404,
                )

            # Check if model has a template file
            if not import_model.template_file:
                # Generate a template if none exists
                self._send_log_message(
                    f"Generating template for {import_model.name}...", "info"
                )
                content = self._generate_template(import_model)

                if not content:
                    return Response(
                        json.dumps({"error": "Could not generate template"}),
                        content_type="application/json",
                        status=500,
                    )
            else:
                # Use the stored template
                content = base64.b64decode(import_model.template_file)

            filename = (
                import_model.template_filename
                or f"{import_model.model_name}_template.xlsx"
            )

            self._send_log_message(
                f"Downloading template for {import_model.name}...", "info"
            )

            # Return the file
            return request.make_response(
                content,
                headers=[
                    (
                        "Content-Type",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    ),
                    ("Content-Disposition", f'attachment; filename="{filename}"'),
                ],
            )

        except Exception as e:
            _logger.error(f"Error downloading template: {str(e)}")
            return Response(
                json.dumps({"error": f"Error downloading template: {str(e)}"}),
                content_type="application/json",
                status=500,
            )

    def _generate_template(self, import_model):
        """Generate a template XLSX file for a model"""
        try:
            import xlsxwriter
            from io import BytesIO

            # Get importable fields
            fields = import_model.get_importable_fields()

            # Create workbook
            output = BytesIO()
            workbook = xlsxwriter.Workbook(output)
            worksheet = workbook.add_worksheet(
                import_model.name[:31]
            )  # Excel limits worksheet names to 31 chars

            # Add header row
            header_format = workbook.add_format({"bold": True, "bg_color": "#E6E6E6"})
            for col, field in enumerate(fields):
                worksheet.write(0, col, field["string"], header_format)
                worksheet.set_column(col, col, max(len(field["string"]), 15))

            # Add help row with field names
            help_format = workbook.add_format({"italic": True, "font_color": "#808080"})
            for col, field in enumerate(fields):
                info = f"{field['name']} ({field['type']})"
                if field["required"]:
                    info += " (Required)"
                worksheet.write(1, col, info, help_format)

            # Add a sample data row
            sample_format = workbook.add_format({"font_color": "#0070C0"})
            for col, field in enumerate(fields):
                sample_value = self._get_sample_value(field)
                worksheet.write(2, col, sample_value, sample_format)

            # Add data validation for certain field types
            for col, field in enumerate(fields):
                if field["type"] == "boolean":
                    worksheet.data_validation(
                        3,
                        col,
                        1000,
                        col,
                        {
                            "validate": "list",
                            "source": ["TRUE", "FALSE", "Yes", "No", "1", "0"],
                        },
                    )
                elif field["type"] == "selection":
                    # This would require fetching selection values which we'll skip for simplicity
                    pass
                elif field["type"] == "date":
                    worksheet.data_validation(
                        3,
                        col,
                        1000,
                        col,
                        {
                            "validate": "date",
                            "criteria": "between",
                            "minimum": "1900-01-01",
                            "maximum": "2100-12-31",
                        },
                    )

            # Freeze header row
            worksheet.freeze_panes(1, 0)

            # Finalize workbook
            workbook.close()
            content = output.getvalue()
            output.close()

            return content

        except Exception as e:
            _logger.error(f"Error generating template: {str(e)}")
            return None

    def _get_sample_value(self, field):
        """Get a sample value for a field based on its type"""
        field_type = field["type"]

        if field_type == "char":
            return "Sample Text"
        elif field_type == "text":
            return "Sample longer text content"
        elif field_type == "integer":
            return 42
        elif field_type == "float":
            return 42.5
        elif field_type == "monetary":
            return 100.00
        elif field_type == "date":
            return "2023-01-01"
        elif field_type == "datetime":
            return "2023-01-01 12:00:00"
        elif field_type == "boolean":
            return "Yes"
        elif field_type == "many2one":
            return "External ID or Database ID"
        elif field_type == "selection":
            return "Selection Value"
        else:
            return ""

    def _send_log_message(self, message, message_type="info"):
        """Send log message to frontend and log to server"""
        # First, log to server
        log_level = {
            "info": _logger.info,
            "error": _logger.error,
            "success": _logger.info,
            "warning": _logger.warning,
        }.get(message_type, _logger.info)

        log_level(f"[{message_type.upper()}] {message}")

        # Then, try to use the websocket helper if available
        try:
            send_log_message(request.env, message, message_type, request.env.user.id)
        except ImportError:
            # Fallback to bus.bus
            try:
                request.env["bus.bus"]._sendone(
                    f"csv_import_logs_{request.env.user.id}",
                    "log_message",
                    {
                        "message": message,
                        "message_type": message_type,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    },
                )
            except Exception as e:
                _logger.warning(f"Failed to send log message via bus: {str(e)}")
