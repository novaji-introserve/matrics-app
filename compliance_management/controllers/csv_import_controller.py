import json
import logging
import os
import tempfile
import base64
from datetime import datetime
import random
import string
import shutil

from odoo import http, _, api
from odoo.http import request, Response
from werkzeug.exceptions import BadRequest
from ..services.websocket.connection import send_message

_logger = logging.getLogger(__name__)

class CSVImportController(http.Controller):
    # Base directory for storing uploaded files
    UPLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'media', 'uploads')
    
    def __init__(self):
        super(CSVImportController, self).__init__()
        # Ensure upload directory exists
        os.makedirs(self.UPLOAD_DIR, exist_ok=True)

    @http.route("/csv_import/upload", type="http", auth="user")
    def csv_upload_page(self):
        """Render the CSV upload page"""
        return request.render("csv_import.csv_import_upload_form", {})

    @http.route("/csv_import/get_import_models", type="json", auth="user")
    def get_import_models(self, search_term=None, limit=50, offset=0):
        """Get available models for import directly from ir.model"""
        try:
            self._send_message("Fetching available models from database...", "info")
            
            # Define domain for model filtering - exclude transient models and those that should not be imported
            domain = [
                ('transient', '=', False),  # Exclude transient models
                ('model', 'not ilike', 'ir.%'),  # Exclude most system models
                ('model', 'not ilike', 'base.%'),
                ('model', 'not ilike', 'bus.%'),
                ('model', 'not ilike', 'base_%'),
            ]
            
            # If there's a search term, add it to the domain
            if search_term:
                domain += [
                    '|',
                    ('name', 'ilike', search_term),
                    ('model', 'ilike', search_term),
                ]
            
            # Log the domain we're using for transparency
            _logger.info(f"Searching ir.model with domain: {domain}")
            
            # Get count of all matching models
            total_count = request.env['ir.model'].sudo().search_count(domain)
            
            # Get the model records - include description field if it exists
            fields_to_fetch = ['id', 'name', 'model']
            # Check if description field exists in ir.model
            if 'description' in request.env['ir.model']._fields:
                fields_to_fetch.append('description')
            
            ir_models = request.env['ir.model'].sudo().search_read(
                domain=domain,
                fields=fields_to_fetch,
                limit=limit,
                offset=offset,
                order='name',
            )
            
            # Convert to the expected format for the frontend
            models = []
            for ir_model in ir_models:
                # Check if we can access the model (it's installed and accessible)
                model_name = ir_model['model']
                if model_name in request.env:
                    try:
                        # Safer approach to check if model is usable without causing SQL errors
                        model_obj = request.env[model_name].sudo()
                        
                        # Check if model is a proper model with a database table
                        if model_obj._abstract or not model_obj._table:
                            continue
                        
                        # Try to safely check if we can access at least one record
                        try:
                            # Use a direct SQL query with a limit to avoid errors with non-existent tables
                            request.env.cr.execute(f"""
                                SELECT EXISTS (
                                    SELECT 1 FROM information_schema.tables 
                                    WHERE table_name = %s
                                )
                            """, (model_obj._table,))
                            table_exists = request.env.cr.fetchone()[0]
                            
                            if not table_exists:
                                continue
                        except Exception as e:
                            _logger.debug(f"Skipping model {model_name}, table check failed: {str(e)}")
                            continue
                        
                        # Get description from ir_model if available, otherwise use default
                        description = ir_model.get('description', False) or f"Import data into {ir_model['name']}"
                        
                        # Add to our list of available models
                        models.append({
                            'id': ir_model['id'],
                            'name': ir_model['name'],
                            'model_name': model_name,
                            'description': description,
                            'template_filename': f"{model_name.replace('.', '_')}_template.xlsx",
                        })
                    except Exception as e:
                        _logger.debug(f"Skipping model {model_name}: {str(e)}")
                        continue
            
            self._send_message(f"Loaded {len(models)} available models for import", "success")
            
            # Return the result
            return {"models": models, "total": len(models)}
            
        except Exception as e:
            error_msg = f"Error loading import models: {str(e)}"
            _logger.exception(error_msg)
            self._send_message(error_msg, "error")
            return {"models": [], "total": 0, "error": error_msg}

    @http.route("/csv_import/get_model_fields", type="json", auth="user")
    def get_model_fields(self, model_id):
        """Get importable fields for a specific model"""
        try:
            # Get the ir.model record
            ir_model = request.env['ir.model'].sudo().browse(int(model_id))
            if not ir_model.exists():
                self._send_message(f"Error: Model with ID {model_id} not found", "error")
                return {"error": "Model not found"}
                
            model_name = ir_model.model
            if model_name not in request.env:
                return {"error": f"Model {model_name} is not accessible"}
                
            self._send_message(f"Getting fields for model: {ir_model.name}", "info")
            
            # Get the model object
            model_obj = request.env[model_name]
            importable_fields = []
            required_fields = []
            
            # Get all fields for the model
            for field_name, field in model_obj._fields.items():
                # Skip non-storable fields, many2many fields, one2many fields and compute fields without inverse
                if (not field.store or
                    field.type in ["many2many", "one2many", "binary", "reference"] or
                    (field.compute and not field.inverse)):
                    continue
                    
                field_info = {
                    "name": field_name,
                    "string": field.string,
                    "type": field.type,
                    "required": field.required,
                    "relation": field.comodel_name if field.type in ["many2one", "many2many"] else False,
                }
                importable_fields.append(field_info)
                
                # Track required fields
                if field.required:
                    required_fields.append(field_name)
                    
            self._send_message(f"Loaded {len(importable_fields)} fields for model {ir_model.name}", "success")
            
            return {
                "fields": importable_fields,
                "required_fields": required_fields,
            }
            
        except Exception as e:
            error_msg = f"Error getting model fields: {str(e)}"
            _logger.exception(error_msg)
            self._send_message(error_msg, "error")
            return {"error": error_msg}
            
    @http.route(
        "/csv_import/upload_chunk",
        type="http",
        auth="user",
        methods=["POST"],
        csrf=False,
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

            # Validate model ID - use ir.model instead of csv.import.model
            ir_model = request.env["ir.model"].sudo().browse(model_id)
            if not ir_model.exists():
                return Response(
                    json.dumps({"error": f"Invalid model ID: {model_id}"}),
                    content_type="application/json",
                    status=400,
                )

            # Get the chunk file - handle different possible formats
            chunk_data = None
            
            # Try to get from files first (multipart form data)
            if 'chunk' in request.httprequest.files:
                chunk_file = request.httprequest.files['chunk']
                chunk_data = chunk_file.read()
                _logger.info(f"Got chunk from files, size: {len(chunk_data)} bytes")
            # Try to get from form data
            elif 'chunk' in request.httprequest.form:
                chunk_data = request.httprequest.form['chunk']
                # If it's a string, try to decode if it's base64
                if isinstance(chunk_data, str):
                    try:
                        chunk_data = base64.b64decode(chunk_data)
                        _logger.info(f"Decoded base64 chunk, size: {len(chunk_data)} bytes")
                    except:
                        _logger.warning("Failed to decode base64, treating as raw data")
            # Try to get raw data
            elif request.httprequest.data:
                chunk_data = request.httprequest.data
                _logger.info(f"Got chunk from raw data, size: {len(chunk_data)} bytes")
                
            if not chunk_data:
                _logger.error("No chunk data found in request")
                return Response(
                    json.dumps({"error": "No chunk file provided"}),
                    content_type="application/json",
                    status=400,
                )

            # Create temp directory for chunks if not exists
            temp_dir = tempfile.gettempdir()
            chunk_dir = os.path.join(temp_dir, "odoo_csv_import", file_id)
            os.makedirs(chunk_dir, exist_ok=True)

            # Save the chunk - write binary data directly
            chunk_path = os.path.join(chunk_dir, f"chunk_{chunk_number}")
            with open(chunk_path, 'wb') as f:
                if isinstance(chunk_data, str):
                    f.write(chunk_data.encode('utf-8'))
                else:
                    f.write(chunk_data)

            # Send log message
            self._send_message(
                f"Successfully saved chunk {chunk_number + 1} of {total_chunks}",
                "success",
            )

            # If this is the last chunk, process the complete file
            if chunk_number == total_chunks - 1:
                self._send_message(
                    "Final chunk received. Starting file reassembly...", "info"
                )
                return self._handle_final_chunk(
                    file_id, total_chunks, original_filename, ir_model, chunk_dir
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
            # Log the full traceback for better debugging
            import traceback
            _logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Try to send message - but don't fail if this fails too
            try:
                self._send_message(f"Error processing chunk: {str(e)}", "error")
            except:
                pass
                
            return Response(
                json.dumps({"error": f"Server error: {str(e)}"}),
                content_type="application/json",
                status=500,
            )
    
    def _handle_final_chunk(
        self, file_id, total_chunks, original_filename, ir_model, chunk_dir
    ):
        """
        Process the final chunk with improved transaction management
        """
        final_path = None
        
        try:
            # Generate unique filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            random_suffix = "".join(
                random.choices(string.ascii_letters + string.digits, k=6)
            )
            name, ext = os.path.splitext(original_filename)
            unique_filename = f"{name}_{timestamp}_{random_suffix}{ext}"

            # Create batch folder in upload directory
            batch_folder = f"batch_{ir_model.model}_{timestamp}"
            batch_dir = os.path.join(self.UPLOAD_DIR, batch_folder)
            os.makedirs(batch_dir, exist_ok=True)

            # Define path for the reassembled file
            final_path = os.path.join(batch_dir, unique_filename)
            
            # Combine all chunks into a single file
            with open(final_path, "wb") as outfile:
                for i in range(total_chunks):
                    chunk_path = os.path.join(chunk_dir, f"chunk_{i}")
                    if not os.path.exists(chunk_path):
                        self._send_message(f"Missing chunk {i}", "error")
                        return Response(
                            json.dumps({"error": f"Missing chunk {i}"}),
                            content_type="application/json",
                            status=500,
                        )

                    with open(chunk_path, "rb") as infile:
                        outfile.write(infile.read())

            # Get file size
            file_size = os.path.getsize(final_path)
            self._send_message(
                f"File successfully reassembled: {self._format_bytes(file_size)}",
                "success",
            )

            # Determine content type
            content_type = "text/csv"
            if original_filename.lower().endswith((".xlsx", ".xls")):
                content_type = (
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            # Important! Create import log using a dedicated cursor to avoid transaction conflicts
            import_log_id = None
            import_log = None
            
            # Use a dedicated cursor for creating the import log
            with request.env.registry.cursor() as new_cr:
                try:
                    # Create environment with new cursor
                    env = api.Environment(new_cr, request.env.uid, request.env.context)
                    
                    # Read file into memory for attachment
                    with open(final_path, "rb") as infile:
                        file_content = base64.b64encode(infile.read())
                    
                    # Create import log with transaction
                    import_log = (
                        env["import.log"]
                        .sudo()
                        .create(
                            {
                                "name": f"Import {unique_filename}",
                                "file_name": unique_filename,
                                "original_filename": original_filename,
                                "content_type": content_type,
                                "ir_model_id": ir_model.id,
                                "file": file_content,
                                "status": "pending",  # Always start in pending status
                                "batch_folder": batch_folder,
                                "file_path": final_path,  # Store physical file path
                                "uploaded_by": request.env.user.id,
                            }
                        )
                    )
                    
                    import_log_id = import_log.id
                    
                    # Commit the import log creation to make it available to other processes
                    new_cr.commit()
                    self._send_message("File successfully uploaded and saved", "success")
                except Exception as e:
                    new_cr.rollback()
                    raise e

            # Clean up chunks to save space
            for i in range(total_chunks):
                chunk_path = os.path.join(chunk_dir, f"chunk_{i}")
                if os.path.exists(chunk_path):
                    os.unlink(chunk_path)

            # Clean up temporary directory
            try:
                os.rmdir(chunk_dir)
            except:
                pass

            # Check if we should use the job queue for processing
            use_queue = False  # Default to not using queue
            
            # First check if the queue_job module is installed
            try:
                queue_job_installed = request.env['ir.module.module'].sudo().search([
                    ('name', '=', 'queue_job'),
                    ('state', '=', 'installed')
                ], limit=1)
                
                use_queue = bool(queue_job_installed)
            except Exception as e:
                _logger.warning(f"Error checking for queue_job module: {str(e)}")
            
            # Get the import log from the registry using a new cursor
            with request.env.registry.cursor() as proc_cr:
                env = api.Environment(proc_cr, request.env.uid, request.env.context)
                process_import_log = env['import.log'].sudo().browse(import_log_id)
                
                # Also check if the model has the required method
                if use_queue and not hasattr(process_import_log, 'with_delay'):
                    use_queue = False
                    _logger.warning("ImportLog model doesn't have with_delay method")

                if use_queue:
                    self._send_message("Queueing file for batch processing...", "info")
                    
                    try:
                        # FIXED: Use the new process_file method instead of process_file_batch
                        job = process_import_log.sudo().with_delay(
                            description=f"Process CSV Import {import_log_id}",
                            channel="csv_import"
                        ).process_file()
                        
                        proc_cr.commit()
                        
                        # Log that job was created
                        self._send_message(
                            f"Job queued for processing", "success"
                        )
                        
                        return Response(
                            json.dumps(
                                {
                                    "status": "success",
                                    "import_id": import_log_id,
                                    "message": "File upload complete, processing queued",
                                    "filename": unique_filename,
                                    "file_path": final_path,
                                }
                            ),
                            content_type="application/json",
                        )
                    except Exception as e:
                        proc_cr.rollback()
                        _logger.error(f"Error creating queue job: {str(e)}")
                        # Fallback to direct processing
                        use_queue = False
                
                # If queue.job not available or failed, start processing directly
                if not use_queue:
                    try:
                        # Direct processing - no queue job
                        self._send_message("Starting direct processing (no job queue)...", "info")
                        # Call process_file directly
                        result = process_import_log.sudo().process_file()
                        proc_cr.commit()
                        
                        return Response(
                            json.dumps(
                                {
                                    "status": "success" if result.get('success', False) else "warning",
                                    "import_id": import_log_id,
                                    "message": result.get('message', "Processing started"),
                                    "filename": unique_filename,
                                    "file_path": final_path,
                                }
                            ),
                            content_type="application/json",
                        )
                    except Exception as e:
                        proc_cr.rollback()
                        _logger.error(f"Error starting direct processing: {str(e)}")
                        return Response(
                            json.dumps(
                                {
                                    "status": "warning",
                                    "import_id": import_log_id,
                                    "message": "File upload complete. Please start processing manually.",
                                    "error": str(e),
                                    "filename": unique_filename,
                                    "file_path": final_path,
                                }
                            ),
                            content_type="application/json",
                        )

        except Exception as e:
            _logger.error(f"Error handling final chunk: {str(e)}")
            # Log the full traceback for better debugging
            import traceback
            _logger.error(f"Traceback: {traceback.format_exc()}")
            
            self._send_message(f"Error processing file: {str(e)}", "error")

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

    @http.route('/csv_import/start_import', type='json', auth='user')
    def start_import(self, import_id):
        """Start the import process with proper transaction management"""
        import_id = int(import_id)
        
        # Use a dedicated cursor for this operation to prevent transaction conflicts
        with request.env.registry.cursor() as cr:
            try:
                # Create an environment with the new cursor
                env = api.Environment(cr, request.env.uid, request.env.context)
                
                # Get the import log record
                import_log = env['import.log'].sudo().browse(import_id)
                if not import_log.exists():
                    return {'success': False, 'error': 'Import not found'}
                    
                # Process the file (with transaction management inside method)
                result = import_log.process_file()
                
                cr.commit()
                return result
                
            except Exception as e:
                cr.rollback()
                _logger.error(f"Error starting import: {str(e)}")
                return {'success': False, 'error': str(e)}

    # def _handle_final_chunk(
    #     self, file_id, total_chunks, original_filename, ir_model, chunk_dir
    # ):
    #     """
    #     Process the final chunk:
    #     1. Reassemble the complete file
    #     2. Save it to the upload directory
    #     3. Create an import log
    #     4. Queue a job for processing
    #     """
    #     final_path = None
    #     cr = request.env.cr
        
    #     try:
    #         # Generate unique filename
    #         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #         random_suffix = "".join(
    #             random.choices(string.ascii_letters + string.digits, k=6)
    #         )
    #         name, ext = os.path.splitext(original_filename)
    #         unique_filename = f"{name}_{timestamp}_{random_suffix}{ext}"

    #         # Create batch folder in upload directory
    #         batch_folder = f"batch_{ir_model.model}_{timestamp}"
    #         batch_dir = os.path.join(self.UPLOAD_DIR, batch_folder)
    #         os.makedirs(batch_dir, exist_ok=True)

    #         # Define path for the reassembled file
    #         final_path = os.path.join(batch_dir, unique_filename)
            
    #         # Combine all chunks into a single file
    #         with open(final_path, "wb") as outfile:
    #             for i in range(total_chunks):
    #                 chunk_path = os.path.join(chunk_dir, f"chunk_{i}")
    #                 if not os.path.exists(chunk_path):
    #                     self._send_message(f"Missing chunk {i}", "error")
    #                     return Response(
    #                         json.dumps({"error": f"Missing chunk {i}"}),
    #                         content_type="application/json",
    #                         status=500,
    #                     )

    #                 with open(chunk_path, "rb") as infile:
    #                     outfile.write(infile.read())

    #         # Get file size
    #         file_size = os.path.getsize(final_path)
    #         self._send_message(
    #             f"File successfully reassembled: {self._format_bytes(file_size)}",
    #             "success",
    #         )

    #         # Read the file into memory for attachment
    #         with open(final_path, "rb") as infile:
    #             file_content = base64.b64encode(infile.read())

    #         # Determine content type
    #         content_type = "text/csv"
    #         if original_filename.lower().endswith((".xlsx", ".xls")):
    #             content_type = (
    #                 "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    #             )

    #         # Important! Start a new transaction for import_log creation
    #         cr.commit()

    #         # Create import log
    #         import_log = (
    #             request.env["import.log"]
    #             .sudo()
    #             .create(
    #                 {
    #                     "name": f"Import {unique_filename}",
    #                     "file_name": unique_filename,
    #                     "original_filename": original_filename,
    #                     "content_type": content_type,
    #                     "ir_model_id": ir_model.id,
    #                     "file": file_content,
    #                     "status": "pending",  # Always start in pending status
    #                     "batch_folder": batch_folder,
    #                     "file_path": final_path,  # Store physical file path
    #                     "uploaded_by": request.env.user.id,
    #                 }
    #             )
    #         )
            
    #         # Commit the import log creation immediately to avoid losing it
    #         cr.commit()
    #         self._send_message("File successfully uploaded and saved", "success")

    #         # Clean up chunks
    #         for i in range(total_chunks):
    #             chunk_path = os.path.join(chunk_dir, f"chunk_{i}")
    #             if os.path.exists(chunk_path):
    #                 os.unlink(chunk_path)

    #         # Clean up temporary directory
    #         try:
    #             os.rmdir(chunk_dir)
    #         except:
    #             pass

    #         # Check if we should use the job queue for processing
    #         use_queue = False  # Default to not using queue
            
    #         # First check if the queue_job module is installed
    #         try:
    #             queue_job_installed = request.env['ir.module.module'].sudo().search([
    #                 ('name', '=', 'queue_job'),
    #                 ('state', '=', 'installed')
    #             ], limit=1)
                
    #             use_queue = bool(queue_job_installed)
    #         except Exception as e:
    #             _logger.warning(f"Error checking for queue_job module: {str(e)}")
            
    #         # Also check if the model has the required method
    #         if use_queue and not hasattr(import_log, 'with_delay'):
    #             use_queue = False
    #             _logger.warning("ImportLog model doesn't have with_delay method")

    #         if use_queue:
    #             self._send_message("Queueing file for batch processing...", "info")
                
    #             # Calculate a reasonable batch size based on file size
    #             # For very large files, use smaller batches
    #             if file_size > 1024 * 1024 * 100:  # > 100MB
    #                 batch_size = 5000
    #             else:
    #                 batch_size = 10000
                
    #             try:
    #                 # FIXED: Use the new process_file method instead of process_file_batch
    #                 import_log.sudo().with_delay(
    #                     description=f"Process CSV Import {import_log.id}"
    #                 ).process_file()
                    
    #                 cr.commit()
                    
    #                 # Log that job was created
    #                 self._send_message(
    #                     f"Job queued for processing", "success"
    #                 )
                    
    #                 return Response(
    #                     json.dumps(
    #                         {
    #                             "status": "success",
    #                             "import_id": import_log.id,
    #                             "message": "File upload complete, processing queued",
    #                             "filename": unique_filename,
    #                             "file_path": final_path,
    #                         }
    #                     ),
    #                     content_type="application/json",
    #                 )
    #             except Exception as e:
    #                 _logger.error(f"Error creating queue job: {str(e)}")
    #                 # Fallback to direct processing
    #                 use_queue = False
            
    #         # If queue.job not available or failed, start processing directly
    #         if not use_queue:
    #             try:
    #                 # Direct processing - no queue job
    #                 self._send_message("Starting direct processing (no job queue)...", "info")
    #                 # Call process_file directly
    #                 result = import_log.sudo().process_file()
                    
    #                 return Response(
    #                     json.dumps(
    #                         {
    #                             "status": "success" if result.get('success', False) else "warning",
    #                             "import_id": import_log.id,
    #                             "message": result.get('message', "Processing started"),
    #                             "filename": unique_filename,
    #                             "file_path": final_path,
    #                         }
    #                     ),
    #                     content_type="application/json",
    #                 )
    #             except Exception as e:
    #                 _logger.error(f"Error starting direct processing: {str(e)}")
    #                 return Response(
    #                     json.dumps(
    #                         {
    #                             "status": "warning",
    #                             "import_id": import_log.id,
    #                             "message": "File upload complete. Please start processing manually.",
    #                             "error": str(e),
    #                             "filename": unique_filename,
    #                             "file_path": final_path,
    #                         }
    #                     ),
    #                     content_type="application/json",
    #                 )

    #     except Exception as e:
    #         _logger.error(f"Error handling final chunk: {str(e)}")
    #         # Log the full traceback for better debugging
    #         import traceback
    #         _logger.error(f"Traceback: {traceback.format_exc()}")
            
    #         # Try to rollback the transaction
    #         try:
    #             cr.rollback()
    #         except:
    #             pass
                
    #         self._send_message(f"Error processing file: {str(e)}", "error")

    #         # Clean up final file if it exists
    #         if final_path and os.path.exists(final_path):
    #             try:
    #                 os.unlink(final_path)
    #             except:
    #                 pass

    #         return Response(
    #             json.dumps({"error": f"Error handling final chunk: {str(e)}"}),
    #             content_type="application/json",
    #             status=500,
    #         )

    @http.route(
            "/csv_import/download_template/<int:model_id>", type="http", auth="user"
        )
    def download_template(self, model_id, **kw):
        """Download a template file for any model"""
        try:
            # Get the model
            ir_model = request.env['ir.model'].sudo().browse(int(model_id))
            if not ir_model.exists():
                return Response(
                    json.dumps({"error": "Model not found"}),
                    content_type="application/json",
                    status=404,
                )
                
            model_name = ir_model.model
            if model_name not in request.env:
                return Response(
                    json.dumps({"error": f"Model {model_name} is not accessible"}),
                    content_type="application/json",
                    status=400,
                )
                
            # Generate a template
            self._send_message(f"Generating template for {ir_model.name}...", "info")
            
            # Get fields for this model
            fields_result = self.get_model_fields(model_id)
            if "error" in fields_result:
                return Response(
                    json.dumps({"error": fields_result["error"]}),
                    content_type="application/json",
                    status=500,
                )
                
            fields = fields_result["fields"]
            
            # Generate the template
            content = self._generate_template(ir_model, fields)
            if not content:
                return Response(
                    json.dumps({"error": "Could not generate template"}),
                    content_type="application/json",
                    status=500,
                )
                
            filename = f"{model_name.replace('.', '_')}_template.xlsx"
            
            self._send_message(f"Downloading template for {ir_model.name}...", "info")
            
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
            self._send_message(f"Error generating template: {str(e)}", "error")
            return Response(
                json.dumps({"error": f"Error downloading template: {str(e)}"}),
                content_type="application/json",
                status=500,
            )

    def _generate_template(self, ir_model, fields):
        """Generate a template XLSX file for any model"""
        try:
            import xlsxwriter
            from io import BytesIO

            # Create workbook
            output = BytesIO()
            workbook = xlsxwriter.Workbook(output)
            worksheet = workbook.add_worksheet(
                ir_model.name[:31]  # Excel limits worksheet names to 31 chars
            )

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

    def _format_bytes(self, size):
        """Format bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"

    def _send_message(self, message, message_type="info"):
        """Send log message to frontend and log to server with safe error handling"""
        log_level = {
            "info": _logger.info,
            "error": _logger.error,
            "success": _logger.info,
            "warning": _logger.warning,
        }.get(message_type, _logger.info)

        log_level(f"[{message_type.upper()}] {message}")

        try:
            send_message(request.env, message, message_type, request.env.user.id)
        except Exception as e:
            # Don't let message sending failures affect the main operation
            _logger.warning(f"Failed to send log message: {str(e)}")
            
    @http.route('/csv_import/ws_status', type='json', auth='user')
    def get_websocket_status(self, **kw):
        from ..services.websocket.manager import get_server_status
        status = get_server_status()
        
        # Add additional connectivity test
        import socket
        status['port_test'] = False
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect(('localhost', int(status['port'])))
            s.close()
            status['port_test'] = True
        except:
            pass
        
        return status

    @http.route('/csv_import/start_ws_server', type='json', auth='user')
    def start_websocket_server(self, **kw):
        from ..services.websocket.manager import start_websocket_server
        return {'success': start_websocket_server()}