from odoo import models, fields, api
import logging
import os
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

_logger = logging.getLogger(__name__)

class ImportLog(models.Model):
    _name = "import.log"
    _description = "Import Log"
    _order = "create_date desc"
    _inherit = ["mail.thread", "mail.activity.mixin"]

    name = fields.Char(string="Name", required=True, default="New Import")
    file_name = fields.Char(string="File Name")
    original_filename = fields.Char(string="Original Filename")
    content_type = fields.Char(string="Content Type")
    file_path = fields.Char(string="Physical File Path", 
                            help="Path to the saved file on the server")
    
    ir_model_id = fields.Many2one(
        "ir.model", string="Target Model", required=True, ondelete="cascade"
    )
    model_name = fields.Char(
        related="ir_model_id.model", string="Model Name", store=True
    )
    file = fields.Binary(string="File", attachment=True)
    total_records = fields.Integer(string="Total Records", default=0)
    successful_records = fields.Integer(string="Successful Records", default=0)
    failed_records = fields.Integer(string="Failed Records", default=0)
    
    # Add current position for resume capability
    current_position = fields.Integer(string="Current Position", default=0, 
                                     help="Current position in the file for resuming imports")
    
    status = fields.Selection(
        [
            ("pending", "Pending"),
            ("processing", "Processing"),
            ("completed", "Completed"),
            ("failed", "Failed"),
        ],
        string="Status",
        default="pending",
        tracking=True,
    )
    uploaded_by = fields.Many2one(
        "res.users", string="Uploaded By", default=lambda self: self.env.user.id
    )
    create_date = fields.Datetime(string="Created Date", readonly=True)
    completed_at = fields.Datetime(string="Completed At")
    error_message = fields.Text(string="Error Message")
    technical_details = fields.Text(string="Technical Details")
    batch_folder = fields.Char(string="Batch Folder")
    duplicate_records = fields.Integer(string="Duplicate Records", default=0)
    mapping_ids = fields.One2many(
        "import.field.mapping", "import_log_id", string="Field Mappings"
    )
    
    # New field to track number of retries
    retry_count = fields.Integer(string="Retry Count", default=0)
    
    # New field to store execution time metrics
    execution_time = fields.Float(string="Execution Time (seconds)", default=0)
    
    # New field to track batch progress
    current_batch = fields.Integer(string="Current Batch", default=0)
    total_batches = fields.Integer(string="Total Batches", default=0)
    
    # Add computed field for progress percentage
    progress_percentage = fields.Float(
        string="Progress",
        compute="_compute_progress_percentage",
        store=False
    )
    
    # Add index for faster searches
    _sql_constraints = [
        ('name_unique', 'unique(name)', 'Import name must be unique!')
    ]

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if not vals.get("name") or vals.get("name") == "New Import":
                vals["name"] = (
                    f"Import {self.env['ir.sequence'].next_by_code('import.log.sequence') or 'New'}"
                )
        return super(ImportLog, self).create(vals_list)
        
    @api.depends("current_position", "total_records")
    def _compute_progress_percentage(self):
        for record in self:
            if record.total_records > 0:
                record.progress_percentage = (record.current_position / record.total_records) * 100
            else:
                record.progress_percentage = 0.0

    def process_file(self):
        """Start processing the file using batch jobs"""
        self.ensure_one()
        
        # Safety check - don't reprocess completed imports
        if self.status == 'completed':
            return {'success': True, 'message': 'Import already completed'}
            
        # Verify the file exists
        if not self.file_path or not os.path.exists(self.file_path):
            self.write({
                'status': 'failed',
                'error_message': 'Import file not found at specified path'
            })
            return {'success': False, 'error_message': 'Import file not found'}
        
        # Update status to processing
        if self.status != 'processing':
            self.write({"status": "processing"})
            self.env.cr.commit()  # Commit status change immediately
        
        # Increment retry counter
        self.retry_count += 1
        
        # Determine batch size based on file size
        file_size = os.path.getsize(self.file_path)
        if file_size > 100 * 1024 * 1024:  # > 100MB
            batch_size = 5000
        else:
            batch_size = 10000
            
        # Create job for first batch using with_delay()
        try:
            # Using with_delay() is the proper way to create jobs
            self.with_delay(description=f"Process CSV Import {self.id} (Batch 1)").process_file_batch(0, batch_size)
            self.env.cr.commit()
            
            return {'success': True, 'message': 'Processing started with job queue'}
        except Exception as e:
            _logger.error(f"Error creating job with with_delay(): {str(e)}")
            # If with_delay doesn't work, process directly
            return self.process_file_batch(0, batch_size)
            
    def process_file_batch(self, start_position, batch_size):
        """
        Process a batch of records from the imported file
        
        Args:
            start_position: Record index to start from
            batch_size: Number of records to process in this batch
        """
        self.ensure_one()
        start_time = datetime.now()
        
        try:
            # Ensure file exists
            if not self.file_path or not os.path.exists(self.file_path):
                raise ValueError("Import file not found at specified path")
                
            # Update current position if needed
            if self.current_position < start_position:
                self.current_position = start_position
            
            # Read the file and get batch information
            file_ext = os.path.splitext(self.file_path)[1].lower()
            
            # First determine total records if not set
            if self.total_records <= 0:
                # Count total records in file to set up batching
                if file_ext in ('.xlsx', '.xls'):
                    # For Excel, use pandas with chunksize
                    df_info = pd.read_excel(self.file_path, nrows=1)
                    # Count total rows (slow but accurate)
                    with pd.ExcelFile(self.file_path) as xlsx:
                        sheet_name = xlsx.sheet_names[0]  # First sheet
                        # Get number of rows in specific sheet - subtract 1 for header
                        self.total_records = xlsx.book.sheet_by_name(sheet_name).nrows - 1
                else:
                    # For CSV, faster counting
                    with open(self.file_path, 'r') as f:
                        # Subtract 1 for header
                        self.total_records = sum(1 for _ in f) - 1
                
                # Calculate total batches
                self.total_batches = (self.total_records + batch_size - 1) // batch_size
                
                # Commit the total records count
                self.env.cr.commit()
            
            # Determine current batch number
            current_batch = start_position // batch_size + 1
            self.current_batch = current_batch
            
            _logger.info(f"Processing batch {current_batch}/{self.total_batches} for import {self.id}")
            
            # Process the batch using CSV processor with batch limits
            from ..services.csv_processor import CSVProcessor
            processor = CSVProcessor(self)
            
            # Set the end position for this batch
            end_position = min(start_position + batch_size, self.total_records)
            
            # Process only this batch
            result = processor.process_batch(start_position, end_position)
            
            # Calculate execution time
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Update the import log with results from this batch
            try:
                # Make sure transaction is in good state
                if self.env.cr._cnx.status != psycopg2.extensions.STATUS_READY:
                    self.env.cr.rollback()
                    
                # Determine if we've completed all batches
                is_last_batch = end_position >= self.total_records
                
                # Set status based on completion
                if is_last_batch:
                    status = "completed"
                    completed_at = fields.Datetime.now()
                else:
                    status = "processing"
                    completed_at = False
                
                # Update the import log
                self.write({
                    "status": status,
                    "successful_records": self.successful_records + result.get("successful", 0),
                    "failed_records": self.failed_records + result.get("failed", 0),
                    "duplicate_records": self.duplicate_records + result.get("duplicates", 0),
                    "current_position": end_position,
                    "execution_time": self.execution_time + execution_time,
                    "completed_at": completed_at
                })
                self.env.cr.commit()
                
                # If not the last batch, queue the next batch using with_delay
                if not is_last_batch:
                    next_batch = current_batch + 1
                    
                    # Queue next batch with with_delay
                    self.with_delay(
                        description=f"Process CSV Import {self.id} (Batch {next_batch})"
                    ).process_file_batch(end_position, batch_size)
                    
                    self.env.cr.commit()
                    
                    _logger.info(f"Queued next batch {next_batch} for import {self.id}")
                
                return {
                    'success': True,
                    'batch': current_batch,
                    'total_batches': self.total_batches,
                    'message': f"Processed batch {current_batch}/{self.total_batches}"
                }
                
            except Exception as e:
                _logger.error(f"Error updating import progress: {str(e)}")
                # Try to rollback in case the transaction is aborted
                try:
                    self.env.cr.rollback()
                except:
                    pass
                    
                # Try a direct update with SQL to avoid ORM issues
                try:
                    self.env.cr.execute("""
                        UPDATE import_log 
                        SET current_position = %s,
                            current_batch = %s,
                            status = %s
                        WHERE id = %s
                    """, (
                        end_position,
                        current_batch,
                        'processing',
                        self.id
                    ))
                    self.env.cr.commit()
                    
                    # Even if update fails, try to queue next batch
                    if not is_last_batch:
                        try:
                            next_batch = current_batch + 1
                            self.with_delay(
                                description=f"Process CSV Import {self.id} (Batch {next_batch})"
                            ).process_file_batch(end_position, batch_size)
                            self.env.cr.commit()
                        except Exception as e2:
                            _logger.error(f"Failed to queue next batch: {e2}")
                except:
                    pass
                    
                return {
                    'success': False,
                    'error_message': f"Error updating import progress: {str(e)}"
                }
                
        except Exception as e:
            import traceback
            
            error_trace = traceback.format_exc()
            error_message = f"Error processing batch: {str(e)}"
            _logger.error(error_message)
            _logger.error(error_trace)
            
            # Try to update the status
            try:
                # Check if transaction is in good state
                if self.env.cr._cnx.status != psycopg2.extensions.STATUS_READY:
                    self.env.cr.rollback()
                    
                # Don't mark as failed if we have made progress and can resume
                if self.current_position > 0:
                    status = "processing"
                    completed_at = False
                else:
                    status = "failed"
                    completed_at = fields.Datetime.now()
                    
                self.write({
                    "status": status,
                    "error_message": error_message,
                    "technical_details": error_trace,
                    "completed_at": completed_at,
                    "execution_time": self.execution_time + (datetime.now() - start_time).total_seconds()
                })
                self.env.cr.commit()
            except Exception as ex:
                _logger.error(f"Failed to update import log status: {ex}")
                try:
                    self.env.cr.rollback()
                    
                    # Try a direct SQL update
                    self.env.cr.execute("""
                        UPDATE import_log 
                        SET status = %s, 
                            error_message = %s, 
                            technical_details = %s
                        WHERE id = %s
                    """, (
                        "processing" if self.current_position > 0 else "failed",
                        error_message,
                        error_trace,
                        self.id
                    ))
                    self.env.cr.commit()
                except:
                    pass
                    
            return {
                'success': False,
                'error_message': error_message,
                'technical_details': error_trace
            }

    def retry_import(self):
        """Retry the import process from where it left off"""
        self.ensure_one()
        
        if self.status not in ['failed', 'processing']:
            raise models.ValidationError('Only failed or processing imports can be retried')
            
        # Reset error fields
        self.write({
            'error_message': False,
            'technical_details': False,
        })
        
        # Start the import process
        return self.process_file()
        
    def reset_import(self):
        """Reset the import to start from the beginning"""
        self.ensure_one()
        
        if self.status == 'completed':
            raise models.ValidationError('Completed imports cannot be reset')
            
        # Reset progress fields
        self.write({
            'current_position': 0,
            'current_batch': 0,
            'successful_records': 0,
            'failed_records': 0,
            'duplicate_records': 0,
            'error_message': False,
            'technical_details': False,
            'status': 'pending',
            'retry_count': 0,
            'execution_time': 0,
        })
        
        self.env.cr.commit()  # Commit the reset
        return True

    def purge_import_files(self, days=30):
        """Delete physical files of old imports"""
        cutoff_date = fields.Datetime.now() - timedelta(days=days)
        old_imports = self.search([
            ('create_date', '<', cutoff_date),
            ('status', 'in', ['completed', 'failed'])
        ])
        
        count = 0
        for imp in old_imports:
            if imp.file_path and os.path.exists(imp.file_path):
                try:
                    os.unlink(imp.file_path)
                    count += 1
                except Exception as e:
                    _logger.error(f"Failed to delete import file {imp.file_path}: {e}")
                    
        return count
        
    def get_importable_fields(self):
        """Get fields that can be imported for this model"""
        self.ensure_one()

        # Get all fields for the model that can be imported
        model_obj = self.env[self.model_name]
        importable_fields = []

        for field_name, field in model_obj._fields.items():
            # Skip non-storable fields, many2many fields, one2many fields and compute fields without inverse
            if (
                not field.store
                or field.type in ["many2many", "one2many", "binary", "reference"]
                or (field.compute and not field.inverse)
            ):
                continue

            field_info = {
                "name": field_name,
                "string": field.string,
                "type": field.type,
                "required": field.required,
                "relation": (
                    field.comodel_name
                    if field.type in ["many2one", "many2many"]
                    else False
                ),
            }
            importable_fields.append(field_info)

        return importable_fields

class ImportFieldMapping(models.Model):
    _name = "import.field.mapping"
    _description = "Import Field Mapping"

    import_log_id = fields.Many2one(
        "import.log", string="Import Log", ondelete="cascade"
    )
    csv_field = fields.Char(string="CSV Field", required=True)
    model_field = fields.Char(string="Model Field", required=True)
    field_type = fields.Char(string="Field Type")
    default_value = fields.Char(string="Default Value")
    required = fields.Boolean(string="Required")
    notes = fields.Text(string="Notes")


# from odoo import models, fields, api
# import logging
# from datetime import datetime

# _logger = logging.getLogger(__name__)

# class ImportLog(models.Model):
#     _name = "import.log"
#     _description = "Import Log"
#     _order = "create_date desc"
#     _inherit = ["mail.thread", "mail.activity.mixin"]

#     name = fields.Char(string="Name", required=True, default="New Import")
#     file_name = fields.Char(string="File Name")
#     original_filename = fields.Char(string="Original Filename")
#     content_type = fields.Char(string="Content Type")
    
#     ir_model_id = fields.Many2one(
#         "ir.model", string="Target Model", required=True, ondelete="cascade"
#     )
#     model_name = fields.Char(
#         related="ir_model_id.model", string="Model Name", store=True
#     )
#     file = fields.Binary(string="File", attachment=True)
#     total_records = fields.Integer(string="Total Records", default=0)
#     successful_records = fields.Integer(string="Successful Records", default=0)
#     failed_records = fields.Integer(string="Failed Records", default=0)
    
#     # Add current position for resume capability
#     current_position = fields.Integer(string="Current Position", default=0, 
#                                      help="Current position in the file for resuming imports")
    
#     status = fields.Selection(
#         [
#             ("pending", "Pending"),
#             ("processing", "Processing"),
#             ("completed", "Completed"),
#             ("failed", "Failed"),
#         ],
#         string="Status",
#         default="pending",
#         tracking=True,
#     )
#     uploaded_by = fields.Many2one(
#         "res.users", string="Uploaded By", default=lambda self: self.env.user.id
#     )
#     create_date = fields.Datetime(string="Created Date", readonly=True)
#     completed_at = fields.Datetime(string="Completed At")
#     error_message = fields.Text(string="Error Message")
#     technical_details = fields.Text(string="Technical Details")
#     batch_folder = fields.Char(string="Batch Folder")
#     duplicate_records = fields.Integer(string="Duplicate Records", default=0)
#     mapping_ids = fields.One2many(
#         "import.field.mapping", "import_log_id", string="Field Mappings"
#     )
    
#     # New field to track number of retries
#     retry_count = fields.Integer(string="Retry Count", default=0)
    
#     # New field to store execution time metrics
#     execution_time = fields.Float(string="Execution Time (seconds)", default=0)
    
#     # Add index for faster searches
#     _sql_constraints = [
#         ('name_unique', 'unique(name)', 'Import name must be unique!')
#     ]

#     @api.model_create_multi
#     def create(self, vals_list):
#         for vals in vals_list:
#             if not vals.get("name") or vals.get("name") == "New Import":
#                 vals["name"] = (
#                     f"Import {self.env['ir.sequence'].next_by_code('import.log.sequence') or 'New'}"
#                 )
#         return super(ImportLog, self).create(vals_list)

#     def process_file(self):
#         """Process the imported file with auto-resume capability"""
#         self.ensure_one()
#         start_time = datetime.now()
        
#         # Update status to processing
#         if self.status != 'processing':
#             self.write({"status": "processing"})
#             self.env.cr.commit()  # Important: commit status change immediately

#         # Import the CSV processor
#         try:
#             # Increment retry counter
#             self.retry_count += 1
            
#             from ..services.csv_processor import CSVProcessor

#             processor = CSVProcessor(self)
#             result = processor.process()

#             # Calculate execution time
#             execution_time = (datetime.now() - start_time).total_seconds()
            
#             # Update the import log with results
#             if result.get("success"):
#                 self.write(
#                     {
#                         "status": "completed",
#                         "total_records": result.get("total_records", 0),
#                         "successful_records": result.get("successful_records", 0),
#                         "failed_records": result.get("failed_records", 0),
#                         "duplicate_records": result.get("duplicate_records", 0),
#                         "completed_at": fields.Datetime.now(),
#                         "execution_time": execution_time,
#                     }
#                 )
#                 self.env.cr.commit()  # Commit the final result
#                 return True
#             else:
#                 status = "failed"
#                 # If we have progress and can resume, keep as processing
#                 if self.current_position > 0:
#                     status = "processing"
                    
#                 self.write(
#                     {
#                         "status": status,
#                         "error_message": result.get("error_message", "Unknown error"),
#                         "technical_details": result.get("technical_details", ""),
#                         "completed_at": fields.Datetime.now() if status == "failed" else False,
#                         "execution_time": execution_time,
#                     }
#                 )
#                 self.env.cr.commit()  # Commit the result
#                 return False

#         except Exception as e:
#             import traceback

#             _logger.error(f"Error processing file: {str(e)}")
#             execution_time = (datetime.now() - start_time).total_seconds()
            
#             status = "failed"
#             # If we have progress and can resume, keep as processing
#             if self.current_position > 0:
#                 status = "processing"
                
#             self.write(
#                 {
#                     "status": status,
#                     "error_message": str(e),
#                     "technical_details": traceback.format_exc(),
#                     "completed_at": fields.Datetime.now() if status == "failed" else False,
#                     "execution_time": execution_time,
#                 }
#             )
#             self.env.cr.commit()  # Commit the error status
#             return False

#     def retry_import(self):
#         """Retry the import process from where it left off"""
#         self.ensure_one()
        
#         if self.status not in ['failed', 'processing']:
#             raise models.ValidationError('Only failed or processing imports can be retried')
            
#         # Reset error fields
#         self.write({
#             'error_message': False,
#             'technical_details': False,
#         })
        
#         # Start the import process
#         return self.process_file()
        
#     def reset_import(self):
#         """Reset the import to start from the beginning"""
#         self.ensure_one()
        
#         if self.status == 'completed':
#             raise models.ValidationError('Completed imports cannot be reset')
            
#         # Reset progress fields
#         self.write({
#             'current_position': 0,
#             'successful_records': 0,
#             'failed_records': 0,
#             'duplicate_records': 0,
#             'error_message': False,
#             'technical_details': False,
#             'status': 'pending',
#             'retry_count': 0,
#         })
        
#         self.env.cr.commit()  # Commit the reset
#         return True
        
#     def get_importable_fields(self):
#         """Get fields that can be imported for this model"""
#         self.ensure_one()

#         # Get all fields for the model that can be imported
#         model_obj = self.env[self.model_name]
#         importable_fields = []

#         for field_name, field in model_obj._fields.items():
#             # Skip non-storable fields, many2many fields, one2many fields and compute fields without inverse
#             if (
#                 not field.store
#                 or field.type in ["many2many", "one2many", "binary", "reference"]
#                 or (field.compute and not field.inverse)
#             ):
#                 continue

#             field_info = {
#                 "name": field_name,
#                 "string": field.string,
#                 "type": field.type,
#                 "required": field.required,
#                 "relation": (
#                     field.comodel_name
#                     if field.type in ["many2one", "many2many"]
#                     else False
#                 ),
#             }
#             importable_fields.append(field_info)

#         return importable_fields

# class ImportFieldMapping(models.Model):
#     _name = "import.field.mapping"
#     _description = "Import Field Mapping"

#     import_log_id = fields.Many2one(
#         "import.log", string="Import Log", ondelete="cascade"
#     )
#     csv_field = fields.Char(string="CSV Field", required=True)
#     model_field = fields.Char(string="Model Field", required=True)
#     field_type = fields.Char(string="Field Type")
#     default_value = fields.Char(string="Default Value")
#     required = fields.Boolean(string="Required")
#     notes = fields.Text(string="Notes")
