from odoo import models, fields, api
import logging
import os
import psycopg2
from datetime import datetime, timedelta
import math
import multiprocessing
import time
import json
from contextlib import contextmanager

_logger = logging.getLogger(__name__)

class ImportLog(models.Model):
    _name = "import.log"
    _description = "Import Log"
    _order = "create_date desc"
    _inherit = ["mail.thread", "mail.activity.mixin"]

    name = fields.Char(string="Name", required=True, default="New Import", tracking=True)
    file_name = fields.Char(string="File Name", tracking=True)
    original_filename = fields.Char(string="Original Filename")
    content_type = fields.Char(string="Content Type")
    file_path = fields.Char(string="Physical File Path", help="Path to the saved file on the server")
    
    ir_model_id = fields.Many2one(
        "ir.model", string="Target Model", required=True, ondelete="cascade", tracking=True
    )
    model_name = fields.Char(
        related="ir_model_id.model", string="Model Name", store=True
    )
    file = fields.Binary(string="File", attachment=True)
    file_size = fields.Integer(string="File Size (bytes)", readonly=True)
    
    # Processing metrics
    total_records = fields.Integer(string="Total Records", default=0)
    successful_records = fields.Integer(string="Successful Records", default=0)
    failed_records = fields.Integer(string="Failed Records", default=0)
    duplicate_records = fields.Integer(string="Duplicate Records", default=0)
    skipped_records = fields.Integer(string="Skipped Records", default=0)
    
    # Processing settings
    batch_size = fields.Integer(string="Batch Size", default=10000)
    parallel_jobs = fields.Integer(string="Parallel Jobs", default=4)
    current_position = fields.Integer(string="Current Position", default=0, help="Current position in the file")
    
    status = fields.Selection(
        [
            ("pending", "Pending"),
            ("processing", "Processing"),
            ("paused", "Paused"),
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
    started_at = fields.Datetime(string="Started At")
    completed_at = fields.Datetime(string="Completed At")
    
    error_message = fields.Text(string="Error Message")
    technical_details = fields.Text(string="Technical Details")
    log_messages = fields.Text(string="Processing Log", readonly=True)
    
    # Batch progress tracking
    current_batch = fields.Integer(string="Current Batch", default=0)
    total_batches = fields.Integer(string="Total Batches", default=0)
    completed_jobs = fields.Integer(string="Completed Jobs", default=0)
    
    # Additional metadata
    batch_folder = fields.Char(string="Batch Folder")
    retry_count = fields.Integer(string="Retry Count", default=0)
    execution_time = fields.Float(string="Execution Time (seconds)", default=0)
    
    # Performance metrics
    records_per_second = fields.Float(
        string="Records per Second", 
        compute="_compute_performance_metrics"
    )
    estimated_time_left = fields.Char(
        string="Estimated Time Left",
        compute="_compute_estimated_time_left"
    )
    progress_percentage = fields.Float(
        string="Progress", 
        compute="_compute_progress_percentage"
    )
    
    # Relationships
    mapping_ids = fields.One2many(
        "import.field.mapping", "import_log_id", string="Field Mappings"
    )

    summary = fields.Text(string="Import Summary", help="JSON summary of import results")
    delete_mode = fields.Boolean(string="Delete Mode", default=False, 
                                help="When enabled, records with matching unique identifiers will be deleted")
    unique_identifier_field = fields.Char(string="Unique Identifier Field", 
                                        help="Field used to identify records for deletion")
    delete_progress = fields.Text(string="Delete Progress", 
                             help="JSON tracking of delete operation progress")
    delete_mode = fields.Boolean(string="Delete Mode", default=False, 
                                help="When enabled, records with matching unique identifiers will be deleted")
    unique_identifier_field = fields.Char(string="Unique Identifier Field", 
                                        help="Field used to identify records for deletion")
    delete_progress = fields.Text(string="Delete Progress", 
                             help="JSON tracking of delete operation progress")
    
    # SQL constraints
    _sql_constraints = [
        ('name_unique', 'unique(name)', 'Import name must be unique!')
    ]

    # ---------- CRUD Methods ----------
    
    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if not vals.get("name") or vals.get("name") == "New Import":
                vals["name"] = f"Import {self.env['ir.sequence'].next_by_code('import.log.sequence') or 'New'}"
            
            # Calculate file size if path is provided
            if vals.get('file_path') and os.path.exists(vals.get('file_path')):
                vals['file_size'] = os.path.getsize(vals.get('file_path'))
                
        return super(ImportLog, self).create(vals_list)
        
    # ---------- Computed Fields ----------
    
    @api.depends("current_position", "total_records")
    def _compute_progress_percentage(self):
        for record in self:
            if record.total_records > 0:
                record.progress_percentage = min(100, (record.current_position / record.total_records) * 100)
            else:
                record.progress_percentage = 0.0
                
    @api.depends("execution_time", "successful_records")
    def _compute_performance_metrics(self):
        for record in self:
            if record.execution_time > 0 and record.successful_records > 0:
                record.records_per_second = record.successful_records / record.execution_time
            else:
                record.records_per_second = 0.0
                
    @api.depends("records_per_second", "current_position", "total_records")
    def _compute_estimated_time_left(self):
        for record in self:
            if record.records_per_second > 0 and record.total_records > record.current_position:
                # Calculate seconds left
                records_left = record.total_records - record.current_position
                seconds_left = records_left / record.records_per_second
                
                # Format into human readable time
                hours, remainder = divmod(seconds_left, 3600)
                minutes, seconds = divmod(remainder, 60)
                
                if hours > 0:
                    record.estimated_time_left = f"{int(hours)}h {int(minutes)}m"
                elif minutes > 0:
                    record.estimated_time_left = f"{int(minutes)}m {int(seconds)}s"
                else:
                    record.estimated_time_left = f"{int(seconds)}s"
            else:
                record.estimated_time_left = "Unknown"
                
    # ---------- Business Methods ----------
    def process_file(self):
        """Process the file using direct SQL to avoid concurrency issues"""
        self.ensure_one()
        
        # Start with a completely new cursor for this operation
        with self.env.registry.cursor() as new_cr:
            try:
                import_id = self.id
                
                # Use strong locking to prevent concurrent processing
                new_cr.execute("SELECT pg_advisory_xact_lock(%s)", (import_id,))
                
                # First check if file exists and status is valid
                new_cr.execute("""
                    SELECT file_path, status, model_name 
                    FROM import_log 
                    WHERE id = %s
                    FOR UPDATE
                """, (import_id,))
                
                result = new_cr.fetchone()
                if not result:
                    return {'success': False, 'error': 'Import not found'}
                    
                file_path, status, model_name = result
                
                # Validate status
                if status == 'completed':
                    return {'success': True, 'message': 'Import already completed'}
                
                if status in ('processing', 'failed'):
                    # Reset counters for retry
                    new_cr.execute("""
                        UPDATE import_log
                        SET completed_jobs = 0,
                            error_message = NULL,
                            technical_details = NULL
                        WHERE id = %s
                    """, (import_id,))
                
                # Check file path
                if not file_path or not os.path.exists(file_path):
                    new_cr.execute("""
                        UPDATE import_log
                        SET status = 'failed',
                            error_message = 'Import file not found at specified path'
                        WHERE id = %s
                    """, (import_id,))
                    new_cr.commit()
                    return {'success': False, 'error_message': 'Import file not found'}
                
                # Start time
                start_time = fields.Datetime.now()
                
                # IMPORTANT: Get current record count before import for verification
                try:
                    # Get the target table name
                    env = api.Environment(new_cr, self.env.uid, self.env.context)
                    model_obj = env[model_name]
                    table_name = model_obj._table
                    
                    # Count existing records
                    new_cr.execute(f"SELECT COUNT(*) FROM {table_name}")
                    record_count_before = new_cr.fetchone()[0]
                    _logger.info(f"Record count before import: {record_count_before}")
                except Exception as count_error:
                    _logger.warning(f"Could not get record count before import: {str(count_error)}")
                    record_count_before = 0
                
                # Get configuration or use defaults
                try:
                    default_batch_size = int(self.env['ir.config_parameter'].sudo().get_param(
                        'csv_import.batch_size', '10000'))
                    default_parallel_jobs = int(self.env['ir.config_parameter'].sudo().get_param(
                        'csv_import.parallel_jobs', '4'))
                except:
                    default_batch_size = 10000
                    default_parallel_jobs = 4
                
                # Adjust batch size based on file size
                file_size = os.path.getsize(file_path)
                batch_size = default_batch_size
                parallel_jobs = default_parallel_jobs
                
                if file_size > 1024 * 1024 * 500:  # > 500MB
                    batch_size = 50000
                elif file_size > 1024 * 1024 * 100:  # > 100MB
                    batch_size = 20000
                    
                # Adjust parallelism based on CPU
                available_cores = multiprocessing.cpu_count()
                if available_cores > 4:
                    parallel_jobs = min(available_cores - 2, 8)
                
                # Count records with external tools or estimate
                record_count = self._count_file_records(file_path)
                
                # Calculate batches
                total_batches = math.ceil(record_count / batch_size) if record_count > 0 else 1
                
                # Update ALL parameters in a single SQL statement
                new_cr.execute("""
                    UPDATE import_log
                    SET status = 'processing',
                        started_at = %s,
                        batch_size = %s,
                        parallel_jobs = %s,
                        total_records = %s,
                        total_batches = %s,
                        retry_count = retry_count + 1,
                        execution_time = 0,
                        error_message = NULL,
                        technical_details = NULL
                    WHERE id = %s
                """, (
                    start_time,
                    batch_size,
                    parallel_jobs,
                    record_count,
                    total_batches,
                    import_id
                ))
                
                # Commit before launching jobs to ensure our changes are visible to them
                new_cr.commit()
                
                # Save record count in context for verification later
                context = dict(self.env.context, record_count_before_import=record_count_before)
                
                # Launch parallel jobs with the new cursor and context
                env = api.Environment(new_cr, self.env.uid, context)
                current_import = env['import.log'].browse(import_id)
                return current_import._launch_parallel_jobs_with_cursor(new_cr, start_time)
                    
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                _logger.error(f"Error in process_file: {str(e)}\n{error_trace}")
                
                try:
                    new_cr.execute("""
                        UPDATE import_log
                        SET status = 'failed',
                            error_message = %s,
                            technical_details = %s
                        WHERE id = %s
                    """, (str(e), error_trace, import_id))
                    new_cr.commit()
                except:
                    pass
                    
                return {'success': False, 'error': str(e)}

    def _count_file_records(self, file_path):
        """Count records in a file with better error handling"""
        try:
            import pandas as pd
            import chardet
            
            file_ext = os.path.splitext(file_path)[1].lower()
            
            # For Excel files
            if file_ext in ('.xlsx', '.xls'):
                with pd.ExcelFile(file_path) as xlsx:
                    sheet_name = xlsx.sheet_names[0]  # First sheet
                    record_count = xlsx.book.sheet_by_name(sheet_name).nrows - 1
            else:
                # For CSV, use faster line counting
                with open(file_path, 'rb') as f:
                    # Read a sample to detect encoding
                    sample = f.read(min(10000, os.path.getsize(file_path)))
                    detection = chardet.detect(sample)
                    encoding = detection["encoding"] or "utf-8"
                    
                # Count lines with proper encoding
                with open(file_path, 'r', encoding=encoding) as f:
                    record_count = sum(1 for _ in f) - 1
                    
            _logger.info(f"Counted {record_count} total records in file")
            return record_count
            
        except Exception as e:
            _logger.warning(f"Error counting records: {str(e)}")
            # Estimate based on file size
            file_size = os.path.getsize(file_path)
            if file_path.lower().endswith(('.xlsx', '.xls')):
                record_count = max(1, int(file_size / 500))  # Excel files are larger
            else:
                record_count = max(1, int(file_size / 200))  # Assume ~200 bytes per record
            
            _logger.info(f"Estimated {record_count} records based on file size")
            return record_count

    def _safe_update(self, values):
        """Safely update import log fields using direct SQL with concurrency handling"""
        # Skip if no values to update
        if not values:
            return False
            
        # Prepare SQL parts
        set_parts = []
        params = []
        
        # Build dynamic SQL update
        for field, value in values.items():
            set_parts.append(f'"{field}" = %s')
            params.append(value)
        
        # Add ID to parameters
        params.append(self.id)
        
        # Execute with retries
        max_retries = 5
        for attempt in range(max_retries):
            try:
                with self.env.registry.cursor() as new_cr:
                    # Use serializable isolation and advisory lock
                    new_cr.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                    lock_id = self.id + 80000000
                    new_cr.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
                    
                    # Build and execute SQL update
                    sql = f"""
                        UPDATE import_log
                        SET {", ".join(set_parts)}
                        WHERE id = %s
                    """
                    new_cr.execute(sql, params)
                    
                    # Commit changes
                    new_cr.commit()
                    
                    # Update local object too
                    for field, value in values.items():
                        setattr(self, field, value)
                        
                    return True
            except Exception as e:
                _logger.warning(f"Retry {attempt+1}/{max_retries} updating fields {list(values.keys())}: {str(e)}")
                time.sleep(min(2 ** attempt, 16))
        
        # If all retries failed, at least update local values
        for field, value in values.items():
            setattr(self, field, value)
            
        _logger.error(f"Failed to update fields {list(values.keys())} after {max_retries} attempts")
        return False
        
    def _optimize_processing_parameters(self):
        """Optimize batch size and parallelism with better concurrency handling"""
        # Get file size and system parameters
        file_size = self.file_size or (os.path.getsize(self.file_path) if self.file_path else 0)
        
        # Calculate optimal parameters
        try:
            default_batch_size = int(self.env['ir.config_parameter'].sudo().get_param(
                'csv_import.batch_size', '10000'))
            default_parallel_jobs = int(self.env['ir.config_parameter'].sudo().get_param(
                'csv_import.parallel_jobs', '4'))
        except:
            default_batch_size = 10000
            default_parallel_jobs = 4
            
        # Determine optimal values
        batch_size = default_batch_size
        parallel_jobs = default_parallel_jobs
        
        # Adjust batch size based on file size
        if file_size > 1024 * 1024 * 500:  # > 500MB
            batch_size = 50000
        elif file_size > 1024 * 1024 * 100:  # > 100MB
            batch_size = 20000
            
        # Adjust parallelism based on available CPU cores
        available_cores = multiprocessing.cpu_count()
        if available_cores > 4:
            # Leave cores for the main Odoo process
            parallel_jobs = min(available_cores - 2, 8)
            
        # Log the values we're going to set
        self._log_message(f"Optimized processing: batch size={batch_size}, parallel jobs={parallel_jobs}")
        
        # Update parameters using the safe method
        self._safe_update({
            'batch_size': batch_size,
            'parallel_jobs': parallel_jobs,
            'retry_count': self.retry_count + 1
        })
        
    def _count_total_records(self):
        """Efficiently count total records in the file with improved concurrency handling"""
        try:
            # First try to count records
            record_count = 0
            file_size = os.path.getsize(self.file_path) if self.file_path else 0
            
            try:
                import pandas as pd
                import chardet
                
                file_ext = os.path.splitext(self.file_path)[1].lower()
                
                # For Excel files
                if file_ext in ('.xlsx', '.xls'):
                    with pd.ExcelFile(self.file_path) as xlsx:
                        sheet_name = xlsx.sheet_names[0]  # First sheet
                        record_count = xlsx.book.sheet_by_name(sheet_name).nrows - 1
                else:
                    # For CSV, use faster line counting
                    with open(self.file_path, 'rb') as f:
                        # Read a sample to detect encoding
                        sample = f.read(min(10000, os.path.getsize(self.file_path)))
                        detection = chardet.detect(sample)
                        encoding = detection["encoding"] or "utf-8"
                        
                    # Count lines with proper encoding
                    with open(self.file_path, 'r', encoding=encoding) as f:
                        record_count = sum(1 for _ in f) - 1
                        
                self._log_message(f"Counted {record_count} total records in file")
                
            except Exception as e:
                _logger.warning(f"Error counting records: {str(e)}")
                # Estimate based on file size
                if self.file_path.lower().endswith(('.xlsx', '.xls')):
                    record_count = max(1, int(file_size / 500))  # Excel files are larger
                else:
                    record_count = max(1, int(file_size / 200))  # Assume ~200 bytes per record
                
                self._log_message(f"Estimated {record_count} records based on file size")
            
            # Update total_records using safe method
            self._safe_update({'total_records': record_count})
                    
        except Exception as e:
            _logger.error(f"Critical error in _count_total_records: {str(e)}")
            # Use a fallback value
            self._safe_update({'total_records': 1000})
                
    def _launch_parallel_jobs_with_cursor(self, cr, start_time):
        """Launch parallel jobs with an explicit cursor to avoid concurrency issues"""
        # Get current values directly from cursor
        cr.execute("""
            SELECT id, total_records, batch_size, parallel_jobs, total_batches 
            FROM import_log 
            WHERE id = %s
        """, (self.id,))
        
        import_id, total_records, batch_size, parallel_jobs, total_batches = cr.fetchone()
        
        # Calculate records per job
        records_per_job = math.ceil(total_records / parallel_jobs)
        
        # Create segment jobs
        for job_num in range(parallel_jobs):
            start_position = job_num * records_per_job
            
            # Skip if beyond total
            if start_position >= total_records:
                continue
                
            # Calculate end position
            end_position = min((job_num + 1) * records_per_job, total_records)
            
            # Calculate batch number
            batch_num = start_position // batch_size + 1
            
            # Create the job
            if hasattr(self, 'with_delay'):
                self.with_delay(
                    priority=job_num+5,
                    description=f"CSV Import {import_id} - Segment {job_num+1}/{parallel_jobs}"
                ).process_file_segment(
                    start_position=start_position,
                    end_position=end_position,
                    batch_num=batch_num,
                    job_index=job_num+1,
                    start_time=start_time
                )
            else:
                # Process directly without queueing
                self.process_file_segment(
                    start_position=start_position,
                    end_position=end_position,
                    batch_num=batch_num,
                    job_index=job_num+1,
                    start_time=start_time
                )
        
        return {
            'success': True,
            'message': f"Started parallel processing with {parallel_jobs} jobs",
            'total_batches': total_batches
        }

    @contextmanager
    def advisory_lock(self, lock_id=None):
        """
        Get an advisory lock to prevent concurrent updates to the same import log.
        This ensures operations on the same import_log are serialized.
        """
        if lock_id is None:
            # Generate a lock ID based on the import log ID
            lock_id = self.id + 10000000  # Add offset to avoid collision with other locks
            
        # Acquire the lock (non-blocking)
        self.env.cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (lock_id,))
        acquired = self.env.cr.fetchone()[0]
        
        if not acquired:
            # Wait a moment and try again (up to 3 times)
            for attempt in range(3):
                time.sleep(0.5)
                self.env.cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (lock_id,))
                acquired = self.env.cr.fetchone()[0]
                if acquired:
                    break
                    
        try:
            yield acquired
        finally:
            # Lock is automatically released at the end of the transaction
            pass
        
    def _create_segment_job(self, start_position, end_position, batch_num, job_index, start_time):
        """Create a job for processing a file segment"""
        # Check if queue_job is available
        if hasattr(self, 'with_delay'):
            # Create a job with priority based on segment order
            self.with_delay(
                priority=job_index+5,  # Higher priority for earlier segments
                description=f"CSV Import {self.id} - Segment {job_index}/{self.parallel_jobs}"
            ).process_file_segment(
                start_position=start_position,
                end_position=end_position,
                batch_num=batch_num,
                job_index=job_index,
                start_time=start_time
            )
        else:
            # Process directly without queueing (slower but works without queue_job)
            self._log_message(
                f"Processing segment {job_index} directly (queue_job not available)", 
                "info"
            )
            self.process_file_segment(
                start_position=start_position,
                end_position=end_position,
                batch_num=batch_num,
                job_index=job_index,
                start_time=start_time
            )
        
    def process_file_segment(self, start_position, end_position, batch_num, job_index, start_time):
        """
        Process a segment of the import file using the enterprise CSV processor
        
        Args:
            start_position: Start record index 
            end_position: End record index (exclusive)
            batch_num: Starting batch number for this segment
            job_index: Index of this job (1-based)
            start_time: When the overall import started
        """
        self.ensure_one()
        segment_start = datetime.now()
        
        # Check for excessive retries to prevent infinite loops
        if self.retry_count > 10:  # Maximum allowed retries
            error_message = f"Import job aborted after {self.retry_count} retries - too many failures"
            self._log_message(error_message, "error")
            
            # Force status to failed
            with self.env.registry.cursor() as new_cr:
                new_cr.execute("""
                    UPDATE import_log
                    SET status = 'failed',
                        error_message = CONCAT(COALESCE(error_message, ''), %s)
                    WHERE id = %s
                """, (f"\n{error_message}", self.id))
                new_cr.commit()
            
            return {
                'success': False,
                'error_message': error_message
            }
        
        try:
            # Verify file exists
            if not self.file_path or not os.path.exists(self.file_path):
                raise ValueError("Import file not found at specified path")
                
            self._log_message(f"Processing segment {start_position}-{end_position} (job {job_index})", "info")
            
            # Import the CSV processor with improved error handling
            try:
                from ..services.csv_processor import CSVProcessor
                from ..services.csv_processor import CSVProcessor
                processor_class = CSVProcessor
            except ImportError as e:
                _logger.error(f"Error importing CSVProcessor: {str(e)}")
                raise ImportError("Could not import CSVProcessor - module may be missing")

            # try:
            #     from compliance_management.services.csv_processor import CSVProcessor
            #     processor_class = CSVProcessor
            # except ImportError as e:
            #     _logger.error(f"Error importing CSVProcessor: {str(e)}")
            #     # Try relative import as fallback
            #     try:
            #         from . import services
            #         processor_class = services.csv_processor.CSVProcessor
            #     except ImportError:
            #         raise ImportError("Could not import CSVProcessor - module may be missing")
                raise ImportError("Could not import CSVProcessor - module may be missing")

            # try:
            #     from compliance_management.services.csv_processor import CSVProcessor
            #     processor_class = CSVProcessor
            # except ImportError as e:
            #     _logger.error(f"Error importing CSVProcessor: {str(e)}")
            #     # Try relative import as fallback
            #     try:
            #         from . import services
            #         processor_class = services.csv_processor.CSVProcessor
            #     except ImportError:
            #         raise ImportError("Could not import CSVProcessor - module may be missing")
            
            # Process batches within this segment with transaction isolation
            with self.env.registry.cursor() as segment_cr:
                env = api.Environment(segment_cr, self.env.uid, self.env.context)
                current_import = env['import.log'].browse(self.id)
                
                try:
                    # Process batch with a dedicated processor instance
                    processor = processor_class(current_import)
                    result = processor.process_batch(start_position, end_position)
                    
                    # Update progress using atomic SQL
                    self._update_progress_counters(
                        current_position=end_position,
                        successful=result.get("successful", 0),
                        failed=result.get("failed", 0),
                        duplicates=result.get("duplicates", 0),
                        processing_time=getattr(processor, 'process_time', 0)
                    )
                    
                    # Commit at segment level
                    segment_cr.commit()
                except Exception as batch_error:
                    # Rollback segment transaction
                    segment_cr.rollback()
                    raise batch_error
            
            # Mark segment as completed
            segment_time = (datetime.now() - segment_start).total_seconds()
            self._mark_job_completed(segment_time, start_time)
            
            self._log_message(
                f"Completed segment {start_position}-{end_position} in {segment_time:.2f}s (job {job_index})",
                "success"
            )
            
            return {
                'success': True,
                'segment': f"{start_position}-{end_position}",
                'time': segment_time
            }
            
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            error_message = f"Error in segment {start_position}-{end_position}: {str(e)}"
            
            _logger.error(error_message)
            _logger.error(error_trace)
            
            # Update import log to record the error
            self._append_error_log(error_message, error_trace)
            
            # Mark job as completed even with error
            self._mark_job_completed(
                (datetime.now() - segment_start).total_seconds(),
                start_time
            )
            
            return {
                'success': False,
                'error_message': error_message,
                'technical_details': error_trace
            }

    def _update_progress_counters(self, current_position, successful, failed, duplicates, processing_time):
        """
        Update progress counters with linear progression across jobs
        """
        # Use exponential backoff for retries
        max_retries = 5
        for attempt in range(max_retries):
            try:
                # Create a new cursor for this operation to isolate it
                with self.env.registry.cursor() as new_cr:
                    # Use advisory lock with the import ID for coordination
                    lock_id = self.id + 10000000 + attempt
                    new_cr.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
                    
                    # First get stats to calculate progress
                    new_cr.execute("""
                        SELECT total_records, current_position, completed_jobs, parallel_jobs,
                            successful_records, failed_records, duplicate_records
                        FROM import_log
                        WHERE id = %s
                        FOR UPDATE
                    """, (self.id,))
                    
                    result = new_cr.fetchone()
                    if not result:
                        _logger.warning(f"Import {self.id} not found when updating progress counters")
                        return False
                        
                    total_records, curr_pos, completed_jobs, parallel_jobs, curr_successful, curr_failed, curr_duplicates = result
                    
                    # Calculate linear progress
                    # Even if there are 6 jobs, they should increment roughly by ~16.7% each
                    if parallel_jobs > 0:
                        # Each job should represent an equal portion of the total
                        portion_per_job = total_records / parallel_jobs
                        
                        # Calculate which job this is (based on position)
                        current_job = completed_jobs  # 0-based index
                        
                        # Calculate the base position for this job
                        base_position = int(current_job * portion_per_job)
                        
                        # The new position should be the base position plus the processed records
                        new_position = min(total_records, base_position + successful + duplicates)
                    else:
                        # Fallback if no parallel jobs defined
                        new_position = min(total_records, curr_pos + successful + duplicates)
                    
                    # For the final job, ensure we get to exactly 100%
                    is_final_job = (completed_jobs + 1 >= parallel_jobs)
                    if is_final_job:
                        # Force to total_records on last job
                        new_position = total_records
                    
                    # Perform the update with direct SQL
                    new_cr.execute("""
                        UPDATE import_log
                        SET 
                            successful_records = successful_records + %s,
                            failed_records = failed_records + %s,
                            duplicate_records = duplicate_records + %s,
                            execution_time = execution_time + %s,
                            current_position = %s,
                            current_batch = current_batch + 1
                        WHERE id = %s
                    """, (
                        successful,
                        failed,
                        duplicates,
                        processing_time,
                        new_position,
                        self.id
                    ))
                    
                    # Calculate progress percentage for logging
                    progress_pct = round(100 * new_position / max(total_records, 1), 1)
                    
                    # Log progress at key points
                    if is_final_job or progress_pct % 10 < 1 or progress_pct > 95:
                        _logger.info(f"Import {self.id} progress: {progress_pct}% ({new_position}/{total_records})")
                    
                    # Send websocket message with accurate progress
                    try:
                        from ..services.websocket.connection import send_message
                        # Only send at meaningful increments (10%, 20%, etc.) or final job
                        if is_final_job or int(progress_pct) % 10 == 0 or progress_pct > 95:
                            progress_msg = f"📊 Import progress: {int(progress_pct)}% complete"
                            send_message(api.Environment(new_cr, self.env.uid, self.env.context),
                                        progress_msg, "info", self.uploaded_by.id)
                    except Exception:
                        pass  # Ignore websocket errors
                    
                    # Commit this isolated transaction
                    new_cr.commit()
                    return True
                    
            except Exception as e:
                _logger.error(f"Error updating import progress (attempt {attempt+1}/{max_retries}): {str(e)}")
                wait_time = min(2 ** attempt, 16)  # Exponential backoff with max 16 seconds
                time.sleep(wait_time)
        
        # If we get here, all retries failed
        _logger.error(f"Failed to update import progress after {max_retries} attempts")
        return False

    def _mark_job_completed(self, segment_time, start_time):
        """Mark a job as completed with robust error handling and parameter validation"""
        max_retries = 5
        import_id = self.id
        
        for attempt in range(max_retries):
            try:
                # Use a separate cursor for updating completion status
                with self.env.registry.cursor() as comp_cr:
                    # Use unique lock ID for each attempt
                    lock_id = import_id + 40000000 + attempt
                    comp_cr.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
                    
                    # Update job counter with FOR UPDATE to prevent race conditions
                    comp_cr.execute("""
                        SELECT completed_jobs, parallel_jobs, status, total_records, 
                            current_position, execution_time
                        FROM import_log 
                        WHERE id = %s
                        FOR UPDATE
                    """, (import_id,))
                    
                    result = comp_cr.fetchone()
                    if not result:
                        _logger.warning(f"Import log {import_id} not found during job completion")
                        return False
                        
                    completed_jobs, total_jobs, current_status, total_records, current_pos, exec_time = result
                    
                    # Skip if already completed
                    if current_status == 'completed':
                        _logger.info(f"Import {import_id} is already marked as completed")
                        return True
                    
                    # Increment completed jobs
                    comp_cr.execute("""
                        UPDATE import_log
                        SET completed_jobs = completed_jobs + 1
                        WHERE id = %s
                    """, (import_id,))
                    
                    # Check if all jobs are now completed
                    should_complete = (completed_jobs + 1 >= total_jobs)
                                
                    # Calculate expected progress for this job
                    if total_jobs > 0:
                        job_progress = (completed_jobs + 1) / total_jobs
                        expected_position = int(total_records * job_progress)
                        
                        # Update position even if not complete
                        comp_cr.execute("""
                            UPDATE import_log
                            SET current_position = %s
                            WHERE id = %s AND current_position < %s
                        """, (expected_position, import_id, expected_position))
                                    
                    # Update status if all jobs completed
                    if should_complete:
                        # Get current time and calculate total duration
                        end_time = fields.Datetime.now()
                        if isinstance(start_time, str):
                            start_time = fields.Datetime.from_string(start_time)
                        
                        # Calculate duration, ensuring it's a positive number
                        try:
                            total_time = (end_time - start_time).total_seconds()
                            if total_time < 0:
                                total_time = segment_time  # Fallback to segment time
                        except:
                            total_time = segment_time  # Another fallback
                        
                        # FIXED: Ensure all parameters are passed correctly
                        comp_cr.execute("""
                            UPDATE import_log
                            SET status = 'completed',
                                completed_at = %s,
                                execution_time = %s,
                                current_position = total_records
                            WHERE id = %s
                        """, (end_time, total_time, import_id))
                        
                        _logger.info(f"Import {import_id} marked as completed with {completed_jobs + 1}/{total_jobs} jobs completed")
                        
                        # FORCE A DIRECT MESSAGE to websocket with 100% progress
                        try:
                            from ..services.websocket.connection import send_message
                            final_message = f"✅ Import completed successfully! Processing {total_records} records complete."
                            send_message(api.Environment(comp_cr, self.env.uid, self.env.context), 
                                        final_message, "success", self.uploaded_by.id)
                        except Exception as ws_error:
                            _logger.error(f"Error sending final websocket message: {str(ws_error)}")
                    
                    # Always send a progress update message
                    try:
                        progress = min(100, round((completed_jobs + 1) / max(total_jobs, 1) * 100))
                        from ..services.websocket.connection import send_message
                        progress_message = f"📊 Import job {completed_jobs + 1}/{total_jobs} completed ({progress}%)"
                        send_message(api.Environment(comp_cr, self.env.uid, self.env.context),
                                    progress_message, "info", self.uploaded_by.id)
                    except Exception as ws_error:
                        pass  # Ignore websocket errors for progress updates
                    
                    # Commit changes immediately before generating summary
                    comp_cr.commit()
                    
                    # If all jobs completed, generate the summary in a separate transaction
                    if should_complete:
                        # Allow a brief moment for all transactions to finalize
                        time.sleep(1)
                        
                        # Try to generate summary
                        try:
                            # Run in a new transaction to avoid conflicts
                            with self.env.registry.cursor() as summary_cr:
                                env = api.Environment(summary_cr, self.env.uid, self.env.context)
                                current_import = env['import.log'].browse(self.id)
                                current_import._create_final_summary()
                                summary_cr.commit()
                        except Exception as e:
                            _logger.error(f"Error generating summary: {str(e)}")
                        
                    return True
                    
            except Exception as e:
                import traceback
                error_message = f"Error marking job complete (attempt {attempt+1}): {str(e)}"
                stack_trace = traceback.format_exc()
                _logger.error(f"{error_message}\n{stack_trace}")
                time.sleep(1 * (attempt + 1))  # Linear backoff
        
        # Last resort - force completion through direct SQL
        try:
            with self.env.registry.cursor() as last_cr:
                last_cr.execute("""
                    UPDATE import_log
                    SET status = 'completed',
                        current_position = total_records,
                        completed_at = NOW()
                    WHERE id = %s AND completed_jobs >= parallel_jobs
                """, (import_id,))
                last_cr.commit()
                _logger.info(f"Forced completion of import {import_id} as last resort")
        except Exception as e:
            _logger.error(f"Final attempt to mark import as complete failed: {str(e)}")
        
        return False

    def _create_final_summary(self):
        """Create a final import summary with proper record counting"""
        try:
            # Use current environment
            env = self.env
            
            # Get counts from database directly for greater accuracy
            try:
                # Try to get actual record count from target table
                model_name = self.model_name
                model_table = env[model_name]._table
                
                # IMPORTANT: Calculate expected records added by this import
                # This is especially useful when appending to existing records
                before_import = self.env.context.get('record_count_before_import', 0)
                
                self.env.cr.execute(f"""
                    SELECT COUNT(*) 
                    FROM {model_table}
                """)
                actual_records = self.env.cr.fetchone()[0]
                added_records = actual_records - before_import
                
                _logger.info(f"Actual records in {model_table}: {actual_records}")
                _logger.info(f"Added approx. {added_records} new records in this import")
            except Exception as count_error:
                _logger.warning(f"Could not get actual record count: {str(count_error)}")
                actual_records = None
                added_records = None
            
            # Get current statistics
            successful = self.successful_records
            failed = self.failed_records
            duplicates = self.duplicate_records
            total = self.total_records
            execution_time = self.execution_time or 0
            
            # Check if we need to update "successful records" based on database verification
            if added_records is not None and abs(successful - added_records) > 1000:
                _logger.warning(f"Large discrepancy between reported successful ({successful}) and actual added records ({added_records})")
                # Update the successful records to match reality
                try:
                    self.env.cr.execute("""
                        UPDATE import_log
                        SET successful_records = %s
                        WHERE id = %s
                    """, (added_records, self.id))
                    self.env.cr.commit()
                    successful = added_records
                except Exception as update_error:
                    _logger.error(f"Error updating successful record count: {str(update_error)}")
            
            # Calculate percentages
            if total > 0:
                success_pct = (successful / total) * 100
                failed_pct = (failed / total) * 100
                duplicate_pct = (duplicates / total) * 100
            else:
                success_pct = failed_pct = duplicate_pct = 0
            
            # Create summary message
            verification = ""
            if actual_records is not None:
                if added_records is not None:
                    verification = f"\n✅ Added {added_records:,} new records to the database (total now: {actual_records:,})"
                else:
                    verification = f"\n✅ Database now contains {actual_records:,} total records"
            
            msg = f"""
    🎉 Import Complete!

    📊 Results for '{self.original_filename}':
    ✅ Successfully Imported: {successful:,} ({success_pct:.1f}%)
    ❌ Failed Records: {failed:,} ({failed_pct:.1f}%)
    ⚠️ Duplicate Records: {duplicates:,} ({duplicate_pct:.1f}%)

    ⏱️ Total Execution Time: {execution_time:.1f} seconds
    ⚡ Average Speed: {successful / max(execution_time, 0.001):.1f} records/second{verification}

    Final status: COMPLETED
            """
            
            # Post message
            self.env['mail.message'].create({
                'body': msg,
                'model': 'import.log',
                'res_id': self.id,
                'message_type': 'comment',
                'subject': 'Import Complete'
            })
            
            # Send notification message
            try:
                from ..services.websocket.connection import send_message
                added_message = f"{added_records:,}" if added_records is not None else f"{successful:,}"
                send_message(self.env, f"🎉 Import Complete! Successfully imported {added_message} records.",
                            "success", self.uploaded_by.id)
            except Exception as ws_error:
                _logger.error(f"Error sending websocket message: {str(ws_error)}")
            
            return True
        except Exception as e:
            _logger.error(f"Error creating final summary: {str(e)}")
            return False
            
    def _append_error_log(self, error_message, error_trace):
        """Append error information to the log with concurrency handling"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Use a separate cursor for error logging
                with self.env.registry.cursor() as err_cr:
                    # Use serializable isolation
                    err_cr.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                    
                    # Use advisory lock with a different offset
                    lock_id = self.id + 30000000
                    err_cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (lock_id,))
                    
                    # Update error logs
                    err_cr.execute("""
                        UPDATE import_log
                        SET error_message = CONCAT(COALESCE(error_message, ''), %s),
                            technical_details = CONCAT(COALESCE(technical_details, ''), %s)
                        WHERE id = %s
                    """, (
                        f"\n{error_message}",
                        f"\n{error_trace}",
                        self.id
                    ))
                    
                    # Commit immediately
                    err_cr.commit()
                    return True
            except Exception as e:
                _logger.warning(f"Failed to append error log (attempt {attempt+1}): {e}")
                time.sleep(0.5 * (2 ** attempt))
        
        # If we get here, all retries failed
        _logger.error(f"Failed to append error log after {max_retries} attempts")
        return False
        
    def _log_message(self, message, message_type="info"):
        """Log a message to the import log and send via websocket with concurrency handling"""
        # Add timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        full_message = f"[{timestamp}] {message}"
        
        # Log to server first (this always works)
        log_level = {
            "info": _logger.info,
            "error": _logger.error,
            "success": _logger.info,
            "warning": _logger.warning,
        }.get(message_type, _logger.info)
        log_level(f"Import {self.id}: {message}")
        
        # Try to append to log in database with retries
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Use a separate cursor for logging to avoid affecting main transaction
                with self.env.registry.cursor() as log_cr:
                    # Use serializable isolation to prevent concurrent update issues
                    log_cr.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                    
                    # Use advisory lock to prevent concurrent updates
                    lock_id = self.id + 20000000  # Different offset from progress updates
                    log_cr.execute("SELECT pg_try_advisory_xact_lock(%s)", (lock_id,))
                    
                    # Update log
                    log_cr.execute("""
                        UPDATE import_log
                        SET log_messages = CONCAT(COALESCE(log_messages, ''), %s)
                        WHERE id = %s
                    """, (f"{full_message}\n", self.id))
                    
                    # Commit immediately
                    log_cr.commit()
                    break
            except Exception as e:
                # Don't fail the whole process for logging issues
                _logger.warning(f"Failed to update log messages (attempt {attempt+1}): {e}")
                time.sleep(0.5 * (2 ** attempt))  # Exponential backoff
        
        # Send to websocket if available
        try:
            from ..services.websocket.connection import send_message
            send_message(self.env, message, message_type, self.uploaded_by.id)
        except Exception as e:
            _logger.warning(f"Failed to send websocket message: {e}")

    def _generate_import_summary(self):
        """Generate a comprehensive summary of the import results with robust error handling"""
        max_retries = 5
        
        for attempt in range(max_retries):
            try:
                # Use a separate cursor for summary generation
                with self.env.registry.cursor() as summary_cr:
                    # Create environment with new cursor
                    env = api.Environment(summary_cr, self.env.uid, self.env.context)
                    current_import = env['import.log'].browse(self.id)
                    
                    # Lock the record to prevent concurrent updates
                    summary_cr.execute("""
                        SELECT id FROM import_log 
                        WHERE id = %s
                        FOR UPDATE NOWAIT
                    """, (self.id,))
                    
                    if not summary_cr.fetchone():
                        _logger.warning(f"Could not lock import record {self.id} for summary generation")
                        time.sleep(1)
                        continue
                    
                    # ENSURE STATUS IS COMPLETED
                    summary_cr.execute("""
                        UPDATE import_log
                        SET status = 'completed',
                            current_position = total_records
                        WHERE id = %s AND completed_jobs >= parallel_jobs
                    """, (self.id,))
                    
                    # Get counts from database directly for greater accuracy
                    try:
                        # Try to get actual record count from target table
                        model_table = env[current_import.model_name]._table
                        
                        # IMPORTANT: Use a different cursor for this to avoid interfering with the main transaction
                        with env.registry.cursor() as count_cr:
                            count_cr.execute(f"""
                                SELECT COUNT(*) 
                                FROM {model_table}
                            """)
                            actual_records = count_cr.fetchone()[0]
                            _logger.info(f"Actual records in {model_table}: {actual_records}")
                    except Exception as count_error:
                        _logger.warning(f"Could not get actual record count: {str(count_error)}")
                        actual_records = None
                    
                    # Get current statistics
                    successful = current_import.successful_records
                    failed = current_import.failed_records
                    duplicates = current_import.duplicate_records
                    total = current_import.total_records
                    execution_time = current_import.execution_time or 0
                    
                    # Calculate percentages
                    if total > 0:
                        success_pct = (successful / total) * 100
                        failed_pct = (failed / total) * 100
                        duplicate_pct = (duplicates / total) * 100
                    else:
                        success_pct = failed_pct = duplicate_pct = 0
                            
                    # Format enhanced summary
                    summary = {
                        'import_id': self.id,
                        'model': current_import.model_name,
                        'file_name': current_import.original_filename,
                        'date': fields.Datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'statistics': {
                            'total_records': total,
                            'successful': successful,
                            'success_percentage': round(success_pct, 1),
                            'failed': failed,
                            'failed_percentage': round(failed_pct, 1),
                            'duplicates': duplicates,
                            'duplicate_percentage': round(duplicate_pct, 1),
                            'execution_time': round(execution_time, 1),
                            'records_per_second': round(successful / max(execution_time, 0.001), 1),
                            'actual_records_in_db': actual_records
                        }
                    }
                    
                    # Store summary as JSON
                    import json
                    summary_cr.execute("""
                        UPDATE import_log
                        SET summary = %s
                        WHERE id = %s
                    """, (json.dumps(summary), self.id))
                    
                    # Generate user-friendly message
                    speed = successful / max(execution_time, 0.001)
                    verification = ""
                    if actual_records is not None:
                        if abs(successful - actual_records) > 10:
                            verification = f"\n⚠️ Verification: Expected {successful:,} records, found {actual_records:,} in database."
                        else:
                            verification = f"\n✅ Verification: All {successful:,} records confirmed in database."
                    
                    msg = f"""
    🎉 Import Complete!

    📊 Results for '{current_import.original_filename}':
    ✅ Successfully Imported: {successful:,} ({success_pct:.1f}%)
    ❌ Failed Records: {failed:,} ({failed_pct:.1f}%)
    ⚠️ Duplicate Records: {duplicates:,} ({duplicate_pct:.1f}%)

    ⏱️ Total Execution Time: {execution_time:.1f} seconds
    ⚡ Average Speed: {speed:.1f} records/second{verification}

    Final status: COMPLETED
                    """
                    
                    # Post message using the new cursor
                    env['mail.message'].create({
                        'body': msg,
                        'model': 'import.log',
                        'res_id': self.id,
                        'message_type': 'comment',
                        'subject': 'Import Complete'
                    })
                    
                    # Send to websocket if possible
                    try:
                        from ..services.websocket.connection import send_message
                        send_message(env, f"🎉 Import Complete! Successfully imported {successful:,} records.",
                                    "success", current_import.uploaded_by.id)
                    except Exception as ws_error:
                        _logger.error(f"Error sending websocket message: {str(ws_error)}")
                    
                    # Commit changes
                    summary_cr.commit()
                    
                    # Send an alert to the user if there's a significant mismatch
                    if actual_records is not None and abs(successful - actual_records) > 1000:
                        try:
                            from ..services.websocket.connection import send_message
                            mismatch_msg = f"⚠️ Records count mismatch: Expected {successful:,}, found {actual_records:,} in database."
                            send_message(env, mismatch_msg, "warning", current_import.uploaded_by.id)
                        except Exception as ws_error:
                            _logger.error(f"Error sending mismatch message: {str(ws_error)}")
                    
                    return summary
                        
            except Exception as e:
                _logger.error(f"Error generating import summary (attempt {attempt+1}): {str(e)}")
                import traceback
                _logger.error(traceback.format_exc())
                time.sleep(1 + attempt)  # Linear backoff
        
        # If all retries failed, try the simple fallback
        try:
            self._create_fallback_summary()
        except Exception as fallback_error:
            _logger.error(f"Final fallback summary also failed: {str(fallback_error)}")
        
        return None


    def _create_fallback_summary(self):
        """Create a basic import summary as a fallback when the main method fails"""
        try:
            # Use a dedicated cursor
            with self.env.registry.cursor() as fallback_cr:
                env = api.Environment(fallback_cr, self.env.uid, self.env.context)
                
                # First ensure the status is completed
                fallback_cr.execute("""
                    UPDATE import_log
                    SET status = 'completed',
                        current_position = total_records,
                        completed_at = COALESCE(completed_at, NOW())
                    WHERE id = %s AND (completed_jobs >= parallel_jobs OR completed_jobs > 0)
                """, (self.id,))
                
                # Fetch the import record
                fallback_cr.execute("""
                    SELECT successful_records, failed_records, duplicate_records, execution_time, original_filename
                    FROM import_log
                    WHERE id = %s
                """, (self.id,))
                
                result = fallback_cr.fetchone()
                if not result:
                    fallback_cr.rollback()
                    return False
                    
                successful, failed, duplicates, execution_time, filename = result
                
                # Format a simple summary
                msg = f"""
    🎉 Import Summary (Fallback) for '{filename}':

    ✅ Successfully Imported: {successful:,}
    ❌ Failed Records: {failed:,}
    ⚠️ Duplicate Records: {duplicates:,}
    ⏱️ Total Execution Time: {execution_time:.1f} seconds

    Import completed at: {fields.Datetime.now()}
                """
                
                # Post message
                env['mail.message'].create({
                    'body': msg,
                    'model': 'import.log',
                    'res_id': self.id,
                    'message_type': 'comment',
                    'subject': 'Import Completed (Fallback Summary)'
                })
                
                # Try to send a websocket message
                try:
                    from ..services.websocket.connection import send_message
                    send_message(env, f"🎉 Import Complete! Successfully imported {successful:,} records.",
                                "success", self.uploaded_by.id)
                except Exception:
                    pass
                    
                fallback_cr.commit()
                return True
        except Exception as e:
            _logger.error(f"Fatal error in fallback summary: {str(e)}")
            return False
            
    # ---------- Action Methods ----------
            
    def retry_import(self):
        """Retry the import process from where it left off"""
        self.ensure_one()
        
        if self.status not in ['failed', 'processing', 'paused']:
            raise models.ValidationError('Only failed, paused or processing imports can be retried')
            
        # Reset error information but keep progress
        self.write({
            'error_message': False,
            'technical_details': False,
            'completed_jobs': 0,
            'status': 'pending'
        })
        
        # Start the import
        return self.process_file()
        
    def reset_import(self):
        """Reset the import to start from the beginning"""
        self.ensure_one()
        
        if self.status == 'completed':
            raise models.ValidationError('Completed imports cannot be reset')
            
        # Reset all progress fields
        self.write({
            'current_position': 0,
            'current_batch': 0,
            'successful_records': 0,
            'failed_records': 0,
            'duplicate_records': 0,
            'skipped_records': 0,
            'error_message': False,
            'technical_details': False,
            'log_messages': False,
            'status': 'pending',
            'retry_count': 0,
            'execution_time': 0,
            'completed_jobs': 0,
            'started_at': False,
            'completed_at': False,
        })
        
        self.env.cr.commit()
        return True
        
    def pause_import(self):
        """Pause a running import"""
        self.ensure_one()
        
        if self.status != 'processing':
            raise models.ValidationError('Only processing imports can be paused')
            
        self.write({'status': 'paused'})
        self._log_message("Import paused by user", "warning")
        
        return True
        
    def cancel_import(self):
        """Cancel the import"""
        self.ensure_one()
        
        if self.status in ['completed', 'failed']:
            raise models.ValidationError('Completed or failed imports cannot be cancelled')
            
        self.write({
            'status': 'failed',
            'error_message': 'Import cancelled by user',
            'completed_at': fields.Datetime.now(),
        })
        
        self._log_message("Import cancelled by user", "warning")
        
        return True
        
    # ---------- Maintenance Methods ----------
        
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

    def get_delete_progress(self):
        """Get the current delete progress as a dictionary"""
        if not self.delete_progress:
            return {
                'total': 0,
                'processed': 0,
                'deleted': 0,
                'failed': 0,
                'status': 'not_started',
            }
            
        try:
            return json.loads(self.delete_progress)
        except Exception as e:
            _logger.error(f"Error parsing delete progress: {str(e)}")
            return {
                'total': 0,
                'processed': 0,
                'deleted': 0,
                'failed': 0,
                'status': 'error',
                'error': str(e)
            }
            
    def reset_delete_progress(self):
        """Reset the delete progress"""
        self._safe_update({
            'delete_progress': json.dumps({
                'total': 0,
                'processed': 0,
                'deleted': 0,
                'failed': 0,
                'status': 'not_started',
                'processed_values': []
            })
        })
        
        self._log_message("Delete progress has been reset", "info")
        return True
        
    def resume_delete_operation(self):
        """Resume an interrupted delete operation"""
        if not self.delete_mode or not self.unique_identifier_field:
            raise ValueError("This import is not configured for delete mode")
            
        progress = self.get_delete_progress()
        if progress.get('status') != 'in_progress':
            raise ValueError("No interrupted delete operation found to resume")
           
        return self.process_file()

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
