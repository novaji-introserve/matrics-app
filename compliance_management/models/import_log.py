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
                    SELECT file_path, status 
                    FROM import_log 
                    WHERE id = %s
                    FOR UPDATE
                """, (import_id,))
                
                result = new_cr.fetchone()
                if not result:
                    return {'success': False, 'error': 'Import not found'}
                    
                file_path, status = result
                
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
                
                # Launch parallel jobs with the new cursor
                return self._launch_parallel_jobs_with_cursor(new_cr, start_time)
                
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
                from odoo.addons.compliance_management.services.csv_processor import CSVProcessor
                processor_class = CSVProcessor
            except ImportError as e:
                _logger.error(f"Error importing CSVProcessor: {str(e)}")
                # Try relative import as fallback
                try:
                    from . import services
                    processor_class = services.csv_processor.CSVProcessor
                except ImportError:
                    raise ImportError("Could not import CSVProcessor - module may be missing")
            
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
            
    # def _update_progress_counters(self, current_position, successful, failed, duplicates, processing_time):
    #     """
    #     Update progress counters with advisory locks to prevent concurrent update issues
    #     """
    #     # with self.advisory_lock() as acquired:
    #     #     if not acquired:
    #     max_retries = 5
    #     for attempt in range(max_retries):
    #         try:
    #             # Use explicit transaction with stronger isolation level
    #             self.env.cr.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                
    #             self.env.cr.execute("""
    #                 UPDATE import_log
    #                 SET 
    #                     successful_records = successful_records + %s,
    #                     failed_records = failed_records + %s,
    #                     duplicate_records = duplicate_records + %s,
    #                     execution_time = execution_time + %s,
    #                     current_position = GREATEST(current_position, %s),
    #                     current_batch = current_batch + 1
    #                 WHERE id = %s
    #             """, (successful, failed, duplicates, processing_time, current_position, self.id))
                
    #             self.env.cr.commit()
    #             return True
    #         except Exception as e:
    #             self.env.cr.rollback()
    #             wait_time = min(2 ** attempt, 16)  # Exponential backoff with max 16 seconds
    #             _logger.warning(f"Retry {attempt+1}/{max_retries} updating progress - waiting {wait_time}s")
    #             time.sleep(wait_time)
        
    #     return False

    def _update_progress_counters(self, current_position, successful, failed, duplicates, processing_time):
        """
        Update progress counters with proper concurrency control and retries
        """
        # Use exponential backoff for retries
        max_retries = 5
        for attempt in range(max_retries):
            try:
                # Create a new cursor for this operation to isolate it
                with self.env.registry.cursor() as new_cr:
                    # Use advisory lock with the import ID for coordination
                    # Add attempt count to the lock to allow different attempts to acquire different locks
                    lock_id = self.id + 10000000 + attempt
                    new_cr.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
                    
                    # Perform the update with direct SQL
                    new_cr.execute("""
                        UPDATE import_log
                        SET 
                            successful_records = successful_records + %s,
                            failed_records = failed_records + %s,
                            duplicate_records = duplicate_records + %s,
                            execution_time = execution_time + %s,
                            current_position = GREATEST(current_position, %s),
                            current_batch = current_batch + 1
                        WHERE id = %s
                    """, (
                        successful,
                        failed,
                        duplicates,
                        processing_time,
                        current_position,
                        self.id
                    ))
                    
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
        # with self.advisory_lock() as acquired:
        #     if not acquired:
        #         # Don't proceed if lock not acquired, wait and retry
        #         time.sleep(1)
        #         _logger.warning(f"Could not acquire lock for import {self.id}, attempting update anyway")
        #         return self._update_progress_counters(current_position, successful, failed, duplicates, processing_time)
                
        #     # Use direct SQL update with GREATEST to prevent race conditions
        #     try:
        #         self.env.cr.execute("""
        #             UPDATE import_log
        #             SET 
        #                 successful_records = successful_records + %s,
        #                 failed_records = failed_records + %s,
        #                 duplicate_records = duplicate_records + %s,
        #                 execution_time = execution_time + %s,
        #                 current_position = GREATEST(current_position, %s),
        #                 current_batch = current_batch + 1
        #             WHERE id = %s
        #         """, (
        #             successful,
        #             failed,
        #             duplicates,
        #             processing_time,
        #             current_position,
        #             self.id
        #         ))
                
        #         # Make sure to commit immediately
        #         self.env.cr.commit()
                
        #     except Exception as e:
        #         _logger.error(f"Error updating import progress: {str(e)}")
        #         self.env.cr.rollback()
                
        #         # Try a simpler update as fallback
        #         try:
        #             self.env.cr.execute("""
        #                 UPDATE import_log
        #                 SET 
        #                     current_batch = current_batch + 1
        #                 WHERE id = %s
        #             """, (self.id,))
        #             self.env.cr.commit()
        #         except:
        #             pass
    
    def _mark_job_completed(self, segment_time, start_time):
        """Mark a job as completed with robust completion detection"""
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
                        SELECT completed_jobs, parallel_jobs, status
                        FROM import_log 
                        WHERE id = %s
                        FOR UPDATE
                    """, (import_id,))
                    
                    result = comp_cr.fetchone()
                    if not result:
                        _logger.warning(f"Import log {import_id} not found during job completion")
                        return False
                        
                    completed_jobs, total_jobs, current_status = result
                    
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
                                    
                    # Update status if all jobs completed
                    if should_complete:
                        # Get current time and calculate total duration
                        end_time = fields.Datetime.now()
                        if isinstance(start_time, str):
                            start_time = fields.Datetime.from_string(start_time)
                        total_time = (end_time - start_time).total_seconds()
                        
                        # Get current counts
                        comp_cr.execute("""
                            SELECT successful_records, failed_records 
                            FROM import_log 
                            WHERE id = %s
                        """, (import_id,))
                        
                        success_count, fail_count = comp_cr.fetchone() or (0, 0)
                        
                        # Determine final status
                        status = 'completed' if success_count > 0 else 'failed'
                        
                        # Update final status
                        comp_cr.execute("""
                            UPDATE import_log
                            SET status = %s,
                                completed_at = %s,
                                execution_time = %s
                            WHERE id = %s
                        """, (status, end_time, total_time, import_id))
                        
                        _logger.info(f"Import {import_id} marked as {status} with {completed_jobs + 1}/{total_jobs} jobs completed")
                    
                    # Commit this transaction
                    comp_cr.commit()
                    
                    # If all jobs completed, generate the summary in a separate transaction
                    if should_complete:
                        try:
                            self._generate_import_summary()
                        except Exception as e:
                            _logger.error(f"Error generating summary: {str(e)}")
                            # Don't stop completion due to summary generation error
                        
                    return True
                    
            except Exception as e:
                _logger.error(f"Error marking job complete (attempt {attempt+1}): {str(e)}")
                time.sleep(1 * (attempt + 1))  # Linear backoff
        
        return False

    def force_complete_import(self):
        """Force completion of stuck imports that have all data processed"""
        self.ensure_one()
        
        # Only work on processing imports
        if self.status != 'processing':
            raise UserError(_("Only 'processing' imports can be force-completed"))
        
        # Use a new cursor for this operation
        with self.env.registry.cursor() as force_cr:
            env = api.Environment(force_cr, self.env.uid, self.env.context)
            current_import = env['import.log'].browse(self.id)
            
            # Check if the import is nearly complete (progress > 95%)
            progress = 0
            if current_import.total_records > 0:
                progress = (current_import.current_position / current_import.total_records) * 100
            
            if progress < 95:
                raise UserError(_("Import is only at %.1f%% - cannot force completion below 95%%") % progress)
            
            # Calculate final statistics
            start_time = current_import.started_at
            end_time = fields.Datetime.now()
            if start_time:
                total_time = (end_time - start_time).total_seconds()
            else:
                total_time = 0
            
            # Update the import to completed status
            force_cr.execute("""
                UPDATE import_log
                SET status = 'completed',
                    completed_at = %s,
                    execution_time = %s,
                    completed_jobs = parallel_jobs  -- Mark all jobs as completed
                WHERE id = %s
            """, (end_time, total_time, self.id))
            
            # Commit changes
            force_cr.commit()
            
            # Generate a summary
            try:
                current_import._generate_import_summary()
            except Exception as e:
                _logger.error(f"Error generating summary during force completion: {str(e)}")
        
        return {
            'type': 'ir.actions.client',
            'tag': 'reload',
        }

    # def _mark_job_completed(self, segment_time, start_time):
    #     """Mark a job as completed with proper concurrency handling"""
    #     max_retries = 5
    #     for attempt in range(max_retries):
    #         try:
    #             # Use a separate cursor for updating completion status
    #             with self.env.registry.cursor() as comp_cr:
    #                 # Use unique lock ID for each attempt
    #                 lock_id = self.id + 40000000 + attempt
    #                 comp_cr.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
                    
    #                 # Update job counter with FOR UPDATE to prevent race conditions
    #                 comp_cr.execute("""
    #                     SELECT completed_jobs, parallel_jobs
    #                     FROM import_log 
    #                     WHERE id = %s
    #                     FOR UPDATE
    #                 """, (self.id,))
                    
    #                 result = comp_cr.fetchone()
    #                 if not result:
    #                     return
                        
    #                 completed_jobs, total_jobs = result
                    
    #                 # Increment completed jobs
    #                 comp_cr.execute("""
    #                     UPDATE import_log
    #                     SET completed_jobs = completed_jobs + 1
    #                     WHERE id = %s
    #                 """, (self.id,))
                    
    #                 # Update status if all jobs completed
    #                 if completed_jobs + 1 >= total_jobs:
    #                     # Get current time and calculate total duration
    #                     end_time = fields.Datetime.now()
    #                     if isinstance(start_time, str):
    #                         start_time = fields.Datetime.from_string(start_time)
    #                     total_time = (end_time - start_time).total_seconds()
                        
    #                     # Get current counts
    #                     comp_cr.execute("""
    #                         SELECT successful_records, failed_records 
    #                         FROM import_log 
    #                         WHERE id = %s
    #                     """, (self.id,))
                        
    #                     success_count, fail_count = comp_cr.fetchone() or (0, 0)
                        
    #                     # Determine final status
    #                     status = 'completed' if success_count > 0 else 'failed'
                        
    #                     # Update final status
    #                     comp_cr.execute("""
    #                         UPDATE import_log
    #                         SET status = %s,
    #                             completed_at = %s,
    #                             execution_time = %s
    #                         WHERE id = %s
    #                     """, (status, end_time, total_time, self.id))
                        
    #                 # Commit this transaction
    #                 comp_cr.commit()
                    
    #                 # If all jobs completed, generate the summary in a separate transaction
    #                 if completed_jobs + 1 >= total_jobs:
    #                     self._generate_import_summary()
                        
    #                 return True
                    
    #         except Exception as e:
    #             _logger.error(f"Error marking job complete (attempt {attempt+1}): {str(e)}")
    #             time.sleep(1 * (attempt + 1))  # Linear backoff
        
    #     return False
            
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
        """Generate a comprehensive summary of the import results with retry logic and enhanced details"""
        import json 
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
                    
                    # Get current statistics
                    successful = current_import.successful_records
                    failed = current_import.failed_records
                    duplicates = current_import.duplicate_records
                    total = current_import.total_records
                    execution_time = current_import.execution_time
                    
                    # Calculate percentages
                    if total > 0:
                        success_pct = (successful / total) * 100
                        failed_pct = (failed / total) * 100
                        duplicate_pct = (duplicates / total) * 100
                    else:
                        success_pct = failed_pct = duplicate_pct = 0
                        
                    # Get detailed failure reasons from logs if possible
                    failure_details = {}
                    try:
                        # Parse JSON data from technical details if available
                        if current_import.summary and isinstance(current_import.summary, str):
                            import json
                            summary_data = json.loads(current_import.summary)
                            if "failure_details" in summary_data:
                                failure_details = summary_data["failure_details"]
                    except Exception as e:
                        _logger.warning(f"Could not parse failure details: {e}")
                    
                    # Format summary
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
                            'records_per_second': round(successful / execution_time, 1) if execution_time > 0 else 0
                        },
                        'failure_details': failure_details
                    }
                    
                    # Store summary as JSON
                    summary_cr.execute("""
                        UPDATE import_log
                        SET summary = %s
                        WHERE id = %s
                    """, (json.dumps(summary), self.id))
                    
                    # Generate user-friendly message with enhanced details
                    failure_messages = []
                    if failure_details:
                        failure_messages.append("Failure reasons:")
                        for reason, count in failure_details.items():
                            readable_reason = reason.replace('_', ' ').title()
                            failure_messages.append(f"  • {readable_reason}: {count:,}")
                    
                    processing_rate = successful / execution_time if execution_time > 0 else 0
                    
                    msg = f"""
    ✅ Successfully imported {successful:,} records
    {'⚠️ Skipped ' + str(duplicates) + ' duplicate records' if duplicates > 0 else ''}
    {'❌ Failed to import ' + str(failed) + ' records' if failed > 0 else ''}
    {chr(10).join(failure_messages) if failure_messages else ''}
    ⏱️ Processed in {execution_time:.1f} seconds ({processing_rate:.1f} records/sec)
    📊 Overall progress: {success_pct:.1f}% complete
                    """
                    
                    # Post message using the new cursor
                    env['mail.message'].create({
                        'body': msg,
                        'model': 'import.log',
                        'res_id': self.id,
                        'message_type': 'comment',
                    })
                    
                    # Commit changes
                    summary_cr.commit()
                    
                    # Return summary
                    return summary
                    
            except Exception as e:
                _logger.error(f"Error generating import summary (attempt {attempt+1}): {str(e)}")
                time.sleep(2 ** attempt)  # Exponential backoff
        
        _logger.error(f"Failed to generate import summary after {max_retries} attempts")
        return None

    # def _generate_import_summary(self):
    #     """Generate a comprehensive summary of the import results with retry logic"""
    #     max_retries = 5
        
    #     for attempt in range(max_retries):
    #         try:
    #             # Use a separate cursor for summary generation
    #             with self.env.registry.cursor() as summary_cr:
    #                 # Create environment with new cursor
    #                 env = api.Environment(summary_cr, self.env.uid, self.env.context)
    #                 current_import = env['import.log'].browse(self.id)
                    
    #                 # Lock the record to prevent concurrent updates
    #                 summary_cr.execute("""
    #                     SELECT id FROM import_log 
    #                     WHERE id = %s
    #                     FOR UPDATE NOWAIT
    #                 """, (self.id,))
                    
    #                 if not summary_cr.fetchone():
    #                     _logger.warning(f"Could not lock import record {self.id} for summary generation")
    #                     time.sleep(1)
    #                     continue
                    
    #                 # Get current statistics
    #                 successful = current_import.successful_records
    #                 failed = current_import.failed_records
    #                 duplicates = current_import.duplicate_records
    #                 total = current_import.total_records
    #                 execution_time = current_import.execution_time
                    
    #                 # Calculate percentages
    #                 if total > 0:
    #                     success_pct = (successful / total) * 100
    #                     failed_pct = (failed / total) * 100
    #                     duplicate_pct = (duplicates / total) * 100
    #                 else:
    #                     success_pct = failed_pct = duplicate_pct = 0
                        
    #                 # Format summary
    #                 summary = {
    #                     'import_id': self.id,
    #                     'model': current_import.model_name,
    #                     'file_name': current_import.original_filename,
    #                     'date': fields.Datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    #                     'statistics': {
    #                         'total_records': total,
    #                         'successful': successful,
    #                         'success_percentage': round(success_pct, 1),
    #                         'failed': failed,
    #                         'failed_percentage': round(failed_pct, 1),
    #                         'duplicates': duplicates,
    #                         'duplicate_percentage': round(duplicate_pct, 1),
    #                         'execution_time': round(execution_time, 1),
    #                         'records_per_second': round(successful / execution_time, 1) if execution_time > 0 else 0
    #                     }
    #                 }
                    
    #                 # Store summary as JSON
    #                 summary_cr.execute("""
    #                     UPDATE import_log
    #                     SET summary = %s
    #                     WHERE id = %s
    #                 """, (json.dumps(summary), self.id))
                    
    #                 # Generate user-friendly message
    #                 msg = f"""
    #         Import Summary for '{current_import.original_filename}':

    #         📊 Records Processed: {total:,}
    #         ✅ Successfully Imported: {successful:,} ({success_pct:.1f}%)
    #         ❌ Failed Records: {failed:,} ({failed_pct:.1f}%)
    #         ⚠️ Duplicate Records: {duplicates:,} ({duplicate_pct:.1f}%)

    #         ⏱️ Total Execution Time: {execution_time:.1f} seconds
    #         ⚡ Import Speed: {successful/execution_time:.1f} records/second if execution_time > 0 else 0
    #                 """
                    
    #                 # Post message using the new cursor
    #                 env['mail.message'].create({
    #                     'body': msg,
    #                     'model': 'import.log',
    #                     'res_id': self.id,
    #                     'message_type': 'comment',
    #                 })
                    
    #                 # Commit changes
    #                 summary_cr.commit()
                    
    #                 # Return summary
    #                 return summary
                    
    #         except Exception as e:
    #             _logger.error(f"Error generating import summary (attempt {attempt+1}): {str(e)}")
    #             time.sleep(2 ** attempt)  # Exponential backoff
        
    #     _logger.error(f"Failed to generate import summary after {max_retries} attempts")
    #     return None
            
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
