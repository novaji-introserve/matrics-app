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
        """Process the file using optimal batch processing strategy"""
        self.ensure_one()
        
        # Safety checks
        if self.status == 'completed':
            return {'success': True, 'message': 'Import already completed'}
            
        if not self.file_path or not os.path.exists(self.file_path):
            self.write({
                'status': 'failed',
                'error_message': 'Import file not found at specified path'
            })
            return {'success': False, 'error_message': 'Import file not found'}
        
        # Update status and record start time
        start_time = fields.Datetime.now()
        self.write({
            "status": "processing",
            "started_at": start_time,
            "execution_time": 0,
            "error_message": False,
            "technical_details": False,
        })
        self.env.cr.commit()
        
        # Increment retry counter
        self.retry_count += 1
        
        # Optimize batch size based on file size and available resources
        self._optimize_processing_parameters()
        
        # Count total records if not already set
        if self.total_records <= 0:
            self._count_total_records()
            
        # Calculate total batches
        if self.total_records > 0:
            self.total_batches = math.ceil(self.total_records / self.batch_size)
        else:
            self.total_batches = 1
            
        # Launch parallel processing jobs
        return self._launch_parallel_jobs(start_time)
        
    def _optimize_processing_parameters(self):
        """Optimize batch size and parallelism based on file size and system resources"""
        # Get file size and system parameters
        file_size = self.file_size or (os.path.getsize(self.file_path) if self.file_path else 0)
        
        # Get config parameters or use defaults
        try:
            default_batch_size = int(self.env['ir.config_parameter'].sudo().get_param(
                'csv_import.batch_size', '10000'))
            default_parallel_jobs = int(self.env['ir.config_parameter'].sudo().get_param(
                'csv_import.parallel_jobs', '4'))
        except:
            default_batch_size = 10000
            default_parallel_jobs = 4
            
        # Default parameters
        self.batch_size = default_batch_size
        self.parallel_jobs = default_parallel_jobs
        
        # Adjust batch size based on file size
        if file_size > 1024 * 1024 * 500:  # > 500MB
            self.batch_size = 50000
        elif file_size > 1024 * 1024 * 100:  # > 100MB
            self.batch_size = 20000
            
        # Adjust parallelism based on available CPU cores
        available_cores = multiprocessing.cpu_count()
        if available_cores > 4:
            # Leave cores for the main Odoo process
            self.parallel_jobs = min(available_cores - 2, 8)
            
        self._log_message(f"Optimized processing: batch size={self.batch_size}, parallel jobs={self.parallel_jobs}")
        
    def _count_total_records(self):
        """Efficiently count total records in the file"""
        try:
            import pandas as pd
            import chardet
            
            file_ext = os.path.splitext(self.file_path)[1].lower()
            
            # For Excel files
            if file_ext in ('.xlsx', '.xls'):
                with pd.ExcelFile(self.file_path) as xlsx:
                    sheet_name = xlsx.sheet_names[0]  # First sheet
                    self.total_records = xlsx.book.sheet_by_name(sheet_name).nrows - 1
            else:
                # For CSV, use faster line counting
                with open(self.file_path, 'rb') as f:
                    # Read a sample to detect encoding
                    sample = f.read(min(10000, os.path.getsize(self.file_path)))
                    detection = chardet.detect(sample)
                    encoding = detection["encoding"] or "utf-8"
                    
                # Count lines with proper encoding
                with open(self.file_path, 'r', encoding=encoding) as f:
                    self.total_records = sum(1 for _ in f) - 1
                    
            self.env.cr.commit()
            self._log_message(f"Counted {self.total_records} total records in file")
            
        except Exception as e:
            _logger.warning(f"Error counting records: {e}")
            # Estimate based on file size
            file_size = os.path.getsize(self.file_path)
            # Rough estimate based on file type
            if self.file_path.lower().endswith(('.xlsx', '.xls')):
                self.total_records = max(1, int(file_size / 500))  # Excel files are larger
            else:
                self.total_records = max(1, int(file_size / 200))  # Assume ~200 bytes per record
                
            self._log_message(f"Estimated {self.total_records} records based on file size")
            self.env.cr.commit()
            
    def _launch_parallel_jobs(self, start_time):
        """Launch parallel jobs for processing different segments of the file"""
        if self.total_records <= 0:
            return {'success': False, 'error_message': 'Cannot determine total records'}
            
        # Calculate records per job based on total and parallel settings
        records_per_job = math.ceil(self.total_records / self.parallel_jobs)
        
        # Reset jobs counter
        self.write({'completed_jobs': 0})
        self.env.cr.commit()
        
        # Create segment jobs
        for job_num in range(self.parallel_jobs):
            start_position = job_num * records_per_job
            
            # Skip if this segment is already processed or beyond total
            if start_position >= self.total_records:
                continue
                
            # Calculate end position (exclusive)
            end_position = min((job_num + 1) * records_per_job, self.total_records)
            
            # Calculate batch number for this segment
            batch_num = start_position // self.batch_size + 1
            
            # Create a job with queueing system or process directly
            self._create_segment_job(
                start_position=start_position,
                end_position=end_position,
                batch_num=batch_num, 
                job_index=job_num+1,
                start_time=start_time
            )
            
            _logger.info(
                f"Created segment {job_num+1}/{self.parallel_jobs} for import {self.id}: "
                f"{start_position}-{end_position}"
            )
            
        # Commit to ensure all jobs are created
        self.env.cr.commit()
        
        return {
            'success': True,
            'message': f"Started parallel processing with {self.parallel_jobs} jobs",
            'total_batches': self.total_batches
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
        
        try:
            # Verify file exists
            if not self.file_path or not os.path.exists(self.file_path):
                raise ValueError("Import file not found at specified path")
                
            self._log_message(f"Processing segment {start_position}-{end_position} (job {job_index})", "info")
            
            # Import the CSV processor - with fallbacks for different module structures
            try:
                # First try direct import
                from odoo.addons.compliance_management.services.csv_processor import CSVProcessor
                processor_class = CSVProcessor
            except ImportError:
                try:
                    # Try relative import
                    from . import services
                    processor_class = services.csv_processor.CSVProcessor
                except ImportError:
                    # Last fallback - look for other processor types
                    try:
                        from odoo.addons.compliance_management.services.csv_processor import CSVProcessor
                        processor_class = CSVProcessor
                    except ImportError:
                        # Final fallback
                        from odoo.addons.compliance_management.services.csv_processor import CSVProcessor
                        processor_class = CSVProcessor
            
            # Process batches within this segment
            current_position = start_position
            current_batch_num = batch_num
            
            while current_position < end_position:
                # Calculate batch end position
                batch_end = min(current_position + self.batch_size, end_position)
                
                # Log batch processing
                self._log_message(
                    f"Processing batch {current_batch_num} ({current_position}-{batch_end}) in job {job_index}",
                    "info"
                )
                
                # Process batch
                processor = processor_class(self)
                result = processor.process_batch(current_position, batch_end)
                
                # Update import log with atomic SQL for thread safety
                self._update_progress_counters(
                    current_position=batch_end,
                    successful=result.get("successful", 0),
                    failed=result.get("failed", 0),
                    duplicates=result.get("duplicates", 0),
                    processing_time=processor.process_time
                )
                
                # Move to next batch
                current_position = batch_end
                current_batch_num += 1
            
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
            
            # Update import log
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
        Update progress counters with advisory locks to prevent concurrent update issues
        """
        # with self.advisory_lock() as acquired:
        #     if not acquired:
        with self.advisory_lock() as acquired:
            if not acquired:
                # Don't proceed if lock not acquired, wait and retry
                time.sleep(1)
                _logger.warning(f"Could not acquire lock for import {self.id}, attempting update anyway")
                return self._update_progress_counters(current_position, successful, failed, duplicates, processing_time)
                
            # Use direct SQL update with GREATEST to prevent race conditions
            try:
                self.env.cr.execute("""
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
                
                # Make sure to commit immediately
                self.env.cr.commit()
                
            except Exception as e:
                _logger.error(f"Error updating import progress: {str(e)}")
                self.env.cr.rollback()
                
                # Try a simpler update as fallback
                try:
                    self.env.cr.execute("""
                        UPDATE import_log
                        SET 
                            current_batch = current_batch + 1
                        WHERE id = %s
                    """, (self.id,))
                    self.env.cr.commit()
                except:
                    pass
        
    def _mark_job_completed(self, segment_time, start_time):
        """Mark a job as completed with proper locking to prevent concurrency issues"""
        with self.advisory_lock() as acquired:
            if not acquired:
                _logger.warning(f"Could not acquire lock for import {self.id}, attempting update anyway")
                
            try:
                # Update job counter
                self.env.cr.execute("""
                    UPDATE import_log
                    SET completed_jobs = completed_jobs + 1
                    WHERE id = %s
                    RETURNING completed_jobs, parallel_jobs
                """, (self.id,))
                
                result = self.env.cr.fetchone()
                if not result:
                    return
                    
                completed_jobs, total_jobs = result
                
                # Update status if all jobs completed
                if completed_jobs >= total_jobs:
                    total_time = (datetime.now() - fields.Datetime.from_string(start_time)).total_seconds()
                    
                    # Get current counts to determine final status
                    self.env.cr.execute("""
                        SELECT successful_records, failed_records 
                        FROM import_log 
                        WHERE id = %s
                    """, (self.id,))
                    
                    success_count, fail_count = self.env.cr.fetchone() or (0, 0)
                    
                    # Determine final status
                    status = 'completed' if success_count > 0 else 'failed'
                    
                    # Update final status
                    self.write({
                        'status': status,
                        'completed_at': fields.Datetime.now(),
                        'execution_time': total_time
                    })
                    
                    # Generate and store final import summary
                    self._generate_import_summary()
                    
                # Always commit at the end
                self.env.cr.commit()
                
            except Exception as e:
                _logger.error(f"Error marking job complete: {str(e)}")
                self.env.cr.rollback()
            
    def _append_error_log(self, error_message, error_trace):
        """Append error information to the log"""
        self.env.cr.execute("""
            UPDATE import_log
            SET error_message = CONCAT(COALESCE(error_message, ''), %s),
                technical_details = CONCAT(COALESCE(technical_details, ''), %s)
            WHERE id = %s
        """, (
            f"\n{error_message}",
            f"\n{error_trace}",
            self.id
        ))
        self.env.cr.commit()
        
    def _log_message(self, message, message_type="info"):
        """Log a message to the import log and send via websocket"""
        # Add timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        full_message = f"[{timestamp}] {message}"
        
        # Append to log
        self.env.cr.execute("""
            UPDATE import_log
            SET log_messages = CONCAT(COALESCE(log_messages, ''), %s)
            WHERE id = %s
        """, (f"{full_message}\n", self.id))
        
        # Log to server
        log_level = {
            "info": _logger.info,
            "error": _logger.error,
            "success": _logger.info,
            "warning": _logger.warning,
        }.get(message_type, _logger.info)
        
        log_level(f"Import {self.id}: {message}")
        
        # Send to websocket if available
        try:
            from ..services.websocket.connection import send_message
            send_message(self.env, message, message_type, self.uploaded_by.id)
        except:
            pass
    
    def _generate_import_summary(self):
        """Generate a comprehensive summary of the import results"""
        try:
            # Get current statistics
            successful = self.successful_records
            failed = self.failed_records
            duplicates = self.duplicate_records
            total = self.total_records
            execution_time = self.execution_time
            
            # Calculate percentages
            if total > 0:
                success_pct = (successful / total) * 100
                failed_pct = (failed / total) * 100
                duplicate_pct = (duplicates / total) * 100
            else:
                success_pct = failed_pct = duplicate_pct = 0
                
            # Format summary
            summary = {
                'import_id': self.id,
                'model': self.model_name,
                'file_name': self.original_filename,
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
                }
            }
            
            # Store summary as JSON
            self.write({
                'summary': json.dumps(summary)
            })
            
            # Generate user-friendly message
            msg = f"""
    Import Summary for '{self.original_filename}':

    📊 Records Processed: {total:,}
    ✅ Successfully Imported: {successful:,} ({success_pct:.1f}%)
    ❌ Failed Records: {failed:,} ({failed_pct:.1f}%)
    ⚠️ Duplicate Records: {duplicates:,} ({duplicate_pct:.1f}%)

    ⏱️ Total Execution Time: {execution_time:.1f} seconds
    ⚡ Import Speed: {successful/execution_time:.1f} records/second
            """
            
            # Log message to chatter for historical reference
            self.message_post(body=msg)
            
            # Return summary
            return summary
            
        except Exception as e:
            _logger.error(f"Error generating import summary: {str(e)}")
            return None
            
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
