# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import logging
import time
import psutil
import os
import gc
import tempfile
import json
import threading
from contextlib import contextmanager
from functools import wraps
import weakref

_logger = logging.getLogger(__name__)

def memory_tracked(func):
    """Decorator to track memory usage of functions"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        memory_before = process.memory_info().rss / 1024 / 1024  # MB
        
        # Force garbage collection before operation
        gc.collect()
        
        start_time = time.time()
        result = None
        
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            # Force garbage collection after operation
            gc.collect()
            
            memory_after = process.memory_info().rss / 1024 / 1024  # MB
            memory_diff = memory_after - memory_before
            execution_time = time.time() - start_time
            
            # Get function name and calling class if possible
            func_name = func.__name__
            class_name = ""
            
            if args and hasattr(args[0], "__class__") and hasattr(args[0].__class__, "__name__"):
                class_name = args[0].__class__.__name__
            
            _logger.info(f"Memory usage for {class_name}.{func_name}: "
                        f"Before={memory_before:.1f}MB, After={memory_after:.1f}MB, "
                        f"Diff={memory_diff:+.1f}MB, Time={execution_time:.2f}s")
            
            # Log to database if significant memory change or slow execution
            if abs(memory_diff) > 50 or execution_time > 5.0:
                try:
                    # Extract self if it exists in args
                    self = args[0] if args and hasattr(args[0], 'env') else None
                    
                    if self and hasattr(self, 'env'):
                        # Log performance data
                        self.env['etl.performance.log'].sudo().create({
                            'name': f"{class_name}.{func_name}",
                            'execution_time': execution_time,
                            'memory_before': memory_before,
                            'memory_after': memory_after,
                            'memory_diff': memory_diff,
                            'details': json.dumps({
                                'args': str(args[1:])[:100] if len(args) > 1 else '',
                                'kwargs': str(kwargs)[:100] if kwargs else ''
                            })
                        })
                except Exception as e:
                    _logger.warning(f"Failed to log performance data: {str(e)}")
    
    return wrapper

class ETLPerformanceLog(models.Model):
    _name = 'etl.performance.log'
    _description = 'ETL Performance Log'
    _order = 'date desc, execution_time desc'
    
    name = fields.Char('Function Name', required=True)
    execution_time = fields.Float('Execution Time (sec)', required=True)
    date = fields.Datetime('Date', required=True, default=fields.Datetime.now)
    user_id = fields.Many2one('res.users', string='User', default=lambda self: self.env.user)
    
    table_id = fields.Many2one('etl.source.table', string='ETL Table')
    memory_before = fields.Float('Memory Before (MB)')
    memory_after = fields.Float('Memory After (MB)')
    memory_diff = fields.Float('Memory Diff (MB)', compute='_compute_memory_diff', store=True)
    details = fields.Text('Execution Details')
    
    @api.depends('memory_before', 'memory_after')
    def _compute_memory_diff(self):
        for log in self:
            log.memory_diff = log.memory_after - log.memory_before if log.memory_before and log.memory_after else 0.0

class ETLBatchProcessor(models.AbstractModel):
    _name = 'etl.batch.processor'
    _description = 'ETL Batch Processing Utilities'
    
    @api.model
    def process_in_batches(self, items, batch_size, process_func, *args, **kwargs):
        """Process a large number of items in batches
        
        Args:
            items: List of items to process
            batch_size: Number of items per batch
            process_func: Function to call with each batch
            *args, **kwargs: Additional arguments to pass to process_func
            
        Returns:
            List of results from each batch
        """
        results = []
        total_items = len(items)
        processed = 0
        
        for i in range(0, total_items, batch_size):
            batch = items[i:i + batch_size]
            
            with memory_profile():
                result = process_func(batch, *args, **kwargs)
                results.append(result)
            
            processed += len(batch)
            _logger.info(f"Processed {processed}/{total_items} items ({processed/total_items*100:.1f}%)")
            
            # Explicitly free memory after each batch
            gc.collect()
            
            # Optional small delay to prevent CPU overload
            time.sleep(0.01)
        
        return results
    
    @api.model
    def generator_batch_process(self, generator, batch_size, process_func, *args, **kwargs):
        """Process items from a generator in batches without loading all into memory
        
        Args:
            generator: Generator yielding items to process
            batch_size: Number of items per batch
            process_func: Function to call with each batch
            *args, **kwargs: Additional arguments to pass to process_func
            
        Returns:
            List of results from each batch
        """
        results = []
        batch = []
        processed = 0
        
        for item in generator:
            batch.append(item)
            
            if len(batch) >= batch_size:
                with memory_profile():
                    result = process_func(batch, *args, **kwargs)
                    results.append(result)
                
                processed += len(batch)
                _logger.info(f"Processed {processed} items")
                
                # Clear batch and free memory
                batch = []
                gc.collect()
        
        # Process any remaining items
        if batch:
            with memory_profile():
                result = process_func(batch, *args, **kwargs)
                results.append(result)
            
            processed += len(batch)
            _logger.info(f"Processed {processed} items (final batch)")
        
        return results
    
    @api.model
    def chunked_queryset(self, model, domain, chunk_size=1000, fields=None, order=None):
        """Generator that yields chunks of records from a query without loading all into memory
        
        Args:
            model: Odoo model to query
            domain: Search domain
            chunk_size: Number of records per chunk
            fields: List of fields to read
            order: Order string for the query
            
        Yields:
            Lists of record dicts
        """
        offset = 0
        
        while True:
            records = model.search(domain, limit=chunk_size, offset=offset, order=order)
            if not records:
                break
                
            if fields:
                yield records.read(fields)
            else:
                yield records
                
            offset += chunk_size
            # Clear records from cache
            model.clear_caches()
    
    @api.model
    def bulk_create(self, model, values_list, batch_size=1000):
        """Create many records in batches to avoid memory issues
        
        Args:
            model: Model to create records in
            values_list: List of value dictionaries
            batch_size: Batch size for creation
            
        Returns:
            IDs of created records
        """
        record_ids = []
        
        for i in range(0, len(values_list), batch_size):
            batch_values = values_list[i:i+batch_size]
            batch_records = model.create(batch_values)
            record_ids.extend(batch_records.ids)
            
            # Commit after each batch to save resources (optional, depends on use case)
            if hasattr(model, 'env') and hasattr(model.env, 'cr'):
                model.env.cr.commit()
                
            # Clear caches
            model.clear_caches()
            
        return record_ids

@contextmanager
def memory_profile():
    """Context manager to profile memory usage"""
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / 1024 / 1024  # MB
    gc.collect()  # Force garbage collection before starting
    
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        gc.collect()  # Force garbage collection after execution
        mem_after = process.memory_info().rss / 1024 / 1024  # MB
        elapsed_time = end_time - start_time
        
        _logger.info(f"Memory usage: Before={mem_before:.2f}MB, After={mem_after:.2f}MB, "
                    f"Diff={mem_after - mem_before:.2f}MB, Time={elapsed_time:.2f}s")

class ETLSystemMonitor(models.AbstractModel):
    _name = 'etl.system.monitor'
    _description = 'ETL System Resource Monitor'
    
    @api.model
    def get_system_resources(self):
        """Get current system resource usage"""
        process = psutil.Process(os.getpid())
        
        # Memory usage
        mem_info = process.memory_info()
        mem_usage = {
            'rss': mem_info.rss / 1024 / 1024,  # MB
            'vms': mem_info.vms / 1024 / 1024,  # MB
        }
        
        # CPU usage
        cpu_percent = process.cpu_percent(interval=0.1)
        
        # System-wide stats
        sys_mem = psutil.virtual_memory()
        sys_cpu = psutil.cpu_percent(interval=0.1)
        
        # Database connections
        db_connections = 0
        try:
            if hasattr(self.env, 'cr') and hasattr(self.env.cr, 'dbname'):
                self.env.cr.execute("SELECT count(*) FROM pg_stat_activity WHERE datname = %s", 
                                 (self.env.cr.dbname,))
                db_connections = self.env.cr.fetchone()[0]
        except Exception as e:
            _logger.warning(f"Failed to get database connections: {str(e)}")
        
        # Queue job stats
        job_stats = {
            'pending': 0,
            'running': 0
        }
        try:
            job_stats['pending'] = self.env['queue.job'].search_count([('state', '=', 'pending')])
            job_stats['running'] = self.env['queue.job'].search_count([('state', '=', 'started')])
        except Exception as e:
            _logger.warning(f"Failed to get job stats: {str(e)}")
        
        return {
            'time': fields.Datetime.now(),
            'process': {
                'memory': mem_usage,
                'cpu_percent': cpu_percent,
            },
            'system': {
                'memory_percent': sys_mem.percent,
                'memory_available': sys_mem.available / 1024 / 1024,  # MB
                'cpu_percent': sys_cpu,
            },
            'database': {
                'connections': db_connections,
            },
            'queue': job_stats
        }
    
    @api.model
    def log_system_resources(self):
        """Log current system resources to database"""
        resources = self.get_system_resources()
        
        try:
            self.env['etl.system.log'].create({
                'date': resources['time'],
                'process_memory_mb': resources['process']['memory']['rss'],
                'process_cpu_percent': resources['process']['cpu_percent'],
                'system_memory_percent': resources['system']['memory_percent'],
                'system_cpu_percent': resources['system']['cpu_percent'],
                'db_connections': resources['database']['connections'],
                'queue_jobs_pending': resources['queue']['pending'],
                'queue_jobs_running': resources['queue']['running'],
                'details': json.dumps(resources)
            })
            self.env.cr.commit()
        except Exception as e:
            _logger.error(f"Failed to log system resources: {str(e)}")
    
    @api.model
    def check_resource_limits(self):
        """Check if system resources are near limits and take action if needed"""
        resources = self.get_system_resources()
        
        # Check memory usage
        if resources['system']['memory_percent'] > 90:
            _logger.warning("System memory usage is over 90%! Taking preventive actions...")
            
            # Actions for high memory usage
            self._handle_high_memory()
        
        # Check database connections
        if resources['database']['connections'] > 80:  # Assuming default max_connections=100
            _logger.warning("Database connections are high! Taking preventive actions...")
            
            # Actions for high DB connections
            self._handle_high_db_connections()
        
        # Check if too many jobs are running
        if resources['queue']['running'] > 20:  # Adjust based on your system capacity
            _logger.warning("Too many jobs running concurrently! Taking preventive actions...")
            
            # Actions for too many running jobs
            self._handle_too_many_jobs()
    
    def _handle_high_memory(self):
        """Handle high memory usage situation"""
        # Force garbage collection
        gc.collect()
        
        # Pause some running jobs if needed
        running_jobs = self.env['queue.job'].search([
            ('state', '=', 'started'),
            ('model_name', '=', 'etl.source.table'),
            ('method_name', '=', 'sync_table_job')
        ], limit=5)
        
        if running_jobs:
            _logger.info(f"Pausing {len(running_jobs)} ETL jobs due to high memory usage")
            for job in running_jobs:
                try:
                    job.button_requeue()
                    time.sleep(1)  # Brief pause between operations
                except Exception as e:
                    _logger.error(f"Failed to pause job {job.uuid}: {str(e)}")
        
        # Clear caches
        self.env.clear()
    
    def _handle_high_db_connections(self):
        """Handle high database connections situation"""
        # Find and close any abandoned transactions
        try:
            self.env.cr.execute("""
                SELECT pid, query_start, state, query
                FROM pg_stat_activity
                WHERE datname = %s 
                AND state = 'idle in transaction'
                AND query_start < NOW() - INTERVAL '30 minutes'
            """, (self.env.cr.dbname,))
            
            for record in self.env.cr.dictfetchall():
                pid = record['pid']
                _logger.warning(f"Terminating idle transaction for PID {pid}")
                try:
                    self.env.cr.execute(f"SELECT pg_terminate_backend({pid})")
                except Exception as e:
                    _logger.error(f"Failed to terminate backend {pid}: {str(e)}")
        except Exception as e:
            _logger.error(f"Failed to check for idle transactions: {str(e)}")
    
    def _handle_too_many_jobs(self):
        """Handle situation with too many concurrent jobs"""
        # Requeue some jobs to run later
        running_jobs = self.env['queue.job'].search([
            ('state', '=', 'started')
        ], order='date_created asc', limit=5)
        
        if running_jobs:
            _logger.info(f"Requeuing {len(running_jobs)} jobs due to high concurrency")
            for job in running_jobs:
                try:
                    job.button_requeue()
                except Exception as e:
                    _logger.error(f"Failed to requeue job {job.uuid}: {str(e)}")

class ETLSystemLog(models.Model):
    _name = 'etl.system.log'
    _description = 'ETL System Resource Log'
    _order = 'date desc'
    
    date = fields.Datetime('Timestamp', required=True)
    process_memory_mb = fields.Float('Process Memory (MB)')
    process_cpu_percent = fields.Float('Process CPU (%)')
    system_memory_percent = fields.Float('System Memory (%)')
    system_cpu_percent = fields.Float('System CPU (%)')
    db_connections = fields.Integer('DB Connections')
    queue_jobs_pending = fields.Integer('Pending Jobs')
    queue_jobs_running = fields.Integer('Running Jobs')
    details = fields.Text('Details')
    
    def name_get(self):
        return [(log.id, f"System Log - {log.date}") for log in self]

class MemoryEfficientJSONSerializer:
    """Utility for memory-efficient JSON serialization/deserialization"""
    
    @staticmethod
    def serialize_to_file(data, file_path=None):
        """Serialize data to a file with minimal memory usage"""
        if file_path is None:
            # Create a temp file
            with tempfile.NamedTemporaryFile(delete=False, mode='w+') as f:
                file_path = f.name
        
        with open(file_path, 'w') as f:
            # Handle different data types
            if isinstance(data, list):
                # Write as JSON lines for large lists
                f.write('[')
                for i, item in enumerate(data):
                    if i > 0:
                        f.write(',')
                    json.dump(item, f)
                f.write(']')
            else:
                # Regular JSON for other types
                json.dump(data, f)
        
        return file_path
    
    @staticmethod
    def deserialize_from_file(file_path):
        """Deserialize data from a file with minimal memory usage"""
        with open(file_path, 'r') as f:
            return json.load(f)
    
    @staticmethod
    def stream_large_list(file_path):
        """Stream a large list from a file one item at a time"""
        with open(file_path, 'r') as f:
            # Check if it starts with a bracket
            char = f.read(1)
            if char != '[':
                f.seek(0)
                # Not a JSON array, try to parse whole file
                return json.load(f)
            
            # Start parsing array items
            buffer = ""
            depth = 1  # We've already seen one [
            in_string = False
            escape_next = False
            
            while True:
                char = f.read(1)
                if not char:  # End of file
                    break
                
                buffer += char
                
                # Handle string boundaries
                if char == '"' and not escape_next:
                    in_string = not in_string
                elif char == '\\' and in_string and not escape_next:
                    escape_next = True
                    continue
                
                if not in_string:
                    if char == '{':
                        depth += 1
                    elif char == '}':
                        depth -= 1
                    elif char == '[':
                        depth += 1
                    elif char == ']':
                        depth -= 1
                    elif char == ',' and depth == 1:
                        # We've reached the end of an item at the top level
                        try:
                            item = json.loads(buffer[:-1])  # Remove the comma
                            yield item
                        except json.JSONDecodeError as e:
                            _logger.error(f"Error parsing JSON item: {str(e)}, content: {buffer}")
                        buffer = ""
                
                escape_next = False
            
            # Handle the last item
            if buffer.rstrip().endswith(']'):
                buffer = buffer[:-1]  # Remove the closing bracket
            
            if buffer.strip():
                try:
                    item = json.loads(buffer)
                    yield item
                except json.JSONDecodeError as e:
                    _logger.error(f"Error parsing last JSON item: {str(e)}, content: {buffer}")
