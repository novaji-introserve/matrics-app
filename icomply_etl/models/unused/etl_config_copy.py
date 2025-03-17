# # -*- coding: utf-8 -*-
# from odoo import models, fields, api, _
# from odoo.exceptions import ValidationError, UserError
# import json
# import logging
# import math
# import psycopg2
# import psycopg2.extras  # For execute_batch
# import pyodbc  # For MSSQL connections
# from datetime import datetime
# from contextlib import contextmanager
# import hashlib
# from decimal import Decimal
# import backoff
# from typing import Dict, List, Any, Set, Tuple, Optional
# import time
# import functools
# from lru import LRU as LRUDict
# # from odoo.addons.queue.queue_job.job import job  # Import the job decorator


# _logger = logging.getLogger(__name__)

# # Constants (adjust based on testing)
# LOOKUP_CACHE_SIZE = 50000
# BATCH_SIZE = 2000  # Adjust based on testing
# PROGRESS_UPDATE_INTERVAL = 5000


# class ETLSourceTable(models.Model):
#     _name = 'etl.source.table'
#     _description = 'ETL Source Table Configuration'
#     _order = 'sequence, name'
#     _inherit = ['mail.thread', 'mail.activity.mixin']
    
#     use_parallel_processing = fields.Boolean(
#         string='Use Parallel Processing',
#         default=False,
#         help='Enable parallel processing for large tables'
#     )

#     name = fields.Char('Table Name', required=True,
#                        help="Source table name (e.g., tbl_customer)")
#     sequence = fields.Integer('Sequence', default=10)
#     target_table = fields.Char(
#         'Target Table', required=True, help="Target table name (e.g., res_partner)")
#     primary_key = fields.Char('Primary Key', required=True)
#     batch_size = fields.Integer('Batch Size', default=BATCH_SIZE)
#     is_base_table = fields.Boolean(
#         'Is Base Table', help="Tables with no dependencies")
#     active = fields.Boolean(default=True)
#     progress_percentage = fields.Float('Progress', readonly=True, default=0)

#     category_id = fields.Many2one(
#         'etl.category', string='Category', required=True)
#     frequency_id = fields.Many2one(
#         'etl.frequency', string='Frequency', required=True)

#     job_uuid = fields.Char('Job UUID', readonly=True, copy=False)
#     job_status = fields.Selection([
#         ('pending', 'Pending'),
#         ('started', 'Started'),
#         ('done', 'Done'),
#         ('failed', 'Failed'),
#         ('canceled', 'Canceled'),
#     ], string='Job Status', readonly=True, copy=False)

#     dependency_ids = fields.Many2many(
#         'etl.source.table',
#         'etl_table_dependencies',
#         'table_id',
#         'dependency_id',
#         string='Dependencies'
#     )

#     mapping_ids = fields.One2many(
#         'etl.column.mapping', 'table_id', string='Column Mappings')
#     sync_log_ids = fields.One2many(
#         'etl.sync.log', 'table_id', string='Sync Logs')

#     last_sync_time = fields.Datetime('Last Sync Time', readonly=True)
#     last_sync_status = fields.Selection([
#         ('success', 'Success'),
#         ('failed', 'Failed'),
#         ('running', 'Running'),
#     ], string='Last Sync Status', readonly=True)
#     last_sync_message = fields.Text('Last Sync Message', readonly=True)

#     total_records_synced = fields.Integer(
#         'Total Records Synced', readonly=True)

#     @api.constrains('dependency_ids')
#     def _check_dependencies(self):
#         for table in self:
#             if table in table.dependency_ids:
#                 raise ValidationError(_("A table cannot depend on itself!"))

#     def get_config_json(self):
#         """Generate JSON configuration for ETL process"""
#         self.ensure_one()

#         # Normalize all mappings to lowercase
#         normalized_mappings = {}
#         for mapping in self.mapping_ids:
#             mapping_dict = {
#                 'target': mapping.target_column.lower(),
#                 'type': mapping.mapping_type,
#             }

#             if mapping.mapping_type == 'lookup':
#                 mapping_dict.update({
#                     'lookup_table': mapping.lookup_table.lower(),
#                     'lookup_key': mapping.lookup_key.lower(),
#                     'lookup_value': mapping.lookup_value.lower()
#                 })

#             # Store with lowercase source column as key
#             normalized_mappings[mapping.source_column.lower()] = mapping_dict

#         return {
#             'source_table': self.name.lower(),
#             'target_table': self.target_table.lower(),
#             'primary_key': self.primary_key.lower(),
#             'batch_size': self.batch_size,
#             'dependencies': [dep.name.lower() for dep in self.dependency_ids],
#             'mappings': normalized_mappings
#         }

   

#     def action_test_connection(self):
#         """Test database connections with improved connection handling"""
#         self.ensure_one()
#         try:
#             etl_manager = self.env['etl.manager']

#             # Initialize connection pools if available
#             if hasattr(etl_manager, 'init_connection_pools'):
#                 try:
#                     etl_manager.init_connection_pools()
#                     _logger.info("Connection pools initialized for testing")
#                 except Exception as e:
#                     _logger.warning(
#                         f"Connection pool initialization failed: {str(e)}")

#             # Use pooled connections if available, otherwise use regular connections
#             connection_method = getattr(
#                 etl_manager, 'get_pooled_connections', etl_manager.get_connections)

#             with connection_method() as (source_conn, pg_conn):
#                 source_cursor = source_conn.cursor()
#                 pg_cursor = pg_conn.cursor()

#                 # Get source DB type
#                 is_postgres_source = isinstance(
#                     source_conn, psycopg2.extensions.connection)

#                 # Test simple queries
#                 test_query = "SELECT 1" if is_postgres_source else "SELECT 1 AS test"
#                 source_cursor.execute(test_query)
#                 pg_cursor.execute("SELECT 1")

#                 # Get more detailed connection info for feedback
#                 source_type = "PostgreSQL" if is_postgres_source else "MSSQL"
#                 source_info = ""

#                 if is_postgres_source:
#                     try:
#                         # Get PostgreSQL version
#                         source_cursor.execute("SELECT version()")
#                         version = source_cursor.fetchone()[0].split(",")[0]
#                         source_info = f"{source_type} ({version})"
#                     except:
#                         source_info = source_type
#                 else:
#                     try:
#                         # Get MSSQL version
#                         source_cursor.execute("SELECT @@VERSION")
#                         version = source_cursor.fetchone()[0].split("\n")[0]
#                         source_info = f"{source_type} ({version})"
#                     except:
#                         source_info = source_type

#                 # Test table access
#                 try:
#                     config = self.get_config_json()
#                     table_delimiter = '"' if is_postgres_source else '['
#                     table_delimiter_end = '"' if is_postgres_source else ']'

#                     count_query = f"SELECT COUNT(*) FROM {table_delimiter}{config['source_table']}{table_delimiter_end}"
#                     source_cursor.execute(count_query)
#                     row_count = source_cursor.fetchone()[0]

#                     return {
#                         'type': 'ir.actions.client',
#                         'tag': 'display_notification',
#                         'params': {
#                             'title': _('Connection Successful'),
#                             'message': _(f'Successfully connected to {source_info}. Table {config["source_table"]} has {row_count} records.'),
#                             'type': 'success',
#                             'sticky': False,
#                         }
#                     }
#                 except Exception as table_error:
#                     # Table access failed but connection succeeded
#                     return {
#                         'type': 'ir.actions.client',
#                         'tag': 'display_notification',
#                         'params': {
#                             'title': _('Connection Successful, Table Access Failed'),
#                             'message': _(f'Connected to {source_info} but failed to access table: {str(table_error)}'),
#                             'type': 'warning',
#                             'sticky': True,
#                         }
#                     }

#         except Exception as e:
#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _('Connection Failed'),
#                     'message': str(e),
#                     'type': 'danger',
#                     'sticky': True,
#                 }
#             }
    
#     def sync_table_job_main(self, total_chunks):
#         """Main job that coordinates multiple chunk jobs"""
#         try:
#             self.write({
#                 'job_status': 'started',
#                 'progress_percentage': 0
#             })

#             # This job doesn't do processing itself - it just waits for all chunks
#             # to complete and then updates the final status

#             _logger.info(
#                 f"Main sync job started for table {self.name} with {total_chunks} chunks")

#             return {
#                 'table': self.name,
#                 'status': 'started',
#                 'message': f'Main sync job started with {total_chunks} chunks'
#             }
#         except Exception as e:
#             error_message = str(e)
#             _logger.error(
#                 f"Error in main sync job for table {self.name}: {error_message}")

#             self.write({
#                 'job_status': 'failed',
#                 'last_sync_status': 'failed',
#                 'last_sync_message': f'Main job error: {error_message}'
#             })

#             return {
#                 'table': self.name,
#                 'status': 'failed',
#                 'message': error_message
#             }

#     def sync_table_job_chunk(self, min_id, max_id, chunk_num, total_chunks):
#         """Process a chunk of a large table"""
#         try:
#             # We don't update the main job status since this is just a chunk
#             _logger.info(
#                 f"Starting chunk {chunk_num}/{total_chunks} for table {self.name}")

#             etl_manager = self.env['etl.manager']
#             stats = etl_manager.process_table_chunk(self, min_id, max_id)

#             # Update progress in main job
#             current_progress = (chunk_num / total_chunks) * 100
#             self.write({
#                 'progress_percentage': current_progress,
#                 'last_sync_message': f'Processed chunk {chunk_num}/{total_chunks} ({current_progress:.1f}%)'
#             })

#             # If this is the last chunk, mark the main job as complete
#             if chunk_num == total_chunks:
#                 self.write({
#                     'job_status': 'done',
#                     'progress_percentage': 100,
#                     'last_sync_message': f'All {total_chunks} chunks completed successfully'
#                 })

#             return {
#                 'table': self.name,
#                 'status': 'success',
#                 'chunk': chunk_num,
#                 'total_chunks': total_chunks,
#                 'message': f'Chunk from {min_id} to {max_id} completed successfully',
#                 'stats': stats
#             }
#         except Exception as e:
#             error_message = str(e)
#             _logger.error(
#                 f"Sync job failed for table {self.name} chunk {chunk_num}: {error_message}")

#             # Don't mark the main job as failed, just update the message
#             self.write({
#                 'last_sync_message': f'Error in chunk {chunk_num}/{total_chunks}: {error_message}'
#             })

#             return {
#                 'table': self.name,
#                 'status': 'failed',
#                 'chunk': chunk_num,
#                 'total_chunks': total_chunks,
#                 'message': error_message
#             }

#     # def sync_table_job(self):
#     #     """Background job to sync a table"""
#     #     try:
#     #         self.write({
#     #             'job_status': 'started',
#     #             'progress_percentage': 0
#     #         })

#     #         etl_manager = self.env['etl.manager']
#     #         etl_manager.process_table(self)

#     #         self.write({
#     #             'job_status': 'done',
#     #             'progress_percentage': 100,
#     #             'last_sync_message': f'Sync completed successfully at {fields.Datetime.now()}'
#     #         })

#     #         return {
#     #             'table': self.name,
#     #             'status': 'success',
#     #             'message': 'Sync completed successfully'
#     #         }

#     #     except Exception as e:
#     #         error_message = str(e)
#     #         _logger.error(
#     #             f"Sync job failed for table {self.name}: {error_message}")

#     #         self.write({
#     #             'job_status': 'failed',
#     #             'last_sync_status': 'failed',
#     #             'last_sync_message': error_message
#     #         })

#     #         return {
#     #             'table': self.name,
#     #             'status': 'failed',
#     #             'message': error_message
#     #         }

#     def action_view_jobs(self):
#         """View jobs related to this table"""
#         self.ensure_one()
#         return {
#             'type': 'ir.actions.act_window',
#             'name': _('Jobs'),
#             'res_model': 'queue.job',
#             'domain': [('uuid', '=', self.job_uuid)],
#             'view_mode': 'tree,form',
#             'context': self.env.context,
#         }

#     def get_primary_key_name(self, config, source_conn):
#         """
#         Retrieves the name of the primary key column from the information schema.
#         Adding schema
#         """
#         source_cursor = source_conn.cursor()
#         table_name = config['source_table']
#         schema_name = 'public'  # CHANGE THIS, get it from the configuration if needed

#         # Quote the table name to handle cases where it may be a reserved word
#         quoted_table_name = f'"{table_name}"'

#         query = f"""
#             SELECT column_name
#             FROM information_schema.key_column_usage
#             WHERE table_name = %s
#             AND table_schema = %s
#             AND constraint_name = 'PRIMARY'
#         """

#         try:
#             source_cursor.execute(query, (table_name, schema_name))
#             result = source_cursor.fetchone()
#             if result:
#                 return result[0]
#             else:
#                 _logger.warning(
#                     f"Primary key not found in information_schema for table {table_name} in schema {schema_name}")
#                 return None
#         except Exception as e:
#             _logger.error(
#                 f"Error fetching primary key from information_schema: {e}")
#             return None
    
#     # @job(default_channel='etl', default_priority=10)
#     def parallel_process_table_job(self):
#         """Job method for parallel processing"""
#         try:
#             # Update job status to started
#             self.write({
#                 'job_status': 'started',
#                 'progress_percentage': 0,
#                 'last_sync_message': f'Starting parallel processing with workers'
#             })

#             etl_manager = self.env['etl.manager']
#             result = etl_manager.parallel_process_table(self)

#             if not result:
#                 self.write({
#                     'job_status': 'done',
#                     'progress_percentage': 100,
#                     'last_sync_message': 'Parallel processing completed successfully'
#                 })
#                 return {
#                     'table': self.name,
#                     'status': 'success',
#                     'message': 'Parallel processing completed successfully'
#                 }

#             # For async parallel processing, just update status
#             self.write({
#                 'last_sync_message': f'Initiated {result.get("chunk_count", 0)} parallel chunks'
#             })

#             return {
#                 'table': self.name,
#                 'status': 'running',
#                 'message': 'Parallel processing initiated',
#                 'sync_log_id': result.get('sync_log_id', False)
#             }

#         except Exception as e:
#             error_message = str(e)
#             _logger.error(
#                 f"Parallel processing job failed for table {self.name}: {error_message}")

#             self.write({
#                 'job_status': 'failed',
#                 'last_sync_status': 'failed',
#                 'last_sync_message': f'Parallel processing error: {error_message}'
#             })

#             return {
#                 'table': self.name,
#                 'status': 'failed',
#                 'message': error_message
#             }
    
#     def sync_table_job(self):
#         """Improved background job to sync a table using optimized methods"""
#         try:
#             # Update job status to started
#             self.write({
#                 'job_status': 'started',
#                 'progress_percentage': 0
#             })

#             # Use the improved process_table method
#             etl_manager = self.env['etl.manager']

#             # Wrap process_table in a custom handler to update progress
#             original_logger_info = _logger.info

#             def custom_logger_info(message):
#                 # Intercept progress log messages to update the progress bar
#                 original_logger_info(message)
#                 if "Progress:" in message:
#                     try:
#                         # Extract progress percentage from the log message
#                         progress_str = message.split("Progress:")[
#                             1].split("%")[0].strip()
#                         progress = float(progress_str)

#                         # Update progress on the record
#                         self.write({
#                             'progress_percentage': progress,
#                             'last_sync_message': message
#                         })
#                     except Exception:
#                         pass  # If parsing fails, just continue

#             # Replace logger temporarily
#             _logger.info = custom_logger_info

#             try:
#                 # Use the improved process_table method
#                 etl_manager.improved_process_table(self)

#                 # Restore original logger
#                 _logger.info = original_logger_info

#                 # Update final status
#                 self.write({
#                     'job_status': 'done',
#                     'progress_percentage': 100,
#                     'last_sync_message': f'Sync completed successfully at {fields.Datetime.now()}'
#                 })

#                 return {
#                     'table': self.name,
#                     'status': 'success',
#                     'message': 'Sync completed successfully'
#                 }
#             finally:
#                 # Ensure logger is restored even if there's an exception
#                 _logger.info = original_logger_info

#         except Exception as e:
#             error_message = str(e)
#             _logger.error(
#                 f"Sync job failed for table {self.name}: {error_message}")

#             self.write({
#                 'job_status': 'failed',
#                 'last_sync_status': 'failed',
#                 'last_sync_message': error_message
#             })

#             return {
#                 'table': self.name,
#                 'status': 'failed',
#                 'message': error_message
#             }

#     # @job(default_channel='etl_chunk', default_priority=10)
#     def process_table_chunk_job(self, min_id, max_id):
#         """Job method for processing a table chunk with optimized methods"""
#         try:
#             _logger.info(
#                 f"Starting optimized chunk processing for table {self.name} (ID range: {min_id}-{max_id})")

#             etl_manager = self.env['etl.manager']
#             stats = etl_manager.process_table_chunk(self, min_id, max_id)

#             return {
#                 'table': self.name,
#                 'status': 'success',
#                 'message': f'Chunk processed successfully (ID range: {min_id}-{max_id})',
#                 'stats': stats
#             }
#         except Exception as e:
#             error_message = str(e)
#             _logger.error(
#                 f"Chunk processing failed for table {self.name}: {error_message}")

#             return {
#                 'table': self.name,
#                 'status': 'failed',
#                 'message': error_message
#             }

#     def action_sync_table(self):
#         """Optimized method to queue a sync job with improved processing strategies"""
#         self.ensure_one()

#         try:
#             # Initialize ETL Manager
#             etl_manager = self.env['etl.manager']

#             # Check for connection pooling initialization
#             if hasattr(etl_manager, 'init_connection_pools'):
#                 try:
#                     etl_manager.init_connection_pools()
#                 except Exception as e:
#                     _logger.warning(
#                         f"Connection pool initialization failed: {str(e)}")

#             with etl_manager.get_connections() as (source_conn, pg_conn):
#                 config = self.get_config_json()
#                 source_cursor = source_conn.cursor()

#                 # Determine source DB type for query syntax
#                 is_postgres_source = isinstance(
#                     source_conn, psycopg2.extensions.connection)
#                 table_delimiter = '"' if is_postgres_source else '['
#                 table_delimiter_end = '"' if is_postgres_source else ']'

#                 # Count total records
#                 count_query = f"SELECT COUNT(*) FROM {table_delimiter}{config['source_table']}{table_delimiter_end}"
#                 source_cursor.execute(count_query)
#                 total_count = source_cursor.fetchone()[0]

#                 _logger.info(f"Table {self.name} has {total_count} records")

#                 # For very small tables, process synchronously
#                 if total_count < 10000:
#                     # Use improved process_table for small tables
#                     etl_manager.improved_process_table(self)
#                     return {
#                         'type': 'ir.actions.client',
#                         'tag': 'display_notification',
#                         'params': {
#                             'title': _('Success'),
#                             'message': _('Table synchronization completed successfully.'),
#                             'type': 'success',
#                             'sticky': False,
#                         }
#                     }

#                 # Decision tree for optimal processing strategy
#                 # if self.use_parallel_processing and total_count > 50000:
#                 if total_count > 50000:
#                     # Determine optimal number of workers based on table size
#                     if total_count < 100000:
#                         num_workers = 2
#                     elif total_count < 500000:
#                         num_workers = 4
#                     elif total_count < 1000000:
#                         num_workers = 6
#                     else:
#                         num_workers = 8

#                     job = self.with_delay(
#                         description=f"Parallel sync for table: {self.name}  workers)",
#                         channel='etl',
#                         priority=10
#                     ).parallel_process_table_job()

#                     self.write({
#                         'job_uuid': job.uuid,
#                         'job_status': 'pending',
#                         'last_sync_status': 'running',
#                         'last_sync_message': f'Parallel sync job queued with 4 workers',
#                         'progress_percentage': 0
#                     })

#                     return {
#                         'type': 'ir.actions.client',
#                         'tag': 'display_notification',
#                         'params': {
#                             'title': _('Parallel Job Queued'),
#                             'message': _(f'Table synchronization queued with 4 parallel workers.'),
#                             'type': 'success',
#                             'sticky': False,
#                         }
#                     }

#                 # For medium-sized tables or when parallel processing is disabled
#                 elif total_count < 500000:
#                     job = self.with_delay(
#                         description=f"Sync ETL table: {self.name} (optimized)"
#                     ).sync_table_job()

#                     self.write({
#                         'job_uuid': job.uuid,
#                         'job_status': 'pending',
#                         'last_sync_status': 'running',
#                         'last_sync_message': 'Optimized sync job queued',
#                         'progress_percentage': 0
#                     })

#                     return {
#                         'type': 'ir.actions.client',
#                         'tag': 'display_notification',
#                         'params': {
#                             'title': _('Job Queued'),
#                             'message': _('Optimized table synchronization job has been queued.'),
#                             'type': 'success',
#                             'sticky': False,
#                         }
#                     }

#                 # For very large tables, use chunking with the new optimized chunk processing
#                 else:
#                     # Get primary key for chunking
#                     primary_key = config['primary_key']
#                     primary_key_original = None

#                     # Get original column name with proper case
#                     query = (
#                         f"SELECT TOP 1 * FROM {table_delimiter}{config['source_table']}{table_delimiter_end}"
#                         if not is_postgres_source
#                         else f'SELECT * FROM "{config["source_table"]}" LIMIT 1'
#                     )
#                     source_cursor.execute(query)
#                     for col in source_cursor.description:
#                         if col[0].lower() == primary_key.lower():
#                             primary_key_original = col[0]
#                             break

#                     if not primary_key_original:
#                         raise ValueError(
#                             f"Could not find primary key {primary_key} in table")

#                     # Calculate optimal chunk size based on total records
#                     if total_count < 750000:
#                         chunk_size = 150000
#                     elif total_count < 2000000:
#                         chunk_size = 200000
#                     else:
#                         chunk_size = 250000

#                     chunks = math.ceil(total_count / chunk_size)

#                     # Get range of primary key values
#                     min_query = (
#                         f"SELECT MIN({table_delimiter}{primary_key_original}{table_delimiter_end}) FROM {table_delimiter}{config['source_table']}{table_delimiter_end}"
#                     )
#                     max_query = (
#                         f"SELECT MAX({table_delimiter}{primary_key_original}{table_delimiter_end}) FROM {table_delimiter}{config['source_table']}{table_delimiter_end}"
#                     )
#                     source_cursor.execute(min_query)
#                     min_id = source_cursor.fetchone()[0]
#                     source_cursor.execute(max_query)
#                     max_id = source_cursor.fetchone()[0]

#                     _logger.info(
#                         f"Splitting table {self.name} into {chunks} chunks (min_id={min_id}, max_id={max_id})"
#                     )

#                     # Create a main job record for tracking overall progress
#                     main_job = self.with_delay(
#                         description=f"Main sync job for table: {self.name}",
#                         channel='etl_main'
#                     ).sync_table_job_main(chunks)

#                     # Update the main job UUID
#                     self.write({
#                         'job_uuid': main_job.uuid,
#                         'job_status': 'pending',
#                         'last_sync_status': 'running',
#                         'last_sync_message': f'Optimized sync job queued in {chunks} chunks',
#                         'progress_percentage': 0
#                     })

#                     # Queue multiple jobs with ID ranges
#                     for i in range(chunks):
#                         # Calculate chunk boundaries
#                         if isinstance(min_id, str) and isinstance(max_id, str):
#                             # For string IDs, divide the chunks evenly based on index
#                             chunk_min = min_id if i == 0 else f"{self.name}_chunk_{i}"
#                             chunk_max = max_id if i == chunks - \
#                                 1 else f"{self.name}_chunk_{i+1}"
#                         else:
#                             # For numeric IDs, calculate range
#                             chunk_min = min_id + \
#                                 (i * (max_id - min_id) // chunks)
#                             chunk_max = min_id + \
#                                 ((i + 1) * (max_id - min_id) // chunks)
#                             if i == chunks - 1:
#                                 chunk_max = max_id  # Ensure we include the max value

#                         # Queue this chunk's job using optimized chunk processing
#                         self.with_delay(
#                             description=f"Optimized sync: {self.name} (chunk {i+1}/{chunks})",
#                             channel='etl_chunk',
#                             priority=10
#                         ).process_table_chunk_job(chunk_min, chunk_max)

#                     return {
#                         'type': 'ir.actions.client',
#                         'tag': 'display_notification',
#                         'params': {
#                             'title': _('Optimized Jobs Queued'),
#                             'message': _(f'Table synchronization split into {chunks} optimized jobs and queued.'),
#                             'type': 'success',
#                             'sticky': False,
#                         }
#                     }

#         except Exception as e:
#             error_message = str(e)
#             _logger.error(f"Error in sync table action: {error_message}")

#             return {
#                 'type': 'ir.actions.client',
#                 'tag': 'display_notification',
#                 'params': {
#                     'title': _('Error'),
#                     'message': error_message,
#                     'type': 'danger',
#                     'sticky': True,
#                 }
#             }




# class ETLCategory(models.Model):
#     _name = 'etl.category'
#     _description = 'ETL Table Category'
#     _order = 'sequence, name'

#     name = fields.Char('Category Name', required=True)
#     code = fields.Char('Category Code', required=True)
#     sequence = fields.Integer('Sequence', default=10)
#     active = fields.Boolean(default=True)

#     _sql_constraints = [
#         ('code_uniq', 'unique (code)', 'Category code must be unique!')
#     ]


# class ETLFrequency(models.Model):
#     _name = 'etl.frequency'
#     _description = 'ETL Sync Frequency'
#     _order = 'sequence, name'

#     name = fields.Char('Frequency Name', required=True)
#     code = fields.Char('Frequency Code', required=True)
#     interval_number = fields.Integer(
#         'Interval Number', default=1, required=True)
#     interval_type = fields.Selection([
#         ('minutes', 'Minutes'),
#         ('hours', 'Hours'),
#         ('days', 'Days'),
#         ('weeks', 'Weeks'),
#         ('months', 'Months')
#     ], string='Interval Type', required=True)
#     sequence = fields.Integer('Sequence', default=10)
#     active = fields.Boolean(default=True)

#     _sql_constraints = [
#         ('code_uniq', 'unique (code)', 'Frequency code must be unique!')
#     ]


# class ETLColumnMapping(models.Model):
#     _name = 'etl.column.mapping'
#     _description = 'ETL Column Mapping'
#     _order = 'sequence, id'

#     sequence = fields.Integer('Sequence', default=10)
#     table_id = fields.Many2one(
#         'etl.source.table', required=True, ondelete='cascade')
#     source_column = fields.Char('Source Column', required=True)
#     target_column = fields.Char('Target Column', required=True)
#     mapping_type = fields.Selection([
#         ('direct', 'Direct'),
#         ('lookup', 'Lookup')
#     ], required=True, default='direct')

#     # For lookup mappings
#     lookup_table = fields.Char('Lookup Table')
#     lookup_key = fields.Char('Lookup Key')
#     lookup_value = fields.Char('Lookup Value')

#     active = fields.Boolean(default=True)

#     @api.model
#     def create(self, vals):
#         """Override create to handle case normalization"""
#         if vals.get('target_column'):
#             vals['target_column'] = vals['target_column'].lower()
#         if vals.get('lookup_table'):
#             vals['lookup_table'] = vals['lookup_table'].lower()
#         if vals.get('lookup_key'):
#             vals['lookup_key'] = vals['lookup_key'].lower()
#         if vals.get('lookup_value'):
#             vals['lookup_value'] = vals['lookup_value'].lower()
#         return super().create(vals)

#     def write(self, vals):
#         """Override write to handle case normalization"""
#         if vals.get('target_column'):
#             vals['target_column'] = vals['target_column'].lower()
#         if vals.get('lookup_table'):
#             vals['lookup_table'] = vals['lookup_table'].lower()
#         if vals.get('lookup_key'):
#             vals['lookup_key'] = vals['lookup_key'].lower()
#         if vals.get('lookup_value'):
#             vals['lookup_value'] = vals['lookup_value'].lower()
#         return super().write(vals)

#     @api.constrains('mapping_type', 'lookup_table', 'lookup_key', 'lookup_value')
#     def _check_lookup_fields(self):
#         for mapping in self:
#             if mapping.mapping_type == 'lookup':
#                 if not (mapping.lookup_table and mapping.lookup_key and mapping.lookup_value):
#                     raise ValidationError(
#                         _("Lookup mappings require lookup table, key, and value!"))


# class ETLSyncLog(models.Model):
#     _name = 'etl.sync.log'
#     _description = 'ETL Synchronization Log'
#     _order = 'create_date desc'

#     table_id = fields.Many2one(
#         'etl.source.table', string='Table', required=True)
#     start_time = fields.Datetime('Start Time', required=True)
#     end_time = fields.Datetime('End Time')
#     status = fields.Selection([
#         ('success', 'Success'),
#         ('failed', 'Failed'),
#         ('running', 'Running'),
#     ], required=True)
#     total_records = fields.Integer('Total Records')
#     new_records = fields.Integer('New Records')
#     updated_records = fields.Integer('Updated Records')
#     error_message = fields.Text('Error Message')
#     row_hashes = fields.Text(
#         'Row Hashes', help="JSON string storing the row hashes for change detection")

#     def name_get(self):
#         return [(log.id, f"{log.table_id.name} - {log.start_time}") for log in self]

#     @api.model
#     def create(self, vals):
#         """Override create to ensure row_hashes is properly formatted"""
#         if 'row_hashes' in vals and isinstance(vals['row_hashes'], dict):
#             vals['row_hashes'] = json.dumps(vals['row_hashes'])
#         return super().create(vals)

#     def write(self, vals):
#         """Override write to ensure row_hashes is properly formatted"""
#         if 'row_hashes' in vals and isinstance(vals['row_hashes'], dict):
#             vals['row_hashes'] = json.dumps(vals['row_hashes'])
#         return super().write(vals)
