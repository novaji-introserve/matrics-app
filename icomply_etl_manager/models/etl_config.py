# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import json
import logging
from datetime import datetime, timedelta

# Simple queue job support
try:
    from odoo.addons.queue_job.job import Job
    QUEUE_JOB_AVAILABLE = True
except ImportError:
    QUEUE_JOB_AVAILABLE = False

_logger = logging.getLogger(__name__)

class ETLSourceTable(models.Model):
    _name = 'etl.source.table'
    _description = 'ETL Source Table Configuration - Simplified'
    _order = 'sequence, name'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char('Table Name', required=True, help="Source table name (e.g., tbl_customer)")
    sequence = fields.Integer('Sequence', default=10)
    
    # Connection Configuration
    source_connection_id = fields.Many2one('etl.database.connection', 
                                          string='Source Database Connection', 
                                          required=True, tracking=True)
    target_connection_id = fields.Many2one('etl.database.connection', 
                                          string='Target Database Connection', 
                                          required=True, tracking=True)
    
    # Table Configuration
    source_table_name = fields.Char('Source Table Name', required=True)
    target_table_name = fields.Char('Target Table Name', required=True)
    primary_key_unique = fields.Char('Primary Key/Unique Column', required=True)
    batch_size = fields.Integer('Batch Size', default=2000)
    
    active = fields.Boolean(default=True)
    
    # Categories
    category_id = fields.Many2one('etl.category', string='Category', required=True)
    
    # Sync Configuration
    full_sync_enabled = fields.Boolean('Enable Full Sync', default=True, tracking=True)
    full_sync_frequency_id = fields.Many2one('etl.frequency', string='Full Sync Frequency')
    
    incremental_sync_enabled = fields.Boolean('Enable Incremental Sync', default=False, tracking=True)
    incremental_frequency_minutes = fields.Integer('Incremental Frequency (Minutes)', default=30)
    incremental_date_column = fields.Char('Date Column for Incremental')
    
    # Dependencies
    dependency_ids = fields.Many2many('etl.source.table', 'etl_table_dependencies', 
                                     'table_id', 'dependency_id', string='Dependencies')
    
    # Mappings and Logs
    mapping_ids = fields.One2many('etl.column.mapping', 'table_id', string='Column Mappings')
    sync_log_ids = fields.One2many('etl.sync.log', 'table_id', string='Sync Logs')
    
    # Simple Status Tracking
    last_full_sync_time = fields.Datetime('Last Full Sync Time', readonly=True)
    last_incremental_sync = fields.Datetime('Last Incremental Sync', readonly=True)
    last_sync_status = fields.Selection([
        ('success', 'Success'),
        ('failed', 'Failed'),
        ('running', 'Running'),
    ], string='Last Sync Status', readonly=True)
    last_sync_message = fields.Text('Last Sync Message', readonly=True)
    
    # Simple Statistics
    total_records_synced = fields.Integer('Total Records Synced', readonly=True)
    estimated_record_count = fields.Integer('Estimated Record Count', readonly=True)
    
    # Simple Job Status
    job_uuid = fields.Char('Job UUID', readonly=True, copy=False)
    job_status = fields.Selection([
        ('pending', 'Pending'),
        ('started', 'Started'),
        ('done', 'Done'),
        ('failed', 'Failed'),
    ], string='Job Status', readonly=True, copy=False)

    # CASE SENSITIVITY AND QUOTING HELPER METHODS (RESTORED FROM ORIGINAL)
    def _is_postgres_connection(self, connection):
        """Check if a connection is PostgreSQL"""
        return connection and connection.database_type == 'postgresql'
    
    def _normalize_for_database(self, value, connection):
        """Normalize value based on database type - preserve case for PostgreSQL"""
        if not value:
            return value
        return value if self._is_postgres_connection(connection) else value.lower()
    
    def _compare_column_names(self, col1, col2, is_postgres):
        """Compare column names considering case sensitivity"""
        if is_postgres:
            return col1 == col2  # Case-sensitive comparison
        else:
            return col1.lower() == col2.lower()  # Case-insensitive comparison

    # def _quote_table_name(self, table_name, connection):
    #     """Quote table name based on database type"""
    #     if not table_name:
    #         return table_name
            
    #     if self._is_postgres_connection(connection):
    #         return f'"{table_name}"'
    #     elif connection.database_type == 'mssql':
    #         return f'[{table_name}]'
    #     elif connection.database_type == 'mysql':
    #         return f'`{table_name}`'
    #     else:
    #         return table_name
    
    def _quote_table_name(self, table_name, connection):
        """Quote table name based on database type - FIXED for Oracle"""
        if not table_name:
            return table_name
            
        if self._is_postgres_connection(connection):
            return f'"{table_name}"'
        elif connection.database_type == 'mssql':
            return f'[{table_name}]'
        elif connection.database_type == 'mysql':
            return f'`{table_name}`'
        elif connection.database_type == 'oracle':
            # FIXED: Oracle - use table name exactly as provided
            # This handles both "table" and "schema.table" formats properly
            return table_name
        else:
            return table_name

    def _quote_column_name(self, column_name, connection):
        """Quote column name based on database type"""
        if not column_name:
            return column_name
            
        if self._is_postgres_connection(connection):
            return f'"{column_name}"'
        elif connection.database_type == 'mssql':
            return f'[{column_name}]'
        elif connection.database_type == 'mysql':
            return f'`{column_name}`'
        else:
            return column_name

    def _get_quoted_table_name(self, table_name_field, connection):
        """Get properly quoted table name from field"""
        table_name = getattr(self, table_name_field)
        return self._quote_table_name(table_name, connection)

    @api.constrains('dependency_ids')
    def _check_dependencies(self):
        for table in self:
            if table in table.dependency_ids:
                raise ValidationError(_("A table cannot depend on itself!"))

    @api.constrains('full_sync_enabled', 'incremental_sync_enabled')
    def _check_sync_types(self):
        for table in self:
            if not table.full_sync_enabled and not table.incremental_sync_enabled:
                raise ValidationError(_("At least one sync type must be enabled!"))

    @api.constrains('incremental_sync_enabled', 'incremental_date_column')
    def _check_incremental_config(self):
        for table in self:
            if table.incremental_sync_enabled and not table.incremental_date_column:
                raise ValidationError(_("Date Column is required when Incremental Sync is enabled!"))


    # def _check_primary_key_unique_mapped(self):
    #     """Ensure primary key/unique column is mapped in column mappings - CASE SENSITIVITY FIXED"""
    #     for table in self:
    #         if not table.primary_key_unique:
    #             continue  # Skip if no primary key set yet
    #             
    #         # Check if primary key is mapped (case-sensitive for PostgreSQL)
    #         pk_mapped = False
    #         source_is_postgres = table._is_postgres_connection(table.source_connection_id)
    #         
    #         for mapping in table.mapping_ids:
    #             if mapping.source_column:
    #                 if table._compare_column_names(mapping.source_column, table.primary_key_unique, source_is_postgres):
    #                     pk_mapped = True
    #                     break
    #         
    #         if not pk_mapped:
    #             raise ValidationError(_(
    #                 "Primary Key/Unique Column '%s' must be mapped in Column Mappings.\n\n"
    #                 "Please add a mapping for this field. For example:\n"
    #                 "• Source Column: %s\n"
    #                 "• Target Column: source_id (or another appropriate target column)\n"
    #                 "• Mapping Type: Direct\n\n"
    #                 "Note: The target column is usually different from the source primary key "
    #                 "since target tables often use auto-increment IDs."
    #             ) % (table.primary_key_unique, table.primary_key_unique))

    def get_config_json(self):
        """Generate JSON configuration for ETL process - WITH PROPER QUOTING"""
        self.ensure_one()
        
        # Determine if connections are PostgreSQL
        source_is_postgres = self._is_postgres_connection(self.source_connection_id)
        target_is_postgres = self._is_postgres_connection(self.target_connection_id)
        
        # Get properly quoted table names
        source_table = self._quote_table_name(self.source_table_name, self.source_connection_id)
        target_table = self._quote_table_name(self.target_table_name, self.target_connection_id)
        
        # Get properly quoted primary key
        primary_key = self._quote_column_name(self.primary_key_unique, self.source_connection_id)
        
        # Normalize all mappings based on database types
        normalized_mappings = {}
        for mapping in self.mapping_ids:
            # Source column: preserve case for PostgreSQL, normalize for others
            if source_is_postgres:
                source_column_key = mapping.source_column
            else:
                source_column_key = mapping.source_column.lower()
            
            mapping_dict = {
                # Target column: preserve case for PostgreSQL, normalize for others
                'target': mapping.target_column if target_is_postgres else mapping.target_column.lower(),
                'type': mapping.mapping_type,
            }
            
            if mapping.mapping_type == 'lookup':
                mapping_dict.update({
                    # Lookup table/columns: preserve case for PostgreSQL, normalize for others
                    'lookup_table': mapping.lookup_table if target_is_postgres else mapping.lookup_table.lower(),
                    'lookup_key': mapping.lookup_key if target_is_postgres else mapping.lookup_key.lower(),
                    'lookup_value': mapping.lookup_value if target_is_postgres else mapping.lookup_value.lower()
                })
            
            # Store with source column as key
            normalized_mappings[source_column_key] = mapping_dict
            
        return {
            'source_connection': self.source_connection_id.get_connection_string(),
            'target_connection': self.target_connection_id.get_connection_string(),
            'source_connection_type': self.source_connection_id.database_type,
            'target_connection_type': self.target_connection_id.database_type,
            'source_table': source_table,  # Already quoted
            'target_table': target_table,  # Already quoted
            'primary_key_unique': primary_key,  # Already quoted
            'batch_size': self.batch_size,
            'mappings': normalized_mappings,
            # Add flags for case sensitivity
            'source_is_postgres': source_is_postgres,
            'target_is_postgres': target_is_postgres,
        }

    def action_test_connections(self):
        """Test both source and target database connections"""
        self.ensure_one()
        
        messages = []
        success = True
        
        # Test source connection
        try:
            result = self.source_connection_id.action_test_connection()
            if result['params']['type'] == 'success':
                messages.append(f"✓ Source ({self.source_connection_id.name}): Connected")
            else:
                messages.append(f"✗ Source ({self.source_connection_id.name}): Failed")
                success = False
        except Exception as e:
            messages.append(f"✗ Source: {str(e)}")
            success = False
        
        # Test target connection
        try:
            result = self.target_connection_id.action_test_connection()
            if result['params']['type'] == 'success':
                messages.append(f"✓ Target ({self.target_connection_id.name}): Connected")
            else:
                messages.append(f"✗ Target ({self.target_connection_id.name}): Failed")
                success = False
        except Exception as e:
            messages.append(f"✗ Target: {str(e)}")
            success = False
        
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': _('Connection Test Results'),
                'message': '\n'.join(messages),
                'type': 'success' if success else 'danger',
                'sticky': not success,
            }
        }

    def action_sync_table_full(self):
        """Trigger full sync - immediate or queued based on threshold"""
        self.ensure_one()
        
        if not self.full_sync_enabled:
            return self._show_notification('Full Sync Disabled', 
                                         'Full sync is not enabled for this table.', 'warning')
        
        # Get immediate sync threshold
        threshold = int(self.env['ir.config_parameter'].sudo().get_param(
            'etl.immediate_sync_threshold', '1000'))
        
        record_count = self.estimated_record_count or 0
        
        if record_count <= threshold and record_count > 0:
            # Sync immediately
            return self._sync_immediately('full')
        else:
            # Queue the job
            return self._queue_sync_job('full')

    def action_sync_table_incremental(self):
        """Trigger incremental sync - immediate or queued based on threshold"""
        self.ensure_one()
        
        if not self.incremental_sync_enabled:
            return self._show_notification('Incremental Sync Disabled', 
                                         'Incremental sync is not enabled for this table.', 'warning')
        
        # Get immediate sync threshold (for incremental, use smaller threshold)
        threshold = int(self.env['ir.config_parameter'].sudo().get_param(
            'etl.immediate_sync_threshold', '1000'))
        
        # For incremental, always use immediate if under threshold
        record_count = self.estimated_record_count or 0
        
        if record_count <= threshold:
            return self._sync_immediately('incremental')
        else:
            return self._queue_sync_job('incremental')

    def _sync_immediately(self, sync_type):
        """Sync immediately without queueing"""
        try:
            self.write({
                'job_status': 'started',
                'last_sync_message': f'{sync_type.title()} sync started immediately'
            })
            
            # Process sync
            processor = self.env['etl.processor']
            if sync_type == 'full':
                processor.process_table_full_sync(self)
            else:
                processor.process_table_incremental_sync(self)
            
            # Update job status to done after successful sync
            self.write({
                'job_status': 'done',
                'last_sync_status': 'success',
                'last_sync_message': f'{sync_type.title()} sync completed successfully'
            })
            
            return self._show_notification(f'{sync_type.title()} Sync Completed', 
                                         f'{sync_type.title()} sync completed immediately', 'success')
            
        except Exception as e:
            error_msg = str(e)
            self.write({
                'job_status': 'failed',
                'last_sync_status': 'failed',
                'last_sync_message': f'Immediate {sync_type} sync failed: {error_msg}'
            })
            return self._show_notification('Sync Failed', f'Immediate sync failed: {error_msg}', 'danger')

    def _queue_sync_job(self, sync_type):
        """Queue sync job using queue_job"""
        if not QUEUE_JOB_AVAILABLE:
            return self._show_notification('Queue Not Available', 
                                         'queue_job module not available. Please install it.', 'danger')
        
        try:
            # Simple queue job
            job = self.with_delay(
                description=f"ETL {sync_type.title()} Sync: {self.name}"
            ).sync_table_job(sync_type)
            
            self.write({
                'job_uuid': job.uuid,
                'job_status': 'pending',
                'last_sync_message': f'{sync_type.title()} sync queued'
            })
            
            return self._show_notification(f'{sync_type.title()} Sync Queued', 
                                         f'{sync_type.title()} sync job queued successfully', 'success')
        except Exception as e:
            return self._show_notification('Queue Failed', str(e), 'danger')

    def sync_table_job(self, sync_type):
        """Background job method for sync (queue_job compatible)"""
        try:
            self.write({
                'job_status': 'started',
                'last_sync_message': f'{sync_type.title()} sync started'
            })
            
            # Process sync
            processor = self.env['etl.processor']
            if sync_type == 'full':
                processor.process_table_full_sync(self)
            else:
                processor.process_table_incremental_sync(self)
            
            self.write({
                'job_status': 'done',
                'last_sync_status': 'success',
                'last_sync_message': f'{sync_type.title()} sync completed successfully'
            })
            
        except Exception as e:
            error_message = str(e)
            self.write({
                'job_status': 'failed',
                'last_sync_status': 'failed',
                'last_sync_message': f'{sync_type.title()} sync failed: {error_message}'
            })
            raise

    def _show_notification(self, title, message, msg_type):
        """Helper to show notifications"""
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': _(title),
                'message': message,
                'type': msg_type,
                'sticky': msg_type == 'danger',
            }
        }

    def action_estimate_record_count(self):
        """Estimate record count in source table - FIXED FOR POSTGRESQL"""
        self.ensure_one()
        
        try:
            # Use our safe method with proper PostgreSQL quoting
            count = self._get_table_record_count_safe(
                self.source_connection_id, 
                self.source_table_name
            )
            
            self.write({'estimated_record_count': count})
            
            return self._show_notification('Record Count Updated', 
                                         f'Estimated record count: {count:,}', 'success')
        except Exception as e:
            return self._show_notification('Error', str(e), 'danger')

    # def _get_table_record_count_safe(self, connection, table_name):
    #     """Get table record count with proper PostgreSQL quoting"""
    #     try:
    #         # Quote the table name properly
    #         quoted_table = self._quote_table_name(table_name, connection)
            
    #         # Build the count query with proper quoting
    #         count_query = f'SELECT COUNT(*) as record_count FROM {quoted_table}'
            
    #         # Use adapter to execute query
    #         adapter_factory = self.env['etl.database.adapter.factory']
    #         adapter = adapter_factory.create_adapter(connection)
            
    #         with adapter.create_connection() as conn:
    #             result = adapter.execute_query(conn, count_query)
    #             if result:
    #                 first_row = result[0]
    #                 if isinstance(first_row, dict):
    #                     return first_row.get('record_count', 0)
    #                 else:
    #                     return first_row[0]
    #             return 0
                    
    #     except Exception as e:
    #         _logger.error(f"Failed to get record count for {table_name}: {str(e)}")
    #         return 0
    
    
    def _get_table_record_count_safe(self, connection, table_name):
        """Get table record count - FIXED for Oracle schema.table"""
        try:
            if connection.database_type == 'oracle':
                # Oracle: Use table name exactly as configured (handles schema.table)
                quoted_table = table_name
            else:
                # Other databases: Apply proper quoting
                quoted_table = self._quote_table_name(table_name, connection)
            
            count_query = f'SELECT COUNT(*) as record_count FROM {quoted_table}'
            
            adapter_factory = self.env['etl.database.adapter.factory']
            adapter = adapter_factory.create_adapter(connection)
            
            with adapter.create_connection() as conn:
                result = adapter.execute_query(conn, count_query)
                
                if result:
                    first_row = result[0]
                    if isinstance(first_row, dict):
                        # Handle different case variations
                        count = (first_row.get('RECORD_COUNT') or 
                                first_row.get('record_count') or 
                                first_row.get('Record_Count') or 0)
                        return count
                    else:
                        return first_row[0]
                return 0
                    
        except Exception as e:
            _logger.error(f"Failed to get record count for {table_name}: {str(e)}")
            return 0


class ETLCategory(models.Model):
    _name = 'etl.category'
    _description = 'ETL Table Category'
    _order = 'sequence, name'
    
    name = fields.Char('Category Name', required=True)
    code = fields.Char('Category Code', required=True)
    sequence = fields.Integer('Sequence', default=10)
    active = fields.Boolean(default=True)
    
    _sql_constraints = [
        ('code_uniq', 'unique (code)', 'Category code must be unique!')
    ]


class ETLFrequency(models.Model):
    _name = 'etl.frequency'
    _description = 'ETL Sync Frequency'
    _order = 'sequence, name'
    
    name = fields.Char('Frequency Name', required=True)
    code = fields.Char('Frequency Code', required=True)
    interval_number = fields.Integer('Interval Number', default=1, required=True)
    interval_type = fields.Selection([
        ('minutes', 'Minutes'),
        ('hours', 'Hours'),
        ('days', 'Days'),
        ('weeks', 'Weeks'),
        ('months', 'Months')
    ], string='Interval Type', required=True)
    sequence = fields.Integer('Sequence', default=10)
    active = fields.Boolean(default=True)
    
    _sql_constraints = [
        ('code_uniq', 'unique (code)', 'Frequency code must be unique!')
    ]


class ETLColumnMapping(models.Model):
    _name = 'etl.column.mapping'
    _description = 'ETL Column Mapping'
    _order = 'sequence, id'

    sequence = fields.Integer('Sequence', default=10)
    table_id = fields.Many2one('etl.source.table', required=True, ondelete='cascade')
    source_column = fields.Char('Source Column', required=True)
    target_column = fields.Char('Target Column', required=True)
    mapping_type = fields.Selection([
        ('direct', 'Direct'),
        ('lookup', 'Lookup')
    ], required=True, default='direct')
    
    # For lookup mappings
    lookup_table = fields.Char('Lookup Table')
    lookup_key = fields.Char('Lookup Key')
    lookup_value = fields.Char('Lookup Value')
    
    active = fields.Boolean(default=True)

    @api.model
    def create(self, vals):
        """Override create to handle case normalization - CASE SENSITIVITY FIXED"""
        
        # Get table to determine database types
        table_id = vals.get('table_id')
        if table_id:
            table = self.env['etl.source.table'].browse(table_id)
            
            # Only normalize for non-PostgreSQL databases
            if not table._is_postgres_connection(table.target_connection_id):
                if vals.get('target_column'):
                    vals['target_column'] = vals['target_column'].lower()
                if vals.get('lookup_table'):
                    vals['lookup_table'] = vals['lookup_table'].lower()
                if vals.get('lookup_key'):
                    vals['lookup_key'] = vals['lookup_key'].lower()
                if vals.get('lookup_value'):
                    vals['lookup_value'] = vals['lookup_value'].lower()
            
        return super().create(vals)

    def write(self, vals):
        """Override write to handle case normalization - CASE SENSITIVITY FIXED"""
        
        # Only normalize for non-PostgreSQL databases
        if not self.table_id._is_postgres_connection(self.table_id.target_connection_id):
            if vals.get('target_column'):
                vals['target_column'] = vals['target_column'].lower()
            if vals.get('lookup_table'):
                vals['lookup_table'] = vals['lookup_table'].lower()
            if vals.get('lookup_key'):
                vals['lookup_key'] = vals['lookup_key'].lower()
            if vals.get('lookup_value'):
                vals['lookup_value'] = vals['lookup_value'].lower()
                
        return super().write(vals)

    @api.constrains('mapping_type', 'lookup_table', 'lookup_key', 'lookup_value')
    def _check_lookup_fields(self):
        for mapping in self:
            if mapping.mapping_type == 'lookup':
                if not (mapping.lookup_table and mapping.lookup_key and mapping.lookup_value):
                    raise ValidationError(_("Lookup mappings require lookup table, key, and value!"))


class ETLSyncLog(models.Model):
    _name = 'etl.sync.log'
    _description = 'ETL Synchronization Log - Simplified'
    _order = 'create_date desc'

    table_id = fields.Many2one('etl.source.table', string='Table', required=True)
    sync_type = fields.Selection([
        ('full', 'Full Sync'),
        ('incremental', 'Incremental Sync'),
    ], string='Sync Type', required=True)
    start_time = fields.Datetime('Start Time', required=True)
    end_time = fields.Datetime('End Time')
    status = fields.Selection([
        ('success', 'Success'),
        ('failed', 'Failed'),
        ('running', 'Running'),
    ], string='Status', required=True)
    total_records = fields.Integer('Total Records')
    new_records = fields.Integer('New Records')
    updated_records = fields.Integer('Updated Records')
    error_message = fields.Text('Error Message')
    
    # Simple performance metrics
    duration_seconds = fields.Float('Duration (seconds)', readonly=True)
    
    def name_get(self):
        return [(log.id, f"{log.table_id.name} - {log.sync_type} - {log.start_time}") for log in self]

    @api.model
    def create(self, vals):
        """Override create to calculate duration"""
        if vals.get('start_time') and vals.get('end_time'):
            try:
                start = fields.Datetime.to_datetime(vals['start_time'])
                end = fields.Datetime.to_datetime(vals['end_time'])
                vals['duration_seconds'] = (end - start).total_seconds()
            except Exception as e:
                _logger.warning(f"Failed to calculate duration for sync log: {str(e)}")
        
        return super().create(vals)