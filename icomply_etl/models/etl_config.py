# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import json
import logging

_logger = logging.getLogger(__name__)

class ETLSourceTable(models.Model):
    _name = 'etl.source.table'
    _description = 'ETL Source Table Configuration'
    _order = 'sequence, name'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char('Table Name', required=True, help="Source table name (e.g., tbl_customer)")
    sequence = fields.Integer('Sequence', default=10)
    target_table = fields.Char('Target Table', required=True, help="Target table name (e.g., res_partner)")
    primary_key = fields.Char('Primary Key', required=True)
    batch_size = fields.Integer('Batch Size', default=2000)
    is_base_table = fields.Boolean('Is Base Table', help="Tables with no dependencies")
    active = fields.Boolean(default=True)

    category_id = fields.Many2one('etl.category', string='Category', required=True)
    frequency_id = fields.Many2one('etl.frequency', string='Frequency', required=True)
    
    # category = fields.Selection([
    #     ('master', 'Master Data'),
    #     ('customer', 'Customer Data'),
    #     ('account', 'Account Data'),
    #     ('transaction', 'Transaction Data')
    # ], required=True, default='master')
    
    # frequency = fields.Selection([
    #     ('hourly', 'Hourly'),
    #     ('daily', 'Daily'),
    #     ('weekly', 'Weekly')
    # ], required=True, default='daily')
    
    dependency_ids = fields.Many2many(
        'etl.source.table', 
        'etl_table_dependencies', 
        'table_id', 
        'dependency_id', 
        string='Dependencies'
    )
    
    mapping_ids = fields.One2many('etl.column.mapping', 'table_id', string='Column Mappings')
    sync_log_ids = fields.One2many('etl.sync.log', 'table_id', string='Sync Logs')
    
    last_sync_time = fields.Datetime('Last Sync Time', readonly=True)
    last_sync_status = fields.Selection([
        ('success', 'Success'),
        ('failed', 'Failed'),
        ('running', 'Running'),
    ], string='Last Sync Status', readonly=True)
    last_sync_message = fields.Text('Last Sync Message', readonly=True)
    
    total_records_synced = fields.Integer('Total Records Synced', readonly=True)
    
    @api.constrains('dependency_ids')
    def _check_dependencies(self):
        for table in self:
            if table in table.dependency_ids:
                raise ValidationError(_("A table cannot depend on itself!"))
    
    def get_config_json(self):
        """Generate JSON configuration for ETL process"""
        self.ensure_one()
        
        # Normalize all mappings to lowercase
        normalized_mappings = {}
        for mapping in self.mapping_ids:
            mapping_dict = {
                'target': mapping.target_column.lower(),
                'type': mapping.mapping_type,
            }
            
            if mapping.mapping_type == 'lookup':
                mapping_dict.update({
                    'lookup_table': mapping.lookup_table.lower(),
                    'lookup_key': mapping.lookup_key.lower(),
                    'lookup_value': mapping.lookup_value.lower()
                })
            
            # Store with lowercase source column as key
            normalized_mappings[mapping.source_column.lower()] = mapping_dict
            
        return {
            'source_table': self.name.lower(),
            'target_table': self.target_table.lower(),
            'primary_key': self.primary_key.lower(),
            'batch_size': self.batch_size,
            'dependencies': [dep.name.lower() for dep in self.dependency_ids],
            'mappings': normalized_mappings
        }
    
    def action_test_connection(self):
        """Test database connections"""
        self.ensure_one()
        try:
            etl_manager = self.env['etl.manager']
            with etl_manager.get_connections() as (mssql_conn, pg_conn):
                mssql_cursor = mssql_conn.cursor()
                pg_cursor = pg_conn.cursor()
                
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': _('Success'),
                        'message': _('Successfully connected to both databases!'),
                        'type': 'success',
                        'sticky': False,
                    }
                }
        except Exception as e:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Error'),
                    'message': str(e),
                    'type': 'danger',
                    'sticky': True,
                }
            }
        
    def action_sync_table(self):
        """Manual sync action"""
        self.ensure_one()
        try:
            etl_manager = self.env['etl.manager']
            etl_manager.process_table(self)
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Success'),
                    'message': _('Table synchronization started successfully'),
                    'type': 'success',
                    'sticky': False,
                }
            }
        except Exception as e:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Error'),
                    'message': str(e),
                    'type': 'danger',
                    'sticky': True,
                }
            }

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
        """Override create to handle case normalization"""
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
        """Override write to handle case normalization"""
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
    _description = 'ETL Synchronization Log'
    _order = 'create_date desc'

    table_id = fields.Many2one('etl.source.table', string='Table', required=True)
    start_time = fields.Datetime('Start Time', required=True)
    end_time = fields.Datetime('End Time')
    status = fields.Selection([
        ('success', 'Success'),
        ('failed', 'Failed'),
        ('running', 'Running'),
    ], required=True)
    total_records = fields.Integer('Total Records')
    new_records = fields.Integer('New Records')
    updated_records = fields.Integer('Updated Records')
    error_message = fields.Text('Error Message')
    row_hashes = fields.Text('Row Hashes', help="JSON string storing the row hashes for change detection")
    
    def name_get(self):
        return [(log.id, f"{log.table_id.name} - {log.start_time}") for log in self]

    @api.model
    def create(self, vals):
        """Override create to ensure row_hashes is properly formatted"""
        if 'row_hashes' in vals and isinstance(vals['row_hashes'], dict):
            vals['row_hashes'] = json.dumps(vals['row_hashes'])
        return super().create(vals)

    def write(self, vals):
        """Override write to ensure row_hashes is properly formatted"""
        if 'row_hashes' in vals and isinstance(vals['row_hashes'], dict):
            vals['row_hashes'] = json.dumps(vals['row_hashes'])
        return super().write(vals)