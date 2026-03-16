# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError, UserError
import json
import logging

_logger = logging.getLogger(__name__)

class ETLDatabaseConnection(models.Model):
    _name = 'etl.database.connection'
    _description = 'ETL Database Connection Configuration - Simplified'
    _order = 'name'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char('Connection Name', required=True, tracking=True)
    
    # Database Connection Details
    database_type = fields.Selection([
        ('postgresql', 'PostgreSQL'),
        ('mysql', 'MySQL'),
        ('mssql', 'Microsoft SQL Server'),
        ('oracle', 'Oracle'),
        ('sqlite', 'SQLite'),
    ], string='Database Type', required=True, tracking=True)
    
    host = fields.Char('Host/IP Address', required=True, tracking=True)
    port = fields.Integer('Port', required=True, tracking=True)
    database_name = fields.Char('Database Name', required=True, tracking=True)
    username = fields.Char('Username', required=True, tracking=True)
    password = fields.Char('Password', required=True, password=True)
    
    # Additional Connection Parameters
    additional_params = fields.Text('Additional Parameters', 
                                   help="JSON format additional connection parameters")
    ssl_enabled = fields.Boolean('Enable SSL/TLS', default=False)
    connection_timeout = fields.Integer('Connection Timeout (seconds)', default=30)
    
    # Connection Status
    active = fields.Boolean('Active', default=True, tracking=True)
    last_test_time = fields.Datetime('Last Connection Test', readonly=True)
    last_test_status = fields.Selection([
        ('success', 'Success'),
        ('failed', 'Failed'),
        ('never', 'Never Tested')
    ], string='Last Test Status', default='never', readonly=True)
    last_test_message = fields.Text('Last Test Message', readonly=True)

    @api.model
    def create(self, vals):
        # Set default port based on database type
        if 'port' not in vals or not vals['port']:
            db_type = vals.get('database_type')
            default_ports = {
                'postgresql': 5432,
                'mysql': 3306,
                'mssql': 1433,
                'oracle': 1521,
                'sqlite': 0,
            }
            vals['port'] = default_ports.get(db_type, 5432)
        result = super().create(vals)
        # Auto-export DB config after creation
        try:
            self.env['etl.source.table'].export_db_config_json()
        except Exception as e:
            _logger.warning(f"Failed to auto-export DB config after connection create: {str(e)}")
        return result
    
    def write(self, vals):
        """Override write to auto-export configs"""
        result = super().write(vals)
        # Auto-export DB config after update
        if vals.get('active', True):  # Only export if still active
            try:
                self.env['etl.source.table'].export_db_config_json()
            except Exception as e:
                _logger.warning(f"Failed to auto-export DB config after connection update: {str(e)}")
        return result

    def get_connection_string(self):
        """Generate connection string based on database type"""
        self.ensure_one()
        
        if self.database_type == 'postgresql':
            conn_str = f"dbname={self.database_name} user={self.username} password={self.password} host={self.host} port={self.port}"
            if self.ssl_enabled:
                conn_str += " sslmode=require"
                
        elif self.database_type == 'mysql':
            conn_str = f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}"
            if self.ssl_enabled:
                conn_str += "?ssl=true"
                
        elif self.database_type == 'mssql':
            driver = "ODBC Driver 18 for SQL Server"
            conn_str = f"Driver={{{driver}}};Server={self.host},{self.port};Database={self.database_name};UID={self.username};PWD={self.password}"
            if self.ssl_enabled:
                conn_str += ";TrustServerCertificate=yes;Encrypt=yes"
            else:
                conn_str += ";TrustServerCertificate=yes"
                
        elif self.database_type == 'oracle':
            conn_str = f"oracle://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}"
            
        elif self.database_type == 'sqlite':
            conn_str = f"sqlite:///{self.database_name}"
            
        else:
            raise UserError(_("Unsupported database type: %s") % self.database_type)
        
        # Add additional parameters if specified
        if self.additional_params:
            try:
                params = json.loads(self.additional_params)
                if self.database_type == 'postgresql':
                    for key, value in params.items():
                        conn_str += f" {key}={value}"
                elif self.database_type in ['mysql', 'oracle']:
                    param_str = "&".join([f"{k}={v}" for k, v in params.items()])
                    separator = "&" if "?" in conn_str else "?"
                    conn_str += f"{separator}{param_str}"
                elif self.database_type == 'mssql':
                    for key, value in params.items():
                        conn_str += f";{key}={value}"
            except json.JSONDecodeError:
                _logger.warning(f"Invalid additional parameters JSON for connection {self.name}")
        
        return conn_str

    def action_test_connection(self):
        """Test the database connection"""
        self.ensure_one()
        
        try:
            # Test connection using adapter
            adapter_factory = self.env['etl.database.adapter.factory']
            adapter = adapter_factory.create_adapter(self)
            result = adapter.test_connection()
            
            self.write({
                'last_test_time': fields.Datetime.now(),
                'last_test_status': 'success' if result['success'] else 'failed',
                'last_test_message': result['message']
            })
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Connection Test'),
                    'message': result['message'],
                    'type': 'success' if result['success'] else 'danger',
                    'sticky': not result['success'],
                }
            }
            
        except Exception as e:
            error_msg = str(e)
            self.write({
                'last_test_time': fields.Datetime.now(),
                'last_test_status': 'failed',
                'last_test_message': error_msg
            })
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Connection Test Failed'),
                    'message': error_msg,
                    'type': 'danger',
                    'sticky': True,
                }
            }