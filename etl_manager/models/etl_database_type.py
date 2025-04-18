# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import json
import logging

_logger = logging.getLogger(__name__)

class ETLDatabaseType(models.Model):
    _name = 'etl.database.type'
    _description = 'ETL Database Type'
    _order = 'name'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    name = fields.Char('Database Type', required=True)
    code = fields.Char('Type Code', required=True)
    driver_module = fields.Char('Python Module', required=True, 
                               help="Python module required for connection (e.g., 'pyodbc', 'psycopg2')")
    connection_template = fields.Text('Connection String Template', required=True,
                                      help="Connection string template with placeholders")
    query_format = fields.Text('Query Format', required=True, 
                              help="Format for queries with placeholders")
    requires_driver = fields.Boolean('Requires Driver', default=False,
                                   help="Check if this database type requires specifying a driver (e.g., ODBC)")
    connection_method = fields.Selection([
        ('string', 'Connection String'),
        ('params', 'Connection Parameters'),
        ('uri', 'Connection URI'),
        ('dsn', 'Data Source Name')
    ], string='Connection Method', required=True, default='string',
       help="Method used to establish connection")
    active = fields.Boolean(default=True)
    notes = fields.Text('Notes', help="Additional information about this database type")
    
    _sql_constraints = [
        ('code_uniq', 'unique (code)', 'Database type code must be unique!')
    ]
    
    def get_connection_class(self):
        """Return the appropriate connection class for this database type"""
        # This will be implemented by a factory method in ETLConnectionFactory
        return None

class ETLDatabaseConnection(models.Model):
    _name = 'etl.database.connection'
    _description = 'ETL Database Connection'
    _order = 'name'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    name = fields.Char('Connection Name', required=True)
    db_type_id = fields.Many2one('etl.database.type', string='Database Type', required=True, 
                                ondelete='restrict', tracking=True)
    db_type_code = fields.Char(related='db_type_id.code', string='Database Type Code', 
                              readonly=True, store=True)
    server = fields.Char('Server/Host', required=True, tracking=True)
    port = fields.Char('Port', tracking=True)
    database = fields.Char('Database Name', required=True, tracking=True)
    username = fields.Char('Username', required=True, tracking=True)
    password = fields.Char('Password', required=True, tracking=True)
    driver_name = fields.Char('Driver Name', 
                           help="For ODBC connections, the driver name (e.g., 'ODBC Driver 18 for SQL Server')")
    connection_options = fields.Text('Additional Connection Options', 
                                   help="JSON formatted additional connection parameters")
    active = fields.Boolean(default=True)
    notes = fields.Text('Notes')
    
    requires_driver = fields.Boolean(related='db_type_id.requires_driver', readonly=True)
    connection_method = fields.Selection(related='db_type_id.connection_method', readonly=True)
    
    is_default_source = fields.Boolean(string="Default Source Connection", default=False)
    is_default_target = fields.Boolean(string="Default Target Connection", default=False)
    
    def get_connection_string(self):
        """Generate connection string based on database type and parameters"""
        self.ensure_one()
        
        # Special handling for MSSQL with ODBC driver
        if self.db_type_code == 'mssql' and self.driver_name:
            # Manually construct the connection string to avoid template formatting issues
            conn_parts = []
            conn_parts.append(f"DRIVER={{{self.driver_name}}}")  # Note the double braces become single in string format
            conn_parts.append(f"SERVER={self.server}")
            if self.port:
                conn_parts.append(f"PORT={self.port}")
            conn_parts.append(f"DATABASE={self.database}")
            conn_parts.append(f"UID={self.username}")
            conn_parts.append(f"PWD={self.password}")
            conn_parts.append("TrustServerCertificate=yes")
            
            # Add any additional parameters from connection_options
            if self.connection_options:
                try:
                    additional_params = json.loads(self.connection_options)
                    for key, value in additional_params.items():
                        conn_parts.append(f"{key}={value}")
                except json.JSONDecodeError:
                    _logger.warning("Invalid JSON in connection options: %s", self.connection_options)
            
            # Join with semicolons
            return ";".join(conn_parts)
            
        elif self.db_type_code == 'postgresql':
            # PostgreSQL typically uses space-separated key=value format
            conn_parts = []
            conn_parts.append(f"dbname={self.database}")
            conn_parts.append(f"user={self.username}")
            conn_parts.append(f"password={self.password}")
            conn_parts.append(f"host={self.server}")
            if self.port:
                conn_parts.append(f"port={self.port}")
                
            # Add any additional parameters
            if self.connection_options:
                try:
                    additional_params = json.loads(self.connection_options)
                    for key, value in additional_params.items():
                        conn_parts.append(f"{key}={value}")
                except json.JSONDecodeError:
                    _logger.warning("Invalid JSON in connection options: %s", self.connection_options)
                    
            # Join with spaces
            return " ".join(conn_parts)
            
        elif self.db_type_code == 'mysql':
            # MySQL typical format
            conn_parts = []
            conn_parts.append(f"host={self.server}")
            if self.port:
                conn_parts.append(f"port={self.port}")
            conn_parts.append(f"user={self.username}")
            conn_parts.append(f"password={self.password}")
            conn_parts.append(f"database={self.database}")
            
            # Add any additional parameters
            if self.connection_options:
                try:
                    additional_params = json.loads(self.connection_options)
                    for key, value in additional_params.items():
                        conn_parts.append(f"{key}={value}")
                except json.JSONDecodeError:
                    _logger.warning("Invalid JSON in connection options: %s", self.connection_options)
                    
            # MySQL connector can use either comma or semicolon separator
            return ",".join(conn_parts)
            
        elif self.db_type_code == 'oracle':
            # Oracle connection string format (traditional)
            return f"{self.username}/{self.password}@{self.server}:{self.port or '1521'}/{self.database}"
            
        elif self.db_type_code == 'sqlite':
            # SQLite simple connection to file
            return self.database
            
        else:
            # For other database types, use template approach
            try:
                params = {
                    'server': self.server,
                    'port': self.port or '',
                    'database': self.database,
                    'username': self.username,
                    'password': self.password,
                }
                
                # Add additional connection options
                if self.connection_options:
                    try:
                        additional_params = json.loads(self.connection_options)
                        params.update(additional_params)
                    except json.JSONDecodeError:
                        _logger.warning("Invalid JSON in connection options: %s", self.connection_options)
                
                return self.db_type_id.connection_template.format(**params)
            except KeyError as e:
                _logger.error("Missing parameter in connection string template: %s", str(e))
                raise ValidationError(_("Missing parameter in connection string template: %s") % str(e))
    
    def action_test_connection(self):
        """Test the database connection"""
        self.ensure_one()
        
        try:
            # Get the appropriate connector
            connector = self.env['etl.connector.factory'].get_connector(self)
            
            # For MSSQL, first check driver availability
            if self.db_type_code == 'mssql' and self.requires_driver:
                import pyodbc
                drivers = pyodbc.drivers()
                if not drivers:
                    return {
                        'type': 'ir.actions.client',
                        'tag': 'display_notification',
                        'params': {
                            'title': _('Error'),
                            'message': _('No ODBC drivers found on this system. Please install the required ODBC driver.'),
                            'type': 'danger',
                            'sticky': True,
                        }
                    }
                
                if self.driver_name not in drivers:
                    drivers_list = "\n• " + "\n• ".join(drivers)
                    return {
                        'type': 'ir.actions.client',
                        'tag': 'display_notification',
                        'params': {
                            'title': _('Driver Not Found'),
                            'message': _(
                                "The specified driver '%s' is not installed on this system.\n\n"
                                "Available drivers: %s\n\n"
                                "Please choose one of the available drivers or install the required one."
                            ) % (self.driver_name, drivers_list),
                            'type': 'danger',
                            'sticky': True,
                        }
                    }
            
            # Test the connection - FIXED: passing self as the connection_config parameter
            connector.test_connection(self)
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Success'),
                    'message': _('Successfully connected to the database!'),
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
            
    @api.constrains('is_default_source', 'is_default_target')
    def _check_default_connections(self):
        if self.is_default_source:
            default_sources = self.search([
                ('id', '!=', self.id),
                ('is_default_source', '=', True)
            ])
            if default_sources:
                for source in default_sources:
                    source.is_default_source = False
                
        if self.is_default_target:
            default_targets = self.search([
                ('id', '!=', self.id),
                ('is_default_target', '=', True)
            ])
            if default_targets:
                for target in default_targets:
                    target.is_default_target = False
            