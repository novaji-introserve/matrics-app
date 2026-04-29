# -*- coding: utf-8 -*-
from odoo import models, api
from odoo.exceptions import UserError
import logging
from abc import ABC, abstractmethod
from decimal import Decimal
from datetime import datetime
import json

# Database-specific imports
try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

try:
    import pyodbc
except ImportError:
    pyodbc = None

try:
    import pymysql
except ImportError:
    pymysql = None

# try:
#     import cx_Oracle
# except ImportError:
#     cx_Oracle = None

try:
    import oracledb
except ImportError:
    oracledb = None

try:
    import sqlite3
except ImportError:
    sqlite3 = None

_logger = logging.getLogger(__name__)

class AbstractDatabaseAdapter(ABC):
    """Abstract base class for database adapters"""
    
    def __init__(self, connection_config):
        self.connection_config = connection_config
        self.connection_string = connection_config.get_connection_string()
        self.database_type = connection_config.database_type
        
    @abstractmethod
    def create_connection(self):
        """Create a database connection"""
        pass
    
    @abstractmethod
    def test_connection(self):
        """Test database connection and return result"""
        pass
    
    @abstractmethod
    def execute_query(self, connection, query, params=None):
        """Execute a query and return results"""
        pass
    
    @abstractmethod
    def get_table_columns(self, connection, table_name):
        """Get column information for a table"""
        pass
    
    @abstractmethod
    def get_record_count(self, connection, table_name):
        """Get total record count for a table"""
        pass
    
    def transform_value(self, value, target_type=None):
        """Transform value for database compatibility"""
        if value is None:
            return None
        
        if isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, (dict, list)):
            return json.dumps(value)
        
        return value
    
    def close_connection(self, connection):
        """Close database connection"""
        if connection:
            try:
                connection.close()
            except Exception as e:
                _logger.warning(f"Error closing connection: {str(e)}")


class PostgreSQLAdapter(AbstractDatabaseAdapter):
    """PostgreSQL database adapter"""
    
    def __init__(self, connection_config):
        super().__init__(connection_config)
        if psycopg2 is None:
            raise UserError("psycopg2 library is required for PostgreSQL connections")
    
    def create_connection(self):
        """Create PostgreSQL connection"""
        try:
            conn = psycopg2.connect(
                self.connection_string,
                connect_timeout=self.connection_config.connection_timeout
            )
            conn.autocommit = False
            return conn
        except psycopg2.Error as e:
            raise UserError(f"PostgreSQL connection failed: {str(e)}")
    
    def test_connection(self):
        """Test PostgreSQL connection"""
        try:
            conn = self.create_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return {
                'success': True,
                'message': f'PostgreSQL connection successful. Version: {version[:50]}...'
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'PostgreSQL connection failed: {str(e)}'
            }
    
    def execute_query(self, connection, query, params=None):
        """Execute PostgreSQL query"""
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(query, params)
            if query.strip().upper().startswith('SELECT'):
                return cursor.fetchall()
            return cursor.rowcount
        except psycopg2.Error as e:
            connection.rollback()
            raise UserError(f"PostgreSQL query error: {str(e)}")
        finally:
            cursor.close()
    
    def get_table_columns(self, connection, table_name):
        """Get PostgreSQL table column information"""
        query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
        """
        return self.execute_query(connection, query, (table_name,))
    
    def get_record_count(self, connection, table_name):
        """Get PostgreSQL table record count"""
        query = f'SELECT COUNT(*) as record_count FROM "{table_name}"'
        result = self.execute_query(connection, query)
        if result:
            first_row = result[0]
            if isinstance(first_row, dict):
                return first_row.get('record_count', 0)
            else:
                return first_row[0]
        return 0


class MySQLAdapter(AbstractDatabaseAdapter):
    """MySQL database adapter"""
    
    def __init__(self, connection_config):
        super().__init__(connection_config)
        if pymysql is None:
            raise UserError("pymysql library is required for MySQL connections")
    
    def create_connection(self):
        """Create MySQL connection"""
        try:
            import urllib.parse as urlparse
            parsed = urlparse.urlparse(self.connection_string)
            
            conn = pymysql.connect(
                host=parsed.hostname,
                port=parsed.port or 3306,
                user=parsed.username,
                password=parsed.password,
                database=parsed.path.lstrip('/'),
                connect_timeout=self.connection_config.connection_timeout,
                autocommit=False
            )
            return conn
        except pymysql.Error as e:
            raise UserError(f"MySQL connection failed: {str(e)}")
    
    def test_connection(self):
        """Test MySQL connection"""
        try:
            conn = self.create_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return {
                'success': True,
                'message': f'MySQL connection successful. Version: {version}'
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'MySQL connection failed: {str(e)}'
            }
    
    def execute_query(self, connection, query, params=None):
        """Execute MySQL query"""
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        try:
            cursor.execute(query, params)
            if query.strip().upper().startswith('SELECT'):
                return cursor.fetchall()
            return cursor.rowcount
        except pymysql.Error as e:
            connection.rollback()
            raise UserError(f"MySQL query error: {str(e)}")
        finally:
            cursor.close()
    
    def get_table_columns(self, connection, table_name):
        """Get MySQL table column information"""
        query = """
            SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type, 
                   IS_NULLABLE as is_nullable, COLUMN_DEFAULT as column_default
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        return self.execute_query(connection, query, (table_name,))
    
    def get_record_count(self, connection, table_name):
        """Get MySQL table record count"""
        query = f"SELECT COUNT(*) as count FROM `{table_name}`"
        result = self.execute_query(connection, query)
        return result[0]['count'] if result else 0


class MSSQLAdapter(AbstractDatabaseAdapter):
    """Microsoft SQL Server database adapter"""
    
    def __init__(self, connection_config):
        super().__init__(connection_config)
        if pyodbc is None:
            raise UserError("pyodbc library is required for MSSQL connections")
    
    def create_connection(self):
        """Create MSSQL connection"""
        try:
            conn = pyodbc.connect(
                self.connection_string,
                timeout=self.connection_config.connection_timeout
            )
            conn.autocommit = False
            return conn
        except pyodbc.Error as e:
            raise UserError(f"MSSQL connection failed: {str(e)}")
    
    def test_connection(self):
        """Test MSSQL connection"""
        try:
            conn = self.create_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return {
                'success': True,
                'message': f'MSSQL connection successful. Version: {version[:50]}...'
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'MSSQL connection failed: {str(e)}'
            }
    
    def execute_query(self, connection, query, params=None):
        """Execute MSSQL query"""
        cursor = connection.cursor()
        try:
            cursor.execute(query, params or [])
            if query.strip().upper().startswith('SELECT'):
                columns = [column[0] for column in cursor.description]
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                return results
            return cursor.rowcount
        except pyodbc.Error as e:
            connection.rollback()
            raise UserError(f"MSSQL query error: {str(e)}")
        finally:
            cursor.close()
    
    def get_table_columns(self, connection, table_name):
        """Get MSSQL table column information"""
        query = """
            SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type, 
                   IS_NULLABLE as is_nullable, COLUMN_DEFAULT as column_default
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """
        return self.execute_query(connection, query, (table_name,))
    
    def get_record_count(self, connection, table_name):
        """Get MSSQL table record count"""
        query = f"SELECT COUNT(*) as record_count FROM [{table_name}]"
        result = self.execute_query(connection, query)
        if result:
            first_row = result[0]
            if isinstance(first_row, dict):
                return first_row.get('record_count', 0)
            else:
                return first_row[0]
        return 0


class OracleAdapter(AbstractDatabaseAdapter):
    """Oracle database adapter - Updated with proper schema.table handling"""
    
    def __init__(self, connection_config):
        super().__init__(connection_config)
        if oracledb is None:
            raise UserError("oracledb library is required for Oracle connections")
    
    def create_connection(self):
        """Create Oracle connection using oracledb"""
        try:
            dsn = f"{self.connection_config.host}:{self.connection_config.port}/{self.connection_config.database_name}"
            conn = oracledb.connect(
                user=self.connection_config.username,
                password=self.connection_config.password,
                dsn=dsn
            )
            conn.autocommit = False
            return conn
        except oracledb.Error as e:
            raise UserError(f"Oracle connection failed: {str(e)}")
    
    def test_connection(self):
        """Test Oracle connection"""
        try:
            conn = self.create_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM v$version WHERE rownum = 1")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return {
                'success': True,
                'message': f'Oracle connection successful. Version: {version[:50]}...'
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'Oracle connection failed: {str(e)}'
            }
    def execute_query(self, connection, query, params=None):
        """Execute Oracle query - FINAL FIX for dictionary keys"""
        cursor = connection.cursor()
        try:
            cursor.execute(query, params or [])
            if query.strip().upper().startswith('SELECT'):
                # Get column names from cursor description (these are UPPERCASE)
                columns = [col[0] for col in cursor.description]
                results = []
                for row in cursor.fetchall():
                    # CRITICAL FIX: Convert UPPERCASE keys to lowercase
                    # Oracle returns: {'COLUMN_NAME': 'COMP_CODE'}
                    # ETL expects: {'column_name': 'COMP_CODE'}
                    row_dict = {}
                    for i, col_name in enumerate(columns):
                        lowercase_key = col_name.lower()  # Convert COLUMN_NAME -> column_name
                        row_dict[lowercase_key] = row[i]
                    results.append(row_dict)
                return results
            return cursor.rowcount
        except oracledb.Error as e:
            connection.rollback()
            raise UserError(f"Oracle query error: {str(e)}")
        finally:
            cursor.close()   
 
    def get_table_columns(self, connection, table_name):
        """Get Oracle table column information - FIXED for schema.table"""
        # Handle schema.table format
        if '.' in table_name:
            schema, table = table_name.split('.', 1)
            query = """
                SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type, 
                       NULLABLE as is_nullable, DATA_DEFAULT as column_default
                FROM ALL_TAB_COLUMNS 
                WHERE UPPER(OWNER) = UPPER(:schema) AND UPPER(TABLE_NAME) = UPPER(:table_name)
                ORDER BY COLUMN_ID
            """
            return self.execute_query(connection, query, {'schema': schema, 'table_name': table})
        else:
            # No schema specified, use user's tables
            query = """
                SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type, 
                       NULLABLE as is_nullable, DATA_DEFAULT as column_default
                FROM USER_TAB_COLUMNS 
                WHERE UPPER(TABLE_NAME) = UPPER(:table_name)
                ORDER BY COLUMN_ID
            """
            return self.execute_query(connection, query, {'table_name': table_name})
    
    def get_record_count(self, connection, table_name):
        """Get Oracle table record count - FIXED for schema.table"""
        # Use table name exactly as provided (handles schema.table automatically)
        query = f"SELECT COUNT(*) as record_count FROM {table_name}"
        result = self.execute_query(connection, query)
        if result:
            return result[0].get('RECORD_COUNT', 0)
        return 0


class SQLiteAdapter(AbstractDatabaseAdapter):
    """SQLite database adapter"""
    
    def __init__(self, connection_config):
        super().__init__(connection_config)
        if sqlite3 is None:
            raise UserError("sqlite3 library is required for SQLite connections")
    
    def create_connection(self):
        """Create SQLite connection"""
        try:
            conn = sqlite3.connect(
                self.connection_config.database_name,
                timeout=self.connection_config.connection_timeout
            )
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.Error as e:
            raise UserError(f"SQLite connection failed: {str(e)}")
    
    def test_connection(self):
        """Test SQLite connection"""
        try:
            conn = self.create_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT sqlite_version()")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return {
                'success': True,
                'message': f'SQLite connection successful. Version: {version}'
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'SQLite connection failed: {str(e)}'
            }
    
    def execute_query(self, connection, query, params=None):
        """Execute SQLite query"""
        cursor = connection.cursor()
        try:
            cursor.execute(query, params or [])
            if query.strip().upper().startswith('SELECT'):
                return [dict(row) for row in cursor.fetchall()]
            return cursor.rowcount
        except sqlite3.Error as e:
            connection.rollback()
            raise UserError(f"SQLite query error: {str(e)}")
        finally:
            cursor.close()
    
    def get_table_columns(self, connection, table_name):
        """Get SQLite table column information"""
        cursor = connection.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = []
        for row in cursor.fetchall():
            columns.append({
                'column_name': row['name'],
                'data_type': row['type'],
                'is_nullable': 'YES' if not row['notnull'] else 'NO',
                'column_default': row['dflt_value']
            })
        cursor.close()
        return columns
    
    def get_record_count(self, connection, table_name):
        """Get SQLite table record count"""
        query = f"SELECT COUNT(*) as record_count FROM {table_name}"
        result = self.execute_query(connection, query)
        return result[0]['record_count'] if result else 0

class ETLDatabaseAdapterFactory(models.AbstractModel):
    _name = 'etl.database.adapter.factory'
    _description = 'Factory for creating database adapters'

    @api.model
    def create_adapter(self, connection_config):
        """Create appropriate database adapter based on connection type"""
        
        adapter_map = {
            'postgresql': PostgreSQLAdapter,
            'mysql': MySQLAdapter,
            'mssql': MSSQLAdapter,
            'oracle': OracleAdapter,
            'sqlite': SQLiteAdapter,
        }
        
        adapter_class = adapter_map.get(connection_config.database_type)
        if not adapter_class:
            raise UserError(f"Unsupported database type: {connection_config.database_type}")
        
        return adapter_class(connection_config)

    @api.model
    def get_supported_databases(self):
        """Get list of supported database types with availability"""
        databases = []
        
        databases.append({
            'type': 'postgresql',
            'name': 'PostgreSQL',
            'available': psycopg2 is not None,
            'library': 'psycopg2'
        })
        
        databases.append({
            'type': 'mysql',
            'name': 'MySQL',
            'available': pymysql is not None,
            'library': 'pymysql'
        })
        
        databases.append({
            'type': 'mssql',
            'name': 'Microsoft SQL Server',
            'available': pyodbc is not None,
            'library': 'pyodbc'
        })
        
        databases.append({
            'type': 'oracle',
            'name': 'Oracle Database',
            'available': oracledb is not None,
            'library': 'oracledb'
        })
        
        databases.append({
            'type': 'sqlite',
            'name': 'SQLite',
            'available': sqlite3 is not None,
            'library': 'sqlite3'
        })
        
        return databases