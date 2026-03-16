# -*- coding: utf-8 -*-
"""
Standalone Database Adapters - No Odoo Dependencies
Extracted from ETL module for standalone execution
"""
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

try:
    import oracledb
except ImportError:
    oracledb = None

try:
    import sqlite3
except ImportError:
    sqlite3 = None

_logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom exception for database errors"""
    pass


class ConnectionConfig:
    """Simple connection config class (replaces Odoo model)"""
    def __init__(self, config_dict):
        self.database_type = config_dict['database_type']
        self.host = config_dict['host']
        self.port = config_dict['port']
        self.database_name = config_dict['database_name']
        self.username = config_dict['username']
        self.password = config_dict['password']
        self.connection_timeout = config_dict.get('connection_timeout', 30)
        self.ssl_enabled = config_dict.get('ssl_enabled', False)
        self.additional_params = config_dict.get('additional_params', '{}')
    
    def get_connection_string(self):
        """Generate connection string based on database type"""
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
            raise DatabaseError(f"Unsupported database type: {self.database_type}")
        
        return conn_str


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
            raise DatabaseError("psycopg2 library is required for PostgreSQL connections")
    
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
            raise DatabaseError(f"PostgreSQL connection failed: {str(e)}")
    
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
            raise DatabaseError(f"PostgreSQL query error: {str(e)}")
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
            raise DatabaseError("pymysql library is required for MySQL connections")
    
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
            raise DatabaseError(f"MySQL connection failed: {str(e)}")
    
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
            raise DatabaseError(f"MySQL query error: {str(e)}")
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
            raise DatabaseError("pyodbc library is required for MSSQL connections")
    
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
            raise DatabaseError(f"MSSQL connection failed: {str(e)}")
    
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
            raise DatabaseError(f"MSSQL query error: {str(e)}")
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
    """Oracle database adapter"""
    
    def __init__(self, connection_config):
        super().__init__(connection_config)
        if oracledb is None:
            raise DatabaseError("oracledb library is required for Oracle connections")
    
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
            raise DatabaseError(f"Oracle connection failed: {str(e)}")
    
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
        """Execute Oracle query"""
        cursor = connection.cursor()
        try:
            cursor.execute(query, params or [])
            if query.strip().upper().startswith('SELECT'):
                columns = [col[0] for col in cursor.description]
                results = []
                for row in cursor.fetchall():
                    row_dict = {}
                    for i, col_name in enumerate(columns):
                        lowercase_key = col_name.lower()
                        row_dict[lowercase_key] = row[i]
                    results.append(row_dict)
                return results
            return cursor.rowcount
        except oracledb.Error as e:
            connection.rollback()
            raise DatabaseError(f"Oracle query error: {str(e)}")
        finally:
            cursor.close()
    
    def get_table_columns(self, connection, table_name):
        """Get Oracle table column information"""
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
            query = """
                SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type, 
                       NULLABLE as is_nullable, DATA_DEFAULT as column_default
                FROM USER_TAB_COLUMNS 
                WHERE UPPER(TABLE_NAME) = UPPER(:table_name)
                ORDER BY COLUMN_ID
            """
            return self.execute_query(connection, query, {'table_name': table_name})
    
    def get_record_count(self, connection, table_name):
        """Get Oracle table record count"""
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
            raise DatabaseError("sqlite3 library is required for SQLite connections")
    
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
            raise DatabaseError(f"SQLite connection failed: {str(e)}")
    
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
            raise DatabaseError(f"SQLite query error: {str(e)}")
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


def create_adapter(connection_config_dict):
    """Factory function to create appropriate database adapter"""
    connection_config = ConnectionConfig(connection_config_dict)
    
    adapter_map = {
        'postgresql': PostgreSQLAdapter,
        'mysql': MySQLAdapter,
        'mssql': MSSQLAdapter,
        'oracle': OracleAdapter,
        'sqlite': SQLiteAdapter,
    }
    
    adapter_class = adapter_map.get(connection_config.database_type)
    if not adapter_class:
        raise DatabaseError(f"Unsupported database type: {connection_config.database_type}")
    
    return adapter_class(connection_config)

