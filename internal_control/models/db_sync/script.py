import json
import pyodbc
import psycopg2
from datetime import datetime
from typing import Dict, List, Any, Set, Tuple, Optional
import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from contextlib import contextmanager
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
from decimal import Decimal
from threading import Lock
import backoff

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            'etl.log',
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)

@dataclass
class TableConfig:
    source_table: str
    target_table: str
    primary_key: str
    batch_size: int
    dependencies: List[str]
    mappings: Dict[str, Dict[str, Any]]

    def __post_init__(self):
        self.source_table = self.source_table.lower()
        self.target_table = self.target_table.lower()
        self.primary_key = self.primary_key.lower()
        self.dependencies = [dep.lower() for dep in self.dependencies]
        
        normalized_mappings = {}
        for source_col, mapping in self.mappings.items():
            normalized_mapping = mapping.copy()
            normalized_mapping['target'] = mapping['target'].lower()  # Normalize target to lowercase
            normalized_mappings[source_col.lower()] = normalized_mapping
            
        self.mappings = normalized_mappings

    def get_target_primary_key(self) -> str:
        for source_col, mapping in self.mappings.items():
            if source_col == self.primary_key:
                return mapping['target']
        return self.primary_key

class DatabaseError(Exception):
    """Custom exception for database-related errors"""
    pass

class LookupCache:
    """Thread-safe cache for lookup values"""
    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()

    def get_cache_key(self, table: str, key_value: str, lookup_key: str) -> str:
        return f"{table}:{lookup_key}:{key_value}"

    def get(self, table: str, key_value: str, lookup_key: str, lookup_value: str) -> Optional[Any]:
        with self._lock:
            cache_key = self.get_cache_key(table, lookup_key, key_value)
            cache_dict = self._cache.get(cache_key, {})
            return cache_dict.get(lookup_value)

    def set(self, table: str, key_value: str, lookup_key: str, lookup_value: str, value: Any):
        with self._lock:
            cache_key = self.get_cache_key(table, lookup_key, key_value)
            if cache_key not in self._cache:
                self._cache[cache_key] = {}
            self._cache[cache_key][lookup_value] = value

    def clear(self):
        with self._lock:
            self._cache.clear()

class ETLManager:
    def __init__(self, db_config: dict, etl_config: dict):
        # Add default start date for initial sync
        self.default_start_date = datetime(2000, 1, 1)

        # Get connection strings from config and format them
        self.mssql_conn_string = db_config['mssql_connection']['connection_string'].format(**db_config['mssql_connection'])
        self.pg_conn_string = db_config['postgres_connection']['connection_string'].format(**db_config['postgres_connection'])

        self.table_configs = {
            name.lower(): TableConfig(
                source_table=config['source_table'],
                target_table=config['target_table'],
                primary_key=config['primary_key'],
                batch_size=config['batch_size'],
                dependencies=config['dependencies'],
                mappings=config['mappings']
            )
            for name, config in etl_config['tables'].items()
        }

        self.lookup_cache = LookupCache()
        self.processed_tables: Set[str] = set()
        self.processed_tables_lock = Lock()

        # Initialize database
        self._initialize_database()

    def get_source_columns(self, mssql_cursor, table_name: str) -> List[str]:
        """Get all column names from source table and normalize them to lowercase."""
        mssql_cursor.execute(f"SELECT TOP 0 * FROM {table_name}")
        return [column[0].lower() for column in mssql_cursor.description]

    def _initialize_database(self):
        """Initialize database tables and indexes"""
        self.create_sync_table()
        self.create_log_table()
        self.create_indexes()

    @backoff.on_exception(
        backoff.expo,
        (pyodbc.Error, psycopg2.Error),
        max_tries=3,
        max_time=30
    )
    @contextmanager
    def get_connections(self):
        """Context manager for database connections with retry logic"""
        mssql_conn = None
        pg_conn = None
        try:
            mssql_conn = pyodbc.connect(self.mssql_conn_string, timeout=100)
            pg_conn = psycopg2.connect(self.pg_conn_string)
            yield mssql_conn, pg_conn
        except Exception as e:
            logging.error(f"Connection error: {str(e)}")
            raise DatabaseError(f"Failed to establish database connection: {str(e)}")
        finally:
            if mssql_conn:
                mssql_conn.close()
            if pg_conn:
                pg_conn.close()

    def create_sync_table(self):
        """Create the sync_timestamps table if it does not exist."""
        with self.get_connections() as (_, pg_conn):
            with pg_conn.cursor() as pg_cursor:
                create_table_query = """
                CREATE TABLE IF NOT EXISTS sync_timestamps (
                    table_name VARCHAR(255) PRIMARY KEY,
                    sync_time TIMESTAMP NOT NULL DEFAULT NOW(),
                    row_hashes JSONB NOT NULL DEFAULT '{}'::jsonb
                );
                """
                pg_cursor.execute(create_table_query)
                pg_conn.commit()

    def create_log_table(self):
        """Create the sync_log table if it does not exist."""
        with self.get_connections() as (_, pg_conn):
            with pg_conn.cursor() as pg_cursor:
                create_log_table_query = """
                CREATE TABLE IF NOT EXISTS sync_log (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(255) NOT NULL,
                    total_rows INT NOT NULL,
                    new_rows INT NOT NULL,
                    updated_rows INT NOT NULL,
                    deleted_rows INT NOT NULL,
                    sync_time TIMESTAMP NOT NULL DEFAULT NOW(),
                    error_message TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_sync_log_table_name 
                ON sync_log(table_name);
                """
                pg_cursor.execute(create_log_table_query)
                pg_conn.commit()

    def create_indexes(self):
        """Create necessary indexes on target tables"""
        with self.get_connections() as (_, pg_conn):
            with pg_conn.cursor() as pg_cursor:
                for config in self.table_configs.values():
                    target_pk = config.get_target_primary_key()
                    create_index_query = f"""
                    CREATE INDEX IF NOT EXISTS idx_{config.target_table}_{target_pk}
                    ON {config.target_table}({target_pk});
                    """
                    pg_cursor.execute(create_index_query)
                pg_conn.commit()

    def calculate_row_hash(self, row: Dict[str, Any]) -> str:
        """Calculate a hash for a row based on its values, using SHA-256."""
        processed_row = {}
        for key, value in row.items():
            if value is None:
                processed_row[key] = 'NULL'
            elif isinstance(value, datetime):
                processed_row[key] = value.isoformat()
            elif isinstance(value, Decimal):
                processed_row[key] = str(value)
            else:
                processed_row[key] = value
        row_str = json.dumps(processed_row, sort_keys=True)
        return hashlib.sha256(row_str.encode()).hexdigest()

    def get_last_sync_info(self, pg_cursor, table_name: str) -> Tuple[datetime, Dict[str, str]]:
        """Get the last sync time and row hashes from the sync table."""
        try:
            logging.debug(f"Fetching last sync info for {table_name}")
            pg_cursor.execute(
                "SELECT sync_time, row_hashes FROM sync_timestamps WHERE table_name = %s",
                (table_name,)
            )
            result = pg_cursor.fetchone()
            
            if result:
                sync_time = result[0]
                row_hashes = result[1] if result[1] else {}
                return sync_time, row_hashes
            
            logging.info(f"No previous sync info found for {table_name}, using default start date")
            return self.default_start_date, {}
            
        except Exception as e:
            logging.error(f"Error getting last sync info for {table_name}: {str(e)}")
            raise

    def update_sync_info(self, pg_cursor, table_name: str, sync_time: datetime, new_hashes: Dict[str, str]):
        """Update sync information in the sync_timestamps table."""
        try:
            pg_cursor.execute(
                "SELECT row_hashes FROM sync_timestamps WHERE table_name = %s",
                (table_name,)
            )
            result = pg_cursor.fetchone()
            existing_hashes = {}
            if result and result[0]:
                existing_hashes = result[0]

            merged_hashes = {**existing_hashes, **new_hashes}

            pg_cursor.execute("""
                INSERT INTO sync_timestamps (table_name, sync_time, row_hashes)
                VALUES (%s, %s, %s)
                ON CONFLICT (table_name)
                DO UPDATE SET
                    sync_time = EXCLUDED.sync_time,
                    row_hashes = EXCLUDED.row_hashes;
            """, (
                table_name,
                sync_time,
                json.dumps(merged_hashes, cls=DateTimeEncoder)
            ))
        except Exception as e:
            logging.error(f"Error updating sync_timestamps for {table_name}: {str(e)}")
            raise

    def log_sync_info(
        self, pg_cursor, table_name: str, total_rows: int,
        new_rows: int, updated_rows: int, deleted_rows: int,
        error_message: Optional[str] = None
    ):
        """Log synchronization details into the sync_log table."""
        pg_cursor.execute("""
            INSERT INTO sync_log (
                table_name, total_rows, new_rows,
                updated_rows, deleted_rows, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (table_name, total_rows, new_rows, updated_rows,
              deleted_rows, error_message))

    def get_dependencies(self, table_name: str) -> Set[str]:
        """Recursively get all dependencies for a table."""
        deps = set()
        for dep in self.table_configs[table_name].dependencies:
            deps.add(dep)
            deps.update(self.get_dependencies(dep))
        return deps

    def resolve_table_order(self) -> List[str]:
        """Resolve the order in which tables should be processed."""
        processed: List[str] = []
        unprocessed = set(self.table_configs.keys())

        while unprocessed:
            for table in unprocessed:
                if all(dep in processed for dep in self.table_configs[table].dependencies):
                    processed.append(table)
                    unprocessed.remove(table)
                    break
            else:
                if unprocessed:
                    raise ValueError(f"Circular dependency detected in tables: {unprocessed}")

        return processed

    @backoff.on_exception(
        backoff.expo,
        (psycopg2.Error, ValueError),
        max_tries=3
    )
    def lookup_value(
        self, pg_cursor, table: str,
        key_column: str, value_column: str,
        key_value: str
    ) -> Optional[Any]:
        """Look up a value in the PostgreSQL database with caching and retry."""
        # Handle null/empty values
        if key_value is None or (isinstance(key_value, str) and not key_value.strip()):
            return None
            
        cached_value = self.lookup_cache.get(table, key_value, key_column, value_column)
        if cached_value is not None:
            return cached_value

        query = f"SELECT {value_column} FROM {table} WHERE {key_column} = %s"
        pg_cursor.execute(query, (key_value,))
        result = pg_cursor.fetchone()
        
        if result:
            self.lookup_cache.set(table, key_value, key_column, value_column, result[0])
            return result[0]

        logging.debug(f"No matching record found in {table} for {key_column}={key_value}")
        return None

    def transform_value(self, mapping: dict, value: Any, pg_cursor) -> Any:
        """Transform a value based on mapping configuration."""
        # First handle None/empty values - return None for lookups, empty string for direct
        if value is None or (isinstance(value, str) and not value.strip()):
            if mapping['type'] == 'lookup':
                return None
            return value
                
        # Handle string values - trim whitespace
        if isinstance(value, str):
            value = value.strip()
        
        if mapping['type'] == 'direct':
            return value
        elif mapping['type'] == 'lookup':
            try:
                # Attempt lookup, return None if lookup fails
                lookup_result = self.lookup_value(
                    pg_cursor,
                    mapping['lookup_table'],
                    mapping['lookup_key'],
                    mapping['lookup_value'],
                    str(value).strip()  # Ensure the lookup key is also trimmed
                )
                return lookup_result
            except Exception as e:
                logging.warning(
                    f"Lookup failed for table {mapping['lookup_table']}, "
                    f"key {mapping['lookup_key']}={value}. Setting value to None."
                )
                return None
        else:
            raise ValueError(f"Unknown transformation type: {mapping['type']}")

    @backoff.on_exception(
        backoff.expo,
        (psycopg2.Error, ValueError),
        max_tries=3
    )
        
    def run_etl(self):
        """Run the complete ETL process."""
        start_time = time.time()
        try:
            self.lookup_cache.clear()
            self.processed_tables.clear()
            
            table_order = self.resolve_table_order()
            logging.info(f"Processing tables in order: {table_order}")
            
            max_workers = min(len(table_order), 5)
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                for table_name in table_order:
                    future = executor.submit(self.process_table, table_name)
                    futures[future] = table_name
                
                completed_tables = set()
                failed_tables = set()
                
                for future in as_completed(futures):
                    table_name = futures[future]
                    try:
                        future.result()
                        completed_tables.add(table_name)
                        logging.info(f"Successfully completed processing table: {table_name}")
                    except Exception as e:
                        failed_tables.add(table_name)
                        logging.error(f"Failed processing table {table_name}: {str(e)}")
                
                if failed_tables:
                    failed_tables_str = ", ".join(failed_tables)
                    raise Exception(f"ETL process completed with failures in tables: {failed_tables_str}")
                
            total_time = time.time() - start_time
            logging.info(
                f"ETL process completed successfully in {total_time:.2f} seconds. "
                f"Processed {len(completed_tables)} tables."
            )
            
        except Exception as e:
            logging.error(f"ETL process failed: {str(e)}")
            raise
        
        finally:
            self.lookup_cache.clear()

    def process_table(self, table_name: str):
        """Process a single table using continuous change detection."""
        start_time = time.time()
        config = self.table_configs[table_name.lower()]
        
        for dep in config.dependencies:
            if dep.lower() not in self.processed_tables:
                logging.info(f"Processing dependency {dep} for {table_name}")
                self.process_table(dep)

        logging.info(f"Starting to process table {table_name}")
        
        with self.get_connections() as (mssql_conn, pg_conn):
            try:
                mssql_cursor = mssql_conn.cursor()
                pg_cursor = pg_conn.cursor()
                
                mssql_cursor.arraysize = config.batch_size
                
                available_source_columns = self.get_source_columns(mssql_cursor, config.source_table)
                mapped_columns = []
                
                for source_col in config.mappings.keys():
                    if source_col not in available_source_columns:
                        logging.warning(
                            f"Mapped column {source_col} not found in source table {config.source_table}. "
                            "Skipping this column."
                        )
                        continue
                    mapped_columns.append(source_col)

                if not mapped_columns:
                    raise ValueError(f"No valid mapped columns found for table {table_name}")

                last_sync_time, last_hashes = self.get_last_sync_info(pg_cursor, table_name)
                
                query = f"SELECT {', '.join(mapped_columns)} FROM {config.source_table}"
                # logging.debug(f"Executing query: {query}")
                mssql_cursor.execute(query)

                current_hashes = {}
                stats = {
                    'new_rows': 0,
                    'updated_rows': 0,
                    'deleted_rows': 0,
                    'total_rows': 0,
                    'error_rows': 0
                }
                rows_to_update = []
                batch_count = 0

                while True:
                    rows = mssql_cursor.fetchmany(config.batch_size)
                    if not rows:
                        break

                    batch_count += 1
                    if batch_count % 10 == 0:
                        logging.info(
                            f"Processing batch {batch_count} of {table_name}. "
                            f"Processed {stats['total_rows']} rows so far"
                        )

                    for row in rows:
                        try:
                            row_dict = {col.lower(): val for col, val in zip(mapped_columns, row)}
                            
                            pk_value = row_dict.get(config.primary_key)
                            if pk_value is None:
                                logging.error(
                                    f"Primary key {config.primary_key} not found in row: {row_dict}. "
                                    "Skipping row."
                                )
                                stats['error_rows'] += 1
                                continue

                            transformed_row = {}
                            for source_col, mapping in config.mappings.items():
                                source_value = row_dict.get(source_col)
                                if source_value is not None:
                                    try:
                                        transformed_value = self.transform_value(mapping, source_value, pg_cursor)
                                        transformed_row[mapping['target']] = transformed_value
                                    except Exception as e:
                                        logging.error(
                                            f"Error transforming column {source_col} in {table_name}: {str(e)}. "
                                            "Skipping this column."
                                        )
                                        continue

                            row_hash = self.calculate_row_hash(transformed_row)
                            str_pk_value = str(pk_value)
                            current_hashes[str_pk_value] = row_hash
                            
                            if str_pk_value not in last_hashes:
                                stats['new_rows'] += 1
                                rows_to_update.append(transformed_row)
                            elif last_hashes[str_pk_value] != row_hash:
                                stats['updated_rows'] += 1
                                rows_to_update.append(transformed_row)
                            
                            stats['total_rows'] += 1

                            if len(rows_to_update) >= config.batch_size:
                                self.batch_update_rows(pg_cursor, config, rows_to_update)
                                rows_to_update = []
                                pg_conn.commit()

                        except Exception as e:
                            logging.error(f"Error processing row in {table_name}: {str(e)}")
                            stats['error_rows'] += 1

                # Final batch update
                if rows_to_update:
                    self.batch_update_rows(pg_cursor, config, rows_to_update)
                    pg_conn.commit()

                # Update sync information
                sync_time = datetime.now()
                self.update_sync_info(pg_cursor, table_name, sync_time, current_hashes)

                # Log the synchronization results
                self.log_sync_info(pg_cursor, table_name, stats['total_rows'], 
                                   stats['new_rows'], stats['updated_rows'], 
                                   stats['deleted_rows'], None)

                logging.info(
                    f"Completed processing table {table_name}. "
                    f"Total rows: {stats['total_rows']}, New rows: {stats['new_rows']}, "
                    f"Updated rows: {stats['updated_rows']}, Deleted rows: {stats['deleted_rows']}, "
                    f"Errors: {stats['error_rows']}"
                )

            except Exception as e:
                logging.error(f"Error processing table {table_name}: {str(e)}")
                self.log_sync_info(pg_cursor, table_name, stats['total_rows'], 
                                   stats['new_rows'], stats['updated_rows'], 
                                   stats['deleted_rows'], str(e))
                raise

            finally:
                mssql_cursor.close()
                pg_cursor.close()

        # Mark the table as processed
        with self.processed_tables_lock:
            self.processed_tables.add(table_name)

    def batch_update_rows(self, pg_cursor, config: TableConfig, rows: List[Dict[str, Any]]):
        """Batch update rows into the target PostgreSQL table."""
        if not rows:
            return

        try:
            # Verify table structure
            pg_cursor.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (config.target_table,))
            
            table_columns = {row[0]: row[1] for row in pg_cursor.fetchall()}
            logging.debug(f"Table columns for {config.target_table}: {table_columns}")

            # Debug log the first row to see what we're working with
            logging.debug(f"First row of batch: {rows[0]}")
            
            # Validate all columns exist in table
            columns = list(rows[0].keys())
            for col in columns:
                if col.lower() not in table_columns:
                    raise ValueError(f"Column {col} not found in table {config.target_table}")

            placeholders = ', '.join(['%s'] * len(columns))
            column_names = ', '.join(f'"{col.lower()}"' for col in columns)
            target_pk = config.get_target_primary_key()

            # Build update SET clause excluding primary key
            update_sets = []
            for col in columns:
                if col.lower() != target_pk.lower():
                    update_sets.append(f'"{col}" = EXCLUDED."{col}"')
            
            update_clause = ', '.join(update_sets)

            insert_query = f"""
                INSERT INTO {config.target_table} ({column_names})
                VALUES ({placeholders})
                ON CONFLICT ({target_pk})
                DO UPDATE SET
                    {update_clause}
            """
            
            logging.debug(f"Generated SQL: {insert_query}")

            # Prepare and validate values
            values = []
            for i, row in enumerate(rows):
                try:
                    row_values = []
                    for col in columns:
                        val = row.get(col)
                        # Add type validation here if needed
                        row_values.append(val)
                    values.append(row_values)
                except Exception as e:
                    logging.error(f"Error processing row {i}: {row}")
                    logging.error(f"Error details: {str(e)}")
                    raise

            # Execute in smaller batches
            batch_size = 1000
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                try:
                    pg_cursor.executemany(insert_query, batch)
                except Exception as e:
                    logging.error(f"Error executing batch {i//batch_size + 1}: {str(e)}")
                    # Log a few problem rows
                    for problem_row in batch[:5]:
                        logging.error(f"Problem row data: {dict(zip(columns, problem_row))}")
                    raise

        except Exception as e:
            logging.error(f"Error in batch_update_rows: {str(e)}")
            logging.error(f"Target table: {config.target_table}")
            logging.error(f"Table structure: {table_columns}")
            logging.error(f"Columns being updated: {columns}")
            logging.error(f"Number of rows in batch: {len(rows)}")
            raise

if __name__ == "__main__":
    with open('db_config.json', 'r') as f:
        db_config = json.load(f)
    
    with open('etl_config.json', 'r') as f:
        etl_config = json.load(f)
    
    etl_manager = ETLManager(db_config, etl_config)
    
    while True:
        try:
            etl_manager.run_etl()
            logging.info("ETL cycle completed successfully")
        except Exception as e:
            logging.error(f"ETL cycle failed: {str(e)}")
        
        time.sleep(120)  # 2 minutes between cycles