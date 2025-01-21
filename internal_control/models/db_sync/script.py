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
import psutil
import threading

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

@dataclass
class ResourceMetrics:
    cpu_percent: float
    memory_percent: float
    available_memory_gb: float
    disk_io_percent: float

class ResourceMonitor:
    def __init__(self):
        self.process = psutil.Process()
        self._lock = threading.Lock()
        self._metrics: Optional[ResourceMetrics] = None
        self._last_update = 0
        self.update_interval = 5  # seconds
        
    def get_metrics(self) -> ResourceMetrics:
        """Get current system resource metrics with caching."""
        current_time = time.time()
        
        with self._lock:
            if self._metrics is None or (current_time - self._last_update) > self.update_interval:
                try:
                    cpu_percent = psutil.cpu_percent(interval=1) / psutil.cpu_count()
                    memory = psutil.virtual_memory()
                    memory_percent = memory.percent
                    available_memory_gb = memory.available / (1024 ** 3)
                    disk_io = psutil.disk_io_counters()
                    disk_io_percent = self.process.io_counters().read_bytes / (1024 ** 2)
                    
                    self._metrics = ResourceMetrics(
                        cpu_percent=cpu_percent,
                        memory_percent=memory_percent,
                        available_memory_gb=available_memory_gb,
                        disk_io_percent=disk_io_percent
                    )
                    self._last_update = current_time
                    
                except Exception as e:
                    logging.error(f"Error getting system metrics: {str(e)}")
                    self._metrics = ResourceMetrics(50, 50, 4, 50)
                    
        return self._metrics

class DynamicResourceManager:
    def __init__(self, initial_batch_size: int = 5000, initial_thread_count: int = 4):
        self.monitor = ResourceMonitor()
        self.min_batch_size = 1000
        self.max_batch_size = 50000
        self.min_threads = 2
        self.max_threads = 16
        
        self.current_batch_size = initial_batch_size
        self.current_thread_count = initial_thread_count
        
        self.cpu_threshold = 75
        self.memory_threshold = 80
        self.io_threshold = 70
        self.min_available_memory = 2
        
    def adjust_resources(self, table_name: str) -> tuple[int, int]:
        """Dynamically adjust batch size and thread count based on system metrics."""
        metrics = self.monitor.get_metrics()
        
        logging.info(
            f"Resource metrics for {table_name}: CPU: {metrics.cpu_percent:.1f}%, "
            f"Memory: {metrics.memory_percent:.1f}%, "
            f"Available Memory: {metrics.available_memory_gb:.1f}GB, "
            f"Disk I/O: {metrics.disk_io_percent:.1f}MB/s"
        )
        
        new_batch_size = self.current_batch_size
        new_thread_count = self.current_thread_count
        
        if metrics.cpu_percent > self.cpu_threshold:
            new_thread_count = max(self.min_threads, self.current_thread_count - 1)
        elif metrics.cpu_percent < 50 and metrics.memory_percent < 70:
            new_thread_count = min(self.max_threads, self.current_thread_count + 1)
            
        if metrics.memory_percent > self.memory_threshold or metrics.available_memory_gb < self.min_available_memory:
            new_batch_size = max(self.min_batch_size, int(self.current_batch_size * 0.8))
        elif metrics.memory_percent < 60 and metrics.available_memory_gb > self.min_available_memory * 2:
            new_batch_size = min(self.max_batch_size, int(self.current_batch_size * 1.2))
            
        if metrics.disk_io_percent > self.io_threshold:
            new_batch_size = max(self.min_batch_size, int(self.current_batch_size * 0.8))
            
        if new_batch_size != self.current_batch_size or new_thread_count != self.current_thread_count:
            logging.info(
                f"Adjusting resources for {table_name}: "
                f"Batch size: {self.current_batch_size} -> {new_batch_size}, "
                f"Threads: {self.current_thread_count} -> {new_thread_count}"
            )
            
        self.current_batch_size = new_batch_size
        self.current_thread_count = new_thread_count
        
        return new_batch_size, new_thread_count

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
            normalized_mapping['target'] = mapping['target'].lower()
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
        self.default_start_date = datetime(2000, 1, 1)
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
        self.resource_manager = DynamicResourceManager()

        self._initialize_database()

    def get_source_query(self, table_name: str, mapped_columns: List[str], last_sync_time: datetime) -> str:
        """Generate optimized source query with change tracking."""
        columns = ', '.join(mapped_columns)
        config = self.table_configs[table_name]
        
        base_query = f"SELECT {columns} FROM {config.source_table}"
        
        try:
            with self.get_connections() as (mssql_conn, _):
                cursor = mssql_conn.cursor()
                cursor.execute(f"SELECT TOP 0 * FROM {config.source_table}")
                available_columns = {col[0].lower() for col in cursor.description}
                
                modified_columns = [
                    'modified_date', 'last_modified', 'update_date', 
                    'modification_date', 'modified_on', 'updated_at'
                ]
                
                mod_column = next((col for col in modified_columns if col in available_columns), None)
                
                if mod_column:
                    return f"""
                        {base_query}
                        WHERE {mod_column} >= ?
                        OR {config.primary_key} IN (
                            SELECT {config.primary_key} 
                            FROM {config.source_table}
                            WHERE {mod_column} IS NULL
                        )
                    """
                
                return base_query
                
        except Exception as e:
            logging.warning(f"Error detecting modification columns for {table_name}: {str(e)}")
            return base_query

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
        max_tries=5
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
        """Process a single table using dynamic resource management."""
        config = self.table_configs[table_name.lower()]
        
        for dep in config.dependencies:
            if dep.lower() not in self.processed_tables:
                self.process_table(dep)

        with self.get_connections() as (mssql_conn, pg_conn):
            try:
                mssql_cursor = mssql_conn.cursor()
                pg_cursor = pg_conn.cursor()
                
                last_sync_time, last_hashes = self.get_last_sync_info(pg_cursor, table_name)
                available_columns = self.get_source_columns(mssql_cursor, config.source_table)
                mapped_columns = [col for col in config.mappings.keys() 
                                if col in available_columns]
                
                if not mapped_columns:
                    raise ValueError(f"No valid mapped columns for {table_name}")
                
                query = self.get_source_query(table_name, mapped_columns, last_sync_time)
                
                if "WHERE" in query:
                    mssql_cursor.execute(query, (last_sync_time,))
                else:
                    mssql_cursor.execute(query)
                
                total_rows_processed = 0
                start_time = time.time()
                
                while True:
                    batch_size, thread_count = self.resource_manager.adjust_resources(table_name)
                    
                    rows = mssql_cursor.fetchmany(batch_size)
                    if not rows:
                        break
                        
                    total_rows_processed += len(rows)
                    
                    elapsed_time = time.time() - start_time
                    if elapsed_time > 0:
                        rows_per_second = total_rows_processed / elapsed_time
                        logging.info(
                            f"Processing {table_name}: {total_rows_processed:,} rows "
                            f"({rows_per_second:.0f} rows/sec)"
                        )
                    
                    with ThreadPoolExecutor(max_workers=thread_count) as executor:
                        futures = []
                        for row in rows:
                            futures.append(
                                executor.submit(
                                    self.transform_row,
                                    row,
                                    mapped_columns,
                                    config,
                                    pg_cursor
                                )
                            )
                            
                        transformed_batch = []
                        for future in as_completed(futures):
                            result = future.result()
                            if result:
                                transformed_batch.append(result)
                                
                        if transformed_batch:
                            self.batch_update_rows(pg_cursor, config, transformed_batch)
                            pg_conn.commit()
                            
                self.update_sync_info(pg_cursor, table_name, datetime.now(), {})
                pg_conn.commit()
                
            finally:
                with self.processed_tables_lock:
                    self.processed_tables.add(table_name)

    def transform_row(self, row: tuple, columns: List[str], 
                     config: TableConfig, pg_cursor) -> Optional[Dict[str, Any]]:
        """Transform a single row with error handling."""
        try:
            row_dict = {col: val for col, val in zip(columns, row)}
            
            if not row_dict.get(config.primary_key):
                return None
                
            transformed = {}
            for source_col, mapping in config.mappings.items():
                if source_col not in row_dict:
                    continue
                    
                value = row_dict[source_col]
                transformed[mapping['target']] = self.transform_value(
                    mapping, value, pg_cursor
                )
                
            return transformed
            
        except Exception as e:
            logging.error(f"Error transforming row: {str(e)}")
            return None

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
            batch_size = 20000
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

# if __name__ == "__main__":
#     with open('db_config.json', 'r') as f:
#         db_config = json.load(f)
    
#     with open('etl_config.json', 'r') as f:
#         etl_config = json.load(f)
    
#     etl_manager = ETLManager(db_config, etl_config)
    
#     while True:
#          try:
#             etl_manager.run_etl()
#             logging.info("ETL cycle completed successfully")
#          except Exception as e:
#             logging.error(f"ETL cycle failed: {str(e)}")
        
#          time.sleep(120)  # 2 minutes between cycles

if __name__ == "__main__":
    with open('db_config.json', 'r') as f:
        db_config = json.load(f)
    
    with open('etl_config.json', 'r') as f:
        etl_config = json.load(f)
    
    # Create config with only transactions table
    transactions_config = {
        'tables': {
            'tbl_transactions': etl_config['tables']['tbl_transactions']
        }
    }
    
    # Remove dependencies since lookup tables are already populated
    transactions_config['tables']['tbl_transactions']['dependencies'] = []
    
    etl_manager = ETLManager(db_config, transactions_config)
    
    while True:
        try:
            etl_manager.run_etl()
            logging.info("Transactions sync completed successfully")
        except Exception as e:
            logging.error(f"Transactions sync failed: {str(e)}")
        
        time.sleep(120)  # 2 minutes between cycles