import json
import pyodbc
import psycopg2
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
from typing import List, Dict, Any, Tuple
import hashlib
from contextlib import contextmanager
import statistics
from decimal import Decimal
import time
from dataclasses import dataclass
import os

# Set up rotating log handler
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            'db_sync.log',
            maxBytes=10 * 1024 * 1024,  # 10MB per file
            backupCount=5,  # Keep 5 backup files
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
    source_name: str        
    target_name: str   
    primary_keys: List[str]
    track_changes: bool
    timestamp_column: str
    batch_size: int = 30000
    retry_attempts: int = 3
    sync_interval: int = 120
    min_batch_size: int = 30000
    max_batch_size: int = 50000
    performance_threshold: float = 2.0
    sync_strategy: str = "row_hash"  # Default strategy

@dataclass
class SyncMetrics:
    total_rows: int = 0
    processed_rows: int = 0
    start_time: datetime = None
    batch_times: List[float] = None
    
    def __post_init__(self):
        self.batch_times = []
        self.start_time = datetime.now()

    def get_progress(self) -> float:
        return (self.processed_rows / self.total_rows * 100) if self.total_rows > 0 else 0

    def get_average_batch_time(self) -> float:
        return statistics.mean(self.batch_times) if self.batch_times else 0

class DatabaseSync:
    @staticmethod
    def load_config(file_path: str) -> dict:
        with open(file_path, 'r') as config_file:
            return json.load(config_file)

    def __init__(self, db_config: dict, table_config: dict):
        self.mssql_conn_string = (
            f"Driver={{SQL Server}};"
            f"Server={db_config['mssql_connection']['server']};"
            f"Database={db_config['mssql_connection']['database']};"
            f"UID={db_config['mssql_connection']['uid']};"
            f"PWD={db_config['mssql_connection']['pwd']};"
        )
        
        self.pg_conn_string = (
            f"dbname={db_config['postgres_connection']['dbname']} "
            f"user={db_config['postgres_connection']['user']} "
            f"password={db_config['postgres_connection']['password']} "
            f"host={db_config['postgres_connection']['host']} "
            f"port={db_config['postgres_connection']['port']}"
        )
        
        self.metrics: Dict[str, SyncMetrics] = {}
        self.table_configs = {
            name: TableConfig(
                source_name=name,  
                target_name=settings['target_name'],
                primary_keys=settings['primary_keys'],
                track_changes=settings['track_changes'],
                timestamp_column=settings.get('timestamp_column', ''),
                batch_size=settings.get('batch_size', 20000),  
                retry_attempts=settings.get('retry_attempts', 3),
                sync_interval=settings.get('sync_interval', 120),
                min_batch_size=settings.get('min_batch_size', 10000),
                max_batch_size=settings.get('max_batch_size', 50000),
                performance_threshold=settings.get('performance_threshold', 2.0),
                sync_strategy=settings.get('sync_strategy')  # Load strategy from config
            )
            for name, settings in table_config.items() if isinstance(settings, dict)
        }
        
        # Load independent and dependent tables from config
        self.independent_tables = table_config.get("independent_tables", [])
        self.dependent_tables = table_config.get("dependent_tables", [])
        
        self.default_start_date = datetime(2000, 1, 1)
        self.first_sync_tracker: Dict[str, bool] = {name: True for name in self.table_configs.keys()}

        # Define sync_interval
        self.sync_interval = table_config.get("sync_interval", 300)  # Default to 300 seconds if not set

    @contextmanager
    def get_connections(self):
        mssql_conn = None
        pg_conn = None
        try:
            mssql_conn = pyodbc.connect(self.mssql_conn_string, timeout=30)
            pg_conn = psycopg2.connect(self.pg_conn_string)
            yield mssql_conn, pg_conn
        except Exception as e:
            logging.error(f"Connection error: {str(e)}")
            raise
        finally:
            if mssql_conn:
                mssql_conn.close()
            if pg_conn:
                pg_conn.close()

    def create_sync_tables(self, pg_cursor):
        table_definitions = {
            "sync_timestamps": """
                CREATE TABLE IF NOT EXISTS sync_timestamps (
                    table_name VARCHAR(255) PRIMARY KEY,
                    sync_time TIMESTAMP NOT NULL,
                    row_hashes JSONB NOT NULL DEFAULT '{}'::jsonb
                )
            """,
            "sync_attempts": """
                CREATE TABLE IF NOT EXISTS sync_attempts (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(255) NOT NULL,
                    attempt_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    changes_found BOOLEAN NOT NULL,
                    rows_processed INT NOT NULL
                )
            """,
            "sync_logs": """
                CREATE TABLE IF NOT EXISTS sync_logs (
                    id SERIAL PRIMARY KEY,
                    source_table_name VARCHAR(255) NOT NULL,
                    target_table_name VARCHAR(255) NOT NULL,
                    sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR(50) NOT NULL,
                    error_message TEXT,
                    metrics JSONB,
                    is_significant BOOLEAN NOT NULL,
                    sync_strategy VARCHAR(50) NOT NULL
                )
            """
        }

        for table_name, create_statement in table_definitions.items():
            try:
                pg_cursor.execute(create_statement)
                logging.info(f"Table {table_name} ensured.")
            except Exception as e:
                logging.error(f"Error creating table {table_name}: {str(e)}")

    def calculate_row_hash(self, row_dict: Dict[str, Any]) -> str:
        row_str = json.dumps(row_dict, sort_keys=True, cls=DateTimeEncoder)
        return hashlib.md5(row_str.encode()).hexdigest()

    def get_last_sync_info(self, pg_cursor, table_name: str) -> Tuple[datetime, Dict[str, str]]:
        try:
            logging.debug(f"Fetching last sync info for {table_name}")
            pg_cursor.execute(
                "SELECT sync_time, row_hashes FROM sync_timestamps WHERE table_name = %s",
                (table_name,)
            )
            result = pg_cursor.fetchone()
            
            if result:
                return result[0], result[1] or {}
            return self.default_start_date, {}
            
        except Exception as e:
            logging.error(f"Error getting last sync info for {table_name}: {str(e)}")
            return self.default_start_date, {}

    def update_sync_info(self, pg_cursor, table_name: str, sync_time: datetime, new_hashes: Dict[str, str]):
        try:
            # First get existing hashes
            pg_cursor.execute(
                "SELECT row_hashes FROM sync_timestamps WHERE table_name = %s",
                (table_name,)
            )
            result = pg_cursor.fetchone()
            existing_hashes = {}
            if result and result[0]:
                existing_hashes = result[0]

            # Merge existing hashes with new ones
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

    def sync_table(self, mssql_cursor, pg_conn, pg_cursor, config: TableConfig):
        source_table_name = config.source_name
        target_table_name = config.target_name

        try:
            self.metrics[source_table_name] = SyncMetrics()

            # Fetch last sync time and hashes
            last_sync_time, last_hashes = self.get_last_sync_info(pg_cursor, source_table_name)

            logging.debug(f"Checking for changes in {source_table_name} since {last_sync_time}")

            # Determine sync strategy
            is_first_sync = self.first_sync_tracker[source_table_name]
            params = None

            if is_first_sync:
                # First sync uses row hash strategy
                all_rows_query = f"SELECT * FROM {source_table_name}"
                sync_strategy_used = "row_hash"
                self.first_sync_tracker[source_table_name] = False
            else:
                # Subsequent syncs: check the strategy
                if config.sync_strategy == "timestamp" and config.timestamp_column:
                    all_rows_query = f"SELECT * FROM {source_table_name} WHERE {config.timestamp_column} > ?"
                    params = (last_sync_time,)
                    sync_strategy_used = "timestamp"
                else:
                    all_rows_query = f"SELECT * FROM {source_table_name}"
                    sync_strategy_used = "row_hash"

            # Execute the query to get rows from source
            if params:
                mssql_cursor.execute(all_rows_query, params)
            else:
                mssql_cursor.execute(all_rows_query)
                
            columns = [column[0] for column in mssql_cursor.description]
            total_rows = 0
            current_hashes = {}
            changed_rows = []

            while True:
                rows = mssql_cursor.fetchmany(config.batch_size)
                if not rows:
                    break

                for row in rows:
                    row_dict = dict(zip(columns, row))
                    pk_value = "_".join(str(row_dict[pk]) for pk in config.primary_keys)
                    row_hash = self.calculate_row_hash(row_dict)
                    current_hashes[pk_value] = row_hash

                    # For row_hash strategy, check if hash has changed
                    if sync_strategy_used == "row_hash" and not is_first_sync:
                        if pk_value not in last_hashes or last_hashes[pk_value] != row_hash:
                            changed_rows.append(row)
                            total_rows += 1
                    else:
                        changed_rows.append(row)
                        total_rows += 1

            self.metrics[source_table_name].total_rows = total_rows

            # Log the sync attempt
            self.log_sync_attempt(pg_cursor, source_table_name, changes_found=(total_rows > 0), rows_processed=total_rows)

            if total_rows == 0:
                logging.debug(f"No changes detected in {source_table_name}, skipping sync")
                # Still update the sync info to maintain latest check time
                self.update_sync_info(pg_cursor, source_table_name, datetime.now(), current_hashes)
                pg_conn.commit()
                return

            # Process only the changed rows
            batch_start_time = time.time()
            changes_detected = False

            # Process rows in batches
            for i in range(0, len(changed_rows), config.batch_size):
                batch_rows = changed_rows[i:i + config.batch_size]
                
                for row in batch_rows:
                    # Perform upsert
                    placeholders = ", ".join(["%s"] * len(row))
                    update_cols = [f"{col} = EXCLUDED.{col}" for col in columns]

                    upsert_query = f"""
                        INSERT INTO {target_table_name} ({', '.join(columns)})
                        VALUES ({placeholders})
                        ON CONFLICT ({', '.join(config.primary_keys)})  -- Use the unique column(s) directly
                        DO UPDATE SET {', '.join(update_cols)}
                    """

                    for attempt in range(config.retry_attempts):
                        try:
                            pg_cursor.execute(upsert_query, row)
                            break
                        except Exception as e:
                            if attempt == config.retry_attempts - 1:
                                raise
                            time.sleep(1)

                batch_time = time.time() - batch_start_time
                self.metrics[source_table_name].batch_times.append(batch_time)
                self.metrics[source_table_name].processed_rows += len(batch_rows)

                changes_detected = True

            if changes_detected:
                # Update sync info with the new hashes
                self.update_sync_info(pg_cursor, source_table_name, datetime.now(), current_hashes)
                self.log_sync(pg_cursor, source_table_name, target_table_name, "success", is_significant=True, strategy=sync_strategy_used)
                self.log_performance_metrics(source_table_name)

            pg_conn.commit()

        except Exception as e:
            logging.error(f"Error syncing table {source_table_name}: {str(e)}")
            self.log_sync(pg_cursor, source_table_name, target_table_name, "failure", str(e), is_significant=True, strategy=sync_strategy_used)
            pg_conn.commit()
            raise

    def log_sync_attempt(self, pg_cursor, table_name: str, changes_found: bool, rows_processed: int = 0):
        try:
            pg_cursor.execute("""
                INSERT INTO sync_attempts 
                (table_name, changes_found, rows_processed)
                VALUES (%s, %s, %s)
            """, (table_name, changes_found, rows_processed))
        except Exception as e:
            logging.error(f"Error logging sync attempt for {table_name}: {str(e)}")

    def log_sync(self, pg_cursor, source_table_name: str, target_table_name: str, status: str, error_message: str = None, is_significant: bool = True, strategy: str = "row_hash"):
        try:
            metrics_data = None
            if source_table_name in self.metrics:
                metrics_data = {
                    'total_rows': self.metrics[source_table_name].total_rows,
                    'processed_rows': self.metrics[source_table_name].processed_rows,
                    'average_batch_time': self.metrics[source_table_name].get_average_batch_time(),
                    'total_time': (datetime.now() - self.metrics[source_table_name].start_time).total_seconds(),
                    'sync_start_time': self.metrics[source_table_name].start_time.isoformat()
                }

            pg_cursor.execute("""
                INSERT INTO sync_logs 
                (source_table_name, target_table_name, sync_time, status, error_message, metrics, is_significant, sync_strategy)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                source_table_name,
                target_table_name,
                datetime.now(),
                status,
                error_message,
                json.dumps(metrics_data, cls=DateTimeEncoder) if metrics_data else None,
                is_significant,
                strategy  # Log the strategy used
            ))
        except Exception as e:
            logging.error(f"Error logging to sync_logs for {source_table_name}: {str(e)}")
            raise

    def get_table_row_count(self, mssql_cursor, table_name: str, last_sync_time: datetime = None) -> int:
        query = f"SELECT COUNT(*) FROM {table_name}"
        if last_sync_time:
            query += f" WHERE {self.table_configs[table_name].timestamp_column} > ?"
            mssql_cursor.execute(query, (last_sync_time,))
        else:
            mssql_cursor.execute(query)
        return mssql_cursor.fetchone()[0]

    def adjust_batch_size(self, table_name: str, current_batch_time: float) -> int:
        config = self.table_configs[table_name]
        current_batch_size = config.batch_size

        if current_batch_time > config.performance_threshold:
            new_size = max(current_batch_size // 2, config.min_batch_size)
        else:
            new_size = min(current_batch_size * 2, config.max_batch_size)

        config.batch_size = new_size
        return new_size

    def log_performance_metrics(self, table_name: str):
        metrics = self.metrics[table_name]
        elapsed_time = (datetime.now() - metrics.start_time).total_seconds()
        avg_batch_time = metrics.get_average_batch_time()
        
        logging.info(f"""
            Performance metrics for {table_name}:
            - Total rows processed: {metrics.processed_rows}
            - Total time: {elapsed_time:.2f} seconds
            - Average batch time: {avg_batch_time:.2f} seconds
            - Current batch size: {self.table_configs[table_name].batch_size}
            - Rows per second: {metrics.processed_rows / elapsed_time:.2f}
        """)

    def get_sync_statistics(self, pg_cursor, days: int = 7):
        query = """
        SELECT 
            table_name,
            COUNT(*) as total_attempts,
            SUM(CASE WHEN changes_found THEN 1 ELSE 0 END) as attempts_with_changes,
            SUM(rows_processed) as total_rows_processed,
            MAX(attempt_time) as last_attempt
        FROM sync_attempts
        WHERE attempt_time > NOW() - INTERVAL '%s days'
        GROUP BY table_name
        """
        pg_cursor.execute(query, (days,))
        return pg_cursor.fetchall()

    def sync_all_tables(self):
        with self.get_connections() as (mssql_conn, pg_conn):
            pg_cursor = pg_conn.cursor()
            self.create_sync_tables(pg_cursor)
            pg_conn.commit() 

        for table_name in self.independent_tables + self.dependent_tables:
            with self.get_connections() as (mssql_conn, pg_conn):
                try:
                    self.sync_table(
                        mssql_conn.cursor(),
                        pg_conn,
                        pg_conn.cursor(),
                        self.table_configs[table_name]
                    )
                except Exception as e:
                    logging.error(f"Failed to sync {table_name}: {str(e)}")
                    continue

if __name__ == "__main__":
    db_config = DatabaseSync.load_config('db_config.json')
    table_config = DatabaseSync.load_config('table_config.json')

    sync_manager = DatabaseSync(db_config, table_config)
    
    while True:
        try:
            sync_manager.sync_all_tables()
            logging.info("Sync cycle completed successfully.")
        except Exception as e:
            logging.error(f"Error during sync cycle: {str(e)}")
        time.sleep(sync_manager.sync_interval)  # Sleep for the defined sync interval