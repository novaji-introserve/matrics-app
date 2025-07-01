import sys
import os
import json
import hashlib
import logging
import time
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass
from threading import Lock
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
import backoff
import orjson  # Faster JSON serialization
from functools import lru_cache
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed


def setup_logging():
    log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ETL.log')
    
    # Check if file exists and exceeds size limit
    if os.path.exists(log_file) and os.path.getsize(log_file) >= 30 * 1024 * 1024:
        # Empty the file
        open(log_file, 'w').close()
    
    # Configure logging to file only
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# Custom JSON Encoder
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return str(obj)
        elif isinstance(obj, bytes):
            try:
                return obj.decode('utf-8')
            except UnicodeDecodeError:
                return obj.hex()
        return super().default(obj)

# Data Classes and Helper Classes (unchanged except for minor optimizations)
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
        normalized_mappings = {
            source_col.lower(): {**mapping, 'target': mapping['target'].lower()}
            for source_col, mapping in self.mappings.items()
        }
        self.mappings = normalized_mappings

    def get_target_primary_key(self) -> str:
        for source_col, mapping in self.mappings.items():
            if source_col == self.primary_key:
                return mapping['target']
        logger.warning(f"Primary key '{self.primary_key}' not mapped for {self.source_table}. Assuming same name.")
        return self.primary_key

class DatabaseError(Exception):
    pass

class LookupCache:
    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()

    def get_cache_key(self, table: str, lookup_key: str, key_value: Any) -> str:
        return f"{table}:{lookup_key}:{str(key_value)}"

    def get(self, table: str, lookup_key: str, key_value: Any, lookup_value_col: str) -> Optional[Any]:
        cache_key = self.get_cache_key(table, lookup_key, key_value)
        with self._lock:
            cache_entry = self._cache.get(cache_key)
            return cache_entry.get(lookup_value_col) if cache_entry else None

    def set(self, table: str, lookup_key: str, key_value: Any, lookup_value_col: str, value: Any):
        cache_key = self.get_cache_key(table, lookup_key, key_value)
        with self._lock:
            if cache_key not in self._cache:
                self._cache[cache_key] = {}
            self._cache[cache_key][lookup_value_col] = value

    def clear(self):
        with self._lock:
            self._cache.clear()
            logger.info("Lookup cache cleared.")

class ETLManager:
    def __init__(self, db_config: dict, etl_config: dict):
        self.default_start_date = datetime(2000, 1, 1)
        try:
            self.source_pg_conn_string = db_config['source_postgres']['connection_string'].format(
                **db_config['source_postgres'])
            self.target_pg_conn_string = db_config['target_postgres']['connection_string'].format(
                **db_config['target_postgres'])
        except KeyError as e:
            logger.error(f"Missing key in db_config.json: {e}")
            raise
        self.table_configs = {name.lower(): TableConfig(**config) for name, config in etl_config['tables'].items()}
        self.lookup_cache = LookupCache()
        self.processed_tables: Set[str] = set()
        self.processed_tables_lock = Lock()
        self._initialize_target_database()

    @lru_cache(maxsize=100)
    def get_source_columns(self, table_name: str) -> Tuple[str, ...]:
        """Cache source column names to avoid repeated queries."""
        with self.get_source_connection() as source_conn:
            with source_conn.cursor() as source_cursor:
                try:
                    source_cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
                    return tuple(col.name.lower() for col in source_cursor.description)
                except psycopg2.Error as e:
                    logger.error(f"Error fetching columns for source table {table_name}: {e}")
                    raise DatabaseError(f"Could not get columns for {table_name}") from e

    def _initialize_target_database(self):
        # Unchanged, but ensure indexes are created efficiently
        logger.info("Initializing target database metadata tables...")
        with self.get_target_connection() as target_conn:
            with target_conn.cursor() as target_cursor:
                self._create_sync_table(target_cursor)
                self._create_log_table(target_cursor)
                self._create_target_indexes(target_cursor)
            target_conn.commit()

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_tries=5, max_time=60, logger=logger)
    @contextmanager
    def get_source_connection(self):
        source_conn = None
        try:
            source_conn = psycopg2.connect(self.source_pg_conn_string)
            yield source_conn
        finally:
            if source_conn:
                source_conn.close()

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_tries=5, max_time=60, logger=logger)
    @contextmanager
    def get_target_connection(self):
        target_conn = None
        try:
            target_conn = psycopg2.connect(self.target_pg_conn_string)
            yield target_conn
        finally:
            if target_conn:
                target_conn.close()

    def _create_sync_table(self, target_cursor):
        target_cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_timestamps (
                table_name VARCHAR(255) PRIMARY KEY,
                sync_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                row_hashes JSONB NOT NULL DEFAULT '{}'::jsonb
            );
        """)

    def _create_log_table(self, target_cursor):
        target_cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_log (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                total_rows_processed INT NOT NULL,
                new_rows_inserted INT NOT NULL,
                rows_updated INT NOT NULL,
                rows_deleted INT NOT NULL DEFAULT 0,
                rows_skipped_errors INT NOT NULL DEFAULT 0,
                sync_start_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                sync_end_time TIMESTAMPTZ,
                duration_seconds FLOAT,
                status VARCHAR(50) NOT NULL DEFAULT 'STARTED',
                error_message TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_sync_log_table_name ON sync_log(table_name);
            CREATE INDEX IF NOT EXISTS idx_sync_log_sync_time ON sync_log(sync_start_time);
        """)

    def _create_target_indexes(self, target_cursor):
        for table_key, config in self.table_configs.items():
            try:
                target_pk = config.get_target_primary_key()
                if target_pk:
                    index_name = f"idx_{config.target_table}_{target_pk}".replace('.', '_')
                    target_cursor.execute(f"""
                        CREATE INDEX IF NOT EXISTS "{index_name}"
                        ON "{config.target_table}"("{target_pk}");
                    """)
            except psycopg2.Error as e:
                logger.error(f"Failed to create index for {config.target_table}: {e}")

    def calculate_row_hash(self, row: Dict[str, Any]) -> str:
        """Use orjson for faster JSON serialization."""
        row_bytes = orjson.dumps(row, option=orjson.OPT_SORT_KEYS)
        return hashlib.sha256(row_bytes).hexdigest()

    def get_last_sync_info(self, target_cursor, table_name: str) -> Tuple[datetime, Dict[str, str]]:
        try:
            target_cursor.execute(
                "SELECT sync_time, row_hashes FROM sync_timestamps WHERE table_name = %s",
                (table_name,)
            )
            result = target_cursor.fetchone()
            if result:
                sync_time, row_hashes = result
                if sync_time.tzinfo is None:
                    sync_time = sync_time.replace(tzinfo=timezone.utc)
                return sync_time, row_hashes or {}
            return self.default_start_date, {}
        except psycopg2.Error as e:
            logger.error(f"Error getting sync info for {table_name}: {e}")
            raise DatabaseError(f"Failed to get sync info for {table_name}") from e

    def update_sync_info(self, target_cursor, table_name: str, sync_time: datetime, current_hashes: Dict[str, str]):
        try:
            target_cursor.execute("""
                INSERT INTO sync_timestamps (table_name, sync_time, row_hashes)
                VALUES (%s, %s, %s)
                ON CONFLICT (table_name)
                DO UPDATE SET sync_time = EXCLUDED.sync_time, row_hashes = EXCLUDED.row_hashes;
            """, (table_name, sync_time, orjson.dumps(current_hashes).decode('utf-8')))
        except psycopg2.Error as e:
            logger.error(f"Error updating sync_timestamps for {table_name}: {e}")
            raise DatabaseError(f"Failed to update sync info for {table_name}") from e

    def log_sync_start(self, target_cursor, table_name: str) -> int:
        try:
            target_cursor.execute("""
                INSERT INTO sync_log (table_name, total_rows_processed, new_rows_inserted, rows_updated, status)
                VALUES (%s, 0, 0, 0, 'STARTED') RETURNING id;
            """, (table_name,))
            return target_cursor.fetchone()[0]
        except psycopg2.Error as e:
            logger.error(f"Failed to create start log for {table_name}: {e}")
            return -1

    def log_sync_end(self, target_cursor, log_id: int, table_name: str, stats: Dict[str, int], start_time: float, status: str = "COMPLETED", error_message: Optional[str] = None):
        end_time = time.time()
        duration = end_time - start_time
        try:
            target_cursor.execute("""
                UPDATE sync_log
                SET total_rows_processed = %s, new_rows_inserted = %s, rows_updated = %s,
                    rows_deleted = %s, rows_skipped_errors = %s, sync_end_time = NOW(),
                    duration_seconds = %s, status = %s, error_message = %s
                WHERE id = %s;
            """, (
                stats.get('total_rows', 0), stats.get('new_rows', 0), stats.get('updated_rows', 0),
                stats.get('deleted_rows', 0), stats.get('error_rows', 0), duration, status, error_message, log_id
            ))
        except psycopg2.Error as e:
            logger.error(f"Failed to update end log {log_id} for {table_name}: {e}")

    def get_dependencies(self, table_name: str) -> Set[str]:
        deps = set()
        config = self.table_configs.get(table_name.lower())
        if not config:
            logger.error(f"Configuration not found for table '{table_name}'.")
            return deps
        for dep in config.dependencies:
            if dep.lower() not in self.table_configs:
                logger.warning(f"Dependency '{dep}' for table '{table_name}' not found.")
                continue
            deps.add(dep.lower())
            deps.update(self.get_dependencies(dep.lower()))
        return deps

    def resolve_table_order(self) -> List[str]:
        processed: List[str] = []
        unprocessed = set(self.table_configs.keys())
        while unprocessed:
            made_progress = False
            for table in list(unprocessed):
                config = self.table_configs[table]
                if all(dep.lower() in processed for dep in config.dependencies):
                    processed.append(table)
                    unprocessed.remove(table)
                    made_progress = True
            if not made_progress:
                problematic_nodes = [
                    f"{table} (depends on: {', '.join([dep for dep in self.table_configs[table].dependencies if dep.lower() in unprocessed])})"
                    for table in unprocessed if any(dep.lower() in unprocessed for dep in self.table_configs[table].dependencies)
                ]
                raise ValueError(f"Circular dependency detected: {', '.join(problematic_nodes or unprocessed)}")
        return processed

    @backoff.on_exception(backoff.expo, (psycopg2.Error, ValueError), max_tries=3, logger=logger)
    def lookup_value(self, target_cursor, table: str, key_column: str, value_column: str, key_value: Any) -> Optional[Any]:
        if key_value is None or (isinstance(key_value, str) and not key_value.strip()):
            return None
        cached_value = self.lookup_cache.get(table, key_column, key_value, value_column)
        if cached_value is not None:
            return cached_value
        try:
            query = f'SELECT "{value_column}" FROM "{table}" WHERE "{key_column}" = %s'
            target_cursor.execute(query, (key_value,))
            result = target_cursor.fetchone()
            value = result[0] if result else None
            self.lookup_cache.set(table, key_column, key_value, value_column, value)
            return value
        except psycopg2.Error as e:
            logger.error(f"Database error during lookup in {table} for {key_column}={key_value}: {e}")
            raise

    def transform_value(self, mapping: dict, source_value: Any, target_cursor) -> Any:
        if source_value is None:
            return None
        if isinstance(source_value, str):
            processed_value = source_value.strip()
            if not processed_value:
                return None
        else:
            processed_value = source_value
        transform_type = mapping.get('type', 'direct')
        if transform_type == 'direct':
            return processed_value
        elif transform_type == 'lookup':
            try:
                return self.lookup_value(
                    target_cursor, mapping['lookup_table'].lower(), mapping['lookup_key'].lower(),
                    mapping['lookup_value'].lower(), processed_value
                )
            except Exception as e:
                logger.warning(f"Lookup failed for {mapping.get('lookup_table', 'N/A')}: {e}")
                return None
        else:
            raise ValueError(f"Unknown transformation type: {transform_type}")

    def process_row(self, row, valid_mappings, source_pk_col, target_conflict_key_name, table_name):
        """Process a single row for transformation and hashing (CPU-bound)."""
        try:
            source_pk_value = row.get(source_pk_col)
            transformed_row = {}
            target_conflict_key_value = None
            has_transform_error = False
            with self.get_target_connection() as temp_conn:
                with temp_conn.cursor() as temp_cursor:
                    for source_col, mapping in valid_mappings.items():
                        try:
                            source_value = row.get(source_col)
                            transformed_value = self.transform_value(mapping, source_value, temp_cursor)
                            target_col_name = mapping['target']
                            transformed_row[target_col_name] = transformed_value
                            if target_col_name == target_conflict_key_name:
                                target_conflict_key_value = transformed_value
                        except Exception as e:
                            logger.error(f"[{table_name}] Transform error for '{source_col}' (PK {source_pk_value}): {e}")
                            transformed_row[mapping['target']] = None
                            has_transform_error = True
            if has_transform_error or target_conflict_key_value is None:
                return None, source_pk_value
            row_hash = self.calculate_row_hash(transformed_row)
            return (transformed_row, target_conflict_key_value, row_hash), source_pk_value
        except Exception as e:
            logger.error(f"[{table_name}] Row error (PK {row.get(source_pk_col)}): {e}")
            return None, row.get(source_pk_col)
    
    def process_table(self, table_name: str):
        """Process a single table with parallelized row transformations using ThreadPoolExecutor."""
        config = self.table_configs[table_name]
        with self.processed_tables_lock:
            if table_name in self.processed_tables:
                logger.info(f"[{table_name}] Table already processed. Skipping.")
                return
            unmet_deps = [dep for dep in config.dependencies if dep not in self.processed_tables]
            if unmet_deps:
                logger.warning(f"[{table_name}] Waiting for dependencies: {unmet_deps}")
                while any(dep not in self.processed_tables for dep in config.dependencies):
                    time.sleep(0.5)

        logger.info(f"--- [{table_name}] Starting processing ---")
        process_start_time = time.time()
        log_id = -1
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'deleted_rows': 0, 'error_rows': 0}
        current_target_hashes = {}

        try:
            with self.get_source_connection() as source_conn, self.get_target_connection() as target_conn:
                source_cursor = source_conn.cursor(cursor_factory=RealDictCursor)
                target_cursor = target_conn.cursor()
                log_id = self.log_sync_start(target_cursor, table_name)
                target_conn.commit()

                available_source_columns = self.get_source_columns(config.source_table)
                columns_to_select = []
                valid_mappings = {}
                for source_col, mapping in config.mappings.items():
                    if source_col in available_source_columns:
                        columns_to_select.append(source_col)
                        valid_mappings[source_col] = mapping
                    else:
                        logger.warning(f"[{table_name}] Mapped column '{source_col}' not found.")
                if config.primary_key not in columns_to_select and config.primary_key in available_source_columns:
                    columns_to_select.append(config.primary_key)
                if not columns_to_select:
                    raise ValueError(f"[{table_name}] No valid mapped columns found.")

                last_sync_time, last_target_hashes = self.get_last_sync_info(target_cursor, table_name)
                select_cols_str = ', '.join([f'"{col}"' for col in columns_to_select])
                source_query = f'SELECT {select_cols_str} FROM "{config.source_table}"'
                source_cursor.execute(source_query)

                total_fetched_count = 0
                batch_fetch_num = 0
                max_thread_workers = min(os.cpu_count() or 4, 8)  # Increased for I/O-bound tasks

                while True:
                    batch_fetch_num += 1
                    fetch_start_time = time.time()
                    source_rows = source_cursor.fetchmany(config.batch_size)
                    fetch_duration = time.time() - fetch_start_time

                    if not source_rows:
                        logger.info(f"[{table_name}] No more rows. Total fetched: {total_fetched_count}.")
                        break

                    rows_in_this_fetch = len(source_rows)
                    total_fetched_count += rows_in_this_fetch
                    logger.info(f"[{table_name}] Fetched batch #{batch_fetch_num} with {rows_in_this_fetch} rows in {fetch_duration:.2f}s.")
                    setup_logging()

                    # Parallelize row processing with ThreadPoolExecutor
                    batch_upsert_candidates = {}
                    with ThreadPoolExecutor(max_workers=max_thread_workers) as executor:
                        futures = [
                            executor.submit(
                                self.process_row, row, valid_mappings, config.primary_key,
                                config.get_target_primary_key(), table_name
                            )
                            for row in source_rows
                        ]
                        for future in as_completed(futures):
                            result, source_pk_value = future.result()
                            if result:
                                transformed_row, target_pk_value, row_hash = result
                                batch_upsert_candidates[target_pk_value] = (transformed_row, row_hash)
                            else:
                                stats['error_rows'] += 1

                    rows_to_upsert = []
                    for target_pk_value, (row, row_hash) in batch_upsert_candidates.items():
                        stats['total_rows'] += 1
                        str_pk_value = str(target_pk_value)
                        current_target_hashes[str_pk_value] = row_hash
                        last_hash = last_target_hashes.get(str_pk_value)
                        if last_hash is None:
                            stats['new_rows'] += 1
                            rows_to_upsert.append(row)
                        elif last_hash != row_hash:
                            stats['updated_rows'] += 1
                            rows_to_upsert.append(row)

                    if rows_to_upsert:
                        upsert_start_time = time.time()
                        self.batch_upsert_rows(target_cursor, config, rows_to_upsert, stats)
                        target_conn.commit()
                        logger.info(f"[{table_name}] Upserted {len(rows_to_upsert)} rows in {time.time() - upsert_start_time:.2f}s.")

                current_sync_time = datetime.now()
                self.update_sync_info(target_cursor, table_name, current_sync_time, current_target_hashes)
                target_conn.commit()

                if log_id != -1:
                    self.log_sync_end(target_cursor, log_id, table_name, stats, process_start_time)
                    target_conn.commit()

                logger.info(f"--- [{table_name}] FINISHED: {stats['total_rows']} rows, {stats['new_rows']} new, {stats['updated_rows']} updated, {stats['error_rows']} errors. Duration: {time.time() - process_start_time:.2f}s")

        except Exception as e:
            logger.exception(f"[{table_name}] CRITICAL ERROR: {e}")
            if 'target_conn' in locals() and not target_conn.closed:
                target_conn.rollback()
            if log_id != -1:
                with self.get_target_connection() as target_conn_err:
                    with target_conn_err.cursor() as target_cursor_err:
                        self.log_sync_end(target_cursor_err, log_id, table_name, stats, process_start_time, "FAILED", str(e))
                        target_conn_err.commit()
            raise

        finally:
            if 'source_cursor' in locals() and not source_cursor.closed:
                source_cursor.close()
            if 'target_cursor' in locals() and not target_cursor.closed:
                target_cursor.close()
            with self.processed_tables_lock:
                self.processed_tables.add(table_name)
    
    
    def batch_upsert_rows(self, target_cursor, config: TableConfig, rows: List[Dict[str, Any]], stats: Dict[str, int]):
        """Batch upsert rows into the target PostgreSQL table using execute_values."""
        if not rows:
            return

        target_pk = config.get_target_primary_key()
        if not target_pk:
            raise ValueError(f"Cannot upsert for {config.target_table}: Target PK undetermined.")

        columns = sorted(rows[0].keys())
        column_names_quoted = ', '.join([f'"{col}"' for col in columns])
        target_table_quoted = f'"{config.target_table}"'
        target_pk_quoted = f'"{target_pk}"'
        update_sets = [f'"{col}" = EXCLUDED."{col}"' for col in columns if col.lower() != target_pk.lower()]
        update_clause = "DO NOTHING" if not update_sets else f"DO UPDATE SET {', '.join(update_sets)}"
        upsert_query = f"""
            INSERT INTO {target_table_quoted} ({column_names_quoted})
            VALUES %s
            ON CONFLICT ({target_pk_quoted})
            {update_clause};
        """

        values_list = []
        for row in rows:
            try:
                values_list.append(tuple(row.get(col) for col in columns))
            except Exception as e:
                logger.error(f"Error preparing row for upsert: {row}. Error: {e}")
                stats['error_rows'] += 1
                continue

        if not values_list:
            logger.warning(f"[{config.target_table}] No valid rows to upsert.")
            return

        try:
            psycopg2.extras.execute_values(
                target_cursor, upsert_query, values_list, page_size=min(len(values_list), 5000)
            )
        except psycopg2.Error as e:
            logger.error(f"Database error during upsert into {config.target_table}: {e}")
            for i, row_vals in enumerate(values_list[:5]):
                logger.error(f"Problem row {i+1}: {dict(zip(columns, row_vals))}")
            raise
    
    def run_etl_cycle(self):
        """Run ETL cycle with optimized worker count."""
        cycle_start_time = time.time()
        logger.info(f"Starting ETL cycle at {datetime.now()}")
        self.lookup_cache.clear()
        self.processed_tables.clear()

        table_order = self.resolve_table_order()
        max_workers = min(len(table_order), os.cpu_count() or 4, 6)
        failed_tables = set()

        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="ETLWorker") as executor:
            futures = {executor.submit(self.process_table, table_name): table_name for table_name in table_order}
            for future in as_completed(futures):
                table_name = futures[future]
                try:
                    future.result()
                    logger.info(f"SUCCESSFULLY completed table: {table_name}")
                except Exception as e:
                    failed_tables.add(table_name)
                    logger.exception(f"FAILED table '{table_name}': {e}")

        if failed_tables:
            logger.error(f"ETL cycle failed for tables: {', '.join(sorted(failed_tables))}")
        logger.info(f"ETL cycle finished. Duration: {time.time() - cycle_start_time:.2f}s")
        self.lookup_cache.clear()

if __name__ == "__main__":
    # Unchanged main block
    logger.info("Starting ETL Script for a single run...")
    exit_code = 0
    try:
        with open('db_config.json', 'r') as f:
            db_config = json.load(f)
        with open('etl_config.json', 'r') as f:
            etl_config = json.load(f)
        if 'source_postgres' not in db_config or 'target_postgres' not in db_config:
            raise ValueError("db_config.json must contain 'source_postgres' and 'target_postgres' sections.")
        if 'tables' not in etl_config:
            raise ValueError("etl_config.json must contain a 'tables' section.")
        etl_manager = ETLManager(db_config, etl_config)
        etl_manager.run_etl_cycle()
        logger.info("ETL cycle finished.")
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        exit_code = 1
    except (ValueError, KeyError) as e:
        logger.error(f"Configuration Error: {e}")
        exit_code = 1
    except Exception as e:
        logger.exception(f"ETL script error: {e}")
        exit_code = 1
    finally:
        logger.info("ETL script finished." if exit_code == 0 else f"ETL script failed (exit code {exit_code}).")
        exit(exit_code)