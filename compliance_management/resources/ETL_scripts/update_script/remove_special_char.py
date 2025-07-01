#!/usr/bin/env python3
import re
import configparser
import psycopg2
import logging
import sys
import time
import os
import signal
import multiprocessing
from multiprocessing import Pool, Value, Lock
from typing import Dict, List, Tuple, Optional, Any
from psycopg2.extensions import connection
from psycopg2 import sql
import argparse


# Configure logging

def setup_logging():
    # log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Logger.log') #log in the same dir
    log_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'UpdateScript.log') #log in the parent dir
    
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
    return logging.getLogger('__SPECIAL_CHAR_CLEANER__')

logger = setup_logging()


# Default configuration file name
DEFAULT_CONFIG_FILE = "/home/lebechinovaji/odoo/ETL_script/update_script/settings.conf"

# Shared counter for progress tracking
processed_rows = Value('i', 0)
updated_rows = Value('i', 0)
counter_lock = Lock()

def load_config(config_path: str) -> Tuple[Dict, Dict[str, List[str]], int, int, int]:
    """
    Load database credentials and table/column specifications from config file
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Tuple containing database config, table/column mapping, batch size, lock timeout, and num workers
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    
    # Extract database credentials
    db_config = {
        'host': config['database']['host'],
        'port': config['database']['port'],
        'username': config['database']['user'],
        'password': config['database']['password'],
        'database': config['database']['dbname']
    }
    
    # Extract tables and their columns
    tables_config = {}
    for table, columns in config['tables'].items():
        tables_config[table] = [col.strip() for col in columns.split(',')]
    
    # Extract settings with defaults
    settings = config['settings'] if 'settings' in config else {}
    batch_size = int(settings.get('batch_size', 1000))
    lock_timeout = int(settings.get('lock_timeout', 5000))
    num_workers = int(settings.get('num_workers', 1))
    
    # Cap workers to CPU count
    max_workers = multiprocessing.cpu_count()
    if num_workers > max_workers:
        logger.warning(f"Requested {num_workers} workers exceeds CPU count ({max_workers}). Capping at {max_workers}.")
        num_workers = max_workers
        
    return db_config, tables_config, batch_size, lock_timeout, num_workers

def connect_to_db(db_config: Dict) -> connection:
    """
    Connect to the database using the provided credentials
    
    Args:
        db_config: Database configuration dictionary
        
    Returns:
        Database connection object
    """
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['username'],
            password=db_config['password'],
            dbname=db_config['database']
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def remove_special_chars(text: str) -> str:
    """
    Remove special characters from text, keeping only alphanumeric, spaces,
    and some basic punctuation. Excludes parentheses and exclamation marks.
    
    Args:
        text: Input text to clean
        
    Returns:
        Cleaned text
    """
    if text is None:
        return None
    
    # Keep alphanumeric characters, spaces, and allowed punctuation
    # Note: Removed () and ! from the allowed characters
    return re.sub(r'[^\w\s.,;:?-]', '', text)

def get_primary_key_column(conn: connection, table: str) -> Optional[str]:
    """
    Get the primary key column name for a table
    
    Args:
        conn: Database connection
        table: Table name
        
    Returns:
        Primary key column name or None if not found
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass
            AND i.indisprimary;
        """, (table,))
        result = cursor.fetchone()
        return result[0] if result else 'id'  # Default to 'id' if no PK found
    except Exception as e:
        logger.warning(f"Could not determine primary key for table {table}: {e}")
        return 'id'  # Default to 'id'
    finally:
        cursor.close()

def get_table_metadata(conn: connection, table: str) -> Tuple[str, int, int, int]:
    """
    Get metadata about a table for processing
    
    Args:
        conn: Database connection
        table: Table name
        
    Returns:
        Tuple of (primary_key, total_rows, min_pk, max_pk)
    """
    cursor = conn.cursor()
    try:
        # Get primary key
        pk_column = get_primary_key_column(conn, table)
        
        # Get total rows
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total_rows = cursor.fetchone()[0]
        
        # Get min and max primary key values
        cursor.execute(
            sql.SQL("SELECT MIN({}), MAX({}) FROM {}").format(
                sql.Identifier(pk_column),
                sql.Identifier(pk_column),
                sql.Identifier(table)
            )
        )
        min_pk, max_pk = cursor.fetchone()
        
        if min_pk is None:
            min_pk = 0
        if max_pk is None:
            max_pk = 0
            
        return pk_column, total_rows, min_pk, max_pk
    finally:
        cursor.close()

def process_batch(args: Dict[str, Any]) -> int:
    """
    Process a single batch of rows
    
    Args:
        args: Dictionary containing batch parameters
        
    Returns:
        Number of rows updated in this batch
    """
    db_config = args['db_config']
    table = args['table']
    column = args['column']
    min_pk = args['min_pk']
    max_pk = args['max_pk']
    pk_column = args['pk_column']
    lock_timeout = args['lock_timeout']
    worker_id = args['worker_id']
    total_rows = args['total_rows']
    
    process_name = f"Worker-{worker_id}"
    multiprocessing.current_process().name = process_name
    
    logger.info(f"{process_name}: Processing batch for column '{column}' in table '{table}' "
                f"(PK range: {min_pk} to {max_pk})")
    
    # Connect to database (each worker needs its own connection)
    conn = None
    try:
        conn = connect_to_db(db_config)
        conn.autocommit = False
        
        cursor = conn.cursor()
        
        # Set application name for better visibility in pg_stat_activity
        cursor.execute(f"SET application_name = 'special_char_cleaner_{worker_id}'")
        
        # Set lock timeout for this connection
        cursor.execute(f"SET lock_timeout = {lock_timeout}")
        
        # Modified regex pattern to also find records with parentheses or exclamation marks
        cursor.execute(
            sql.SQL("""
            SELECT {}, {} 
            FROM {} 
            WHERE {} BETWEEN %s AND %s
            AND {} IS NOT NULL
            AND ({}::text ~ '[^\\w\\s.,;:?-]' OR {}::text ~ '[\\(\\)!]')
            ORDER BY {}
            FOR UPDATE SKIP LOCKED
            """).format(
                sql.Identifier(pk_column),
                sql.Identifier(column),
                sql.Identifier(table),
                sql.Identifier(pk_column),
                sql.Identifier(column),
                sql.Identifier(column),
                sql.Identifier(column),
                sql.Identifier(pk_column)
            ),
            (min_pk, max_pk)
        )
        
        rows_to_update = cursor.fetchall()
        batch_updated = 0
        
        # Update progress counter
        with counter_lock:
            processed_rows.value += len(rows_to_update)
            progress = (processed_rows.value / total_rows) * 100 if total_rows > 0 else 0
            
        if rows_to_update:
            logger.info(f"{process_name}: Found {len(rows_to_update)} rows to clean "
                        f"({progress:.1f}% of total table processed)")
        
        # Process each row
        for row in rows_to_update:
            pk_value, text_value = row
            cleaned_value = remove_special_chars(text_value)
            
            if cleaned_value != text_value:
                cursor.execute(
                    sql.SQL("UPDATE {} SET {} = %s WHERE {} = %s").format(
                        sql.Identifier(table),
                        sql.Identifier(column),
                        sql.Identifier(pk_column)
                    ),
                    (cleaned_value, pk_value)
                )
                batch_updated += 1
        
        # Commit this batch
        conn.commit()
        
        # Update total updated counter
        with counter_lock:
            updated_rows.value += batch_updated
            
        logger.info(f"{process_name}: Updated {batch_updated} rows "
                    f"({updated_rows.value} total rows updated so far)")
        
        return batch_updated
        
    except psycopg2.Error as e:
        logger.error(f"{process_name}: Database error processing batch: {e}")
        if conn:
            conn.rollback()
        return 0
    except Exception as e:
        logger.error(f"{process_name}: Error processing batch: {e}")
        if conn:
            conn.rollback()
        return 0
    finally:
        if conn:
            conn.close()

def clean_column_with_workers(
    db_config: Dict,
    table: str,
    column: str,
    batch_size: int,
    pk_column: str,
    min_pk: int,
    max_pk: int,
    lock_timeout: int,
    num_workers: int,
    total_rows: int
) -> int:
    """
    Clean a column using multiple worker processes
    
    Args:
        db_config: Database configuration
        table: Table name
        column: Column name to clean
        batch_size: Size of each batch
        pk_column: Primary key column name
        min_pk: Minimum primary key value
        max_pk: Maximum primary key value
        lock_timeout: Lock timeout in milliseconds
        num_workers: Number of worker processes to use
        total_rows: Total rows in table for progress reporting
        
    Returns:
        Total number of rows updated
    """
    # Reset counters
    with counter_lock:
        processed_rows.value = 0
        updated_rows.value = 0
    
    # If data range is small, use fewer workers
    pk_range = max_pk - min_pk + 1
    effective_workers = min(num_workers, max(1, pk_range // batch_size))
    
    if effective_workers < num_workers:
        logger.info(f"Reducing workers from {num_workers} to {effective_workers} "
                    f"based on data size ({pk_range} rows)")
    
    if effective_workers == 1:
        # For small tables, just process directly without multiprocessing overhead
        args = {
            'db_config': db_config,
            'table': table,
            'column': column,
            'min_pk': min_pk,
            'max_pk': max_pk,
            'pk_column': pk_column,
            'lock_timeout': lock_timeout,
            'worker_id': 0,
            'total_rows': total_rows
        }
        return process_batch(args)
    
    # Calculate batch ranges for workers
    pk_range_per_worker = (max_pk - min_pk + 1) // effective_workers
    batch_ranges = []
    
    for i in range(effective_workers):
        batch_min = min_pk + (i * pk_range_per_worker)
        batch_max = min_pk + ((i + 1) * pk_range_per_worker) - 1 if i < effective_workers - 1 else max_pk
        
        # Further divide into sub-batches if needed
        if batch_max - batch_min + 1 > batch_size:
            current = batch_min
            while current <= batch_max:
                sub_max = min(current + batch_size - 1, batch_max)
                batch_ranges.append((current, sub_max, i))
                current = sub_max + 1
        else:
            batch_ranges.append((batch_min, batch_max, i))
    
    # Create task arguments
    tasks = []
    for i, (batch_min, batch_max, worker_id) in enumerate(batch_ranges):
        tasks.append({
            'db_config': db_config,
            'table': table,
            'column': column,
            'min_pk': batch_min,
            'max_pk': batch_max,
            'pk_column': pk_column,
            'lock_timeout': lock_timeout,
            'worker_id': worker_id,
            'total_rows': total_rows
        })
    
    # Define signal handler for clean termination
    original_sigint_handler = signal.getsignal(signal.SIGINT)
    
    def sigint_handler(sig, frame):
        logger.info("Received interrupt, terminating workers...")
        if 'pool' in locals():
            pool.terminate()
            pool.join()
        signal.signal(signal.SIGINT, original_sigint_handler)
        sys.exit(1)
    
    signal.signal(signal.SIGINT, sigint_handler)
    
    # Process batches using a worker pool
    try:
        with Pool(processes=effective_workers) as pool:
            results = pool.map(process_batch, tasks)
            return sum(results)
    finally:
        # Restore original signal handler
        signal.signal(signal.SIGINT, original_sigint_handler)

def clean_table_columns(
    db_config: Dict,
    table: str,
    columns: List[str],
    batch_size: int,
    lock_timeout: int,
    num_workers: int
) -> int:
    """
    Clean special characters from specified columns in a table
    
    Args:
        db_config: Database configuration
        table: Table name
        columns: List of column names to clean
        batch_size: Number of rows to process per batch
        lock_timeout: Lock timeout in milliseconds
        num_workers: Number of worker processes to use
        
    Returns:
        Number of rows updated
    """
    # Connect to verify table and columns
    conn = connect_to_db(db_config)
    try:
        cursor = conn.cursor()
        
        # Check if table exists
        try:
            cursor.execute(f"SELECT 1 FROM {table} LIMIT 1")
        except psycopg2.Error:
            logger.error(f"Table '{table}' does not exist")
            return 0
        
        # Verify columns
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table,))
        existing_columns = [col[0] for col in cursor.fetchall()]
        
        valid_columns = []
        for col in columns:
            if col not in existing_columns:
                logger.warning(f"Column '{col}' does not exist in table '{table}'")
            else:
                valid_columns.append(col)
        
        if not valid_columns:
            logger.warning(f"No valid columns to clean in table '{table}'")
            return 0
        
        # Get table metadata
        pk_column, total_rows, min_pk, max_pk = get_table_metadata(conn, table)
        logger.info(f"Table '{table}': Using '{pk_column}' as primary key, {total_rows} total rows, "
                    f"PK range: {min_pk} to {max_pk}")
        
        # For each column, update with clean text using workers
        total_updated = 0
        for column in valid_columns:
            logger.info(f"Starting to clean column '{column}' in table '{table}'")
            start_time = time.time()
            
            col_updates = clean_column_with_workers(
                db_config, table, column, batch_size, pk_column, 
                min_pk, max_pk, lock_timeout, num_workers, total_rows
            )
            
            elapsed_time = time.time() - start_time
            logger.info(f"Completed cleaning column '{column}' in table '{table}': "
                        f"{col_updates} rows updated in {elapsed_time:.2f} seconds")
            total_updated += col_updates
        
        return total_updated
            
    except Exception as e:
        logger.error(f"Error cleaning table '{table}': {e}")
        raise
    finally:
        conn.close()

# def main():
#     """
#     Main function to coordinate the database cleaning process
#     """
#     # Parse command line arguments
#     if len(sys.argv) > 2:
#         print("Usage: python clean_special_chars.py [config_file]")
#         sys.exit(1)
        
#     config_path = sys.argv[1] if len(sys.argv) == 2 else DEFAULT_CONFIG_FILE
    
#     try:
#         logger.info(f"Starting database special character cleaner")
#         logger.info(f"Loading configuration from {config_path}")
        
#         # Load configuration
#         db_config, tables_config, batch_size, lock_timeout, num_workers = load_config(config_path)
        
#         logger.info(f"Configuration loaded successfully")
#         logger.info(f"Using {num_workers} worker process(es)")
#         logger.info(f"Batch size: {batch_size}")
#         logger.info(f"Lock timeout: {lock_timeout}ms")
#         logger.info(f"Removing special characters including parentheses and exclamation marks")
        
#         # Process tables
#         total_cleaned = 0
#         start_time = time.time()
        
#         for table, columns in tables_config.items():
#             logger.info(f"Processing table '{table}' with columns: {columns}")
            
#             try:
#                 table_start_time = time.time()
#                 rows_updated = clean_table_columns(
#                     db_config, table, columns, batch_size, lock_timeout, num_workers
#                 )
#                 table_elapsed_time = time.time() - table_start_time
                
#                 logger.info(f"Updated {rows_updated} rows in table '{table}' in {table_elapsed_time:.2f} seconds")
#                 total_cleaned += rows_updated
                
#             except Exception as e:
#                 logger.error(f"Failed to process table '{table}': {e}")
        
#         total_elapsed_time = time.time() - start_time
#         logger.info(f"Cleaning complete. Total rows updated: {total_cleaned} in {total_elapsed_time:.2f} seconds")
        
#     except FileNotFoundError:
#         logger.error(f"Configuration file not found: {config_path}")
#         print(f"Configuration file not found: {config_path}")
#         print(f"Please create a configuration file named '{DEFAULT_CONFIG_FILE}' or specify a path.")
#         sys.exit(1)
#     except Exception as e:
#         logger.error(f"Error during script execution: {e}")
#         sys.exit(1)

def main():
    """
    Main function to coordinate the database cleaning process
    """
    # Parse command line arguments using argparse
    parser = argparse.ArgumentParser(description='Clean special characters from database columns')
    parser.add_argument('--config', dest='config_path', default=DEFAULT_CONFIG_FILE,
                        help=f'Path to configuration file (default: {DEFAULT_CONFIG_FILE})')
    args = parser.parse_args()
    
    config_path = args.config_path
    
    try:
        logger.info(f"Starting database special character cleaner")
        logger.info(f"Loading configuration from {config_path}")
        
        # Load configuration
        db_config, tables_config, batch_size, lock_timeout, num_workers = load_config(config_path)
        
        logger.info(f"Configuration loaded successfully")
        logger.info(f"Using {num_workers} worker process(es)")
        logger.info(f"Batch size: {batch_size}")
        logger.info(f"Lock timeout: {lock_timeout}ms")
        logger.info(f"Removing special characters including parentheses and exclamation marks")
        
        # Process tables
        total_cleaned = 0
        start_time = time.time()
        
        for table, columns in tables_config.items():
            logger.info(f"Processing table '{table}' with columns: {columns}")
            
            try:
                table_start_time = time.time()
                rows_updated = clean_table_columns(
                    db_config, table, columns, batch_size, lock_timeout, num_workers
                )
                table_elapsed_time = time.time() - table_start_time
                
                logger.info(f"Updated {rows_updated} rows in table '{table}' in {table_elapsed_time:.2f} seconds")
                total_cleaned += rows_updated
                
            except Exception as e:
                logger.error(f"Failed to process table '{table}': {e}")
        
        total_elapsed_time = time.time() - start_time
        logger.info(f"Cleaning complete. Total rows updated: {total_cleaned} in {total_elapsed_time:.2f} seconds")
        
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        print(f"Configuration file not found: {config_path}")
        print(f"Please create a configuration file named '{DEFAULT_CONFIG_FILE}' or specify a path.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during script execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Set multiprocessing start method
    # 'spawn' is safer but slower, 'fork' is faster but can have issues with threaded libraries
    try:
        multiprocessing.set_start_method('spawn')
    except RuntimeError:
        # Already set, ignore
        pass
        
    main()
    