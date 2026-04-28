#!/usr/bin/env python3
import psycopg2
import logging
import time
import argparse
import configparser
import os
import re
from datetime import datetime

DEFAULT_CONFIG_FILE = "/data/odoo/ETL_script/update_script/settings.conf"

# Configure logging
def setup_logging():
    
    # log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Logger.log') #log in the same dir
    log_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'UpdateScript.log') #log in the parent dir    # Check if file exists and exceeds size limit
    if os.path.exists(log_file) and os.path.getsize(log_file) >= 30 * 1024 * 1024:
        # Empty the file
        open(log_file, 'w').close()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8')
        ]
    )
    return logging.getLogger("__DELETE_UPDATE_SCRIPT__")

def connect_to_db(db_config):
    """Establish connection to PostgreSQL database using configuration"""
    try:
        conn = psycopg2.connect(
            host=db_config.get('host', 'localhost'),
            port=db_config.get('port', '5432'),
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password']
        )
        return conn
    except Exception as e:
        raise ConnectionError(f"Error connecting to database: {e}")

def batch_operation(conn, operation_config, logger, batch_size=1000, sleep_time=1):
    """Perform batch operations (UPDATE or DELETE) and log progress"""
    operation_type = operation_config['type'].upper()
    table_name = operation_config['table']
    condition = operation_config['condition']
    operation_name = operation_config.get('name', f"{operation_type}_{table_name}")
    
    if operation_type not in ('UPDATE', 'DELETE'):
        raise ValueError(f"Unsupported operation type: {operation_type}")
    
    # For UPDATE operations, we need additional fields
    set_clause = operation_config.get('set_clause', '')
    if operation_type == 'UPDATE' and not set_clause:
        raise ValueError(f"SET clause is required for UPDATE operation: {operation_name}")
    
    logger.info(f"Starting batch {operation_type} ({operation_name}) on {table_name} where {condition}")
    
    total_processed = 0
    start_time = time.time()
    
    try:
        with conn.cursor() as cursor:
            # First, get total count of records to be processed
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {condition}")
            total_records = cursor.fetchone()[0]
            logger.info(f"Found {total_records} records to {operation_type} in {table_name}")
            
            if total_records == 0:
                logger.info(f"No records to {operation_type}. Skipping operation {operation_name}.")
                return 0
            
            # Process in batches
            while True:
                if operation_type == 'DELETE':
                    operation_query = f"""
                    WITH rows AS (
                        SELECT ctid FROM {table_name} 
                        WHERE {condition}
                        LIMIT {batch_size}
                    )
                    DELETE FROM {table_name} 
                    WHERE ctid IN (SELECT ctid FROM rows)
                    RETURNING ctid
                    """
                else:  # UPDATE
                    operation_query = f"""
                    WITH rows AS (
                        SELECT ctid FROM {table_name} 
                        WHERE {condition}
                        LIMIT {batch_size}
                    )
                    UPDATE {table_name} 
                    SET {set_clause}
                    WHERE ctid IN (SELECT ctid FROM rows)
                    RETURNING ctid
                    """
                
                cursor.execute(operation_query)
                processed_count = cursor.rowcount
                conn.commit()
                
                if processed_count == 0:
                    break
                    
                total_processed += processed_count
                progress_percent = (total_processed / total_records) * 100 if total_records > 0 else 100
                logger.info(f"{operation_name}: {operation_type}D batch of {processed_count} records. Total: {total_processed}/{total_records} ({progress_percent:.2f}%)")
                
                if processed_count < batch_size:
                    # No more records to process
                    break
                    
                # Sleep to reduce database load
                time.sleep(sleep_time)
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Error during {operation_name}: {e}")
        raise
        
    execution_time = time.time() - start_time
    logger.info(f"{operation_name} completed. Total records processed: {total_processed}")
    logger.info(f"{operation_name} execution time: {execution_time:.2f} seconds")
    
    return total_processed

def load_config(config_file):
    """Load configuration from .conf file with support for multiple operations"""
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
        
    config = configparser.ConfigParser()
    config.read(config_file)
    
    # Validate database section
    if 'database' not in config:
        raise ValueError("Missing required section in config file: database")
    
    # Validate database configuration
    db_required = ['dbname', 'user', 'password']
    for option in db_required:
        if option not in config['database']:
            raise ValueError(f"Missing required database option in config file: {option}")
    
    # Find all operation sections (they should be named operation_1, operation_2, etc. or just 'operation')
    operation_sections = [section for section in config.sections() if section == 'operation' or re.match(r'operation_\d+', section)]
    
    if not operation_sections:
        raise ValueError("No operation sections found in config file. Expected 'operation' or 'operation_X' sections.")
    
    # Validate each operation section
    for section in operation_sections:
        op_required = ['type', 'table', 'condition']
        for option in op_required:
            if option not in config[section]:
                raise ValueError(f"Missing required option '{option}' in section '{section}'")
        
        # For UPDATE operations, validate SET clause
        if config[section]['type'].upper() == 'UPDATE' and 'set_clause' not in config[section]:
            raise ValueError(f"Missing 'set_clause' for UPDATE operation in section '{section}'")
    
    return config, operation_sections

def main():
    parser = argparse.ArgumentParser(description='Batch operations (UPDATE/DELETE) for PostgreSQL')
    parser.add_argument('--config', required=False, default=DEFAULT_CONFIG_FILE, help='Path to configuration file')
    parser.add_argument('--batch-size', type=int, help='Number of records to process in each batch')
    parser.add_argument('--sleep', type=float, help='Sleep time between batches in seconds')
    parser.add_argument('--operations', help='Comma-separated list of operation names to run (empty=all)')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    try:
        # Load configuration from file
        config, operation_sections = load_config(args.config)
        
        # Extract requested operations if specified
        requested_operations = None
        if args.operations:
            requested_operations = [op.strip() for op in args.operations.split(',')]
        
        # Connect to database
        logger.info("Connecting to database...")
        conn = connect_to_db(dict(config['database']))
        logger.info("Database connection established")
        
        # Track overall statistics
        total_operations = 0
        total_records_processed = 0
        overall_start_time = time.time()
        
        # Execute each batch operation
        for section in operation_sections:
            operation_config = dict(config[section])
            operation_name = operation_config.get('name', section)
            
            # Skip if specific operations were requested and this one isn't in the list
            if requested_operations and operation_name not in requested_operations:
                logger.info(f"Skipping operation {operation_name} (not in requested list)")
                continue
                
            # Get batch_size and sleep_time from config, override with command line args if provided
            batch_size = args.batch_size if args.batch_size is not None else int(operation_config.get('batch_size', 1000))
            sleep_time = args.sleep if args.sleep is not None else float(operation_config.get('sleep_time', 1.0))
            
            try:
                logger.info(f"Starting operation: {operation_name}")
                records_processed = batch_operation(
                    conn, 
                    operation_config,
                    logger,
                    batch_size=batch_size,
                    sleep_time=sleep_time
                )
                total_operations += 1
                total_records_processed += records_processed
                logger.info(f"Operation {operation_name} completed successfully")
            except Exception as e:
                logger.error(f"Error in operation {operation_name}: {e}")
                logger.info(f"Moving to next operation (if any)...")
        
        # Log overall summary
        overall_execution_time = time.time() - overall_start_time
        logger.info(f"All operations completed. Summary:")
        logger.info(f"Total operations executed: {total_operations}")
        logger.info(f"Total records processed: {total_records_processed}")
        logger.info(f"Total execution time: {overall_execution_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error in batch operation process: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()