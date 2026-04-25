#!/usr/bin/env python3
import configparser
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
import multiprocessing
import logging
import sys
import os
import argparse
import time
import traceback
from datetime import datetime
from contextlib import contextmanager
import psycopg2.pool

DEFAULT_CONFIG_FILE = "/data/odoo/ETL_script/update_script/settings.conf"

# Configure logging
def setup_logging():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    etl_scripts_dir = os.path.dirname(script_dir)
    log_dir = os.path.join(etl_scripts_dir, 'Logs')

    # Ensure the Logs directory exists
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, 'UpdateScript.log')
    
    # Check if file exists and exceeds size limit (30MB)
    if os.path.exists(log_file) and os.path.getsize(log_file) >= 30 * 1024 * 1024:
        # Create a backup of old log
        backup_name = f"UpdateScript_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log.bak"
        os.rename(log_file, os.path.join(log_dir, backup_name))
    
    # Configure logging to file with proper format
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
        ]
    )
    return logging.getLogger("__COMPUTE_AVERAGE_RISK_SCORE_PER_BRANCH__")

# Get database configuration
def get_db_config(config_file=DEFAULT_CONFIG_FILE):
    """Get database configuration from settings.conf."""
    try:
        # Get the directory where the script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Look for config file in the script directory
        config_path = os.path.join(script_dir, config_file)
        
        # Check if config file exists
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_file}. Expected at: {config_path}")
        
        config = configparser.ConfigParser()
        config.read(config_path)
        
        # Check if database section exists
        if 'database' not in config:
            raise KeyError(f"Missing [database] section in {config_path}. File contains sections: {config.sections()}")
        
        # Check for required fields
        required_fields = ['host', 'port', 'user', 'password', 'dbname']
        missing_fields = [field for field in required_fields if field not in config['database']]
        
        if missing_fields:
            raise ValueError(f"Missing required fields in [database] section: {missing_fields}")
        
        return dict(config['database'])
    except Exception as e:
        logger.error(f"Failed to read config file: {str(e)}")
        raise

# Connection pool for efficiency
class DatabaseConnectionPool:
    def __init__(self, db_config, min_conn=2, max_conn=10):
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=db_config['password'],
            dbname=db_config['dbname']
        )
        self.db_config = db_config

    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = self.pool.getconn()
            conn.autocommit = False
            yield conn
        except Exception as e:
            logger.error(f"Connection error: {str(e)}")
            raise
        finally:
            if conn:
                self.pool.putconn(conn)

    def close(self):
        if self.pool:
            self.pool.closeall()

# For processes that need their own connection
def get_db_connection(db_config):
    """Create a database connection using provided credentials."""
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=int(db_config['port']),
            user=db_config['user'],
            password=db_config['password'],
            dbname=db_config['dbname']
        )
        conn.autocommit = False
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to database: {str(e)}")
        raise

def process_branch_batch(task_data):
    """Process a single branch in batches and compute weighted average risk scores."""
    branch_name, branch_id, customers, db_config, batch_size = task_data
    
    # Create a new connection for this process
    conn = None
    start_time = time.time()
    total_customers = len(customers)
    
    try:
        conn = get_db_connection(db_config)
        
        # Initialize counters
        risk_counts = {'low': 0, 'medium': 0, 'high': 0}
        risk_scores = {'low': 0, 'medium': 0, 'high': 0}
        
        # Process in batches
        for i in range(0, total_customers, batch_size):
            batch = customers[i:i+batch_size]
            batch_end = min(i+batch_size, total_customers)
            logger.info(f"Processing branch '{branch_name}' batch {i+1}-{batch_end} of {total_customers}")
            
            # Process this batch
            for rec in batch:
                # Handle None values for risk_level
                risk_level = 'low'
                if rec['risk_level'] is not None:
                    risk_level = rec['risk_level'].lower()
                
                risk_counts[risk_level] = risk_counts.get(risk_level, 0) + 1
                
                # Handle None values for risk_score
                risk_score = 0.0
                if rec['risk_score'] is not None:
                    risk_score = float(rec['risk_score'])
                
                risk_scores[risk_level] += risk_score
        
        # Calculate final results
        formatted_key = f"{branch_name}({total_customers})" if total_customers > 0 else branch_name
        
        if total_customers == 0:
            weighted_avg = 0.0
        else:
            # Compute mean average per risk level
            mean_avg_low = risk_scores['low'] / risk_counts['low'] if risk_counts['low'] > 0 else 0.0
            mean_avg_medium = risk_scores['medium'] / risk_counts['medium'] if risk_counts['medium'] > 0 else 0.0
            mean_avg_high = risk_scores['high'] / risk_counts['high'] if risk_counts['high'] > 0 else 0.0
            
            weighted_avg = (
                (risk_counts['low'] * mean_avg_low) + 
                (risk_counts['medium'] * mean_avg_medium) + 
                (risk_counts['high'] * mean_avg_high)
            ) / total_customers
        
        # Store in customer_agg_risk_score
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO customer_agg_risk_score 
                (branch_id, weighted_avg_risk_score, total_customers, formatted_name)
                VALUES (%s, %s, %s, %s)
            """, (branch_id, weighted_avg, total_customers, formatted_key))
            conn.commit()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Processed branch: {branch_name}, total customers: {total_customers}, time: {elapsed_time:.2f}s")
        
        return branch_name, weighted_avg, total_customers, elapsed_time
    
    except psycopg2.Error as e:
        logger.error(f"Database error processing branch {branch_name}: {str(e)}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Error processing branch {branch_name}: {str(e)}")
        logger.error(traceback.format_exc())
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def compute_risk_scores(config_file=DEFAULT_CONFIG_FILE, batch_size=1000, max_workers=4):
    """Main function to compute and store weighted average risk scores."""
    start_time = time.time()
    logger.info("Starting risk score computation")
    
    db_config = get_db_config(config_file)
    conn = None
    
    try:
        # Create a connection
        conn = get_db_connection(db_config)
        
        # Clear existing records with proper error handling
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM customer_agg_risk_score")
                conn.commit()
            logger.info("Cleared existing risk score records")
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"Failed to clear existing records: {str(e)}")
            raise
        
        # Get branch data with retry logic
        branches = {}
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("SELECT id, name FROM res_branch")
                    branches = {row['id']: row['name'] for row in cursor.fetchall()}
                break
            except psycopg2.OperationalError as e:
                retry_count += 1
                logger.warning(f"Database operation failed, retry {retry_count}/{max_retries}: {str(e)}")
                if retry_count >= max_retries:
                    raise
                time.sleep(2)  # Wait before retrying
        
        logger.info(f"Retrieved {len(branches)} branches")
        
        # Get all customers with optimized query
        customers = []
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, branch_id, risk_level, risk_score
                    FROM res_partner
                    WHERE internal_category = 'customer'
                    AND origin IN ('demo', 'test', 'prod')
                """)
                customers = cursor.fetchall()
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"Failed to retrieve customers: {str(e)}")
            raise
        
        logger.info(f"Found {len(customers)} customers")
        
        # Group customers by branch with progress logging
        grouped_data = {}
        chunk_size = 10000
        total_chunks = (len(customers) + chunk_size - 1) // chunk_size
        
        for chunk_index in range(total_chunks):
            start_idx = chunk_index * chunk_size
            end_idx = min((chunk_index + 1) * chunk_size, len(customers))
            chunk = customers[start_idx:end_idx]
            
            for record in chunk:
                branch_id = record['branch_id']
                
                # Handle None branch_id
                if branch_id is None:
                    branch_id = 'no_branch'
                    branch_name = 'No Branch'
                else:
                    branch_name = branches.get(branch_id, 'Unknown Branch')
                
                if branch_id not in grouped_data:
                    grouped_data[branch_id] = {
                        'name': branch_name,
                        'customers': []
                    }
                grouped_data[branch_id]['customers'].append(record)
            
            if total_chunks > 1:
                logger.info(f"Grouped customers chunk {chunk_index+1}/{total_chunks} ({end_idx}/{len(customers)} records)")
        
        logger.info(f"Grouped customers into {len(grouped_data)} branches")
        
        # Get branch id for 'No Branch'
        no_branch_id = None
        if 'no_branch' in grouped_data:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id FROM res_branch WHERE name = 'No Branch' LIMIT 1")
                    result = cursor.fetchone()
                    if result:
                        no_branch_id = result[0]
            except psycopg2.Error as e:
                logger.warning(f"Failed to get 'No Branch' ID: {str(e)}")
        
        # Prepare tasks for multiprocessing
        tasks = []
        for branch_id, data in grouped_data.items():
            # Handle special case for 'no_branch'
            if branch_id == 'no_branch':
                branch_id = no_branch_id
            
            tasks.append((data['name'], branch_id, data['customers'], db_config, batch_size))
        
        # Create a pool of workers
        num_workers = min(max_workers, len(tasks))
        logger.info(f"Starting multiprocessing with {num_workers} workers")
        
        with multiprocessing.Pool(processes=num_workers) as pool:
            results = pool.map(process_branch_batch, tasks)
        
        # Calculate and log performance metrics
        total_time = time.time() - start_time
        total_processed = sum(result[2] for result in results if result)
        branches_processed = len([result for result in results if result])
        
        logger.info(f"Completed computing risk scores for all branches")
        logger.info(f"Performance summary:")
        logger.info(f"  - Total time: {total_time:.2f} seconds")
        logger.info(f"  - Branches processed: {branches_processed}")
        logger.info(f"  - Total customers processed: {total_processed}")
        if total_time > 0:
            logger.info(f"  - Processing rate: {total_processed/total_time:.2f} customers/second")
        
    except Exception as e:
        logger.error(f"Error computing risk scores: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Setup logging
    logger = setup_logging()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Compute risk scores for customers')
    parser.add_argument('--config', default='settings.conf', help='Path to config file')
    parser.add_argument('--create-config', action='store_true', help='Create a template config file if none exists')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')
    parser.add_argument('--workers', type=int, default=4, help='Number of worker processes')
    args = parser.parse_args()
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Resolve config path relative to script directory if not absolute
    if not os.path.isabs(args.config):
        config_path = os.path.join(script_dir, args.config)
    else:
        config_path = args.config
            
    try:
        # Print start information
        logger.info(f"Starting compute_average_risk_per_branch with batch size {args.batch_size} and {args.workers} workers")
        
        # Try to run with specified config file
        compute_risk_scores(args.config, args.batch_size, args.workers)
        
        logger.info("Risk score computation completed successfully")
    except FileNotFoundError as e:
        logger.error(f"ERROR: {e}")
        logger.error("\nTo create a template config file, run:")
        logger.error(f"python {os.path.basename(__file__)} --create-config --config={args.config}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"ERROR: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
