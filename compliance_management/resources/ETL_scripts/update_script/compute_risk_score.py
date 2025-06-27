#!/usr/bin/env python3
"""
Risk Score Calculator - Standalone Script
Modified version that reads customer IDs from CSV files for better performance
"""
import os
import sys
import time
import logging
import configparser
import psycopg2
import psycopg2.extras
import csv
from datetime import datetime
import glob
import math
import hashlib
import json
import argparse  # Add this import
from concurrent.futures import ThreadPoolExecutor, as_completed


def setup_logging():
    log_file = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), 'UpdateScript.log')

    # Check if file exists and exceeds size limit
    if os.path.exists(log_file) and os.path.getsize(log_file) >= 30 * 1024 * 1024:
        open(log_file, 'w').close()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8')
        ]
    )
    return logging.getLogger('__COMPUTE_RISK_SCORE__')


logger = setup_logging()

# Constants
BATCH_SIZE = 50
LOW_RISK_THRESHOLD = 3
MEDIUM_RISK_THRESHOLD = 6
HIGH_RISK_THRESHOLD = 9
DEFAULT_CONFIG_FILE = "/data/odoo/ETL_script/update_script/settings.conf"
CSV_RECORDS_PER_FILE = 100000
CSV_BASE_PATH = "/data/odoo/ETL_script/update_script/csv_data"
METADATA_FILE = "metadata.json"
DEFAULT_SCORE=3
DEFAULT_LEVEL='low'
PROCESSING_BATCH_SIZE=20000
PROCESSING_MODE='batch'


class RiskScoreCalculator:
    def __init__(self, config_path=DEFAULT_CONFIG_FILE):
        self.config = self._load_config(config_path)
        self.conn = None
        self.cursor = None
        self.total_processed = 0
        self.total_errors = 0
        self.start_time = time.time()
        
        # Ensure CSV directory exists
        self.csv_path = self.config.get('csv', 'path', fallback=CSV_BASE_PATH)
        os.makedirs(self.csv_path, exist_ok=True)
        
        # Force CSV regeneration if specified
        self.force_csv_regeneration = self.config.getboolean('csv', 'force_regeneration', fallback=False)
        
        # Processing mode
        self.processing_mode = self.config.get('processing', 'mode', fallback=PROCESSING_MODE)
        self.parallel_workers = self.config.getint('processing', 'parallel_workers', fallback=4)

    def _load_config(self, config_path):
        if not os.path.exists(config_path):
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}")

        config = configparser.ConfigParser()
        config.read(config_path)
        
        # Add default sections if not exists
        if 'csv' not in config:
            config['csv'] = {
                'path': CSV_BASE_PATH,
                'force_regeneration': 'False'
            }
        
        if 'processing' not in config:
            config['processing'] = {
                'mode': PROCESSING_MODE,  # Options: batch, bulk, parallel
                'bulk_batch_size': PROCESSING_BATCH_SIZE,
                'enable_prepared_statements': 'True',
                'enable_indexes': 'True',
                'parallel_workers': '5'
            }
            
        # Save updated config
        with open(config_path, 'w') as f:
            config.write(f)
                
        return config

    def connect_to_database(self):
        try:
            self.conn = psycopg2.connect(
                host=self.config.get('database', 'host'),
                port=self.config.get('database', 'port'),
                database=self.config.get('database', 'dbname'),
                user=self.config.get('database', 'user'),
                password=self.config.get('database', 'password')
            )
            self.conn.autocommit = False
            self.cursor = self.conn.cursor(
                cursor_factory=psycopg2.extras.DictCursor)
            logger.info("Successfully connected to the database")

            # Test write permissions by performing a simple operation
            try:
                self.cursor.execute(
                    "CREATE TEMP TABLE write_test (id INTEGER)")
                self.cursor.execute("DROP TABLE write_test")
                logger.info("Database write permissions confirmed")
            except Exception as e:
                logger.error(
                    f"Database write permission test failed: {str(e)}")

            return True
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            sys.exit(1)
            
    def check_csv_files(self):
        """
        Check if CSV files exist and are valid based on metadata
        Returns True if files are valid, False if they need to be regenerated
        """
        metadata_path = os.path.join(self.csv_path, METADATA_FILE)
        
        # If force regeneration is enabled, return False
        if self.force_csv_regeneration:
            logger.info("Forced CSV regeneration enabled")
            return False
            
        # Check if metadata file exists
        if not os.path.exists(metadata_path):
            logger.info("CSV metadata file not found")
            return False
            
        try:
            # Load metadata
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
                
            # Check file count
            expected_files = metadata.get('file_count', 0)
            csv_files = glob.glob(os.path.join(self.csv_path, 'res_partner_ids_*.csv'))
            
            if len(csv_files) != expected_files:
                logger.warning(f"CSV file count mismatch: expected {expected_files}, found {len(csv_files)}")
                return False
                
            # Verify database hash to detect schema changes
            db_hash = self._get_database_hash()
            if db_hash != metadata.get('db_hash', ''):
                logger.info("Database hash has changed, regenerating CSV files")
                return False
                
            logger.info(f"Found {expected_files} valid CSV files with {metadata.get('total_records', 0)} total records")
            return True
            
        except Exception as e:
            logger.error(f"Error checking CSV files: {str(e)}")
            return False
            
    def _get_database_hash(self):
        """
        Generate a hash of database table structure to detect schema changes
        """
        try:
            # Get table structure for res_partner
            self.cursor.execute("""
                SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns 
                WHERE table_name = 'res_partner'
                ORDER BY column_name
            """)
            columns = self.cursor.fetchall()
            
            # Create a string representation of the schema
            schema_str = str(columns)
            
            # Generate hash
            return hashlib.md5(schema_str.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Error generating database hash: {str(e)}")
            return ""
            
    def export_customer_ids(self):
        """
        Export all customer IDs from res_partner table to CSV files
        Each file contains CSV_RECORDS_PER_FILE records
        """
        try:
            # Count total customers
            self.cursor.execute("SELECT COUNT(*) FROM res_partner")
            total_customers = self.cursor.fetchone()[0]
            
            if total_customers == 0:
                logger.error("No customers found in database")
                return False
                
            # Calculate number of files needed
            file_count = math.ceil(total_customers / CSV_RECORDS_PER_FILE)
            logger.info(f"Exporting {total_customers:,} customer IDs to {file_count} CSV files")
            
            # Clear existing CSV files
            for file in glob.glob(os.path.join(self.csv_path, 'res_partner_ids_*.csv')):
                os.remove(file)
                
            # Export data in chunks
            for i in range(file_count):
                file_path = os.path.join(self.csv_path, f'res_partner_ids_{i+1}.csv')
                offset = i * CSV_RECORDS_PER_FILE
                
                logger.info(f"Exporting file {i+1}/{file_count}: {file_path}")
                
                # Get batch of customer IDs
                self.cursor.execute(f"""
                    SELECT id FROM res_partner 
                    ORDER BY id
                    LIMIT {CSV_RECORDS_PER_FILE} OFFSET {offset}
                """)
                customers = self.cursor.fetchall()
                
                # Write to CSV
                with open(file_path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['id'])  # Header
                    writer.writerows([[customer['id']] for customer in customers])
                    
            # Create metadata file
            metadata = {
                'created_at': datetime.now().isoformat(),
                'total_records': total_customers,
                'file_count': file_count,
                'records_per_file': CSV_RECORDS_PER_FILE,
                'db_hash': self._get_database_hash()
            }
            
            with open(os.path.join(self.csv_path, METADATA_FILE), 'w') as f:
                json.dump(metadata, f)
                
            logger.info(f"Successfully exported {total_customers:,} customer IDs to CSV files")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting customer IDs: {str(e)}")
            return False
            
    def get_customers_from_csv(self, batch_idx):
        """
        Read a batch of customer IDs from CSV files
        Returns a list of dictionaries with 'id' key for compatibility with existing code
        """
        try:
            # Calculate which file to read from
            file_idx = batch_idx * BATCH_SIZE // CSV_RECORDS_PER_FILE + 1
            file_path = os.path.join(self.csv_path, f'res_partner_ids_{file_idx}.csv')
            
            if not os.path.exists(file_path):
                logger.warning(f"CSV file not found: {file_path}")
                return []
                
            # Calculate position within file
            position_in_file = (batch_idx * BATCH_SIZE) % CSV_RECORDS_PER_FILE
            
            customers = []
            with open(file_path, 'r', newline='') as f:
                reader = csv.DictReader(f)
                
                # Skip to the right position
                for _ in range(position_in_file):
                    next(reader, None)
                    
                # Read batch
                count = 0
                for row in reader:
                    if count >= BATCH_SIZE:
                        break
                    customers.append({'id': int(row['id'])})
                    count += 1
                    
            return customers
            
        except Exception as e:
            logger.error(f"Error reading customer IDs from CSV: {str(e)}")
            return []

    # def process_customers_bulk(self, plans, plan_setting, batch_size=PROCESSING_BATCH_SIZE):
    #     """
    #     Process customers in bulk with PROPERLY FIXED database operations
    #     """
    #     # Read all customers from CSV files in batches
    #     batch_idx = 0
    #     total_batches_processed = 0
        
    #     while True:
    #         # Load batch_size customers
    #         all_customers = []
    #         current_batch_idx = batch_idx
            
    #         while len(all_customers) < batch_size:
    #             customers = self.get_customers_from_csv(current_batch_idx)
    #             if not customers:
    #                 break
    #             all_customers.extend([c['id'] for c in customers])
    #             current_batch_idx += 1
                
    #         if not all_customers:
    #             break
                
    #         # Process this batch
    #         batch_start_time = time.time()
    #         batch_size_actual = len(all_customers)
    #         logger.info(f"Processing bulk batch {total_batches_processed+1} with {batch_size_actual} customers")
            
    #         try:
    #             # Bulk delete existing risk plan lines
    #             self.cursor.execute("""
    #                 DELETE FROM res_partner_risk_plan_line 
    #                 WHERE partner_id = ANY(%s)
    #             """, (all_customers,))
                
    #             # Use a temporary table to store results for bulk operations
    #             self.cursor.execute("""
    #                 CREATE TEMPORARY TABLE IF NOT EXISTS temp_risk_results (
    #                     partner_id INT,
    #                     plan_id INT,
    #                     risk_score FLOAT
    #                 )
    #             """)
    #             self.cursor.execute("TRUNCATE temp_risk_results")
                
    #             # Process each risk plan for all customers at once
    #             for plan in plans:
    #                 plan_id = plan['id']
    #                 query = plan['sql_query']
    #                 risk_score = float(plan['risk_score']) if plan['risk_score'] is not None else 0
    #                 plan_start_time = time.time()
                    
    #                 # PROPERLY FIXED: Extract WHERE conditions instead of malformed transformation
    #                 try:
    #                     bulk_query = self._create_proper_bulk_query(query, plan_id, risk_score)
                        
    #                     if bulk_query:
    #                         logger.debug(f"Executing bulk query for plan {plan_id}: {bulk_query}")
    #                         self.cursor.execute(bulk_query, (all_customers,))
    #                         matched = self.cursor.rowcount
    #                         plan_time = time.time() - plan_start_time
    #                         logger.info(f"  Plan {plan_id} processed in {plan_time:.2f}s - {matched} matches")
    #                     else:
    #                         # If transformation failed, fall back to individual processing
    #                         raise ValueError("Could not transform query to bulk format")
                            
    #                 except Exception as e:
    #                     logger.warning(f"Error bulk processing plan {plan_id}: {str(e)}")
    #                     self.conn.rollback()
                        
    #                     # Fall back to individual processing for this plan - THIS WORKS!
    #                     fallback_start = time.time()
    #                     logger.info(f"  Falling back to individual processing for plan {plan_id}")
    #                     matched = 0
                        
    #                     for customer_id in all_customers:
    #                         try:
    #                             self.cursor.execute(query, (customer_id,))
    #                             result = self.cursor.fetchone()
    #                             if result is not None:
    #                                 matched += 1
    #                                 self.cursor.execute("""
    #                                     INSERT INTO temp_risk_results (partner_id, plan_id, risk_score)
    #                                     VALUES (%s, %s, %s)
    #                                 """, (customer_id, plan_id, risk_score))
    #                         except Exception as e2:
    #                             logger.warning(f"Error processing plan {plan_id} for customer {customer_id}: {str(e2)}")
                                
    #                     plan_time = time.time() - fallback_start
    #                     logger.info(f"  Plan {plan_id} fallback processing completed in {plan_time:.2f}s - {matched} matches")
                
    #             # Bulk insert risk plan lines from temp table
    #             self.cursor.execute("""
    #                 INSERT INTO res_partner_risk_plan_line (partner_id, plan_line_id, risk_score)
    #                 SELECT partner_id, plan_id, risk_score FROM temp_risk_results
    #             """)
    #             inserted = self.cursor.rowcount
    #             logger.info(f"  Inserted {inserted} risk plan lines")
                
    #             # Calculate final risk scores and update customers
    #             if plan_setting == 'avg':
    #                 self.cursor.execute("""
    #                     WITH risk_scores AS (
    #                         SELECT partner_id, AVG(risk_score) as final_score
    #                         FROM temp_risk_results
    #                         GROUP BY partner_id
    #                     ),
    #                     risk_levels AS (
    #                         SELECT partner_id, final_score,
    #                         CASE
    #                             WHEN final_score <= %s THEN 'low'
    #                             WHEN final_score <= %s THEN 'medium'
    #                             WHEN final_score <= %s THEN 'high'
    #                             ELSE 'low'
    #                         END as risk_level
    #                         FROM risk_scores
    #                     )
    #                     UPDATE res_partner p
    #                     SET risk_score = r.final_score, risk_level = r.risk_level
    #                     FROM risk_levels r
    #                     WHERE p.id = r.partner_id
    #                 """, (LOW_RISK_THRESHOLD, MEDIUM_RISK_THRESHOLD, HIGH_RISK_THRESHOLD))
    #             else:  # max
    #                 self.cursor.execute("""
    #                     WITH risk_scores AS (
    #                         SELECT partner_id, MAX(risk_score) as final_score
    #                         FROM temp_risk_results
    #                         GROUP BY partner_id
    #                     ),
    #                     risk_levels AS (
    #                         SELECT partner_id, final_score,
    #                         CASE
    #                             WHEN final_score <= %s THEN 'low'
    #                             WHEN final_score <= %s THEN 'medium'
    #                             WHEN final_score <= %s THEN 'high'
    #                             ELSE 'low'
    #                         END as risk_level
    #                         FROM risk_scores
    #                     )
    #                     UPDATE res_partner p
    #                     SET risk_score = r.final_score, risk_level = r.risk_level
    #                     FROM risk_levels r
    #                     WHERE p.id = r.partner_id
    #                 """, (LOW_RISK_THRESHOLD, MEDIUM_RISK_THRESHOLD, HIGH_RISK_THRESHOLD))
    #             updated_with_scores = self.cursor.rowcount
    #             logger.info(f"  Updated {updated_with_scores} customers with scores")
                
    #             # Set default scores for customers with no matches
    #             self.cursor.execute("""
    #                 WITH processed_customers AS (
    #                     SELECT DISTINCT partner_id FROM temp_risk_results
    #                 )
    #                 UPDATE res_partner p
    #                 SET risk_score = %s, risk_level = %s
    #                 WHERE p.id = ANY(%s)
    #                 AND p.id NOT IN (SELECT partner_id FROM processed_customers)
    #             """, (DEFAULT_SCORE, DEFAULT_LEVEL, all_customers,))
    #             updated_without_scores = self.cursor.rowcount
    #             logger.info(f"  Set {updated_without_scores} customers to default score")
                
    #             # Commit changes for this batch
    #             self.conn.commit()
    #             self.total_processed += batch_size_actual
                
    #             # Log progress
    #             batch_time = time.time() - batch_start_time
    #             logger.info(f"Completed bulk batch {total_batches_processed+1} in {batch_time:.2f}s")
    #             logger.info(f"Total processed: {self.total_processed} customers")
                
    #             # Move to next batch
    #             batch_idx = current_batch_idx
    #             total_batches_processed += 1
                
    #         except Exception as e:
    #             logger.error(f"Error processing bulk batch: {str(e)}")
    #             self.conn.rollback()
    #             self.total_errors += batch_size_actual
    #             batch_idx = current_batch_idx
                
    #     return True
    
    # def _create_proper_bulk_query(self, original_query, plan_id, risk_score):
    #     """
    #     PROPERLY create bulk query by extracting WHERE conditions correctly
    #     """
    #     try:
    #         query = original_query.strip().lower()
            
    #         # Only handle SELECT queries from res_partner
    #         if not (query.startswith('select') and 'from res_partner' in query):
    #             return None
                
    #         # Extract the WHERE clause part after the parameter
    #         if ' where id = %s' in query:
    #             # Split on the parameter part
    #             parts = query.split(' where id = %s')
    #             if len(parts) == 2:
    #                 where_conditions = parts[1].strip()
                    
    #                 # Remove leading AND if present
    #                 if where_conditions.startswith('and '):
    #                     where_conditions = where_conditions[4:].strip()
                    
    #                 # Create the bulk query
    #                 if where_conditions:
    #                     bulk_query = f"""
    #                         INSERT INTO temp_risk_results (partner_id, plan_id, risk_score)
    #                         SELECT p.id, {plan_id}, {risk_score}
    #                         FROM res_partner p
    #                         WHERE p.id = ANY(%s)
    #                         AND {where_conditions}
    #                     """
    #                 else:
    #                     # No additional conditions, just match all customers in the batch
    #                     bulk_query = f"""
    #                         INSERT INTO temp_risk_results (partner_id, plan_id, risk_score)
    #                         SELECT p.id, {plan_id}, {risk_score}
    #                         FROM res_partner p
    #                         WHERE p.id = ANY(%s)
    #                     """
    #                 return bulk_query
                    
    #         elif ' where id=%s' in query:
    #             # Handle queries without spaces around =
    #             parts = query.split(' where id=%s')
    #             if len(parts) == 2:
    #                 where_conditions = parts[1].strip()
                    
    #                 if where_conditions.startswith('and '):
    #                     where_conditions = where_conditions[4:].strip()
                    
    #                 if where_conditions:
    #                     bulk_query = f"""
    #                         INSERT INTO temp_risk_results (partner_id, plan_id, risk_score)
    #                         SELECT p.id, {plan_id}, {risk_score}
    #                         FROM res_partner p
    #                         WHERE p.id = ANY(%s)
    #                         AND {where_conditions}
    #                     """
    #                 else:
    #                     bulk_query = f"""
    #                         INSERT INTO temp_risk_results (partner_id, plan_id, risk_score)
    #                         SELECT p.id, {plan_id}, {risk_score}
    #                         FROM res_partner p
    #                         WHERE p.id = ANY(%s)
    #                     """
    #                 return bulk_query
            
    #         # If we can't parse it safely, return None to trigger fallback
    #         return None
            
    #     except Exception as e:
    #         logger.error(f"Error creating bulk query: {str(e)}")
    #         return None
    
    def test_single_customer(self, customer_id=2825447):
        """Test processing for a single customer to debug issues"""
        
        if customer_id is None:
            # Get the first customer
            self.cursor.execute("SELECT id FROM res_partner where customer_id is not null LIMIT 1")
            result = self.cursor.fetchone()
            if not result:
                logger.error("No customers found in database!")
                return
            customer_id = result['id']
        
        logger.info(f"Testing customer ID: {customer_id}")
        
        # Verify customer exists
        self.cursor.execute("SELECT id, name, risk_score, risk_level FROM res_partner WHERE id = %s", (customer_id,))
        customer = self.cursor.fetchone()
        if not customer:
            logger.error(f"Customer {customer_id} not found!")
            return
            
        logger.info(f"Customer found: {customer['name']}, current risk_score: {customer['risk_score']}, risk_level: {customer['risk_level']}")
        
        # Get and test each risk plan
        plans = self.get_active_risk_plans()
        logger.info(f"Testing {len(plans)} active risk plans")
        
        for plan in plans:
            logger.info(f"\nTesting Plan {plan['id']}:")
            logger.info(f"  SQL: {plan['sql_query']}")
            logger.info(f"  Risk Score: {plan['risk_score']}")
            
            try:
                self.cursor.execute(plan['sql_query'], (customer_id,))
                result = self.cursor.fetchone()
                
                if result is not None:
                    logger.info(f"  ✓ MATCH! Customer {customer_id} matches this plan")
                    logger.info(f"  Query result: {result}")
                else:
                    logger.info(f"  ✗ No match for customer {customer_id}")
                    
            except Exception as e:
                logger.error(f"  ERROR executing plan: {str(e)}")
    
    def get_active_risk_plans(self):
        try:
            self.cursor.execute("""
                SELECT id, priority, sql_query, risk_score 
                FROM res_compliance_risk_assessment_plan 
                WHERE state = 'active'
                ORDER BY priority
            """)
            plans = self.cursor.fetchall()
            logger.info(f"Found {len(plans)} active risk plans")

            # Validate SQL queries in plans
            valid_plans = []
            for plan in plans:
                if not plan['sql_query'] or len(plan['sql_query'].strip()) == 0:
                    logger.warning(
                        f"Plan ID {plan['id']} has empty SQL query - skipping")
                    continue

                # Check for required parameter in SQL query
                if '%s' not in plan['sql_query'] and '%(id)s' not in plan['sql_query']:
                    logger.warning(
                        f"Plan ID {plan['id']} SQL query doesn't contain parameter placeholder - skipping")
                    continue

                valid_plans.append(plan)

            logger.info(f"{len(valid_plans)} plans have valid SQL queries")
            return valid_plans
        except Exception as e:
            logger.error(f"Error fetching risk plans: {str(e)}")
            return []

    def get_risk_plan_setting(self):
        try:
            self.cursor.execute("""
                SELECT val FROM res_compliance_settings 
                WHERE code = 'risk_plan_computation' 
                LIMIT 1
            """)
            result = self.cursor.fetchone()
            setting = result['val'] if result else 'avg'
            logger.info(f"Using risk plan computation: {setting}")
            return setting
        except Exception as e:
            logger.error(f"Error fetching risk plan settings: {str(e)}")
            return 'avg'

    def compute_customer_rating(self, score):
        try:
            if score is None or score == 0:
                return 'low'
            if score <= LOW_RISK_THRESHOLD:
                return 'low'
            if score <= MEDIUM_RISK_THRESHOLD:
                return 'medium'
            if score <= HIGH_RISK_THRESHOLD:
                return 'high'
            return 'low'
        except Exception:
            return 'low'


    
    def process_customers_bulk(self, plans, plan_setting, batch_size=PROCESSING_BATCH_SIZE):
        """
        Process customers in bulk with optimized database operations
        """
        # Read all customers from CSV files in batches
        batch_idx = 0
        total_batches_processed = 0
        
        while True:
            # Load batch_size customers
            all_customers = []
            current_batch_idx = batch_idx
            
            while len(all_customers) < batch_size:
                customers = self.get_customers_from_csv(current_batch_idx)
                if not customers:
                    break
                all_customers.extend([c['id'] for c in customers])
                current_batch_idx += 1
                
            if not all_customers:
                break
                
            # Process this batch
            batch_start_time = time.time()
            batch_size_actual = len(all_customers)
            logger.info(f"Processing bulk batch {total_batches_processed+1} with {batch_size_actual} customers")
            
            try:
                # Bulk delete existing risk plan lines
                self.cursor.execute("""
                    DELETE FROM res_partner_risk_plan_line 
                    WHERE partner_id = ANY(%s)
                """, (all_customers,))
                
                # Use a temporary table to store results for bulk operations
                self.cursor.execute("""
                    CREATE TEMPORARY TABLE IF NOT EXISTS temp_risk_results (
                        partner_id INT,
                        plan_id INT,
                        risk_score FLOAT
                    )
                """)
                self.cursor.execute("TRUNCATE temp_risk_results")
                
                # Process each risk plan for all customers at once
                for plan in plans:
                    plan_id = plan['id']
                    query = plan['sql_query']
                    risk_score = float(plan['risk_score']) if plan['risk_score'] is not None else 0
                    plan_start_time = time.time()
                    
                    # Try to create a bulk query for this plan
                    try:
                        # Replace the parameter placeholder with a subquery
                        # This is where the dynamic handling of different queries happens
                        if '%s' in query:
                            # Standard parameter style
                            param_position = query.find('%s')
                            bulk_query = f"""
                                INSERT INTO temp_risk_results (partner_id, plan_id, risk_score)
                                SELECT p.id, {plan_id}, {risk_score}
                                FROM res_partner p
                                WHERE p.id = ANY(%s)
                                AND EXISTS ({query[:param_position]} p.id {query[param_position+2:]})
                            """
                        elif '%(id)s' in query:
                            # Named parameter style
                            bulk_query = f"""
                                INSERT INTO temp_risk_results (partner_id, plan_id, risk_score)
                                SELECT p.id, {plan_id}, {risk_score}
                                FROM res_partner p
                                WHERE p.id = ANY(%s)
                                AND EXISTS ({query.replace('%(id)s', 'p.id')})
                            """
                        else:
                            # Shouldn't get here due to validation in get_active_risk_plans
                            raise ValueError("Query doesn't contain a valid parameter placeholder")
                        
                        self.cursor.execute(bulk_query, (all_customers,))
                        matched = self.cursor.rowcount
                        plan_time = time.time() - plan_start_time
                        logger.info(f"  Plan {plan_id} processed in {plan_time:.2f}s - {matched} matches")
                        
                    except Exception as e:
                        logger.warning(f"Error bulk processing plan {plan_id}: {str(e)}")
                        self.conn.rollback()
                        
                        # Fall back to individual processing for this plan
                        fallback_start = time.time()
                        logger.info(f"  Falling back to individual processing for plan {plan_id}")
                        matched = 0
                        
                        for customer_id in all_customers:
                            try:
                                self.cursor.execute(query, (customer_id,))
                                result = self.cursor.fetchone()
                                if result is not None:
                                    matched += 1
                                    self.cursor.execute("""
                                        INSERT INTO temp_risk_results (partner_id, plan_id, risk_score)
                                        VALUES (%s, %s, %s)
                                    """, (customer_id, plan_id, risk_score))
                            except Exception as e2:
                                logger.warning(f"Error processing plan {plan_id} for customer {customer_id}: {str(e2)}")
                                
                        plan_time = time.time() - fallback_start
                        logger.info(f"  Plan {plan_id} fallback processing completed in {plan_time:.2f}s - {matched} matches")
                
                # Bulk insert risk plan lines from temp table
                self.cursor.execute("""
                    INSERT INTO res_partner_risk_plan_line (partner_id, plan_line_id, risk_score)
                    SELECT partner_id, plan_id, risk_score FROM temp_risk_results
                """)
                inserted = self.cursor.rowcount
                logger.info(f"  Inserted {inserted} risk plan lines")
                
                # Calculate final risk scores and update customers
                if plan_setting == 'avg':
                    self.cursor.execute("""
                        WITH risk_scores AS (
                            SELECT partner_id, AVG(risk_score) as final_score
                            FROM temp_risk_results
                            GROUP BY partner_id
                        ),
                        risk_levels AS (
                            SELECT partner_id, final_score,
                            CASE
                                WHEN final_score <= %s THEN 'low'
                                WHEN final_score <= %s THEN 'medium'
                                WHEN final_score <= %s THEN 'high'
                                ELSE 'low'
                            END as risk_level
                            FROM risk_scores
                        )
                        UPDATE res_partner p
                        SET risk_score = r.final_score, risk_level = r.risk_level
                        FROM risk_levels r
                        WHERE p.id = r.partner_id
                    """, (LOW_RISK_THRESHOLD, MEDIUM_RISK_THRESHOLD, HIGH_RISK_THRESHOLD))
                else:  # max
                    self.cursor.execute("""
                        WITH risk_scores AS (
                            SELECT partner_id, MAX(risk_score) as final_score
                            FROM temp_risk_results
                            GROUP BY partner_id
                        ),
                        risk_levels AS (
                            SELECT partner_id, final_score,
                            CASE
                                WHEN final_score <= %s THEN 'low'
                                WHEN final_score <= %s THEN 'medium'
                                WHEN final_score <= %s THEN 'high'
                                ELSE 'low'
                            END as risk_level
                            FROM risk_scores
                        )
                        UPDATE res_partner p
                        SET risk_score = r.final_score, risk_level = r.risk_level
                        FROM risk_levels r
                        WHERE p.id = r.partner_id
                    """, (LOW_RISK_THRESHOLD, MEDIUM_RISK_THRESHOLD, HIGH_RISK_THRESHOLD))
                updated_with_scores = self.cursor.rowcount
                logger.info(f"  Updated {updated_with_scores} customers with scores")
                
                # Set risk_score = 0 and risk_level = 'low' for customers with no matches
                self.cursor.execute("""
                    WITH processed_customers AS (
                        SELECT DISTINCT partner_id FROM temp_risk_results
                    )
                    UPDATE res_partner p
                    SET risk_score = %s, risk_level = %s
                    WHERE p.id = ANY(%s)
                    AND p.id NOT IN (SELECT partner_id FROM processed_customers)
                """, (DEFAULT_SCORE,DEFAULT_LEVEL,all_customers,))
                updated_without_scores = self.cursor.rowcount
                logger.info(f"  Set {updated_without_scores} customers to default score")
                
                # Commit changes for this batch
                self.conn.commit()
                self.total_processed += batch_size_actual
                
                # Log progress
                batch_time = time.time() - batch_start_time
                logger.info(f"Completed bulk batch {total_batches_processed+1} in {batch_time:.2f}s")
                logger.info(f"Total processed: {self.total_processed} customers")
                logger.info(f"Average processing rate: {batch_size_actual / batch_time:.2f} customers/second")
                
                # Verify after first batch
                if total_batches_processed == 0:
                    try:
                        self.cursor.execute("""
                            SELECT COUNT(*) FROM res_partner WHERE risk_score > 0
                        """)
                        updated_count = self.cursor.fetchone()[0]
                        logger.info(f"Verification: {updated_count} customers have non-zero risk scores")
                        
                        self.cursor.execute("""
                            SELECT COUNT(*) FROM res_partner_risk_plan_line
                        """)
                        plan_lines_count = self.cursor.fetchone()[0]
                        logger.info(f"Verification: {plan_lines_count} risk plan lines exist")
                    except Exception as e:
                        logger.error(f"Verification query failed: {str(e)}")
                
                # Move to next batch
                batch_idx = current_batch_idx
                total_batches_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing bulk batch: {str(e)}")
                self.conn.rollback()
                self.total_errors += batch_size_actual
                batch_idx = current_batch_idx  # Skip this batch on error
                
        # Drop temp table when finished
        try:
            self.cursor.execute("DROP TABLE IF EXISTS temp_risk_results")
            self.conn.commit()
        except Exception:
            pass
                    
        return True
    
    def process_customers(self):
        """
        Main method to process customers - selects the appropriate processing method
        """
        # Get risk plans and settings
        plans = self.get_active_risk_plans()
        if not plans:
            logger.error("No active risk plans found. Aborting.")
            return False

        plan_setting = self.get_risk_plan_setting()

        # Check if we need to generate CSV files
        if not self.check_csv_files():
            logger.info("CSV files are missing or invalid. Exporting customer IDs...")
            if not self.export_customer_ids():
                logger.error("Failed to export customer IDs. Aborting.")
                return False

        # Get customer count from metadata
        try:
            with open(os.path.join(self.csv_path, METADATA_FILE), 'r') as f:
                metadata = json.load(f)
                total_customers = metadata.get('total_records', 0)
                logger.info(f"Total customers to process: {total_customers:,}")
        except Exception as e:
            logger.error(f"Error reading metadata: {str(e)}")
            total_customers = 0
            
        # Select appropriate processing method based on configuration
        processing_mode = self.config.get('processing', 'mode', fallback='bulk').lower()
        logger.info(f"Using processing mode: {processing_mode}")
        
        if processing_mode == 'batch':
            # Traditional batch processing (one customer at a time)
            return self.process_customers_batch(plans, plan_setting)
        elif processing_mode == 'parallel':
            # Multi-threaded parallel processing
            workers = self.config.getint('processing', 'parallel_workers', fallback=4)
            return self.process_customers_parallel(plans, plan_setting, max_workers=workers)
        else:
            # Default: bulk processing
            bulk_batch_size = self.config.getint('processing', 'bulk_batch_size', fallback=PROCESSING_BATCH_SIZE)
            return self.process_customers_bulk(plans, plan_setting, bulk_batch_size)
            
    def process_customers_batch(self, plans, plan_setting):
        """
        Original batch processing method (renamed from the previous process_customers)
        Processes one customer at a time
        """
        # Process customers in batches
        batch_idx = 0
        while True:
            batch_start_time = time.time()
            customers = self.get_customers_from_csv(batch_idx)
            if not customers:
                logger.info(f"No more customers found at batch index {batch_idx}")
                break

            logger.info(
                f"Processing batch of {len(customers)} customers at index {batch_idx}")

            # Process each customer in batch
            try:
                customer_ids = []  # Store IDs for bulk operations
                for customer in customers:
                    customer_id = customer['id']
                    customer_ids.append(customer_id)

                    # Clear existing plan lines for this customer
                    self.cursor.execute("""
                        DELETE FROM res_partner_risk_plan_line 
                        WHERE partner_id = %s
                    """, (customer_id,))

                    # Track all scores for this customer
                    scores = []

                    # Process each risk plan
                    for plan in plans:
                        try:
                            # IMPORTANT: Always use static score from the plan
                            # Ignore the compute_score_from column
                            risk_score = float(
                                plan['risk_score']) if plan['risk_score'] is not None else 0

                            # Execute the plan's SQL query just to check if it matches
                            self.cursor.execute(
                                plan['sql_query'], (customer_id,))
                            result = self.cursor.fetchone()

                            # Only add score if there's a match
                            if result is not None:
                                scores.append(risk_score)

                                # Insert risk plan line
                                self.cursor.execute("""
                                    INSERT INTO res_partner_risk_plan_line (partner_id, plan_line_id, risk_score)
                                    VALUES (%s, %s, %s)
                                """, (customer_id, plan['id'], risk_score))

                        except Exception as e:
                            logger.warning(
                                f"Error processing plan {plan['id']} for customer {customer_id}: {str(e)}")
                            self.conn.rollback()  # Rollback this plan's changes but continue with next plan

                    # Calculate final risk score based on setting
                    final_score = 0
                    if scores:
                        if plan_setting == 'avg':
                            final_score = sum(scores) / len(scores)
                        else:  # 'max'
                            final_score = max(scores)

                    # Compute risk level
                    risk_level = self.compute_customer_rating(final_score)

                    # Update customer
                    self.cursor.execute("""
                        UPDATE res_partner 
                        SET risk_score = %s, risk_level = %s
                        WHERE id = %s
                    """, (final_score, risk_level, customer_id))

                # Commit changes for this batch
                self.conn.commit()
                self.total_processed += len(customers)

                # Log progress
                batch_time = time.time() - batch_start_time
                logger.info(
                    f"Completed batch at index {batch_idx} in {batch_time:.2f}s")
                logger.info(
                    f"Total processed: {self.total_processed} customers")

                # Verify some updates took place
                if batch_idx == 0:
                    try:
                        self.cursor.execute("""
                            SELECT COUNT(*) FROM res_partner WHERE risk_score > 0
                        """)
                        updated_count = self.cursor.fetchone()[0]
                        logger.info(
                            f"Verification: {updated_count} customers have non-zero risk scores")

                        self.cursor.execute("""
                            SELECT COUNT(*) FROM res_partner_risk_plan_line
                        """)
                        plan_lines_count = self.cursor.fetchone()[0]
                        logger.info(
                            f"Verification: {plan_lines_count} risk plan lines exist")
                    except Exception as e:
                        logger.error(f"Verification query failed: {str(e)}")

            except Exception as e:
                logger.error(
                    f"Error processing batch at index {batch_idx}: {str(e)}")
                self.conn.rollback()
                self.total_errors += len(customers)

            batch_idx += 1
            
        return True

        # Final stats
        elapsed = time.time() - self.start_time
        logger.info(f"Risk score calculation completed!")
        logger.info(f"Total processed: {self.total_processed} customers")
        logger.info(f"Total errors: {self.total_errors}")
        logger.info(f"Total time: {elapsed:.2f} seconds")
        if elapsed > 0:
            logger.info(
                f"Average rate: {self.total_processed / elapsed:.2f} customers/second")

        return True

    def process_customers_parallel(self, plans, plan_setting, max_workers=5):
        """
        Process customers in parallel using multiple worker threads
        """
        # Check if CSV files exist
        if not self.check_csv_files():
            logger.info(
                "CSV files are missing or invalid. Exporting customer IDs...")
            if not self.export_customer_ids():
                logger.error("Failed to export customer IDs. Aborting.")
                return False

        # Get total file count from metadata
        try:
            with open(os.path.join(self.csv_path, METADATA_FILE), 'r') as f:
                metadata = json.load(f)
                file_count = metadata.get('file_count', 0)
        except Exception as e:
            logger.error(f"Error reading metadata: {str(e)}")
            return False

        logger.info(f"Starting parallel processing with {max_workers} workers")

        # Process CSV files in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit jobs to process each CSV file
            futures = {}
            for file_idx in range(1, file_count + 1):
                future = executor.submit(
                    self._process_file_task,
                    file_idx,
                    plans,
                    plan_setting
                )
                futures[future] = file_idx

            # Process results as they complete
            for future in as_completed(futures):
                file_idx = futures[future]
                try:
                    processed_count, error_count, elapsed = future.result()
                    self.total_processed += processed_count
                    self.total_errors += error_count
                    logger.info(
                        f"File {file_idx}/{file_count} completed: processed {processed_count} customers in {elapsed:.2f}s")
                    logger.info(
                        f"Running total processed: {self.total_processed} customers")
                except Exception as e:
                    logger.error(f"Error processing file {file_idx}: {str(e)}")

        return True

    def _process_file_task(self, file_idx, plans, plan_setting):
        """
        Worker task to process all customers in a single CSV file
        Returns (processed_count, error_count, elapsed_time)
        """
        # Create a new database connection for this thread
        conn = None
        cursor = None
        processed = 0
        errors = 0
        start_time = time.time()

        try:
            # Connect to database
            conn = psycopg2.connect(
                host=self.config.get('database', 'host'),
                port=self.config.get('database', 'port'),
                database=self.config.get('database', 'dbname'),
                user=self.config.get('database', 'user'),
                password=self.config.get('database', 'password')
            )
            conn.autocommit = False
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictReader)

            # Get customers from this file
            file_path = os.path.join(
                self.csv_path, f'res_partner_ids_{file_idx}.csv')

            if not os.path.exists(file_path):
                logger.warning(f"CSV file not found: {file_path}")
                return 0, 0, time.time() - start_time

            # Process in batches
            batch_size = 100

            with open(file_path, 'r', newline='') as f:
                reader = csv.DictReader(f)
                customer_batch = []

                for row in reader:
                    customer_batch.append(int(row['id']))

                    # Process batch when it reaches batch_size
                    if len(customer_batch) >= batch_size:
                        success, count, batch_errors = self._process_customer_batch(
                            customer_batch, plans, plan_setting, conn, cursor)
                        processed += count
                        errors += batch_errors
                        customer_batch = []

                # Process any remaining customers
                if customer_batch:
                    success, count, batch_errors = self._process_customer_batch(
                        customer_batch, plans, plan_setting, conn, cursor)
                    processed += count
                    errors += batch_errors

            return processed, errors, time.time() - start_time

        except Exception as e:
            logger.error(f"Thread error processing file {file_idx}: {str(e)}")
            if conn:
                conn.rollback()
            return processed, errors, time.time() - start_time
        finally:
            # Clean up database connections
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def _process_customer_batch(self, customer_ids, plans, plan_setting, conn, cursor):
        """
        Process a batch of customers
        Returns (success, processed_count, error_count)
        """
        if not customer_ids:
            return True, 0, 0

        try:
            # Use temporary table for risk results
            cursor.execute("""
                CREATE TEMPORARY TABLE IF NOT EXISTS temp_thread_results (
                    partner_id INT,
                    plan_id INT,
                    risk_score FLOAT
                )
            """)
            cursor.execute("TRUNCATE temp_thread_results")

            # Process each customer and plan
            for customer_id in customer_ids:
                # Clear existing plan lines
                cursor.execute("""
                    DELETE FROM res_partner_risk_plan_line 
                    WHERE partner_id = %s
                """, (customer_id,))

                # Process each risk plan
                for plan in plans:
                    try:
                        risk_score = float(
                            plan['risk_score']) if plan['risk_score'] is not None else 0

                        # Check if this plan matches the customer
                        cursor.execute(plan['sql_query'], (customer_id,))
                        result = cursor.fetchone()

                        if result is not None:
                            # Store in temp table
                            cursor.execute("""
                                INSERT INTO temp_thread_results (partner_id, plan_id, risk_score)
                                VALUES (%s, %s, %s)
                            """, (customer_id, plan['id'], risk_score))
                    except Exception as e:
                        logger.warning(
                            f"Error in thread processing plan {plan['id']} for customer {customer_id}: {str(e)}")

            # Insert risk plan lines from temp table
            cursor.execute("""
                INSERT INTO res_partner_risk_plan_line (partner_id, plan_line_id, risk_score)
                SELECT partner_id, plan_id, risk_score FROM temp_thread_results
            """)

            # Calculate final scores and update customer records
            for customer_id in customer_ids:
                # Get all scores for this customer
                cursor.execute("""
                    SELECT risk_score FROM temp_thread_results WHERE partner_id = %s
                """, (customer_id,))
                scores = [row[0] for row in cursor.fetchall()]

                # Calculate final score
                final_score = 0
                if scores:
                    if plan_setting == 'avg':
                        final_score = sum(scores) / len(scores)
                    else:  # 'max'
                        final_score = max(scores)

                # Determine risk level
                if final_score <= LOW_RISK_THRESHOLD:
                    risk_level = 'low'
                elif final_score <= MEDIUM_RISK_THRESHOLD:
                    risk_level = 'medium'
                elif final_score <= HIGH_RISK_THRESHOLD:
                    risk_level = 'high'
                else:
                    risk_level = 'low'

                # Update customer
                cursor.execute("""
                    UPDATE res_partner 
                    SET risk_score = %s, risk_level = %s
                    WHERE id = %s
                """, (final_score, risk_level, customer_id))

            # Commit changes
            conn.commit()
            return True, len(customer_ids), 0

        except Exception as e:
            logger.error(f"Error in thread batch processing: {str(e)}")
            conn.rollback()
            return False, 0, len(customer_ids)

    def analyze_risk_plan_queries(self, plans):
        """
        Analyze risk plan queries to identify fields for indexing
        Returns a set of fields/conditions that should be indexed
        """
        index_patterns = []

        for plan in plans:
            query = plan['sql_query'].lower()

            # Skip invalid queries
            if not query or '%s' not in query and '%(id)s' not in query:
                continue

            # Extract the WHERE conditions from the query
            try:
                where_part = query.split(
                    'where')[1] if 'where' in query else ''

                # Look for common patterns
                if 'is_blacklist' in where_part:
                    index_patterns.append(('is_blacklist', True))
                if 'is_pep' in where_part:
                    index_patterns.append(('is_pep', True))
                if 'is_watchlist' in where_part:
                    index_patterns.append(('is_watchlist', True))
                if 'customer_phone' in where_part:
                    index_patterns.append(('customer_phone', False))
                if 'mobile is null' in where_part or 'phone is null' in where_part:
                    index_patterns.append(('mobile_phone_null', True))
                if 'bvn is null' in where_part or "bvn like" in where_part:
                    index_patterns.append(('bvn_pattern', True))
                if 'trim(name)' in where_part:
                    index_patterns.append(('name_pattern', True))

                # Generic pattern matching for other potential fields
                # Look for comparisons in the WHERE clause
                fields = []
                for condition in where_part.split('and'):
                    # Extract field names from conditions
                    field_match = condition.strip().split(
                        '=')[0].strip() if '=' in condition else ''
                    if field_match and field_match not in ('id', 'partner_id'):
                        fields.append(field_match)

                # Add any other fields found
                for field in fields:
                    if field and field not in [p[0] for p in index_patterns]:
                        index_patterns.append((field, False))

            except Exception as e:
                logger.warning(
                    f"Error analyzing query for plan {plan['id']}: {str(e)}")

        logger.info(
            f"Identified {len(index_patterns)} potential fields for indexing")
        return index_patterns

    def create_performance_indexes(self, plans):
        """
        Create temporary indexes to improve query performance during processing
        Dynamically analyzes risk plan queries to determine which indexes to create
        """
        try:
            logger.info("Creating temporary performance indexes...")

            # Only create indexes if configured to do so
            if not self.config.getboolean('processing', 'enable_indexes', fallback=True):
                logger.info("Performance indexes disabled in configuration")
                return False

            # Analyze risk plan queries to determine which fields need indexing
            index_patterns = self.analyze_risk_plan_queries(plans)

            # Map patterns to index creation statements
            index_statements = []

            for pattern, is_condition in index_patterns:
                # Create appropriate index based on the pattern
                if pattern == 'is_blacklist':
                    index_statements.append(
                        "CREATE INDEX IF NOT EXISTS temp_idx_blacklist ON res_partner (id) WHERE is_blacklist=true")
                elif pattern == 'is_pep':
                    index_statements.append(
                        "CREATE INDEX IF NOT EXISTS temp_idx_pep ON res_partner (id) WHERE is_pep=true")
                elif pattern == 'is_watchlist':
                    index_statements.append(
                        "CREATE INDEX IF NOT EXISTS temp_idx_watchlist ON res_partner (id) WHERE is_watchlist=true")
                elif pattern == 'customer_phone':
                    index_statements.append(
                        "CREATE INDEX IF NOT EXISTS temp_idx_customer_phone ON res_partner (customer_phone) WHERE customer_phone IS NOT NULL AND customer_phone != ''")
                elif pattern == 'mobile_phone_null':
                    index_statements.append(
                        "CREATE INDEX IF NOT EXISTS temp_idx_mobile_phone ON res_partner (id) WHERE mobile IS NULL AND phone IS NULL")
                elif pattern == 'bvn_pattern':
                    index_statements.append(
                        "CREATE INDEX IF NOT EXISTS temp_idx_bvn ON res_partner (id) WHERE (bvn IS NULL OR bvn LIKE '%[a-zA-Z]%' OR bvn LIKE 'NOBVN%')")
                elif pattern == 'name_pattern':
                    index_statements.append(
                        "CREATE INDEX IF NOT EXISTS temp_idx_name_special ON res_partner (id) WHERE (trim(name) = '' OR trim(name) ~ '^[^a-zA-Z0-9]')")
                elif not is_condition:
                   
                    safe_name = pattern.replace('.', '_').replace('(', '_').replace(')', '_')
                    index_name = f"temp_idx_{safe_name}"
                    
                    # For function expressions, use the expression as-is in the index definition
                    index_statements.append(f"CREATE INDEX IF NOT EXISTS {index_name} ON res_partner {pattern})")

            # Add a few general purpose indexes that might help with any query
            index_statements.append(
                "CREATE INDEX IF NOT EXISTS temp_idx_partner_id ON res_partner_risk_plan_line (partner_id)")

            # Make the index statements unique
            index_statements = list(set(index_statements))

            # Execute each index creation statement
            for idx, stmt in enumerate(index_statements):
                index_start = time.time()
                try:
                    self.cursor.execute(stmt)
                    self.conn.commit()
                    logger.info(
                        f"  Created index {idx+1}/{len(index_statements)} in {time.time() - index_start:.2f}s")
                except Exception as e:
                    logger.warning(f"  Failed to create index: {str(e)}")
                    self.conn.rollback()

            logger.info("Performance indexes created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating performance indexes: {str(e)}")
            self.conn.rollback()
            return False

    def drop_performance_indexes(self):
        """
        Drop temporary performance indexes
        """
        try:
            logger.info("Dropping temporary performance indexes...")

            # Only drop indexes if they were created
            if not self.config.getboolean('processing', 'enable_indexes', fallback=True):
                return

            # List all temporary indexes to drop
            drop_statements = [
                "DROP INDEX IF EXISTS temp_idx_blacklist",
                "DROP INDEX IF EXISTS temp_idx_pep",
                "DROP INDEX IF EXISTS temp_idx_watchlist",
                "DROP INDEX IF EXISTS temp_idx_customer_phone",
                "DROP INDEX IF EXISTS temp_idx_name_special",
                "DROP INDEX IF EXISTS temp_idx_mobile_phone",
                "DROP INDEX IF EXISTS temp_idx_bvn"
            ]

            # Execute each drop statement
            for stmt in drop_statements:
                try:
                    self.cursor.execute(stmt)
                    self.conn.commit()
                except Exception as e:
                    logger.warning(f"Failed to drop index: {str(e)}")
                    self.conn.rollback()

            logger.info("Performance indexes dropped")
        except Exception as e:
            logger.error(f"Error dropping performance indexes: {str(e)}")
            self.conn.rollback()
    
    def run(self):
        try:
            logger.info("Starting risk score calculation for all customers")
            self.start_time = time.time()

            # Connect to database
            self.connect_to_database()
            
            # Get risk plans first
            plans = self.get_active_risk_plans()
            if not plans:
                logger.error("No active risk plans found. Aborting.")
                return False
                
            # Create temporary performance indexes based on the active plans
            self.create_performance_indexes(plans)

            try:
                # Process all customers
                self.test_single_customer()
                self.process_customers()
            finally:
                # Always drop the temporary indexes, even if processing fails
                self.drop_performance_indexes()

        except Exception as e:
            logger.error(f"Critical error: {str(e)}")
            if self.conn:
                self.conn.rollback()
        finally:
            # Clean up
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logger.info("Database connection closed")


# if __name__ == "__main__":
#     try:
#         # Parse command line arguments if needed
#         config_path = DEFAULT_CONFIG_FILE
#         if len(sys.argv) > 1:
#             config_path = sys.argv[1]

#         # Create and run the calculator
#         calculator = RiskScoreCalculator(config_path)
#         calculator.run()
#     except KeyboardInterrupt:
#         logger.info("Process interrupted by user")
#         sys.exit(1)
#     except Exception as e:
#         logger.critical(f"Unhandled exception: {str(e)}")
#         sys.exit(1)
        
        
if __name__ == "__main__":
    try:
        # Parse command line arguments properly
        parser = argparse.ArgumentParser(description='Risk Score Calculator')
        parser.add_argument('--config', 
                        default=DEFAULT_CONFIG_FILE,
                        help=f'Path to configuration file (default: {DEFAULT_CONFIG_FILE})')
        
        
        args = parser.parse_args()
        config_path = args.config

        # Create and run the calculator
        calculator = RiskScoreCalculator(config_path)
        
        # If test customer specified, only test that customer
       
        calculator.run()
            
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Unhandled exception: {str(e)}")
        sys.exit(1)