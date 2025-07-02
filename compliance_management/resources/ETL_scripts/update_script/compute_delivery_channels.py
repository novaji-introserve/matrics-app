#!/usr/bin/env python3
"""
Ultra-Optimized Database Migration Script with Multi-Worker Support
Converts customer channel subscription data with performance optimizations.
"""

import os
import sys
import gc
import time
import logging
import configparser
import threading
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor, execute_values

# Default configuration file path
DEFAULT_CONFIG_FILE = "/data/odoo/ETL_script/update_script/settings.conf"


def setup_logging():
    """Setup logging with file size management"""
    log_file = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), 'UpdateScript.log')

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


# Initialize logger
logger = logging.getLogger("__CUSTOMER_DELIVERY_CHANNELS__")


@dataclass
class MigrationStats:
    """Thread-safe migration statistics"""

    def __init__(self):
        self.lock = threading.Lock()
        self.subscriptions_created = 0
        self.legacy_migrated = 0
        self.partner_links_updated = 0
        self.batches_completed = 0
        self.errors = 0

    def update(self, subscriptions=0, legacy=0, partners=0, batches=0, errors=0):
        with self.lock:
            self.subscriptions_created += subscriptions
            self.legacy_migrated += legacy
            self.partner_links_updated += partners
            self.batches_completed += batches
            self.errors += errors

    def get_stats(self):
        with self.lock:
            return {
                'subscriptions_created': self.subscriptions_created,
                'legacy_migrated': self.legacy_migrated,
                'partner_links_updated': self.partner_links_updated,
                'batches_completed': self.batches_completed,
                'errors': self.errors
            }


class DatabaseMigrator:
    """Main migration class with multi-worker support"""

    def __init__(self, config_file: str = DEFAULT_CONFIG_FILE):
        self.config_file = config_file
        # Fixed at 4 workers as specified
        self.num_workers = 4

        self.logger = logger  # Use the global logger - set this first
        self.config = self._load_config()
        self.connection_pool = None
        self.stats = MigrationStats()

        # Memory management settings
        self.memory_limit_percent = 80  # Don't use more than 80% of system memory
        self.ram_gb = psutil.virtual_memory().total / (1024 * 1024 * 1024)

        # Thread-safe progress tracking
        self.progress_lock = threading.Lock()
        self.total_batches = 0
        self.completed_batches = 0

    def _load_config(self) -> Dict:
        """Load database configuration from settings.conf"""
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(
                f"Configuration file {self.config_file} not found")

        config = configparser.ConfigParser()
        config.read(self.config_file)

        if 'database' not in config:
            raise ValueError(
                "Missing [database] section in configuration file")

        db_config = dict(config['database'])

        # Validate required fields
        required_fields = ['host', 'port', 'user', 'password', 'dbname']
        missing_fields = [
            field for field in required_fields if field not in db_config]

        if missing_fields:
            raise ValueError(
                f"Missing required database fields: {missing_fields}")

        # Convert port to int
        db_config['port'] = int(db_config['port'])

        self.logger.info(
            f"Loaded configuration for database: {db_config['dbname']} at {db_config['host']}:{db_config['port']}")
        return db_config

    def create_connection_pool(self):
        """Create thread-safe connection pool with optimizations"""
        try:
            # Create pool with enough connections for workers + overhead
            min_connections = self.num_workers
            max_connections = self.num_workers * 2 + 4  # Increased for more concurrency

            # Connection parameters with advanced optimizations
            connection_params = {
                'host': self.config['host'],
                'port': self.config['port'],
                'user': self.config['user'],
                'password': self.config['password'],
                'database': self.config['dbname'],
                'cursor_factory': RealDictCursor,
                # Enhanced session-level optimizations
                'options': '-c synchronous_commit=off -c statement_timeout=0 -c idle_in_transaction_session_timeout=0',
                'keepalives': 1,
                'keepalives_idle': 30,
                'keepalives_interval': 10,
                'keepalives_count': 5,
                'application_name': 'OdooChannelMigration'  # For monitoring
            }

            self.connection_pool = ThreadedConnectionPool(
                min_connections,
                max_connections,
                **connection_params
            )

            self.logger.info(
                f"Created optimized connection pool with {min_connections}-{max_connections} connections")

            # Apply database-level optimizations
            self._optimize_database_settings()

        except Exception as e:
            self.logger.error(f"Failed to create connection pool: {e}")
            raise

    def _optimize_database_settings(self):
        """Apply temporary database optimizations for bulk operations"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # Enhanced optimizations for maximum performance
                optimizations = [
                    "SET synchronous_commit = off",
                    f"SET work_mem = '{min(2048, max(256, int(self.ram_gb * 256)))}MB'",
                    f"SET maintenance_work_mem = '{min(4096, max(512, int(self.ram_gb * 512)))}MB'",
                    f"SET temp_buffers = '{min(256, max(32, int(self.ram_gb * 32)))}MB'",
                    f"SET effective_cache_size = '{min(8, max(1, int(self.ram_gb * 0.5)))}GB'",
                    "SET random_page_cost = 1.1",
                    "SET seq_page_cost = 1.0",
                    "SET cpu_tuple_cost = 0.01",
                    "SET cpu_index_tuple_cost = 0.005",
                    "SET cpu_operator_cost = 0.0025",
                    "SET enable_partitionwise_join = on",
                    "SET enable_partitionwise_aggregate = on",
                    "SET jit = on",                    # Enable JIT compilation for faster query execution
                    "SET jit_above_cost = 50000",      # Lower threshold to JIT more queries
                    "SET jit_inline_above_cost = 100000",
                    "SET jit_optimize_above_cost = 200000",
                    "SET statement_timeout = 0",       # No timeout for long operations
                    "SET lock_timeout = '10min'",      # But prevent indefinite lock waits
                    "SET idle_in_transaction_session_timeout = 0"  # No timeout for idle transactions
                ]

                for optimization in optimizations:
                    try:
                        cursor.execute(optimization)
                        self.logger.info(f"Applied: {optimization}")
                    except Exception as e:
                        self.logger.warning(
                            f"Could not apply {optimization}: {e}")

                # Analyze tables for better query planning
                tables_to_analyze = [
                    "res_partner",
                    "digital_delivery_channel",
                    "customer_channel_subscription",
                    "customer_digital_product"
                ]

                for table in tables_to_analyze:
                    try:
                        cursor.execute(f"ANALYZE {table}")
                        self.logger.info(f"Analyzed table: {table}")
                    except Exception as e:
                        self.logger.warning(f"Could not analyze {table}: {e}")

                conn.commit()
                self.logger.info(
                    "Enhanced database optimizations applied")

        except Exception as e:
            self.logger.warning(f"Database optimization failed: {e}")
        finally:
            self.return_connection(conn)

    def get_connection(self):
        """Get connection from pool with retry logic"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                return self.connection_pool.getconn()
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                self.logger.warning(
                    f"Connection pool get failed (attempt {attempt + 1}): {e}")
                time.sleep(0.5 * (attempt + 1))  # Exponential backoff

    def return_connection(self, conn):
        """Return connection to pool with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return self.connection_pool.putconn(conn)
            except Exception as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Failed to return connection: {e}")
                    # Last resort: try to close it directly
                    try:
                        conn.close()
                    except:
                        pass
                    return
                self.logger.warning(
                    f"Connection pool return failed (attempt {attempt + 1}): {e}")
                time.sleep(0.5 * (attempt + 1))

    def execute_with_retry(self, conn, query, params=None, max_retries=3):
        """Execute query with improved retry logic"""
        last_error = None
        for attempt in range(max_retries):
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    conn.commit()
                    return cursor.rowcount
            except psycopg2.errors.DeadlockDetected as e:
                # Special handling for deadlocks - always retry with longer wait
                last_error = e
                self.logger.warning(
                    f"Deadlock detected (attempt {attempt + 1}): {e}")
                time.sleep(2 * (attempt + 1))  # Longer backoff for deadlocks
                try:
                    conn.rollback()
                except:
                    pass
            except Exception as e:
                last_error = e
                if attempt == max_retries - 1:
                    break
                self.logger.warning(
                    f"Query failed (attempt {attempt + 1}): {e}")
                time.sleep(1 * (attempt + 1))
                try:
                    conn.rollback()
                except:
                    pass

        # If we got here, all attempts failed
        raise last_error or Exception("Query failed after all retries")

    def execute_values_with_retry(self, conn, query, values, template=None, max_retries=3):
        """Execute values with retry logic using psycopg2's execute_values"""
        last_error = None
        for attempt in range(max_retries):
            try:
                with conn.cursor() as cursor:
                    execute_values(cursor, query, values, template)
                    conn.commit()
                    return cursor.rowcount
            except psycopg2.errors.DeadlockDetected as e:
                # Special handling for deadlocks
                last_error = e
                self.logger.warning(
                    f"Deadlock detected in execute_values (attempt {attempt + 1}): {e}")
                time.sleep(2 * (attempt + 1))
                try:
                    conn.rollback()
                except:
                    pass
            except Exception as e:
                last_error = e
                if attempt == max_retries - 1:
                    break
                self.logger.warning(
                    f"Execute values failed (attempt {attempt + 1}): {e}")
                time.sleep(1 * (attempt + 1))
                try:
                    conn.rollback()
                except:
                    pass

        # If we got here, all attempts failed
        raise last_error or Exception(
            "Execute values failed after all retries")

    def create_initial_channels(self):
        """Create initial digital delivery channels"""
        conn = self.get_connection()
        try:
            channels_data = [
                ('USSD', 'ussd', 'USSD Banking Services', 'active'),
                ('One Bank', 'onebank', 'One Bank Mobile App', 'active'),
                ('Card', 'carded_customer', 'Card Services', 'active'),
                ('Alt Bank', 'alt_bank', 'Alternative Banking', 'active'),
                ('Sterling Pro', 'sterling_pro', 'Sterling Pro Services', 'active'),
                ('Banca', 'banca', 'Banca Services', 'active'),
                ('Doubble', 'doubble', 'Doubble Services', 'active'),
                ('Specta', 'specta', 'Specta Services', 'active'),
                ('Switch', 'switch', 'Switch Services', 'active')
            ]

            # Use execute_values for faster bulk insert
            query = """
                INSERT INTO digital_delivery_channel (name, code, description, status)
                VALUES %s
                ON CONFLICT (code) DO NOTHING
            """

            self.execute_values_with_retry(conn, query, channels_data)
            self.logger.info("Created initial delivery channels")

        finally:
            self.return_connection(conn)

    def create_performance_indexes(self):
        """Create all performance indexes with parallel execution"""
        # Execute index creation in parallel for faster setup
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = []

            # Regular indexes
            indexes = [
                ('digital_delivery_channel_code_idx',
                 'digital_delivery_channel', ['code']),
                ('digital_delivery_channel_status_idx',
                 'digital_delivery_channel', ['status']),
                ('customer_channel_subscription_customer_idx',
                 'customer_channel_subscription', ['customer_id']),
                ('customer_channel_subscription_channel_idx',
                 'customer_channel_subscription', ['channel_id']),
                ('customer_channel_subscription_partner_idx',
                 'customer_channel_subscription', ['partner_id']),
                ('customer_channel_subscription_value_idx',
                 'customer_channel_subscription', ['value']),
                ('res_partner_customer_id_idx',
                 'res_partner', ['customer_id']),
            ]

            # Composite indexes
            composite_indexes = [
                ('customer_channel_subscription_customer_channel_idx',
                 'customer_channel_subscription', ['customer_id', 'channel_id']),
                ('customer_channel_subscription_partner_channel_idx',
                 'customer_channel_subscription', ['partner_id', 'channel_id']),
            ]

            # Partial index
            partial_idx = {
                'name': 'res_partner_customer_id_not_null_idx',
                'query': """
                    CREATE INDEX IF NOT EXISTS res_partner_customer_id_not_null_idx 
                    ON res_partner (customer_id) 
                    WHERE customer_id IS NOT NULL AND customer_id != ''
                """
            }

            # Submit regular and composite index creation tasks
            for idx_info in indexes + composite_indexes:
                futures.append(executor.submit(self._create_index, idx_info))

            # Submit partial index creation task
            futures.append(executor.submit(
                self._create_partial_index, partial_idx))

            # Wait for all index creations to complete
            for future in as_completed(futures):
                try:
                    result = future.result()
                    self.logger.info(f"Index creation result: {result}")
                except Exception as e:
                    self.logger.error(f"Index creation failed: {e}")

    def _create_index(self, idx_info):
        """Helper method to create an index"""
        idx_name, table, columns = idx_info
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                query = f"""
                    CREATE INDEX IF NOT EXISTS {idx_name} 
                    ON {table} ({','.join(columns)})
                """
                cursor.execute(query)
                conn.commit()
                return f"✅ Index {idx_name} created"
        except Exception as e:
            return f"❌ Index {idx_name} failed: {e}"
        finally:
            self.return_connection(conn)

    def _create_partial_index(self, idx_info):
        """Helper method to create a partial index"""
        idx_name, query = idx_info['name'], idx_info['query']
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()
                return f"✅ Partial index {idx_name} created"
        except Exception as e:
            return f"❌ Partial index {idx_name} failed: {e}"
        finally:
            self.return_connection(conn)

    def get_migration_parameters(self):
        """Get migration parameters and counts with better error handling"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # Get total customer count with more reliable query
                cursor.execute("""
                    SELECT COUNT(*) as total_customers FROM res_partner 
                    WHERE customer_id IS NOT NULL AND customer_id != ''
                """)
                total_customers = cursor.fetchone()['total_customers']

                # Get channel count
                cursor.execute(
                    "SELECT COUNT(*) as channel_count FROM digital_delivery_channel WHERE status = 'active'")
                channel_count = cursor.fetchone()['channel_count'] or 0

                # Get existing subscription count
                cursor.execute(
                    "SELECT COUNT(*) as existing_subscriptions FROM customer_channel_subscription")
                existing_subscriptions = cursor.fetchone()[
                    'existing_subscriptions'] or 0

                # Check if subscriptions are already created (optimization)
                expected_subscriptions = total_customers * channel_count
                subscriptions_complete = expected_subscriptions > 0 and existing_subscriptions >= (
                    expected_subscriptions * 0.9)  # 90% threshold

                # Optimized for large databases (45M+ subscriptions)
                # Use larger batch sizes for efficiency
                batch_size = 300000  # Large fixed batch size for efficiency

                # Calculate total batches needed
                total_batches = (total_customers +
                                 batch_size - 1) // batch_size

                # Adjust chunk size for optimal memory management
                chunk_size = 10000  # Fixed chunk size for consistent performance

                self.logger.info(
                    f"Migration analysis: {total_customers:,} customers, {existing_subscriptions:,} existing subscriptions")
                self.logger.info(
                    f"Expected subscriptions: {expected_subscriptions:,}, Complete: {subscriptions_complete}")
                self.logger.info(
                    f"System memory: {self.ram_gb:.1f}GB, Batch size: {batch_size:,}, Chunk size: {chunk_size:,}")

                return {
                    'total_customers': total_customers,
                    'channel_count': channel_count,
                    'existing_subscriptions': existing_subscriptions,
                    'batch_size': batch_size,
                    'chunk_size': chunk_size,
                    'total_batches': total_batches,
                    'subscriptions_complete': subscriptions_complete,
                    'expected_subscriptions': expected_subscriptions
                }
        finally:
            self.return_connection(conn)

    def process_batch_worker(self, batch_info: Tuple[int, int, int, Dict]) -> Dict:
        """Worker function to process a single batch - ULTRA-OPTIMIZED"""
        batch_num, offset, batch_size, params = batch_info
        thread_name = threading.current_thread().name

        conn = self.get_connection()
        batch_results = {
            'batch_num': batch_num,
            'subscriptions': 0,
            'legacy': 0,
            'partners': 0,
            'errors': 0
        }

        try:
            self.logger.info(
                f"[{thread_name}] Processing batch {batch_num} (offset: {offset}, size: {batch_size})")

            # Step 1: Create subscriptions (SKIP if already complete)
            if not params['subscriptions_complete']:
                try:
                    # Process in smaller chunks for better memory management
                    chunk_size = params['chunk_size']
                    total_subscriptions = 0

                    for chunk_offset in range(0, batch_size, chunk_size):
                        # Adjust chunk size for last chunk if needed
                        current_chunk_size = min(
                            chunk_size, batch_size - chunk_offset)
                        current_offset = offset + chunk_offset

                        subscriptions = self._create_subscriptions_for_chunk_ultra_optimized(
                            conn, current_offset, current_chunk_size)
                        total_subscriptions += subscriptions

                        # Log progress for large chunks
                        if chunk_offset > 0 and subscriptions > 0:
                            self.logger.info(
                                f"[{thread_name}] Batch {batch_num}: Chunk progress {chunk_offset+current_chunk_size}/{batch_size} - Created {subscriptions:,} subscriptions")

                    batch_results['subscriptions'] = total_subscriptions
                    self.logger.info(
                        f"[{thread_name}] Batch {batch_num}: Created total {total_subscriptions:,} subscriptions")
                except Exception as e:
                    self.logger.error(
                        f"[{thread_name}] Batch {batch_num} subscription creation failed: {e}")
                    batch_results['errors'] += 1
            else:
                self.logger.info(
                    f"[{thread_name}] Batch {batch_num}: Skipping subscription creation (already complete)")

            # Step 2: Migrate legacy data (ULTRA-OPTIMIZED)
            try:
                # Process in smaller chunks for better memory management
                chunk_size = params['chunk_size']
                total_legacy = 0

                for chunk_offset in range(0, batch_size, chunk_size):
                    # Adjust chunk size for last chunk if needed
                    current_chunk_size = min(
                        chunk_size, batch_size - chunk_offset)
                    current_offset = offset + chunk_offset

                    legacy = self._migrate_legacy_for_chunk_ultra_optimized(
                        conn, current_offset, current_chunk_size)
                    total_legacy += legacy

                    # Log progress for large chunks
                    if chunk_offset > 0 and legacy > 0:
                        self.logger.info(
                            f"[{thread_name}] Batch {batch_num}: Chunk progress {chunk_offset+current_chunk_size}/{batch_size} - Migrated {legacy:,} legacy records")

                batch_results['legacy'] = total_legacy
                self.logger.info(
                    f"[{thread_name}] Batch {batch_num}: Migrated total {total_legacy:,} legacy records")
            except Exception as e:
                self.logger.error(
                    f"[{thread_name}] Batch {batch_num} legacy migration failed: {e}")
                batch_results['errors'] += 1

            # Step 3: Update partner relationships (ULTRA-OPTIMIZED)
            try:
                # Process in smaller chunks for better memory management
                chunk_size = params['chunk_size']
                total_partners = 0

                for chunk_offset in range(0, batch_size, chunk_size):
                    # Adjust chunk size for last chunk if needed
                    current_chunk_size = min(
                        chunk_size, batch_size - chunk_offset)
                    current_offset = offset + chunk_offset

                    partners = self._update_partners_for_chunk_ultra_optimized(
                        conn, current_offset, current_chunk_size)
                    total_partners += partners

                    # Log progress for large chunks
                    if chunk_offset > 0 and partners > 0:
                        self.logger.info(
                            f"[{thread_name}] Batch {batch_num}: Chunk progress {chunk_offset+current_chunk_size}/{batch_size} - Updated {partners:,} partner links")

                batch_results['partners'] = total_partners
                self.logger.info(
                    f"[{thread_name}] Batch {batch_num}: Updated total {total_partners:,} partner links")
            except Exception as e:
                self.logger.error(
                    f"[{thread_name}] Batch {batch_num} partner update failed: {e}")
                batch_results['errors'] += 1

            # Update progress
            with self.progress_lock:
                self.completed_batches += 1
                progress = (self.completed_batches / self.total_batches) * 100
                self.logger.info(
                    f"[{thread_name}] Batch {batch_num} COMPLETE - Progress: {progress:.1f}%")

            # Memory cleanup - more aggressive
            gc.collect()

        except Exception as e:
            self.logger.error(f"[{thread_name}] Batch {batch_num} failed: {e}")
            batch_results['errors'] += 1

        finally:
            self.return_connection(conn)

        return batch_results

    def _create_subscriptions_for_chunk_ultra_optimized(self, conn, offset: int, chunk_size: int) -> int:
        """ULTRA-OPTIMIZED: Create subscriptions using true bulk operations with temporary tables"""
        with conn.cursor() as cursor:
            # Create a thread-specific temporary table for this operation
            thread_id = threading.get_ident()
            temp_customer_table = f"temp_batch_customers_{thread_id}"
            temp_result_table = f"temp_subscriptions_{thread_id}"

            try:
                # Step 1: Create temp tables
                cursor.execute(f"""
                    CREATE TEMP TABLE {temp_customer_table} (
                        partner_id INTEGER,
                        customer_id TEXT
                    ) ON COMMIT DROP
                """)

                cursor.execute(f"""
                    CREATE TEMP TABLE {temp_result_table} (
                        customer_id TEXT,
                        partner_id INTEGER,
                        channel_id INTEGER,
                        value TEXT,
                        last_updated TIMESTAMP
                    ) ON COMMIT DROP
                """)

                # Step 2: Load customer batch into temp table
                cursor.execute(f"""
                    INSERT INTO {temp_customer_table} (partner_id, customer_id)
                    SELECT id, customer_id 
                    FROM res_partner 
                    WHERE customer_id IS NOT NULL AND customer_id != ''
                    ORDER BY id
                    LIMIT {chunk_size} OFFSET {offset}
                """)

                # Step 3: Prepare all potential subscriptions in temp table
                cursor.execute(f"""
                    INSERT INTO {temp_result_table} (customer_id, partner_id, channel_id, value, last_updated)
                    SELECT 
                        c.customer_id,
                        c.partner_id,
                        dc.id as channel_id,
                        'NO' as value,
                        NOW() as last_updated
                    FROM {temp_customer_table} c
                    CROSS JOIN digital_delivery_channel dc
                    WHERE dc.status = 'active'
                """)

                # Step 4: Perform fast bulk insert from temp table
                cursor.execute(f"""
                    INSERT INTO customer_channel_subscription 
                        (customer_id, partner_id, channel_id, value, last_updated)
                    SELECT 
                        t.customer_id, t.partner_id, t.channel_id, t.value, t.last_updated
                    FROM {temp_result_table} t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM customer_channel_subscription ccs
                        WHERE ccs.customer_id = t.customer_id 
                        AND ccs.channel_id = t.channel_id
                    )
                """)

                row_count = cursor.rowcount
                conn.commit()
                return row_count

            except Exception as e:
                conn.rollback()
                self.logger.error(f"Error in subscription creation: {e}")
                raise
            finally:
                # Cleanup temp tables explicitly (though ON COMMIT DROP should handle this)
                try:
                    cursor.execute(
                        f"DROP TABLE IF EXISTS {temp_customer_table}")
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_result_table}")
                    conn.commit()
                except:
                    pass

    def _migrate_legacy_for_chunk_ultra_optimized(self, conn, offset: int, chunk_size: int) -> int:
        """ULTRA-OPTIMIZED: Migrate legacy data with hyper-optimized bulk operations"""
        with conn.cursor() as cursor:
            # Check if legacy table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'customer_digital_product'
                ) as table_exists
            """)

            if not cursor.fetchone()['table_exists']:
                return 0

            # Create a thread-specific temporary table for the customer batch
            thread_id = threading.get_ident()
            temp_batch_table = f"temp_batch_customers_{thread_id}"

            try:
                # Step 1: Create temp table for customer batch
                cursor.execute(f"""
                    CREATE TEMP TABLE {temp_batch_table} (
                        customer_id TEXT PRIMARY KEY
                    ) ON COMMIT DROP
                """)

                # Step 2: Load customer batch into temp table
                cursor.execute(f"""
                    INSERT INTO {temp_batch_table} (customer_id)
                    SELECT customer_id 
                    FROM res_partner 
                    WHERE customer_id IS NOT NULL AND customer_id != ''
                    ORDER BY id
                    LIMIT {chunk_size} OFFSET {offset}
                """)

                # Create index on temp table for better join performance
                cursor.execute(
                    f"CREATE INDEX ON {temp_batch_table} (customer_id)")

                # Step 3: Perform optimized update with advanced SQL
                cursor.execute(f"""
                    WITH channel_updates AS (
                        SELECT 
                            ccs.id as subscription_id,
                            CASE dc.code
                                WHEN 'ussd' THEN COALESCE(cdp.ussd, 'NO')
                                WHEN 'onebank' THEN COALESCE(cdp.onebank, 'NO')
                                WHEN 'carded_customer' THEN COALESCE(cdp.carded_customer, 'NO')
                                WHEN 'alt_bank' THEN COALESCE(cdp.alt_bank, 'NO')
                                WHEN 'sterling_pro' THEN COALESCE(cdp.sterling_pro, 'NO')
                                WHEN 'banca' THEN COALESCE(cdp.banca, 'NO')
                                WHEN 'doubble' THEN COALESCE(cdp.doubble, 'NO')
                                WHEN 'specta' THEN COALESCE(cdp.specta, 'NO')
                                WHEN 'switch' THEN COALESCE(cdp.switch, 'NO')
                                ELSE 'NO'
                            END as new_value
                        FROM customer_channel_subscription ccs
                        JOIN digital_delivery_channel dc ON ccs.channel_id = dc.id
                        JOIN {temp_batch_table} tbc ON ccs.customer_id = tbc.customer_id
                        JOIN customer_digital_product cdp ON ccs.customer_id = cdp.customer_id
                        WHERE dc.status = 'active'
                        AND (
                            (dc.code = 'ussd' AND cdp.ussd IS NOT NULL AND cdp.ussd != ccs.value) OR
                            (dc.code = 'onebank' AND cdp.onebank IS NOT NULL AND cdp.onebank != ccs.value) OR
                            (dc.code = 'carded_customer' AND cdp.carded_customer IS NOT NULL AND cdp.carded_customer != ccs.value) OR
                            (dc.code = 'alt_bank' AND cdp.alt_bank IS NOT NULL AND cdp.alt_bank != ccs.value) OR
                            (dc.code = 'sterling_pro' AND cdp.sterling_pro IS NOT NULL AND cdp.sterling_pro != ccs.value) OR
                            (dc.code = 'banca' AND cdp.banca IS NOT NULL AND cdp.banca != ccs.value) OR
                            (dc.code = 'doubble' AND cdp.doubble IS NOT NULL AND cdp.doubble != ccs.value) OR
                            (dc.code = 'specta' AND cdp.specta IS NOT NULL AND cdp.specta != ccs.value) OR
                            (dc.code = 'switch' AND cdp.switch IS NOT NULL AND cdp.switch != ccs.value)
                        )
                    )
                    UPDATE customer_channel_subscription ccs
                    SET value = cu.new_value, last_updated = NOW()
                    FROM channel_updates cu
                    WHERE ccs.id = cu.subscription_id
                """)

                row_count = cursor.rowcount
                conn.commit()
                return row_count

            except Exception as e:
                conn.rollback()
                self.logger.error(f"Error in legacy migration: {e}")
                raise
            finally:
                # Cleanup temp table explicitly
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_batch_table}")
                    conn.commit()
                except:
                    pass

    def _update_partners_for_chunk_ultra_optimized(self, conn, offset: int, chunk_size: int) -> int:
        """ULTRA-OPTIMIZED: Update partner relationships with hyper-optimized bulk operations"""
        with conn.cursor() as cursor:
            # Create a thread-specific temporary table
            thread_id = threading.get_ident()
            temp_batch_table = f"temp_partner_batch_{thread_id}"

            try:
                # Step 1: Create temp table for partner-customer mapping
                cursor.execute(f"""
                    CREATE TEMP TABLE {temp_batch_table} (
                        partner_id INTEGER,
                        customer_id TEXT
                    ) ON COMMIT DROP
                """)

                # Step 2: Load partner batch into temp table
                cursor.execute(f"""
                    INSERT INTO {temp_batch_table} (partner_id, customer_id)
                    SELECT id, customer_id 
                    FROM res_partner 
                    WHERE customer_id IS NOT NULL AND customer_id != ''
                    ORDER BY id
                    LIMIT {chunk_size} OFFSET {offset}
                """)

                # Create index on temp table for better join performance
                cursor.execute(
                    f"CREATE INDEX ON {temp_batch_table} (customer_id)")

                # Step 3: Perform bulk update with temp table
                cursor.execute(f"""
                    UPDATE customer_channel_subscription ccs
                    SET partner_id = tpb.partner_id, last_updated = NOW()
                    FROM {temp_batch_table} tpb
                    WHERE ccs.customer_id = tpb.customer_id
                    AND (ccs.partner_id IS NULL OR ccs.partner_id != tpb.partner_id)
                """)

                row_count = cursor.rowcount
                conn.commit()
                return row_count

            except Exception as e:
                conn.rollback()
                self.logger.error(f"Error in partner update: {e}")
                raise
            finally:
                # Cleanup temp table explicitly
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_batch_table}")
                    conn.commit()
                except:
                    pass

    def create_materialized_view(self):
        """Create materialized view with improved query"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # Check if view exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_matviews
                        WHERE matviewname = 'customer_digital_product_mat'
                    ) as view_exists
                """)

                if cursor.fetchone()['view_exists']:
                    self.logger.info(
                        "Materialized view already exists, checking for proper index...")

                    # Check if a proper unique index exists for concurrent refresh
                    cursor.execute("""
                        SELECT count(*) as suitable_indexes FROM pg_indexes 
                        WHERE tablename = 'customer_digital_product_mat'
                        AND indexdef LIKE 'CREATE UNIQUE INDEX%'
                        AND indexdef NOT LIKE '%WHERE%'
                    """)

                    has_suitable_index = cursor.fetchone()[
                        'suitable_indexes'] > 0

                    if not has_suitable_index:
                        self.logger.info(
                            "No suitable unique index found for concurrent refresh. Creating one...")
                        try:
                            # Try to create a proper unique index
                            cursor.execute("""
                                CREATE UNIQUE INDEX IF NOT EXISTS customer_digital_mat_id_unique_idx 
                                ON customer_digital_product_mat(id)
                            """)
                            conn.commit()
                            self.logger.info(
                                "Created unique index for concurrent refresh")
                            has_suitable_index = True
                        except Exception as e:
                            self.logger.warning(
                                f"Could not create unique index: {e}")

                    # Try to refresh the view
                    try:
                        if has_suitable_index:
                            # Try concurrent refresh
                            self.logger.info(
                                "Refreshing materialized view concurrently...")
                            cursor.execute(
                                "REFRESH MATERIALIZED VIEW CONCURRENTLY customer_digital_product_mat")
                        else:
                            # Fall back to regular refresh
                            self.logger.info(
                                "Refreshing materialized view (non-concurrent)...")
                            cursor.execute(
                                "REFRESH MATERIALIZED VIEW customer_digital_product_mat")

                        conn.commit()
                        self.logger.info(
                            "Materialized view refreshed successfully")
                        return "View refreshed successfully"
                    except Exception as e:
                        self.logger.warning(f"Could not refresh view: {e}")
                        self.logger.info("Will skip view refresh")
                        return "View refresh skipped"

                # View doesn't exist, so create it
                # Check if legacy table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = 'customer_digital_product'
                    ) as table_exists
                """)

                if not cursor.fetchone()['table_exists']:
                    self.logger.info(
                        "No legacy table, creating basic materialized view...")

                    # Create a more optimized basic view
                    cursor.execute("""
                        CREATE MATERIALIZED VIEW customer_digital_product_mat AS (
                            WITH distinct_customers AS (
                                SELECT DISTINCT customer_id 
                                FROM customer_channel_subscription
                                LIMIT 1000
                            )
                            SELECT 
                                ROW_NUMBER() OVER (ORDER BY customer_id) as id,
                                customer_id,
                                'Placeholder' as customer_name,
                                'Standard' as customer_segment,
                                NULL as ussd, NULL as onebank, NULL as carded_customer,
                                NULL as alt_bank, NULL as sterling_pro, NULL as banca,
                                NULL as doubble, NULL as specta, NULL as switch
                            FROM distinct_customers
                        ) WITH DATA
                    """)
                else:
                    # Create full featured view from legacy table
                    self.logger.info(
                        "Creating full materialized view from legacy table...")
                    cursor.execute("""
                        CREATE MATERIALIZED VIEW customer_digital_product_mat AS (
                            SELECT * FROM customer_digital_product
                        ) WITH DATA
                    """)

                # Create a proper unique index for concurrent refresh
                cursor.execute("""
                    CREATE UNIQUE INDEX customer_digital_mat_id_unique_idx 
                    ON customer_digital_product_mat(id)
                """)

                # Create additional indexes if needed
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS customer_digital_mat_customer_idx 
                    ON customer_digital_product_mat(customer_id)
                """)

                conn.commit()
                self.logger.info(
                    "Materialized view created with proper indexes")
                return "View created successfully"

        except Exception as e:
            self.logger.error(f"Materialized view creation failed: {e}")
            return f"View error: {e}"
        finally:
            self.return_connection(conn)

    def run_migration(self):
        """Run the complete migration process - ULTRA-OPTIMIZED"""
        start_time = time.time()

        try:
            self.logger.info(
                f"Starting ULTRA-OPTIMIZED migration with {self.num_workers} workers...")
            self.logger.info(f"System memory: {self.ram_gb:.1f}GB")

            # Step 1: Setup with more aggressive optimization
            self.create_connection_pool()
            self.create_initial_channels()

            # Step 1.5: Get migration parameters first to determine what to do
            params = self.get_migration_parameters()
            self.total_batches = params['total_batches']

            if params['total_customers'] == 0:
                self.logger.info("No customers found for migration")
                return

            # Only create indexes if we actually need to do work
            if not params['subscriptions_complete'] or params['existing_subscriptions'] < params['expected_subscriptions']:
                self.logger.info(
                    "Creating performance indexes (work needed)...")
                self.create_performance_indexes()
            else:
                self.logger.info(
                    "Skipping index creation (migration appears complete)")

            self.logger.info(
                f"ULTRA-OPTIMIZED Migration plan: {params['total_customers']:,} customers in "
                f"{params['total_batches']} batches of {params['batch_size']:,} using {self.num_workers} workers"
            )

            # Skip migration entirely if nothing to do
            if params['subscriptions_complete'] and params['existing_subscriptions'] >= params['expected_subscriptions']:
                self.logger.info(
                    "Migration appears to be already complete. Creating materialized view only...")
                view_result = self.create_materialized_view()
                elapsed_time = time.time() - start_time
                self.logger.info(
                    f"🎉 MIGRATION SKIPPED (already complete) in {elapsed_time:.2f} seconds! {view_result}")
                return

            # Step 3: Prepare batch jobs
            batch_jobs = []
            for batch_num in range(1, params['total_batches'] + 1):
                offset = (batch_num - 1) * params['batch_size']
                batch_jobs.append(
                    (batch_num, offset, params['batch_size'], params))

            # Step 4: Execute with workers - with optimized scheduling
            self.logger.info(
                f"Processing {len(batch_jobs)} batches with {self.num_workers} workers...")
            self.logger.info(
                f"Ultra-optimizations: Subscriptions complete: {params['subscriptions_complete']}, "
                f"Batch size: {params['batch_size']:,}, Chunk size: {params['chunk_size']:,}")

            # Performance monitoring
            batch_times = []
            start_processing = time.time()

            with ThreadPoolExecutor(max_workers=self.num_workers, thread_name_prefix='MigrationWorker') as executor:
                # Submit all batch jobs
                future_to_batch = {
                    executor.submit(self.process_batch_worker, batch_info): batch_info
                    for batch_info in batch_jobs
                }

                # Process completed batches
                for future in as_completed(future_to_batch):
                    batch_info = future_to_batch[future]
                    try:
                        batch_start = time.time()
                        batch_results = future.result()
                        batch_end = time.time()
                        batch_time = batch_end - batch_start
                        batch_times.append(batch_time)

                        # Update global stats
                        self.stats.update(
                            subscriptions=batch_results['subscriptions'],
                            legacy=batch_results['legacy'],
                            partners=batch_results['partners'],
                            batches=1,
                            errors=batch_results['errors']
                        )

                        # Performance metrics logging
                        if len(batch_times) > 1:
                            avg_time = sum(batch_times) / len(batch_times)
                            est_remaining = avg_time * \
                                (params['total_batches'] -
                                 self.completed_batches)
                            self.logger.info(
                                f"Batch {batch_info[0]} completed in {batch_time:.2f}s (avg: {avg_time:.2f}s, "
                                f"est. remaining: {est_remaining/60:.1f}min)"
                            )

                    except Exception as e:
                        self.logger.error(f"Batch {batch_info[0]} failed: {e}")
                        self.stats.update(errors=1)

            processing_time = time.time() - start_processing
            self.logger.info(
                f"All batches completed in {processing_time:.2f}s")

            # Step 5: Create materialized view
            self.logger.info("Creating materialized view...")
            view_result = self.create_materialized_view()

            # Step 6: Final cleanup and summary
            elapsed_time = time.time() - start_time
            final_stats = self.stats.get_stats()

            # Calculate processing rates for performance analysis
            customers_per_second = params['total_customers'] / \
                processing_time if processing_time > 0 else 0

            self.logger.info(
                f"🎉 MIGRATION COMPLETE in {elapsed_time:.2f} seconds!\n"
                f"   Batches completed: {final_stats['batches_completed']}/{params['total_batches']}\n"
                f"   Subscriptions created: {final_stats['subscriptions_created']:,}\n"
                f"   Legacy records migrated: {final_stats['legacy_migrated']:,}\n"
                f"   Partner links updated: {final_stats['partner_links_updated']:,}\n"
                f"   Performance: {customers_per_second:.1f} customers/second\n"
                f"   Errors: {final_stats['errors']}\n"
                f"   Materialized view: {view_result}"
            )

            # Return final stats for potential API consumers
            return {
                'success': True,
                'elapsed_time': elapsed_time,
                'stats': final_stats,
                'performance': {
                    'customers_per_second': customers_per_second,
                    'total_time': elapsed_time,
                    'processing_time': processing_time
                }
            }

        except Exception as e:
            self.logger.error(f"Migration failed: {e}")
            raise

        finally:
            if self.connection_pool:
                self.connection_pool.closeall()
                self.logger.info("Connection pool closed")


def main():
    """Main function with simplified command line argument support"""
    import argparse

    # Setup logging first
    setup_logging()

    parser = argparse.ArgumentParser(
        description='Ultra-Optimized Database Migration Script')
    parser.add_argument('--config', default=DEFAULT_CONFIG_FILE,
                        help='Configuration file path')

    args = parser.parse_args()

    logger.info(f"Starting migration script with config: {args.config}")

    # Create migrator instance with fixed 4 workers
    migrator = DatabaseMigrator(config_file=args.config)
    migrator.run_migration()


if __name__ == '__main__':
    main()
