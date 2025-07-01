import json
import psycopg2
import psycopg2.extras  # Import extras for RealDictCursor and execute_values
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
import json
from datetime import datetime, date, timezone # Ensure date is imported
import os



def setup_logging():
    # log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ETL.log')
    log_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ETL.log')
    # Check if file exists and exceeds size limit
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
    
setup_logging()
# Call this at the start of your script
logger = logging.getLogger("__CUSTOMER__")
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configs', 'customer_etl_config.json')
db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configs', 'db_config.json')



class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        # Handle datetime objects (includes timezone if present)
        if isinstance(obj, datetime):
            return obj.isoformat()
        # --- ADD THIS BLOCK ---
        # Handle date objects specifically
        elif isinstance(obj, date):
            return obj.isoformat() # Converts date to 'YYYY-MM-DD' string
        # --- END OF ADDED BLOCK ---
        # Handle Decimal
        elif isinstance(obj, Decimal):
            return str(obj)
        # Handle bytes
        elif isinstance(obj, bytes):
            try:
                return obj.decode('utf-8') # Try decoding as UTF-8
            except UnicodeDecodeError:
                return obj.hex() # Fallback to hex representation
        # Let the base class default method raise the TypeError for other unserializable types.
        return super().default(obj)


@dataclass
class TableConfig:
    source_table: str
    target_table: str
    primary_key: str  # Source primary key column name (lowercase)
    batch_size: int
    dependencies: List[str]
    mappings: Dict[str, Dict[str, Any]]

    def __post_init__(self):
        # Normalize names to lowercase for consistency
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
        """Gets the corresponding target primary key column name."""
        for source_col, mapping in self.mappings.items():
            if source_col == self.primary_key:
                return mapping['target']
        # Fallback if PK mapping isn't explicitly defined (assuming same name)
        logger.warning(
            f"Primary key '{self.primary_key}' not explicitly mapped for {self.source_table}. Assuming target PK has the same name.")
        return self.primary_key


class DatabaseError(Exception):
    """Custom exception for database-related errors"""
    pass


class LookupCache:
    """Thread-safe cache for lookup values (same as before)"""

    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()

    def get_cache_key(self, table: str, lookup_key: str, key_value: Any) -> str:
        # Use a tuple for the key components to handle different types naturally
        return f"{table}:{lookup_key}:{str(key_value)}"

    def get(self, table: str, lookup_key: str, key_value: Any, lookup_value_col: str) -> Optional[Any]:
        # Key needs to match how it's set
        cache_key = self.get_cache_key(table, lookup_key, key_value)
        with self._lock:
            # The inner dict stores {lookup_value_col: actual_value}
            cache_entry = self._cache.get(cache_key)
            if cache_entry:
                return cache_entry.get(lookup_value_col)
            return None

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

        # --- Updated Connection String Handling ---
        try:
            self.source_pg_conn_string = db_config['source_postgres']['connection_string'].format(
                **db_config['source_postgres'])
            self.target_pg_conn_string = db_config['target_postgres']['connection_string'].format(
                **db_config['target_postgres'])
        except KeyError as e:
            logger.error(
                f"Missing key in db_config.json: {e}. Ensure 'source_postgres' and 'target_postgres' sections exist with 'connection_string' and necessary parameters.")
            raise
        except Exception as e:
            logger.error(f"Error processing db_config.json: {e}")
            raise

        self.table_configs = {
            name.lower(): TableConfig(**config)  # Use **kwargs for cleaner dataclass init
            for name, config in etl_config['tables'].items()
        }

        self.lookup_cache = LookupCache()
        self.processed_tables: Set[str] = set()
        self.processed_tables_lock = Lock()

        # Initialize target database metadata tables
        self._initialize_target_database()

    def get_source_columns(self, source_cursor, table_name: str) -> List[str]:
        """Get all column names from source table (PostgreSQL) and normalize."""
        try:
            # Use LIMIT 0 for efficiency, no need for TOP
            source_cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
            return [col.name.lower() for col in source_cursor.description]
        except psycopg2.Error as e:
            logger.error(
                f"Error fetching columns for source table {table_name}: {e}")
            raise DatabaseError(
                f"Could not get columns for {table_name}") from e

    def _initialize_target_database(self):
        """Initialize metadata tables in the target database"""
        logger.info("Initializing target database metadata tables...")
        try:
            # Use target connection context manager
            with self.get_target_connection() as target_conn:
                with target_conn.cursor() as target_cursor:
                    self._create_sync_table(target_cursor)
                    self._create_log_table(target_cursor)
                    
                    # Add migration for existing tables - this is the new part
                    self._migrate_sync_table_schema(target_cursor)
                    
                    self._create_target_indexes(target_cursor)
                target_conn.commit()
            logger.info("Target database metadata tables initialized.")
        except Exception as e:
            logger.error(f"Failed to initialize target database: {e}")
            
        # Allow the script to continue, but log the failure
    def _migrate_sync_table_schema(self, target_cursor):
        """Add necessary columns to existing tables if they don't exist"""
        logger.info("Checking for required schema updates...")
        
        # Check if use_hash_table column exists
        target_cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'sync_timestamps' 
            AND column_name = 'use_hash_table';
        """)
        
        column_exists = target_cursor.fetchone() is not None
        
        if not column_exists:
            logger.info("Adding use_hash_table column to sync_timestamps table")
            target_cursor.execute("""
                ALTER TABLE sync_timestamps 
                ADD COLUMN use_hash_table BOOLEAN NOT NULL DEFAULT FALSE;
            """)
            
            logger.info("Modifying row_hashes column to allow NULL values")
            target_cursor.execute("""
                ALTER TABLE sync_timestamps 
                ALTER COLUMN row_hashes DROP NOT NULL;
            """)
        
        # Create the row_hashes table if it doesn't exist
        target_cursor.execute("""
            CREATE TABLE IF NOT EXISTS row_hashes (
                table_name VARCHAR(255) NOT NULL,
                row_key VARCHAR(255) NOT NULL,
                row_hash VARCHAR(64) NOT NULL,
                PRIMARY KEY (table_name, row_key)
            );
        """)
        
        # Create index if it doesn't exist
        target_cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_row_hashes_table
            ON row_hashes(table_name);
        """)
        
        logger.info("Schema migration completed successfully")


    
    # --- Context Managers for Connections ---

    @backoff.on_exception(
        backoff.expo,
        psycopg2.OperationalError,  # More specific for connection issues
        max_tries=5,
        max_time=60,
        logger=logger,
        on_backoff=lambda details: logger.warning(
            f"Connection failed. Retrying in {details['wait']:.1f} seconds... (Attempt {details['tries']})")
    )
    @contextmanager
    def get_source_connection(self):
        """Context manager for the source PostgreSQL database connection."""
        source_conn = None
        try:
            source_conn = psycopg2.connect(self.source_pg_conn_string)
            logger.debug("Source PostgreSQL connection established.")
            yield source_conn
        except psycopg2.Error as e:
            logger.error(f"Source PostgreSQL connection error: {e}")
            raise DatabaseError(
                "Failed to establish source database connection") from e
        finally:
            if source_conn:
                source_conn.close()
                logger.debug("Source PostgreSQL connection closed.")

    @backoff.on_exception(
        backoff.expo,
        psycopg2.OperationalError,
        max_tries=5,
        max_time=60,
        logger=logger,
        on_backoff=lambda details: logger.warning(
            f"Target connection failed. Retrying in {details['wait']:.1f} seconds... (Attempt {details['tries']})")
    )
    @contextmanager
    def get_target_connection(self):
        """Context manager for the target PostgreSQL database connection."""
        target_conn = None
        try:
            target_conn = psycopg2.connect(self.target_pg_conn_string)
            logger.debug("Target PostgreSQL connection established.")
            yield target_conn
        except psycopg2.Error as e:
            logger.error(f"Target PostgreSQL connection error: {e}")
            raise DatabaseError(
                "Failed to establish target database connection") from e
        finally:
            if target_conn:
                target_conn.close()
                logger.debug("Target PostgreSQL connection closed.")
    # --- End Context Managers ---

    # --- Metadata Table Creation (Target DB) ---
    def _create_sync_table(self, target_cursor):
        """Create the sync_timestamps table if it does not exist."""
        logger.debug("Ensuring sync_timestamps table exists...")
        target_cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_timestamps (
                table_name VARCHAR(255) PRIMARY KEY,
                sync_time TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- Use TIMESTAMPTZ
                row_hashes JSONB NOT NULL DEFAULT '{}'::jsonb
            );
        """)

    def _create_log_table(self, target_cursor):
        """Create the sync_log table if it does not exist."""
        logger.debug("Ensuring sync_log table exists...")
        target_cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_log (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                total_rows_processed INT NOT NULL,
                new_rows_inserted INT NOT NULL,
                rows_updated INT NOT NULL,
                rows_deleted INT NOT NULL DEFAULT 0, -- Added default, deletion logic needs implementation
                rows_skipped_errors INT NOT NULL DEFAULT 0,
                sync_start_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                sync_end_time TIMESTAMPTZ,
                duration_seconds FLOAT,
                status VARCHAR(50) NOT NULL DEFAULT 'STARTED', -- Track status
                error_message TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_sync_log_table_name
            ON sync_log(table_name);
            CREATE INDEX IF NOT EXISTS idx_sync_log_sync_time
            ON sync_log(sync_start_time);
        """)

    def _create_target_indexes(self, target_cursor):
        """Create necessary indexes on target tables"""
        logger.info("Ensuring necessary indexes exist on target tables...")
        for table_key, config in self.table_configs.items():
            try:
                target_pk = config.get_target_primary_key()
                # Ensure target_pk is valid before creating index
                if not target_pk:
                    logger.error(
                        f"Cannot determine target primary key for table '{config.target_table}'. Skipping index creation.")
                    continue

                index_name = f"idx_{config.target_table}_{target_pk}".replace(
                    '.', '_')  # Sanitize index name
                # Quote table and column names for safety
                create_index_query = f"""
                CREATE INDEX IF NOT EXISTS "{index_name}"
                ON "{config.target_table}"("{target_pk}");
                """
                logger.debug(f"Executing: {create_index_query}")
                target_cursor.execute(create_index_query)
            except (psycopg2.Error, ValueError) as e:
                logger.error(
                    f"Failed to create index for table {config.target_table} on column {target_pk}: {e}")
            except Exception as e:  # Catch unexpected errors
                logger.error(
                    f"Unexpected error creating index for table {config.target_table}: {e}")

    # --- Hashing and Sync Info (Target DB) ---

    def calculate_row_hash(self, row: Dict[str, Any]) -> str:
        """Calculate SHA-256 hash for a row dictionary."""
        # Ensure consistent key order and use the custom JSON encoder for types
        row_str = json.dumps(row, sort_keys=True, cls=DateTimeEncoder)
        return hashlib.sha256(row_str.encode('utf-8')).hexdigest()

    def get_last_sync_info(self, target_cursor, table_name: str) -> Tuple[datetime, Dict[str, str]]:
        """Get the last sync time and row hashes from the target sync table."""
        try:
            logger.debug(f"Fetching last sync info for {table_name} from target DB")
            
            # First check if the column exists to avoid errors
            target_cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'sync_timestamps' 
                AND column_name = 'use_hash_table';
            """)
            
            use_hash_table_exists = target_cursor.fetchone() is not None
            
            # Query based on schema version
            if use_hash_table_exists:
                target_cursor.execute(
                    "SELECT sync_time, row_hashes, use_hash_table FROM sync_timestamps WHERE table_name = %s",
                    (table_name,)
                )
            else:
                target_cursor.execute(
                    "SELECT sync_time, row_hashes FROM sync_timestamps WHERE table_name = %s",
                    (table_name,)
                )
                
            result = target_cursor.fetchone()

            if result:
                sync_time = result[0]
                row_hashes = result[1] if result[1] else {}
                
                # Check if we're using the new schema with hash table
                use_hash_table = False
                if use_hash_table_exists and len(result) > 2:
                    use_hash_table = result[2]
                
                # Get hashes from separate table if needed
                if use_hash_table:
                    logger.info(f"Fetching hashes from row_hashes table for {table_name}")
                    target_cursor.execute(
                        "SELECT row_key, row_hash FROM row_hashes WHERE table_name = %s",
                        (table_name,)
                    )
                    hash_rows = target_cursor.fetchall()
                    row_hashes = {row[0]: row[1] for row in hash_rows}
                    logger.debug(f"Fetched {len(row_hashes)} hashes from row_hashes table")
                    
                logger.debug(f"Found last sync info for {table_name}: time={sync_time}, {len(row_hashes)} hashes")
                
                # Ensure sync_time is offset-aware if it's TIMESTAMPTZ
                if isinstance(sync_time, datetime) and sync_time.tzinfo is None:
                    logger.warning(f"Sync time for {table_name} is timezone-naive. Assuming UTC.")
                    from datetime import timezone
                    sync_time = sync_time.replace(tzinfo=timezone.utc)
                    
                return sync_time, row_hashes

            logger.info(f"No previous sync info found for {table_name}, using default start date {self.default_start_date}")
            return self.default_start_date, {}

        except psycopg2.Error as e:
            logger.error(f"Error getting last sync info for {table_name}: {e}")
            raise DatabaseError(f"Failed to get sync info for {table_name}") from e
        except Exception as e:
            logger.error(f"Unexpected error getting last sync info for {table_name}: {e}")
            raise
    
    # def update_sync_info(self, target_cursor, table_name: str, sync_time: datetime, current_hashes: Dict[str, str]):
    #     """Update sync information in the target sync_timestamps table."""
    #     try:
    #         # Check if we have the new schema
    #         target_cursor.execute("""
    #             SELECT column_name 
    #             FROM information_schema.columns 
    #             WHERE table_name = 'sync_timestamps' 
    #             AND column_name = 'use_hash_table';
    #         """)
            
    #         use_hash_table_exists = target_cursor.fetchone() is not None
    #         hash_count = len(current_hashes)
    #         logger.debug(f"Updating sync info for {table_name} with {hash_count} hashes.")
            
    #         # For large hash sets, use the row_hashes table if available
    #         if hash_count > 100000 and use_hash_table_exists:
    #             logger.info(f"Using row_hashes table for {table_name} with {hash_count} hashes")
                
    #             # First, update the sync_timestamps entry with no hashes
    #             target_cursor.execute("""
    #                 INSERT INTO sync_timestamps (table_name, sync_time, row_hashes, use_hash_table)
    #                 VALUES (%s, %s, NULL, TRUE)
    #                 ON CONFLICT (table_name)
    #                 DO UPDATE SET
    #                     sync_time = EXCLUDED.sync_time,
    #                     row_hashes = NULL,
    #                     use_hash_table = TRUE;
    #             """, (
    #                 table_name,
    #                 sync_time,
    #             ))
                
    #             # Delete any existing hashes for this table
    #             target_cursor.execute("DELETE FROM row_hashes WHERE table_name = %s", (table_name,))
                
    #             # Insert new hashes in batches
    #             batch_size = 10000
    #             rows_to_insert = []
    #             count = 0
                
    #             for key, hash_value in current_hashes.items():
    #                 rows_to_insert.append((table_name, key, hash_value))
    #                 count += 1
    #                 if len(str(key)) > 250:  # Leave some buffer
    #                     logger.warning(f"Key too long for row_hashes table: {key[:50]}... ({len(str(key))} chars)")
    #                     continue
    #                 rows_to_insert.append((table_name, key, hash_value))
                    
    #                 # Insert in batches to avoid memory issues
    #                 if count % batch_size == 0:
    #                     psycopg2.extras.execute_values(
    #                         target_cursor,
    #                         "INSERT INTO row_hashes (table_name, row_key, row_hash) VALUES %s",
    #                         rows_to_insert
    #                     )
    #                     logger.debug(f"Inserted batch of {len(rows_to_insert)} hashes")
    #                     rows_to_insert = []
                
    #             # Insert any remaining rows
    #             if rows_to_insert:
    #                 psycopg2.extras.execute_values(
    #                     target_cursor,
    #                     "INSERT INTO row_hashes (table_name, row_key, row_hash) VALUES %s",
    #                     rows_to_insert
    #                 )
    #                 logger.debug(f"Inserted final batch of {len(rows_to_insert)} hashes")
    #         else:
    #             # Use the original JSONB approach for smaller tables or if new schema not available
    #             logger.debug(f"Using JSONB storage for {table_name} hashes")
                
    #             if use_hash_table_exists:
    #                 target_cursor.execute("""
    #                     INSERT INTO sync_timestamps (table_name, sync_time, row_hashes, use_hash_table)
    #                     VALUES (%s, %s, %s, FALSE)
    #                     ON CONFLICT (table_name)
    #                     DO UPDATE SET
    #                         sync_time = EXCLUDED.sync_time,
    #                         row_hashes = EXCLUDED.row_hashes,
    #                         use_hash_table = FALSE;
    #                 """, (
    #                     table_name,
    #                     sync_time,
    #                     json.dumps(current_hashes)
    #                 ))
    #             else:
    #                 # Original code path for systems without schema update
    #                 target_cursor.execute("""
    #                     INSERT INTO sync_timestamps (table_name, sync_time, row_hashes)
    #                     VALUES (%s, %s, %s)
    #                     ON CONFLICT (table_name)
    #                     DO UPDATE SET
    #                         sync_time = EXCLUDED.sync_time,
    #                         row_hashes = EXCLUDED.row_hashes;
    #                 """, (
    #                     table_name,
    #                     sync_time,
    #                     json.dumps(current_hashes)
    #                 ))
                    
    #         logger.debug(f"Sync info updated SUCCESSFULLY for {table_name}.")
    #     except psycopg2.Error as e:
    #         logger.error(f"Error updating sync_timestamps for {table_name}: {e}")
    #         raise DatabaseError(f"Failed to update sync info for {table_name}") from e
    #     except Exception as e:
    #         logger.error(f"Unexpected error updating sync info for {table_name}: {e}")
    #         raise    
    
    def update_sync_info(self, target_cursor, table_name: str, sync_time: datetime, current_hashes: Dict[str, str]):
        """Update sync information in the target sync_timestamps table."""
        try:
            # Check if we have the new schema
            target_cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'sync_timestamps' 
                AND column_name = 'use_hash_table';
            """)
            
            use_hash_table_exists = target_cursor.fetchone() is not None
            hash_count = len(current_hashes)
            logger.debug(f"Updating sync info for {table_name} with {hash_count} hashes.")
            
            # For large hash sets, use the row_hashes table if available
            if hash_count > 100000 and use_hash_table_exists:
                logger.info(f"Using row_hashes table for {table_name} with {hash_count} hashes")
                
                # First, update the sync_timestamps entry with no hashes
                target_cursor.execute("""
                    INSERT INTO sync_timestamps (table_name, sync_time, row_hashes, use_hash_table)
                    VALUES (%s, %s, NULL, TRUE)
                    ON CONFLICT (table_name)
                    DO UPDATE SET
                        sync_time = EXCLUDED.sync_time,
                        row_hashes = NULL,
                        use_hash_table = TRUE;
                """, (
                    table_name,
                    sync_time,
                ))
                
                # Delete any existing hashes for this table and commit immediately
                # to ensure deletion completes before insertion
                target_cursor.execute("DELETE FROM row_hashes WHERE table_name = %s", (table_name,))
                target_cursor.connection.commit()
                logger.info(f"Deleted existing hashes for {table_name}")
                
                # Insert new hashes in batches
                batch_size = 5000
                rows_to_insert = []
                count = 0
                # Track keys to prevent duplicates within the current process
                processed_keys = set()
                
                for key, hash_value in current_hashes.items():
                    # Skip duplicates
                    if key in processed_keys:
                        logger.warning(f"Skipping duplicate key in hashes: {key}")
                        continue
                    
                    processed_keys.add(key)
                    rows_to_insert.append((table_name, str(key), hash_value))
                    count += 1
                    
                    # Insert in batches to avoid memory issues
                    if count % batch_size == 0:
                        try:
                            # Use DO NOTHING to handle any remaining duplicates
                            psycopg2.extras.execute_values(
                                target_cursor,
                                """
                                INSERT INTO row_hashes (table_name, row_key, row_hash) 
                                VALUES %s 
                                ON CONFLICT (table_name, row_key) DO NOTHING
                                """,
                                rows_to_insert
                            )
                            logger.debug(f"Inserted batch of {len(rows_to_insert)} hashes")
                            target_cursor.connection.commit()
                            rows_to_insert = []
                        except Exception as e:
                            logger.error(f"Error inserting hash batch: {e}")
                            target_cursor.connection.rollback()
                            # Continue with next batch instead of failing entire process
                            rows_to_insert = []
                
                # Insert any remaining rows
                if rows_to_insert:
                    try:
                        psycopg2.extras.execute_values(
                            target_cursor,
                            """
                            INSERT INTO row_hashes (table_name, row_key, row_hash) 
                            VALUES %s 
                            ON CONFLICT (table_name, row_key) DO NOTHING
                            """,
                            rows_to_insert
                        )
                        logger.debug(f"Inserted final batch of {len(rows_to_insert)} hashes")
                        target_cursor.connection.commit()
                    except Exception as e:
                        logger.error(f"Error inserting final hash batch: {e}")
                        target_cursor.connection.rollback()
                
                logger.info(f"Completed hash insertion for {table_name}")
            else:
                # Use the original JSONB approach for smaller tables or if new schema not available
                logger.debug(f"Using JSONB storage for {table_name} hashes")
                
                if use_hash_table_exists:
                    target_cursor.execute("""
                        INSERT INTO sync_timestamps (table_name, sync_time, row_hashes, use_hash_table)
                        VALUES (%s, %s, %s, FALSE)
                        ON CONFLICT (table_name)
                        DO UPDATE SET
                            sync_time = EXCLUDED.sync_time,
                            row_hashes = EXCLUDED.row_hashes,
                            use_hash_table = FALSE;
                    """, (
                        table_name,
                        sync_time,
                        json.dumps(current_hashes)
                    ))
                else:
                    # Original code path for systems without schema update
                    target_cursor.execute("""
                        INSERT INTO sync_timestamps (table_name, sync_time, row_hashes)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (table_name)
                        DO UPDATE SET
                            sync_time = EXCLUDED.sync_time,
                            row_hashes = EXCLUDED.row_hashes;
                    """, (
                        table_name,
                        sync_time,
                        json.dumps(current_hashes)
                    ))
                    
            logger.debug(f"Sync info updated SUCCESSFULLY for {table_name}.")
        except psycopg2.Error as e:
            logger.error(f"Error updating sync_timestamps for {table_name}: {e}")
            raise DatabaseError(f"Failed to update sync info for {table_name}") from e
        except Exception as e:
            logger.error(f"Unexpected error updating sync info for {table_name}: {e}")
            raise
    
    def log_sync_start(self, target_cursor, table_name: str) -> int:
        """Log the start of a sync cycle and return the log ID."""
        try:
            target_cursor.execute("""
                INSERT INTO sync_log (table_name, total_rows_processed, new_rows_inserted, rows_updated, status)
                VALUES (%s, 0, 0, 0, 'STARTED') RETURNING id;
            """, (table_name,))
            log_id = target_cursor.fetchone()[0]
            logger.info(
                f"Started sync log entry {log_id} for table {table_name}")
            return log_id
        except psycopg2.Error as e:
            logger.error(
                f"Failed to create start log entry for {table_name}: {e}")
            return -1  # Indicate failure

    def log_sync_end(
        self, target_cursor, log_id: int, table_name: str, stats: Dict[str, int],
        start_time: float, status: str = "COMPLETED", error_message: Optional[str] = None
    ):
        """Log synchronization completion details into the sync_log table."""
        end_time = time.time()
        duration = end_time - start_time
        try:
            target_cursor.execute("""
                UPDATE sync_log
                SET total_rows_processed = %s,
                    new_rows_inserted = %s,
                    rows_updated = %s,
                    rows_deleted = %s,
                    rows_skipped_errors = %s,
                    sync_end_time = NOW(),
                    duration_seconds = %s,
                    status = %s,
                    error_message = %s
                WHERE id = %s;
            """, (
                stats.get('total_rows', 0),
                stats.get('new_rows', 0),
                stats.get('updated_rows', 0),
                stats.get('deleted_rows', 0),
                stats.get('error_rows', 0),
                duration,
                status,
                error_message,
                log_id
            ))
            logger.info(
                f"Completed sync log entry {log_id} for table {table_name} with status: {status}. Duration: {duration:.2f}s")
        except psycopg2.Error as e:
            logger.error(
                f"Failed to update end log entry {log_id} for {table_name}: {e}")

    # --- Dependency Resolution and Main Execution ---

    def get_dependencies(self, table_name: str) -> Set[str]:
        """Recursively get all dependencies for a table (same as before)."""
        deps = set()
        config = self.table_configs.get(table_name.lower())
        if not config:
            logger.error(
                f"Configuration not found for table '{table_name}' while checking dependencies.")
            return deps  # Return empty set if config missing
        for dep in config.dependencies:
            if dep.lower() not in self.table_configs:
                logger.warning(
                    f"Dependency '{dep}' for table '{table_name}' not found in main configuration. Skipping.")
                continue
            deps.add(dep.lower())
            # Recurse with lowercase
            deps.update(self.get_dependencies(dep.lower()))
        return deps

    def resolve_table_order(self) -> List[str]:
        """Resolve the order in which tables should be processed based on dependencies."""
        processed: List[str] = []
        # Already lowercase from init
        unprocessed = set(self.table_configs.keys())
        processing_stack = set()  # To detect cycles

        while unprocessed:
            made_progress = False
            nodes_to_process = list(unprocessed)  # Process a stable list copy
            for table in nodes_to_process:
                config = self.table_configs[table]
                # Check if all dependencies are already processed
                if all(dep.lower() in processed for dep in config.dependencies):
                    processed.append(table)
                    unprocessed.remove(table)
                    made_progress = True
                    # Break and restart scan to potentially unlock other tables sooner
                    # break # Optional: may improve speed slightly in some graphs

            if not made_progress:
                # If no progress and still unprocessed tables, check for cycles
                # Find a node whose dependencies are not met but are in unprocessed
                problematic_nodes = []
                for table in unprocessed:
                    unmet_deps = [
                        dep for dep in self.table_configs[table].dependencies if dep.lower() in unprocessed]
                    if unmet_deps:
                         problematic_nodes.append(
                             f"{table} (depends on: {', '.join(unmet_deps)})")

                cycle_info = ", ".join(
                    problematic_nodes) if problematic_nodes else str(unprocessed)
                raise ValueError(
                    f"Circular dependency or missing dependency detected. Unable to resolve order. Check tables: {cycle_info}")

        logger.info(f"Resolved table processing order: {processed}")
        return processed

    @backoff.on_exception(
        backoff.expo,
        # Catch DB errors or unexpected value issues
        (psycopg2.Error, ValueError),
        max_tries=3,
        logger=logger,
        on_backoff=lambda details: logger.warning(
            f"Lookup failed. Retrying in {details['wait']:.1f} seconds... (Attempt {details['tries']})")
    )
    def lookup_value(
        self, target_cursor, table: str,
        key_column: str, value_column: str,
        key_value: Any  # Key value can be various types
    ) -> Optional[Any]:
        """Look up a value in the target PostgreSQL database with caching and retry."""
        # Handle typical "empty" values that shouldn't be looked up
        if key_value is None or (isinstance(key_value, str) and not key_value.strip()):
            logger.debug(
                f"Skipping lookup in {table} for null/empty key value.")
            return None

        # Attempt to fetch from cache first
        cached_value = self.lookup_cache.get(
            table, key_column, key_value, value_column)
        if cached_value is not None:
            logger.debug(
                f"Cache hit for lookup: {table}.{key_column}={key_value} -> {value_column}")
            return cached_value

        logger.debug(
            f"Cache miss. Querying target DB: SELECT {value_column} FROM {table} WHERE {key_column} = %s with value {key_value} ({type(key_value)})")
        try:
            # Quote table and column names for safety
            query = f'SELECT "{value_column}" FROM "{table}" WHERE "{key_column}" = %s'
            # Pass value directly, psycopg2 handles type adaptation
            target_cursor.execute(query, (key_value,))
            result = target_cursor.fetchone()

            if result:
                found_value = result[0]
                logger.debug(
                    f"Lookup successful: {table}.{key_column}={key_value} -> {value_column}={found_value}")
                self.lookup_cache.set(
                    table, key_column, key_value, value_column, found_value)
                return found_value
            else:
                logger.warning(
                    f"Lookup failed: No matching record found in target table '{table}' for {key_column} = '{key_value}'")
                # Cache the failure (None) to avoid repeated lookups for the same missing key
                self.lookup_cache.set(
                    table, key_column, key_value, value_column, None)
                return None
        except psycopg2.Error as e:
            logger.error(
                f"Database error during lookup in {table} for {key_column}={key_value}: {e}")
            # Re-raise after logging to trigger backoff or fail the row
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error during lookup in {table} for {key_column}={key_value}: {e}")
            raise

    def transform_value(self, mapping: dict, source_value: Any, target_cursor) -> Any:
        """Transform a value based on mapping configuration using target DB lookups."""
        transform_type = mapping.get(
            'type', 'direct')  # Default to direct mapping

        # Handle Nulls early - return None if source is None, regardless of type
        if source_value is None:
            return None

        # Apply stripping for strings before any transformation
        if isinstance(source_value, str):
            processed_value = source_value.strip()
            # Handle empty strings after stripping - often treated like NULL in source systems
            if not processed_value:
                return None  # Treat empty strings as null after stripping
        else:
            processed_value = source_value

        # Apply transformations
        if transform_type == 'direct':
            return processed_value
        elif transform_type == 'lookup':
            try:
                # Perform lookup using the processed value
                lookup_result = self.lookup_value(
                    target_cursor,
                    mapping['lookup_table'].lower(),
                    mapping['lookup_key'].lower(),
                    mapping['lookup_value'].lower(),
                    processed_value  # Use the potentially stripped value
                )
                # Return the lookup result (could be None if lookup failed)
                return lookup_result
            except KeyError as e:
                logger.error(
                    f"Lookup configuration missing key: {e} in mapping {mapping}. Returning None.")
                return None
            except Exception as e:
                # Log specific lookup failure, return None to avoid row failure
                logger.warning(
                    f"Lookup failed for table '{mapping.get('lookup_table', 'N/A')}', "
                    f"key '{mapping.get('lookup_key', 'N/A')}' = '{processed_value}'. Error: {e}. Setting target value to NULL."
                )
                return None
        # Add other transformation types here if needed
        # elif transform_type == 'calculated':
        #    ...
        else:
            logger.error(
                f"Unknown transformation type: {transform_type} in mapping {mapping}")
            raise ValueError(f"Unknown transformation type: {transform_type}")

    def run_etl_cycle(self):
        """Runs a single cycle of the ETL process for all configured tables."""
        cycle_start_time = time.time()
        logger.info("=" * 60)
        logger.info(f"Starting ETL cycle at {datetime.now()}")
        logger.info("=" * 60)

        try:
            # Clear cache at the start of each cycle
            self.lookup_cache.clear()
            # Reset processed tables for the current cycle
            self.processed_tables.clear()

            table_order = self.resolve_table_order()
            logger.info(f"Processing tables in resolved order: {table_order}")

            # Determine max workers - min(cpu_count, num_tables, hard_limit)
            # import os
            # max_workers = min(len(table_order), os.cpu_count() or 1, 8) # Limit to 8 workers max
            # Keep original limit for now
            max_workers = min(len(table_order), 6)
            logger.info(f"Using up to {max_workers} worker threads.")

            failed_tables = set()
            processed_count = 0

            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="ETLWorker") as executor:
                # Submit tasks respecting dependencies implicitly by processing order
                # (though explicit dependency check is still in process_table)
                futures = {executor.submit(
                    self.process_table, table_name): table_name for table_name in table_order}

                for future in as_completed(futures):
                    table_name = futures[future]
                    try:
                        # result() will raise exceptions from the worker thread
                        future.result()
                        processed_count += 1
                        logger.info(
                            f"SUCCESSFULLY completed processing for table: {table_name}")
                    except Exception as e:
                        failed_tables.add(table_name)
                        # Log the exception traceback for detailed debugging
                        # Use logger.exception
                        logger.exception(
                            f"FAILED processing table '{table_name}': {e}")

            if failed_tables:
                failed_list = ", ".join(sorted(list(failed_tables)))
                logger.error(
                    f"ETL cycle completed with failures in tables: {failed_list}")
                # Optionally raise an exception to halt the scheduler if critical failures occur
                # raise RuntimeError(f"ETL cycle failed for tables: {failed_list}")
            else:
                logger.info(
                    f"ETL cycle completed SUCCESSFULLY for {processed_count} tables.")

        except ValueError as e:  # Catch dependency resolution errors
            logger.exception(f"ETL configuration error: {e}")
             # Stop if config is wrong
            raise
        except Exception as e:
            logger.exception(f"Unhandled exception during ETL cycle: {e}")
            # Depending on requirements, might want to raise this
            # raise

        finally:
            cycle_end_time = time.time()
            total_time = cycle_end_time - cycle_start_time
            logger.info("-" * 60)
            logger.info(
                f"ETL cycle finished at {datetime.now()}. Duration: {total_time:.2f} seconds.")
            logger.info("-" * 60)
            # Ensure cache is cleared even if errors occurred mid-cycle
            self.lookup_cache.clear()


    def process_table(self, table_name: str):
        """Processes a single table: fetches source data, transforms, and upserts to target."""
        # Ensure dependencies are met *before* starting processing this table
        # Assume table_name is lowercase
        config = self.table_configs[table_name]
        with self.processed_tables_lock:
            if table_name in self.processed_tables:
                logger.info(
                    f"[{table_name}] Table already processed in this cycle. Skipping.")
                return
            # Check dependencies
            unmet_deps = [
                dep for dep in config.dependencies if dep not in self.processed_tables]
            if unmet_deps:
                logger.warning(
                    f"[{table_name}] Attempted to process but dependencies not yet met: {unmet_deps}. Waiting...")
                while any(dep not in self.processed_tables for dep in config.dependencies):
                    time.sleep(0.5)
                logger.info(
                    f"[{table_name}] Dependencies {unmet_deps} are now met. Proceeding.")

        logger.info(f"--- [{table_name}] Starting processing ---")
        process_start_time = time.time()
        log_id = -1  # Initialize log_id

        # Initialize stats
        stats = {
            'total_rows': 0, 'new_rows': 0, 'updated_rows': 0,
            'deleted_rows': 0, 'error_rows': 0
        }
        current_target_hashes = {}  # Hashes based on *transformed* data for the target table

        try:
            # Use separate context managers for clarity and independent retry
            with self.get_source_connection() as source_conn, \
                    self.get_target_connection() as target_conn:

                # Use RealDictCursor for source for easy dict access
                source_cursor = source_conn.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor)
                target_cursor = target_conn.cursor()  # Standard cursor for target ops

                # Start logging in target DB
                log_id = self.log_sync_start(target_cursor, table_name)
                target_conn.commit()  # Commit log start immediately

                # 1. Get available source columns
                available_source_columns = self.get_source_columns(
                    source_cursor, config.source_table)

                # 2. Determine columns to actually SELECT based on mappings
                columns_to_select = []
                valid_mappings = {}
                for source_col, mapping in config.mappings.items():
                    if source_col in available_source_columns:
                        columns_to_select.append(source_col)
                        valid_mappings[source_col] = mapping
                    else:
                        logger.warning(
                            f"[{table_name}] Mapped source column '{source_col}' not found in source table "
                            f"'{config.source_table}'. It will be skipped."
                        )
                if not columns_to_select:
                    raise ValueError(
                        f"[{table_name}] No valid mapped columns found after checking source.")

                # Ensure source primary key is selected (needed for context/logging even if not target PK)
                source_pk_col = config.primary_key
                if source_pk_col not in columns_to_select:
                    if source_pk_col in available_source_columns:
                        columns_to_select.append(source_pk_col)
                        logger.debug(
                            f"[{table_name}] Added source PK '{source_pk_col}' to select list.")
                    else:
                        raise ValueError(
                            f"[{table_name}] Source primary key '{source_pk_col}' not found in source table '{config.source_table}'. Cannot process table.")

                # 3. Get last sync info from target DB
                last_sync_time, last_target_hashes = self.get_last_sync_info(
                    target_cursor, table_name)
                logger.info(
                    f"[{table_name}] Last sync time: {last_sync_time}. Found {len(last_target_hashes)} previous hashes.")

                # 4. Fetch data from source in batches
                select_cols_str = ', '.join(
                    [f'"{col}"' for col in columns_to_select])
                source_query = f'SELECT {select_cols_str} FROM "{config.source_table}"'
                logger.debug(
                    f"[{table_name}] Executing source query: {source_query}")

                # --- Execute Source Query ---
                # Consider using server-side cursor for very large tables if client memory is an issue
                # source_cursor = source_conn.cursor(name='large_table_cursor', cursor_factory=psycopg2.extras.RealDictCursor, scrollable=True)
                source_cursor.execute(source_query)
                logger.info(
                    f"[{table_name}] Source query executed. Starting fetch loop.")
                # --- End Execute Source Query ---

                # --- Initialize Loop Variables ---
                total_fetched_count = 0  # Counter for total rows fetched across all batches
                batch_fetch_num = 0     # Counter for fetchmany calls
                # --- End Initialize Loop Variables ---

                # ==============================================
                # === MAIN FETCH AND PROCESS LOOP START      ===
                # ==============================================
                while True:
                    batch_fetch_num += 1  # Increment fetch counter
                    logger.info(
                        f"[{table_name}] Attempting to fetch batch #{batch_fetch_num} (Batch Size: {config.batch_size})...")
                    fetch_start_time = time.time()

                    # --- Fetch Data ---
                    source_rows = source_cursor.fetchmany(config.batch_size)
                    # --- End Fetch Data ---

                    fetch_duration = time.time() - fetch_start_time

                    if not source_rows:
                        logger.info(
                            f"[{table_name}] No more rows from source after {batch_fetch_num-1} fetches. Total fetched: {total_fetched_count}.")
                        break  # Exit the while loop

                    rows_in_this_fetch = len(source_rows)
                    total_fetched_count += rows_in_this_fetch
                    logger.info(
                        f"[{table_name}] Fetched batch #{batch_fetch_num} with {rows_in_this_fetch} rows in {fetch_duration:.2f}s. Total fetched so far: {total_fetched_count}")
                    setup_logging()


                    # Dictionary for deduplicating rows within this fetched batch based on the target conflict key
                    batch_upsert_candidates = {}
                    rows_processed_in_fetch = 0
                    # Time processing for this specific fetch
                    process_batch_start_time = time.time()

                    # --- Process Rows within the Fetched Batch ---
                    for idx, source_row_dict in enumerate(source_rows):
                        rows_processed_in_fetch += 1

                        # Optional: Log progress every N rows within a batch (useful for very large batch sizes)
                        # Log approx every 10%
                        if idx > 0 and config.batch_size > 500 and idx % (config.batch_size // 10) == 0:
                            logger.debug(
                                f"[{table_name}] Processing row {idx+1}/{rows_in_this_fetch} within fetch #{batch_fetch_num}...")

                        try:
                            # Get original source PK for context/logging
                            source_pk_value = source_row_dict.get(
                                source_pk_col)

                            # --- Transform Row ---
                            transformed_row = {}
                            has_transform_error = False
                            # Variable to hold the value of the target ON CONFLICT column
                            target_conflict_key_value = None
                            # Get the name (e.g., 'short_code' or 'id')
                            target_conflict_key_name = config.get_target_primary_key()

                            for source_col, mapping in valid_mappings.items():
                                try:
                                    # Get value using the correct case from the query result
                                    source_value = source_row_dict.get(
                                        source_col)
                                    transformed_value = self.transform_value(
                                        mapping, source_value, target_cursor)
                                    target_col_name = mapping['target']
                                    transformed_row[target_col_name] = transformed_value
                                    # Capture the value used for ON CONFLICT check
                                    if target_col_name == target_conflict_key_name:
                                        target_conflict_key_value = transformed_value
                                except Exception as transform_err:
                                    logger.error(
                                        f"[{table_name}] Error transforming column '{source_col}' for source PK {source_pk_value}: {transform_err}. Setting target to NULL.")
                                    # Set target to NULL on transform error
                                    transformed_row[mapping['target']] = None
                                    has_transform_error = True  # Flag row if needed

                            if has_transform_error:
                                # Decide action for rows with transform errors (log already happened)
                                pass  # Continue processing the row with NULLs for failed transforms

                            # --- Deduplication Logic ---
                            if target_conflict_key_value is None:
                                logger.error(
                                    f"[{table_name}] Value for target conflict key '{target_conflict_key_name}' is NULL after transformation for source PK {source_pk_value}. Cannot deduplicate or upsert. Skipping row.")
                                stats['error_rows'] += 1
                                continue  # Skip this row, cannot use None as dict key for deduplication

                            # Use the target conflict key value for deduplication within the batch
                            # The last row encountered for a given key in this fetch will overwrite previous ones
                            batch_upsert_candidates[target_conflict_key_value] = transformed_row
                            # --- End Deduplication Logic ---

                        except Exception as row_err:
                            stats['error_rows'] += 1
                            logger.exception(
                                f"[{table_name}] Unhandled error processing source row (PK: {source_pk_value}): {source_row_dict}. Error: {row_err}. Skipping row.")
                            # Continue to next row in the fetch

                    # --- End Processing Rows within the Fetched Batch ---

                    process_batch_duration = time.time() - process_batch_start_time
                    unique_candidates_count = len(batch_upsert_candidates)
                    logger.info(f"[{table_name}] Finished processing {rows_processed_in_fetch} fetched rows in {process_batch_duration:.2f}s. Found {unique_candidates_count} unique candidates for upsert (based on '{target_conflict_key_name}').")

                    # --- Prepare Final Batch for Upsert (Check Hashes) ---
                    rows_to_upsert_this_batch = []
                    if unique_candidates_count > 0:
                        logger.debug(
                            f"[{table_name}] Comparing {unique_candidates_count} unique candidates against previous hashes...")
                        hash_check_start_time = time.time()
                        for target_pk_value, final_row_for_pk in batch_upsert_candidates.items():
                            # Increment total *processed* count here (post-deduplication)
                            stats['total_rows'] += 1

                            # Calculate hash based on the *final transformed* data
                            current_row_hash = self.calculate_row_hash(
                                final_row_for_pk)

                            # Store hash using string representation of the TARGET PK for consistency
                            str_pk_value_for_hash = str(target_pk_value)
                            current_target_hashes[str_pk_value_for_hash] = current_row_hash

                            # Compare with last known hash for this target PK
                            last_hash = last_target_hashes.get(
                                str_pk_value_for_hash)

                            if last_hash is None:
                                # Target primary key not seen before -> New row
                                stats['new_rows'] += 1
                                rows_to_upsert_this_batch.append(
                                    final_row_for_pk)
                                # logger.debug(f"[{table_name}] New row added to batch: PK={str_pk_value_for_hash}")
                            elif last_hash != current_row_hash:
                                # Hash mismatch -> Updated row
                                stats['updated_rows'] += 1
                                rows_to_upsert_this_batch.append(
                                    final_row_for_pk)
                                # logger.debug(f"[{table_name}] Updated row added to batch: PK={str_pk_value_for_hash}")
                            else:
                                # Hashes match -> No change, skip upsert for this row
                                # logger.debug(f"[{table_name}] Row unchanged: PK={str_pk_value_for_hash}")
                                pass
                            hash_check_duration = time.time() - hash_check_start_time
                            logger.info(
                                f"[{table_name}] Hash comparison completed in {hash_check_duration:.2f}s. Found {len(rows_to_upsert_this_batch)} rows requiring insert/update.")
                    # --- End Prepare Final Batch for Upsert ---

                    # --- Upsert the Deduplicated and Changed Batch ---
                    if rows_to_upsert_this_batch:
                        upsert_start_time = time.time()
                        rows_in_upsert_batch = len(rows_to_upsert_this_batch)
                        logger.info(
                            f"[{table_name}] Upserting deduplicated batch of {rows_in_upsert_batch} rows...")
                        try:
                            # Call the batch upsert function
                            self.batch_upsert_rows(
                                target_cursor, config, rows_to_upsert_this_batch)
                            upsert_duration = time.time() - upsert_start_time

                            # Commit the transaction for this batch
                            commit_start_time = time.time()
                            target_conn.commit()
                            commit_duration = time.time() - commit_start_time

                            logger.info(
                                f"[{table_name}] Batch upsert ({upsert_duration:.2f}s) and commit ({commit_duration:.2f}s) successful for {rows_in_upsert_batch} rows.")

                        except Exception as upsert_err:
                            logger.exception(
                                f"[{table_name}] Error during batch upsert after deduplication. Rolling back batch transaction.")
                            try:
                                    target_conn.rollback()  # Rollback the failed batch
                            except Exception as rb_err:
                                logger.error(
                                    f"[{table_name}] Error during rollback attempt: {rb_err}")
                            # Re-raise the error to be caught by the main try/except for the table
                            raise upsert_err
                    else:
                        logger.info(
                            f"[{table_name}] No rows in this batch required insert/update after deduplication and hash check.")
                    # --- End Upsert the Deduplicated and Changed Batch ---

                # ==============================================
                # === MAIN FETCH AND PROCESS LOOP END        ===
                # ==============================================

                # 6. Handle Deletions (Optional - Implement if needed)
                # ... (Keep existing deletion logic placeholder if you have it) ...
                logger.info(
                    f"[{table_name}] Deletion check skipped (or logic not implemented).")

                # 7. Update sync info in target DB
                logger.info(
                    f"[{table_name}] Updating final sync information...")
                sync_update_start = time.time()
                current_sync_time = datetime.now()  # Use timezone-aware UTC time
                self.update_sync_info(
                    target_cursor, table_name, current_sync_time, current_target_hashes)
                target_conn.commit()  # Commit sync info update
                logger.info(
                    f"[{table_name}] Sync info update committed in {time.time() - sync_update_start:.2f}s.")

                # 8. Finalize log entry
                if log_id != -1:
                    logger.info(
                        f"[{table_name}] Finalizing success log entry {log_id}...")
                    self.log_sync_end(
                        target_cursor, log_id, table_name, stats, process_start_time, status="COMPLETED")
                    target_conn.commit()

                logger.info(
                    f"--- [{table_name}] FINISHED Processing --- "
                    f"Total Checked: {stats['total_rows']}, New Inserted: {stats['new_rows']}, "
                    f"Updated: {stats['updated_rows']}, Deleted(Detected): {stats['deleted_rows']}, "
                    f"Errors: {stats['error_rows']}. Duration: {time.time() - process_start_time:.2f}s"
                )

        except (DatabaseError, psycopg2.Error, ValueError) as e:
            logger.exception(
                f"[{table_name}] CRITICAL ERROR during processing: {e}")
            # Ensure transaction is rolled back on error
            try:
                if 'target_conn' in locals() and target_conn and not target_conn.closed:
                    logger.info(
                        f"[{table_name}] Rolling back target transaction due to error.")
                    target_conn.rollback()
            except Exception as rb_err:
                logger.error(
                    f"[{table_name}] Error during rollback after critical error: {rb_err}")

            # Update log entry to FAILED status if possible
            if log_id != -1:
                logger.info(
                    f"[{table_name}] Attempting to update log entry {log_id} to FAILED status.")
                try:
                    # Need a new connection/cursor for logging the failure if the main one failed
                    with self.get_target_connection() as target_conn_err:
                        with target_conn_err.cursor() as target_cursor_err:
                            self.log_sync_end(target_cursor_err, log_id, table_name, stats, process_start_time,
                                                status="FAILED", error_message=f"{type(e).__name__}: {e}")
                            target_conn_err.commit()
                        logger.info(
                            f"[{table_name}] Successfully updated log entry {log_id} to FAILED.")
                except Exception as log_err:
                    logger.error(
                        f"[{table_name}] FAILED TO UPDATE LOG STATUS to FAILED for log_id {log_id}: {log_err}")
            # Re-raise the exception to be caught by the main loop/executor
            raise

        except Exception as e:  # Catch any other unexpected errors
            logger.exception(
                f"[{table_name}] UNHANDLED CRITICAL EXCEPTION during processing: {e}")
            if 'target_conn' in locals() and target_conn and not target_conn.closed:
                logger.info(
                        f"[{table_name}] Rolling back target transaction due to unhandled exception.")
                target_conn.rollback()
                if log_id != -1:
                    logger.info(
                        f"[{table_name}] Attempting to update log entry {log_id} to FAILED status after unhandled exception.")
                    try:
                        with self.get_target_connection() as target_conn_err:
                            with target_conn_err.cursor() as target_cursor_err:
                                self.log_sync_end(target_cursor_err, log_id, table_name, stats, process_start_time,
                                                status="FAILED", error_message=f"Unhandled Exception: {e}")
                                target_conn_err.commit()
                            logger.info(
                                f"[{table_name}] Successfully updated log entry {log_id} to FAILED.")
                    except Exception as log_err:
                        logger.error(
                            f"[{table_name}] FAILED TO UPDATE LOG STATUS to FAILED for log_id {log_id} after unhandled exception: {log_err}")
                    raise

        finally:
            # Close cursors if they were opened (connections are closed by context manager)
            if 'source_cursor' in locals() and source_cursor and not source_cursor.closed:
                source_cursor.close()
                logger.debug(f"[{table_name}] Source cursor closed.")
            if 'target_cursor' in locals() and target_cursor and not target_cursor.closed:
                target_cursor.close()
                logger.debug(f"[{table_name}] Target cursor closed.")

            # Mark table as processed *after* all operations, including potential errors
            with self.processed_tables_lock:
                self.processed_tables.add(table_name)
            logger.debug(f"[{table_name}] Marked table as processed.")

    def batch_upsert_rows(self, target_cursor, config: TableConfig, rows: List[Dict[str, Any]]):
        """Batch upsert rows into the target PostgreSQL table using execute_values."""
        if not rows:
            return

        target_pk = config.get_target_primary_key()
        if not target_pk:
            raise ValueError(
                f"Cannot perform upsert for table {config.target_table}: Target primary key could not be determined.")

        # Get column names from the first row (assume all rows have same keys)
        # Ensure consistent order
        columns = sorted(rows[0].keys())
        column_names_quoted = ', '.join([f'"{col}"' for col in columns])
        target_table_quoted = f'"{config.target_table}"'
        target_pk_quoted = f'"{target_pk}"'

        # Build the SET clause for the ON CONFLICT update
        update_sets = []
        for col in columns:
            # Don't update the PK itself
            if col.lower() != target_pk.lower():
                # Quote column name and use EXCLUDED alias
                update_sets.append(f'"{col}" = EXCLUDED."{col}"')

        if not update_sets:
            # This happens if the table *only* has a primary key. Handle as INSERT...ON CONFLICT DO NOTHING.
            update_clause = "DO NOTHING"
            logger.warning(
                f"[{config.target_table}] No columns to update besides PK. Using ON CONFLICT DO NOTHING.")
        else:
            update_clause = f"DO UPDATE SET {', '.join(update_sets)}"

        # Construct the optimized INSERT query for execute_values
        # Note the single %s placeholder for execute_values
        upsert_query = f"""
            INSERT INTO {target_table_quoted} ({column_names_quoted})
            VALUES %s
            ON CONFLICT ({target_pk_quoted})
            {update_clause};
        """
        logger.debug(f"[{config.target_table}] Upsert query: {upsert_query}")

        # Prepare data as a list of tuples in the correct column order
        values_list = []
        for row in rows:
            try:
                # Ensure row has all expected columns and values are in correct order
                values_list.append(tuple(row.get(col) for col in columns))
            except Exception as e:
                logger.error(
                    f"Error preparing row for batch upsert: {row}. Error: {e}")
                # Decide how to handle: skip row, raise error? For now, log and skip.
                stats['error_rows'] += 1  # Assuming stats is accessible or passed in
                continue  # Skip this row

        if not values_list:
            logger.warning(f"[{config.target_table}] No valid rows to upsert after preparation.")
            return

        try:
            # Use execute_values for efficient bulk upsert
            psycopg2.extras.execute_values(
                target_cursor,
                upsert_query,
                values_list,
                template=None,  # Let execute_values handle template based on list of tuples
                page_size=min(len(values_list), 1000)  # Adjust page_size based on row size/memory
            )
            logger.debug(
                f"[{config.target_table}] execute_values successful for {len(values_list)} rows.")
        except psycopg2.Error as e:
            logger.error(
                f"Database error during batch upsert into {config.target_table}: {e}")
            logger.error(f"Failed Query (template): {upsert_query}")
            # Log first few problem rows data for debugging
            for i, row_vals in enumerate(values_list[:5]):
                try:
                    problem_row = dict(zip(columns, row_vals))
                    logger.error(
                         f"Problem row data (first {i+1}): {json.dumps(problem_row, cls=DateTimeEncoder, indent=2)}")
                except Exception as json_err:
                    logger.error(f"Problem row data (first {i+1}, raw tuple): {row_vals} - JSON log error: {json_err}")
            raise  # Re-raise the error to signify batch failure
        except Exception as e:
            logger.exception(
                f"Unexpected error during batch upsert using execute_values for {config.target_table}: {e}")
            raise


if __name__ == "__main__":
    logger.info("Starting ETL Script for a single run...")
    exit_code = 0  # Default to success
    try:
        # --- Configuration Loading ---
        with open(db_path, 'r') as f:
            db_config = json.load(f)
        logger.info("Database configuration loaded.")

        with open(config_path, 'r') as f:
            etl_config = json.load(f)
        logger.info("ETL configuration loaded.")

        # --- Configuration Validation ---
        if 'source_postgres' not in db_config or 'target_postgres' not in db_config:
            raise ValueError(
                "db_config.json must contain 'source_postgres' and 'target_postgres' sections.")
        if 'tables' not in etl_config:
            raise ValueError(
                "etl_config.json must contain a 'tables' section.")

        # --- ETL Manager Initialization ---
        etl_manager = ETLManager(db_config, etl_config)

        # --- Single ETL Cycle Execution ---
        logger.info("Executing the ETL cycle...")
        etl_manager.run_etl_cycle()  # Execute the cycle just once
        logger.info("ETL cycle finished.")  # Log completion

    except FileNotFoundError as e:
        logger.error(
            f"Configuration file not found: {e}. Please ensure db_config.json and etl_config.json exist.")
        exit_code = 1  # Indicate failure
    except (ValueError, KeyError) as e:  # Catch config validation errors
        logger.error(f"Configuration Error: {e}")
        exit_code = 1  # Indicate failure
    except Exception as e:
        # This will now catch errors from run_etl_cycle() as well as setup
        logger.exception(
            f"ETL script encountered an error during execution: {e}")
        exit_code = 1  # Indicate failure

    finally:
        # Log final exit status
        if exit_code == 0:
            logger.info("ETL script finished successfully.")
        else:
            logger.error(
                f"ETL script finished with errors (exit code {exit_code}).")
        # Use exit() to ensure the script terminates with the correct status code
        exit(exit_code)
