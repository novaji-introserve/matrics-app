# -------------------------------------------------------------
# Performance Configuration Constants - Fine tune as needed
# -------------------------------------------------------------
import multiprocessing


DEFAULT_BATCH_SIZE = 10000  # Increased from 2000
LARGE_TABLE_THRESHOLD = 500000  # When to use chunking
MAX_WORKER_PROCESSES = max(4, min(8, multiprocessing.cpu_count()))  # Adaptive based on CPU
MEMORY_THRESHOLD = 0.7  # 70% memory usage triggers optimization
CONNECTION_POOL_SIZE = 10  # Max connections per db
MAX_RETRIES = 3  # Number of retries for transient errors
MONITORING_INTERVAL = 60  # Seconds between status updates
TEMP_TABLE_PREFIX = "tmp_etl_"  # Prefix for temp tables
BASE_DIR = "/tmp/etl_manager/"  # Base directory for temp files
