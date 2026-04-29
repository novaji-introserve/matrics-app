#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FDW Scheduler
Runs incremental sync every 5 minutes for all FDW-synced tables
"""
import sys
import time
import logging
import signal
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
SCRIPT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SCRIPT_DIR))

from icomply_odoo.matrics_addons.icomply_etl_manager.utils.fdw_helpers import get_all_table_names, get_db_config_from_env
from icomply_odoo.matrics_addons.icomply_etl_manager.scripts.fdw_sync.incremental_load import sync_table

# Setup logging
LOG_DIR = SCRIPT_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'fdw_scheduler.log'),
        logging.StreamHandler()
    ]
)
_logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown = False


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown
    _logger.info("Received shutdown signal, stopping scheduler...")
    shutdown = True


def main():
    """Main scheduler loop."""
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Get database config
    db_config = get_db_config_from_env()
    _logger.info(f"FDW Scheduler started. Database: {db_config['database']}@{db_config['host']}")
    
    # Get all tables
    tables = get_all_table_names()
    _logger.info(f"Monitoring {len(tables)} tables: {', '.join(tables)}")
    
    # Scheduler loop
    interval = 300  # 5 minutes in seconds
    _logger.info(f"Scheduler will run every {interval // 60} minutes")
    
    while not shutdown:
        try:
            cycle_start = datetime.now()
            _logger.info("=" * 60)
            _logger.info(f"Starting sync cycle at {cycle_start}")
            
            total_synced = 0
            for table_name in tables:
                if shutdown:
                    break
                
                try:
                    _logger.info(f"Syncing {table_name}...")
                    rows = sync_table(table_name, db_config, incremental_minutes=5)
                    total_synced += rows or 0
                except Exception as e:
                    _logger.error(f"Error syncing {table_name}: {e}", exc_info=True)
                    # Continue with next table
                    continue
            
            cycle_end = datetime.now()
            duration = (cycle_end - cycle_start).total_seconds()
            _logger.info(f"Cycle completed in {duration:.1f}s. Total rows synced: {total_synced:,}")
            
            # Wait for next cycle (unless shutdown)
            if not shutdown:
                _logger.info(f"Waiting {interval // 60} minutes until next cycle...")
                # Sleep in small increments to check shutdown flag
                for _ in range(interval):
                    if shutdown:
                        break
                    time.sleep(1)
            
        except KeyboardInterrupt:
            _logger.info("Keyboard interrupt received")
            shutdown = True
        except Exception as e:
            _logger.error(f"Unexpected error in scheduler: {e}", exc_info=True)
            # Wait a bit before retrying
            if not shutdown:
                time.sleep(60)
    
    _logger.info("FDW Scheduler stopped")


if __name__ == '__main__':
    main()

