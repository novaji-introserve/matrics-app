#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FDW Initial Load Script
Performs 7-day initial load for FDW-synced tables
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path for imports
SCRIPT_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(SCRIPT_DIR))

from scripts.fdw_sync.base_fdw_sync import BaseFDWSync
from utils.fdw_helpers import (
    get_db_config_from_env,
    get_config_path,
    get_all_table_names,
    update_watermark_in_odoo
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(SCRIPT_DIR / 'logs' / 'fdw_initial_load.log'),
        logging.StreamHandler()
    ]
)
_logger = logging.getLogger(__name__)


def sync_table(table_name: str, db_config: dict, initial_days: int = 7):
    """Sync a single table with initial load."""
    _logger.info(f"Starting initial load for table: {table_name}")
    
    try:
        # Get config path
        config_path = get_config_path(table_name)
        
        # Initialize sync engine
        sync_engine = BaseFDWSync(str(config_path), db_config)
        
        # Calculate date range
        date_to = datetime.now()
        date_from = date_to - timedelta(days=initial_days)
        
        _logger.info(f"Date range: {date_from} to {date_to}")
        
        # Get total row count
        total_rows = sync_engine.count_rows(date_from, date_to)
        _logger.info(f"Total rows to sync: {total_rows:,}")
        
        if total_rows == 0:
            _logger.info(f"No rows to sync for {table_name}")
            update_watermark_in_odoo(table_name, date_to, 0, db_config)
            return
        
        # Get batch size from config
        batch_size = sync_engine.batch_size
        total_batches = (total_rows + batch_size - 1) // batch_size
        
        _logger.info(f"Processing in {total_batches} batches of {batch_size:,} rows")
        
        # Process in batches
        total_synced = 0
        for batch_num in range(total_batches):
            offset = batch_num * batch_size
            
            _logger.info(f"Processing batch {batch_num + 1}/{total_batches} (offset: {offset:,})")
            
            rows_affected = sync_engine.execute_sync(
                date_from=date_from,
                date_to=date_to,
                limit=batch_size,
                offset=offset
            )
            
            total_synced += rows_affected
            progress = (batch_num + 1) / total_batches * 100
            _logger.info(f"Batch {batch_num + 1} completed: {rows_affected:,} rows. "
                        f"Total: {total_synced:,}/{total_rows:,} ({progress:.1f}%)")
        
        # Update watermark
        update_watermark_in_odoo(table_name, date_to, total_synced, db_config)
        
        _logger.info(f"Initial load completed for {table_name}: {total_synced:,} rows synced")
        
    except Exception as e:
        _logger.error(f"Error syncing {table_name}: {e}", exc_info=True)
        raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='FDW Initial Load Script')
    parser.add_argument('--table', type=str, help='Table name to sync (e.g., ActiveTransaction)')
    parser.add_argument('--all', action='store_true', help='Sync all tables')
    parser.add_argument('--days', type=int, default=7, help='Number of days to load (default: 7)')
    
    args = parser.parse_args()
    
    # Get database config
    db_config = get_db_config_from_env()
    _logger.info(f"Database: {db_config['database']}@{db_config['host']}")
    
    if args.all:
        # Sync all tables
        tables = get_all_table_names()
        _logger.info(f"Syncing {len(tables)} tables: {', '.join(tables)}")
        
        for table_name in tables:
            try:
                sync_table(table_name, db_config, args.days)
                _logger.info(f"Completed: {table_name}")
            except Exception as e:
                _logger.error(f"Failed to sync {table_name}: {e}")
                # Continue with next table
                continue
        
        _logger.info("All tables processed")
        
    elif args.table:
        # Sync single table
        sync_table(args.table, db_config, args.days)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()

