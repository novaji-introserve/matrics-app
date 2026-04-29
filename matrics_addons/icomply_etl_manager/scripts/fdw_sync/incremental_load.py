#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FDW Incremental Load Script
Performs 5-minute incremental sync for FDW-synced tables
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path for imports
SCRIPT_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(SCRIPT_DIR))

from icomply_odoo.matrics_addons.icomply_etl_manager.scripts.fdw_sync.base_fdw_sync import BaseFDWSync
from icomply_odoo.matrics_addons.icomply_etl_manager.utils.fdw_helpers import (
    get_db_config_from_env,
    get_config_path,
    get_all_table_names,
    get_watermark_from_odoo,
    update_watermark_in_odoo
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(SCRIPT_DIR / 'logs' / 'fdw_incremental_load.log'),
        logging.StreamHandler()
    ]
)
_logger = logging.getLogger(__name__)


def sync_table(table_name: str, db_config: dict, incremental_minutes: int = 5):
    """Sync a single table with incremental load."""
    _logger.info(f"Starting incremental load for table: {table_name}")
    
    try:
        # Get config path
        config_path = get_config_path(table_name)
        
        # Initialize sync engine
        sync_engine = BaseFDWSync(str(config_path), db_config)
        
        # Get watermark (last sync time)
        watermark_str = get_watermark_from_odoo(table_name, db_config)
        
        if watermark_str:
            # Parse watermark
            if isinstance(watermark_str, str):
                date_from = datetime.fromisoformat(watermark_str.replace(' ', 'T'))
            else:
                date_from = watermark_str
            _logger.info(f"Last sync time: {date_from}")
        else:
            # No watermark - use incremental_minutes as fallback
            date_from = datetime.now() - timedelta(minutes=incremental_minutes)
            _logger.info(f"No watermark found, using last {incremental_minutes} minutes")
        
        # Calculate date range
        date_to = datetime.now()
        
        # If date_from is too old, limit to incremental_minutes
        max_lookback = datetime.now() - timedelta(minutes=incremental_minutes)
        if date_from < max_lookback:
            _logger.warning(f"Watermark is too old, limiting to last {incremental_minutes} minutes")
            date_from = max_lookback
        
        _logger.info(f"Date range: {date_from} to {date_to}")
        
        # Get row count
        row_count = sync_engine.count_rows(date_from, date_to)
        _logger.info(f"Rows to sync: {row_count:,}")
        
        if row_count == 0:
            _logger.info(f"No new rows for {table_name}")
            return 0
        
        # Execute sync (no batching for incremental - usually small)
        rows_affected = sync_engine.execute_sync(
            date_from=date_from,
            date_to=date_to
        )
        
        # Update watermark
        update_watermark_in_odoo(table_name, date_to, rows_affected, db_config)
        
        _logger.info(f"Incremental load completed for {table_name}: {rows_affected:,} rows synced")
        
        return rows_affected
        
    except Exception as e:
        _logger.error(f"Error syncing {table_name}: {e}", exc_info=True)
        raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='FDW Incremental Load Script')
    parser.add_argument('--table', type=str, help='Table name to sync')
    parser.add_argument('--all', action='store_true', help='Sync all tables')
    parser.add_argument('--minutes', type=int, default=5, help='Minutes to look back (default: 5)')
    
    args = parser.parse_args()
    
    # Get database config
    db_config = get_db_config_from_env()
    _logger.info(f"Database: {db_config['database']}@{db_config['host']}")
    
    if args.all:
        # Sync all tables
        tables = get_all_table_names()
        _logger.info(f"Syncing {len(tables)} tables")
        
        total_synced = 0
        for table_name in tables:
            try:
                rows = sync_table(table_name, db_config, args.minutes)
                total_synced += rows or 0
            except Exception as e:
                _logger.error(f"Failed to sync {table_name}: {e}")
                # Continue with next table
                continue
        
        _logger.info(f"All tables processed. Total rows synced: {total_synced:,}")
        
    elif args.table:
        # Sync single table
        sync_table(args.table, db_config, args.minutes)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()

