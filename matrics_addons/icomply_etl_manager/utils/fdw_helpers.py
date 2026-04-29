#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FDW Helper Functions
Utilities for FDW sync operations
"""
import os
import json
import logging
from pathlib import Path
from typing import Dict, Optional, List

_logger = logging.getLogger(__name__)


def get_db_config_from_env() -> Dict:
    """
    Get database configuration from environment variables.
    Used when running scripts standalone (not in Odoo).
    
    Environment variables are read from .env file (in same directory as docker-compose.yml).
    Docker Compose automatically loads .env file and makes variables available to containers.
    
    Required variables in .env file:
        DB_HOST=172.20.160.111          # Target database host (where Odoo runs)
        DB_PORT=5432                    # Target database port
        DB_NAME=iComply                 # Target database name
        DB_USER=icompy_novaji_user      # Target database user
        DB_PASSWORD=your_password       # Target database password
    
    This connects to the TARGET database (where foreign tables are created via FDW).
    """
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'odoo'),
        'user': os.getenv('DB_USER', 'odoo'),
        'password': os.getenv('DB_PASSWORD', 'odoo'),
    }


def get_config_path(table_name: str) -> Path:
    """
    Get path to FDW config file for a table.
    
    :param table_name: Name of the table (e.g., 'ActiveTransaction')
    :return: Path to config file
    """
    # Map table names to config file names
    config_map = {
        'ActiveTransaction': 'active_transactions_fdw.json',
        'Accounts': 'accounts_fdw.json',
        'ActiveLien': 'active_lien_fdw.json',
        'customer_profile': 'customer_profile_fdw.json',
        'AuditLog': 'audit_log_fdw.json',
    }
    
    config_filename = config_map.get(table_name)
    if not config_filename:
        # Try to construct from table name
        config_filename = f"{table_name.lower()}_fdw.json"
    
    # Get script directory and navigate to configs
    script_dir = Path(__file__).parent.parent
    config_path = script_dir / 'configs' / 'fdw_sync' / config_filename
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    return config_path


def get_all_table_names() -> List[str]:
    """Get list of all table names that have FDW configs."""
    script_dir = Path(__file__).parent.parent
    config_dir = script_dir / 'configs' / 'fdw_sync'
    
    if not config_dir.exists():
        return []
    
    tables = []
    for config_file in config_dir.glob('*_fdw.json'):
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
                tables.append(config['table_name'])
        except Exception as e:
            _logger.warning(f"Error reading config {config_file}: {e}")
    
    return tables


def get_watermark_from_odoo(table_name: str, db_config: Dict) -> Optional[str]:
    """
    Get watermark from Odoo database using direct SQL query.
    This is used when scripts run standalone (not in Odoo context).
    
    :param table_name: Name of the table
    :param db_config: Database connection config
    :return: Last sync time as string, or None if not found
    """
    import psycopg2
    
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config.get('port', 5432),
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT last_sync_time 
                FROM fdw_sync_watermark 
                WHERE table_name = %s
            """, (table_name,))
            
            result = cur.fetchone()
            if result:
                return result[0]
            return None
            
    except Exception as e:
        _logger.warning(f"Could not get watermark from Odoo: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def update_watermark_in_odoo(table_name: str, sync_time: str, 
                             rows_synced: Optional[int], db_config: Dict):
    """
    Update watermark in Odoo database using direct SQL query.
    
    :param table_name: Name of the table
    :param sync_time: Sync timestamp
    :param rows_synced: Number of rows synced
    :param db_config: Database connection config
    """
    import psycopg2
    from datetime import datetime
    
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config.get('port', 5432),
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        
        with conn.cursor() as cur:
            # Check if watermark exists
            cur.execute("""
                SELECT id FROM fdw_sync_watermark WHERE table_name = %s
            """, (table_name,))
            
            exists = cur.fetchone()
            
            if exists:
                # Update existing
                if rows_synced is not None:
                    cur.execute("""
                        UPDATE fdw_sync_watermark 
                        SET last_sync_time = %s, 
                            last_sync_rows = %s,
                            updated_at = %s
                        WHERE table_name = %s
                    """, (sync_time, rows_synced, datetime.now(), table_name))
                else:
                    cur.execute("""
                        UPDATE fdw_sync_watermark 
                        SET last_sync_time = %s,
                            updated_at = %s
                        WHERE table_name = %s
                    """, (sync_time, datetime.now(), table_name))
            else:
                # Insert new
                if rows_synced is not None:
                    cur.execute("""
                        INSERT INTO fdw_sync_watermark 
                        (table_name, last_sync_time, last_sync_rows, updated_at)
                        VALUES (%s, %s, %s, %s)
                    """, (table_name, sync_time, rows_synced, datetime.now()))
                else:
                    cur.execute("""
                        INSERT INTO fdw_sync_watermark 
                        (table_name, last_sync_time, updated_at)
                        VALUES (%s, %s, %s)
                    """, (table_name, sync_time, datetime.now()))
            
            conn.commit()
            _logger.info(f"Watermark updated for {table_name}: {sync_time}")
            
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
        _logger.error(f"Error updating watermark: {e}", exc_info=True)
        raise
    finally:
        if 'conn' in locals():
            conn.close()

