"""
FDW-based Postgres-to-Postgres sync engine.
This handles direct mappings and lookups for ETL syncs using Foreign Data Wrappers.
"""
import psycopg2
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json

_logger = logging.getLogger(__name__)


class FDWSyncEngine:
    """Engine for syncing data using PostgreSQL Foreign Data Wrappers."""
    
    def __init__(self, config_path: str, target_db_config: Dict):
        """
        Initialize FDW sync engine.
        
        :param config_path: Path to the ETL config JSON file
        :param target_db_config: Target database connection config
        """
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.target_db_config = target_db_config
        self.source_table = self.config['source_table_name']
        self.target_table = self.config['target_table_name']
        self.primary_key = self.config['primary_key_unique']
        self.date_column = self.config.get('incremental_date_column')
        self.mappings = self.config.get('mappings', [])
        
        # Build lookup cache for performance
        self.lookup_cache = {}
        
    def get_target_connection(self):
        """Get connection to target database."""
        return psycopg2.connect(
            host=self.target_db_config['host'],
            port=self.target_db_config['port'],
            database=self.target_db_config['database_name'],
            user=self.target_db_config['username'],
            password=self.target_db_config['password']
        )
    
    def build_lookup_cache(self, conn):
        """Pre-build lookup caches for all lookup mappings."""
        _logger.info("Building lookup caches...")
        
        for mapping in self.mappings:
            if mapping.get('mapping_type') == 'lookup':
                lookup_table = mapping['lookup_table']
                lookup_key = mapping['lookup_key']
                lookup_value = mapping['lookup_value']
                
                cache_key = f"{lookup_table}_{lookup_key}"
                if cache_key not in self.lookup_cache:
                    try:
                        with conn.cursor() as cur:
                            # Get table name (handle schema if present)
                            table_parts = lookup_table.split('.')
                            if len(table_parts) == 2:
                                schema, table = table_parts
                                query = f'SELECT "{lookup_key}", "{lookup_value}" FROM "{schema}"."{table}"'
                            else:
                                query = f'SELECT "{lookup_key}", "{lookup_value}" FROM "{lookup_table}"'
                            
                            cur.execute(query)
                            rows = cur.fetchall()
                            
                            # Build dict: lookup_key -> lookup_value
                            cache = {}
                            for row in rows:
                                key_val = row[0]
                                value_val = row[1]
                                if key_val is not None:
                                    cache[str(key_val)] = value_val
                            
                            self.lookup_cache[cache_key] = cache
                            _logger.info(f"Cached {len(cache)} entries for {cache_key}")
                    except Exception as e:
                        _logger.warning(f"Could not build cache for {cache_key}: {e}")
                        self.lookup_cache[cache_key] = {}
    
    def resolve_lookup(self, mapping: Dict, source_value: Any) -> Optional[Any]:
        """Resolve a lookup value from cache."""
        if not source_value:
            return None
            
        lookup_table = mapping['lookup_table']
        lookup_key = mapping['lookup_key']
        cache_key = f"{lookup_table}_{lookup_key}"
        
        cache = self.lookup_cache.get(cache_key, {})
        return cache.get(str(source_value))

