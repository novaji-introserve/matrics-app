#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base FDW Sync Class
Handles SQL JOIN-based syncing using PostgreSQL Foreign Data Wrappers
"""
import psycopg2
import logging
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

_logger = logging.getLogger(__name__)


class BaseFDWSync:
    """Base class for FDW-based data synchronization."""
    
    def __init__(self, config_path: str, db_config: Dict):
        """
        Initialize FDW sync engine.
        
        :param config_path: Path to FDW config JSON file
        :param db_config: Database connection config (host, port, database, user, password)
        """
        self.config_path = Path(config_path)
        with open(self.config_path, 'r') as f:
            self.config = json.load(f)
        
        self.db_config = db_config
        self.table_name = self.config['table_name']
        self.fdw_schema = self.config['fdw_schema']
        self.source_table = self.config['source_table']
        self.target_table = self.config['target_table']
        self.unique_column = self.config['unique_column']
        self.date_column = self.config.get('date_column')
        self.batch_size = self.config.get('batch_size', 50000)
        
        self.field_mappings = self.config.get('field_mappings', {})
        self.lookups = self.config.get('lookups', {})
        
        _logger.info(f"Initialized FDW sync for table: {self.table_name}")
    
    def get_connection(self):
        """Get PostgreSQL connection to target database."""
        return psycopg2.connect(
            host=self.db_config['host'],
            port=self.db_config.get('port', 5432),
            database=self.db_config['database'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )
    
    def build_select_columns(self) -> Tuple[str, List[str]]:
        """
        Build SELECT clause with JOINs for lookups.
        Returns: (SELECT clause, list of target columns)
        """
        select_parts = []
        target_columns = []
        
        # Add direct field mappings
        for source_field, target_field in self.field_mappings.items():
            select_parts.append(f"fdw.{source_field} AS {target_field}")
            target_columns.append(target_field)
        
        # Add lookup fields with JOINs
        lookup_aliases = {}
        for target_field, lookup_config in self.lookups.items():
            source_field = lookup_config['source_field']
            lookup_table = lookup_config['lookup_table']
            lookup_key = lookup_config['lookup_key']
            
            # Create alias for lookup table
            alias = f"{target_field}_lookup"
            lookup_aliases[target_field] = alias
            
            # Add to SELECT
            select_parts.append(f"{alias}.id AS {target_field}")
            target_columns.append(target_field)
        
        select_clause = ",\n    ".join(select_parts)
        return select_clause, target_columns, lookup_aliases
    
    def build_join_clauses(self, lookup_aliases: Dict) -> str:
        """Build LEFT JOIN clauses for lookups."""
        join_clauses = []
        
        for target_field, lookup_config in self.lookups.items():
            alias = lookup_aliases[target_field]
            source_field = lookup_config['source_field']
            lookup_table = lookup_config['lookup_table']
            lookup_key = lookup_config['lookup_key']
            
            # Handle Odoo model names (res.partner -> res_partner table)
            # Odoo models use dots, but tables use underscores
            if '.' in lookup_table:
                # Convert Odoo model name to table name
                table_name = lookup_table.replace('.', '_')
                table_ref = f'public.{table_name}'
            else:
                table_ref = f'public.{lookup_table}'
            
            join_clause = (
                f"LEFT JOIN {table_ref} {alias} "
                f"ON fdw.{source_field} = {alias}.{lookup_key}"
            )
            join_clauses.append(join_clause)
        
        return "\n".join(join_clauses) if join_clauses else ""
    
    def build_sync_query(self, date_from: Optional[datetime] = None, 
                        date_to: Optional[datetime] = None,
                        limit: Optional[int] = None,
                        offset: Optional[int] = None) -> Tuple[str, List]:
        """
        Build complete INSERT ... SELECT query with JOINs.
        
        Returns: (SQL query, parameters list)
        """
        # Build SELECT columns and JOINs
        select_clause, target_columns, lookup_aliases = self.build_select_columns()
        join_clauses = self.build_join_clauses(lookup_aliases)
        
        # Build WHERE clause
        where_parts = []
        params = []
        
        if date_from and self.date_column:
            where_parts.append(f"fdw.{self.date_column} >= %s")
            params.append(date_from)
        
        if date_to and self.date_column:
            where_parts.append(f"fdw.{self.date_column} < %s")
            params.append(date_to)
        
        where_clause = " AND ".join(where_parts) if where_parts else "1=1"
        
        # Build LIMIT/OFFSET
        limit_clause = ""
        if limit:
            limit_clause = f"LIMIT {limit}"
            if offset:
                limit_clause += f" OFFSET {offset}"
        
        # Build complete SELECT query
        select_query = f"""
SELECT
    {select_clause}
FROM {self.fdw_schema}.{self.source_table} fdw
{join_clauses}
WHERE {where_clause}
{limit_clause}
"""
        
        # Build INSERT with ON CONFLICT
        target_columns_str = ", ".join(target_columns)
        conflict_columns = [self.unique_column]
        
        # Build UPDATE clause for ON CONFLICT
        update_parts = []
        for col in target_columns:
            if col != self.unique_column:  # Don't update unique column
                update_parts.append(f"{col} = EXCLUDED.{col}")
        update_clause = ", ".join(update_parts)
        
        insert_query = f"""
INSERT INTO {self.target_table} ({target_columns_str})
{select_query}
ON CONFLICT ({self.unique_column}) DO UPDATE SET
    {update_clause}
"""
        
        return insert_query, params
    
    def execute_sync(self, date_from: Optional[datetime] = None,
                    date_to: Optional[datetime] = None,
                    limit: Optional[int] = None,
                    offset: Optional[int] = None) -> int:
        """
        Execute sync query and return number of rows affected.
        
        Returns: Number of rows inserted/updated
        """
        conn = None
        try:
            conn = self.get_connection()
            query, params = self.build_sync_query(date_from, date_to, limit, offset)
            
            _logger.info(f"Executing sync query for {self.table_name}...")
            _logger.debug(f"Query: {query[:500]}...")  # Log first 500 chars
            
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows_affected = cur.rowcount
                conn.commit()
                
                _logger.info(f"Sync completed: {rows_affected} rows affected")
                return rows_affected
                
        except Exception as e:
            if conn:
                conn.rollback()
            _logger.error(f"Error executing sync for {self.table_name}: {e}", exc_info=True)
            raise
        finally:
            if conn:
                conn.close()
    
    def count_rows(self, date_from: Optional[datetime] = None,
                   date_to: Optional[datetime] = None) -> int:
        """Count rows that would be synced."""
        conn = None
        try:
            conn = self.get_connection()
            
            where_parts = []
            params = []
            
            if date_from and self.date_column:
                where_parts.append(f"{self.date_column} >= %s")
                params.append(date_from)
            
            if date_to and self.date_column:
                where_parts.append(f"{self.date_column} < %s")
                params.append(date_to)
            
            where_clause = " AND ".join(where_parts) if where_parts else "1=1"
            
            query = f"""
SELECT COUNT(*) 
FROM {self.fdw_schema}.{self.source_table}
WHERE {where_clause}
"""
            
            with conn.cursor() as cur:
                cur.execute(query, params)
                count = cur.fetchone()[0]
                return count
                
        except Exception as e:
            _logger.error(f"Error counting rows for {self.table_name}: {e}", exc_info=True)
            raise
        finally:
            if conn:
                conn.close()

