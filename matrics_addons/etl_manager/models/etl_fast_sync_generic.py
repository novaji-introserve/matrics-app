# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import logging
import time
import gc
from datetime import datetime
from decimal import Decimal
import json
import hashlib
import os
import csv

_logger = logging.getLogger(__name__)

class ETLFastSyncGeneric(models.AbstractModel):
    _name = 'etl.fast.sync.generic'
    _description = 'Generic Fast ETL for Cross-Database Sync'
    
    @api.model
    def sync_data(self, table_config):
        """
        Generic optimized ETL process for syncing between different database types
        using CSV files as intermediate format
        """
        start_time = time.time()
        stats = {
            'total_rows': 0,
            'new_rows': 0,
            'updated_rows': 0,
            'unchanged_rows': 0,
            'error_rows': 0,
            'execution_time': 0
        }
        
        # Get connector service
        connector_service = self.env['etl.database.connector.service']
        source_db = table_config.source_db_connection_id
        target_db = table_config.target_db_connection_id
        
        # Get configuration
        config = table_config.get_config_json()
        source_table = config['source_table']
        target_table = config['target_table']
        primary_key = config['primary_key']
        
        # Get table size to determine best approach
        try:
            table_size = connector_service.get_table_count(source_db, source_table)
            _logger.info(f"Generic fast sync for table {source_table} with {table_size} rows")
            
            # For very large tables, use file-based transfer with chunking
            if table_size > 100000:
                stats = self._sync_with_csv_chunks(
                    connector_service, source_db, target_db, 
                    config, table_config, primary_key, table_size
                )
            else:
                # For smaller tables, use direct memory transfer
                stats = self._sync_with_direct_transfer(
                    connector_service, source_db, target_db, 
                    config, table_config, primary_key
                )
        except Exception as e:
            _logger.error(f"Error determining table size: {str(e)}")
            # Fall back to direct memory transfer
            stats = self._sync_with_direct_transfer(
                connector_service, source_db, target_db, 
                config, table_config, primary_key
            )
        
        stats['execution_time'] = time.time() - start_time
        return stats
    
    def _sync_with_csv_chunks(self, connector_service, source_db, target_db, config, table_config, primary_key, table_size):
        """Transfer data using CSV files as intermediate format, in chunks"""
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'unchanged_rows': 0, 'error_rows': 0}
        
        try:
            # Calculate optimal chunk size based on table size
            if table_size > 1000000:  # > 1M rows
                chunk_size = 100000
            elif table_size > 500000:  # > 500K rows
                chunk_size = 50000
            else:
                chunk_size = 20000
            
            # Get source columns and prepare mappings
            source_columns = connector_service.get_columns(source_db, config['source_table'])[0]
            
            # Map source to target columns
            column_map = {}
            target_columns = []
            
            for source_col, mapping in config['mappings'].items():
                if isinstance(mapping, dict) and mapping.get('target'):
                    original_col = source_columns.get(source_col.lower())
                    if original_col:
                        column_map[original_col] = mapping['target'].lower()
                        target_columns.append(mapping['target'].lower())
            
            # Process in chunks
            offset = 0
            total_chunks = (table_size + chunk_size - 1) // chunk_size
            current_chunk = 0
            
            while offset < table_size:
                current_chunk += 1
                _logger.info(f"Processing chunk {current_chunk}/{total_chunks} (offset {offset}, size {chunk_size})")
                
                # Create unique filenames for this chunk
                csv_filename = f"/tmp/etl_export_{table_config.id}_{current_chunk}_{int(time.time())}.csv"
                
                # Extract data from source
                self._extract_to_csv(
                    connector_service, source_db, config['source_table'],
                    column_map, offset, chunk_size, csv_filename
                )
                
                # Load data to target
                chunk_stats = self._load_from_csv(
                    connector_service, target_db, config['target_table'],
                    target_columns, primary_key, csv_filename
                )
                
                # Update stats
                stats['total_rows'] += chunk_stats['total_rows']
                stats['new_rows'] += chunk_stats['new_rows']
                stats['updated_rows'] += chunk_stats['updated_rows']
                
                # Clean up CSV file
                try:
                    os.remove(csv_filename)
                except:
                    pass
                
                # Update offset for next chunk
                offset += chunk_size
                
                # Force garbage collection
                gc.collect()
            
            return stats
            
        except Exception as e:
            _logger.error(f"Error in CSV chunk sync: {str(e)}")
            
            # Clean up CSV file if it exists
            try:
                if 'csv_filename' in locals() and os.path.exists(csv_filename):
                    os.remove(csv_filename)
            except:
                pass
                
            raise
    
    def _extract_to_csv(self, connector_service, source_db, source_table, column_map, offset, limit, csv_filename):
        """Extract data from source database to CSV file"""
        import csv
        
        # Get source database type
        db_type = source_db.db_type_code
        
        # Build query based on database type
        if db_type == 'postgresql':
            query = f"""
                SELECT {', '.join([f'"{col}"' for col in column_map.keys()])}
                FROM "{source_table}" 
                ORDER BY "{list(column_map.keys())[0]}" 
                LIMIT {limit} OFFSET {offset}
            """
        elif db_type == 'mssql':
            query = f"""
                SELECT {', '.join([f'[{col}]' for col in column_map.keys()])}
                FROM [{source_table}] 
                ORDER BY [{list(column_map.keys())[0]}] 
                OFFSET {offset} ROWS
                FETCH NEXT {limit} ROWS ONLY
            """
        elif db_type == 'mysql':
            query = f"""
                SELECT {', '.join([f'`{col}`' for col in column_map.keys()])}
                FROM `{source_table}` 
                ORDER BY `{list(column_map.keys())[0]}`
                LIMIT {limit} OFFSET {offset}
            """
        else:
            # Generic query
            query = f"""
                SELECT {', '.join(column_map.keys())}
                FROM {source_table} 
                LIMIT {limit} OFFSET {offset}
            """
        
        # Execute query
        source_data = connector_service.execute_query(source_db, query)
        
        if not source_data:
            return 0
        
        # Write to CSV with correct mapping
        with open(csv_filename, 'w', newline='') as csvfile:
            # Use target column names as headers
            fieldnames = [column_map[col] for col in column_map.keys()]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Write rows with mapped column names
            for row in source_data:
                transformed_row = {}
                for source_col, target_col in column_map.items():
                    transformed_row[target_col] = row.get(source_col)
                writer.writerow(transformed_row)
        
        return len(source_data)
    
    def _load_from_csv(self, connector_service, target_db, target_table, target_columns, primary_key, csv_filename):
        """Load data from CSV file to target database with UPSERT operation"""
        import csv
        
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0}
        
        # Get target database type
        db_type = target_db.db_type_code
        
        # Read CSV into memory for processing
        rows = []
        with open(csv_filename, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                rows.append(row)
        
        stats['total_rows'] = len(rows)
        
        # Process based on database type
        if db_type == 'postgresql':
            # Use PostgreSQL's COPY and INSERT ON CONFLICT
            with connector_service.cursor(target_db) as cursor:
                # Create temp table
                temp_table = f"tmp_load_{int(time.time())}"
                cursor.execute(f'CREATE TEMPORARY TABLE "{temp_table}" (LIKE "{target_table}")')
                
                # Use COPY to load data
                # copy_sql = f'COPY "{temp_table}" ({", ".join([f\'"{col}"\' for col in target_columns])}) FROM STDIN WITH CSV HEADER'
                columns_part = ", ".join(f'"{col}"' for col in target_columns)
                copy_sql = f'COPY "{temp_table}" ({columns_part}) FROM STDIN WITH CSV HEADER'
                
                with open(csv_filename, 'r') as f:
                    cursor.copy_expert(copy_sql, f)
                
                # Now use INSERT ON CONFLICT to merge data
                merge_sql = f"""
                    WITH merge_result AS (
                        INSERT INTO "{target_table}" ({", ".join([f'"{col}"' for col in target_columns])})
                        SELECT {", ".join([f'"{col}"' for col in target_columns])} FROM "{temp_table}"
                        ON CONFLICT ("{primary_key}")
                        DO UPDATE SET
                            {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in target_columns if col.lower() != primary_key.lower()])}
                        RETURNING (xmax = 0) AS inserted
                    )
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN inserted THEN 1 ELSE 0 END) AS inserted,
                        SUM(CASE WHEN NOT inserted THEN 1 ELSE 0 END) AS updated
                    FROM merge_result
                """
                
                cursor.execute(merge_sql)
                result = cursor.fetchone()
                
                if result:
                    stats['new_rows'] = result['inserted'] if 'inserted' in result else 0
                    stats['updated_rows'] = result['updated'] if 'updated' in result else 0
                
                # Drop temp table
                cursor.execute(f'DROP TABLE "{temp_table}"')
                
        elif db_type == 'mssql':
            # Use SQL Server's MERGE statement
            with connector_service.cursor(target_db) as cursor:
                # Create temp table
                temp_table = f"#tmp_load_{int(time.time())}"
                cursor.execute(f"SELECT TOP 0 * INTO {temp_table} FROM [{target_table}]")
                
                # Insert data into temp table
                for row in rows:
                    values = []
                    for col in target_columns:
                        value = row.get(col)
                        if value is None:
                            values.append("NULL")
                        elif isinstance(value, str):
                            # Escape single quotes
                            value = value.replace("'", "''")
                            values.append(f"'{value}'")
                        else:
                            values.append(f"'{value}'")
                    
                    insert_sql = f"""
                        INSERT INTO {temp_table} ({', '.join([f'[{col}]' for col in target_columns])})
                        VALUES ({', '.join(values)})
                    """
                    cursor.execute(insert_sql)
                
                # Use MERGE to update/insert
                merge_sql = f"""
                    DECLARE @result TABLE (
                        [action] NVARCHAR(10),
                        [{primary_key}] NVARCHAR(255)
                    )
                    
                    MERGE INTO [{target_table}] AS target
                    USING {temp_table} AS source
                    ON target.[{primary_key}] = source.[{primary_key}]
                    WHEN MATCHED THEN
                        UPDATE SET {', '.join([f'target.[{col}] = source.[{col}]' for col in target_columns if col.lower() != primary_key.lower()])}
                    WHEN NOT MATCHED THEN
                        INSERT ({', '.join([f'[{col}]' for col in target_columns])})
                        VALUES ({', '.join([f'source.[{col}]' for col in target_columns])})
                    OUTPUT
                        $action,
                        INSERTED.[{primary_key}]
                    INTO @result;
                    
                    -- Get counts
                    SELECT
                        SUM(CASE WHEN [action] = 'INSERT' THEN 1 ELSE 0 END) AS inserted_count,
                        SUM(CASE WHEN [action] = 'UPDATE' THEN 1 ELSE 0 END) AS updated_count
                    FROM @result;
                """
                
                cursor.execute(merge_sql)
                result = cursor.fetchone()
                
                if result:
                    stats['new_rows'] = result['inserted_count'] or 0
                    stats['updated_rows'] = result['updated_count'] or 0
                
                # Drop temp table
                cursor.execute(f"IF OBJECT_ID('tempdb..{temp_table}') IS NOT NULL DROP TABLE {temp_table}")
                
        elif db_type == 'mysql':
            # Use MySQL's INSERT ON DUPLICATE KEY UPDATE
            with connector_service.cursor(target_db) as cursor:
                # Insert data in batches
                batch_size = 1000
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i+batch_size]
                    
                    # Prepare multi-value insert
                    values_list = []
                    for row in batch:
                        row_values = []
                        for col in target_columns:
                            value = row.get(col)
                            if value is None:
                                row_values.append("NULL")
                            elif isinstance(value, str):
                                # Escape single quotes
                                value = value.replace("'", "''")
                                row_values.append(f"'{value}'")
                            else:
                                row_values.append(f"'{value}'")
                        values_list.append(f"({', '.join(row_values)})")
                    
                    # Build update clause
                    update_clause = ', '.join([
                        f"`{col}` = VALUES(`{col}`)" 
                        for col in target_columns 
                        if col.lower() != primary_key.lower()
                    ])
                    
                    # Execute INSERT ON DUPLICATE KEY UPDATE
                    insert_sql = f"""
                        INSERT INTO `{target_table}` ({', '.join([f'`{col}`' for col in target_columns])})
                        VALUES {', '.join(values_list)}
                        ON DUPLICATE KEY UPDATE {update_clause}
                    """
                    cursor.execute(insert_sql)
                    
                    # MySQL doesn't return detailed stats, so estimate based on affected rows
                    # MySQL returns 1 for each INSERT, 2 for each UPDATE
                    affected = cursor.rowcount
                    if affected > len(batch):
                        # Some rows were updated (counted twice)
                        updated = affected - len(batch)
                        inserted = len(batch) - updated
                    else:
                        # All were new inserts
                        inserted = affected
                        updated = 0
                    
                    stats['new_rows'] += inserted
                    stats['updated_rows'] += updated
                    
        else:
            # Generic approach - process row by row
            for row in rows:
                # Check if record exists
                check_query = f"SELECT 1 FROM {target_table} WHERE {primary_key} = %s"
                result = connector_service.execute_query(target_db, check_query, [row.get(primary_key)])
                
                if result and len(result) > 0:
                    # Update existing record
                    update_sets = []
                    update_values = []
                    
                    for col in target_columns:
                        if col.lower() != primary_key.lower():
                            update_sets.append(f"{col} = %s")
                            update_values.append(row.get(col))
                    
                    update_sql = f"UPDATE {target_table} SET {', '.join(update_sets)} WHERE {primary_key} = %s"
                    update_values.append(row.get(primary_key))
                    
                    connector_service.execute_query(target_db, update_sql, update_values)
                    stats['updated_rows'] += 1
                else:
                    # Insert new record
                    insert_columns = ', '.join(target_columns)
                    placeholders = ', '.join(['%s'] * len(target_columns))
                    
                    insert_sql = f"INSERT INTO {target_table} ({insert_columns}) VALUES ({placeholders})"
                    insert_values = [row.get(col) for col in target_columns]
                    
                    connector_service.execute_query(target_db, insert_sql, insert_values)
                    stats['new_rows'] += 1
        
        return stats
    
    def _sync_with_direct_transfer(self, connector_service, source_db, target_db, config, table_config, primary_key):
        """Process data directly in memory for smaller tables"""
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'unchanged_rows': 0, 'error_rows': 0}
        
        try:
            # Get source columns and prepare mappings
            source_columns = connector_service.get_columns(source_db, config['source_table'])[0]
            
            # Map source to target columns
            column_map = {}
            target_columns = []
            
            for source_col, mapping in config['mappings'].items():
                if isinstance(mapping, dict) and mapping.get('target'):
                    original_col = source_columns.get(source_col.lower())
                    if original_col:
                        column_map[original_col] = mapping['target'].lower()
                        target_columns.append(mapping['target'].lower())
            
            # Get all source data
            source_columns_list = list(column_map.keys())
            
            # Format source query based on database type
            db_type = source_db.db_type_code
            if db_type == 'postgresql':
                # source_query = f'SELECT {", ".join([f\'"{col}"\' for col in source_columns_list])} FROM "{config["source_table"]}"'
                columns_part = ", ".join(f'"{col}"' for col in source_columns_list)
                source_query = f'SELECT {columns_part} FROM "{config["source_table"]}"'
            elif db_type == 'mssql':
                source_query = f"SELECT {', '.join([f'[{col}]' for col in source_columns_list])} FROM [{config['source_table']}]"
            elif db_type == 'mysql':
                source_query = f"SELECT {', '.join([f'`{col}`' for col in source_columns_list])} FROM `{config['source_table']}`"
            else:
                source_query = f"SELECT {', '.join(source_columns_list)} FROM {config['source_table']}"
            
            # Execute query
            source_data = connector_service.execute_query(source_db, source_query)
            
            if not source_data:
                return stats
            
            # Transform data
            transformed_data = []
            stats['total_rows'] = len(source_data)
            
            for row in source_data:
                transformed_row = {}
                for source_col, target_col in column_map.items():
                    value = row.get(source_col)
                    
                    # Look up mapping for this column
                    found_mapping = None
                    for s_col, mapping in config['mappings'].items():
                        if source_columns.get(s_col.lower()) == source_col:
                            found_mapping = mapping
                            break
                    
                    if found_mapping:
                        if found_mapping['type'] == 'direct':
                            transformed_row[target_col] = value
                        elif found_mapping['type'] == 'lookup' and value is not None:
                            # Lookup value
                            lookup_value = self._lookup_value(
                                connector_service,
                                target_db,
                                found_mapping['lookup_table'],
                                found_mapping['lookup_key'],
                                found_mapping['lookup_value'],
                                str(value)
                            )
                            transformed_row[target_col] = lookup_value
                
                transformed_data.append(transformed_row)
            
            # Process the transformed data based on target database type
            target_db_type = target_db.db_type_code
            
            # Define batch size - smaller than default to handle memory better
            batch_size = min(500, len(transformed_data))
            
            for i in range(0, len(transformed_data), batch_size):
                batch = transformed_data[i:i+batch_size]
                
                # Process this batch
                batch_stats = self._process_target_batch(
                    connector_service, target_db, target_db_type, 
                    config['target_table'], target_columns, primary_key, batch
                )
                
                # Update stats
                stats['new_rows'] += batch_stats.get('new_rows', 0)
                stats['updated_rows'] += batch_stats.get('updated_rows', 0)
                
                # Log progress
                _logger.info(f"Processed batch {i//batch_size + 1}/{(len(transformed_data) + batch_size - 1)//batch_size}: "
                           f"{batch_stats.get('new_rows', 0)} new, {batch_stats.get('updated_rows', 0)} updated")
                
                # Force garbage collection
                gc.collect()
            
            return stats
            
        except Exception as e:
            _logger.error(f"Error in direct memory transfer: {str(e)}")
            raise
    
    def _process_target_batch(self, connector_service, target_db, db_type, target_table, target_columns, primary_key, batch):
        """Process a batch of data against the target database"""
        stats = {'new_rows': 0, 'updated_rows': 0}
        
        if db_type == 'postgresql':
            # For PostgreSQL, use optimized UPSERT
            with connector_service.cursor(target_db) as cursor:
                # Prepare column list and update clause
                column_list = ', '.join([f'"{col}"' for col in target_columns])
                update_clause = ', '.join([
                    f'"{col}" = EXCLUDED."{col}"' 
                    for col in target_columns 
                    if col.lower() != primary_key.lower()
                ])
                
                # Prepare SQL and data
                upsert_sql = f"""
                    WITH upsert_result AS (
                        INSERT INTO "{target_table}" ({column_list})
                        VALUES %s
                        ON CONFLICT ("{primary_key}")
                        DO UPDATE SET {update_clause}
                        RETURNING (xmax = 0) AS inserted
                    )
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN inserted THEN 1 ELSE 0 END) AS inserted,
                        SUM(CASE WHEN NOT inserted THEN 1 ELSE 0 END) AS updated
                    FROM upsert_result
                """
                
                # Prepare values for execute_values
                from psycopg2.extras import execute_values
                
                values = []
                for row in batch:
                    row_values = []
                    for col in target_columns:
                        row_values.append(row.get(col))
                    values.append(tuple(row_values))
                
                # Execute the upsert with execute_values
                execute_values(
                    cursor,
                    upsert_sql,
                    values,
                    template=f"({', '.join(['%s'] * len(target_columns))})",
                    fetch=True
                )
                
                # Get results
                result = cursor.fetchone()
                
                if result:
                    stats['new_rows'] = result['inserted'] if 'inserted' in result else 0
                    stats['updated_rows'] = result['updated'] if 'updated' in result else 0
                
        elif db_type == 'mssql':
            # For SQL Server, use MERGE statement
            with connector_service.cursor(target_db) as cursor:
                # Create a temp table for this batch
                temp_table = f"#tmp_batch_{int(time.time())}"
                cursor.execute(f"SELECT TOP 0 * INTO {temp_table} FROM [{target_table}]")
                
                # Insert data into temp table
                for row in batch:
                    values = []
                    for col in target_columns:
                        value = row.get(col)
                        if value is None:
                            values.append("NULL")
                        elif isinstance(value, str):
                            # Escape single quotes
                            value = value.replace("'", "''")
                            values.append(f"'{value}'")
                        else:
                            values.append(f"'{value}'")
                    
                    insert_sql = f"""
                        INSERT INTO {temp_table} ({', '.join([f'[{col}]' for col in target_columns])})
                        VALUES ({', '.join(values)})
                    """
                    cursor.execute(insert_sql)
                
                # Execute MERGE
                merge_sql = f"""
                    DECLARE @result TABLE (
                        [action] NVARCHAR(10),
                        [{primary_key}] NVARCHAR(255)
                    )
                    
                    MERGE INTO [{target_table}] AS target
                    USING {temp_table} AS source
                    ON target.[{primary_key}] = source.[{primary_key}]
                    WHEN MATCHED THEN
                        UPDATE SET {', '.join([f'target.[{col}] = source.[{col}]' for col in target_columns if col.lower() != primary_key.lower()])}
                    WHEN NOT MATCHED THEN
                        INSERT ({', '.join([f'[{col}]' for col in target_columns])})
                        VALUES ({', '.join([f'source.[{col}]' for col in target_columns])})
                    OUTPUT
                        $action,
                        INSERTED.[{primary_key}]
                    INTO @result;
                    
                    -- Get counts
                    SELECT
                        SUM(CASE WHEN [action] = 'INSERT' THEN 1 ELSE 0 END) AS inserted_count,
                        SUM(CASE WHEN [action] = 'UPDATE' THEN 1 ELSE 0 END) AS updated_count
                    FROM @result;
                """
                
                cursor.execute(merge_sql)
                result = cursor.fetchone()
                
                if result:
                    stats['new_rows'] = result['inserted_count'] or 0
                    stats['updated_rows'] = result['updated_count'] or 0
                
                # Drop temp table
                cursor.execute(f"IF OBJECT_ID('tempdb..{temp_table}') IS NOT NULL DROP TABLE {temp_table}")
                
        elif db_type == 'mysql':
            # For MySQL, use INSERT ON DUPLICATE KEY UPDATE
            with connector_service.cursor(target_db) as cursor:
                # Prepare values
                values_list = []
                for row in batch:
                    row_values = []
                    for col in target_columns:
                        value = row.get(col)
                        if value is None:
                            row_values.append("NULL")
                        elif isinstance(value, str):
                            # Escape single quotes
                            value = value.replace("'", "''")
                            row_values.append(f"'{value}'")
                        else:
                            row_values.append(f"'{value}'")
                    values_list.append(f"({', '.join(row_values)})")
                
                # Build update clause
                update_clause = ', '.join([
                    f"`{col}` = VALUES(`{col}`)" 
                    for col in target_columns 
                    if col.lower() != primary_key.lower()
                ])
                
                # Execute INSERT ON DUPLICATE KEY UPDATE
                insert_sql = f"""
                    INSERT INTO `{target_table}` ({', '.join([f'`{col}`' for col in target_columns])})
                    VALUES {', '.join(values_list)}
                    ON DUPLICATE KEY UPDATE {update_clause}
                """
                cursor.execute(insert_sql)
                
                # MySQL doesn't return detailed stats, so estimate based on affected rows
                # MySQL returns 1 for each INSERT, 2 for each UPDATE
                affected = cursor.rowcount
                if affected > len(batch):
                    # Some rows were updated (counted twice)
                    updated = affected - len(batch)
                    inserted = len(batch) - updated
                else:
                    # All were new inserts
                    inserted = affected
                    updated = 0
                
                stats['new_rows'] = inserted
                stats['updated_rows'] = updated
                
        else:
            # Generic approach - process row by row
            for row in batch:
                # Check if record exists
                check_query = f"SELECT 1 FROM {target_table} WHERE {primary_key} = %s"
                result = connector_service.execute_query(target_db, check_query, [row.get(primary_key)])
                
                if result and len(result) > 0:
                    # Update existing record
                    update_sets = []
                    update_values = []
                    
                    for col in target_columns:
                        if col.lower() != primary_key.lower():
                            update_sets.append(f"{col} = %s")
                            update_values.append(row.get(col))
                    
                    update_sql = f"UPDATE {target_table} SET {', '.join(update_sets)} WHERE {primary_key} = %s"
                    update_values.append(row.get(primary_key))
                    
                    connector_service.execute_query(target_db, update_sql, update_values)
                    stats['updated_rows'] += 1
                else:
                    # Insert new record
                    insert_columns = ', '.join(target_columns)
                    placeholders = ', '.join(['%s'] * len(target_columns))
                    
                    insert_sql = f"INSERT INTO {target_table} ({insert_columns}) VALUES ({placeholders})"
                    insert_values = [row.get(col) for col in target_columns]
                    
                    connector_service.execute_query(target_db, insert_sql, insert_values)
                    stats['new_rows'] += 1
        
        return stats
    
    def _lookup_value(self, connector_service, target_db, table, key_col, value_col, key_value):
        """Perform a lookup with local caching"""
        if not key_value:
            return None
            
        # Use class-level cache if available
        cache_key = f"{table}:{key_col}:{key_value}"
        
        # Check in-memory cache (this instance only)
        if not hasattr(self, '_lookup_cache'):
            self._lookup_cache = {}
            
        if cache_key in self._lookup_cache:
            return self._lookup_cache[cache_key]
        
        try:
            # Format query based on database type
            db_type = target_db.db_type_code
            if db_type == 'postgresql':
                query = f'SELECT "{value_col}" FROM "{table}" WHERE "{key_col}" = %s LIMIT 1'
            elif db_type == 'mssql':
                query = f"SELECT [{value_col}] FROM [{table}] WHERE [{key_col}] = ?"
            elif db_type == 'mysql':
                query = f"SELECT `{value_col}` FROM `{table}` WHERE `{key_col}` = %s LIMIT 1"
            else:
                query = f"SELECT {value_col} FROM {table} WHERE {key_col} = %s LIMIT 1"
            
            result = connector_service.execute_query(target_db, query, [key_value])
            
            if result and len(result) > 0:
                # Get value
                value = None
                for k in result[0]:
                    if k.lower() == value_col.lower():
                        value = result[0][k]
                        break
                
                # Cache the result
                self._lookup_cache[cache_key] = value
                return value
                
            # Cache null result too
            self._lookup_cache[cache_key] = None
            return None
        except Exception as e:
            _logger.warning(f"Lookup error: {str(e)}")
            return None
