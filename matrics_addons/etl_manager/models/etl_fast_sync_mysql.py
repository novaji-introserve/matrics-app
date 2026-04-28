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

_logger = logging.getLogger(__name__)

class ETLFastSyncMySQL(models.AbstractModel):
    _name = 'etl.fast.sync.mysql'
    _description = 'Fast MySQL to MySQL ETL'
    
    @api.model
    def sync_data(self, table_config):
        """
        Highly optimized MySQL to MySQL data sync using
        LOAD DATA INFILE and INSERT ON DUPLICATE KEY UPDATE
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
        table_size = connector_service.get_table_count(source_db, source_table)
        _logger.info(f"MySQL fast sync for table {source_table} with {table_size} rows")
        
        # For very large tables (>1M rows), use direct LOAD DATA INFILE
        if table_size > 1000000:
            stats = self._sync_with_load_data_infile(
                connector_service, source_db, target_db, 
                config, table_config, primary_key
            )
        
        # For medium to large tables (100K-1M rows), use batched approach
        elif table_size > 100000:
            stats = self._sync_with_batched_load(
                connector_service, source_db, target_db, 
                config, table_config, primary_key
            )
            
        # For smaller tables, use bulk insert with ON DUPLICATE KEY UPDATE
        else:
            stats = self._sync_with_bulk_insert(
                connector_service, source_db, target_db, 
                config, table_config, primary_key
            )
        
        stats['execution_time'] = time.time() - start_time
        return stats
    
    def _sync_with_load_data_infile(self, connector_service, source_db, target_db, config, table_config, primary_key):
        """Use MySQL LOAD DATA INFILE for super fast transfer of large tables"""
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'unchanged_rows': 0, 'error_rows': 0}
        target_table = config['target_table'] 
        
        try:
            # Get source columns and mappings
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
            
            # Create a temporary outfile
            outfile_path = f"/tmp/etl_mysql_export_{table_config.id}_{int(time.time())}.csv"
            
            # Export data to CSV - MySQL doesn't use standard CSV, but tab-delimited by default
            with connector_service.cursor(source_db) as cursor:
                # Add backticks around column names
                select_columns = ', '.join([f'`{col}`' for col in column_map.keys()])
                
                # Create SQL for export
                export_sql = f"""
                    SELECT {select_columns}
                    INTO OUTFILE '{outfile_path}'
                    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
                    LINES TERMINATED BY '\\n'
                    FROM `{config['source_table']}`
                """
                
                # Execute export (requires FILE privileges on MySQL server)
                try:
                    cursor.execute(export_sql)
                except Exception as e:
                    # If direct OUTFILE fails (common due to permissions), fall back to streaming export
                    _logger.warning(f"Direct OUTFILE failed: {str(e)}. Falling back to streaming export.")
                    return self._sync_with_batched_load(
                        connector_service, source_db, target_db, 
                        config, table_config, primary_key
                    )
            
            # Load data into target
            # First, create a temporary table with same structure
            with connector_service.cursor(target_db) as cursor:
                # Create temporary table
                temp_table = f"tmp_load_{int(time.time())}"
                cursor.execute(f"CREATE TEMPORARY TABLE `{temp_table}` LIKE `{target_table}`")
                
                # Load data into temporary table
                load_sql = f"""
                    LOAD DATA INFILE '{outfile_path}'
                    INTO TABLE `{temp_table}`
                    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
                    LINES TERMINATED BY '\\n'
                    ({', '.join([f'`{col}`' for col in target_columns])})
                """
                
                try:
                    cursor.execute(load_sql)
                except Exception as e:
                    # If load fails, clean up and fall back
                    _logger.warning(f"LOAD DATA INFILE failed: {str(e)}. Falling back to batched load.")
                    cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS `{temp_table}`")
                    
                    # Try to clean up CSV file
                    try:
                        os.remove(outfile_path)
                    except:
                        pass
                        
                    return self._sync_with_batched_load(
                        connector_service, source_db, target_db, 
                        config, table_config, primary_key
                    )
                
                # Count rows loaded
                cursor.execute(f"SELECT COUNT(*) AS count FROM `{temp_table}`")
                result = cursor.fetchone()
                loaded_count = result['count']
                stats['total_rows'] = loaded_count
                
                # Now create INSERT ... ON DUPLICATE KEY UPDATE statement
                update_clause = ', '.join([
                    f"`{col}` = VALUES(`{col}`)" 
                    for col in target_columns 
                    if col.lower() != primary_key.lower()
                ])
                
                merge_sql = f"""
                    INSERT INTO `{target_table}` ({', '.join([f'`{col}`' for col in target_columns])})
                    SELECT {', '.join([f'`{col}`' for col in target_columns])} FROM `{temp_table}`
                    ON DUPLICATE KEY UPDATE {update_clause}
                """
                
                # Execute merge
                cursor.execute(merge_sql)
                
                # MySQL returns: 1 for each INSERT, 2 for each UPDATE, 0 for no change
                affected_rows = cursor.rowcount
                
                # Estimate new vs updated rows
                if affected_rows > loaded_count:
                    # Some rows were updated (counted twice)
                    stats['updated_rows'] = affected_rows - loaded_count
                    stats['new_rows'] = loaded_count - stats['updated_rows']
                else:
                    # All were new inserts
                    stats['new_rows'] = affected_rows
                    stats['updated_rows'] = 0
                
                # Clean up temporary table
                cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS `{temp_table}`")
            
            # Clean up CSV file
            try:
                os.remove(outfile_path)
            except:
                pass
            
            return stats
            
        except Exception as e:
            _logger.error(f"Error in MySQL LOAD DATA INFILE sync: {str(e)}")
            
            # Clean up resources
            try:
                with connector_service.cursor(target_db) as cursor:
                    if 'temp_table' in locals():
                        cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS `{temp_table}`")
            except:
                pass
                
            try:
                if 'outfile_path' in locals() and os.path.exists(outfile_path):
                    os.remove(outfile_path)
            except:
                pass
                
            raise
    
    def _sync_with_batched_load(self, connector_service, source_db, target_db, config, table_config, primary_key):
        """Use batched load for better memory management"""
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'unchanged_rows': 0, 'error_rows': 0}
        target_table = config['target_table'] 
        
        try:
            # Get source columns and mappings
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
            
            # Select optimal batch size based on column count
            column_count = len(column_map)
            if column_count > 50:
                batch_size = 500
            elif column_count > 20:
                batch_size = 1000
            else:
                batch_size = 2000
            
            # Process in batches
            offset = 0
            has_more = True
            total_new = 0
            total_updated = 0
            
            while has_more:
                # Get a batch of data
                select_columns = ', '.join([f'`{col}`' for col in column_map.keys()])
                query = f"""
                    SELECT {select_columns}
                    FROM `{config['source_table']}`
                    ORDER BY `{primary_key}`
                    LIMIT {batch_size} OFFSET {offset}
                """
                
                batch_data = connector_service.execute_query(source_db, query)
                
                if not batch_data or len(batch_data) == 0:
                    has_more = False
                    break
                
                # Transform data
                transformed_batch = []
                for row in batch_data:
                    transformed_row = {}
                    for source_col, target_col in column_map.items():
                        value = row.get(source_col)
                        
                        # Apply mappings
                        for s_col, mapping in config['mappings'].items():
                            if source_columns.get(s_col.lower()) == source_col:
                                if mapping['type'] == 'direct':
                                    transformed_row[target_col] = value
                                elif mapping['type'] == 'lookup' and value is not None:
                                    lookup_value = self._lookup_value(
                                        connector_service,
                                        target_db,
                                        mapping['lookup_table'],
                                        mapping['lookup_key'],
                                        mapping['lookup_value'],
                                        str(value)
                                    )
                                    transformed_row[target_col] = lookup_value
                    
                    transformed_batch.append(transformed_row)
                
                # Insert with ON DUPLICATE KEY UPDATE
                with connector_service.cursor(target_db) as cursor:
                    # Build multi-value insert with ON DUPLICATE KEY UPDATE
                    values_parts = []
                    insert_values = []
                    
                    for row in transformed_batch:
                        # For each row, add placeholders and values
                        row_values = []
                        placeholders = []
                        
                        for col in target_columns:
                            placeholders.append('%s')
                            row_values.append(row.get(col))
                        
                        values_parts.append(f"({', '.join(placeholders)})")
                        insert_values.extend(row_values)
                    
                    # Build ON DUPLICATE KEY UPDATE part
                    update_parts = []
                    for col in target_columns:
                        if col.lower() != primary_key.lower():
                            update_parts.append(f"`{col}` = VALUES(`{col}`)")
                    
                    # Complete SQL
                    insert_sql = f"""
                        INSERT INTO `{target_table}` ({', '.join([f'`{col}`' for col in target_columns])})
                        VALUES {', '.join(values_parts)}
                        ON DUPLICATE KEY UPDATE {', '.join(update_parts)}
                    """
                    
                    # Execute batch insert
                    cursor.execute(insert_sql, insert_values)
                    
                    # Count affected rows
                    affected_rows = cursor.rowcount
                    
                    # MySQL returns: 1 for each INSERT, 2 for each UPDATE, 0 for no change
                    if affected_rows > len(transformed_batch):
                        # Some rows were updated (counted twice)
                        batch_updated = affected_rows - len(transformed_batch)
                        batch_new = len(transformed_batch) - batch_updated
                    else:
                        # All were new inserts
                        batch_new = affected_rows
                        batch_updated = 0
                    
                    # Update stats
                    stats['total_rows'] += len(transformed_batch)
                    total_new += batch_new
                    total_updated += batch_updated
                
                # Update offset for next batch
                offset += len(batch_data)
                
                # Check if we've reached the end
                if len(batch_data) < batch_size:
                    has_more = False
                
                # Log progress
                _logger.info(f"Processed batch at offset {offset}: {len(transformed_batch)} rows "
                             f"({batch_new} new, {batch_updated} updated)")
                
                # Force garbage collection after each batch
                gc.collect()
            
            # Update final stats
            stats['new_rows'] = total_new
            stats['updated_rows'] = total_updated
            
            return stats
            
        except Exception as e:
            _logger.error(f"Error in MySQL batched load: {str(e)}")
            raise
    
    def _sync_with_bulk_insert(self, connector_service, source_db, target_db, config, table_config, primary_key):
        """Use bulk insert with ON DUPLICATE KEY UPDATE for smaller tables"""
        stats = {'total_rows': 0, 'new_rows': 0, 'updated_rows': 0, 'unchanged_rows': 0, 'error_rows': 0}
        target_table = config['target_table'] 
        
        try:
            # Get all data in one go for small tables
            # Get source columns and mappings
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
            select_columns = ', '.join([f'`{col}`' for col in column_map.keys()])
            query = f"""
                SELECT {select_columns}
                FROM `{config['source_table']}`
            """
            
            source_data = connector_service.execute_query(source_db, query)
            
            if not source_data:
                return stats
            
            # Transform all data
            transformed_data = []
            for row in source_data:
                transformed_row = {}
                for source_col, target_col in column_map.items():
                    value = row.get(source_col)
                    
                    # Apply mappings
                    for s_col, mapping in config['mappings'].items():
                        if source_columns.get(s_col.lower()) == source_col:
                            if mapping['type'] == 'direct':
                                transformed_row[target_col] = value
                            elif mapping['type'] == 'lookup' and value is not None:
                                lookup_value = self._lookup_value(
                                    connector_service,
                                    target_db,
                                    mapping['lookup_table'],
                                    mapping['lookup_key'],
                                    mapping['lookup_value'],
                                    str(value)
                                )
                                transformed_row[target_col] = lookup_value
                
                transformed_data.append(transformed_row)
            
            # Update stats total
            stats['total_rows'] = len(transformed_data)
            
            # Bulk insert all at once
            with connector_service.cursor(target_db) as cursor:
                # For bulk insert, use placeholders and parameter binding
                placeholders = []
                insert_values = []
                
                for row in transformed_data:
                    # Add a tuple of placeholders for each row
                    row_placeholders = []
                    row_values = []
                    
                    for col in target_columns:
                        row_placeholders.append('%s')
                        row_values.append(row.get(col))
                    
                    placeholders.append(f"({', '.join(row_placeholders)})")
                    insert_values.extend(row_values)
                
                # Build ON DUPLICATE KEY UPDATE part
                update_parts = []
                for col in target_columns:
                    if col.lower() != primary_key.lower():
                        update_parts.append(f"`{col}` = VALUES(`{col}`)")
                
                # Complete SQL
                insert_sql = f"""
                    INSERT INTO `{target_table}` ({', '.join([f'`{col}`' for col in target_columns])})
                    VALUES {', '.join(placeholders)}
                    ON DUPLICATE KEY UPDATE {', '.join(update_parts)}
                """
                
                # Execute bulk insert
                cursor.execute(insert_sql, insert_values)
                
                # Count affected rows
                affected_rows = cursor.rowcount
                
                # MySQL returns: 1 for each INSERT, 2 for each UPDATE, 0 for no change
                if affected_rows > len(transformed_data):
                    # Some rows were updated (counted twice)
                    stats['updated_rows'] = affected_rows - len(transformed_data)
                    stats['new_rows'] = len(transformed_data) - stats['updated_rows']
                else:
                    # All were new inserts or no changes
                    stats['new_rows'] = affected_rows
                    stats['updated_rows'] = 0
            
            return stats
            
        except Exception as e:
            _logger.error(f"Error in MySQL bulk insert: {str(e)}")
            raise
    
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
            query = f"SELECT `{value_col}` FROM `{table}` WHERE `{key_col}` = %s LIMIT 1"
            result = connector_service.execute_query(target_db, query, [key_value])
            
            if result and len(result) > 0:
                value = result[0][value_col]
                # Cache the result
                self._lookup_cache[cache_key] = value
                return value
                
            # Cache null result too
            self._lookup_cache[cache_key] = None
            return None
        except Exception as e:
            _logger.warning(f"Lookup error: {str(e)}")
            return None
