# -*- coding: utf-8 -*-
from odoo import models, fields, api, _
from odoo.exceptions import UserError
import logging
import time
import psutil
import os
import gc
import tempfile
import json
import threading
from contextlib import contextmanager
from functools import wraps
import weakref

_logger = logging.getLogger(__name__)

def memory_tracked(func):
    """Decorator to track memory usage of functions"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        memory_before = process.memory_info().rss / 1024 / 1024  # MB
        
        # Force garbage collection before operation
        gc.collect()
        
        start_time = time.time()
        result = None
        
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            # Force garbage collection after operation
            gc.collect()
            
            memory_after = process.memory_info().rss / 1024 / 1024  # MB
            memory_diff = memory_after - memory_before
            execution_time = time.time() - start_time
            
            # Get function name and calling class if possible
            func_name = func.__name__
            class_name = ""
            
            if args and hasattr(args[0], "__class__") and hasattr(args[0].__class__, "__name__"):
                class_name = args[0].__class__.__name__
            
            _logger.info(f"Memory usage for {class_name}.{func_name}: "
                        f"Before={memory_before:.1f}MB, After={memory_after:.1f}MB, "
                        f"Diff={memory_diff:+.1f}MB, Time={execution_time:.2f}s")
            
            # Log to database if significant memory change or slow execution
            if abs(memory_diff) > 50 or execution_time > 5.0:
                try:
                    # Extract self if it exists in args
                    self = args[0] if args and hasattr(args[0], 'env') else None
                    
                    if self and hasattr(self, 'env'):
                        # Log performance data
                        self.env['etl.performance.log'].sudo().create({
                            'name': f"{class_name}.{func_name}",
                            'execution_time': execution_time,
                            'memory_before': memory_before,
                            'memory_after': memory_after,
                            'memory_diff': memory_diff,
                            'details': json.dumps({
                                'args': str(args[1:])[:100] if len(args) > 1 else '',
                                'kwargs': str(kwargs)[:100] if kwargs else ''
                            })
                        })
                except Exception as e:
                    _logger.warning(f"Failed to log performance data: {str(e)}")
    
    return wrapper

class MemoryProcessor(models.AbstractModel):
    _name = 'etl.memory.processor'
    _description = 'Memory-Optimized ETL Processing'
    
    @api.model
    @memory_tracked
    def process_large_dataset(self, table_config, data_source_func, transform_func, load_func):
        """
        Process a large dataset with memory optimization techniques
        
        Args:
            table_config: The ETL table configuration
            data_source_func: Function that returns a generator of data chunks
            transform_func: Function to transform a data chunk
            load_func: Function to load a transformed chunk
        """
        start_time = time.time()
        total_records = 0
        chunk_count = 0
        
        # Memory monitoring
        process = psutil.Process(os.getpid())
        peak_memory = 0
        
        # Create a sync log
        sync_log = self.env['etl.sync.log'].create({
            'table_id': table_config.id,
            'start_time': fields.Datetime.now(),
            'status': 'running'
        })
        
        try:
            # Get data generator
            data_chunks = data_source_func(table_config)
            
            # Process chunks with memory management
            for chunk_data in data_chunks:
                chunk_count += 1
                chunk_size = len(chunk_data) if hasattr(chunk_data, '__len__') else 'unknown'
                
                _logger.info(f"Processing chunk {chunk_count} with ~{chunk_size} records")
                
                # Transform chunk
                transformed_data = self._process_chunk_with_memory_check(
                    chunk_data, transform_func, table_config
                )
                
                # If transformation was successful
                if transformed_data:
                    # Load the transformed data
                    load_result = self._process_chunk_with_memory_check(
                        transformed_data, load_func, table_config
                    )
                    
                    # Update counters
                    if isinstance(load_result, dict):
                        total_records += load_result.get('total', len(transformed_data))
                    else:
                        total_records += len(transformed_data)
                
                # Update progress
                if hasattr(table_config, 'write'):
                    table_config.write({
                        'progress_percentage': min(99, chunk_count),
                        'last_sync_message': f'Processed {total_records} records in {chunk_count} chunks'
                    })
                
                # Check memory usage
                current_memory = process.memory_info().rss / 1024 / 1024  # MB
                peak_memory = max(peak_memory, current_memory)
                
                # Log memory usage every 10 chunks
                if chunk_count % 10 == 0:
                    _logger.info(f"Memory usage after {chunk_count} chunks: {current_memory:.1f}MB (peak: {peak_memory:.1f}MB)")
                
                # Periodic commit to release memory in the transaction
                if chunk_count % 5 == 0:
                    self.env.cr.commit()
                
                # Force garbage collection after each chunk
                gc.collect()
                
                # If memory usage is too high, take corrective action
                if current_memory > 1500:  # Over 1.5GB
                    _logger.warning(f"High memory usage detected: {current_memory:.1f}MB. Taking corrective action.")
                    self._reduce_memory_pressure()
            
            # Update sync log
            sync_log.write({
                'end_time': fields.Datetime.now(),
                'status': 'success',
                'total_records': total_records
            })
            
            # Update table status
            if hasattr(table_config, 'write'):
                table_config.write({
                    'job_status': 'done',
                    'progress_percentage': 100,
                    'last_sync_status': 'success',
                    'last_sync_message': f'Successfully processed {total_records} records in {chunk_count} chunks',
                    'total_records_synced': total_records
                })
            
            execution_time = time.time() - start_time
            _logger.info(f"Memory-optimized processing completed: {total_records} records in {execution_time:.2f}s. "
                        f"Peak memory: {peak_memory:.1f}MB")
            
            return {
                'status': 'success',
                'total_records': total_records,
                'chunks': chunk_count,
                'execution_time': execution_time,
                'peak_memory': peak_memory
            }
            
        except Exception as e:
            error_message = str(e)
            _logger.error(f"Error in memory-optimized processing: {error_message}")
            
            # Update sync log
            sync_log.write({
                'end_time': fields.Datetime.now(),
                'status': 'failed',
                'error_message': error_message
            })
            
            # Update table status
            if hasattr(table_config, 'write'):
                table_config.write({
                    'job_status': 'failed',
                    'last_sync_status': 'failed',
                    'last_sync_message': f'Error: {error_message}'
                })
            
            raise
    
    def _process_chunk_with_memory_check(self, chunk_data, process_func, table_config):
        """Process a chunk with memory monitoring"""
        try:
            # Check memory before processing
            process = psutil.Process(os.getpid())
            memory_before = process.memory_info().rss / 1024 / 1024  # MB
            
            # Process the chunk
            result = process_func(chunk_data, table_config)
            
            # Check memory after processing
            memory_after = process.memory_info().rss / 1024 / 1024  # MB
            memory_diff = memory_after - memory_before
            
            # Log if there's a significant memory change
            if abs(memory_diff) > 50:
                _logger.info(f"Significant memory change in chunk processing: {memory_diff:+.1f}MB")
                
                # If memory increased too much, force garbage collection
                if memory_diff > 100:
                    _logger.info("Forcing garbage collection due to high memory increase")
                    gc.collect()
            
            return result
            
        except MemoryError:
            _logger.error("Memory error occurred during chunk processing")
            self._reduce_memory_pressure()
            # Re-try with reduced chunk
            if hasattr(chunk_data, '__len__') and len(chunk_data) > 1:
                # Try with half the data
                half_size = len(chunk_data) // 2
                _logger.info(f"Retrying with reduced chunk size: {half_size}")
                
                # Process first half
                first_half = chunk_data[:half_size]
                result1 = self._process_chunk_with_memory_check(first_half, process_func, table_config)
                
                # Process second half
                second_half = chunk_data[half_size:]
                result2 = self._process_chunk_with_memory_check(second_half, process_func, table_config)
                
                # Combine results if possible
                if isinstance(result1, list) and isinstance(result2, list):
                    return result1 + result2
                elif isinstance(result1, dict) and isinstance(result2, dict):
                    # Merge dictionaries - simple case
                    combined = result1.copy()
                    for key, value in result2.items():
                        if key in combined and isinstance(value, (int, float)):
                            combined[key] += value
                        else:
                            combined[key] = value
                    return combined
                else:
                    # Just return the first result
                    return result1
            raise
        except Exception as e:
            _logger.error(f"Error processing chunk: {str(e)}")
            raise
    
    def _reduce_memory_pressure(self):
        """Take actions to reduce memory pressure"""
        # Force garbage collection
        gc.collect()
        
        # Clear Odoo caches
        self.env.clear()
        
        # Log current memory usage
        process = psutil.Process(os.getpid())
        current_memory = process.memory_info().rss / 1024 / 1024  # MB
        _logger.info(f"Memory usage after pressure reduction: {current_memory:.1f}MB")
    
    @api.model
    def get_chunked_data_generator(self, table_config, batch_size=1000):
        """Create a generator that yields data in chunks to control memory usage"""
        # Get connectors
        connector_service = self.env['etl.database.connector.service']
        source_db = table_config.source_db_connection_id
        
        # Get config and columns
        config = table_config.get_config_json()
        source_columns = connector_service.get_columns(source_db, config['source_table'])
        
        # Get primary key
        primary_key_original = source_columns.get(config['primary_key'].lower())
        if not primary_key_original:
            raise ValueError(f"Primary key {config['primary_key']} not found in table")
        
        # Get total count for logging purposes
        try:
            total_count = connector_service.get_table_count(source_db, config['source_table'])
            _logger.info(f"Starting chunked processing of {total_count} records from {config['source_table']}")
        except Exception as e:
            _logger.warning(f"Could not get table count: {str(e)}")
            total_count = None
        
        # Initialize variables for pagination
        offset = 0
        has_more_data = True
        
        while has_more_data:
            # Use file-based approach for very large chunks to minimize memory usage
            temp_file = None
            try:
                # Build query for this chunk
                query = f"""
                    SELECT * FROM {config['source_table']} 
                    ORDER BY {primary_key_original} 
                    LIMIT {batch_size} OFFSET {offset}
                """
                
                # Execute query
                chunk_data = connector_service.execute_query(source_db, query)
                
                # Check if we got any data
                if not chunk_data or len(chunk_data) == 0:
                    has_more_data = False
                    break
                
                # If chunk is very large, use file-based approach
                if len(chunk_data) > 10000:
                    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w+')
                    json.dump(chunk_data, temp_file)
                    temp_file.close()
                    
                    # Return file path instead of data
                    yield {'file_path': temp_file.name, 'count': len(chunk_data)}
                else:
                    # Return the data directly
                    yield chunk_data
                
                # Update offset for next chunk
                offset += len(chunk_data)
                
                # Check if we've reached the end
                if len(chunk_data) < batch_size:
                    has_more_data = False
                
                # Log progress
                if total_count:
                    progress = min(100, (offset / total_count) * 100)
                    _logger.info(f"Chunked data generator progress: {progress:.1f}% ({offset}/{total_count})")
                else:
                    _logger.info(f"Chunked data generator: loaded {offset} records so far")
                
                # Force garbage collection after each chunk
                chunk_data = None
                gc.collect()
                
            except Exception as e:
                _logger.error(f"Error in chunked data generator: {str(e)}")
                # Clean up temp file if it exists
                if temp_file and hasattr(temp_file, 'name'):
                    try:
                        os.unlink(temp_file.name)
                    except:
                        pass
                raise
    
    @api.model
    def transform_chunk(self, chunk_data, table_config):
        """Transform a chunk of data with memory optimization"""
        # Handle file-based chunks
        if isinstance(chunk_data, dict) and 'file_path' in chunk_data:
            with open(chunk_data['file_path'], 'r') as f:
                chunk_data = json.load(f)
            
            # Delete temp file after reading
            try:
                os.unlink(chunk_data['file_path'])
            except:
                pass
        
        # Get config
        config = table_config.get_config_json()
        
        # Initialize result storage with appropriate size
        transformed_data = []
        transformed_data_capacity = len(chunk_data)
        transformed_data = [None] * transformed_data_capacity
        actual_count = 0
        
        # Get mapping configuration
        mappings = config['mappings']
        
        # Get connector service for lookups
        connector_service = self.env['etl.database.connector.service']
        target_db = table_config.target_db_connection_id
        
        # Create a lookup cache for this transform operation
        lookup_cache = {}
        
        # Process each row with minimal memory overhead
        for i, row in enumerate(chunk_data):
            transformed_row = {}
            
            # Apply transformations based on mappings
            for source_col, mapping in mappings.items():
                source_value = row.get(source_col)
                
                if source_value is not None:
                    if mapping['type'] == 'direct':
                        transformed_row[mapping['target'].lower()] = source_value
                    elif mapping['type'] == 'lookup':
                        # Full lookup implementation
                        lookup_value = self._perform_lookup(
                            connector_service,
                            target_db,
                            mapping['lookup_table'],
                            mapping['lookup_key'],
                            mapping['lookup_value'],
                            str(source_value),
                            lookup_cache
                        )
                        transformed_row[mapping['target'].lower()] = lookup_value
            
            # Store in pre-allocated array
            if actual_count < transformed_data_capacity:
                transformed_data[actual_count] = transformed_row
                actual_count += 1
            else:
                # This shouldn't happen, but if it does, handle it
                transformed_data.append(transformed_row)
                actual_count += 1
        
        # Trim array to actual size
        if actual_count < transformed_data_capacity:
            transformed_data = transformed_data[:actual_count]
        
        return transformed_data

    def _perform_lookup(self, connector_service, target_db, table, key_col, value_col, key_value, lookup_cache=None):
        """Perform a lookup with local caching for efficiency"""
        if not key_value or key_value.strip() == '':
            return None
            
        # Use cache if provided
        if lookup_cache is not None:
            # Create a cache key
            cache_key = f"{table}:{key_col}:{key_value}"
            
            # Check if this lookup is already in the cache
            if cache_key in lookup_cache:
                return lookup_cache[cache_key]
        
        try:
            # Format query based on database type
            db_type = target_db.db_type_code
            
            if db_type == 'postgresql':
                query = f'SELECT "{value_col}" FROM "{table}" WHERE "{key_col}" = %s LIMIT 1'
            elif db_type == 'mssql':
                query = f"SELECT [{value_col}] FROM [{table}] WHERE [{key_col}] = ?"
            elif db_type == 'mysql':
                query = f"SELECT `{value_col}` FROM `{table}` WHERE `{key_col}` = %s LIMIT 1"
            elif db_type == 'oracle':
                query = f'SELECT "{value_col}" FROM "{table}" WHERE "{key_col}" = :1'
            else:
                query = f"SELECT {value_col} FROM {table} WHERE {key_col} = ?"
            
            # Execute query
            result = connector_service.execute_query(target_db, query, [key_value])
            
            if result and len(result) > 0:
                # Extract value from result
                if isinstance(result[0], dict):
                    # Dictionary result (most databases)
                    value = list(result[0].values())[0]
                else:
                    # Tuple result (some databases)
                    value = result[0][0]
                
                # Store in cache if available
                if lookup_cache is not None:
                    lookup_cache[cache_key] = value
                    
                return value
            
            # No matching record found
            if lookup_cache is not None:
                lookup_cache[cache_key] = None
                
            return None
            
        except Exception as e:
            _logger.warning(f"Error in lookup {table}.{key_col}={key_value}: {str(e)}")
            return None
    
    @api.model
    def load_chunk(self, transformed_data, table_config):
        """Load a transformed chunk with memory optimization"""
        if not transformed_data:
            return {'total': 0, 'new': 0, 'updated': 0}
        
        # Get connectors
        connector_service = self.env['etl.database.connector.service']
        target_db = table_config.target_db_connection_id
        
        # Get config
        config = table_config.get_config_json()
        
        # Find primary key
        primary_key = None
        for source_col, mapping in config['mappings'].items():
            if source_col.lower() == config['primary_key'].lower():
                primary_key = mapping['target'].lower()
                break
        
        if not primary_key:
            raise ValueError(f"Primary key mapping not found for {config['primary_key']}")
        
        # Get columns from first row
        if transformed_data:
            columns = list(transformed_data[0].keys())
        else:
            return {'total': 0, 'new': 0, 'updated': 0}
        
        # Use optimized batch update
        try:
            # Call batch update with memory tracking
            result = connector_service.batch_update(
                target_db, 
                config['target_table'], 
                primary_key, 
                columns, 
                transformed_data
            )
            
            # Process result if provided
            if isinstance(result, dict):
                return result
            else:
                # Default stats
                return {
                    'total': len(transformed_data),
                    'new': len(transformed_data) // 2,  # Estimate
                    'updated': len(transformed_data) // 2  # Estimate
                }
                
        except Exception as e:
            _logger.error(f"Error in memory-optimized load: {str(e)}")
            raise
    
    @api.model
    def process_table(self, table_config):
        """Memory-optimized main process for a table"""
        return self.process_large_dataset(
            table_config,
            self.get_chunked_data_generator,
            self.transform_chunk,
            self.load_chunk
        )
        
class MemoryEfficientJSONSerializer:
    """Utility for memory-efficient JSON serialization/deserialization"""
    
    @staticmethod
    def serialize_to_file(data, file_path=None):
        """Serialize data to a file with minimal memory usage"""
        if file_path is None:
            # Create a temp file
            with tempfile.NamedTemporaryFile(delete=False, mode='w+') as f:
                file_path = f.name
        
        with open(file_path, 'w') as f:
            # Handle different data types
            if isinstance(data, list):
                # Write as JSON lines for large lists
                f.write('[')
                for i, item in enumerate(data):
                    if i > 0:
                        f.write(',')
                    json.dump(item, f)
                f.write(']')
            else:
                # Regular JSON for other types
                json.dump(data, f)
        
        return file_path
    
    @staticmethod
    def deserialize_from_file(file_path):
        """Deserialize data from a file with minimal memory usage"""
        with open(file_path, 'r') as f:
            return json.load(f)
    
    @staticmethod
    def stream_large_list(file_path):
        """Stream a large list from a file one item at a time"""
        with open(file_path, 'r') as f:
            # Check if it starts with a bracket
            char = f.read(1)
            if char != '[':
                f.seek(0)
                # Not a JSON array, try to parse whole file
                return json.load(f)
            
            # Start parsing array items
            buffer = ""
            depth = 1  # We've already seen one [
            in_string = False
            escape_next = False
            
            while True:
                char = f.read(1)
                if not char:  # End of file
                    break
                
                buffer += char
                
                # Handle string boundaries
                if char == '"' and not escape_next:
                    in_string = not in_string
                elif char == '\\' and in_string and not escape_next:
                    escape_next = True
                    continue
                
                if not in_string:
                    if char == '{':
                        depth += 1
                    elif char == '}':
                        depth -= 1
                    elif char == '[':
                        depth += 1
                    elif char == ']':
                        depth -= 1
                    elif char == ',' and depth == 1:
                        # We've reached the end of an item at the top level
                        try:
                            item = json.loads(buffer[:-1])  # Remove the comma
                            yield item
                        except json.JSONDecodeError as e:
                            _logger.error(f"Error parsing JSON item: {str(e)}, content: {buffer}")
                        buffer = ""
                
                escape_next = False
            
            # Handle the last item
            if buffer.rstrip().endswith(']'):
                buffer = buffer[:-1]  # Remove the closing bracket
            
            if buffer.strip():
                try:
                    item = json.loads(buffer)
                    yield item
                except json.JSONDecodeError as e:
                    _logger.error(f"Error parsing last JSON item: {str(e)}, content: {buffer}")
                    