# Add this to a new file called etl_transaction.py

from contextlib import contextmanager
from odoo import models, api, _
import logging
import time
import traceback
import hashlib
import random

_logger = logging.getLogger(__name__)

class ETLTransaction(models.AbstractModel):
    _name = 'etl.transaction'
    _description = 'ETL Transaction Management Utilities'
    
    @api.model
    @contextmanager
    def transaction_context(self, name="transaction", retry_count=3, isolation_level=None):
        """
        Context manager for safe transaction handling with retry mechanism
        
        Args:
            name: Name for the transaction (for logging)
            retry_count: Maximum number of retries on conflict
            isolation_level: Optional isolation level (e.g. "READ COMMITTED")
        
        Yields:
            The transaction context itself
        """
        attempt = 0
        while attempt <= retry_count:
            attempt += 1
            
            # Create savepoint for rollback
            savepoint_name = f"{name}_{int(time.time() * 1000) % 10000}"
            
            try:
                # Create savepoint for this operation
                self.env.cr.execute(f"SAVEPOINT {savepoint_name}")
                
                # Set isolation level if specified
                if isolation_level:
                    self.env.cr.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                
                # Yield control to the context block
                yield self
                
                # Release savepoint on successful completion
                self.env.cr.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                
                # Exit the retry loop on success
                break
                
            except Exception as e:
                # Check if this is a database conflict error that we can retry
                is_conflict = False
                if hasattr(e, 'pgcode'):
                    # PostgreSQL serialization/deadlock error codes
                    if e.pgcode in ('40001', '40P01'):
                        is_conflict = True
                elif 'could not serialize access' in str(e).lower() or 'deadlock detected' in str(e).lower():
                    is_conflict = True
                    
                # Roll back to savepoint
                try:
                    self.env.cr.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                except Exception as rollback_error:
                    _logger.error(f"Error in rollback to savepoint: {str(rollback_error)}")
                    # Fall back to full transaction rollback
                    self.env.cr.rollback()
                    
                # Handle the error based on retry status
                if is_conflict and attempt <= retry_count:
                    # Sleep with exponential backoff before retry
                    backoff_time = 0.1 * (2 ** (attempt - 1)) * (0.5 + random.random())
                    _logger.warning(f"Transaction conflict in {name} (attempt {attempt}/{retry_count}), "
                                f"retrying in {backoff_time:.2f}s: {str(e)}")
                    time.sleep(backoff_time)
                else:
                    # Either not a conflict or we've exceeded retries
                    _logger.error(f"Transaction error in {name}: {str(e)}")
                    raise  # Re-raise the exception
    # def transaction_context(self, name=None, retry_count=3, isolation_level=None):
    #     """
    #     Returns a context manager for transaction control with savepoints.
        
    #     Args:
    #         name: Name for this transaction (optional, will generate one if not provided)
    #         retry_count: Number of retries for retryable errors
    #         isolation_level: SQL isolation level (None uses system default)
            
    #     Usage:
    #         with self.env['etl.transaction'].transaction_context('my_operation') as tx:
    #             # Code to execute in a safe transaction context
    #             # If an exception is raised, it will automatically roll back to savepoint
    #     """
    #     return TransactionContext(self.env, name, retry_count, isolation_level)
    
    @api.model
    def generate_unique_name(self, prefix='tx'):
        """Generate a unique transaction name"""
        timestamp = int(time.time() * 1000)
        random_part = random.randint(1000, 9999)
        name_hash = hashlib.md5(f"{timestamp}_{random_part}".encode()).hexdigest()[:8]
        return f"{prefix}_{name_hash}"
    
    @api.model
    def is_retryable_error(self, exception):
        """Check if an exception is retryable"""
        error_str = str(exception).lower()
        
        # Common database error patterns that indicate retry may succeed
        retryable_patterns = [
            'deadlock',
            'could not serialize access',
            'serialization failure',
            'lock timeout',
            'duplicate key',
            'concurrent update',
            'idle in transaction',
            'statement timeout',
            'connection reset',
            'server closed the connection'
        ]
        
        return any(pattern in error_str for pattern in retryable_patterns)
    
    @api.model
    def handle_error(self, exception, name, attempt, max_attempts):
        """Handle an error in a transaction"""
        if attempt < max_attempts and self.is_retryable_error(exception):
            # Retryable error, will retry
            retry_delay = 2 ** attempt  # Exponential backoff
            _logger.warning(
                f"Retryable error in transaction {name} (attempt {attempt}/{max_attempts}): "
                f"{type(exception).__name__}: {str(exception)}. "
                f"Retrying in {retry_delay}s."
            )
            time.sleep(retry_delay)
            return True  # Retry
        else:
            # Non-retryable error or max retries exceeded
            _logger.error(
                f"Error in transaction {name} (attempt {attempt}/{max_attempts}): "
                f"{type(exception).__name__}: {str(exception)}\n"
                f"{traceback.format_exc()}"
            )
            return False  # Don't retry
            

class TransactionContext:
    """Context manager for safe transaction handling with savepoints"""
    
    def __init__(self, env, name=None, retry_count=3, isolation_level=None):
        self.env = env
        self.name = name or env['etl.transaction'].generate_unique_name()
        self.retry_count = retry_count
        self.isolation_level = isolation_level
        self.savepoint_name = f"sp_{self.name.replace('-', '_')}"
        self.attempt = 0
        self.transaction_handler = env['etl.transaction']
    
    # def __enter__(self):
    #     """Begin a transaction or create a savepoint"""
    #     self.attempt += 1
        
    #     # Set isolation level if specified
    #     if self.isolation_level:
    #         # Temporarily change isolation level
    #         self.env.cr.execute(f"SET TRANSACTION ISOLATION LEVEL {self.isolation_level}")
        
    #     # Create a savepoint
    #     self.env.cr.execute(f"SAVEPOINT {self.savepoint_name}")
    #     _logger.debug(f"Transaction {self.name} started (attempt {self.attempt}/{self.retry_count})")
        
    #     return self
    
    def __enter__(self):
        """Begin a transaction or create a savepoint"""
        self.attempt += 1
        
        try:
            # Create a savepoint first - this works whether or not a transaction is in progress
            self.env.cr.execute(f"SAVEPOINT {self.savepoint_name}")
            
            # Only try to set isolation level for a new transaction
            # This won't work if we're in the middle of a transaction, so wrap in try/except
            if self.isolation_level:
                try:
                    self.env.cr.execute(f"SET TRANSACTION ISOLATION LEVEL {self.isolation_level}")
                except Exception as e:
                    # If this fails, log it but continue with the savepoint approach
                    _logger.debug(f"Could not set isolation level {self.isolation_level}: {str(e)}")
                    
            _logger.debug(f"Transaction {self.name} started (attempt {self.attempt}/{self.retry_count})")
            return self
            
        except Exception as e:
            _logger.error(f"Failed to start transaction context: {str(e)}")
            # Re-raise to be handled by __exit__
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """End the transaction - commit or rollback as needed"""
        if exc_type is None:
            # No exception, release savepoint
            self.env.cr.execute(f"RELEASE SAVEPOINT {self.savepoint_name}")
            _logger.debug(f"Transaction {self.name} completed successfully")
            return False  # Don't suppress exception
        
        # Exception occurred, rollback to savepoint
        try:
            self.env.cr.execute(f"ROLLBACK TO SAVEPOINT {self.savepoint_name}")
            _logger.debug(f"Rolled back to savepoint {self.savepoint_name}")
        except Exception as e:
            # Issue with rollback, try full rollback
            _logger.error(f"Error in rollback to savepoint: {str(e)}")
            try:
                self.env.cr.rollback()
                _logger.warning("Performed full transaction rollback after savepoint rollback failure")
            except:
                pass  # Avoid nested exception
        
        # Check if we should retry
        should_retry = self.transaction_handler.handle_error(
            exc_val, self.name, self.attempt, self.retry_count
        )
        
        if should_retry and self.attempt < self.retry_count:
            # Try again
            return self.__enter__()
        
        # Don't suppress the exception
        return False

