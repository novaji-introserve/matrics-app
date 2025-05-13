from odoo import models, fields, api
import json
import base64
from datetime import datetime, timedelta
import logging
import random
import time
import threading
from odoo import registry as odoo_registry

_logger = logging.getLogger(__name__)

# Thread registry for background refresh
class ThreadRegistry:
    def __init__(self):
        self._lock = threading.RLock()
        self._threads = {}
    
    def register(self, key, thread):
        with self._lock:
            self._threads[key] = {
                'thread': thread,
                'timestamp': time.time()
            }
    
    def unregister(self, key):
        with self._lock:
            if key in self._threads:
                del self._threads[key]
    
    def is_active(self, key):
        with self._lock:
            return (key in self._threads and 
                  self._threads[key]['thread'].is_alive() and
                  time.time() - self._threads[key]['timestamp'] < 300)
    
    def cleanup_stale(self):
        with self._lock:
            for key in list(self._threads.keys()):
                if not self._threads[key]['thread'].is_alive() or time.time() - self._threads[key]['timestamp'] > 300:
                    del self._threads[key]

# Global thread registry instance
THREAD_REGISTRY = ThreadRegistry()

class DashboardCache(models.Model):
    _name = 'res.dashboard.cache'
    _description = 'Dashboard Cache Storage'

    # Original fields - kept as they were
    name = fields.Char(string='Cache Key', required=True, index=True)
    user_id = fields.Many2one('res.users', string='User', required=True, index=True)
    cache_data = fields.Binary(string='Cache Data', attachment=True)  # Original field name
    last_updated = fields.Datetime(string='Last Updated', default=fields.Datetime.now, index=True)
    expiry_time = fields.Datetime(string='Expiry Time', index=True)
    refresh_after = fields.Datetime(string='Refresh After', compute='_compute_refresh_after', store=True, index=True)
    refresh_in_progress = fields.Boolean(string='Refresh In Progress', default=False, index=True)
    
    # Add size indicator for monitoring (keeping original fields intact)
    data_size = fields.Integer(string='Data Size (bytes)', default=0)
    
    _sql_constraints = [
        ('unique_cache_key_user', 'UNIQUE(name, user_id)', 'Cache key must be unique per user!')
    ]
    
    @api.depends('last_updated', 'expiry_time')
    def _compute_refresh_after(self):
        """Compute when this cache entry should be refreshed"""
        for cache in self:
            if cache.last_updated and cache.expiry_time:
                ttl = (cache.expiry_time - cache.last_updated).total_seconds()
                cache.refresh_after = cache.last_updated + timedelta(seconds=ttl * 0.7)
            else:
                cache.refresh_after = fields.Datetime.now()
    
    @api.model
    def set_cache(self, key, data, user_id=None, ttl=600):
        """Store data in cache using ORM methods instead of direct SQL"""
        if user_id is None:
            user_id = self.env.user.id
        
        # Calculate size for monitoring
        json_data = json.dumps(data)
        data_size = len(json_data.encode('utf-8'))
        binary_data = base64.b64encode(json_data.encode('utf-8'))
        expiry = datetime.now() + timedelta(seconds=ttl)
        
        # Use ORM with a new cursor to avoid transaction issues
        max_retries = 5
        for attempt in range(max_retries):
            try:
                with self.env.registry.cursor() as new_cr:
                    new_env = self.env(cr=new_cr)
                    
                    existing = new_env['res.dashboard.cache'].search([
                        ('name', '=', key), 
                        ('user_id', '=', user_id)
                    ])
                    
                    if existing:
                        existing.write({
                            'cache_data': binary_data,
                            'last_updated': fields.Datetime.now(),
                            'expiry_time': expiry,
                            'refresh_in_progress': False,
                            'data_size': data_size
                        })
                    else:
                        new_env['res.dashboard.cache'].create({
                            'name': key,
                            'user_id': user_id,
                            'cache_data': binary_data,
                            'last_updated': fields.Datetime.now(),
                            'expiry_time': expiry,
                            'refresh_in_progress': False,
                            'data_size': data_size
                        })
                    
                    new_cr.commit()
                
                return True
                
            except Exception as e:
                if attempt < max_retries - 1:
                    delay = random.uniform(0.5, 2) * (2 ** attempt)
                    _logger.warning(f"Cache set attempt {attempt+1} failed: {str(e)}. Retrying in {delay:.2f}s...")
                    time.sleep(delay)
                else:
                    _logger.error(f"All cache set attempts failed for key {key}: {str(e)}")
        
        return False
    
    @api.model
    def get_cache(self, key, user_id=None):
        """Retrieve data from cache using ORM instead of direct SQL"""
        if user_id is None:
            user_id = self.env.user.id
            
        # Use ORM to find the cache entry
        cache = self.search([
            ('name', '=', key), 
            ('user_id', '=', user_id),
            ('expiry_time', '>', fields.Datetime.now())
        ], limit=1)
        
        if not cache:
            return None
            
        try:
            # Check if we should trigger background refresh
            now = fields.Datetime.now()
            
            if now > cache.refresh_after and not cache.refresh_in_progress:
                _logger.debug(f"Triggering background refresh for key: {key}")
                self._trigger_background_refresh(key, user_id)
            
            # Return the cached data
            binary_data = cache.cache_data
            if not binary_data:
                return None
                
            json_data = base64.b64decode(binary_data).decode('utf-8')
            return json.loads(json_data)
            
        except Exception as e:
            _logger.error(f"Error retrieving cache: {str(e)}")
            return None
    
    @api.model
    def _trigger_background_refresh(self, key, user_id):
        """Trigger background refresh using ORM methods"""
        try:
            # Check if a thread is already running
            if THREAD_REGISTRY.is_active(key):
                return
            
            # Mark cache as being refreshed using ORM
            cache = self.search([
                ('name', '=', key), 
                ('user_id', '=', user_id),
                ('refresh_in_progress', '=', False)
            ], limit=1)
            
            if not cache:
                return
                
            cache.refresh_in_progress = True
            
            # Start thread for background refresh
            thread_id = f"{key}_{user_id}_{time.time()}"
            refresh_thread = threading.Thread(
                target=self._refresh_cache_background_thread,
                args=(self.env.cr.dbname, self.env.uid, key, user_id, thread_id),
                daemon=True
            )
            
            THREAD_REGISTRY.register(key, refresh_thread)
            refresh_thread.start()
            
        except Exception as e:
            _logger.error(f"Error triggering background refresh: {str(e)}")
            
            # Try to reset the flag
            try:
                cache = self.search([
                    ('name', '=', key), 
                    ('user_id', '=', user_id)
                ], limit=1)
                if cache:
                    cache.refresh_in_progress = False
            except:
                pass
    
    @staticmethod
    def _refresh_cache_background_thread(dbname, uid, key, user_id, thread_id):
        """Background thread to refresh cache data"""
        try:
            # Small delay to prevent race conditions
            time.sleep(0.25)
            
            _logger.debug(f"Starting background refresh for key: {key}")
            
            # Create a new environment for this thread
            with odoo_registry(dbname).cursor() as cr:
                env = api.Environment(cr, uid, {})
                
                # Check that the key is still valid
                cache = env['res.dashboard.cache'].search([
                    ('name', '=', key), 
                    ('user_id', '=', user_id),
                    ('refresh_in_progress', '=', True)
                ], limit=1)
                
                if not cache:
                    return
                
                # Refresh the data
                try:
                    result = None
                    
                    if 'charts_data_' in key:
                        result = env['res.dashboard.cache']._refresh_charts_data(key, user_id)
                    elif 'all_stats_' in key:
                        result = env['res.dashboard.cache']._refresh_stats_data(key, user_id)
                    
                    if result:
                        env['res.dashboard.cache'].set_cache(key, result, user_id)
                except Exception as e:
                    _logger.error(f"Error in refresh operation: {str(e)}")
                
                # Reset refresh flag
                try:
                    if cache.exists():
                        cache.refresh_in_progress = False
                        cr.commit()
                except Exception as e:
                    _logger.error(f"Error resetting refresh flag: {str(e)}")
            
        except Exception as e:
            _logger.error(f"Fatal error in background refresh thread: {str(e)}")
        finally:
            THREAD_REGISTRY.unregister(key)
    
    # The rest of your methods unchanged
    @api.model
    def _refresh_charts_data(self, key, user_id):
        """Refresh charts data based on parameters encoded in the key"""
        # Extract parameters from the key
        # Format: charts_data_{cco}_{branches_id}_{unique_id}
        parts = key.split('_')
        if len(parts) >= 4:
            cco_part = parts[2]
            cco = cco_part.lower() == 'true'
            
            # Extract branches_id (might be a JSON string of an array)
            branches_str = parts[3]
            try:
                branches_id = json.loads(branches_str)
            except:
                branches_id = []
            
            # Call the controller method with a smaller timeout
            from ..controllers.charts import DynamicChartController
            controller = DynamicChartController()
            return controller.get_chart_data_internal(cco, branches_id)
        
        return None
    
    @api.model
    def _refresh_stats_data(self, key, user_id):
        """Refresh stats data based on parameters encoded in the key"""
        # Extract parameters for stats
        # Format: all_stats_{cco}_{branches_id}_{unique_id}
        parts = key.split('_')
        if len(parts) >= 4:
            cco_part = parts[2]
            cco = cco_part.lower() == 'true'
            
            # Extract branches_id
            branches_str = parts[3]
            try:
                branches_id = json.loads(branches_str)
            except:
                branches_id = []
            
            # Call the controller method
            from ..controllers.controllers import Compliance
            stats_controller = Compliance()
            return stats_controller.get_all_stats_internal(cco, branches_id)
        
        return None
    
    @api.model
    def clear_expired_cache(self):
        """Clear all expired cache entries and stale refresh flags - called by cron job"""
        # Clear truly expired entries
        expired = self.search([
            ('expiry_time', '<', fields.Datetime.now())
        ])
        if expired:
            _logger.info(f"Clearing {len(expired)} expired cache entries")
            expired.unlink()
        
        # Reset stale refresh_in_progress flags (older than 10 minutes)
        stale_time = fields.Datetime.now() - timedelta(minutes=10)
        stale_refreshes = self.search([
            ('refresh_in_progress', '=', True),
            ('last_updated', '<', stale_time)
        ])
        
        if stale_refreshes:
            _logger.info(f"Resetting {len(stale_refreshes)} stale refresh flags")
            stale_refreshes.write({'refresh_in_progress': False})
        
        # Clean up the thread registry
        THREAD_REGISTRY.cleanup_stale()
        
        return True























# from odoo import models, fields, api
# import json
# import base64
# from datetime import datetime, timedelta
# import logging
# import random
# import time
# import threading
# from odoo import registry as odoo_registry

# _logger = logging.getLogger(__name__)

# # Thread pool for background cache refreshing
# REFRESH_THREAD_POOL = {}

# class DashboardCache(models.Model):
#     _name = 'res.dashboard.cache'
#     _description = 'Dashboard Cache Storage'

#     name = fields.Char(string='Cache Key', required=True, index=True)
#     user_id = fields.Many2one('res.users', string='User', required=True, index=True)
#     cache_data = fields.Binary(string='Cache Data', attachment=True)  # Uses filestore
#     last_updated = fields.Datetime(string='Last Updated', default=fields.Datetime.now)
#     expiry_time = fields.Datetime(string='Expiry Time')
#     refresh_after = fields.Datetime(string='Refresh After', compute='_compute_refresh_after', store=True)
#     refresh_in_progress = fields.Boolean(string='Refresh In Progress', default=False)
    
#     _sql_constraints = [
#         ('unique_cache_key_user', 'UNIQUE(name, user_id)', 'Cache key must be unique per user!')
#     ]
    
#     @api.depends('last_updated', 'expiry_time')
#     def _compute_refresh_after(self):
#         """Compute when this cache entry should be refreshed (at 70% of its lifetime)"""
#         for cache in self:
#             if cache.last_updated and cache.expiry_time:
#                 # Calculate the total TTL in seconds
#                 ttl = (cache.expiry_time - cache.last_updated).total_seconds()
#                 # Set refresh_after to 70% of the cache lifetime 
#                 cache.refresh_after = cache.last_updated + timedelta(seconds=ttl * 0.7)
#             else:
#                 cache.refresh_after = fields.Datetime.now()
    
#     @api.model
#     def set_cache(self, key, data, user_id=None, ttl=600):  # Extended to 10 minutes
#         """Store data in cache with expiry time (10 minutes default)"""
#         if user_id is None:
#             user_id = self.env.user.id
            
#         json_data = json.dumps(data)
#         binary_data = base64.b64encode(json_data.encode('utf-8'))
        
#         expiry = datetime.now() + timedelta(seconds=ttl)
        
#         # Try the operation with retries
#         max_retries = 5
#         for attempt in range(max_retries):
#             try:
#                 # Create a new cursor for each attempt to avoid transaction abortion issues
#                 with self.env.registry.cursor() as new_cr:
#                     # Create a new environment with the new cursor
#                     new_env = self.env(cr=new_cr)
                    
#                     # Get existing cache record if any
#                     existing = new_env['res.dashboard.cache'].search([('name', '=', key), ('user_id', '=', user_id)])
                    
#                     if existing:
#                         # Update with a new cursor - write directly to avoid attachment issues
#                         existing.with_context(no_document=True).write({
#                             'cache_data': binary_data,
#                             'last_updated': fields.Datetime.now(),
#                             'expiry_time': expiry,
#                             'refresh_in_progress': False  # Reset the refresh flag
#                         })
#                     else:
#                         # Create with a new cursor
#                         new_env['res.dashboard.cache'].create({
#                             'name': key,
#                             'user_id': user_id,
#                             'cache_data': binary_data,
#                             'last_updated': fields.Datetime.now(),
#                             'expiry_time': expiry,
#                             'refresh_in_progress': False
#                         })
                    
#                     # Commit the transaction explicitly
#                     new_cr.commit()
                
#                 # If we get here, it worked
#                 return True
                
#             except Exception as e:
#                 if attempt < max_retries - 1:
#                     # Random backoff before retry
#                     delay = random.uniform(0.5, 2) * (attempt + 1)
#                     _logger.warning(f"Cache set attempt {attempt+1} failed: {str(e)}. Retrying in {delay:.2f}s...")
#                     time.sleep(delay)
#                 else:
#                     _logger.error(f"All cache set attempts failed: {str(e)}")
#                     # Continue without caching in this case
        
#         return False
    
#     # @api.model
#     # def get_cache(self, key, user_id=None):
#     #     """Retrieve data from cache if not expired and trigger background refresh if needed"""
#     #     if user_id is None:
#     #         user_id = self.env.user.id
            
#     #     cache = self.search([
#     #         ('name', '=', key), 
#     #         ('user_id', '=', user_id),
#     #         ('expiry_time', '>', fields.Datetime.now())
#     #     ], limit=1)
        
#     #     if not cache:
#     #         return None
            
#     #     try:
#     #         # Check if we should trigger a background refresh (if 70% of TTL has passed)
#     #         now = fields.Datetime.now()
#     #         if now > cache.refresh_after and not cache.refresh_in_progress:
#     #             self._trigger_background_refresh(key, user_id)
            
#     #         # Return the current cached data
#     #         binary_data = cache.cache_data
#     #         json_data = base64.b64decode(binary_data).decode('utf-8')
#     #         return json.loads(json_data)
#     #     except Exception as e:
#     #         _logger.error(f"Error retrieving cache: {str(e)}")
#     #         return None
    
#     @api.model
#     def get_cache(self, key, user_id=None):
#         """Retrieve data from cache if not expired and trigger background refresh if needed"""
#         if user_id is None:
#             user_id = self.env.user.id
            
#         cache = self.search([
#             ('name', '=', key), 
#             ('user_id', '=', user_id),
#             ('expiry_time', '>', fields.Datetime.now())
#         ], limit=1)
        
#         if not cache:
#             return None
            
#         try:
#             # Check if we should trigger a background refresh (if 70% of TTL has passed)
#             now = fields.Datetime.now()
            
#             # Add detailed logging here to see why refresh is not being triggered
#             _logger.debug(f"CACHE DEBUG - key: {key}, refresh_after: {cache.refresh_after}, now: {now}")
#             _logger.debug(f"CACHE DEBUG - Should refresh? {now > cache.refresh_after}, In progress? {cache.refresh_in_progress}")
            
#             if now > cache.refresh_after and not cache.refresh_in_progress:
#                 _logger.debug(f"CACHE DEBUG - Attempting to trigger background refresh for key: {key}")
#                 self._trigger_background_refresh(key, user_id)
#                 _logger.debug(f"CACHE DEBUG - Successfully called _trigger_background_refresh")
#             else:
#                 if now <= cache.refresh_after:
#                     _logger.debug(f"CACHE DEBUG - Refresh not needed yet. Time remaining: {(cache.refresh_after - now).total_seconds()}s")
#                 if cache.refresh_in_progress:
#                     _logger.debug(f"CACHE DEBUG - Refresh already in progress for this key")
            
#             # Return the current cached data
#             binary_data = cache.cache_data
#             json_data = base64.b64decode(binary_data).decode('utf-8')
#             return json.loads(json_data)
#         except Exception as e:
#             _logger.error(f"Error retrieving cache: {str(e)}")
#             return None
    
#     # @api.model
#     # def _trigger_background_refresh(self, key, user_id):
#     #     """Trigger background refresh of cache data without using cron jobs"""
#     #     # Mark the cache as being refreshed to prevent multiple refreshes
#     #     try:
#     #         with self.env.registry.cursor() as cr:
#     #             env = self.env(cr=cr)
#     #             cache = env['res.dashboard.cache'].search([('name', '=', key), ('user_id', '=', user_id)], limit=1)
#     #             if cache and not cache.refresh_in_progress:
#     #                 cache.refresh_in_progress = True
#     #                 cr.commit()
                    
#     #                 # Generate a unique thread identifier
#     #                 thread_id = f"{key}_{user_id}_{time.time()}"
                    
#     #                 # Check if a refresh thread is already running for this key
#     #                 if key in REFRESH_THREAD_POOL and REFRESH_THREAD_POOL[key].is_alive():
#     #                     _logger.debug(f"Refresh already in progress for key: {key}")
#     #                     return
                    
#     #                 # Start a new thread to refresh the cache
#     #                 refresh_thread = threading.Thread(
#     #                     target=self._refresh_cache_background_thread,
#     #                     args=(self.env.cr.dbname, self.env.uid, key, user_id, thread_id),
#     #                     daemon=True
#     #                 )
#     #                 REFRESH_THREAD_POOL[key] = refresh_thread
#     #                 refresh_thread.start()
#     #                 _logger.debug(f"Started background refresh for cache key: {key}")
#     #     except Exception as e:
#     #         _logger.error(f"Error triggering background refresh: {str(e)}")
    
#     @api.model
#     def _trigger_background_refresh(self, key, user_id):
#         """Trigger background refresh of cache data without using cron jobs"""
#         # Mark the cache as being refreshed to prevent multiple refreshes
#         try:
#             _logger.debug(f"REFRESH DEBUG - Entered _trigger_background_refresh for key: {key}")
            
#             with self.env.registry.cursor() as cr:
#                 env = self.env(cr=cr)
#                 cache = env['res.dashboard.cache'].search([('name', '=', key), ('user_id', '=', user_id)], limit=1)
                
#                 _logger.debug(f"REFRESH DEBUG - Found cache record? {bool(cache)}")
                
#                 if cache and not cache.refresh_in_progress:
#                     _logger.debug(f"REFRESH DEBUG - Setting refresh_in_progress=True for key: {key}")
#                     cache.refresh_in_progress = True
#                     cr.commit()
                    
#                     # Generate a unique thread identifier
#                     thread_id = f"{key}_{user_id}_{time.time()}"
                    
#                     # Check if a refresh thread is already running for this key
#                     if key in REFRESH_THREAD_POOL and REFRESH_THREAD_POOL[key].is_alive():
#                         _logger.debug(f"REFRESH DEBUG - Refresh already in progress for key: {key}")
#                         return
                    
#                     # Log thread pool status
#                     _logger.debug(f"REFRESH DEBUG - Current thread pool: {list(REFRESH_THREAD_POOL.keys())}")
                    
#                     # Start a new thread to refresh the cache
#                     _logger.debug(f"REFRESH DEBUG - Creating thread for key: {key}")
#                     refresh_thread = threading.Thread(
#                         target=self._refresh_cache_background_thread,
#                         args=(self.env.cr.dbname, self.env.uid, key, user_id, thread_id),
#                         daemon=True
#                     )
                    
#                     REFRESH_THREAD_POOL[key] = refresh_thread
#                     _logger.debug(f"REFRESH DEBUG - Starting thread for key: {key}")
#                     refresh_thread.start()
#                     _logger.debug(f"REFRESH DEBUG - Thread started for key: {key}")
#                 else:
#                     if not cache:
#                         _logger.debug(f"REFRESH DEBUG - Cache record not found for key: {key}")
#                     elif cache.refresh_in_progress:
#                         _logger.debug(f"REFRESH DEBUG - Refresh already in progress for key: {key}")
#         except Exception as e:
#             _logger.error(f"REFRESH DEBUG - Error triggering background refresh: {str(e)}")
    
#     @staticmethod
#     def _refresh_cache_background_thread(dbname, uid, key, user_id, thread_id):
#         """Static method to run in a background thread to refresh cache data"""
#         try:
#             # Small delay to ensure we don't interfere with ongoing transactions
#             time.sleep(0.5)
            
#             _logger.debug(f"====== STARTING BACKGROUND REFRESH for key: {key} ======")
#             print(f"====== STARTING BACKGROUND REFRESH for key: {key} ======")
            
#             # Create a new environment for this thread
#             with odoo_registry(dbname).cursor() as cr:
#                 env = api.Environment(cr, uid, {})
                    
#                 _logger.debug(f"====== CREATED ENVIRONMENT for key: {key} ======")
#                 print(f"====== CREATED ENVIRONMENT for key: {key} ======")
                
#                 # Check if thread is properly running
#                 _logger.debug(f"====== THREAD IS RUNNING WITH ID: {thread_id} ======")
#                 print(f"====== THREAD IS RUNNING WITH ID: {thread_id} ======")
                
#                 # Determine which data to refresh based on the key
#                 if 'charts_data_' in key:
#                     # Extract parameters from the key
#                     # Format: charts_data_{cco}_{branches_id}_{datepicked}_{unique_id}
#                     parts = key.split('_')
#                     if len(parts) >= 5:
#                         cco_part = parts[2]
#                         cco = cco_part.lower() == 'true'
                        
#                         # Extract branches_id (might be a JSON string of an array)
#                         branches_str = parts[3]
#                         try:
#                             branches_id = json.loads(branches_str)
#                         except:
#                             branches_id = []
                        
#                         # Extract datepicked
#                         try:
#                             datepicked = int(parts[4])
#                         except:
#                             datepicked = 20000
                        
#                         # Call the controller method
#                         charts_controller = env['compliance.chart.controller'].new({})
#                         result = charts_controller.get_chart_data(cco, branches_id, datepicked)
                        
#                         if result:
#                             # Store the refreshed data
#                             env['res.dashboard.cache'].set_cache(key, result, user_id)
#                             _logger.debug(f"Successfully refreshed charts data for key: {key}")
                
#                 elif 'all_stats_' in key:
#                     # Extract parameters for stats
#                     # Format: all_stats_{cco}_{branches_id}_{datepicked}_{unique_id}
#                     parts = key.split('_')
#                     if len(parts) >= 5:
#                         cco_part = parts[2]
#                         cco = cco_part.lower() == 'true'
                        
#                         # Extract branches_id
#                         branches_str = parts[3]
#                         try:
#                             branches_id = json.loads(branches_str)
#                         except:
#                             branches_id = []
                        
#                         # Extract datepicked
#                         try:
#                             datepicked = int(parts[4])
#                         except:
#                             datepicked = 20000
                        
#                         # Call the controller method
#                         stats_controller = env['compliance.stats.controller'].new({})
#                         result = stats_controller.get_all_stats(cco, branches_id, datepicked)
                        
#                         if result:
#                             # Store the refreshed data
#                             env['res.dashboard.cache'].set_cache(key, result, user_id)
#                             _logger.debug(f"Successfully refreshed stats data for key: {key}")
                
#                 # Add handling for other cache types if needed
                
#                 # Reset the refresh_in_progress flag even if refresh failed
#                 cache = env['res.dashboard.cache'].search([('name', '=', key), ('user_id', '=', user_id)], limit=1)
#                 if cache:
#                     cache.refresh_in_progress = False
#                     cr.commit()
            
#             _logger.debug(f"====== COMPLETED BACKGROUND REFRESH for key: {key} ======")
#             print(f"====== COMPLETED BACKGROUND REFRESH for key: {key} ======")
            
#         except Exception as e:
#             _logger.error(f"====== FAILED BACKGROUND REFRESH for key {key}: {str(e)} ======")
#             # Try to reset the refresh flag in a new transaction
#             try:
#                 with api.Environment.manage():
#                     registry = models.registry(dbname)
#                     with registry.cursor() as cr:
#                         env = api.Environment(cr, uid, {})
#                         cache = env['res.dashboard.cache'].search([('name', '=', key), ('user_id', '=', user_id)], limit=1)
#                         if cache:
#                             cache.refresh_in_progress = False
#                             cr.commit()
#             except:
#                 pass  # If this fails too, the cleanup cron job will eventually clear expired entries
        
#         finally:
#             # Remove thread from pool
#             if key in REFRESH_THREAD_POOL:
#                 del REFRESH_THREAD_POOL[key]
    
#     @api.model
#     def clear_expired_cache(self):
#         """Clear all expired cache entries - called by cron job"""
#         expired = self.search([
#             ('expiry_time', '<', fields.Datetime.now())
#         ])
#         if expired:
#             _logger.debug(f"Clearing {len(expired)} expired cache entries")
#             expired.unlink()
#             return True
#         return False













# from odoo import models, fields, api
# import json
# import base64
# from datetime import datetime, timedelta
# import logging
# import random
# import time

# _logger = logging.getLogger(__name__)

# class DashboardCache(models.Model):
#     _name = 'res.dashboard.cache'
#     _description = 'Dashboard Cache Storage'

#     name = fields.Char(string='Cache Key', required=True, index=True)
#     user_id = fields.Many2one('res.users', string='User', required=True, index=True)
#     cache_data = fields.Binary(string='Cache Data', attachment=True)  # Uses filestore
#     last_updated = fields.Datetime(string='Last Updated', default=fields.Datetime.now)
#     expiry_time = fields.Datetime(string='Expiry Time')
    
#     _sql_constraints = [
#         ('unique_cache_key_user', 'UNIQUE(name, user_id)', 'Cache key must be unique per user!')
#     ]
    
#     @api.model
#     def set_cache(self, key, data, user_id=None, ttl=300):
#         """Store data in cache with expiry time (5 minutes default)"""
#         if user_id is None:
#             user_id = self.env.user.id
            
#         json_data = json.dumps(data)
#         binary_data = base64.b64encode(json_data.encode('utf-8'))
        
#         expiry = datetime.now() + timedelta(seconds=ttl)
        
#         # Try the operation with retries
#         max_retries = 5
#         for attempt in range(max_retries):
#             try:
#                 # Create a new cursor for each attempt to avoid transaction abortion issues
#                 with self.env.registry.cursor() as new_cr:
#                     # Create a new environment with the new cursor
#                     new_env = self.env(cr=new_cr)
                    
#                     # Get existing cache record if any
#                     existing = new_env['res.dashboard.cache'].search([('name', '=', key), ('user_id', '=', user_id)])
                    
#                     if existing:
#                         # Update with a new cursor - write directly to avoid attachment issues
#                         existing.with_context(no_document=True).write({
#                             'cache_data': binary_data,
#                             'last_updated': fields.Datetime.now(),
#                             'expiry_time': expiry
#                         })
#                     else:
#                         # Create with a new cursor
#                         new_env['res.dashboard.cache'].create({
#                             'name': key,
#                             'user_id': user_id,
#                             'cache_data': binary_data,
#                             'last_updated': fields.Datetime.now(),
#                             'expiry_time': expiry
#                         })
                    
#                     # Commit the transaction explicitly
#                     new_cr.commit()
                
#                 # If we get here, it worked
#                 return True
                
#             except Exception as e:
#                 if attempt < max_retries - 1:
#                     # Random backoff before retry
#                     delay = random.uniform(0.5, 2) * (attempt + 1)
#                     _logger.warning(f"Cache set attempt {attempt+1} failed: {str(e)}. Retrying in {delay:.2f}s...")
#                     time.sleep(delay)
#                 else:
#                     _logger.error(f"All cache set attempts failed: {str(e)}")
#                     # Continue without caching in this case
        
#         return False
    
#     @api.model
#     def get_cache(self, key, user_id=None):
#         """Retrieve data from cache if not expired"""
#         if user_id is None:
#             user_id = self.env.user.id
            
#         cache = self.search([
#             ('name', '=', key), 
#             ('user_id', '=', user_id),
#             ('expiry_time', '>', fields.Datetime.now())
#         ], limit=1)
        
#         if not cache:
#             return None
            
#         try:
#             binary_data = cache.cache_data
#             json_data = base64.b64decode(binary_data).decode('utf-8')
#             return json.loads(json_data)
#         except Exception as e:
#             _logger.error(f"Error retrieving cache: {str(e)}")
#             return None
    
#     @api.model
#     def clear_expired_cache(self):
#         """Clear all expired cache entries - called by cron job"""
#         expired = self.search([
#             ('expiry_time', '<', fields.Datetime.now())
#         ])
#         if expired:
#             _logger.debug(f"Clearing {len(expired)} expired cache entries")
#             expired.unlink()
#             return True
#         return False



