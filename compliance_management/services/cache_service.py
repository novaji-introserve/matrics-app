# -*- coding: utf-8 -*-

import logging
import json
import base64
import random
import time
import threading
from datetime import datetime, timedelta
from odoo import fields, api

_logger = logging.getLogger(__name__)

class ThreadRegistry:
    """Registry for managing threads with safety and cleanup functionality."""
    
    def __init__(self):
        """
        Initialize the ThreadRegistry.

        This sets up a reentrant lock and a dictionary to hold thread information.
        """
        self._lock = threading.RLock()
        self._threads = {}

    def register(self, key, thread):
        """Register a new thread with a unique key.

        Args:
            key (str): The unique key to associate with the thread.
            thread (threading.Thread): The thread to register.
        """
        with self._lock:
            self._threads[key] = {"thread": thread, "timestamp": time.time()}

    def unregister(self, key):
        """Unregister a thread using its unique key.

        Args:
            key (str): The unique key associated with the thread to unregister.
        """
        with self._lock:
            if key in self._threads:
                del self._threads[key]

    def is_active(self, key):
        """Check if a thread is active and within its allowed time limit.

        Args:
            key (str): The unique key associated with the thread.

        Returns:
            bool: True if the thread is active, False otherwise.
        """
        with self._lock:
            return (
                key in self._threads
                and self._threads[key]["thread"].is_alive()
                and time.time() - self._threads[key]["timestamp"] < 300
            )

    def cleanup_stale(self):
        """Remove threads that are no longer active or have exceeded their time limit."""
        with self._lock:
            for key in list(self._threads.keys()):
                if (
                    not self._threads[key]["thread"].is_alive()
                    or time.time() - self._threads[key]["timestamp"] > 300
                ):
                    del self._threads[key]

# Global thread registry
THREAD_REGISTRY = ThreadRegistry()

class CacheService:
    """Service for caching operations."""

    def __init__(self, env=None):
        """Initialize the CacheService.

        Args:
            env (Environment, optional): The Odoo environment. Defaults to None.
        """
        self.env = env

    def get_cache(self, key, user_id=None):
        """Retrieve data from cache.

        Args:
            key (str): The cache key from which to retrieve data.
            user_id (int, optional): The ID of the user associated with the cache.

        Returns:
            any: The cached data or None if not found or expired.
        """
        if not self.env:
            return None
            
        if user_id is None:
            user_id = self.env.user.id
            
        cache = self.env["res.dashboard.cache"].search(
            [
                ("name", "=", key),
                ("user_id", "=", user_id),
                ("expiry_time", ">", fields.Datetime.now()),
            ],
            limit=1,
        )
        
        if not cache:
            return None
            
        try:
            now = fields.Datetime.now()
            if now > cache.refresh_after and not cache.refresh_in_progress:
                _logger.debug(f"Triggering background refresh for key: {key}")
                self._trigger_background_refresh(key, user_id)
                
            binary_data = cache.cache_data
            if not binary_data:
                return None
                
            json_data = base64.b64decode(binary_data).decode("utf-8")
            return json.loads(json_data)
        except Exception as e:
            _logger.error(f"Error retrieving cache: {str(e)}")
            return None

    def set_cache(self, key, data, user_id=None, ttl=600):
        """Store data in cache.

        Args:
            key (str): The cache key under which to store the data.
            data (any): The data to be cached.
            user_id (int, optional): The ID of the user associated with the cache.
            ttl (int, optional): Time-to-live for the cache entry in seconds.

        Returns:
            bool: True if the cache was successfully set, False otherwise.
        """
        if not self.env:
            return False
            
        if user_id is None:
            user_id = self.env.user.id
            
        json_data = json.dumps(data)
        data_size = len(json_data.encode("utf-8"))
        binary_data = base64.b64encode(json_data.encode("utf-8"))
        expiry = datetime.now() + timedelta(seconds=ttl)
        
        max_retries = 5
        for attempt in range(max_retries):
            try:
                with self.env.registry.cursor() as new_cr:
                    new_env = self.env(cr=new_cr)
                    existing = new_env["res.dashboard.cache"].search(
                        [("name", "=", key), ("user_id", "=", user_id)]
                    )
                    
                    if existing:
                        existing.write(
                            {
                                "cache_data": binary_data,
                                "last_updated": fields.Datetime.now(),
                                "expiry_time": expiry,
                                "refresh_in_progress": False,
                                "data_size": data_size,
                            }
                        )
                    else:
                        new_env["res.dashboard.cache"].create(
                            {
                                "name": key,
                                "user_id": user_id,
                                "cache_data": binary_data,
                                "last_updated": fields.Datetime.now(),
                                "expiry_time": expiry,
                                "refresh_in_progress": False,
                                "data_size": data_size,
                            }
                        )
                    new_cr.commit()
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    delay = random.uniform(0.5, 2) * (2**attempt)
                    _logger.warning(
                        f"Cache set attempt {attempt+1} failed: {str(e)}. Retrying in {delay:.2f}s..."
                    )
                    time.sleep(delay)
                else:
                    _logger.error(
                        f"All cache set attempts failed for key {key}: {str(e)}"
                    )
        return False

    def _trigger_background_refresh(self, key, user_id):
        """Trigger a background refresh for a cache entry.

        Args:
            key (str): The cache key to refresh.
            user_id (int): The ID of the user associated with the cache.
        """
        if not self.env:
            return
            
        try:
            if THREAD_REGISTRY.is_active(key):
                return
                
            cache = self.env["res.dashboard.cache"].search(
                [
                    ("name", "=", key),
                    ("user_id", "=", user_id),
                    ("refresh_in_progress", "=", False),
                ],
                limit=1,
            )
            
            if not cache:
                return
                
            cache.refresh_in_progress = True
            thread_id = f"{key}_{user_id}_{time.time()}"
            refresh_thread = threading.Thread(
                target=self._refresh_cache_background_thread,
                args=(self.env.cr.dbname, self.env.uid, key, user_id, thread_id),
                daemon=True,
            )
            
            THREAD_REGISTRY.register(key, refresh_thread)
            refresh_thread.start()
        except Exception as e:
            _logger.error(f"Error triggering background refresh: {str(e)}")
            try:
                cache = self.env["res.dashboard.cache"].search(
                    [("name", "=", key), ("user_id", "=", user_id)], limit=1
                )
                if cache:
                    cache.refresh_in_progress = False
            except:
                pass

    @staticmethod
    def _refresh_cache_background_thread(dbname, uid, key, user_id, thread_id):
        """Background thread to refresh cache data.

        Args:
            dbname (str): The name of the database.
            uid (int): The user ID for the operation.
            key (str): The cache key to refresh.
            user_id (int): The ID of the user associated with the cache.
            thread_id (str): The unique ID for the thread.
        """
        try:
            time.sleep(0.25)
            _logger.debug(f"Starting background refresh for key: {key}")
            
            from odoo import registry
            with registry(dbname).cursor() as cr:
                env = api.Environment(cr, uid, {})
                cache = env["res.dashboard.cache"].search(
                    [
                        ("name", "=", key),
                        ("user_id", "=", user_id),
                        ("refresh_in_progress", "=", True),
                    ],
                    limit=1,
                )
                
                if not cache:
                    _logger.warning(f"Cache key {key} no longer valid for refresh")
                    return
                    
                try:
                    result = None
                    cache_service = CacheService(env)
                    
                    if "charts_data_" in key:
                        result = cache_service._refresh_charts_data(key, user_id)
                    elif "all_stats_" in key:
                        result = cache_service._refresh_stats_data(key, user_id)
                        
                    if result:
                        cache_service.set_cache(key, result, user_id)
                        _logger.info(f"Successfully refreshed cache for {key}")
                    else:
                        _logger.warning(f"No result data for refreshing {key}")
                except Exception as e:
                    _logger.error(f"Error in refresh operation: {str(e)}")
                finally:
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

    def _refresh_charts_data(self, key, user_id):
        """Refresh charts data based on parameters encoded in the key.

        Args:
            key (str): The cache key containing parameters for refresh.
            user_id (int): The ID of the user requesting the refresh.

        Returns:
            any: The refreshed chart data or None if not applicable.
        """
        if not self.env:
            return None
            
        parts = key.split("_")
        if len(parts) >= 4:
            cco_part = parts[2]
            cco = cco_part.lower() == "true"
            branches_str = parts[3]
            try:
                branches_id = json.loads(branches_str)
            except:
                branches_id = []

            from ..utils.request_context import RequestContextManager
            with RequestContextManager(self.env) as request:
                request.uid = user_id

                from ..controllers.charts import DynamicChartController
                controller = DynamicChartController()
                return controller.get_chart_data(cco, branches_id)
        return None

    def _refresh_stats_data(self, key, user_id):
        """Refresh stats data based on parameters encoded in the key.

        Args:
            key (str): The cache key containing parameters for refresh.
            user_id (int): The ID of the user requesting the refresh.

        Returns:
            any: The refreshed stats data or None if not applicable.
        """
        if not self.env:
            return None
            
        parts = key.split("_")
        datepicked = 20000
        if len(parts) >= 4:
            cco_part = parts[2]
            cco = cco_part.lower() == "true"
            branches_str = parts[3]
            try:
                branches_id = json.loads(branches_str)
            except:
                branches_id = []

            from ..utils.request_context import RequestContextManager
            with RequestContextManager(self.env) as request:
                request.uid = user_id

                from ..controllers.controllers import Compliance
                stats_controller = Compliance()
                return stats_controller.getAllstats(cco, branches_id, datepicked)
        return None

    def clear_expired_cache(self):
        """Clear all expired cache entries and stale refresh flags.

        Returns:
            bool: True if the cleanup was successful.
        """
        if not self.env:
            return False
            
        expired = self.env["res.dashboard.cache"].search([("expiry_time", "<", fields.Datetime.now())])
        if expired:
            _logger.info(f"Clearing {len(expired)} expired cache entries")
            expired.unlink()
            
        stale_time = fields.Datetime.now() - timedelta(minutes=10)
        stale_refreshes = self.env["res.dashboard.cache"].search(
            [("refresh_in_progress", "=", True), ("last_updated", "<", stale_time)]
        )
        if stale_refreshes:
            _logger.info(f"Resetting {len(stale_refreshes)} stale refresh flags")
            stale_refreshes.write({"refresh_in_progress": False})
            
        THREAD_REGISTRY.cleanup_stale()
        return True