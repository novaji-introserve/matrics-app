# -*- coding: utf-8 -*-

from odoo import models, fields, api
from datetime import timedelta
import logging

from ..services.cache_service import CacheService, THREAD_REGISTRY

_logger = logging.getLogger(__name__)

class DashboardCache(models.Model):
    """Model for dashboard cache storage with service delegation"""
    
    _name = "res.dashboard.cache"
    _description = "Dashboard Cache Storage"
    
    name = fields.Char(string="Cache Key", required=True, index=True)
    user_id = fields.Many2one("res.users", string="User", required=True, index=True)
    cache_data = fields.Binary(string="Cache Data", attachment=True)
    last_updated = fields.Datetime(
        string="Last Updated", default=fields.Datetime.now, index=True
    )
    expiry_time = fields.Datetime(string="Expiry Time", index=True)
    refresh_after = fields.Datetime(
        string="Refresh After", compute="_compute_refresh_after", store=True, index=True
    )
    refresh_in_progress = fields.Boolean(
        string="Refresh In Progress", default=False, index=True
    )
    data_size = fields.Integer(string="Data Size (bytes)", default=0)
    _sql_constraints = [
        (
            "unique_cache_key_user",
            "UNIQUE(name, user_id)",
            "Cache key must be unique per user!",
        )
    ]

    @api.depends("last_updated", "expiry_time")
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
        """Store data in cache by delegating to CacheService.
        
        Args:
            key (str): The cache key under which to store the data.
            data (any): The data to be cached.
            user_id (int, optional): The ID of the user associated with the cache.
            ttl (int, optional): Time-to-live for the cache entry in seconds.
            
        Returns:
            bool: True if the cache was successfully set, False otherwise.
        """
        cache_service = CacheService(self.env)
        return cache_service.set_cache(key, data, user_id, ttl)

    @api.model
    def get_cache(self, key, user_id=None):
        """Retrieve data from cache by delegating to CacheService.
        
        Args:
            key (str): The cache key from which to retrieve data.
            user_id (int, optional): The ID of the user associated with the cache.
            
        Returns:
            any: The cached data or None if not found or expired.
        """
        cache_service = CacheService(self.env)
        return cache_service.get_cache(key, user_id)

    @api.model
    def _trigger_background_refresh(self, key, user_id):
        """Trigger background refresh by delegating to CacheService.
        
        Args:
            key (str): The cache key to refresh.
            user_id (int): The ID of the user associated with the cache.
        """
        cache_service = CacheService(self.env)
        try:
            if THREAD_REGISTRY.is_active(key):
                return
            cache = self.search(
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
            cache_service._trigger_background_refresh(key, user_id)
        except Exception as e:
            _logger.error(f"Error triggering background refresh: {str(e)}")
            try:
                cache = self.search(
                    [("name", "=", key), ("user_id", "=", user_id)], limit=1
                )
                if cache:
                    cache.refresh_in_progress = False
            except Exception as reset_error:
                _logger.error(f"Error resetting refresh flag: {reset_error}")

    @api.model
    def _refresh_charts_data(self, key, user_id):
        """Refresh charts data by delegating to CacheService.
        
        Args:
            key (str): The cache key containing parameters for refresh.
            user_id (int): The ID of the user requesting the refresh.
            
        Returns:
            any: The refreshed chart data or None if not applicable.
        """
        cache_service = CacheService(self.env)
        return cache_service._refresh_charts_data(key, user_id)

    @api.model
    def _refresh_stats_data(self, key, user_id):
        """Refresh stats data by delegating to CacheService.
        
        Args:
            key (str): The cache key containing parameters for refresh.
            user_id (int): The ID of the user requesting the refresh.
            
        Returns:
            any: The refreshed stats data or None if not applicable.
        """
        cache_service = CacheService(self.env)
        return cache_service._refresh_stats_data(key, user_id)

    @api.model
    def clear_expired_cache(self):
        """Clear all expired cache entries and stale refresh flags.
        
        Returns:
            bool: True if the cleanup was successful.
        """
        cache_service = CacheService(self.env)
        return cache_service.clear_expired_cache()
    