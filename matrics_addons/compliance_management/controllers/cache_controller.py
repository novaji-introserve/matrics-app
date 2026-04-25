# -*- coding: utf-8 -*-
"""
Cache Controller
================
This controller handles cache operations for the dashboard, allowing users to 
get, set, invalidate, and check the status of cached entries. It provides 
endpoints to manage user-specific cache data efficiently.
"""

from odoo import http
from odoo.http import request
import logging
from datetime import datetime
from ..services.security_service import SecurityService
from ..decorators.security_decorators import log_access

_logger = logging.getLogger(__name__)

class CacheController(http.Controller):
    """Controller for handling cache operations"""

    @http.route("/dashboard/cache/get", type="json", auth="user")
    @log_access
    def get_cache(self, key, **kw):
        """Get data from cache for the current user.

        Args:
            key (str): The cache key to retrieve data for.

        Returns:
            dict: A response indicating success or failure, along with the cached data if available.
        """
        try:
            # Validate and sanitize input parameters
            security_service = SecurityService()
            
            # Validate key parameter
            if not key or not isinstance(key, str):
                security_service.log_security_event(
                    "CACHE_INVALID_KEY",
                    f"Invalid cache key parameter: {key}"
                )
                return {"success": False, "message": "Request validation failed"}
            
            # Sanitize key parameter
            key = security_service.sanitize_sql_parameter(key)
            
            # Log access attempt
            user_id = request.env.user.id
            _logger.debug(f"Cache GET for key: {key}")

            cache_data = request.env["res.dashboard.cache"].get_cache(key, user_id)

            if cache_data:
                return {"success": True, "data": cache_data, "from_cache": True}

            return {"success": False, "message": "No valid cache found"}

        except Exception as e:
            _logger.error(f"Error in cache get: {str(e)}")
            return {"success": False, "message": "Request validation failed"}

    @http.route("/dashboard/cache/set", type="json", auth="user")
    @log_access
    def set_cache(self, key, data, **kw):
        """Set data in cache for the current user with explicit TTL.

        Args:
            key (str): The cache key to store data under.
            data (any): The data to be cached.
            kw (dict): Additional parameters, including TTL.

        Returns:
            dict: A response indicating success or failure of the cache set operation.
        """
        try:
            # Validate and sanitize input parameters
            security_service = SecurityService()
            
            # Validate key parameter
            if not key or not isinstance(key, str):
                security_service.log_security_event(
                    "CACHE_INVALID_KEY",
                    f"Invalid cache key parameter: {key}"
                )
                return {"success": False, "message": "Request validation failed"}
            
            # Sanitize parameters
            key = security_service.sanitize_sql_parameter(key)
            data = security_service.sanitize_sql_parameter(data)
            
            user_id = request.env.user.id
            ttl = kw.get("ttl", 2400)
            
            # Validate TTL parameter
            if not isinstance(ttl, (int, float)) or ttl <= 0:
                security_service.log_security_event(
                    "CACHE_INVALID_TTL",
                    f"Invalid TTL parameter: {ttl}"
                )
                return {"success": False, "message": "Request validation failed"}

            _logger.debug(f"Setting cache for key: {key}")

            success = request.env["res.dashboard.cache"].set_cache(
                key, data, user_id, ttl=ttl
            )

            if success:
                return {"success": True}
            else:
                return {"success": False, "message": "Failed to set cache"}

        except Exception as e:
            _logger.error(f"Error in cache set: {str(e)}")
            return {"success": False, "message": "Request validation failed"}

    @http.route("/dashboard/cache/invalidate", type="json", auth="user")
    @log_access
    def invalidate_cache(self, key=None, **kw):
        """Invalidate cache entries for the current user.

        Args:
            key (str, optional): The specific cache key to invalidate. If not provided, all cache entries are invalidated.

        Returns:
            dict: A response indicating success or failure, along with a message.
        """
        try:
            # Validate and sanitize input parameters
            security_service = SecurityService()
            
            # Validate key parameter if provided
            if key is not None:
                if not isinstance(key, str):
                    security_service.log_security_event(
                        "CACHE_INVALID_KEY",
                        f"Invalid cache key parameter: {key}"
                    )
                    return {"success": False, "message": "Request validation failed"}
                # Sanitize key parameter
                key = security_service.sanitize_sql_parameter(key)
            
            user_id = request.env.user.id

            if key:
                _logger.debug(f"Invalidating cache for key: {key}")
                cache = request.env["res.dashboard.cache"].search(
                    [("name", "=", key), ("user_id", "=", user_id)]
                )
                if cache:
                    cache.unlink()
                    return {"success": True, "message": f"Cache key {key} invalidated"}
                return {"success": False, "message": "Cache key not found"}
            else:
                _logger.debug(f"Invalidating all cache for user: {user_id}")
                cache = request.env["res.dashboard.cache"].search(
                    [("user_id", "=", user_id)]
                )
                if cache:
                    count = len(cache)
                    cache.unlink()
                    return {
                        "success": True,
                        "message": f"{count} cache entries invalidated",
                    }
                return {
                    "success": False,
                    "message": "No cache entries found for this user",
                }

        except Exception as e:
            _logger.error(f"Error in cache invalidate: {str(e)}")
            return {"success": False, "message": "Request validation failed"}

    @http.route("/dashboard/cache/status", type="json", auth="user")
    @log_access
    def get_cache_status(self, key=None, **kw):
        """Get cache status information for debugging.

        Args:
            key (str, optional): The specific cache key to check. If not provided, status of all user cache entries is returned.

        Returns:
            dict: A response with cache status details, including total entries, active and expired entries, and specific key information if provided.
        """
        try:
            # Validate and sanitize input parameters
            security_service = SecurityService()
            
            # Validate key parameter if provided
            if key is not None:
                if not isinstance(key, str):
                    security_service.log_security_event(
                        "CACHE_INVALID_KEY",
                        f"Invalid cache key parameter: {key}"
                    )
                    return {"success": False, "message": "Request validation failed"}
                # Sanitize key parameter
                key = security_service.sanitize_sql_parameter(key)
            
            user_id = request.env.user.id

            if key:
                cache = request.env["res.dashboard.cache"].search(
                    [("name", "=", key), ("user_id", "=", user_id)], limit=1
                )

                if cache:
                    now = datetime.now()
                    time_left = (cache.expiry_time - now).total_seconds()
                    refresh_point = (
                        (cache.refresh_after - cache.last_updated).total_seconds()
                        if cache.refresh_after and cache.last_updated
                        else 0
                    )
                    total_ttl = (
                        (cache.expiry_time - cache.last_updated).total_seconds()
                        if cache.expiry_time and cache.last_updated
                        else 0
                    )

                    return {
                        "success": True,
                        "key": key,
                        "exists": True,
                        "expired": (
                            cache.expiry_time < now if cache.expiry_time else True
                        ),
                        "time_left_seconds": max(0, time_left),
                        "total_ttl_seconds": total_ttl,
                        "refresh_point_seconds": refresh_point,
                        "refresh_threshold_percent": (
                            round((refresh_point / total_ttl) * 100)
                            if total_ttl > 0
                            else 0
                        ),
                        "refresh_needed": (
                            now > cache.refresh_after if cache.refresh_after else False
                        ),
                        "refresh_in_progress": cache.refresh_in_progress,
                        "last_updated": (
                            cache.last_updated.strftime("%Y-%m-%d %H:%M:%S")
                            if cache.last_updated
                            else None
                        ),
                        "expiry_time": (
                            cache.expiry_time.strftime("%Y-%m-%d %H:%M:%S")
                            if cache.expiry_time
                            else None
                        ),
                        "refresh_after": (
                            cache.refresh_after.strftime("%Y-%m-%d %H:%M:%S")
                            if cache.refresh_after
                            else None
                        ),
                        "data_size": cache.data_size,
                    }
                return {"success": True, "key": key, "exists": False}
            else:
                all_cache = request.env["res.dashboard.cache"].search(
                    [("user_id", "=", user_id)]
                )

                now = datetime.now()
                cache_stats = {
                    "success": True,
                    "total_entries": len(all_cache),
                    "active_entries": len(
                        all_cache.filtered(
                            lambda c: c.expiry_time > now if c.expiry_time else False
                        )
                    ),
                    "expired_entries": len(
                        all_cache.filtered(
                            lambda c: c.expiry_time <= now if c.expiry_time else True
                        )
                    ),
                    "refresh_needed": len(
                        all_cache.filtered(
                            lambda c: (
                                c.refresh_after < now if c.refresh_after else False
                            )
                        )
                    ),
                    "refresh_in_progress": len(
                        all_cache.filtered(lambda c: c.refresh_in_progress)
                    ),
                    "entries": [],
                }

                for cache in all_cache[:100]:
                    time_left = (
                        (cache.expiry_time - now).total_seconds()
                        if cache.expiry_time
                        else 0
                    )
                    cache_stats["entries"].append(
                        {
                            "key": cache.name,
                            "expired": (
                                cache.expiry_time <= now if cache.expiry_time else True
                            ),
                            "time_left_seconds": max(0, time_left),
                            "refresh_needed": (
                                now > cache.refresh_after
                                if cache.refresh_after
                                else False
                            ),
                            "refresh_in_progress": cache.refresh_in_progress,
                            "data_size": cache.data_size,
                        }
                    )

                return cache_stats

        except Exception as e:
            _logger.error(f"Error in cache status: {str(e)}")
            return {"success": False, "message": "Request validation failed"}
