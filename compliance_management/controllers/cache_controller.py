from odoo import http
from odoo.http import request
import json
import logging
from datetime import datetime

_logger = logging.getLogger(__name__)

class CacheController(http.Controller):
    """Controller for handling cache operations"""
    
    @http.route('/dashboard/cache/get', type='json', auth='user')
    def get_cache(self, key, **kw):
        """Get data from cache for the current user"""
        try:
            user_id = request.env.user.id
            _logger.debug(f"Cache GET for key: {key}")
            
            # Use ORM method instead of direct SQL
            cache_data = request.env['res.dashboard.cache'].get_cache(key, user_id)
            
            if cache_data:
                return {'success': True, 'data': cache_data, 'from_cache': True}
            
            return {'success': False, 'message': 'No valid cache found'}
        
        except Exception as e:
            _logger.error(f"Error in cache get: {str(e)}")
            return {'success': False, 'message': f'Error: {str(e)}'}
    
    @http.route('/dashboard/cache/set', type='json', auth='user')
    def set_cache(self, key, data, **kw):
        """Set data in cache for the current user with explicit TTL"""
        try:
            user_id = request.env.user.id
            ttl = kw.get('ttl', 600)  
            
            _logger.debug(f"Setting cache for key: {key}")
            
            success = request.env['res.dashboard.cache'].set_cache(key, data, user_id, ttl=ttl)
            
            if success:
                return {'success': True}
            else:
                return {'success': False, 'message': 'Failed to set cache'}
        
        except Exception as e:
            _logger.error(f"Error in cache set: {str(e)}")
            return {'success': False, 'message': f'Error: {str(e)}'}
    
    @http.route('/dashboard/cache/invalidate', type='json', auth='user')
    def invalidate_cache(self, key=None, **kw):
        """Invalidate cache entries for the current user"""
        try:
            user_id = request.env.user.id
            
            if key:
                # Invalidate specific key
                _logger.debug(f"Invalidating cache for key: {key}")
                cache = request.env['res.dashboard.cache'].search([
                    ('name', '=', key),
                    ('user_id', '=', user_id)
                ])
                if cache:
                    cache.unlink()
                    return {'success': True, 'message': f'Cache key {key} invalidated'}
                return {'success': False, 'message': 'Cache key not found'}
            else:
                # Invalidate all keys for this user
                _logger.debug(f"Invalidating all cache for user: {user_id}")
                cache = request.env['res.dashboard.cache'].search([
                    ('user_id', '=', user_id)
                ])
                if cache:
                    count = len(cache)
                    cache.unlink()
                    return {'success': True, 'message': f'{count} cache entries invalidated'}
                return {'success': False, 'message': 'No cache entries found for this user'}
        
        except Exception as e:
            _logger.error(f"Error in cache invalidate: {str(e)}")
            return {'success': False, 'message': f'Error: {str(e)}'}
    
    @http.route('/dashboard/cache/status', type='json', auth='user')
    def get_cache_status(self, key=None, **kw):
        """Get cache status information for debugging"""
        try:
            user_id = request.env.user.id
            
            if key:
                # Get status for specific key
                cache = request.env['res.dashboard.cache'].search([
                    ('name', '=', key),
                    ('user_id', '=', user_id)
                ], limit=1)
                
                if cache:
                    # Calculate timing information
                    now = datetime.now()
                    time_left = (cache.expiry_time - now).total_seconds()
                    refresh_point = (cache.refresh_after - cache.last_updated).total_seconds() if cache.refresh_after and cache.last_updated else 0
                    total_ttl = (cache.expiry_time - cache.last_updated).total_seconds() if cache.expiry_time and cache.last_updated else 0
                    
                    return {
                        'success': True,
                        'key': key,
                        'exists': True,
                        'expired': cache.expiry_time < now if cache.expiry_time else True,
                        'time_left_seconds': max(0, time_left),
                        'total_ttl_seconds': total_ttl,
                        'refresh_point_seconds': refresh_point,
                        'refresh_threshold_percent': round((refresh_point / total_ttl) * 100) if total_ttl > 0 else 0,
                        'refresh_needed': now > cache.refresh_after if cache.refresh_after else False,
                        'refresh_in_progress': cache.refresh_in_progress,
                        'last_updated': cache.last_updated.strftime('%Y-%m-%d %H:%M:%S') if cache.last_updated else None,
                        'expiry_time': cache.expiry_time.strftime('%Y-%m-%d %H:%M:%S') if cache.expiry_time else None,
                        'refresh_after': cache.refresh_after.strftime('%Y-%m-%d %H:%M:%S') if cache.refresh_after else None,
                        'data_size': cache.data_size
                    }
                return {'success': True, 'key': key, 'exists': False}
            else:
                # Get overall cache statistics
                all_cache = request.env['res.dashboard.cache'].search([
                    ('user_id', '=', user_id)
                ])
                
                now = datetime.now()
                cache_stats = {
                    'success': True,
                    'total_entries': len(all_cache),
                    'active_entries': len(all_cache.filtered(lambda c: c.expiry_time > now if c.expiry_time else False)),
                    'expired_entries': len(all_cache.filtered(lambda c: c.expiry_time <= now if c.expiry_time else True)),
                    'refresh_needed': len(all_cache.filtered(lambda c: c.refresh_after < now if c.refresh_after else False)),
                    'refresh_in_progress': len(all_cache.filtered(lambda c: c.refresh_in_progress)),
                    'entries': []
                }
                
                # Get basic info for each cache entry (limit to 100 for performance)
                for cache in all_cache[:100]:
                    time_left = (cache.expiry_time - now).total_seconds() if cache.expiry_time else 0
                    cache_stats['entries'].append({
                        'key': cache.name,
                        'expired': cache.expiry_time <= now if cache.expiry_time else True,
                        'time_left_seconds': max(0, time_left),
                        'refresh_needed': now > cache.refresh_after if cache.refresh_after else False,
                        'refresh_in_progress': cache.refresh_in_progress,
                        'data_size': cache.data_size
                    })
                
                return cache_stats
        
        except Exception as e:
            _logger.error(f"Error in cache status: {str(e)}")
            return {'success': False, 'message': f'Error: {str(e)}'}














# from odoo import http, fields
# from odoo.http import request
# import json
# import logging
# from datetime import datetime

# _logger = logging.getLogger(__name__)

# class CacheController(http.Controller):
#     @http.route('/dashboard/cache/get', type='json', auth='user')
#     def get_cache(self, key, **kw):
#         """Get data from cache for the current user with enhanced logging"""
#         try:
#             user_id = request.env.user.id
#             _logger.debug(f"Cache GET requested for key: {key}, user: {user_id}")
            
#             # Get from cache - this will trigger background refresh if needed
#             cache_data = request.env['res.dashboard.cache'].get_cache(key, user_id)
            
#             if cache_data:
#                 # Check if this cache is near expiry (for debugging)
#                 cache_record = request.env['res.dashboard.cache'].search([
#                     ('name', '=', key),
#                     ('user_id', '=', user_id)
#                 ], limit=1)
                
#                 if cache_record:
#                     # Log cache status for debugging
#                     time_left = (cache_record.expiry_time - fields.Datetime.now()).total_seconds()
#                     refresh_needed = cache_record.refresh_after < fields.Datetime.now()
#                     refresh_status = "NEEDS REFRESH" if refresh_needed else "still fresh"
                    
#                     _logger.debug(f"Cache HIT for {key}: {time_left:.1f}s TTL left, {refresh_status}")
                    
#                     # Check if we should have triggered a refresh
#                     if refresh_needed and not cache_record.refresh_in_progress:
#                         _logger.warning(f"Cache should have triggered refresh but didn't: {key}")
                
#                 return {'success': True, 'data': cache_data, 'from_cache': True}
            
#             _logger.debug(f"Cache MISS for {key}")
#             return {'success': False, 'message': 'No valid cache found'}
        
#         except Exception as e:
#             _logger.error(f"Error in cache get: {str(e)}")
#             return {'success': False, 'message': f'Error: {str(e)}'}
    
#     @http.route('/dashboard/cache/set', type='json', auth='user')
#     def set_cache(self, key, data, **kw):
#         """Set data in cache for the current user with explicit TTL"""
#         try:
#             user_id = request.env.user.id
#             # Use 600 seconds (10 minutes) TTL explicitly
#             ttl = kw.get('ttl', 600)  
            
#             _logger.debug(f"Setting cache for key: {key}, user: {user_id}, TTL: {ttl}s")
            
#             success = request.env['res.dashboard.cache'].set_cache(key, data, user_id, ttl=ttl)
            
#             if success:
#                 _logger.debug(f"Successfully set cache for {key}")
#                 return {'success': True}
#             else:
#                 _logger.warning(f"Failed to set cache for {key}")
#                 return {'success': False, 'message': 'Failed to set cache'}
        
#         except Exception as e:
#             _logger.error(f"Error in cache set: {str(e)}")
#             return {'success': False, 'message': f'Error: {str(e)}'}
    
#     @http.route('/dashboard/cache/invalidate', type='json', auth='user')
#     def invalidate_cache(self, key=None, **kw):
#         """Invalidate cache entries for the current user"""
#         try:
#             user_id = request.env.user.id
            
#             if key:
#                 # Invalidate specific key
#                 _logger.debug(f"Invalidating cache for key: {key}, user: {user_id}")
#                 cache = request.env['res.dashboard.cache'].search([
#                     ('name', '=', key),
#                     ('user_id', '=', user_id)
#                 ])
#                 if cache:
#                     cache.unlink()
#                     _logger.debug(f"Invalidated cache for key: {key}")
#                     return {'success': True, 'message': f'Cache key {key} invalidated'}
#                 return {'success': False, 'message': 'Cache key not found'}
#             else:
#                 # Invalidate all keys for this user
#                 _logger.debug(f"Invalidating all cache for user: {user_id}")
#                 cache = request.env['res.dashboard.cache'].search([
#                     ('user_id', '=', user_id)
#                 ])
#                 if cache:
#                     count = len(cache)
#                     cache.unlink()
#                     _logger.debug(f"Invalidated {count} cache entries for user: {user_id}")
#                     return {'success': True, 'message': f'{count} cache entries invalidated'}
#                 return {'success': False, 'message': 'No cache entries found for this user'}
        
#         except Exception as e:
#             _logger.error(f"Error in cache invalidate: {str(e)}")
#             return {'success': False, 'message': f'Error: {str(e)}'}
    
#     @http.route('/dashboard/cache/status', type='json', auth='user')
#     def get_cache_status(self, key=None, **kw):
#         """Get cache status information for debugging"""
#         try:
#             user_id = request.env.user.id
            
#             if key:
#                 # Get status for specific key
#                 cache = request.env['res.dashboard.cache'].search([
#                     ('name', '=', key),
#                     ('user_id', '=', user_id)
#                 ], limit=1)
                
#                 if cache:
#                     # Calculate timing information
#                     now = fields.Datetime.now()
#                     time_left = (cache.expiry_time - now).total_seconds()
#                     time_since_refresh = (now - cache.refresh_after).total_seconds() if now > cache.refresh_after else 0
#                     refresh_point = (cache.refresh_after - cache.last_updated).total_seconds()
#                     total_ttl = (cache.expiry_time - cache.last_updated).total_seconds()
                    
#                     return {
#                         'success': True,
#                         'key': key,
#                         'exists': True,
#                         'expired': cache.expiry_time < now,
#                         'time_left_seconds': time_left,
#                         'total_ttl_seconds': total_ttl,
#                         'refresh_point_seconds': refresh_point,
#                         'refresh_threshold_percent': round((refresh_point / total_ttl) * 100),
#                         'refresh_needed': now > cache.refresh_after,
#                         'refresh_in_progress': cache.refresh_in_progress,
#                         'time_since_refresh_point': time_since_refresh if time_since_refresh > 0 else 0,
#                         'last_updated': fields.Datetime.to_string(cache.last_updated),
#                         'expiry_time': fields.Datetime.to_string(cache.expiry_time),
#                         'refresh_after': fields.Datetime.to_string(cache.refresh_after)
#                     }
#                 return {'success': True, 'key': key, 'exists': False}
#             else:
#                 # Get overall cache statistics
#                 all_cache = request.env['res.dashboard.cache'].search([
#                     ('user_id', '=', user_id)
#                 ])
                
#                 now = fields.Datetime.now()
#                 cache_stats = {
#                     'success': True,
#                     'total_entries': len(all_cache),
#                     'active_entries': len(all_cache.filtered(lambda c: c.expiry_time > now)),
#                     'expired_entries': len(all_cache.filtered(lambda c: c.expiry_time <= now)),
#                     'refresh_needed': len(all_cache.filtered(lambda c: c.refresh_after < now and c.expiry_time > now)),
#                     'refresh_in_progress': len(all_cache.filtered(lambda c: c.refresh_in_progress)),
#                     'entries': []
#                 }
                
#                 # Get basic info for each cache entry
#                 for cache in all_cache:
#                     time_left = (cache.expiry_time - now).total_seconds() if cache.expiry_time > now else 0
#                     cache_stats['entries'].append({
#                         'key': cache.name,
#                         'expired': cache.expiry_time <= now,
#                         'time_left_seconds': time_left if time_left > 0 else 0,
#                         'refresh_needed': now > cache.refresh_after and cache.expiry_time > now,
#                         'refresh_in_progress': cache.refresh_in_progress
#                     })
                
#                 return cache_stats
        
#         except Exception as e:
#             _logger.error(f"Error in cache status: {str(e)}")
#             return {'success': False, 'message': f'Error: {str(e)}'}

















# from odoo import http
# from odoo.http import request
# import json

# class CacheController(http.Controller):
    
#     @http.route('/dashboard/cache/get', type='json', auth='user')
#     def get_cache(self, key, **kw):
#         """Get data from cache for the current user"""
#         user_id = request.env.user.id
        
#         cache_data = request.env['res.dashboard.cache'].get_cache(key, user_id)
#         if cache_data:
#             return {'success': True, 'data': cache_data, 'from_cache': True}
                
#         return {'success': False, 'message': 'No valid cache found'}
    
#     @http.route('/dashboard/cache/set', type='json', auth='user')
#     def set_cache(self, key, data, **kw):
#         """Set data in cache for the current user"""
#         user_id = request.env.user.id
        
#         request.env['res.dashboard.cache'].set_cache(key, data, user_id)
#         return {'success': True}
    
#     @http.route('/dashboard/cache/invalidate', type='json', auth='user')
#     def invalidate_cache(self, key=None, **kw):
#         """Invalidate cache entries for the current user"""
#         user_id = request.env.user.id
        
#         if key:
#             # Invalidate specific key
#             cache = request.env['res.dashboard.cache'].search([
#                 ('name', '=', key),
#                 ('user_id', '=', user_id)
#             ])
#             if cache:
#                 cache.unlink()
#                 return {'success': True, 'message': f'Cache key {key} invalidated'}
#             return {'success': False, 'message': 'Cache key not found'}
#         else:
#             # Invalidate all keys for this user
#             cache = request.env['res.dashboard.cache'].search([
#                 ('user_id', '=', user_id)
#             ])
#             if cache:
#                 cache.unlink()
#                 return {'success': True, 'message': f'{len(cache)} cache entries invalidated'}
#             return {'success': False, 'message': 'No cache entries found for this user'}