from odoo import http
from odoo.http import request
import json

class CacheController(http.Controller):
    
    @http.route('/dashboard/cache/get', type='json', auth='user')
    def get_cache(self, key, **kw):
        """Get data from cache for the current user"""
        user_id = request.env.user.id
        
        cache_data = request.env['res.dashboard.cache'].get_cache(key, user_id)
        if cache_data:
            return {'success': True, 'data': cache_data, 'from_cache': True}
                
        return {'success': False, 'message': 'No valid cache found'}
    
    @http.route('/dashboard/cache/set', type='json', auth='user')
    def set_cache(self, key, data, **kw):
        """Set data in cache for the current user"""
        user_id = request.env.user.id
        
        request.env['res.dashboard.cache'].set_cache(key, data, user_id)
        return {'success': True}
    
    @http.route('/dashboard/cache/invalidate', type='json', auth='user')
    def invalidate_cache(self, key=None, **kw):
        """Invalidate cache entries for the current user"""
        user_id = request.env.user.id
        
        if key:
            # Invalidate specific key
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
            cache = request.env['res.dashboard.cache'].search([
                ('user_id', '=', user_id)
            ])
            if cache:
                cache.unlink()
                return {'success': True, 'message': f'{len(cache)} cache entries invalidated'}
            return {'success': False, 'message': 'No cache entries found for this user'}