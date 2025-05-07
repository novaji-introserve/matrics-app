from odoo import http
from odoo.http import request
import json

class CacheController(http.Controller):
    
    @http.route('/dashboard/cache/get', type='json', auth='user')
    def get_cache(self, key, **kw):
        """Get data from cache for the current user"""
        user = request.env.user
        
        cache_data = request.env['res.dashboard.cache'].get_cache(key, user.id)
        if cache_data:
            return {'success': True, 'data': cache_data, 'from_cache': True}
                
        return {'success': False, 'message': 'No valid cache found'}
    
    @http.route('/dashboard/cache/set', type='json', auth='user')
    def set_cache(self, key, data, **kw):
        """Set data in cache for the current user"""
        user = request.env.user
        
        request.env['res.dashboard.cache'].set_cache(key, data, user.id)
        return {'success': True}