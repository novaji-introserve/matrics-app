from odoo import http
from odoo.http import request
import json

class CacheController(http.Controller):
    
    @http.route('/dashboard/cache/get', type='json', auth='user')
    def get_cache(self, key, **kw):
        """Get data from cache for the user's group (CCO/BCO)"""
        user = request.env.user
        
        # Find the primary group (CCO or BCO)
        primary_group_id = self._get_primary_group_id(user)
            
        if not primary_group_id:
            return {'success': False, 'message': 'No suitable group found'}
        
        cache_data = request.env['res.dashboard.cache'].get_cache(key, primary_group_id)
        if cache_data:
            return {'success': True, 'data': cache_data, 'from_cache': True}
                
        return {'success': False, 'message': 'No valid cache found'}
    
    @http.route('/dashboard/cache/set', type='json', auth='user')
    def set_cache(self, key, data, **kw):
        """Set data in cache for the user's primary group (CCO or BCO)"""
        user = request.env.user
        
        # Find the primary group (CCO or BCO)
        primary_group_id = self._get_primary_group_id(user)
            
        if not primary_group_id:
            return {'success': False, 'message': 'No suitable group found for cache storage'}
            
        request.env['res.dashboard.cache'].set_cache(key, data, primary_group_id)
        return {'success': True}

    def _get_primary_group_id(self, user):
        """Determine the primary group ID (CCO or BCO) for the user"""
        primary_group_id = None
        for group in user.groups_id:
            if any(role in group.name.lower() for role in ['cco', 'bco', 'compliance']):
                primary_group_id = group.id
                break
        
        # Fallback to first group if no specific group found
        if not primary_group_id and user.groups_id:
            primary_group_id = user.groups_id[0].id
            
        return primary_group_id