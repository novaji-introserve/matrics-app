from odoo import models, fields, api
import json
import base64
from datetime import datetime, timedelta
import logging
import random
import time

_logger = logging.getLogger(__name__)

class DashboardCache(models.Model):
    _name = 'res.dashboard.cache'
    _description = 'Dashboard Cache Storage'

    name = fields.Char(string='Cache Key', required=True, index=True)
    user_id = fields.Many2one('res.users', string='User', required=True, index=True)
    cache_data = fields.Binary(string='Cache Data', attachment=True)  # Uses filestore
    last_updated = fields.Datetime(string='Last Updated', default=fields.Datetime.now)
    expiry_time = fields.Datetime(string='Expiry Time')
    
    _sql_constraints = [
        ('unique_cache_key_user', 'UNIQUE(name, user_id)', 'Cache key must be unique per user!')
    ]
    
    @api.model
    def set_cache(self, key, data, user_id=None, ttl=300):
        """Store data in cache with expiry time (5 minutes default)"""
        if user_id is None:
            user_id = self.env.user.id
            
        json_data = json.dumps(data)
        binary_data = base64.b64encode(json_data.encode('utf-8'))
        
        expiry = datetime.now() + timedelta(seconds=ttl)
        
        # Try the operation with retries
        max_retries = 5
        for attempt in range(max_retries):
            try:
                # Create a new cursor for each attempt to avoid transaction abortion issues
                with self.env.registry.cursor() as new_cr:
                    # Create a new environment with the new cursor
                    new_env = self.env(cr=new_cr)
                    
                    # Get existing cache record if any
                    existing = new_env['res.dashboard.cache'].search([('name', '=', key), ('user_id', '=', user_id)])
                    
                    if existing:
                        # Update with a new cursor - write directly to avoid attachment issues
                        existing.with_context(no_document=True).write({
                            'cache_data': binary_data,
                            'last_updated': fields.Datetime.now(),
                            'expiry_time': expiry
                        })
                    else:
                        # Create with a new cursor
                        new_env['res.dashboard.cache'].create({
                            'name': key,
                            'user_id': user_id,
                            'cache_data': binary_data,
                            'last_updated': fields.Datetime.now(),
                            'expiry_time': expiry
                        })
                    
                    # Commit the transaction explicitly
                    new_cr.commit()
                
                # If we get here, it worked
                return True
                
            except Exception as e:
                if attempt < max_retries - 1:
                    # Random backoff before retry
                    delay = random.uniform(0.5, 2) * (attempt + 1)
                    _logger.warning(f"Cache set attempt {attempt+1} failed: {str(e)}. Retrying in {delay:.2f}s...")
                    time.sleep(delay)
                else:
                    _logger.error(f"All cache set attempts failed: {str(e)}")
                    # Continue without caching in this case
        
        return False
    
    @api.model
    def get_cache(self, key, user_id=None):
        """Retrieve data from cache if not expired"""
        if user_id is None:
            user_id = self.env.user.id
            
        cache = self.search([
            ('name', '=', key), 
            ('user_id', '=', user_id),
            ('expiry_time', '>', fields.Datetime.now())
        ], limit=1)
        
        if not cache:
            return None
            
        try:
            binary_data = cache.cache_data
            json_data = base64.b64decode(binary_data).decode('utf-8')
            return json.loads(json_data)
        except Exception as e:
            _logger.error(f"Error retrieving cache: {str(e)}")
            return None
    
    @api.model
    def clear_expired_cache(self):
        """Clear all expired cache entries - called by cron job"""
        expired = self.search([
            ('expiry_time', '<', fields.Datetime.now())
        ])
        if expired:
            _logger.info(f"Clearing {len(expired)} expired cache entries")
            expired.unlink()
            return True
        return False





# from odoo import models, fields, api
# import json
# import base64
# from datetime import datetime, timedelta
# import logging

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
        
#         # Find existing cache or create new
#         existing = self.search([('name', '=', key), ('user_id', '=', user_id)])
#         if existing:
#             existing.write({
#                 'cache_data': binary_data,
#                 'last_updated': fields.Datetime.now(),
#                 'expiry_time': expiry
#             })
#         else:
#             self.create({
#                 'name': key,
#                 'user_id': user_id,
#                 'cache_data': binary_data,
#                 'last_updated': fields.Datetime.now(),
#                 'expiry_time': expiry
#             })
#         return True
    
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
#             _logger.info(f"Clearing {len(expired)} expired cache entries")
#             expired.unlink()
#             return True
#         return False
