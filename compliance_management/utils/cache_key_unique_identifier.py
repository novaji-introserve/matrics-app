import hashlib
import json
import logging
from odoo.http import request

_logger = logging.getLogger(__name__)

def get_unique_client_identifier():
    """Generate a unique client identifier that's consistent for the same session"""
    try:
        # Try to get a unique but consistent identifier for this user session
        if hasattr(request, 'session') and request.session:
            # Use session ID for uniqueness
            session_id = request.session.sid
            return hashlib.md5(session_id.encode()).hexdigest()[:8]
        
        # Fallback
        return 'default'
    except Exception as e:
        _logger.error(f"Error generating unique client ID: {e}")
        return 'default'

def generate_cache_key(prefix, params):
    """
    Generate a consistent cache key based on a prefix and parameters
    
    Args:
        prefix (str): Key prefix to identify the cache category
        params (dict): Parameters to include in the key
        
    Returns:
        str: Consistent cache key
    """
    try:
        # Sort params to ensure consistent order
        ordered_params = json.dumps(params, sort_keys=True, separators=(',', ':'))
        
        # Create a hash of the params for a shorter key
        params_hash = hashlib.md5(ordered_params.encode()).hexdigest()[:12]
        
        # Get unique client ID
        client_id = get_unique_client_identifier()
        
        # Combine elements for the final key
        return f"{prefix}_{params_hash}_{client_id}"
    except Exception as e:
        _logger.error(f"Error generating cache key: {e}")
        # Fallback to a basic key with timestamp to prevent cache collision
        import time
        return f"{prefix}_fallback_{int(time.time())}"


# from odoo import http
# import logging
# import hashlib
# from datetime import datetime

# _logger = logging.getLogger(__name__)

# def get_unique_client_identifier(request=None):
#     """Create a unique client identifier using multiple factors"""
#     if not request:
#         request = http.request
    
#     session_id = request.session.sid
    
#     user_agent = request.httprequest.headers.get('User-Agent', '')
    
#     day_part = datetime.now().strftime('%Y%m%d')
    
#     unique_id = hashlib.md5(f"{session_id}:{user_agent}:{day_part}".encode()).hexdigest()
    
#     return unique_id
