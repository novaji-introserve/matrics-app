from odoo import http
import requests
import logging

_logger = logging.getLogger(__name__)

def get_unique_client_identifier(request=None):
    """Create a unique client identifier using multiple factors"""
    if not request:
        request = http.request
    
    session_id = request.session.sid
    
    user_agent = request.httprequest.headers.get('User-Agent', '')
    
    from datetime import datetime
    day_part = datetime.now().strftime('%Y%m%d')
    
    import hashlib
    # Create a hash of these components
    unique_id = hashlib.md5(f"{session_id}:{user_agent}:{day_part}".encode()).hexdigest()
    
    return unique_id
