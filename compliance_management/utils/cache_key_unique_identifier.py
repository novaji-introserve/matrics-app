from odoo import http
import logging
import hashlib
from datetime import datetime

_logger = logging.getLogger(__name__)

def get_unique_client_identifier(request=None):
    """Create a unique client identifier using multiple factors"""
    if not request:
        request = http.request
    
    session_id = request.session.sid
    
    user_agent = request.httprequest.headers.get('User-Agent', '')
    
    day_part = datetime.now().strftime('%Y%m%d')
    
    unique_id = hashlib.md5(f"{session_id}:{user_agent}:{day_part}".encode()).hexdigest()
    
    return unique_id
