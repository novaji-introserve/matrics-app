import threading
from werkzeug.local import Local
from odoo.http import Request
from odoo import api

class RequestContextManager:
    """Context manager to create a fake request for background operations"""
    
    def __init__(self, env):
        self.env = env
        self.request = None
        self.context = None
        self.local = None
    
    def __enter__(self):
        self.local = threading.current_thread().__dict__.get('werkzeug.local', None)
        
        local = Local()
        threading.current_thread().__dict__['werkzeug.local'] = local
        
        # Create a fake request object
        # Note: We're only setting the minimal attributes that controllers typically use
        request = Request.blank('/')
        request.env = self.env
        request.httprequest = type('FakeRequest', (), {
            'cookies': {},
            'remote_addr': '127.0.0.1',
            'host': 'localhost',
            'path': '/',
            'base_url': 'http://localhost',
        })()
        
        local.request = request
        self.request = request
        
        return request
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.local is None:
            threading.current_thread().__dict__.pop('werkzeug.local', None)
        else:
            threading.current_thread().__dict__['werkzeug.local'] = self.local