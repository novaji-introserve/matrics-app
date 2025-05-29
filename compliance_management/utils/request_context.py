# -*- coding: utf-8 -*-

import threading
from werkzeug.local import Local
from werkzeug.test import EnvironBuilder
from werkzeug.wrappers import Request as WerkzeugRequest
from odoo.http import Request
from odoo import api
import logging

_logger = logging.getLogger(__name__)

class RequestContextManager:
    """Context manager to create a fake request for background operations"""
    
    def __init__(self, env):
        self.env = env
        self.request = None
        self.context = None
        self.local = None
    
    def __enter__(self):
        try:
            self.local = threading.current_thread().__dict__.get('werkzeug.local', None)
            
            local = Local()
            threading.current_thread().__dict__['werkzeug.local'] = local
            
            builder = EnvironBuilder(
                path='/',
                method='GET',
                base_url='http://localhost',
                headers={'Host': 'localhost'}
            )
            environ = builder.get_environ()
            
            werkzeug_request = WerkzeugRequest(environ)
            
            request = Request(werkzeug_request)
            request.update_env(self.env)
            
            request.httprequest = type('FakeRequest', (), {
                'cookies': {},
                'remote_addr': '127.0.0.1',
                'host': 'localhost',
                'path': '/',
                'base_url': 'http://localhost',
                'method': 'GET',
                'args': {},
                'form': {},
                'files': {},
                'headers': {},
                'url': 'http://localhost/',
                'environ': environ,
            })()
            
            local.request = request
            self.request = request
            
            return request
            
        except Exception as e:
            _logger.error(f"Error creating request context: {e}")
            # Fallback: create a minimal request-like object
            class MinimalRequest:
                def __init__(self, env):
                    self.env = env
                    self.httprequest = type('FakeRequest', (), {
                        'cookies': {},
                        'remote_addr': '127.0.0.1',
                        'host': 'localhost',
                        'path': '/',
                        'base_url': 'http://localhost',
                    })()
            
            minimal_request = MinimalRequest(self.env)
            if 'local' in locals():
                local.request = minimal_request
            return minimal_request
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.local is None:
                threading.current_thread().__dict__.pop('werkzeug.local', None)
            else:
                threading.current_thread().__dict__['werkzeug.local'] = self.local
        except Exception as e:
            _logger.error(f"Error cleaning up request context: {e}")




# This code is a context manager to create a fake request for background operations in Odoo.
# DO NOT DELETE 

# import threading
# from werkzeug.local import Local
# from odoo.http import Request
# from odoo import api

# class RequestContextManager:
#     """Context manager to create a fake request for background operations"""
    
#     def __init__(self, env):
#         self.env = env
#         self.request = None
#         self.context = None
#         self.local = None
    
#     def __enter__(self):
#         self.local = threading.current_thread().__dict__.get('werkzeug.local', None)
        
#         local = Local()
#         threading.current_thread().__dict__['werkzeug.local'] = local
        
#         # Create a fake request object
#         # Note: We're only setting the minimal attributes that controllers typically use
#         request = Request.blank('/')
#         request.env = self.env
#         request.httprequest = type('FakeRequest', (), {
#             'cookies': {},
#             'remote_addr': '127.0.0.1',
#             'host': 'localhost',
#             'path': '/',
#             'base_url': 'http://localhost',
#         })()
        
#         local.request = request
#         self.request = request
        
#         return request
    
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         if self.local is None:
#             threading.current_thread().__dict__.pop('werkzeug.local', None)
#         else:
#             threading.current_thread().__dict__['werkzeug.local'] = self.local