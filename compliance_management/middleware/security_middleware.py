# -*- coding: utf-8 -*-

import logging
import json
from odoo.http import request
from odoo.exceptions import ValidationError
from ..services.security_service import SecurityService

_logger = logging.getLogger(__name__)

class SecurityMiddleware:
    """Middleware to handle security validation for incoming requests."""
    
    def __init__(self):
        self.security_service = SecurityService()
    
    def process_request(self, httprequest):
        """
        Process incoming HTTP requests for security validation.
        
        Args:
            httprequest: The HTTP request object
            
        Returns:
            None if request is safe, otherwise raises ValidationError
        """
        try:
            # Skip security checks for certain endpoints
            skip_paths = ['/web/static/', '/web/assets/', '/web/image/', '/favicon.ico']
            if any(httprequest.path.startswith(path) for path in skip_paths):
                return
            
            # Check for suspicious patterns in request data
            self._validate_request_headers(httprequest)
            
            # For JSON requests, validate the request body
            if httprequest.content_type and 'application/json' in httprequest.content_type:
                self._validate_json_request_body(httprequest)
                
        except Exception as e:
            self.security_service.log_security_event(
                "REQUEST_VALIDATION_ERROR",
                f"Request validation failed: {str(e)} - Path: {httprequest.path}"
            )
            # Don't block the request for validation errors, just log them
            pass
    
    def _validate_request_headers(self, httprequest):
        """
        Validate request headers for suspicious content.
        
        Args:
            httprequest: The HTTP request object
        """
        suspicious_headers = [
            'x-forwarded-for', 'x-real-ip', 'x-originating-ip'
        ]
        
        for header_name in suspicious_headers:
            header_value = httprequest.headers.get(header_name, '')
            if header_value:
                # Check for SQL injection patterns in headers
                is_safe, error_msg = self.security_service.validate_sql_query(header_value)
                if not is_safe:
                    self.security_service.log_security_event(
                        "HEADER_INJECTION_ATTEMPT",
                        f"Suspicious header {header_name}: {error_msg} - Value: {header_value[:100]}..."
                    )
    
    def _validate_json_request_body(self, httprequest):
        """
        Validate JSON request body for dangerous content.
        
        Args:
            httprequest: The HTTP request object
        """
        try:
            # Get the request body
            if hasattr(httprequest, 'get_data'):
                body = httprequest.get_data(as_text=True)
            elif hasattr(httprequest, 'data'):
                body = httprequest.data.decode('utf-8') if isinstance(httprequest.data, bytes) else httprequest.data
            else:
                return
            
            if not body:
                return
            
            # Parse JSON
            try:
                json_data = json.loads(body)
            except (json.JSONDecodeError, ValueError):
                return  
            
            # Recursively check for dangerous content
            self._check_json_content(json_data, httprequest.path)
            
        except Exception as e:
            _logger.warning(f"Error validating JSON request body: {str(e)}")
    
    def _check_json_content(self, data, request_path):
        """
        Recursively check JSON content for dangerous patterns.
        
        Args:
            data: The JSON data to check
            request_path: The request path for logging
        """
        if isinstance(data, dict):
            for key, value in data.items():
                # Check for SQL queries in the data
                if key == 'sql_query' or 'query' in key.lower():
                    if isinstance(value, str):
                        is_safe, error_msg = self.security_service.validate_sql_query(value)
                        if not is_safe:
                            self.security_service.log_security_event(
                                "JSON_SQL_INJECTION_ATTEMPT",
                                f"Dangerous SQL in JSON field '{key}': {error_msg} - Path: {request_path}"
                            )
                
                # Recursively check nested structures
                self._check_json_content(value, request_path)
                
        elif isinstance(data, list):
            for item in data:
                self._check_json_content(item, request_path)
                
        elif isinstance(data, str):
            # Check string values for basic injection patterns
            dangerous_patterns = [
                r'<script', r'javascript:', r'vbscript:', r'onload=', r'onerror=',
                r'union\s+select', r'drop\s+table', r'delete\s+from'
            ]
            
            for pattern in dangerous_patterns:
                if pattern in data.lower():
                    self.security_service.log_security_event(
                        "DANGEROUS_CONTENT_DETECTED",
                        f"Suspicious content pattern '{pattern}' in request - Path: {request_path}"
                    )
    
    @staticmethod
    def add_security_headers(response):
        """
        Add security headers to HTTP responses.
        
        Args:
            response: The HTTP response object
            
        Returns:
            The modified response with security headers
        """
        if response:
            # Add security headers
            response.headers.update({
                'X-Content-Type-Options': 'nosniff',
                'X-Frame-Options': 'DENY',
                'X-XSS-Protection': '1; mode=block',
                'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
                'Content-Security-Policy': "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval'; style-src 'self' 'unsafe-inline';",
                'Referrer-Policy': 'strict-origin-when-cross-origin'
            })
        
        return response

# Global instance for use in controllers
security_middleware = SecurityMiddleware()
