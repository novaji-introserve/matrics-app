# -*- coding: utf-8 -*-

import functools
import logging
from odoo.http import request
from odoo.exceptions import ValidationError, AccessDenied
from ..services.security_service import SecurityService

_logger = logging.getLogger(__name__)

def validate_sql_input(func):
    """
    Decorator to validate SQL inputs in controller methods.
    
    This decorator automatically validates any parameter that contains 'query' in its name
    and logs security events for suspicious activity.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        security_service = SecurityService()
        
        try:
            # Check all keyword arguments for SQL queries
            for param_name, param_value in kwargs.items():
                if 'query' in param_name.lower() and isinstance(param_value, str):
                    is_safe, error_msg = security_service.validate_sql_query(param_value)
                    if not is_safe:
                        security_service.log_security_event(
                            "DECORATED_ENDPOINT_SQL_INJECTION",
                            f"Blocked SQL injection in {func.__name__}.{param_name}: {error_msg}"
                        )
                        return {"error": "Request validation failed"}
            
            # Check positional arguments if they might be SQL queries
            for i, arg in enumerate(args):
                if isinstance(arg, str) and ('select' in arg.lower() or 'from' in arg.lower()):
                    is_safe, error_msg = security_service.validate_sql_query(arg)
                    if not is_safe:
                        security_service.log_security_event(
                            "DECORATED_ENDPOINT_SQL_INJECTION",
                            f"Blocked SQL injection in {func.__name__} arg[{i}]: {error_msg}"
                        )
                        return {"error": "Request validation failed"}
            
            # Call the original function
            return func(self, *args, **kwargs)
            
        except Exception as e:
            _logger.error(f"Security validation error in {func.__name__}: {str(e)}")
            return {"error": "Security validation failed"}
    
    return wrapper

def rate_limit(max_calls=100, time_window=3600):
    """
    Decorator to implement rate limiting on controller endpoints.
    
    Args:
        max_calls (int): Maximum number of calls allowed
        time_window (int): Time window in seconds
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if not request or not request.env:
                return func(self, *args, **kwargs)
            
            user_id = request.env.user.id
            endpoint_name = f"{func.__module__}.{func.__name__}"
            
            # For now, I will just log the rate limiting attempt
            # TODO: implement actual rate limiting with Redis or database storage
            _logger.info(f"Rate limit check for user {user_id} on {endpoint_name}")
            
            return func(self, *args, **kwargs)
        
        return wrapper
    return decorator

def require_admin(func):
    """
    Decorator to require admin privileges for accessing an endpoint.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not request or not request.env:
            raise AccessDenied("Authentication required")
        
        user = request.env.user
        if not user.has_group("base.group_system"):
            SecurityService.log_security_event(
                "UNAUTHORIZED_ADMIN_ACCESS_ATTEMPT",
                f"User {user.id} attempted to access admin endpoint {func.__name__}"
            )
            raise AccessDenied("Administrator privileges required")
        
        return func(self, *args, **kwargs)
    
    return wrapper

def log_access(func):
    """
    Decorator to log access to sensitive endpoints.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if request and request.env:
            user_id = request.env.user.id
            endpoint_name = f"{func.__module__}.{func.__name__}"
            
            _logger.info(f"ACCESS LOG - User {user_id} accessing {endpoint_name}")
        
        return func(self, *args, **kwargs)
    
    return wrapper