# -*- coding: utf-8 -*-

import hashlib
import json
import logging
from odoo.http import request
import time

_logger = logging.getLogger(__name__)

def get_unique_client_identifier():
    """
    Generate a unique client identifier that remains consistent for the same session.

    This function checks if the current request has an associated session. If so, it 
    generates a hash based on the session ID to create a unique identifier. If 
    there is no session or an error occurs, it returns a default identifier.

    Returns:
        str: A unique identifier for the client session, or 'default' if no session is available.
    """
    try:
        if hasattr(request, 'session') and request.session:
            session_id = request.session.sid
            return hashlib.md5(session_id.encode('utf-8')).hexdigest()[:8]

        return 'default'
    except Exception as e:
        _logger.error(f"Error generating unique client ID: {e}")
        return 'default'

def generate_cache_key(prefix, params):
    """
    Generate a consistent cache key based on a prefix and parameters.

    This function takes a prefix and a dictionary of parameters, orders the parameters 
    consistently, and creates a hash to ensure that the generated cache key is unique 
    for the given input. It also includes a unique client identifier for session-based 
    differentiation.

    Args:
        prefix (str): Key prefix to identify the cache category.
        params (dict): Parameters to include in the key.

    Returns:
        str: A consistent cache key that includes the prefix, hashed parameters, and the client ID.
    """
    try:
        ordered_params = json.dumps(params, sort_keys=True, separators=(',', ':'))
        
        params_hash = hashlib.md5(ordered_params.encode('utf-8')).hexdigest()[:12]
        
        client_id = get_unique_client_identifier()
        
        return f"{prefix}_{params_hash}_{client_id}"
    except Exception as e:
        _logger.error(f"Error generating cache key: {e}")
        return f"{prefix}_fallback_{int(time.time())}"

def normalize_cache_key_components(cco, branches_id, datepicked, unique_id):
    """
    Create normalized components for cache keys to ensure client/server consistency.

    This function normalizes the input components, formats the branch IDs, and 
    converts them to a consistent string representation. This ensures that cache 
    keys generated from these components will be uniform across different calls.

    Args:
        cco (str): Input string to be normalized.
        branches_id (list): List of branch IDs to be normalized.
        datepicked (str): Date selected, formatted as a string.
        unique_id (str): A unique identifier for the session.

    Returns:
        tuple: A tuple containing normalized components (cco_str, branches_str, datepicked_str, unique_id).
    """
    cco_str = str(cco).lower()
    
    if isinstance(branches_id, list) and branches_id:
        sorted_branches = sorted(int(b) for b in branches_id if str(b).isdigit())
        branches_str = str(sorted_branches).replace(',', ', ')
    else:
        branches_str = "[]"
    
    datepicked_str = str(datepicked)
    
    return cco_str, branches_str, datepicked_str, unique_id
