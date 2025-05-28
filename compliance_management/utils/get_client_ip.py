from odoo import http
import requests
import logging

_logger = logging.getLogger(__name__)

def get_external_ip():
    """Get the external IP address using a public service"""
    try:
        response = requests.get('https://api.ipify.org?format=json', timeout=3)
        if response.status_code == 200:
            return response.json().get('ip')
    except Exception as e:
        _logger.warning("Failed to get external IP: %s", e)
    return None

def get_client_ip(request=None):
    """Get the real client IP address"""
    if not request:
        request = http.request
    
    for header in ['X-Forwarded-For', 'X-Real-IP', 'CF-Connecting-IP']:
        if request.httprequest.headers.get(header):
            ip = request.httprequest.headers.get(header).split(',')[0].strip()
            if ip and ip != '127.0.0.1':
                return ip
    
    # If on localhost, get external IP
    if request.httprequest.remote_addr == '127.0.0.1':
        external_ip = get_external_ip()
        if external_ip:
            return external_ip
    
    return request.httprequest.remote_addr