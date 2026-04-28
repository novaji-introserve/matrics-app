# -*- coding: utf-8 -*-
"""
WebSocket Connection Utilities
============================
Utilities for integrating WebSocket connections with Odoo.
This module provides functions for sending messages to WebSocket clients
from Odoo code.
"""

import json
import logging
import requests
from datetime import datetime
import pytz
from odoo import api, models
from odoo.tools import config

_logger = logging.getLogger(__name__)

def send_message(env, message, message_type="info", user_id=None, group=None, channel=None):
    """
    Send a message to WebSocket clients and/or bus channels.
    
    Args:
        env: Odoo environment
        message (str): The message to send
        message_type (str): Type of message (info, warning, error, success)
        user_id (int): Target user ID (optional)
        group (str): The group to send to (optional)
        channel (str): Specific channel to use (optional)
        
    Returns:
        bool: True if the message was sent successfully
    """
    try:
        # Create message data with full date-time format to match JavaScript
        # timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        nigeria_tz = pytz.timezone('Africa/Lagos')
        local_time = datetime.now(nigeria_tz)
        timestamp = local_time.strftime("%Y-%m-%d %H:%M:%S")
        # timestamp = datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S")
        message_data = {
            "type": "log_message",
            "message": message,
            "message_type": message_type,
            "timestamp": timestamp
        }
        
        # Log message locally
        log_level = {
            "info": _logger.info,
            "error": _logger.error,
            "success": _logger.info,
            "warning": _logger.warning
        }.get(message_type, _logger.info)
        
        log_level(f"[{timestamp}] [{message_type.upper()}] {message}")
        
        # Try WebSocket server first if user_id is provided
        websocket_sent = False
        if user_id:
            websocket_sent = _broadcast_to_user(message_data, user_id)
        
        # Also send through the bus for redundancy
        bus_sent = _send_via_bus(env, message_data, user_id, group, channel)
        
        return websocket_sent or bus_sent
        
    except Exception as e:
        error_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _logger.error(f"[{error_timestamp}] Error sending WebSocket message: {e}")
        return False

def _broadcast_to_user(message_data, user_id):
    """
    Send a message to all connections for a specific user.
    
    Args:
        message_data (dict): Message data to send (includes timestamp)
        user_id (int): Target user ID
        
    Returns:
        bool: True if the message was sent successfully
    """
    # In this implementation, we rely on the bus service as the primary method
    # If you implement direct WebSocket connection, ensure message_data is passed unchanged
    return False

def _send_via_bus(env, message_data, user_id=None, group=None, channel=None):
    """
    Send a message via Odoo's bus system.
    
    Args:
        env: Odoo environment
        message_data (dict): The message data to send (includes timestamp)
        user_id (int): Target user ID (optional)
        group (str): The group to send to (optional)
        channel (str): Specific channel to use (optional)
        
    Returns:
        bool: True if the message was sent successfully
    """
    try:
        # Determine channel
        if channel:
            bus_channel = channel
        elif user_id:
            bus_channel = f"csv_import_logs_{user_id}"
        elif group:
            bus_channel = f"csv_import_logs_{group}"
        else:
            bus_channel = "csv_import_logs"
        
        # Send through the bus - ensure the entire message_data (with timestamp) is sent
        env["bus.bus"]._sendone(bus_channel, "log_message", message_data)
        _logger.info(f"[{message_data['timestamp']}] Message sent via bus channel: {bus_channel}")
        return True
        
    except Exception as e:
        error_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _logger.error(f"[{error_timestamp}] Error sending via bus: {e}")
        return False
        