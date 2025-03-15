# -*- coding: utf-8 -*-

from . import controllers
from . import models
from . import services


# Start WebSocket server when module is loaded
from .controllers.websocket import start_websocket_server, stop_websocket_server
import threading
import time
import logging

_logger = logging.getLogger(__name__)

def post_init_hook(cr, registry):
    """Start WebSocket server after module initialization"""
    # Start in a separate thread with a slight delay to avoid blocking module installation
    def delayed_start():
        time.sleep(5)  # Wait for Odoo to fully start
        _logger.info("Starting WebSocket server from post_init_hook")
        start_websocket_server()
    
    threading.Thread(target=delayed_start, daemon=True).start()

def uninstall_hook(cr, registry):
    """Stop WebSocket server when module is uninstalled"""
    _logger.info("Stopping WebSocket server from uninstall_hook")
    stop_websocket_server()