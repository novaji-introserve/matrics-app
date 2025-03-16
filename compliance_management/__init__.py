# -*- coding: utf-8 -*-

from . import controllers
from . import models
from . import services


# Import WebSocket initialization functions
from odoo import http
from odoo import api, SUPERUSER_ID
import threading
import logging
import time
from .controllers.websocket import start_websocket_server

_logger = logging.getLogger(__name__)

# def start_websocket_server_with_delay():
#     """Start the WebSocket server with a delay to ensure all modules are loaded"""    
#     # Wait for Odoo to fully initialize
#     time.sleep(10)
#     _logger.info("Starting WebSocket server from module initialization")
#     start_websocket_server()

# # Start WebSocket in a separate thread to avoid blocking
# threading.Thread(target=start_websocket_server_with_delay, daemon=True).start()
def start_websocket_server_with_delay():
    """Start the WebSocket server with a delay to ensure all modules are loaded"""
    import time
    from .controllers.websocket import start_websocket_server
    
    # Wait for Odoo to fully initialize
    time.sleep(5)
    _logger.info("Starting WebSocket server from module initialization")
    start_websocket_server()

# Start the function directly in the main thread after a delay
threading.Timer(10.0, start_websocket_server_with_delay).start()

def post_init_hook(cr, registry):
    """Post-initialization hook registered in __manifest__.py"""
    _logger.info("Running post_init_hook for WebSocket server")
    env = api.Environment(cr, SUPERUSER_ID, {})
    # Set configuration parameters if needed
    env['ir.config_parameter'].set_param('enable_websockets', True)

def uninstall_hook(cr, registry):
    """Uninstallation hook registered in __manifest__.py"""
    from .controllers.websocket import stop_websocket_server
    _logger.info("Stopping WebSocket server from uninstall_hook")
    stop_websocket_server()

# Start WebSocket server when module is loaded
# from .controllers.websocket import start_websocket_server, stop_websocket_server
# import threading
# import time
# import logging

# _logger = logging.getLogger(__name__)

# def post_init_hook(cr, registry):
#     """Start WebSocket server after module initialization"""
#     # Start in a separate thread with a slight delay to avoid blocking module installation
#     def delayed_start():
#         time.sleep(5)  # Wait for Odoo to fully start
#         _logger.info("Starting WebSocket server from post_init_hook")
#         start_websocket_server()
    
#     threading.Thread(target=delayed_start, daemon=True).start()

# def uninstall_hook(cr, registry):
#     """Stop WebSocket server when module is uninstalled"""
#     _logger.info("Stopping WebSocket server from uninstall_hook")
#     stop_websocket_server()