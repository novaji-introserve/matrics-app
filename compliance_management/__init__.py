# -*- coding: utf-8 -*-
"""
Compliance Management Module
===========================
Main module entry point that handles initialization of Odoo models,
controllers, and services.
"""

# from odoo import api, SUPERUSER_ID
# import logging

# from . import websocket_patch
from . import hooks
from . import models
from . import controllers
from . import services
from . import utils

# _logger = logging.getLogger(__name__)

# # Initialize WebSocket service when module is loaded
# from .services.websocket import manager

# # Export the post-init and uninstall hooks for module lifecycle management
# def post_init_hook(cr, registry):
#     """Post-initialization hook called when the module is installed"""
#     from .services.websocket.manager import start_websocket_server
#     _logger.info("Running post-init hook for compliance_management")
#     start_websocket_server()


#     env = api.Environment(cr, SUPERUSER_ID, {})
#     # Call menu hiding after module installation
#     try:
#         env['ir.ui.menu']._hide_unwanted_menus()
#         env['dashboard.chart.view.refresher'].setup_dashboard_tables()
#     except Exception as e:
#         _logger.error(f"Error in post_init_hook: {e}")

# def uninstall_hook(cr, registry):
#     """Uninstallation hook called when the module is uninstalled"""
#     from .services.websocket.manager import stop_websocket_server
#     _logger.info("Running uninstall hook for compliance_management")
#     stop_websocket_server()

