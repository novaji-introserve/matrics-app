# -*- coding: utf-8 -*-
"""
Compliance Management Module Hooks
===========================
Main module entry that handles initialization of hooks
"""

from odoo import api, SUPERUSER_ID
import logging

_logger = logging.getLogger(__name__)

def post_init_hook(cr, registry):
    """Post-initialization hook called when the module is installed"""
    _logger.info("Starting post-init hook for compliance_management")
    env = api.Environment(cr, SUPERUSER_ID, {})

    # Start WebSocket server
    try:
        from .services.websocket.manager import start_websocket_server
        start_websocket_server()
        _logger.info("WebSocket server started successfully")
    except Exception as e:
        _logger.error(f"Error starting WebSocket server: {e}")

    # Hide unwanted menus
    try:
        env['ir.ui.menu']._hide_unwanted_menus()
        _logger.info("Menus hidden successfully")
    except Exception as e:
        _logger.error(f"Error hiding menus: {e}")

    # Set up dashboard tables
    try:
        result = env['dashboard.chart.view.refresher'].setup_dashboard_tables()
        if result:
            cr.commit()
            _logger.info("Dashboard tables created successfully")
        else:
            _logger.error("Failed to create dashboard tables")
    except Exception as e:
        _logger.error(f"Error setting up dashboard tables: {e}")
    
    # Create performance indexes
    try:
        result = env['dashboard.chart.view.refresher'].create_performance_indexes()
        if result:
            cr.commit()
            _logger.info("Performance indexes created successfully")
        else:
            _logger.error("Failed to create performance indexes")
    except Exception as e:
        _logger.error(f"Error creating performance indexes: {e}")

    # Initialize database settings (persistent settings only)
    try:
        result = env['dashboard.chart.view.refresher'].initialize_database_settings_persistent()
        if result:
            cr.commit()
            _logger.info("Persistent database settings initialized successfully")
        else:
            _logger.error("Failed to initialize persistent database settings")
    except Exception as e:
        _logger.error(f"Error initializing persistent database settings: {e}")

    _logger.info("Completed post-init hook for compliance_management")

def uninstall_hook(cr, registry):
    """Uninstallation hook called when the module is uninstalled"""
    try:
        from .services.websocket.manager import stop_websocket_server
        stop_websocket_server()
        _logger.info("WebSocket server stopped successfully")
    except Exception as e:
        _logger.error(f"Error stopping WebSocket server: {e}")