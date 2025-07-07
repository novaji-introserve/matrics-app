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
    # installed alert management
    install_alert_management(cr, registry)

def uninstall_hook(cr, registry):
    """Uninstallation hook called when the module is uninstalled"""
    try:
        from .services.websocket.manager import stop_websocket_server
        stop_websocket_server()
        _logger.info("WebSocket server stopped successfully")
    except Exception as e:
        _logger.error(f"Error stopping WebSocket server: {e}")
        
     # Uninstall alert_management module
    try:
        env = api.Environment(cr, SUPERUSER_ID, {})
        
        # Find installed alert_management module
        alert_module = env['ir.module.module'].search([
            ('name', '=', 'alert_management'),
            ('state', '=', 'installed')
        ])
        
        if alert_module:
            _logger.info("Found installed alert_management module, uninstalling...")
            alert_module.button_uninstall()
            cr.commit()
            _logger.info("✅ Alert Management auto-uninstalled successfully!")
        else:
            # Check current state
            alert_any_state = env['ir.module.module'].search([
                ('name', '=', 'alert_management')
            ])
            if alert_any_state:
                _logger.info(f"ℹ️ Alert Management found with state: {alert_any_state.state}")
            else:
                _logger.info("ℹ️ Alert Management module not found")
                
    except Exception as e:
        _logger.error(f"Error auto-uninstalling alert_management: {e}")
        import traceback
        _logger.error(traceback.format_exc())
   


def install_alert_management(cr, registry):
    """
    Auto-install alert_management when compliance_management is installed
    """
    _logger.info("=== Starting alert_management auto-installation ===")
    
    try:
        # Use existing environment instead of creating new one
        env = api.Environment(cr, SUPERUSER_ID, {})
        
        # First, let's see all modules with 'alert' in the name
        all_alert_modules = env['ir.module.module'].search([
            ('name', 'ilike', 'alert')
        ])
        _logger.info(f"Found {len(all_alert_modules)} modules with 'alert' in name:")
        for module in all_alert_modules:
            _logger.info(f"  - {module.name}: {module.state}")
        
        # Check if alert_management exists and is not installed
        alert_module = env['ir.module.module'].search([
            ('name', '=', 'alert_management'),
            ('state', '=', 'uninstalled')
        ])
        
        _logger.info(f"Found {len(alert_module)} uninstalled alert_management modules")
        
        if alert_module:
            _logger.info("Found uninstalled alert_management module, installing...")
            alert_module.button_install()
            cr.commit()
            _logger.info("✅ Alert Management auto-installed successfully!")
        else:
            # Check if already installed
            installed_alert = env['ir.module.module'].search([
                ('name', '=', 'alert_management')
            ])
            _logger.info(f"Found {len(installed_alert)} alert_management modules in any state")
            
            if installed_alert:
                _logger.info(f"ℹ️ Alert Management found with state: {installed_alert.state}")
            else:
                _logger.warning("❌ Alert Management module not found at all")
                
                # Let's check if the module is in the filesystem but not in database
                # This happens when modules aren't scanned yet
                _logger.info("Attempting to update module list...")
                env['ir.module.module'].update_list()
                cr.commit()
                
                # Try again after updating module list
                alert_module_after_update = env['ir.module.module'].search([
                    ('name', '=', 'alert_management')
                ])
                
                if alert_module_after_update:
                    _logger.info(f"Found alert_management after update: {alert_module_after_update.state}")
                    if alert_module_after_update.state == 'uninstalled':
                        alert_module_after_update.button_immediate_install()
                        cr.commit()
                        _logger.info("✅ Alert Management installed after module list update!")
                else:
                    _logger.error("❌ Still no alert_management module found after update")
                
    except Exception as e:
        _logger.error(f"Error auto-installing alert_management: {e}")
        import traceback
        _logger.error(traceback.format_exc())
