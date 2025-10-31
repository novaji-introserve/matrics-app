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
        
    # Remove unwanted partner actions
    try:
        _logger.info("Removing unwanted partner actions...")
        env['res.partner'].remove_unwanted_partner_actions()
        _logger.info("Partner actions removed successfully")
    except Exception as e:
        _logger.error(f"Error removing partner actions: {e}")

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
            _logger.info(
                "Persistent database settings initialized successfully")
        else:
            _logger.error("Failed to initialize persistent database settings")
    except Exception as e:
        _logger.error(f"Error initializing persistent database settings: {e}")

    _logger.info("Completed post-init hook for compliance_management")
    # Install related modules
    install_alert_management(cr, registry)
    install_case_management(cr, registry)


def uninstall_hook(cr, registry):
    """Uninstallation hook called when the module is uninstalled"""
    try:
        from .services.websocket.manager import stop_websocket_server
        stop_websocket_server()
        _logger.info("WebSocket server stopped successfully")
    except Exception as e:
        _logger.error(f"Error stopping WebSocket server: {e}")

    # Uninstall related modules
    uninstall_alert_management(cr, registry)
    uninstall_case_management(cr, registry)


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
        _logger.info(
            f"Found {len(all_alert_modules)} modules with 'alert' in name:")
        for module in all_alert_modules:
            _logger.info(f"  - {module.name}: {module.state}")

        # Check if alert_management exists and is not installed
        alert_module = env['ir.module.module'].search([
            ('name', '=', 'alert_management'),
            ('state', '=', 'uninstalled')
        ])

        _logger.info(
            f"Found {len(alert_module)} uninstalled alert_management modules")

        if alert_module:
            _logger.info(
                "Found uninstalled alert_management module, installing...")
            alert_module.button_install()
            cr.commit()
            _logger.info("✅ Alert Management auto-installed successfully!")
        else:
            # Check if already installed
            installed_alert = env['ir.module.module'].search([
                ('name', '=', 'alert_management')
            ])
            _logger.info(
                f"Found {len(installed_alert)} alert_management modules in any state")

            if installed_alert:
                _logger.info(
                    f"ℹ️ Alert Management found with state: {installed_alert.state}")
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
                    _logger.info(
                        f"Found alert_management after update: {alert_module_after_update.state}")
                    if alert_module_after_update.state == 'uninstalled':
                        alert_module_after_update.button_immediate_install()
                        cr.commit()
                        _logger.info(
                            "✅ Alert Management installed after module list update!")
                else:
                    _logger.error(
                        "❌ Still no alert_management module found after update")

    except Exception as e:
        _logger.error(f"Error auto-installing alert_management: {e}")
        import traceback
        _logger.error(traceback.format_exc())


def uninstall_alert_management(cr, registry):
    """
    Auto-uninstall alert_management when compliance_management is uninstalled
    """
    _logger.info("=== Starting alert_management auto-uninstallation ===")

    try:
        env = api.Environment(cr, SUPERUSER_ID, {})

        # Find installed alert_management module
        alert_module = env['ir.module.module'].search([
            ('name', '=', 'alert_management'),
            ('state', '=', 'installed')
        ])

        if alert_module:
            _logger.info(
                "Found installed alert_management module, uninstalling...")
            alert_module.button_uninstall()
            cr.commit()
            _logger.info("✅ Alert Management auto-uninstalled successfully!")
        else:
            # Check current state
            alert_any_state = env['ir.module.module'].search([
                ('name', '=', 'alert_management')
            ])
            if alert_any_state:
                _logger.info(
                    f"ℹ️ Alert Management found with state: {alert_any_state.state}")
            else:
                _logger.info("ℹ️ Alert Management module not found")

    except Exception as e:
        _logger.error(f"Error auto-uninstalling alert_management: {e}")
        import traceback
        _logger.error(traceback.format_exc())


def install_case_management(cr, registry):
    """
    Auto-install case_management when compliance_management is installed
    """
    _logger.info("=== Starting case_management auto-installation ===")

    try:
        env = api.Environment(cr, SUPERUSER_ID, {})

        # First, let's see all modules with 'case' in the name
        all_case_modules = env['ir.module.module'].search([
            ('name', '=', 'case_management_v2')
        ])
        _logger.info(
            f"Found {len(all_case_modules)} modules with 'case' in name:")
        for module in all_case_modules:
            _logger.info(f"  - {module.name}: {module.state}")

        # Check if case_management exists and is not installed
        case_module = env['ir.module.module'].search([
            ('name', '=', 'case_management_v2'),
            ('state', '=', 'uninstalled')
        ])

        _logger.info(
            f"Found {len(case_module)} uninstalled case_management modules")

        if case_module:
            _logger.info(
                "Found uninstalled case_management module, installing...")
            case_module.button_install()
            cr.commit()
            _logger.info("✅ Case Management auto-installed successfully!")
        else:
            # Check if already installed
            installed_case = env['ir.module.module'].search([
                ('name', '=', 'case_management_v2')
            ])
            _logger.info(
                f"Found {len(installed_case)} case_management modules in any state")

            if installed_case:
                _logger.info(
                    f"ℹ️ Case Management found with state: {installed_case.state}")
            else:
                _logger.warning("❌ Case Management module not found at all")

                # Let's check if the module is in the filesystem but not in database
                # This happens when modules aren't scanned yet
                _logger.info("Attempting to update module list...")
                env['ir.module.module'].update_list()
                cr.commit()

                # Try again after updating module list
                case_module_after_update = env['ir.module.module'].search([
                    ('name', '=', 'case_management_v2')
                ])

                if case_module_after_update:
                    _logger.info(
                        f"Found case_management after update: {case_module_after_update.state}")
                    if case_module_after_update.state == 'uninstalled':
                        case_module_after_update.button_immediate_install()
                        cr.commit()
                        _logger.info(
                            "✅ Case Management installed after module list update!")
                else:
                    _logger.error(
                        "❌ Still no case_management module found after update")

    except Exception as e:
        _logger.error(f"Error auto-installing case_management: {e}")
        import traceback
        _logger.error(traceback.format_exc())


def uninstall_case_management(cr, registry):
    """
    Auto-uninstall case_management when compliance_management is uninstalled
    """
    _logger.info("=== Starting case_management auto-uninstallation ===")

    try:
        env = api.Environment(cr, SUPERUSER_ID, {})

        # Find installed case_management module
        case_module = env['ir.module.module'].search([
            ('name', '=', 'case_management_v2'),
            ('state', '=', 'installed')
        ])

        if case_module:
            _logger.info(
                "Found installed case_management module, uninstalling...")
            case_module.button_uninstall()
            cr.commit()
            _logger.info("✅ Case Management auto-uninstalled successfully!")
        else:
            # Check current state
            case_any_state = env['ir.module.module'].search([
                ('name', '=', 'case_management_v2')
            ])
            if case_any_state:
                _logger.info(
                    f"ℹ️ Case Management found with state: {case_any_state.state}")
            else:
                _logger.info("ℹ️ Case Management module not found")

    except Exception as e:
        _logger.error(f"Error auto-uninstalling case_management: {e}")
        import traceback
        _logger.error(traceback.format_exc())
