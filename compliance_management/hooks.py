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
    """Post-initialization hook called when the module is installed.

    Keep this hook limited to local setup for this module. Triggering other
    module installs from here can contend on ``ir_module_module`` during
    installs/upgrades and cause lock timeouts.
    """
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
        
    # Create trigger and index and res partner table
    try:
        _logger.info("Creating Trigger and index on res partner...")
        env['res.partner'].create_customer_trigger()
        _logger.info("Creating Trigger and index on res partner was successful")
    except Exception as e:
        _logger.error(f"Error Creating Trigger and index on res partner: {e}")
        
    # Create trigger and index and res.partner.account table
    try:
        _logger.info("Creating Trigger and index on res.partner.account...")
        env['res.partner.account'].customer_account_triggers_and_indexes()
        _logger.info("Creating Trigger and index on res.partner.account was successful")
    except Exception as e:
        _logger.error(f"Error Creating Trigger and index on res.partner.account: {e}")
        
    try:
        _logger.info("Creating res_customer_pep view...")
        cr.execute("""
            CREATE OR REPLACE VIEW res_customer_pep AS (
                SELECT 
                    c.id AS id,
                    c.id AS customer_id,
                    c.branch_id,
                    c.firstname,
                    c.lastname,
                    c.internal_category,
                    c.name,
                    c.global_pep_id AS pep_id,
                    c.is_pep
                FROM res_partner c
                WHERE c.is_pep = TRUE
            )
        """)
        cr.commit()  # Commit the schema change
        _logger.info("Successfully created res_customer_pep view.")
    except Exception as e:
        _logger.error(f"Error creating res_customer_pep view: {e}")

    _logger.info("Completed post-init hook for compliance_management")


def uninstall_hook(cr, registry):
    """Uninstallation hook called when the module is uninstalled"""
    try:
        from .services.websocket.manager import stop_websocket_server
        stop_websocket_server()
        _logger.info("WebSocket server stopped successfully")
    except Exception as e:
        _logger.error(f"Error stopping WebSocket server: {e}")

    # Do not chain module uninstalls from this hook. Keep module lifecycle
    # operations explicit to avoid lock contention on ir_module_module.


def install_alert_management(cr, registry):
    """
    Auto-install alert_management when compliance_management is installed
    """
    _logger.info(
        "Skipping automatic alert_management installation from compliance_management hook"
    )


def uninstall_alert_management(cr, registry):
    """
    Auto-uninstall alert_management when compliance_management is uninstalled
    """
    _logger.info(
        "Skipping automatic alert_management uninstallation from compliance_management hook"
    )


def install_case_management(cr, registry):
    """
    Auto-install case_management when compliance_management is installed
    """
    _logger.info(
        "Skipping automatic case_management installation from compliance_management hook"
    )


def uninstall_case_management(cr, registry):
    """
    Auto-uninstall case_management when compliance_management is uninstalled
    """
    _logger.info(
        "Skipping automatic case_management uninstallation from compliance_management hook"
    )
