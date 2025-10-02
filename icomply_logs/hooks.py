# -*- coding: utf-8 -*-
from odoo import api, SUPERUSER_ID
import logging

_logger = logging.getLogger(__name__)


def post_init_hook(cr, registry):
    """Post-initialization hook called when the module is installed"""
    _logger.info("Starting post-init hook for rabba")
    env = api.Environment(cr, SUPERUSER_ID, {})

    # Hide unwanted menus
    try:
        env['ir.ui.menu']._hide_unwanted_menus()
        _logger.info("Menus hidden successfully")
    except Exception as e:
        _logger.error(f"Error hiding menus: {e}")