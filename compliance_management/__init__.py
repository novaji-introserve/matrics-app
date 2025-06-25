# -*- coding: utf-8 -*-
"""
Compliance Management Module
===========================
Main module entry point that handles initialization of Odoo models,
controllers, and services.
"""

# from . import websocket_patch
from odoo import api, SUPERUSER_ID
import logging
from . import models
from . import controllers
from . import services
from . import utils
from .hooks import post_init_hook, uninstall_hook
# Add this line


_logger = logging.getLogger(__name__)



