# -*- coding: utf-8 -*-
"""
IrConfigParameter Model
========================
This model extends the 'ir.config_parameter' model to manage configuration 
parameters, specifically for initializing and managing an encryption key 
used for URL encryption in the application.
"""

from odoo import models, api

class IrConfigParameter(models.Model):
    _inherit = 'ir.config_parameter'
    
    @api.model
    def init_url_encryption_key(self):
        """Initialize encryption key if it does not already exist.

        This method checks if the URL encryption key is set in the configuration 
        parameters. If not, it generates a new key using Fernet encryption and 
        stores it in the parameters.

        Returns:
            None
        """
        if not self.get_param('url_encryption.key'):
            from cryptography.fernet import Fernet
            key = Fernet.generate_key().decode()
            self.set_param('url_encryption.key', key)
