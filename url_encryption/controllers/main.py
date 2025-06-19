# -*- coding: utf-8 -*-

"""
URLEncryptionController
=======================
This controller handles URL parameter encryption and decryption for secure data 
transmission. It uses Fernet symmetric encryption and supports compression to 
reduce payload size.
"""

from odoo import http
from odoo.http import request
import base64
import json
import zlib
from cryptography.fernet import Fernet
import logging

_logger = logging.getLogger(__name__)

class URLEncryptionController(http.Controller):
    
    def _get_encryption_key(self):
        """Get or generate encryption key.

        This method retrieves the encryption key from the configuration parameters. 
        If the key does not exist, it generates a new key and stores it.

        Returns:
            bytes: The encryption key.
        """
        IrConfigParameter = request.env['ir.config_parameter'].sudo()
        key = IrConfigParameter.get_param('url_encryption.key')
        
        if not key:
            key = Fernet.generate_key().decode()
            IrConfigParameter.set_param('url_encryption.key', key)
        
        return key.encode()
    
    def _encrypt_data(self, data):
        """Encrypt data using Fernet encryption with compression.

        Args:
            data (dict): The data to be encrypted.

        Returns:
            str or None: The encrypted, base64-encoded string if successful; 
            None if an error occurs.
        """
        try:
            key = self._get_encryption_key()
            f = Fernet(key)
            json_data = json.dumps(data, separators=(',', ':'))
            compressed_data = zlib.compress(json_data.encode(), level=9)
            encrypted_data = f.encrypt(compressed_data)
            encoded_data = base64.urlsafe_b64encode(encrypted_data).decode()
            
            if len(encoded_data) <= 70:
                return encoded_data
            else:
                _logger.warning(f"Token length {len(encoded_data)} exceeds 70 characters limit")
                return encoded_data[:70] if len(encoded_data) > 70 else encoded_data
                
        except Exception as e:
            _logger.error(f"Encryption error: {e}")
            return None
    
    def _decrypt_data(self, encrypted_data):
        """Decrypt data using Fernet encryption with decompression.

        Args:
            encrypted_data (str): The encrypted, base64-encoded string to decrypt.

        Returns:
            dict or None: The decrypted data as a dictionary if successful; 
            None if an error occurs.
        """
        try:
            key = self._get_encryption_key()
            f = Fernet(key)
            decoded_data = base64.urlsafe_b64decode(encrypted_data.encode())
            decrypted_data = f.decrypt(decoded_data)
            
            decompressed_data = zlib.decompress(decrypted_data)
            return json.loads(decompressed_data.decode())
        except Exception as e:
            _logger.error(f"Decryption error: {e}")
            return None
    
    @http.route('/web/encrypt_url', type='json', auth='user')
    def encrypt_url_params(self, **params):
        """Encrypt URL parameters.

        This method encrypts sensitive URL parameters and returns them along with 
        any regular parameters.

        Args:
            **params: The URL parameters to be processed.

        Returns:
            dict: A response indicating success or failure, along with the 
            parameters (encrypted and regular).
        """
        try:
            sensitive_params = {}
            regular_params = {}
            
            encrypt_keys = ['action', 'menu_id', 'id', 'active_id', 'active_ids', 'model', 'cids', 'view_type']
            
            for key, value in params.items():
                if key in encrypt_keys:
                    sensitive_params[key] = value
                else:
                    regular_params[key] = value
            
            if sensitive_params:
                encrypted_token = self._encrypt_data(sensitive_params)
                if encrypted_token:
                    regular_params['t'] = encrypted_token
                    return {'success': True, 'params': regular_params}
            
            return {'success': False, 'error': 'Encryption failed'}
        except Exception as e:
            _logger.error(f"URL encryption error: {e}")
            return {'success': False, 'error': str(e)}
    
    @http.route('/web/decrypt_url', type='json', auth='user')
    def decrypt_url_params(self, token=None, **params):
        """Decrypt URL parameters.

        This method decrypts the provided token and combines it with any 
        additional URL parameters.

        Args:
            token (str, optional): The encrypted token containing sensitive parameters.
            **params: Additional URL parameters.

        Returns:
            dict: A response indicating success or failure, along with the 
            decrypted parameters.
        """
        try:
            if not token:
                return {'success': False, 'error': 'No token provided'}
            
            decrypted_params = self._decrypt_data(token)
            if decrypted_params:
                result_params = dict(params)
                result_params.update(decrypted_params)
                return {'success': True, 'params': result_params}
            
            return {'success': False, 'error': 'Decryption failed'}
        except Exception as e:
            _logger.error(f"URL decryption error: {e}")
            return {'success': False, 'error': str(e)}
