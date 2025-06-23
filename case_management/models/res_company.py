# models/res_company.py
from odoo import models, fields, api
import base64
import os
import logging

_logger = logging.getLogger(__name__)

class ResCompany(models.Model):
    _inherit = 'res.company'
    
    name = fields.Char(required=True)
    
    alt_bank_logo = fields.Binary(
        string="Alternative Bank Logo",
        help="Upload the alternative bank logo (will replace existing file)"
    )
    
    # def write(self, vals):
    #     """Override write to save logo to file system when uploaded"""
    #     result = super().write(vals)
        
    #     # Check if alt_bank_logo was updated
    #     if 'alt_bank_logo' in vals:
    #         for company in self:
    #             if company.alt_bank_logo:
    #                 company._save_logo_to_filesystem()
    #             else:
    #                 # If logo was removed, optionally delete the file
    #                 company._remove_logo_file()
        
    #     return result
    
    def write(self, vals):
        # Avoid breaking updates that don't touch alt_bank_logo
        result = super().write(vals)

        if 'alt_bank_logo' in vals:
            for company in self:
                if company.alt_bank_logo:
                    company._save_logo_to_filesystem()
                else:
                    company._remove_logo_file()

        return result

    
    def _save_logo_to_filesystem(self):
        """Save the uploaded logo to the specified path, replacing existing file"""
        if not self.alt_bank_logo:
            return
            
        try:
            # Decode base64 data
            logo_binary = base64.b64decode(self.alt_bank_logo)
            
            # Fixed file path - always the same name
            logo_path = '/home/novaji/odoo16/custom_addons/icomply_odoo/case_management/static/src/img/alt_bank_logo.png'
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(logo_path), exist_ok=True)
            
            # Write the file (this will overwrite existing file)
            with open(logo_path, 'wb') as f:
                f.write(logo_binary)
                
            _logger.info(f"Logo saved successfully to {logo_path}")
            
        except Exception as e:
            _logger.error(f"Error saving logo: {str(e)}")
            
    def _remove_logo_file(self):
        """Remove the logo file when logo is deleted"""
        logo_path = '/home/novaji/odoo16/custom_addons/icomply_odoo/case_management/static/src/img/alt_bank_logo.png'
        
        try:
            if os.path.exists(logo_path):
                os.remove(logo_path)
                _logger.info(f"Logo file removed: {logo_path}")
        except Exception as e:
            _logger.error(f"Error removing logo file: {str(e)}")
    
    @api.model
    def get_alt_bank_logo_url(self):
        """Get the URL for the alternative bank logo"""
        logo_path = '/home/novaji/odoo16/custom_addons/icomply_odoo/case_management/static/src/img/alt_bank_logo.png'
        
        # Check if file exists
        if os.path.exists(logo_path):
            return '/icomply_odoo/static/src/img/alt_bank_logo.png'
        return False