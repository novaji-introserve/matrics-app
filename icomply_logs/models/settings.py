from odoo import models, fields, api, SUPERUSER_ID, _
import logging

_logger = logging.getLogger(__name__)


class IrUiMenu(models.Model):
    _inherit = 'ir.ui.menu'

    @api.model
    def _hide_unwanted_menus(self):
        """Hide specified menus using safe methods"""
        _logger.info("Starting to hide unwanted menus...")
        
        # Define menus to hide by XML ID
        menus_to_hide = [
            # Project menus
            'project.menu_main_pm',
            'project.menu_projects', 
            'project.menu_project_config',
            # Email marketing
            'mass_mailing.mass_mailing_menu_root',
            # Link tracker
            'utm.menu_link_tracker_root',
            # Contacts
            'contacts.menu_contacts',
            'contacts.res_partner_menu_contacts',
            # Messaging
            'mail.menu_root_discuss',
            # Surveys
            'survey.menu_surveys',
            'survey.menu_survey_form',
        ]

        # Method 1: Hide by XML ID directly
        for xml_id in menus_to_hide:
            try:
                # Use safe reference lookup
                menu = self.env.ref(xml_id, raise_if_not_found=False)
                if menu and menu.exists() and menu.active:
                    menu.sudo().write({'active': False})
                    _logger.info(f"Hidden menu '{menu.name}' via XML ID '{xml_id}'")
            except Exception as e:
                _logger.warning(f"Failed to hide menu '{xml_id}': {e}")

        # Method 2: Hide by name (fallback for menus that might not have expected XML IDs)
        menu_names_to_hide = [
            'Project', 'Projects', 'Email Marketing', 'Link Tracker',
            'Contacts', 'Messaging', 'Discuss', 'Surveys','Apps', 'SMS Marketing','Employees'
        ]
        
        for name in menu_names_to_hide:
            try:
                menus = self.sudo().search([
                    ('name', '=', name), 
                    ('active', '=', True)
                ])
                for menu in menus:
                    menu.write({'active': False})
                    _logger.info(f"Hidden menu '{menu.name}' via name search")
            except Exception as e:
                _logger.warning(f"Failed to hide menu by name '{name}': {e}")

        # Method 3: Hide children of specific parent menus
        parent_xmlids = [
            'mail.mail_menu_root',      # Parent of Messaging
            'contacts.menu_contacts',    # Parent of Contacts  
        ]
        
        for xml_id in parent_xmlids:
            try:
                parent_menu = self.env.ref(xml_id, raise_if_not_found=False)
                if parent_menu and parent_menu.exists():
                    # Find and hide all active children
                    child_menus = self.sudo().search([
                        ('parent_id', '=', parent_menu.id), 
                        ('active', '=', True)
                    ])
                    for child in child_menus:
                        child.write({'active': False})
                        _logger.info(f"Hidden child menu '{child.name}' of '{parent_menu.name}'")
                        
                    # Also hide the parent if desired
                    if parent_menu.active:
                        parent_menu.sudo().write({'active': False})
                        _logger.info(f"Hidden parent menu '{parent_menu.name}'")
                        
            except Exception as e:
                _logger.warning(f"Failed to hide children of menu '{xml_id}': {e}")

        _logger.info("Finished hiding unwanted menus")
        return True

    @api.model
    def load_menus(self, debug):
        """Override load_menus to ensure our menus stay hidden"""
        # Load menus normally first
        res = super(IrUiMenu, self).load_menus(debug)
        
        # Then apply our hiding logic
        try:
            self.sudo()._hide_unwanted_menus()
        except Exception as e:
            _logger.error(f"Error in load_menus override: {e}")
            
        return res

    @api.model_create_multi  
    def create(self, vals_list):
        """Override create to hide menus that might be created after our hiding"""
        menus = super(IrUiMenu, self).create(vals_list)
        
        # Check if any of the newly created menus should be hidden
        for menu in menus:
            if self._should_hide_menu(menu):
                try:
                    menu.sudo().write({'active': False})
                    _logger.info(f"Auto-hidden newly created menu '{menu.name}'")
                except Exception as e:
                    _logger.warning(f"Failed to auto-hide menu '{menu.name}': {e}")
                    
        return menus

    def _should_hide_menu(self, menu):
        """Check if a menu should be hidden based on our criteria"""
        hide_names = ['Project', 'Projects', 'Email Marketing', 'Link Tracker',
                      'Contacts', 'Messaging', 'Discuss', 'Surveys','Apps','SMS Marketing','Employees']
        
        return menu.name in hide_names