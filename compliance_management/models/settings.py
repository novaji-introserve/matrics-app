from odoo import models, fields, api, SUPERUSER_ID, _

import logging



class ComplianceSettings(models.Model):
    _name = 'res.compliance.settings'
    _description = 'Compliance Settings'
    _sql_constraints = [
        ('uniq_compl_settings_code', 'unique(code)',
         "Code already exists. Value must be unique!")
    ]

    name = fields.Char(string="Name", required=True)
    code = fields.Char(string='Code', required=True,index=True)
    val = fields.Char(string='Value')
    narration = fields.Text(string='Narration')

    active = fields.Boolean(default=True, help='Set to false to hide the record without deleting it.')

    @api.model
    def get_setting(self, code):
        """Get a setting value by code"""
        setting = self.search([('code', '=', code)], limit=1)
        if setting:
            return setting.val
        return None


_logger = logging.getLogger(__name__)

class IrUiMenu(models.Model):
    _inherit = 'ir.ui.menu'
    @api.model
    def _auto_init(self):
        res = super(IrUiMenu, self)._auto_init()
        # Avoid menu writes during registry init/module upgrade.
        # Menu hiding is handled by the post-init hook and load_menus().
        return res
    @api.model
    def _hide_unwanted_menus(self):
        """Hide specified menus using safe methods"""
        # Define menus to hide
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
        # 1. First attempt: Hide by XML ID directly
        for xml_id in menus_to_hide:
            try:
                menu = self.env.ref(xml_id, raise_if_not_found=False)
                if menu and menu.active:
                    menu.sudo().write({'active': False})
                    _logger.info(f"Hidden menu '{menu.name}' via XML ID '{xml_id}'")
            except Exception as e:
                _logger.warning(f"Failed to hide menu '{xml_id}': {e}")
        # 2. Second attempt: Hide by name
        menu_names = [
            'Project', 'Projects', 'Email Marketing', 'Link Tracker',
            'Contacts', 'Messaging', 'Discuss', 'Surveys'
        ]
        for name in menu_names:
            try:
                menus = self.sudo().search([('name', '=', name), ('active', '=', True)])
                for menu in menus:
                    menu.write({'active': False})
                    _logger.info(f"Hidden menu '{menu.name}' via name search")
            except Exception as e:
                _logger.warning(f"Failed to hide menu by name '{name}': {e}")
        # 3. Special handling for parent menus
        parent_xmlids = [
            'mail.mail_menu_root',  # Parent of Messaging
            'contacts.menu_contacts_root',  # Parent of Contacts
        ]
        for xml_id in parent_xmlids:
            try:
                menu = self.env.ref(xml_id, raise_if_not_found=False)
                if menu:
                    # Find all children and hide them
                    child_menus = self.sudo().search([('parent_id', '=', menu.id), ('active', '=', True)])
                    for child in child_menus:
                        child.write({'active': False})
                        _logger.info(f"Hidden child menu '{child.name}' of '{menu.name}'")
            except Exception as e:
                _logger.warning(f"Failed to hide children of menu '{xml_id}': {e}")
        return True
    # SAFE way to ensure menus stay hidden without breaking the UI
    @api.model
    def load_menus(self, debug):
        """Override load_menus to ensure our menus stay hidden"""
        res = super(IrUiMenu, self).load_menus(debug)
        try:
            # Apply hiding after menus are loaded but before returning
            # This ensures our changes happen at the right time
            self.sudo()._hide_unwanted_menus()
        except Exception as e:
            _logger.error(f"Error in load_menus override: {e}")
        return res

