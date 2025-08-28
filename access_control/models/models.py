from odoo.tools.safe_eval import safe_eval
from odoo import models, fields, api
from odoo import models, fields, api, http
import logging
from odoo.exceptions import UserError, AccessError


_logger = logging.getLogger(__name__)

class ViewAccessRule(models.Model):
    _name = 'view.access.rule'
    _description = 'View Access Control Rules'

    name = fields.Char('Rule Name', required=True)
    model_id = fields.Many2one('ir.model', string='Model', required=True, ondelete='cascade')
    model_name = fields.Char(related='model_id.model', string='Model Name', store=True)

    # Window Actions (existing)
    action_ids = fields.Many2many('ir.actions.act_window', 'view_access_rule_action_rel', 'rule_id', 'action_id', string='Window Actions')
    
    # Client Actions (existing) 
    client_action_ids = fields.Many2many('ir.actions.client', 'view_access_rule_client_action_rel', 'rule_id', 'client_action_id', string='Client Actions (Dashboards)')
    
    # Server Actions (NEW - integrated with Odoo security)
    server_action_ids = fields.Many2many('ir.actions.server', 'view_access_rule_server_action_rel', 'rule_id', 'server_action_id', string='Server Actions')
    
    # Menu restrictions
    menu_ids = fields.Many2many('ir.ui.menu', 'view_access_rule_menu_rel', 'rule_id', 'menu_id', string='Restricted Menus')

    # Computed fields for faster lookup
    action_id_numbers = fields.Char(string='Action IDs', compute='_compute_action_id_numbers', store=True)
    client_action_id_numbers = fields.Char(string='Client Action IDs', compute='_compute_client_action_id_numbers', store=True)
    menu_id_numbers = fields.Char(string='Menu IDs', compute='_compute_menu_id_numbers', store=True)
    
    client_action_tags = fields.Char('Client Action Tags', help="Comma-separated list of client action tags (for dashboard cards)")
    group_ids = fields.Many2many('res.groups', string='Allowed Groups')
    active = fields.Boolean(default=True)

    @api.depends('action_ids')
    def _compute_action_id_numbers(self):
        for rule in self:
            if rule.action_ids:
                rule.action_id_numbers = ','.join([str(action.id) for action in rule.action_ids])
            else:
                rule.action_id_numbers = False

    @api.depends('client_action_ids')
    def _compute_client_action_id_numbers(self):
        for rule in self:
            if rule.client_action_ids:
                rule.client_action_id_numbers = ','.join([str(action.id) for action in rule.client_action_ids])
            else:
                rule.client_action_id_numbers = False

    @api.depends('menu_ids')
    def _compute_menu_id_numbers(self):
        for rule in self:
            if rule.menu_ids:
                rule.menu_id_numbers = ','.join([str(menu.id) for menu in rule.menu_ids])
            else:
                rule.menu_id_numbers = False

    # NEW: Sync server action groups with access rule groups (KEY INTEGRATION)
    def _sync_server_action_groups(self):
        """Sync server action groups with access rule groups"""
        for rule in self:
            if rule.server_action_ids and rule.group_ids:
                for server_action in rule.server_action_ids:
                    # Set server action groups to match rule's allowed groups
                    server_action.write({'groups_id': [(6, 0, rule.group_ids.ids)]})

    @api.model
    def create(self, vals):
        """Override create to sync server action and menu groups"""
        rule = super().create(vals)
        rule._sync_server_action_groups()
        rule._sync_menu_groups()  # Add this line
        return rule


    def write(self, vals):
        """Override write to sync server action and menu groups"""
        result = super().write(vals)
        if 'server_action_ids' in vals or 'group_ids' in vals:
            self._sync_server_action_groups()
        if 'menu_ids' in vals or 'group_ids' in vals:  # Add this block
            self._sync_menu_groups()
        return result

    @api.model
    def get_restricted_models(self):
        """Get list of restricted models"""
        rules = self.search([('active', '=', True)])
        return list(set(rule.model_name for rule in rules if rule.model_name))

    @api.model
    def get_restricted_actions(self):
        """Get list of restricted action IDs (window, client, and server)"""
        rules = self.search([('active', '=', True)])
        window_action_ids = []
        client_action_ids = []
        server_action_ids = []

        for rule in rules:
            if rule.action_ids:
                window_action_ids.extend(rule.action_ids.ids)
            if rule.client_action_ids:
                client_action_ids.extend(rule.client_action_ids.ids)
            if rule.server_action_ids:
                server_action_ids.extend(rule.server_action_ids.ids)

        return {
            'window_actions': list(set(window_action_ids)),
            'client_actions': list(set(client_action_ids)),
            'server_actions': list(set(server_action_ids))
        }

    @api.model
    def get_restricted_menus(self):
        """Get list of restricted menu IDs"""
        rules = self.search([('active', '=', True)])
        menu_ids = []
        for rule in rules:
            if rule.menu_ids:
                menu_ids.extend(rule.menu_ids.ids)
        return list(set(menu_ids))

    @api.model
    def check_access(self, user, model=None, action_id=None, is_client_action=False):
        """Check if user has access to the specified model, action, or client action"""
        domain = [('active', '=', True)]

        if model:
            domain.append(('model_name', '=', model))

        rules = self.search(domain)
        if not rules:
            return True

        # Handle client action tags
        if action_id and not isinstance(action_id, int):
            try:
                action_id = int(action_id)
            except (ValueError, TypeError):
                for rule in rules:
                    if rule.client_action_tags:
                        tag_list = [tag.strip() for tag in rule.client_action_tags.split(',')]
                        if action_id in tag_list:
                            if any(group in rule.group_ids for group in user.groups_id):
                                return True
                            return False
                return True

        # Filter rules based on action type
        if action_id:
            filtered_rules = self.env['view.access.rule']
            for rule in rules:
                if not rule.action_ids and not rule.client_action_ids:
                    filtered_rules |= rule
                elif not is_client_action and rule.action_ids and action_id in rule.action_ids.ids:
                    filtered_rules |= rule
                elif is_client_action and rule.client_action_ids and action_id in rule.client_action_ids.ids:
                    filtered_rules |= rule
            rules = filtered_rules
            if not rules:
                return True

        # Check if user belongs to any allowed groups
        for rule in rules:
            if any(group in rule.group_ids for group in user.groups_id):
                return True
        return False

    # NEW: Server action access check (integrates with Odoo's built-in security)
    @api.model
    def check_server_action_access(self, user, action_id):
        """Check server action access using Odoo's built-in security + our rules"""
        
        # Check if this server action is managed by our rules
        rules = self.search([
            ('active', '=', True),
            ('server_action_ids', 'in', [action_id])
        ])
        
        if rules:
            # Server action is managed by our access rules
            for rule in rules:
                if any(group in rule.group_ids for group in user.groups_id):
                    return True
            return False
        
        # If not managed by our rules, fall back to Odoo's built-in check
        server_action = self.env['ir.actions.server'].sudo().browse(action_id)
        if server_action.exists() and server_action.groups_id:
            return bool(server_action.groups_id & user.groups_id)
        
        # No restrictions, allow access
        return True

    @api.model
    def check_menu_access(self, user, menu_id):
        """Check menu access using Odoo's built-in security + our rules"""

        # Check if this menu is managed by our rules
        rules = self.search([
            ('active', '=', True),
            ('menu_ids', 'in', [menu_id])
        ])

        if rules:
            # Menu is managed by our access rules
            for rule in rules:
                if any(group in rule.group_ids for group in user.groups_id):
                    return True
            return False

        # If not managed by our rules, fall back to Odoo's built-in check
        menu = self.env['ir.ui.menu'].sudo().browse(menu_id)
        if menu.exists() and menu.groups_id:
            return bool(menu.groups_id & user.groups_id)

        # No restrictions, allow access
        return True

    @api.model
    def get_action_type(self, action_id):
        """Determine action type including server actions"""
        if not isinstance(action_id, int):
            try:
                action_id = int(action_id)
            except (ValueError, TypeError):
                return 'client_tag'

        if self.env['ir.actions.act_window'].sudo().browse(action_id).exists():
            return 'window'
        if self.env['ir.actions.client'].sudo().browse(action_id).exists():
            return 'client'
        if self.env['ir.actions.server'].sudo().browse(action_id).exists():
            return 'server'
        return False

    def _sync_menu_groups(self):
        """Sync menu groups with access rule groups (same as server actions)"""
        for rule in self:
            if rule.menu_ids and rule.group_ids:
                for menu in rule.menu_ids:
                    # Set menu groups to match rule's allowed groups
                    menu.write({'groups_id': [(6, 0, rule.group_ids.ids)]})

class ViewAccessModelList(models.TransientModel):
    _name = 'view.access.model.list'
    _description = 'Available Models for Access Control'

    @api.model
    def get_available_models(self):
        models = self.env['ir.model'].search([])
        return [{'id': model.id, 'name': model.name, 'model': model.model} for model in models]

    @api.model
    def get_available_actions(self):
        actions = self.env['ir.actions.act_window'].search([])
        return [{'id': action.id, 'name': action.name, 'model': action.res_model, 'type': 'window'} for action in actions]

    @api.model
    def get_available_client_actions(self):
        actions = self.env['ir.actions.client'].search([])
        return [{'id': action.id, 'name': action.name, 'tag': action.tag, 'type': 'client'} for action in actions]

    @api.model
    def get_available_server_actions(self):
        actions = self.env['ir.actions.server'].search([])
        return [{'id': action.id, 'name': action.name, 'model': action.model_name, 'type': 'server'} for action in actions]

    @api.model
    def get_available_menus(self):
        menus = self.env['ir.ui.menu'].search([])
        return [{'id': menu.id, 'name': menu.name, 'complete_name': menu.complete_name, 'action': menu.action} for menu in menus]

    @api.model
    def get_available_groups(self):
        groups = self.env['res.groups'].search([])
        return [{'id': group.id, 'name': group.name, 'category': group.category_id.name} for group in groups]
        
class IrUiMenuInherit(models.Model):
    _inherit = 'ir.ui.menu'

    # Add link to show which access rules manage this menu
    access_rule_ids = fields.Many2many(
        'view.access.rule',
        'view_access_rule_menu_rel',
        'menu_id', 'rule_id',
        string='Managed by Access Rules',
        readonly=True
    )
    
# Inherit server actions to integrate with your system
class IrActionsServerInherit(models.Model):
    _inherit = 'ir.actions.server'

    access_rule_ids = fields.Many2many(
        'view.access.rule',
        'view_access_rule_server_action_rel',
        'server_action_id', 'rule_id',
        string='Managed by Access Rules',
        readonly=True
    )

    def run(self):
        """Override run to integrate with your access control system"""
        for action in self:
            access_rule_env = self.env['view.access.rule']
            if not access_rule_env.check_server_action_access(self.env.user, action.id):
                raise AccessError(
                    _('Access Denied: You do not have permission to execute "%s". '
                      'Contact your administrator for access.') % action.name
                )
        return super().run()

