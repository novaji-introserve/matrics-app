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

    model_id = fields.Many2one('ir.model', string='Model', required=True,
                               ondelete='cascade')
    model_name = fields.Char(related='model_id.model',
                             string='Model Name', store=True)

    # Window Actions (existing field)
    action_ids = fields.Many2many(
        'ir.actions.act_window',
        'view_access_rule_action_rel',
        'rule_id', 'action_id',
        string='Window Actions'
    )

    # Add Client Actions (new field)
    client_action_ids = fields.Many2many(
        'ir.actions.client',
        'view_access_rule_client_action_rel',
        'rule_id', 'client_action_id',
        string='Client Actions (Dashboards)'
    )

    # Store action IDs for faster lookup (extended to include client actions)
    action_id_numbers = fields.Char(
        string='Action IDs',
        compute='_compute_action_id_numbers',
        store=True
    )

    # Store client action IDs for faster lookup
    client_action_id_numbers = fields.Char(
        string='Client Action IDs',
        compute='_compute_client_action_id_numbers',
        store=True
    )

    client_action_tags = fields.Char('Client Action Tags',
                                     help="Comma-separated list of client action tags (for dashboard cards)")

    group_ids = fields.Many2many('res.groups', string='Allowed Groups')
    active = fields.Boolean(default=True)

    @api.depends('action_ids')
    def _compute_action_id_numbers(self):
        """Store action IDs as comma-separated string for faster lookup"""
        for rule in self:
            if rule.action_ids:
                rule.action_id_numbers = ','.join(
                    [str(action.id) for action in rule.action_ids])
            else:
                rule.action_id_numbers = False

    @api.depends('client_action_ids')
    def _compute_client_action_id_numbers(self):
        """Store client action IDs as comma-separated string for faster lookup"""
        for rule in self:
            if rule.client_action_ids:
                rule.client_action_id_numbers = ','.join(
                    [str(action.id) for action in rule.client_action_ids])
            else:
                rule.client_action_id_numbers = False

    @api.model
    def get_restricted_models(self):
        """Get list of restricted models"""
        rules = self.search([('active', '=', True)])
        return list(set(rule.model_name for rule in rules if rule.model_name))

    @api.model
    def get_restricted_actions(self):
        """Get list of restricted action IDs (both window and client)"""
        rules = self.search([('active', '=', True)])
        action_ids = []
        client_action_ids = []

        for rule in rules:
            if rule.action_ids:
                action_ids.extend(rule.action_ids.ids)
            if rule.client_action_ids:
                client_action_ids.extend(rule.client_action_ids.ids)

        # Return a dictionary with both types of actions
        return {
            'window_actions': list(set(action_ids)),
            'client_actions': list(set(client_action_ids))
        }

    @api.model
    def check_access(self, user, model=None, action_id=None, is_client_action=False):
        """Check if user has access to the specified model, action, or client action"""
        domain = [('active', '=', True)]

        if model:
            domain.append(('model_name', '=', model))

        # Get all rules matching the domain
        rules = self.search(domain)

        if not rules:
            # If no rules exist for this model, allow access by default
            return True

        # Special handling for client action tags (non-integer action IDs)
        if action_id and not isinstance(action_id, int):
            try:
                action_id = int(action_id)
            except (ValueError, TypeError):
                # This is a client action tag - check if it's in any restricted tags
                for rule in rules:
                    if rule.client_action_tags:
                        tag_list = [tag.strip()
                                    for tag in rule.client_action_tags.split(',')]
                        if action_id in tag_list:
                            # Check if user belongs to any allowed groups
                            if any(group in rule.group_ids for group in user.groups_id):
                                return True
                            return False
                # No specific tag rules, allow access
                return True

        # Filter rules based on the action type (for numeric action IDs)
        if action_id:
            filtered_rules = self.env['view.access.rule']

            for rule in rules:
                # Rules with no actions apply to all actions
                if not rule.action_ids and not rule.client_action_ids:
                    filtered_rules |= rule
                # Check window actions
                elif not is_client_action and rule.action_ids and action_id in rule.action_ids.ids:
                    filtered_rules |= rule
                # Check client actions
                elif is_client_action and rule.client_action_ids and action_id in rule.client_action_ids.ids:
                    filtered_rules |= rule

            rules = filtered_rules

            if not rules:
                # If no rules exist for this specific action, allow access by default
                return True

        # For each rule, check if user belongs to any of the allowed groups
        for rule in rules:
            if any(group in rule.group_ids for group in user.groups_id):
                return True

        return False

    # Helper method to determine action type

    @api.model
    def get_action_type(self, action_id):
        """Determine if an action ID is a window action or client action"""
        # Handle non-integer action IDs (client action tags)
        if not isinstance(action_id, int):
            try:
                action_id = int(action_id)
            except (ValueError, TypeError):
                # This is likely a client action tag
                return 'client_tag'

        # Try window action first (most common)
        window_action = self.env['ir.actions.act_window'].sudo().browse(
            action_id)
        if window_action.exists():
            return 'window'

        # Try client action next
        client_action = self.env['ir.actions.client'].sudo().browse(action_id)
        if client_action.exists():
            return 'client'

        # Not found or another type
        return False


class ViewAccessModelList(models.TransientModel):
    _name = 'view.access.model.list'
    _description = 'Available Models for Access Control'

    @api.model
    def get_available_models(self):
        """Get list of all models in the system"""
        models = self.env['ir.model'].search([])
        return [{'id': model.id, 'name': model.name, 'model': model.model}
                for model in models]

    @api.model
    def get_available_actions(self):
        """Get list of all window actions in the system"""
        actions = self.env['ir.actions.act_window'].search([])
        return [{'id': action.id, 'name': action.name, 'model': action.res_model, 'type': 'window'}
                for action in actions]

    @api.model
    def get_available_client_actions(self):
        """Get list of all client actions (dashboards) in the system"""
        actions = self.env['ir.actions.client'].search([])
        return [{'id': action.id, 'name': action.name, 'tag': action.tag, 'type': 'client'}
                for action in actions]

    @api.model
    def get_available_groups(self):
        """Get list of all groups in the system"""
        groups = self.env['res.groups'].search([])
        return [{'id': group.id, 'name': group.name, 'category': group.category_id.name}
                for group in groups]
