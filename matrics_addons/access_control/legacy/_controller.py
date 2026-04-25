from odoo import http
from odoo import models, fields, api, http
from odoo.http import request
from odoo.addons.web.controllers.home import Home
from odoo.addons.web.controllers.dataset import DataSet
from odoo.addons.web.controllers.action import Action
from odoo.exceptions import AccessError
import logging

class ViewSecurityController(http.Controller):

    def _check_access(self, user, model=None, action_id=None, is_client_action=False):
        """Check if user has access to restricted management views"""
        return request.env['view.access.rule'].check_access(user, model, action_id, is_client_action)

    def _check_action_type_and_access(self, user, action_id):
        """Check what type of action this is and if user has access"""
        if not action_id:
            return True, None

        # Convert action_id to int if possible
        original_action_id = action_id

        try:
            action_id = int(action_id)
        except (ValueError, TypeError):
            # This is a client action tag, not an integer ID
            action_type = 'client_tag'
            # For tags, we use the original string value
            has_access = self._check_access(user, action_id=original_action_id, is_client_action=True)
            return has_access, action_type

        # For numeric IDs, proceed as before
        action_type = request.env['view.access.rule'].get_action_type(action_id)

        if not action_type:
            return True, None  # Unknown action type, allow by default

        # Check access based on action type
        has_access = self._check_access(
            user,
            action_id=action_id,
            is_client_action=(action_type == 'client')
        )

        return has_access, action_type

        # ...


    def _check_access_for_action(self, user, model, action_id=None):
        """Check if user has access to a specific model+action combination"""
        # Get all rules for this model
        model_rules = request.env['view.access.rule'].search([
            ('active', '=', True),
            ('model_name', '=', model)
        ])

        if not model_rules:
            return True  # No restrictions for this model

        # First, check rules that apply to all actions (no specific actions defined)
        wildcard_rules = model_rules.filtered(lambda r: not r.action_ids)

        # If there are wildcard rules (rules with no specific actions)
        if wildcard_rules:
            # Check if user belongs to any allowed groups for these wildcard rules
            for rule in wildcard_rules:
                if any(group in rule.group_ids for group in user.groups_id):
                    return True

            # If we're here and there are wildcard rules, it means user doesn't belong
            # to any allowed group for model-wide rules, so access should be denied
            return False

        # If we get here, there are no wildcard rules, only action-specific rules
        # If no action_id is provided (e.g., model-level access), allow access
        if not action_id:
            return True

        # Check for rules that restrict this specific action
        action_rules = model_rules.filtered(
            lambda r: action_id in r.action_ids.ids)

        # If no rules exist for this specific action, allow access
        if not action_rules:
            return True

        # Check if user belongs to any allowed groups for this action
        for rule in action_rules:
            if any(group in rule.group_ids for group in user.groups_id):
                return True

        # If we reach here, user doesn't have access to this specific action
        return False

    @http.route('/web', type='http', auth="user", website=False)
    def web_client_security(self, s_action=None, **kw):
        """Block access to specific management views via URLs"""
        action_id = kw.get('action')
        user = request.env.user

        # Get restricted actions from the model
        restricted_actions = request.env['view.access.rule'].get_restricted_actions(
        )
        window_actions = restricted_actions.get('window_actions', [])
        client_actions = restricted_actions.get('client_actions', [])

        if action_id:
            try:
                action_id = int(action_id)

                # Check if it's a window action or client action
                has_access, action_type = self._check_action_type_and_access(
                    user, action_id)

                if not has_access:
                    action_name = "action"

                    # Try to get a friendly name for the action
                    if action_type == 'window':
                        try:
                            action = request.env['ir.actions.act_window'].sudo().browse(
                                action_id)
                            if action.exists():
                                action_name = action.name
                        except:
                            pass
                    elif action_type == 'client':
                        try:
                            action = request.env['ir.actions.client'].sudo().browse(
                                action_id)
                            if action.exists():
                                action_name = action.name
                        except:
                            pass

                    return request.render('web.access_denied', {
                        'message': f'You are not authorized to access "{action_name}".'
                    })
            except (ValueError, TypeError):
                pass

        # Call original web client
        return Home().web_client(s_action, **kw)

    @http.route('/web/dataset/call_kw/<path:path>', type='json', auth="user")
    def call_kw_security(self, model, method, args, kwargs, path=None):
        """Block model access based on dynamic rules"""
        # Almost unchanged from your original implementation
        # The only modification needed is when checking action_id
        user = request.env.user

        # Get restricted models from the model
        restricted_models = request.env['view.access.rule'].get_restricted_models(
        )

        if model in restricted_models:
            # Check if this is a main view access (not a related field or popup)
            context = kwargs.get('context', {})

            # Extract current action ID from context if available
            current_action_id = None
            if context and context.get('params') and context['params'].get('action'):
                current_action_id = context['params']['action']

            # Only check access if we're accessing a main view, not a related field
            is_main_view = not context.get('from_related_field')

            # If it's a main view operation that should be restricted
            if is_main_view and (method == 'get_views' or method == 'web_search_read'):
                # Check access for this specific action or for the model as a whole
                if not self._check_access_for_action(user, model, current_action_id):
                    # Get friendly names for better error messages
                    model_name = model
                    action_name = ""

                    try:
                        ir_model = request.env['ir.model'].sudo().search(
                            [('model', '=', model)], limit=1)
                        if ir_model:
                            model_name = ir_model.name

                        if current_action_id:
                            # Check if it's a window action or client action
                            action_type = request.env['view.access.rule'].get_action_type(
                                current_action_id)

                            if action_type == 'window':
                                action = request.env['ir.actions.act_window'].sudo().browse(
                                    current_action_id)
                                if action.exists():
                                    action_name = f" ({action.name})"
                            elif action_type == 'client':
                                action = request.env['ir.actions.client'].sudo().browse(
                                    current_action_id)
                                if action.exists():
                                    action_name = f" ({action.name})"
                    except Exception as e:
                        _logger.error(f"Error fetching names: {e}")

                    if method == 'get_views':
                        raise AccessError(
                            f'Access denied to {model_name} view{action_name}. Contact your administrator for access.')
                    elif method == 'web_search_read':
                        raise AccessError(
                            f'Access denied to {model_name} listing{action_name}. Contact your administrator for access.')

        return DataSet().call_kw(model, method, args, kwargs, path)

    @http.route('/web/action/load', type='json', auth="user")
    def load_action_security(self, action_id, additional_context=None):
        """Block loading of specific management actions"""
        user = request.env.user

        try:
            # Handle non-integer action_id (client action tags)
            if isinstance(action_id, str) and not action_id.isdigit():
                # For client action tags, check access directly
                has_access = self._check_access(
                    user, action_id=action_id, is_client_action=True)

                if not has_access:
                    return {
                        'type': 'ir.actions.client',
                        'tag': 'display_notification',
                        'params': {
                            'type': 'danger',
                            'message': f'Access Denied: You do not have permission to access this dashboard component.',
                            'sticky': False,
                        }
                    }

                # If access is allowed, let Action.load handle it
                return Action().load(action_id, additional_context)

            # For numeric action IDs, continue with existing logic
            has_access, action_type = self._check_action_type_and_access(
                user, action_id)

            if not has_access:
                # Get friendly names for better error message
                action_name = f"Action #{action_id}"
                model_name = ""

                try:
                    if action_type == 'window':
                        action = request.env['ir.actions.act_window'].sudo().browse(
                            action_id)
                        if action.exists():
                            action_name = action.name

                            # Get model name if available
                            if action.res_model:
                                ir_model = request.env['ir.model'].sudo().search(
                                    [('model', '=', action.res_model)], limit=1)
                                if ir_model:
                                    model_name = f" ({ir_model.name})"
                    elif action_type == 'client':
                        action = request.env['ir.actions.client'].sudo().browse(
                            action_id)
                        if action.exists():
                            action_name = action.name
                            # Most client actions are dashboards
                            model_name = " (Dashboard)"
                except Exception as e:
                    _logger.error(f"Error fetching names: {e}")

                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'type': 'danger',
                        'message': f'Access Denied: You do not have permission to access "{action_name}"{model_name}.',
                        'sticky': False,
                    }
                }

            # Window action specific logic
            if action_type == 'window':
                action = request.env['ir.actions.act_window'].sudo().browse(
                    action_id)
                if action.exists() and action.res_model:
                    model = action.res_model
                    model_rules = request.env['view.access.rule'].search([
                        ('active', '=', True),
                        ('model_name', '=', model)
                    ])

                    if model_rules and not self._check_access_for_action(user, model, action_id):
                        # Get friendly names for better error message
                        model_name = model
                        action_name = f"Action #{action_id}"

                        try:
                            ir_model = request.env['ir.model'].sudo().search(
                                [('model', '=', model)], limit=1)
                            if ir_model:
                                model_name = ir_model.name

                            action_name = action.name
                        except Exception as e:
                            _logger.error(f"Error fetching names: {e}")

                        return {
                            'type': 'ir.actions.client',
                            'tag': 'display_notification',
                            'params': {
                                'type': 'danger',
                                'message': f'Access Denied: You do not have permission to access "{action_name}" ({model_name}).',
                                'sticky': False,
                            }
                        }
        except Exception as e:
            _logger.error(f"Error in load_action_security: {e}")

        return Action().load(action_id, additional_context)


    @http.route('/web/action/run', type='json', auth="user")
    def run_action_security(self, action_id, **kw):
        """Block running of client actions (dashboards)"""
        user = request.env.user

        # Add debugging
        _logger.info(f"Action run requested: action_id={action_id}, user={user.name}, kw={kw}")

        try:
            # Check if this is a client action and if access is denied
            has_access, action_type = self._check_action_type_and_access(
                user, action_id)

            _logger.info(f"Access check result: has_access={has_access}, action_type={action_type}")

            if action_type == 'client' and not has_access:
                action_name = f"Dashboard #{action_id}"

                try:
                    action = request.env['ir.actions.client'].sudo().browse(
                        action_id)
                    if action.exists():
                        action_name = action.name
                except Exception as e:
                    _logger.error(f"Error fetching client action name: {e}")

                _logger.warning(f"Access denied for client action: {action_name}")
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'type': 'danger',
                        'message': f'Access Denied: You do not have permission to access "{action_name}".',
                        'sticky': False,
                    }
                }
        except Exception as e:
            _logger.error(f"Error in run_action_security: {e}")

        # If access is granted or it's not a restricted action, delegate to original handler
        _logger.info(f"Delegating to original Action.run for action_id={action_id}")
        return Action().run(action_id, **kw)

    @http.route('/view_access/get_available_models', type='json', auth='user')
    def get_available_models(self):
        """API endpoint to get all available models"""
        return request.env['view.access.model.list'].get_available_models()

    @http.route('/view_access/get_available_actions', type='json', auth='user')
    def get_available_actions(self):
        """API endpoint to get all available window actions"""
        return request.env['view.access.model.list'].get_available_actions()

    @http.route('/view_access/get_available_client_actions', type='json', auth='user')
    def get_available_client_actions(self):
        """API endpoint to get all available client actions (dashboards)"""
        return request.env['view.access.model.list'].get_available_client_actions()

    @http.route('/view_access/get_available_groups', type='json', auth='user')
    def get_available_groups(self):
        """API endpoint to get all available groups"""
        return request.env['view.access.model.list'].get_available_groups()
