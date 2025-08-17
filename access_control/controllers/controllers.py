from odoo import http
from odoo import models, fields, api, http
from odoo.http import request
from odoo.addons.web.controllers.home import Home
from odoo.addons.web.controllers.dataset import DataSet
from odoo.addons.web.controllers.action import Action
from odoo.exceptions import AccessError
import logging

_logger = logging.getLogger(__name__)


class ViewSecurityController(http.Controller):

    def _check_access(self, user, model=None, action_id=None, is_client_action=False, is_server_action=False):
        """Check if user has access to restricted management views"""
        if is_server_action:
            return request.env['view.access.rule'].check_server_action_access(user, action_id)
        else:
            return request.env['view.access.rule'].check_access(user, model, action_id, is_client_action)

    def _check_menu_access(self, user, menu_id):
        """Check if user has access to a specific menu"""
        return request.env['view.access.rule'].check_menu_access(user, menu_id)

    def _check_action_type_and_access(self, user, action_id):
        """Check what type of action this is and if user has access"""
        if not action_id:
            return True, None

        original_action_id = action_id

        try:
            action_id = int(action_id)
        except (ValueError, TypeError):
            action_type = 'client_tag'
            has_access = self._check_access(
                user, action_id=original_action_id, is_client_action=True)
            return has_access, action_type

        action_type = request.env['view.access.rule'].get_action_type(
            action_id)

        if not action_type:
            return True, None

        # Server action specific debug and handling
        if action_type == 'server':
            _logger.info(f"=== SERVER ACTION ACCESS CHECK ===")
            _logger.info(f"User: {user.name}")
            _logger.info(f"Server Action ID: {action_id}")

            has_access = self._check_access(
                user, action_id=action_id, is_server_action=True)
            _logger.info(f"Server action access result: {has_access}")

            return has_access, action_type

        elif action_type == 'window':
            has_access = self._check_access(
                user, action_id=action_id, is_client_action=False, is_server_action=False)
        elif action_type == 'client':
            has_access = self._check_access(
                user, action_id=action_id, is_client_action=True, is_server_action=False)
        else:
            has_access = True

        return has_access, action_type

    def _check_access_for_action(self, user, model, action_id=None):
        """Check if user has access to a specific model+action combination"""
        model_rules = request.env['view.access.rule'].search([
            ('active', '=', True),
            ('model_name', '=', model)
        ])

        if not model_rules:
            return True

        wildcard_rules = model_rules.filtered(
            lambda r: not r.action_ids and not r.client_action_ids and not r.server_action_ids)

        if wildcard_rules:
            for rule in wildcard_rules:
                if any(group in rule.group_ids for group in user.groups_id):
                    return True
            return False

        if not action_id:
            return True

        action_type = request.env['view.access.rule'].get_action_type(
            action_id)

        if action_type == 'window':
            action_rules = model_rules.filtered(
                lambda r: action_id in r.action_ids.ids)
        elif action_type == 'client':
            action_rules = model_rules.filtered(
                lambda r: action_id in r.client_action_ids.ids)
        elif action_type == 'server':
            action_rules = model_rules.filtered(
                lambda r: action_id in r.server_action_ids.ids)
        else:
            action_rules = self.env['view.access.rule']

        if not action_rules:
            return True

        for rule in action_rules:
            if any(group in rule.group_ids for group in user.groups_id):
                return True

        return False

    @http.route('/web', type='http', auth="user", website=False)
    def web_client_security(self, s_action=None, **kw):
        """Block access to specific management views via URLs"""
        action_id = kw.get('action')
        menu_id = kw.get('menu_id')
        user = request.env.user

        if menu_id:
            try:
                menu_id = int(menu_id)
                if not self._check_menu_access(user, menu_id):
                    menu_name = f"Menu #{menu_id}"
                    try:
                        menu = request.env['ir.ui.menu'].sudo().browse(menu_id)
                        if menu.exists():
                            menu_name = menu.name
                    except:
                        pass
                    return request.render('web.access_denied', {
                        'message': f'You are not authorized to access "{menu_name}".'
                    })
            except (ValueError, TypeError):
                pass

        if action_id:
            try:
                action_id = int(action_id)
                has_access, action_type = self._check_action_type_and_access(
                    user, action_id)

                if not has_access:
                    action_name = "action"
                    try:
                        if action_type == 'window':
                            action = request.env['ir.actions.act_window'].sudo().browse(
                                action_id)
                            if action.exists():
                                action_name = action.name
                        elif action_type == 'client':
                            action = request.env['ir.actions.client'].sudo().browse(
                                action_id)
                            if action.exists():
                                action_name = action.name
                        elif action_type == 'server':
                            action = request.env['ir.actions.server'].sudo().browse(
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


        return Home().web_client(s_action, **kw)

    @http.route('/web/dataset/call_kw/<path:path>', type='json', auth="user")
    def call_kw_security(self, model, method, args, kwargs, path=None):
        """Block model access based on dynamic rules"""
        user = request.env.user
        restricted_models = request.env['view.access.rule'].get_restricted_models(
        )

        if model in restricted_models:
            context = kwargs.get('context', {})
            current_action_id = None
            if context and context.get('params') and context['params'].get('action'):
                current_action_id = context['params']['action']

            is_main_view = not context.get('from_related_field')

            if is_main_view and (method == 'get_views' or method == 'web_search_read'):
                if not self._check_access_for_action(user, model, current_action_id):
                    model_name = model
                    action_name = ""

                    try:
                        ir_model = request.env['ir.model'].sudo().search(
                            [('model', '=', model)], limit=1)
                        if ir_model:
                            model_name = ir_model.name

                        if current_action_id:
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
                            elif action_type == 'server':
                                action = request.env['ir.actions.server'].sudo().browse(
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
            if isinstance(action_id, str) and not action_id.isdigit():
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
                from odoo.addons.web.controllers.action import Action
                return Action().load(action_id, additional_context)

            has_access, action_type = self._check_action_type_and_access(
                user, action_id)

            if not has_access:
                action_name = f"Action #{action_id}"
                model_name = ""

                try:
                    if action_type == 'window':
                        action = request.env['ir.actions.act_window'].sudo().browse(
                            action_id)
                        if action.exists():
                            action_name = action.name
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
                            model_name = " (Dashboard)"
                    elif action_type == 'server':
                        action = request.env['ir.actions.server'].sudo().browse(
                            action_id)
                        if action.exists():
                            action_name = action.name
                            if action.model_id:
                                model_name = f" ({action.model_id.name})"
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

            if action_type == 'server':
                action = request.env['ir.actions.server'].sudo().browse(
                    action_id)
                if action.exists() and action.model_id:
                    model = action.model_id.model
                    model_rules = request.env['view.access.rule'].search([
                        ('active', '=', True),
                        ('model_name', '=', model)
                    ])

                    if model_rules and not self._check_access_for_action(user, model, action_id):
                        model_name = action.model_id.name
                        action_name = action.name
                        return {
                            'type': 'ir.actions.client',
                            'tag': 'display_notification',
                            'params': {
                                'type': 'danger',
                                'message': f'Access Denied: You do not have permission to access "{action_name}" ({model_name}).',
                                'sticky': False,
                            }
                        }

            elif action_type == 'window':
                action = request.env['ir.actions.act_window'].sudo().browse(
                    action_id)
                if action.exists() and action.res_model:
                    model = action.res_model
                    model_rules = request.env['view.access.rule'].search([
                        ('active', '=', True),
                        ('model_name', '=', model)
                    ])

                    if model_rules and not self._check_access_for_action(user, model, action_id):
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

        from odoo.addons.web.controllers.action import Action
        return Action().load(action_id, additional_context)

    @http.route('/web/action/run', type='json', auth="user")
    def run_action_security(self, action_id, **kw):
        """Block running of actions - integrated with server action security"""
        user = request.env.user

        _logger.info(
            f"Action run requested: action_id={action_id}, user={user.name}")

        try:
            has_access, action_type = self._check_action_type_and_access(
                user, action_id)
            _logger.info(
                f"Access check result: has_access={has_access}, action_type={action_type}")

            # For server actions, let Odoo's built-in security handle it (integrated approach)
            # For client actions, block here
            if action_type == 'client' and not has_access:
                action_name = f"Dashboard #{action_id}"
                try:
                    action = request.env['ir.actions.client'].sudo().browse(
                        action_id)
                    if action.exists():
                        action_name = action.name
                except Exception as e:
                    _logger.error(f"Error fetching client action name: {e}")

                _logger.warning(
                    f"Access denied for client action: {action_name}")
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

        _logger.info(
            f"Delegating to original Action.run for action_id={action_id}")
        from odoo.addons.web.controllers.action import Action
        return Action().run(action_id, **kw)

    # API endpoints
    @http.route('/view_access/get_available_models', type='json', auth='user')
    def get_available_models(self):
        return request.env['view.access.model.list'].get_available_models()

    @http.route('/view_access/get_available_actions', type='json', auth='user')
    def get_available_actions(self):
        return request.env['view.access.model.list'].get_available_actions()

    @http.route('/view_access/get_available_client_actions', type='json', auth='user')
    def get_available_client_actions(self):
        return request.env['view.access.model.list'].get_available_client_actions()

    @http.route('/view_access/get_available_server_actions', type='json', auth='user')
    def get_available_server_actions(self):
        return request.env['view.access.model.list'].get_available_server_actions()

    @http.route('/view_access/get_available_menus', type='json', auth='user')
    def get_available_menus(self):
        return request.env['view.access.model.list'].get_available_menus()

    @http.route('/view_access/get_available_groups', type='json', auth='user')
    def get_available_groups(self):
        return request.env['view.access.model.list'].get_available_groups()
    
  