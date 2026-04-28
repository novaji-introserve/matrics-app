from odoo import http
from odoo.http import request
from odoo.addons.web.controllers.home import Home
from odoo.addons.web.controllers.dataset import DataSet
from odoo.addons.web.controllers.action import Action
from odoo.exceptions import AccessError
import logging

_logger = logging.getLogger(__name__)


class ViewSecurityController(http.Controller):

    def _check_access(self, user):
        """Check if user has access to restricted management views"""
        allowed_groups = [
            'rule_book.group_compliance_manager_',
            'rule_book.group_chief_compliance_officer_'
        ]
        return any(user.has_group(group) for group in allowed_groups)

    @http.route('/web', type='http', auth="user", website=False)
    def web_client_security(self, s_action=None, **kw):
        """Block access to specific management views via URLs"""
        action_id = kw.get('action')
        user = request.env.user

        # Block specific action IDs for Users and Employee management views
        restricted_actions = [70, 159]

        if action_id:
            try:
                action_id = int(action_id)
                if action_id in restricted_actions and not self._check_access(user):
                    return request.render('web.access_denied', {
                        'message': 'You are not authorized to access this management view.'
                    })
            except (ValueError, TypeError):
                pass

        # Call original web client
        return Home().web_client(s_action, **kw)

    @http.route('/web/dataset/call_kw/<path:path>', type='json', auth="user")
    def call_kw_security(self, model, method, args, kwargs, path=None):
        """Block only main management view access"""
        user = request.env.user

        restricted_models = ['res.users', 'hr.employee']

        if (model in restricted_models and not self._check_access(user)):

            # Check if this is a main view access (not a related field or popup)
            context = kwargs.get('context', {})

            # Block if it's a main view operation
            if method == 'get_views' and not context.get('from_related_field'):
                raise AccessError(f'Access denied to {model} management views')

            # Block tree view data loading for main views
            if method == 'web_search_read' and not context.get('from_related_field'):
                raise AccessError(f'Access denied to {model} tree view')

        return DataSet().call_kw(model, method, args, kwargs, path)

    @http.route('/web/action/load', type='json', auth="user")
    def load_action_security(self, action_id, additional_context=None):
        """Block loading of specific management actions"""
        user = request.env.user
        restricted_actions = [70, 159]  # Users and Employee management actions

        if action_id in restricted_actions and not self._check_access(user):
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'type': 'danger',
                    'message': 'Access Denied: You do not have permission to access this management view.',
                    'sticky': True,
                }
            }

        return Action().load(action_id, additional_context)
