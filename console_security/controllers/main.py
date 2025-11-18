# -*- coding: utf-8 -*-
from odoo import http
from odoo.http import request
import logging
import uuid

_logger = logging.getLogger(__name__)


class ConsoleSecurityController(http.Controller):
    
    @http.route('/web/console_security/params', type='json', auth="user")
    def get_console_security_params(self):
        """Fetch console security parameters from system parameters and whether user is support."""
        try:
            IrConfig = request.env['ir.config_parameter'].sudo().with_context(prefetch_fields=False)

            # Fetch parameters
            params = {
                'enabled': IrConfig.get_param('console_security.enabled', 'False') == 'True',
                'mode': IrConfig.get_param('console_security.mode', 'off'),
                'debug_key': IrConfig.get_param('console_security.debug_key', ''),
                'support_group': IrConfig.get_param('console_security.support_group', 'base.group_system'),
            }

            # Determine if current user is in support group
            is_support = False
            try:
                if params['support_group']:
                    is_support = request.env.user.has_group(params['support_group'])
            except Exception as eg:
                _logger.warning('console_security: has_group check failed: %s', eg)

            return {
                'enabled': params['enabled'],
                'mode': params['mode'],
                'debug_key': params['debug_key'],
                'is_support': is_support,
            }
        except Exception as e:
            _logger.warning('Failed to load console security params: %s', e)
            request.env.cr.rollback()
            return {
                'enabled': False,
                'mode': 'off',
                'debug_key': '',
                'is_support': False,
                'error': str(e)
            }

    @http.route('/web/console_security/log', type='json', auth="user")
    def log_console_security_error(self, ref=None, message=None, stack=None, meta=None):
        """Log a client-side error with a reference id; always return the ref back to caller."""
        try:
            ref_id = ref or str(uuid.uuid4())[:8]
            user = request.env.user
            _logger.error('CLIENT_ERROR [%s] user=%s(%s) message=%s stack=%s meta=%s',
                          ref_id, user.login, user.id, message, stack, meta)
            return {'ok': True, 'ref': ref_id}
        except Exception as e:
            _logger.error('CLIENT_ERROR logging failed: %s', e)
            return {'ok': False, 'ref': ref or ''}




