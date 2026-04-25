import logging
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
from odoo.exceptions import ValidationError, UserError, AccessError
from odoo.http import request

class ConditionalMethodMixin(models.AbstractModel):
    _name = 'conditional.method.mixin'

    def call_if_module_installed(self, module_name, method_name, *args, **kwargs):
        """Call method only if specified module is installed"""
        module = self.env['ir.module.module'].sudo().search([
            ('name', '=', module_name),
            ('state', '=', 'installed')
        ], limit=1)

        if module:
            method = getattr(self, method_name, None)
            if method and callable(method):
                return method(*args, **kwargs)
        return None

    # Convenience method for session_control specifically
    def call_session_method(self, method_name, *args, **kwargs):
        """Shortcut for session_control module methods"""
        return self.call_if_module_installed('session_control', method_name, *args, **kwargs)
