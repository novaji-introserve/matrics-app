# -*- coding: utf-8 -*-
from odoo import http
from odoo.http import request
from odoo.addons.web.controllers.home import Home

class InactivityTimeoutController(Home):
    @http.route('/web/login', type='http', auth="none", website=True, sitemap=False)
    def web_login(self, redirect=None, **kw):
        # Check if timeout parameter is present
        timeout = kw.get('timeout')
        
        # Call the original login method
        response = super(InactivityTimeoutController, self).web_login(redirect=redirect, **kw)
        
        # If timeout parameter is present, add a message
        if timeout:
            request.params['error'] = "Your session has expired due to inactivity."
        
        return response

    @http.route('/web/inactivity/params', type='json', auth="user")
    def get_inactivity_params(self):
        """Fetch inactivity timeout parameters from system parameters"""
        # Use sudo() with a fresh environment to avoid caching issues
        IrConfigParam = request.env['ir.config_parameter'].sudo().with_context(prefetch_fields=False)
        
        try:
            # Force reload of parameters from database
            request.env.cr.execute("SELECT key, value FROM ir_config_parameter WHERE key IN %s", 
                              (('inactivity_timeout.timeout', 'inactivity_timeout.warning'),))
            params = dict(request.env.cr.fetchall())
            
            # Get timeout values with defaults
            timeout = int(params.get('inactivity_timeout.timeout', 300))
            warning = int(params.get('inactivity_timeout.warning', 60))
            
            return {
                'inactivity_timeout': timeout,
                'warning_timeout': warning,
            }
        except Exception as e:
            request.env.cr.rollback()
            return {'error': str(e)}