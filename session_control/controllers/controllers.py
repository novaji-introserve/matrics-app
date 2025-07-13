from odoo import http
from odoo.http import request
import logging

_logger = logging.getLogger(__name__)

# This controller validates the token from the browser against the database.

class SessionController(http.Controller):
    @http.route('/web/session/validate_custom', type='json', auth="user")
    def validate_session(self):
        user_id = request.session.uid
        current_token = request.session.get('custom_session_token')

        if not current_token:
           
            return {'valid': False}

        session = request.env['user.session'].sudo().search([
            ('user_id', '=', user_id),
            ('token', '=', current_token),
            ('active', '=', True)
        ], limit=1)

        is_valid = bool(session)
        
        return {'valid': is_valid}
