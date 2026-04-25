from odoo import http
from odoo.http import request
import logging
from odoo import  fields
from datetime import datetime



_logger = logging.getLogger(__name__)

# This controller validates the token from the browser against the database.

class SessionController(http.Controller):
    @http.route('/web/session/validate_custom', type='json', auth="user")
    def validate_session(self):
        user_id = request.session.uid
        current_token = request.session.get('custom_session_token')
        session_id = request.session.sid
        user_session = request.env['user.session']
        user_agent = request.httprequest.user_agent.string if request else ''
        ip_address = request.httprequest.remote_addr if request else ''
        login_period = datetime.now()
        timestamp = int(login_period.timestamp())
        device_fingerprint = user_session.generate_device_fingerprint(
            user_agent, ip_address)
        signature = user_session.generate_session_signature(
            session_id, user_id, ip_address, timestamp)


        if not current_token:
           
            return {'valid': False}

        session = user_session.sudo().search([
            ('user_id', '=', user_id),
            ('token', '=', current_token),
            ('active', '=', True)
        ], limit=1)
        
        sessions_to_update = user_session.sudo().search([
            ('user_id', '=', user_id),
            ('active', '=', True),
            ('session_id_updated', '=', False)
        ],limit=1)

        is_valid = bool(session) 
        
        if sessions_to_update and is_valid :
            
            sessions_to_update.sudo().write({
                'session_id': session_id,
                'session_id_updated': True,
                'last_session_update': fields.Datetime.now(),
                'login_time': login_period,
                'device_fingerprint': device_fingerprint,
                'signature': signature

            })
        
        return {'valid': is_valid}
