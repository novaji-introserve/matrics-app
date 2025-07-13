from odoo import api, fields, models
from odoo.http import request
import uuid
import logging
import json
from datetime import datetime
from odoo.http import request, Session
from datetime import datetime, timedelta
_logger = logging.getLogger(__name__)
 

# Store the original, unpatched authenticate method
_original_authenticate = Session.authenticate


def _new_authenticate(self, *args, **kwargs):
    """
    This is a patch for the core odoo.http.Session.authenticate method.
    It uses *args and **kwargs to transparently handle any call signature.
    """

    # First, let the original Odoo method do its work with the exact arguments it received.
    try:
        result_uid = _original_authenticate(self, *args, **kwargs)
    except Exception as e:
        _logger.warning(
            "SESSION_CONTROL: Original authentication failed. Exception: %s", e)
        # Re-raise the exception to let Odoo handle the login failure.
        raise

    # If the original method succeeded, self.uid will be set.
    if self.uid:
        _logger.info(
            "SESSION_CONTROL: Core auth successful for user_id: %s. Registering token.", self.uid)

        # Execute our custom logic
        token = request.env['user.session'].sudo().register_session(self.uid)

        # 'self' here is the session object itself.
        self['custom_session_token'] = token

        # This is essential to ensure the session is saved.
        self.modified = True

        _logger.info("SESSION_CONTROL: Stored token %s... on Odoo session for user_id: %s and marked for saving.",
                     token[:8], self.uid)

    return result_uid


# Apply the patch
Session.authenticate = _new_authenticate

class UserSession(models.Model):
    _name = 'user.session'
    _description = 'User Session Control'
    _order = 'login_time desc'

    user_id = fields.Many2one(
        'res.users', string='User', required=True, ondelete='cascade', index=True)
    token = fields.Char(string='Session Token', required=True, index=True)
    login_time = fields.Datetime(
        string='Login Time', default=fields.Datetime.now, required=True)
    user_agent = fields.Char(string='User Agent')
    ip_address = fields.Char(string='IP Address')
    active = fields.Boolean(string='Active', default=True, index=True)

    @api.model
    def register_session(self, user_id):
        _logger.info(
            "SESSION_CONTROL: Registering new session for user_id: %s", user_id)

        # Deactivate all existing active sessions for this user
        existing_sessions = self.search(
            [('user_id', '=', user_id), ('active', '=', True)])
        if existing_sessions:
            _logger.info("SESSION_CONTROL: Found %s active session(s) to deactivate for user_id: %s", len(
                existing_sessions), user_id)
            existing_sessions.write({'active': False})
        else:
            _logger.info(
                "SESSION_CONTROL: No previous active sessions found for user_id: %s", user_id)

        # Create a new session record
        token = str(uuid.uuid4())
        user_agent = request.httprequest.user_agent.string if request else ''
        ip_address = request.httprequest.remote_addr if request else ''

        self.create({
            'user_id': user_id,
            'token': token,
            'user_agent': user_agent,
            'ip_address': ip_address,
            'active': True,
        })

        return token
    
    @api.model
    def _cron_cleanup_sessions(self):
        """
        This method is called by a cron job to delete old, inactive session records.
        """
        _logger.info("Starting user session cleanup cron job...")

        # Get the retention period from System Parameters for flexibility. Default to 30 days.
        retention_days = int(self.env['ir.config_parameter'].sudo().get_param(
            'session_control.session_retention_days', 7
        ))

        
        cutoff_date = datetime.now() - timedelta(days=retention_days)

        domain = [
            ('active', '=', False),
            ('write_date', '<', cutoff_date)
        ]

        sessions_to_delete = self.search(domain)

        if sessions_to_delete:
            count = len(sessions_to_delete)
            _logger.info(
                "Found %s inactive user sessions older than %s days. Deleting now.",
                count,
                retention_days
            )
            sessions_to_delete.unlink()
            _logger.info("%s old sessions successfully deleted.", count)
        else:
            _logger.info("No old inactive user sessions found to delete.")

        return True

