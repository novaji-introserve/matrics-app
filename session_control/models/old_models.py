from odoo import api, fields, models
from odoo.http import request
import uuid
import logging
import json
from datetime import datetime
from odoo.http import request, Session
from datetime import datetime, timedelta
import odoo
import threading
import time


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
        # *** CHANGE: Set flag to update session_id on next request
        self['needs_session_id_update'] = True

        # This is essential to ensure the session is saved.
        self.modified = True

        _logger.info("SESSION_CONTROL: Stored token %s... on Odoo session for user_id: %s and marked for saving.",
                     token[:8], self.uid)

    return result_uid


# *** NEW: Store original __call__ method from Session
_original_call = Session.__call__

def _new_call(self, *args, **kwargs):
    """
    Patch the session __call__ method to update session_id on subsequent requests
    """
    # Call original method first
    result = _original_call(self, *args, **kwargs)
    
    # Check if we need to update session_id (this runs on EVERY request)
    if (hasattr(self, 'uid') and self.uid and 
        self.get('needs_session_id_update') and 
        self.get('custom_session_token')):
        
        try:
            token = self.get('custom_session_token')
            current_session_id = self.sid
            
            # Update the session_id in the database
            if hasattr(request, 'env') and request.env:
                user_session = request.env['user.session'].sudo().search([
                    ('token', '=', token),
                    ('active', '=', True)
                ], limit=1)
                
                if user_session and user_session.session_id != current_session_id:
                    old_session_id = user_session.session_id
                    user_session.write({'session_id': current_session_id})
                    _logger.info(
                        "SESSION_CONTROL: Updated session_id from %s to %s for token %s...", 
                        old_session_id, current_session_id, token[:8]
                    )
                    
                # Remove the flag after successful update
                del self['needs_session_id_update']
                self.modified = True
                
        except Exception as e:
            _logger.warning(
                "SESSION_CONTROL: Failed to update session_id: %s", e)
    
    return result


# Apply the patches
Session.authenticate = _new_authenticate
Session.__call__ = _new_call


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
    session_id = fields.Char(string='Session ID')
    active = fields.Boolean(string='Active', default=True, index=True)
    session_id_updated = fields.Boolean(
        string='Session ID Updated', default=False, index=True)
    last_session_update = fields.Datetime(string='Last Session Update')


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
        session_id = request.session.sid

        self.create({
            'user_id': user_id,
            'token': token,
            'user_agent': user_agent,
            'ip_address': ip_address,
            'session_id': session_id,
            'active': True,
        })

        _logger.info("SESSION_CONTROL: Created session record with initial session_id: %s for token %s...", 
                     session_id, token[:8])

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