from odoo import api, fields, models
from odoo.http import request
import uuid
import logging
import json
import hashlib
import time
import hmac
from datetime import datetime
from odoo.http import request, Session
from datetime import datetime, timedelta
from odoo.http import request

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
    session_id = fields.Char(string='Session ID')
    active = fields.Boolean(string='Active', default=True, index=True)
    session_id_updated = fields.Boolean(
        string='Session ID Updated', default=False, index=True)
    last_session_update = fields.Datetime(string='Last Session Update')
    signature = fields.Char(string='Session Signature', index=True)
    device_fingerprint = fields.Char(string='Device Fingerprint', index=True)

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

        # Calculate device fingerprint
        device_fingerprint = self.generate_device_fingerprint(
            user_agent, ip_address)

        # Calculate timestamp for signature
        timestamp = int(datetime.now().timestamp())

        session = self.create({
            'user_id': user_id,
            'token': token,
            'user_agent': user_agent,
            'ip_address': ip_address,
            'session_id': session_id,
            'active': True,
            'device_fingerprint': device_fingerprint,

        })
        signature = self.generate_session_signature(
            session_id, user_id, ip_address, timestamp)
        session.write({'signature': signature})

        return token

    @api.model
    def get_current_session_info(self):
        """
        Returns session_id and user_id of the currently logged in user

        """
        try:
            # Check if we have a request context
            if not request or not hasattr(request, 'session'):
                return {
                    'user_id': False,
                    'session_id': False,
                    'token': False,
                    'is_valid': False,
                    'error': 'No request context available'
                }

            # Get current user and token from session
            current_user_id = request.session.uid
            current_token = request.session.get('custom_session_token')
            current_session_id = request.session.sid

            if not current_user_id or not current_token:
                return {
                    'user_id': current_user_id or False,
                    'session_id': current_session_id or False,
                    'token': False,
                    'is_valid': False,
                    'error': 'User not logged in or no session token'
                }

            # Find the session record in database
            user_session = self.sudo().search([
                ('user_id', '=', current_user_id),
                ('token', '=', current_token),
                ('active', '=', True)
            ], limit=1)

            if user_session:
                return {
                    'user_id': current_user_id,
                    'session_id': user_session.session_id,
                    'token': current_token,
                    'is_valid': True,
                    'login_time': user_session.login_time,
                    'ip_address': user_session.ip_address,
                    'user_agent': user_session.user_agent
                }
            else:
                return {
                    'user_id': current_user_id,
                    'session_id': current_session_id,
                    'token': current_token,
                    'is_valid': False,
                    'error': 'Session record not found in database'
                }

        except Exception as e:
            _logger.warning(
                "SESSION_CONTROL: Error getting current session info: %s", e)
            return {
                'user_id': False,
                'session_id': False,
                'token': False,
                'is_valid': False,
                'error': str(e)
            }

    @api.model
    def get_user_by_session_id(self, session_id=None):
        """
        Check if session_id exists in the table and return the user_id
        """
        try:
            # If no session_id provided, get current session_id
            if not session_id:
                if not request or not hasattr(request, 'session'):
                    return {
                        'exists': False,
                        'user_id': False,
                        'session_record': False,
                        'is_active': False,
                        'error': 'No session_id provided and no request context'
                    }
                session_id = request.session.sid

            # Search for session_id in the table
            user_session = self.sudo().search([
                ('session_id', '=', session_id)
            ], limit=1)

            if user_session:
                return {
                    'exists': True,
                    'user_id': user_session.user_id.id,
                    'session_record': user_session,
                    'is_active': user_session.active,
                    'token': user_session.token,
                    'login_time': user_session.login_time,
                    'ip_address': user_session.ip_address,
                    'user_agent': user_session.user_agent
                }
            else:
                return {
                    'exists': False,
                    'user_id': False,
                    'session_record': False,
                    'is_active': False,
                    'error': f'Session ID {session_id} not found in table'
                }

        except Exception as e:
            _logger.warning(
                "SESSION_CONTROL: Error checking session_id %s: %s", session_id, e)
            return {
                'exists': False,
                'user_id': False,
                'session_record': False,
                'is_active': False,
                'error': str(e)
            }

    @api.model
    def validate_current_session_in_table(self):
        """
        Validate that the current session_id exists in the table and return user_id
        """
        try:
            # Get current session info
            session_info = self.get_current_session_info()
            current_session_id = session_info.get('session_id', False)
            current_user_id = session_info.get('user_id', False)

            if not current_session_id:
                return {
                    'valid': False,
                    'user_id': False,
                    'current_session_matches_table': False,
                    'error': 'No current session_id found'
                }

            # Check if current session_id exists in table
            table_result = self.get_user_by_session_id(current_session_id)

            if table_result['exists']:
                table_user_id = table_result['user_id']
                matches = (current_user_id ==
                           table_user_id and table_result['is_active'])

                return {
                    'valid': table_result['is_active'],
                    'user_id': table_user_id,
                    'current_session_matches_table': matches,
                    'table_session_active': table_result['is_active'],
                    'session_record': table_result['session_record']
                }
            else:
                return {
                    'valid': False,
                    'user_id': False,
                    'current_session_matches_table': False,
                    'error': 'Current session_id not found in table'
                }

        except Exception as e:
            _logger.warning(
                "SESSION_CONTROL: Error validating current session: %s", e)
            return {
                'valid': False,
                'user_id': False,
                'current_session_matches_table': False,
                'error': str(e)
            }

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

    def generate_session_signature(self, session_id, user_id, ip_address, timestamp):
        """Generate tamper-proof session signature"""
        secret_key = self.env['ir.config_parameter'].sudo(
        ).get_param('database.secret')
        message = f"{session_id}:{user_id}:{ip_address}:{timestamp}"
        return hmac.new(secret_key.encode(), message.encode(), hashlib.sha256).hexdigest()

    def validate_session_signature(self, session_record, current_ip):
        """Validate session hasn't been tampered with"""
        expected_sig = self.generate_session_signature(
            session_record.session_id,
            session_record.user_id.id,
            session_record.ip_address,
            int(session_record.login_time.timestamp())
        )
        return hmac.compare_digest(session_record.signature, expected_sig)

    def validate_ip_address(self, session_record, current_ip):
        """Validate request comes from original IP"""
        if session_record.ip_address != current_ip:
            # Log security event
            _logger.warning(f"IP_MISMATCH', {session_record}, {current_ip}")
            return False

        return True

    def generate_device_fingerprint(self, user_agent, ip_address):
        """Create unique device identifier"""
        fingerprint_data = f"{user_agent}:{ip_address}"
        return hashlib.sha256(fingerprint_data.encode()).hexdigest()

    def validate_device_fingerprint(self, session_record, current_fingerprint):
        """Ensure same device is being used"""
        return session_record.device_fingerprint == current_fingerprint

    def validate_current_session_secure(self):
        """
        ENHANCED session validation with multiple security layers
        """
        try:
            # Get current session info (your existing method)
            session_info = self.get_current_session_info()
            current_session_id = session_info.get('session_id', False)
            current_user_id = session_info.get('user_id', False)

            if not current_session_id:
                return {'valid': False, 'error': 'No current session_id found'}

            # Get current request details
            current_user_agent = request.httprequest.user_agent.string if request else ''
            current_ip = request.httprequest.remote_addr if request else ''

            # Find session record (your existing logic)
            table_result = self.get_user_by_session_id(current_session_id)

            if not table_result['exists']:
                return {'valid': False, 'error': 'Session not found in table'}

            session_record = table_result['session_record']

            # ENHANCED SECURITY VALIDATIONS

            # 1. Validate session signature
            if not self.validate_session_signature(session_record, current_ip):
                _logger.warning(f"SIGNATURE_MISMATCH {session_record}")
                return {'valid': False, 'error': 'Invalid session signature'}

            # 2. Validate IP address
            if not self.validate_ip_address(session_record, current_ip):
                _logger.warning(f"IP_MISMATCH {session_record}")
                return {'valid': False, 'error': 'IP address mismatch'}

            # 3. Validate device fingerprint
            current_fingerprint = self.generate_device_fingerprint(
                current_user_agent, current_ip)
            if not self.validate_device_fingerprint(session_record, current_fingerprint):
                _logger.warning(f"DEVICE_MISMATCH {session_record}")
                return {'valid': False, 'error': 'Device fingerprint mismatch'}

            # All validations passed
            return {
                'valid': True,
                'user_id': current_user_id,
                'session_id': current_session_id,
                'session_record': session_record,
            }

        except Exception as e:
            _logger.error("Enhanced session validation error: %s", e)
            return {'valid': False, 'error': 'Session validation failed'}
