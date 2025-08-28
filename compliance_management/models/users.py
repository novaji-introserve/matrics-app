from odoo import _, api, fields, models
import logging
from odoo import models
from odoo.http import request, SessionExpiredException
from odoo.tools import consteq
import time
import hashlib


_logger = logging.getLogger(__name__)


class Users(models.Model):
    _inherit = 'res.users'

    branches_id = fields.Many2many(
        'res.branch', 'res_branch_users_rel', 'user_id', 'branch_id', string='Branches')
    default_branch_id = fields.Many2one(
        comodel_name='res.branch', string='Default Branch')
    
    """
    @api.model
    def create(self, vals):
        #Override create to handle branch assignments
        user = super(Users, self).create(vals)
        return user
    
    def write(self, vals):
        #Override write to handle branch updates
        result = super(Users, self).write(vals)
        return result
    """


class IrHttp(models.AbstractModel):
    _inherit = 'ir.http'

    @classmethod
    def _authenticate(cls, endpoint):
        """
        Enhanced authentication with session hijacking protection
        """
        # Call parent authentication first (without conflicts)
        parent_class = super(IrHttp, cls)
        if hasattr(parent_class, '_authenticate'):
            try:
                parent_class._authenticate(endpoint)
            except Exception as e:
                _logger.error("Error calling parent _authenticate: %s", e)
                raise

        # Our enhanced session validation for authenticated users
        if request.session.uid and hasattr(endpoint, 'routing') and endpoint.routing.get('auth') == 'user':
            cls._validate_session_security()

    @classmethod
    def _validate_session_security(cls):
        """
        Multi-layered session security validation
        """
        session = request.session

        # 1. Basic session token validation
        stored_token = session.get('session_token')
        if not stored_token:
            _logger.warning("Session missing token for UID %s", session.uid)
            cls._invalidate_session("missing_token")
            return

        # 2. Validate the stored session token
        user = request.env['res.users'].browse(session.uid)
        expected_token = user._compute_session_token(session.sid)

        if not consteq(stored_token, expected_token):
            _logger.warning("Session token mismatch for UID %s", session.uid)
            cls._invalidate_session("token_mismatch")
            return

        # 3. Session age validation (prevent indefinite sessions)
        max_session_age = 8 * 60 * 60  # 8 hours in seconds
        login_time = session.get('login_timestamp')

        if login_time and (time.time() - login_time) > max_session_age:
            _logger.info("Session expired due to age for UID %s", session.uid)
            cls._invalidate_session("session_expired")
            return

        _logger.debug("Session validation passed for UID %s", session.uid)

    @classmethod
    def _invalidate_session(cls, reason):
        """
        Securely invalidate a session and log the security event
        """
        session = request.session
        _logger.warning(
            "Session invalidated for UID %s, reason: %s, IP: %s",
            session.uid, reason, request.httprequest.environ.get('REMOTE_ADDR')
        )
        session.logout(keep_db=True)
        raise SessionExpiredException(f"Session invalid: {reason}")


class Session(models.TransientModel):
    """
    Override session to store security metadata on login
    """
    _name = 'http.session.security'
    _description = 'Session Security Metadata'

    def enhance_session_on_login(self, session):
        """
        Store security metadata when user logs in
        """
        if request and session:
            session['login_timestamp'] = time.time()
            session['login_ip'] = request.httprequest.environ.get(
                'REMOTE_ADDR')
            session['login_user_agent_hash'] = hashlib.sha256(
                request.httprequest.environ.get('HTTP_USER_AGENT', '').encode()
            ).hexdigest()


# class IrHttp(models.AbstractModel):
#     _inherit = 'ir.http'

#     @classmethod
#     def _authenticate(cls, endpoint):
#         """
#         Override the main authentication method to add a strict session token validation
#         on every authenticated request.
#         """
#         _logger.info(
#             "CUSTOM _authenticate method called with endpoint: %s", endpoint)

#         # Check if the parent class has the _authenticate method
#         parent_class = super(IrHttp, cls)
#         if hasattr(parent_class, '_authenticate'):
#             _logger.info(
#                 "Parent class has _authenticate method, calling it...")
#             try:
#                 parent_class._authenticate(endpoint)
#                 _logger.info(
#                     "Parent _authenticate method completed successfully")
#             except Exception as e:
#                 _logger.error("Error calling parent _authenticate: %s", e)
#                 raise
#         else:
#             _logger.warning("Parent class does not have _authenticate method")
#             # If there's no parent method, we'll do minimal auth checking ourselves
#             if hasattr(endpoint, 'routing'):
#                 auth = endpoint.routing.get('auth', 'user')
#                 if auth == 'user' and not request.session.uid:
#                     raise SessionExpiredException("Authentication required")

#         # After standard authentication, if a user is logged in, perform our strict check.
#         # This applies only to routes that require a fully authenticated user.
#         if request.session.uid and hasattr(endpoint, 'routing') and endpoint.routing.get('auth') == 'user':
#             _logger.info(
#                 "Performing session token validation for user %s", request.session.uid)

#             stored_token = request.session.get('session_token')

#             if not stored_token:
#                 _logger.warning(
#                     "Session for UID %s is missing a session_token. Invalidating session.",
#                     request.session.uid
#                 )
#                 request.session.logout(keep_db=True)
#                 raise SessionExpiredException(
#                     "Session is invalid (missing token).")

#             # Re-compute the expected token for the current user and session ID.
#             user = request.env['res.users'].browse(request.session.uid)
#             expected_token = user._compute_session_token(request.session.sid)

#             # Compare the stored token with the expected one using a constant-time
#             # comparison function to prevent timing attacks.
#             if not consteq(stored_token, expected_token):
#                 _logger.warning(
#                     "Potential session hijacking attempt for UID %s! "
#                     "Session token mismatch. Invalidating session.",
#                     request.session.uid
#                 )
#                 # The token does not match! Invalidate the session immediately.
#                 request.session.logout(keep_db=True)
#                 raise SessionExpiredException(
#                     "Session token is invalid. Please log in again.")

#             _logger.info(
#                 "Session token validation passed for user %s", request.session.uid)
