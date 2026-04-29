"""Microsoft login"""

import json, logging, requests
from odoo.http import request
from odoo import api, models
from odoo import exceptions
from odoo.addons import base
from odoo.addons.auth_signup.models.res_users import SignupError

base.models.res_users.USER_PRIVATE_FIELDS.append('oauth_access_token')
_logger = logging.getLogger(__name__)

try:
    import jwt
except ImportError:
    _logger.warning(
        "Login with Microsoft account won't be available.Please install PyJWT "
        "python library, ")
    jwt = None


class ResUsers(models.Model):
    """This class is used to inheriting the res.users and provides the oauth
    access"""
    _inherit = 'res.users'

    @api.model
    def _auth_oauth_rpc(self, endpoint, access_token):
        """This is used to pass the response of sign in."""
        if not endpoint:
            return super()._auth_oauth_rpc(endpoint, access_token)
        
        try:
            # Make the request
            response = requests.get(endpoint, params={'access_token': access_token})
            
            # Log the response for debugging
            # _logger.info("OAuth API response status: %s", response.status_code)
            # _logger.info("OAuth API response content: %s", response.text[:1000])  # Log first 1000 chars to avoid huge logs
            
            # Check if the response is valid before trying to parse it
            if response.status_code != 200:
                _logger.error("OAuth API request failed with status code: %s", response.status_code)
                return {'error': 'invalid_response', 'error_description': f'API request failed with status code {response.status_code}'}
            
            # Check if content is empty
            if not response.text.strip():
                _logger.error("OAuth API returned empty response")
                return {'error': 'empty_response', 'error_description': 'API returned empty response'}
            
            # Try to parse the JSON
            return response.json()
        except json.decoder.JSONDecodeError as e:
            _logger.error("JSONDecodeError in _auth_oauth_rpc: %s, Response content: %s", e, response.text[:1000])
            return {'error': 'invalid_json', 'error_description': 'API did not return valid JSON'}
        except requests.exceptions.RequestException as e:
            _logger.error("RequestException in _auth_oauth_rpc: %s", e)
            return {'error': 'request_failed', 'error_description': str(e)}
        except Exception as e:
            _logger.error("Unexpected error in _auth_oauth_rpc: %s", e)
            return {'error': 'unexpected_error', 'error_description': str(e)}

    @api.model
    def _auth_oauth_code_validate(self, provider, code):
        """ Return the validation data corresponding to the access token """
        auth_oauth_provider = self.env['auth.oauth.provider'].browse(provider)
        base_url = self.env['ir.config_parameter'].sudo().get_param('web.app.url')
        # Ensure it's using HTTPS
        if base_url.startswith('http://'):
            base_url = base_url.replace('http://', 'https://')
        # Use this for the redirect URI
        redirect_uri = base_url + '/auth_oauth/signin'
        req_params = dict(
            client_id=auth_oauth_provider.client_id,
            client_secret=auth_oauth_provider.client_secret_id,
            grant_type='authorization_code',
            code=code,
            # redirect_uri=request.httprequest.url_root + 'auth_oauth/signin',
            redirect_uri=redirect_uri,
        )
        headers = {'Accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded'}
        
        try:
            response = requests.post(auth_oauth_provider.validation_endpoint,
                                    headers=headers, data=req_params)
            # _logger.info("Token endpoint response status: %s", response.status_code)
            # _logger.info("Token endpoint response content: %s", response.text[:1000])
            
            if response.status_code != 200:
                _logger.error("Token endpoint request failed with status code: %s", response.status_code)
                raise Exception(f"Token endpoint request failed with status code: {response.status_code}")
                
            token_info = response.json()
        except json.decoder.JSONDecodeError as e:
            _logger.error("JSONDecodeError in token endpoint: %s, Response content: %s", e, response.text[:1000])
            raise Exception(f"Invalid JSON response from token endpoint: {e}")
        except Exception as e:
            _logger.critical(f"token endpoint error: {e} ... request params {req_params} .... auth provider {auth_oauth_provider}")
            raise
        
        if token_info.get("error"):
            _logger.critical(f"token info {token_info} ... request params {req_params} .... auth provider {auth_oauth_provider}")
            raise Exception(token_info['error'])
            
        access_token = token_info.get('access_token')
        validation = {
            'access_token': access_token
        }
        if token_info.get('id_token'):
            if not jwt:
                raise exceptions.AccessDenied()
            try:
                data = jwt.decode(token_info['id_token'], options={"verify_signature": False, "verify_aud": False, "verify_iat": False, "verify_exp": False, "verify_nbf": False, "verify_iss": False, "verify_sub": False, "verify_jti": False, "verify_at_hash": False}, algorithms=["RS256"])
                # _logger.info("JWT token successfully decoded: %s", list(data.keys()))
            except Exception as e:
                _logger.error("Error decoding JWT token: %s", e)
                raise
        else:
            data = self._auth_oauth_rpc(auth_oauth_provider.data_endpoint,
                                        access_token)
            
        validation.update(data)
        return validation

    @api.model
    def _find_user_case_insensitive(self, email):
        """Find user by email with case-insensitive search"""
        if not email:
            return False
        
        # First try exact match (for performance)
        user = self.search([('login', '=', email)], limit=1)
        if user:
            return user
            
        # If no exact match, try case-insensitive search
        # Use ilike with exact match to avoid partial matches
        user = self.search([('login', 'ilike', email)], limit=1)
        if user:
            # Double-check that it's actually the same email (case-insensitive)
            if user.login.lower() == email.lower():
                return user
        
        return False

    @api.model
    def _auth_oauth_signin(self, provider, validation, params):
        """ 
        NEW LOGIC: Only allow login for pre-existing users
        - Search for user case-insensitively 
        - If user exists: proceed with login
        - If user doesn't exist: block login with admin message
        - NO automatic user creation
        """
        user_email = str(validation.get('email', '')).strip()
        
        if not user_email:
            _logger.error("No email provided in Microsoft OAuth validation")
            raise exceptions.AccessDenied("Authentication failed: No email provided by Microsoft")
        
        # Search for existing user (case-insensitive)
        user = self._find_user_case_insensitive(user_email)
        
        if not user:
            # User doesn't exist - block login and show admin message
            _logger.warning(f"Microsoft login blocked for non-existing user: {user_email}")
            raise exceptions.AccessDenied(
                "Access denied. Your account has not been set up yet. "
                "Please contact your administrator to get onboarded to the system."
            )
        
        _logger.info(f"Microsoft login successful for existing user: {user.login}")
        
        # User exists - update OAuth info and proceed
        user.write({
            'oauth_provider_id': provider,
            'oauth_uid': validation.get('user_id'),
            'oauth_access_token': params.get('access_token'),
        })
        
        # Copy template user settings if configured (optional feature)
        provider_id = self.env['auth.oauth.provider'].sudo().browse(provider)
        if hasattr(provider_id, 'template_user_id') and provider_id.template_user_id:
            # Only update if user doesn't already have these settings
            if not user.groups_id:
                user.groups_id = [(6, 0, provider_id.template_user_id.groups_id.ids)]
            if hasattr(user, 'is_contractor') and not user.is_contractor:
                user.is_contractor = provider_id.template_user_id.is_contractor
            if hasattr(user, 'contractor') and not user.contractor:
                user.contractor = provider_id.template_user_id.contractor
        
        return user.login

    @api.model
    def auth_oauth(self, provider, params):
        """This is used to take the access token to sign in with the user account."""
        try:
            if params.get('code'):
                validation = self._auth_oauth_code_validate(provider,
                                                            params['code'])
                access_token = validation.pop('access_token')
                params['access_token'] = access_token
            else:
                access_token = params.get('access_token')
                validation = self._auth_oauth_validate(provider, access_token)
                # _logger.info("OAuth validation response: %s", validation)

            # Check if validation contains an error
            if validation.get('error'):
                _logger.error("OAuth validation error: %s - %s", 
                            validation.get('error'), 
                            validation.get('error_description', 'No description'))
                raise exceptions.AccessDenied(validation.get('error_description', 'Authentication failed'))

            if not validation.get('user_id'):
                # Try alternative keys
                if validation.get('id'):
                    validation['user_id'] = validation['id']
                elif validation.get('oid'):
                    validation['user_id'] = validation['oid']
                elif validation.get('unique_name'):
                    validation['user_id'] = validation.get('unique_name')
                elif validation.get('email'):
                    validation['user_id'] = validation.get('email')
                else:
                    _logger.critical("Cannot find user_id in token: %s", validation)
                    raise exceptions.AccessDenied()
                    
            # Ensure email is present for user lookup
            if not validation.get('email') and validation.get('unique_name'):
                if '@' in validation['unique_name']:
                    validation['email'] = validation['unique_name']
                    
            login = self._auth_oauth_signin(provider, validation, params)
            if not login:
                raise exceptions.AccessDenied()
            if provider and params:
                return (self.env.cr.dbname, login, access_token)
        except Exception as e:
            _logger.error("Exception in auth_oauth: %s", e, exc_info=True)
            raise
            
        return super(ResUsers, self).auth_oauth(provider, params)