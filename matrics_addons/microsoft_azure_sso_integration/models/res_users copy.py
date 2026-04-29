"""Microsoft login"""
# -*- coding: utf-8 -*-
#############################################################################
#
#    Cybrosys Technologies Pvt. Ltd.
#
#    Copyright (C) 2023-TODAY Cybrosys Technologies(<https://www.cybrosys.com>)
#    Author: Cybrosys Techno Solutions(<https://www.cybrosys.com>)
#
#    You can modify it under the terms of the GNU LESSER
#    GENERAL PUBLIC LICENSE (LGPL v3), Version 3.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU LESSER GENERAL PUBLIC LICENSE (LGPL v3) for more details.
#
#    You should have received a copy of the GNU LESSER GENERAL PUBLIC LICENSE
#    (LGPL v3) along with this program.
#    If not, see <http://www.gnu.org/licenses/>.
#
#############################################################################
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
            response = requests.get(
                endpoint, params={'access_token': access_token})

            # Log the response for debugging
            _logger.info("OAuth API response status: %s", response.status_code)
            # Log first 1000 chars to avoid huge logs
            _logger.info("OAuth API response content: %s",
                         response.text[:1000])

            # Check if the response is valid before trying to parse it
            if response.status_code != 200:
                _logger.critical(
                    "OAuth API request failed with status code: %s", response.status_code)
                return {'error': 'invalid_response', 'error_description': f'API request failed with status code {response.status_code}'}

            # Check if content is empty
            if not response.text.strip():
                _logger.critical("OAuth API returned empty response")
                return {'error': 'empty_response', 'error_description': 'API returned empty response'}

            # Try to parse the JSON
            return response.json()
        except json.decoder.JSONDecodeError as e:
            _logger.critical(
                "JSONDecodeError in _auth_oauth_rpc: %s, Response content: %s", e, response.text[:1000])
            return {'error': 'invalid_json', 'error_description': 'API did not return valid JSON'}
        except requests.exceptions.RequestException as e:
            _logger.critical("RequestException in _auth_oauth_rpc: %s", e)
            return {'error': 'request_failed', 'error_description': str(e)}
        except Exception as e:
            _logger.critical("Unexpected error in _auth_oauth_rpc: %s", e)
            return {'error': 'unexpected_error', 'error_description': str(e)}

    @api.model
    def _auth_oauth_code_validate(self, provider, code):
        """ Return the validation data corresponding to the access token """
        auth_oauth_provider = self.env['auth.oauth.provider'].browse(provider)
        base_url = self.env['ir.config_parameter'].sudo(
        ).get_param('web.base.url')
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
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/x-www-form-urlencoded'}

        try:
            response = requests.post(auth_oauth_provider.validation_endpoint,
                                     headers=headers, data=req_params)
            _logger.info("Token endpoint response status: %s",
                         response.status_code)
            _logger.info("Token endpoint response content: %s",
                         response.text[:1000])

            if response.status_code != 200:
                _logger.critical(
                    "Token endpoint request failed with status code: %s", response.status_code)
                raise Exception(
                    f"Token endpoint request failed with status code: {response.status_code}")

            token_info = response.json()
        except json.decoder.JSONDecodeError as e:
            _logger.critical(
                "JSONDecodeError in token endpoint: %s, Response content: %s", e, response.text[:1000])
            raise Exception(f"Invalid JSON response from token endpoint: {e}")
        except Exception as e:
            _logger.critical(
                f"token endpoint error: {e} ... request params {req_params} .... auth provider {auth_oauth_provider}")
            raise

        if token_info.get("error"):
            _logger.critical(
                f"token info {token_info} ... request params {req_params} .... auth provider {auth_oauth_provider}")
            raise Exception(token_info['error'])

        access_token = token_info.get('access_token')
        validation = {
            'access_token': access_token
        }
        if token_info.get('id_token'):
            if not jwt:
                raise exceptions.AccessDenied()
            try:
                data = jwt.decode(token_info['id_token'], options={"verify_signature": False, "verify_aud": False, "verify_iat": False, "verify_exp": False,
                                  "verify_nbf": False, "verify_iss": False, "verify_sub": False, "verify_jti": False, "verify_at_hash": False}, algorithms=["RS256"])
                _logger.critical(
                    "JWT token successfully decoded: %s", list(data.keys()))
            except Exception as e:
                _logger.critical("Error decoding JWT token: %s", e)
                raise
        else:
            data = self._auth_oauth_rpc(auth_oauth_provider.data_endpoint,
                                        access_token)

        validation.update(data)
        return validation

    @api.model
    def _auth_oauth_signin(self, provider, validation, params):
        """ Retrieve and sign in the user corresponding to provider and validated access token
                    :param provider: oauth provider id (int)
                    :param validation: result of validation of access token (dict)
                    :param params: oauth parameters (dict)
                    :return: user login (str)
                    :raise: AccessDenied if signin failed

                    This method can be overridden to add alternative signin methods.
                """
        user = self.search([('login', '=', str(validation.get('email')))])
        if not user:
            user = self.create({
                'login': str(validation.get('email')),
                'name': str(validation.get('name'))
            })
            provider_id = self.env['auth.oauth.provider'].sudo().browse(
                provider)
            if provider_id.template_user_id:
                user.is_contractor = provider_id.template_user_id.is_contractor
                user.contractor = provider_id.template_user_id.contractor
                user.groups_id = [
                    (6, 0, provider_id.template_user_id.groups_id.ids)]
        user.write({
            'oauth_provider_id': provider,
            'oauth_uid': validation['user_id'],
            'oauth_access_token': params['access_token'],
        })
        oauth_uid = validation['user_id']
        try:
            oauth_user = self.search([("oauth_uid", "=", oauth_uid),
                                      ('oauth_provider_id', '=', provider)])
            if not oauth_user:
                raise exceptions.AccessDenied()
            assert len(oauth_user) == 1
            oauth_user.write({'oauth_access_token': params['access_token']})
            return oauth_user.login
        except (exceptions.AccessDenied, exceptions.access_denied_exception):
            if self.env.context.get('no_user_creation'):
                return None
            state = json.loads(params['state'])
            token = state.get('t')
            values = self._generate_signup_values(provider, validation, params)
            try:
                _, login, _ = self.signup(values, token)
                return login
            except SignupError:
                raise exceptions.access_denied_exception
        return super()._auth_oauth_signin(provider, validation,
                                          params)

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
                _logger.info("OAuth validation response: %s", validation)

            # Check if validation contains an error
            if validation.get('error'):
                _logger.critical("OAuth validation error: %s - %s",
                                 validation.get('error'),
                                 validation.get('error_description', 'No description'))
                raise exceptions.AccessDenied(validation.get(
                    'error_description', 'Authentication failed'))

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
                    _logger.critical(
                        "Cannot find user_id in token: %s", validation)
                    raise exceptions.AccessDenied()

            # Ensure email is present for user creation
            if not validation.get('email') and validation.get('unique_name'):
                if '@' in validation['unique_name']:
                    validation['email'] = validation['unique_name']

            login = self._auth_oauth_signin(provider, validation, params)
            if not login:
                raise exceptions.AccessDenied()
            if provider and params:
                return (self.env.cr.dbname, login, access_token)
        except Exception as e:
            _logger.critical("Exception in auth_oauth: %s", e, exc_info=True)
            raise

        return super(ResUsers, self).auth_oauth(provider, params)
