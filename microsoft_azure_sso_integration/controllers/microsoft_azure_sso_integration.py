"""Microsoft Azure SSO Integration Controller"""

import json
import logging
import werkzeug.urls
import werkzeug.utils
from odoo.http import request
from odoo.addons.auth_signup.controllers.main import AuthSignupHome as Home

_logger = logging.getLogger(__name__)


class OAuthLogin(Home):
    """This class handles Microsoft OAuth login flow"""
   
    def list_providers(self):
        """
        Generates OAuth provider links for login
        Ensures HTTPS redirect URLs for security
        """
        # Call parent method to maintain compatibility
        super().list_providers()
        
        try:
            # Get all enabled OAuth providers
            auth_providers = request.env['auth.oauth.provider'].sudo().search_read([
                ('enabled', '=', True)
            ])
        except Exception as e:
            _logger.error("Failed to retrieve OAuth providers: %s", e)
            auth_providers = []
        
        for provider in auth_providers:
            try:
                # Get base URL from system parameters
                base_url = request.env['ir.config_parameter'].sudo().get_param('web.app.url')
                
                if not base_url:
                    # Fallback to request URL if no system parameter set
                    base_url = request.httprequest.url_root.rstrip('/')
                
                # Ensure HTTPS for security (required by Microsoft)
                if base_url.startswith('http://'):
                    base_url = base_url.replace('http://', 'https://')
                
                # Construct the OAuth redirect URI
                redirect_uri = base_url + '/auth_oauth/signin'
                
                # Generate state parameter for CSRF protection
                state = self.get_state(provider)
                
                # Build OAuth authorization URL parameters
                oauth_params = {
                    'response_type': provider.get('response_type', 'code'),
                    'client_id': provider['client_id'],
                    'redirect_uri': redirect_uri,
                    'scope': provider.get('scope', 'openid email profile'),
                    'state': json.dumps(state),
                }
                
                # Remove None values
                oauth_params = {k: v for k, v in oauth_params.items() if v is not None}
                
                # Construct the full authorization URL
                auth_endpoint = provider['auth_endpoint']
                provider['auth_link'] = f"{auth_endpoint}?{werkzeug.urls.url_encode(oauth_params)}"
                
                # _logger.info(
                #     "Generated OAuth link for provider %s: %s", 
                #     provider.get('name', 'Unknown'), 
                #     provider['auth_link']
                # )
                
            except Exception as e:
                _logger.error(
                    "Failed to generate OAuth link for provider %s: %s", 
                    provider.get('name', 'Unknown'), 
                    e
                )
                # Set empty auth_link to prevent errors in frontend
                provider['auth_link'] = ''
        
        _logger.debug("OAuth providers prepared: %s", [p.get('name') for p in auth_providers])
        return auth_providers