from odoo import models, fields, api,_
import requests
import json
import logging
from odoo.exceptions import UserError, ValidationError
import base64
import time

_logger = logging.getLogger(__name__)

HTTP_METHODS = [
    ('GET', 'GET'),
    ('POST', 'POST'),
    ('PUT', 'PUT'),
    ('DELETE', 'DELETE'),
    ('PATCH', 'PATCH'),
    ('HEAD', 'HEAD'),
]

CONTENT_TYPES = [
    ('application/json', 'JSON'),
    ('application/x-www-form-urlencoded', 'Form URL Encoded'),
    ('text/plain', 'Plain Text'),
    ('application/xml', 'XML'),
    ('multipart/form-data', 'Form Data'),
]

class ApiRequest(models.Model):
    _name = 'api.request'
    _description = 'API Request'

    # name = fields.Char(string='Name', required=True)
    url = fields.Char(string='URL', required=True)
    method = fields.Selection(HTTP_METHODS, string='HTTP Method', required=True)
    headers = fields.Text(string='Headers', help="Headers in JSON format")
    params = fields.Text(string='Query Parameters', help="Parameters in JSON format")
    body = fields.Text(string='Request Body')
    content_type = fields.Selection(CONTENT_TYPES, string='Content Type', default='application/json')  

    response_code = fields.Integer(string='Response Code', readonly=True)
    response_headers = fields.Text(string='Response Headers', readonly=True)
    response_body = fields.Text(string='Response Body', readonly=True)
    response_time = fields.Float(string='Response Time (s)', readonly=True)

    auth_type = fields.Selection([
        ('none', 'None'),
        ('basic', 'Basic Auth'),
        ('bearer', 'Bearer Token'),
        ('api_key', 'API Key'),
        ('oauth2', 'OAuth 2.0'),
    ], string='Auth Type', default='none')
    
    auth_username = fields.Char(string='Username')
    auth_password = fields.Char(string='Password')
    auth_token = fields.Char(string='Token')
    auth_key_name = fields.Char(string='Key Name')
    auth_key_value = fields.Char(string='Key Value')
    auth_key_in = fields.Selection([
        ('header', 'Header'),
        ('query', 'Query Parameter'),
    ], string='Key In', default='header')
    
    attachment_ids = fields.Many2many('ir.attachment', string='Attachments')

    def send_request(self):
        self.ensure_one()

        headers = {}
        data = None
        files = None

        if self.headers:
            try:
                headers = json.loads(self.headers)
            except json.JSONDecodeError:
                raise ValidationError(_("Invalid JSON format in headers."))
        
        if self.content_type and 'content-type' not in headers:
            headers['Content-Type'] = self.content_type


        # Add authentication
        if self.auth_type == 'basic':
            auth_string = f"{self.auth_username}:{self.auth_password}"
            headers['Authorization'] = f"Basic {base64.b64encode(auth_string.encode()).decode()}"
        elif self.auth_type == 'bearer':
            headers['Authorization'] = f"Bearer {self.auth_token}"
        elif self.auth_type == 'api_key' and self.auth_key_in == 'header':
            headers[self.auth_key_name] = self.auth_key_value

        # Prepare parameters
        params = {}
        if self.params:
            try:
                params = json.loads(self.params)
            except json.JSONDecodeError:
                raise UserError(_("Invalid JSON format in parameters"))
        
        # Add API key auth if it's in query params
        if self.auth_type == 'api_key' and self.auth_key_in == 'query':
            params[self.auth_key_name] = self.auth_key_value


        # Prepare request body
        
        if self.method in ['POST', 'PUT', 'PATCH']:
            if self.content_type == 'application/json' and self.body:
                try:
                    data = json.loads(self.body)
                except json.JSONDecodeError:
                    raise UserError(_("Invalid JSON format in body"))
            elif self.content_type == 'application/x-www-form-urlencoded' and self.body:
                try:
                    data = json.loads(self.body)
                except json.JSONDecodeError:
                    raise UserError(_("Invalid JSON format in body"))
            elif self.content_type == 'multipart/form-data':
                files = {}
                for attachment in self.attachment_ids:
                    files[attachment.name] = (attachment.name, base64.b64decode(attachment.datas), attachment.mimetype)
                    
                # If we have body data for multipart form
                if self.body:
                    try:
                        form_data = json.loads(self.body)
                        data = form_data
                    except json.JSONDecodeError:
                        raise UserError(_("Invalid JSON format in form data"))
            else:
                data = self.body

        start_time = time.time()

        try:
            response = requests.request(
                method=self.method,
                url=self.url,
                headers=headers,
                params=params,
                data=data if not isinstance(data, dict) and data else None,
                json=data if isinstance(data, dict) else None,
                files=files,
                timeout=30
                )
            duration = time.time() - start_time

            # Format response headers
            response_headers = dict(response.headers)

            # format for response body
            response_body = response.text
            try:
                if 'application/json' in response.headers.get('Content-Type', ''):
                    response_body = json.dumps(response.json(), indent=4, sort_keys=True)
            except:
                pass


            # Update record with response
            self.write({
                    'response_code': response.status_code,
                    'response_headers': json.dumps(response_headers, indent=4),
                    'response_body': response_body,
                    'response_time': round(duration, 3),
                })

                # Create history entry
            self.env['api.request.history'].create({
                    # 'request_id': self.id,
                    'url': self.url,
                    'method': self.method,
                   'params': self.params,
                    'body': self.body,
                    'content_type': self.content_type,
                    'response_code': response.status_code,
                    'response_headers': json.dumps(response_headers, indent=4),
                    'response_body': response_body,
                    'response_time': round(duration, 3),
                })
                
            return {
                    'type': 'ir.actions.client',
                    'tag': 'reload',
                }
                
        except requests.exceptions.RequestException as e:
            raise UserError(_("Request error: %s") % str(e))


class ApiRequestHistory(models.Model):
    _name = 'api.request.history'
    _description = 'API Request History'
    _order = 'create_date desc'


    create_date = fields.Datetime(string='Date', readonly=True)
    url = fields.Char(string='URL', readonly=True)
    method = fields.Selection(HTTP_METHODS, string='Method', readonly=True)
    headers = fields.Text(string='Headers', readonly=True)
    params = fields.Text(string='Parameters', readonly=True)
    body = fields.Text(string='Request Body', readonly=True)
    content_type = fields.Selection(CONTENT_TYPES, string='Content Type', readonly=True)
    
    response_code = fields.Integer(string='Response Code', readonly=True)
    response_headers = fields.Text(string='Response Headers', readonly=True)
    response_body = fields.Text(string='Response Body', readonly=True)
    response_time = fields.Float(string='Response Time (s)', readonly=True)