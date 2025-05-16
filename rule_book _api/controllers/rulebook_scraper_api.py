# -*- coding: utf-8 -*-
# from odoo import models, fields, api, http
from odoo import http
from odoo.http import request, Response
import json 
from datetime import datetime, timedelta
import base64
import os
from dotenv import load_dotenv
load_dotenv()

import logging
_logger = logging.getLogger(__name__)

RATE_LIMIT =   {}


API_KEY = os.getenv('API_KEY')
MAX_REQUESTS = 10
TIME_WINDOW = timedelta(minutes=1) # 5 minutes
VALID_SOURCES = ['cbn', 'ndic', 'sec', 'nfiu']


class RuleBookScraperAPI(http.Controller):
    



    def rate_limit_exceeded(self, ip):
        current_time = datetime.now()
        requests_from_ip = RATE_LIMIT.get(ip, [])
        requests_from_ip = [t for t in requests_from_ip if current_time - t < TIME_WINDOW]
        RATE_LIMIT[ip] = requests_from_ip
        is_exceeded = len(requests_from_ip) >= MAX_REQUESTS
        _logger.info(f"[{current_time}] IP: {ip}, Requests: {len(requests_from_ip)}, Exceeded: {is_exceeded}")
        return is_exceeded
    
    def record_request(self, ip):
        current_time = datetime.now()
        RATE_LIMIT.setdefault(ip, []).append(current_time)

    @staticmethod
    def check_api_key(request):
        api_key =request.httprequest.headers.get('X-API-Key') or request.params.get('api_key') 
        return api_key == API_KEY
       

    # get rule_book document
    @http.route('/api/rulebooks/<string:source>', type='http', auth='none', methods=['GET'], csrf=False)
    def get_rulebooks(self, source, **kwrgs):

        _logger.info(f"Request for rulebooks from {source}")
        if source not in VALID_SOURCES:
            return Response(
                json.dumps({'status': 'error', 'message': f'Invalid source. Valid sources are: {", ".join(VALID_SOURCES)}'}),
                content_type='application/json',
                status=400
            )
        # Rate limiting
        ip = request.httprequest.remote_addr
        if self.rate_limit_exceeded(ip):
            _logger.warning(f"Rate limit exceeded for IP: {ip}")
            return request.make_response(
                json.dumps({'error': 'Rate limit exceeded. Try again later.'}),
                headers=[('Content-Type', 'application/json')],
                status=429
            )

        self.record_request(ip)
        _logger.info(f"Request allowed from IP: {ip}")

        # Check API key
        if not self.check_api_key(request):
            return Response(
                json.dumps({
                    'status': 'error',
                    'message': 'Invalid API key'
                }),
                content_type='application/json',
                status=403
            )
        try:
            base_url = request.httprequest.host_url.rstrip('/')
            limit = int(kwrgs.get('limit', 100))
            offset = int(kwrgs.get('offset', 0))
            domain = [
                ('source_id.name', 'ilike', source.upper()),
                ('status', '=', 'active'),
                '|',
                ('input_type', '=', 'manual'),
                ('input_type', '=', 'ai'),   
                
                ]
            rulebooks = request.env['regulatory.document'].sudo().search(
                domain, limit=limit, 
                offset=offset,
                order='create_date desc'
                )
            _logger.info(f"Rulebooks found: {len(rulebooks)}")
            
            data = [{
                'id': r.id,
                'title': r.name,
                'filename': r.file_name or '',
                'released_date': r.released_date.strftime('%Y-%m-%d') if r.released_date else '',
                'source': r.source_id.name,
                'reference_number': r.ref_number or '',
                'file_url': r.external_resource_url or '',
                'created_on': r.create_date.strftime('%Y-%m-%d %H:%M:%S'),
                'status': r.status,
                'download_url': f"{base_url}/api/rulebooks/{r.id}/download",
                'view_url': f"{base_url}/api/rulebooks/{r.id}/view",
            } for r in rulebooks]

            return Response(
                json.dumps({
                    'status': 'success',
                    'data': data,
                    'count': len(data),
                    'total': request.env['regulatory.document'].sudo().search_count(domain),
                }),
                content_type='application/json',
                status=200
            )
        except Exception as e:
            _logger.error(f"Error in {source} rulebooks: {str(e)}")
            return Response(
                json.dumps({
                    'status': 'error',
                    'message': str(e)
                }),
                content_type='application/json',
                status=500
            )
        
    # filter doucumnets from the last 30 days

    @http.route('/api/rulebooks/<string:source>/<int:days>', type='http', auth='none', methods=['GET'], csrf=False)
    def get_rulebooks_by_date(self, source, days, **kwargs):
        _logger.info(f"Request for rulebooks from {source} in the last {days} days")

        # Validate 'days'
        if days < 1 or days > 30:
            return Response(
                json.dumps({'status': 'error', 'message': 'Invalid number of days. Must be between 1 and 30.'}),
                content_type='application/json',
                status=400
            )
        if source not in VALID_SOURCES:
            return Response(
                json.dumps({'status': 'error', 'message': f'Invalid source. Valid sources are: {", ".join(VALID_SOURCES)}'}),
                content_type='application/json',
                status=400
            )

        try:
            now = datetime.utcnow()
            since = now - timedelta(days=days)
            base_url = request.httprequest.host_url.rstrip('/')
            limit = int(kwargs.get('limit', 100))
            offset = int(kwargs.get('offset', 0))

            # Build domain
            domain = [
                ('source_id.name', 'ilike', source.upper()),
                ('status', '=', 'active'),
                '|', ('input_type', '=', 'manual'), ('input_type', '=', 'ai'),
                '|',
                '&', ('released_date', '!=', False), ('released_date', '>=', since),
                '&', ('released_date', '=', False), ('create_date', '>=', since)
            ]

            rulebooks = request.env['regulatory.document'].sudo().search(
                domain, limit=limit, offset=offset, order='create_date desc'
            )

            data = [{
                'id': r.id,
                'title': r.name,
                'filename': r.file_name or '',
                'released_date': r.released_date.strftime('%Y-%m-%d') if r.released_date else '',
                'source': r.source_id.name,
                'reference_number': r.ref_number or '',
                'file_url': r.external_resource_url or '',
                'created_on': r.create_date.strftime('%Y-%m-%d %H:%M:%S'),
                'status': r.status,
                'download_url': f"{base_url}/api/rulebooks/{r.id}/download",
                'view_url': f"{base_url}/api/rulebooks/{r.id}/view",
            } for r in rulebooks]
            
            if data:
                return Response(json.dumps({'status': 'success', 'data': data}),
                            content_type='application/json', status=200)
            else:
                return Response(json.dumps({'status': 'success', 'data': [], 'message': 'No rulebooks found'}),
                            content_type='application/json', status=200)

        except Exception as e:
            _logger.error(f"Error in getting rulebooks for {source}: {str(e)}")
            return Response(json.dumps({'status': 'error', 'message': str(e)}),
                            content_type='application/json', status=500)
        


    # download rulebook document
    @http.route('/api/rulebooks/<int:id>/download', type='http', auth='none',csrf=False)
    def download_rulebook(self, id, **kwargs):
        _logger.info(f"Download request for rulebook ID: {id}")
        # Check API key
        if not self.check_api_key(request):
            return Response(
                json.dumps({
                    'status': 'error',
                    'message': 'Invalid API key'
                }),
                content_type='application/json',
                status=403
            )
        try:
            rulebook = request.env['regulatory.document'].sudo().browse(id)
            if not rulebook.exists():
                return Response(
                    json.dumps({
                        'status': 'error',
                        'message': 'Rulebook not found'
                    }),
                    content_type='application/json',
                    status=404
                )
            if not rulebook.file:
                return Response(
                    json.dumps({
                        'status': 'error',
                        'message': 'File not found'
                    }),
                    content_type='application/json',
                    status=404
                )
            # Get the file content
            file_content = base64.b64decode(rulebook.file)
            filename = rulebook.file_name or f"rulebook_{id}.pdf"
            if not filename.lower().endswith('.pdf'):
                filename += '.pdf'

            return Response(
                file_content,
                headers=[
                    ('content-type', 'application/pdf'),
                    ('Content-Disposition', f'attachment; filename="{filename}"'),
                ],
                status=200
            )
        except Exception as e:
            _logger.error(f"Error in download_rulebook: {str(e)}")
            return Response(
                json.dumps({
                    'status': 'error',
                    'message': str(e)
                }),
                content_type='application/json',
                status=500
            )


    # view rulebook document
    @http.route('/api/rulebooks/<int:id>/view', type='http', auth='none', methods=['GET'], csrf=False)
    def view_rulebook(self, id, **kwargs):
        _logger.info(f"View request for rulebook ID: {id}")
        # Check API key
        if not self.check_api_key(request):
            return Response(
                json.dumps({
                    'status': 'error',
                    'message': 'Invalid API key'
                }),
                content_type='application/json',
                status=403
            )
        try:
            rulebook = request.env['regulatory.document'].sudo().browse(id)
            if not rulebook.exists():
                return Response(
                    json.dumps({
                        'status': 'error',
                        'message': 'Rulebook not found'
                    }),
                    content_type='application/json',
                    status=404
                )
            if not rulebook.file:
                return Response(
                    json.dumps({
                        'status': 'error',
                        'message': 'File not found'
                    }),
                    content_type='application/json',
                    status=404
                )
            # Get the file content
            file_content = base64.b64decode(rulebook.file)

            # Get the file name
            filename = rulebook.file_name or f"rulebook_{id}.pdf"

            return Response(
                file_content,
                headers=[
                    ('content-type', 'application/pdf'),
                    ('Content-Disposition', f'inline; filename="{filename}"'),
                ],
                status=200
            )
        except Exception as e:
            _logger.error(f"Error in view_rulebook: {str(e)}")
            return Response(
                json.dumps({
                    'status': 'error',
                    'message': str(e)
                }),
                content_type='application/json',
                status=500
            )
         
    @http.route('/api/rulebooks/docs', auth='none', methods=['GET'], type='http')
    def api_documentation(self, **kwargs):
        if not self.check_api_key(request):
            return Response(
                json.dumps({
                    'status': 'error',
                    'message': 'Invalid API key'
                }),
                content_type='application/json',
                status=403
            )

        base_url = request.httprequest.host_url.rstrip('/')

        docs = {
            "info": {
                "title": "Regulatory Rulebooks API",
                "description": "Access regulatory documents from CBN, NDIC, SEC, and NFIU."
            },
            "servers": [{"url": base_url}],
            "paths": {
                "/api/rulebooks/{source}": {
                    "get": {
                        "summary": "Get rulebooks by source",
                        "parameters": [
                            {"name": "source", "in": "path", "required": True, "schema": {"type": "string", "enum": ["cbn", "ndic", "sec", "nfiu"]}},
                            {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 100}},
                            {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0}},
                            {"name": "X-API-Key", "in": "header", "required": True, "schema": {"type": "string"}}
                        ],
                        "responses": {
                            "200": {"description": "List of rulebooks"},
                            "400": {"description": "Invalid source"},
                            "403": {"description": "Invalid API key"},
                            "500": {"description": "Server error"}
                        }
                    }
                },
                "/api/rulebooks/{source}/{days}": {
                    "get": {
                        "summary": "Get recent rulebooks",
                        "parameters": [
                            {"name": "source", "in": "path", "required": True, "schema": {"type": "string", "enum": ["cbn", "ndic", "sec", "nfiu"]}},
                            {"name": "days", "in": "path", "required": True, "schema": {"type": "integer", "minimum": 1, "maximum": 30}},
                            {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 100}},
                            {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0}},
                            {"name": "X-API-Key", "in": "header", "required": True, "schema": {"type": "string"}}
                        ],
                        "responses": {
                            "200": {"description": "Filtered rulebooks"},
                            "400": {"description": "Invalid days or source"}
                        }
                    }
                },
                "/api/rulebooks/{id}/download": {
                    "get": {
                        "summary": "Download rulebook file",
                        "parameters": [
                            {"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}},
                            {"name": "X-API-Key", "in": "header", "required": True, "schema": {"type": "string"}}
                        ],
                        "responses": {
                            "200": {"description": "PDF file"},
                            "404": {"description": "Not found"}
                        }
                    }
                },
                "/api/rulebooks/{id}/view": {
                    "get": {
                        "summary": "View rulebook in browser",
                        "parameters": [
                            {"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}},
                            {"name": "X-API-Key", "in": "header", "required": True, "schema": {"type": "string"}}
                        ],
                        "responses": {
                            "200": {"description": "Inline PDF"},
                            "404": {"description": "Not found"}
                        }
                    }
                }
            },
            "components": {
                "securitySchemes": {
                    "ApiKeyAuth": {"type": "apiKey", "in": "header", "name": "X-API-Key"}
                }
            },
            "security": [{"ApiKeyAuth": []}]
        }

        return Response(
            json.dumps(docs),
            content_type='application/json',
            status=200
        )
