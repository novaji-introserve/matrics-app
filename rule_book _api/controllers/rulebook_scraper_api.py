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
       

    # Route to get CBN rulebooks
    @http.route('/api/rulebooks/cbn', type='http', auth='none', methods=['GET'], csrf=False)
    def get_cbn_rulebooks(self, **kwrgs):
        # Rate limiting
        ip = request.httprequest.remote_addr

        # Check if limit exceeded
        if self.rate_limit_exceeded(ip):
            _logger.warning(f"Rate limit exceeded for IP: {ip}")
            return request.make_response(
                json.dumps({'error': 'Rate limit exceeded. Try again later.'}),
                headers=[('Content-Type', 'application/json')],
                status=429
            )

        # Allow request
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
                ('source_id.name', 'ilike', 'CBN'),
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
            

            data =[]
            for rulebook in rulebooks:
                data.append({
                    'id': rulebook.id,
                    'title': rulebook.name,
                    'filename': rulebook.file_name or '',
                    'released_date': rulebook.released_date.strftime('%Y-%m-%d') if rulebook.released_date else '',
                    'source': rulebook.source_id.name,
                    'reference_number': rulebook.ref_number or '',
                    'file_url': rulebook.external_resource_url or '',
                    'created_on': rulebook.created_on.strftime('%Y-%m-%d %H:%M:%S'),
                    'status': rulebook.status,
                    'download_url': f"{base_url}/api/rulebooks/{rulebook.id}/download",
                    'view_url': f"{base_url}/api/rulebooks/{rulebook.id}/view",
                })
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
            _logger.error(f"Error in get_cbn_rulebooks: {str(e)}")
            return Response(
                json.dumps({
                    'status': 'error',
                    'message': str(e)
                }),
                content_type='application/json',
                status=500
            )
        
    # Route to get NDIC rulebooks
    @http.route('/api/rulebooks/ndic', type='http', auth='none', methods=['GET'], csrf=False)
    def get_ndic_rulebooks(self, **kwargs):
        # Rate limiting
        ip = request.httprequest.remote_addr

        # Check if limit exceeded
        if self.rate_limit_exceeded(ip):
            _logger.warning(f"Rate limit exceeded for IP: {ip}")
            return request.make_response(
                json.dumps({'error': 'Rate limit exceeded. Try again later.'}),
                headers=[('Content-Type', 'application/json')],
                status=429
            )

        # Allow request
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
            limit = int(kwargs.get('limit', 100))
            offset = int(kwargs.get('offset', 0))
            domain = [
                ('source_id.name', 'ilike', 'NDIC'),
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
                
            data =[]
            for rulebook in rulebooks:
                data.append({
                    'id': rulebook.id,
                    'title': rulebook.name,
                    'filename': rulebook.file_name or '',
                    'released_date': rulebook.released_date.strftime('%Y-%m-%d') if rulebook.released_date else '',
                    'source': rulebook.source_id.name,
                    'reference_number': rulebook.ref_number or '',
                    'file_url': rulebook.external_resource_url or '',
                    'created_on': rulebook.created_on.strftime('%Y-%m-%d %H:%M:%S'),
                    'status': rulebook.status,
                    'download_url': f"{base_url}/api/rulebooks/{rulebook.id}/download",
                    'view_url': f"{base_url}/api/rulebooks/{rulebook.id}/view",
                })
            return Response(
                    json.dumps({
                        'status':'success',
                        'data': data,
                        'count': len(data),
                        'total': request.env['regulatory.document'].sudo().search_count(domain),
                    }),
                    content_type='application/json',
                    status=200
                )
        except Exception as e:
            _logger.error(f"Error in get_ndic_rulebooks: {str(e)}")
            return Response(
                json.dumps({
                    'status': 'error',
                    'message': str(e)
                }),
                content_type='application/json',
                status=500
            )

    #Route to get  SEC rulebooks
    @http.route('/api/rulebooks/sec', type='http', auth='none', methods=['GET'], csrf=False)
    def get_sec_rulebooks(self, **kwargs):
        # Rate limiting
        ip = request.httprequest.remote_addr

        # Check if limit exceeded
        if self.rate_limit_exceeded(ip):
            _logger.warning(f"Rate limit exceeded for IP: {ip}")
            return request.make_response(
                json.dumps({'error': 'Rate limit exceeded. Try again later.'}),
                headers=[('Content-Type', 'application/json')],
                status=429
            )

        # Allow request
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
            limit = int(kwargs.get('limit', 100))
            offset = int(kwargs.get('offset', 0))
            domain = [
                ('source_id.name', 'ilike', 'SEC'),
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
                
            data =[]
            for rulebook in rulebooks:
                data.append({
                    'id': rulebook.id,
                    'title': rulebook.name,
                    'filename': rulebook.file_name or '',
                    'released_date': rulebook.released_date.strftime('%Y-%m-%d') if rulebook.released_date else '',
                    'source': rulebook.source_id.name,
                    'reference_number': rulebook.ref_number or '',
                    'file_url': rulebook.external_resource_url or '',
                    'created_on': rulebook.created_on.strftime('%Y-%m-%d %H:%M:%S'),
                    'status': rulebook.status,
                    'download_url': f"{base_url}/api/rulebooks/{rulebook.id}/download",
                    'view_url': f"{base_url}/api/rulebooks/{rulebook.id}/view",
                })
            return Response(
                    json.dumps({
                        'status':'success',
                        'data': data,
                        'count': len(data),
                        'total': request.env['regulatory.document'].sudo().search_count(domain),
                    }),
                    content_type='application/json',
                    status=200
                )
        except Exception as e:
            _logger.error(f"Error in get_sec_rulebooks: {str(e)}")
            return Response(
                json.dumps({
                    'status': 'error',
                    'message': str(e)
                }),
                content_type='application/json',
                status=500
            ) 
    # Route to get NFIU rulebooks
    @http.route('/api/rulebooks/nfiu', type='http', auth='none', methods=['GET'], csrf=False)
    def get_nfiu_rulebooks(self, **kwargs):
        # Rate limiting
        ip = request.httprequest.remote_addr

        # Check if limit exceeded
        if self.rate_limit_exceeded(ip):
            _logger.warning(f"Rate limit exceeded for IP: {ip}")
            return request.make_response(
                json.dumps({'error': 'Rate limit exceeded. Try again later.'}),
                headers=[('Content-Type', 'application/json')],
                status=429
            )

        # Allow request
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
            limit = int(kwargs.get('limit', 100))
            offset = int(kwargs.get('offset', 0))
            domain = [
                ('source_id.name', 'ilike', 'NFIU'),
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
                
            data =[]
            for rulebook in rulebooks:
                data.append({
                    'id': rulebook.id,
                    'title': rulebook.name,
                    'filename': rulebook.file_name or '',
                    'released_date': rulebook.released_date.strftime('%Y-%m-%d') if rulebook.released_date else '',
                    'source': rulebook.source_id.name,
                    'reference_number': rulebook.ref_number or '',
                    'file_url': rulebook.external_resource_url or '',
                    'created_on': rulebook.created_on.strftime('%Y-%m-%d %H:%M:%S'),
                    'status': rulebook.status,
                    'download_url': f"{base_url}/api/rulebooks/{rulebook.id}/download",
                    'view_url': f"{base_url}/api/rulebooks/{rulebook.id}/view",
                })
            return Response(
                    json.dumps({
                        'status':'success',
                        'data': data,
                        'count': len(data),
                        'total': request.env['regulatory.document'].sudo().search_count(domain),
                    }),
                    content_type='application/json',
                    status=200
                )
        except Exception as e:
            _logger.error(f"Error in get_nfiu_rulebooks: {str(e)}")
            return Response(
                json.dumps({
                    'status': 'error',
                    'message': str(e)
                }),
                content_type='application/json',
                status=500
            )


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
        # API documentation endpoint
        docs = {
            "endpoints": {
                "/api/rulebooks/cbn": {
                    "description": "Get CBN rulebooks",
                    "parameters": {
                        "limit": "Number of records to return (default: 100)",
                        "offset": "Pagination offset (default: 0)"
                    },
                    "method": "GET"
                },

                "/api/rulebooks/ndic": {
                    "description": "Get NDIC rulebooks",
                    "parameters": {
                        "limit": "Number of records to return (default: 100)",
                        "offset": "Pagination offset (default: 0)"
                    },
                    "method": "GET"
                },

                "/api/rulebooks/sec": {
                    "description": "Get SEC rulebooks",
                    "parameters": {
                        "limit": "Number of records to return (default: 100)",
                        "offset": "Pagination offset (default: 0)"
                    },
                    "method": "GET"
                },

                "/api/rulebooks/nfiu": {
                    "description": "Get NFIU rulebooks",
                    "parameters": {
                        "limit": "Number of records to return (default: 100)",
                        "offset": "Pagination offset (default: 0)"
                    },
                    "method": "GET"
                },

                "/api/rulebooks/<int:rulebook_id>/download":{
                    "description": "Download rulebook record",
                    "parameters": {
                        "rulebook_id": "ID of the rulebook to download"
                    },
                    "method": "GET"
                },

                "/api/rulebooks/<int:rulebook_id>/view":{
                    "description": "View rulebook file in browser",
                    "parameters": {
                        "rulebook_id": "ID of the rulebook to view"
                    },
                    "method": "GET"
                
                },

                },
                
            }
        
        return Response(
            json.dumps(docs),
            content_type='application/json',
            status=200
        )
    
    @staticmethod
    def check_api_key(request):
        api_key =request.httprequest.headers.get('X-API-Key') or request.params.get('api_key') 
        return api_key == API_KEY