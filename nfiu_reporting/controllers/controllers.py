# -*- coding: utf-8 -*-
# from odoo import http


# class NfirReporting(http.Controller):
#     @http.route('/nfir_reporting/nfir_reporting', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/nfir_reporting/nfir_reporting/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('nfir_reporting.listing', {
#             'root': '/nfir_reporting/nfir_reporting',
#             'objects': http.request.env['nfir_reporting.nfir_reporting'].search([]),
#         })

#     @http.route('/nfir_reporting/nfir_reporting/objects/<model("nfir_reporting.nfir_reporting"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('nfir_reporting.object', {
#             'object': obj
#         })
