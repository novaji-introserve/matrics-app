# -*- coding: utf-8 -*-
# from odoo import http


# class IcomplyEtl(http.Controller):
#     @http.route('/icomply_etl/icomply_etl', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/icomply_etl/icomply_etl/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('icomply_etl.listing', {
#             'root': '/icomply_etl/icomply_etl',
#             'objects': http.request.env['icomply_etl.icomply_etl'].search([]),
#         })

#     @http.route('/icomply_etl/icomply_etl/objects/<model("icomply_etl.icomply_etl"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('icomply_etl.object', {
#             'object': obj
#         })
