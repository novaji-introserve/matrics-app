# -*- coding: utf-8 -*-
# from odoo import http


# class IcomplyEtlManager(http.Controller):
#     @http.route('/icomply_etl_manager/icomply_etl_manager', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/icomply_etl_manager/icomply_etl_manager/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('icomply_etl_manager.listing', {
#             'root': '/icomply_etl_manager/icomply_etl_manager',
#             'objects': http.request.env['icomply_etl_manager.icomply_etl_manager'].search([]),
#         })

#     @http.route('/icomply_etl_manager/icomply_etl_manager/objects/<model("icomply_etl_manager.icomply_etl_manager"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('icomply_etl_manager.object', {
#             'object': obj
#         })
