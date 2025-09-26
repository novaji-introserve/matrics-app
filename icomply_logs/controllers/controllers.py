# -*- coding: utf-8 -*-
# from odoo import http


# class IcomplyLogs(http.Controller):
#     @http.route('/icomply_logs/icomply_logs', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/icomply_logs/icomply_logs/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('icomply_logs.listing', {
#             'root': '/icomply_logs/icomply_logs',
#             'objects': http.request.env['icomply_logs.icomply_logs'].search([]),
#         })

#     @http.route('/icomply_logs/icomply_logs/objects/<model("icomply_logs.icomply_logs"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('icomply_logs.object', {
#             'object': obj
#         })
