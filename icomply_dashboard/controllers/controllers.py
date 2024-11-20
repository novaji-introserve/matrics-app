# -*- coding: utf-8 -*-
# from odoo import http


# class Mydashboard(http.Controller):
#     @http.route('/mydashboard/mydashboard', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/mydashboard/mydashboard/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('mydashboard.listing', {
#             'root': '/mydashboard/mydashboard',
#             'objects': http.request.env['mydashboard.mydashboard'].search([]),
#         })

#     @http.route('/mydashboard/mydashboard/objects/<model("mydashboard.mydashboard"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('mydashboard.object', {
#             'object': obj
#         })
