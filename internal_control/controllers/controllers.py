# -*- coding: utf-8 -*-
# from odoo import http


# class InternalControl(http.Controller):
#     @http.route('/internal_control/internal_control', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/internal_control/internal_control/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('internal_control.listing', {
#             'root': '/internal_control/internal_control',
#             'objects': http.request.env['internal_control.internal_control'].search([]),
#         })

#     @http.route('/internal_control/internal_control/objects/<model("internal_control.internal_control"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('internal_control.object', {
#             'object': obj
#         })
