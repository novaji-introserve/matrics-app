# -*- coding: utf-8 -*-
# from odoo import http


# class CustomBackendTheme(http.Controller):
#     @http.route('/custom_backend_theme/custom_backend_theme', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/custom_backend_theme/custom_backend_theme/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('custom_backend_theme.listing', {
#             'root': '/custom_backend_theme/custom_backend_theme',
#             'objects': http.request.env['custom_backend_theme.custom_backend_theme'].search([]),
#         })

#     @http.route('/custom_backend_theme/custom_backend_theme/objects/<model("custom_backend_theme.custom_backend_theme"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('custom_backend_theme.object', {
#             'object': obj
#         })
