# -*- coding: utf-8 -*-
# from odoo import http


# class CustomSettings(http.Controller):
#     @http.route('/custom_settings/custom_settings', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/custom_settings/custom_settings/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('custom_settings.listing', {
#             'root': '/custom_settings/custom_settings',
#             'objects': http.request.env['custom_settings.custom_settings'].search([]),
#         })

#     @http.route('/custom_settings/custom_settings/objects/<model("custom_settings.custom_settings"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('custom_settings.object', {
#             'object': obj
#         })
