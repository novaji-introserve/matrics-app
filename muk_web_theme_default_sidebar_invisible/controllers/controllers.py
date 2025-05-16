# -*- coding: utf-8 -*-
from odoo import http

# class MukWebThemeDefaultSidebarInvisible(http.Controller):
#     @http.route('/muk_web_theme_default_sidebar_invisible/muk_web_theme_default_sidebar_invisible/', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/muk_web_theme_default_sidebar_invisible/muk_web_theme_default_sidebar_invisible/objects/', auth='public')
#     def list(self, **kw):
#         return http.request.render('muk_web_theme_default_sidebar_invisible.listing', {
#             'root': '/muk_web_theme_default_sidebar_invisible/muk_web_theme_default_sidebar_invisible',
#             'objects': http.request.env['muk_web_theme_default_sidebar_invisible.muk_web_theme_default_sidebar_invisible'].search([]),
#         })

#     @http.route('/muk_web_theme_default_sidebar_invisible/muk_web_theme_default_sidebar_invisible/objects/<model("muk_web_theme_default_sidebar_invisible.muk_web_theme_default_sidebar_invisible"):obj>/', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('muk_web_theme_default_sidebar_invisible.object', {
#             'object': obj
#         })