# -*- coding: utf-8 -*-
# from odoo import http


# class LoginBackgroundZg(http.Controller):
#     @http.route('/login_background__zg/login_background__zg', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/login_background__zg/login_background__zg/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('login_background__zg.listing', {
#             'root': '/login_background__zg/login_background__zg',
#             'objects': http.request.env['login_background__zg.login_background__zg'].search([]),
#         })

#     @http.route('/login_background__zg/login_background__zg/objects/<model("login_background__zg.login_background__zg"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('login_background__zg.object', {
#             'object': obj
#         })
