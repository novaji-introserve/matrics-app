# -*- coding: utf-8 -*-
# from odoo import http


# class UserPermission(http.Controller):
#     @http.route('/user_permission/user_permission', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/user_permission/user_permission/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('user_permission.listing', {
#             'root': '/user_permission/user_permission',
#             'objects': http.request.env['user_permission.user_permission'].search([]),
#         })

#     @http.route('/user_permission/user_permission/objects/<model("user_permission.user_permission"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('user_permission.object', {
#             'object': obj
#         })
