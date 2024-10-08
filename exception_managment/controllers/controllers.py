# -*- coding: utf-8 -*-
# from odoo import http


# class ExceptionManagment(http.Controller):
#     @http.route('/exception_managment/exception_managment', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/exception_managment/exception_managment/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('exception_managment.listing', {
#             'root': '/exception_managment/exception_managment',
#             'objects': http.request.env['exception_managment.exception_managment'].search([]),
#         })

#     @http.route('/exception_managment/exception_managment/objects/<model("exception_managment.exception_managment"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('exception_managment.object', {
#             'object': obj
#         })
