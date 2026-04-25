# -*- coding: utf-8 -*-
# from odoo import http


# class Utility(http.Controller):
#     @http.route('/utility/utility', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/utility/utility/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('utility.listing', {
#             'root': '/utility/utility',
#             'objects': http.request.env['utility.utility'].search([]),
#         })

#     @http.route('/utility/utility/objects/<model("utility.utility"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('utility.object', {
#             'object': obj
#         })
