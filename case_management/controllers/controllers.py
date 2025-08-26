# -*- coding: utf-8 -*-
# from odoo import http


# class CaseManagement(http.Controller):
#     @http.route('/case_management/case_management', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/case_management/case_management/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('case_management.listing', {
#             'root': '/case_management/case_management',
#             'objects': http.request.env['case_management.case_management'].search([]),
#         })

#     @http.route('/case_management/case_management/objects/<model("case_management.case_management"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('case_management.object', {
#             'object': obj
#         })
