# -*- coding: utf-8 -*-
# from odoo import http


# class CaseManagementV2(http.Controller):
#     @http.route('/case_management_v2/case_management_v2', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/case_management_v2/case_management_v2/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('case_management_v2.listing', {
#             'root': '/case_management_v2/case_management_v2',
#             'objects': http.request.env['case_management_v2.case_management_v2'].search([]),
#         })

#     @http.route('/case_management_v2/case_management_v2/objects/<model("case_management_v2.case_management_v2"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('case_management_v2.object', {
#             'object': obj
#         })
