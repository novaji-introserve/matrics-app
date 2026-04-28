# -*- coding: utf-8 -*-
# from odoo import http


# class RegulatoryReports(http.Controller):
#     @http.route('/regulatory_reports/regulatory_reports', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/regulatory_reports/regulatory_reports/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('regulatory_reports.listing', {
#             'root': '/regulatory_reports/regulatory_reports',
#             'objects': http.request.env['regulatory_reports.regulatory_reports'].search([]),
#         })

#     @http.route('/regulatory_reports/regulatory_reports/objects/<model("regulatory_reports.regulatory_reports"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('regulatory_reports.object', {
#             'object': obj
#         })
