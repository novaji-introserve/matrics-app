# -*- coding: utf-8 -*-
# from odoo import http


# class AlertManagement(http.Controller):
#     @http.route('/alert_management/alert_management', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/alert_management/alert_management/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('alert_management.listing', {
#             'root': '/alert_management/alert_management',
#             'objects': http.request.env['alert_management.alert_management'].search([]),
#         })

#     @http.route('/alert_management/alert_management/objects/<model("alert_management.alert_management"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('alert_management.object', {
#             'object': obj
#         })
