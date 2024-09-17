# -*- coding: utf-8 -*-
# from odoo import http


# class AlertManagment(http.Controller):
#     @http.route('/alert_managment/alert_managment', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/alert_managment/alert_managment/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('alert_managment.listing', {
#             'root': '/alert_managment/alert_managment',
#             'objects': http.request.env['alert_managment.alert_managment'].search([]),
#         })

#     @http.route('/alert_managment/alert_managment/objects/<model("alert_managment.alert_managment"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('alert_managment.object', {
#             'object': obj
#         })
