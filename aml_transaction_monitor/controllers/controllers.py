# -*- coding: utf-8 -*-
# from odoo import http


# class AmlTransactionMonitor(http.Controller):
#     @http.route('/aml_transaction_monitor/aml_transaction_monitor', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/aml_transaction_monitor/aml_transaction_monitor/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('aml_transaction_monitor.listing', {
#             'root': '/aml_transaction_monitor/aml_transaction_monitor',
#             'objects': http.request.env['aml_transaction_monitor.aml_transaction_monitor'].search([]),
#         })

#     @http.route('/aml_transaction_monitor/aml_transaction_monitor/objects/<model("aml_transaction_monitor.aml_transaction_monitor"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('aml_transaction_monitor.object', {
#             'object': obj
#         })
