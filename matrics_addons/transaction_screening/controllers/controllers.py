# -*- coding: utf-8 -*-
# from odoo import http


# class TransactionScreening(http.Controller):
#     @http.route('/transaction_screening/transaction_screening', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/transaction_screening/transaction_screening/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('transaction_screening.listing', {
#             'root': '/transaction_screening/transaction_screening',
#             'objects': http.request.env['transaction_screening.transaction_screening'].search([]),
#         })

#     @http.route('/transaction_screening/transaction_screening/objects/<model("transaction_screening.transaction_screening"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('transaction_screening.object', {
#             'object': obj
#         })
