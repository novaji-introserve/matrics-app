# -*- coding: utf-8 -*-
# from odoo import http


# class RuleBook(http.Controller):
#     @http.route('/rule_book/rule_book', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/rule_book/rule_book/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('rule_book.listing', {
#             'root': '/rule_book/rule_book',
#             'objects': http.request.env['rule_book.rule_book'].search([]),
#         })

#     @http.route('/rule_book/rule_book/objects/<model("rule_book.rule_book"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('rule_book.object', {
#             'object': obj
#         })
