# -*- coding: utf-8 -*-
# from odoo import http


# class GlobalPepListWebScrapper(http.Controller):
#     @http.route('/global_pep_list_web_scrapper/global_pep_list_web_scrapper', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/global_pep_list_web_scrapper/global_pep_list_web_scrapper/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('global_pep_list_web_scrapper.listing', {
#             'root': '/global_pep_list_web_scrapper/global_pep_list_web_scrapper',
#             'objects': http.request.env['global_pep_list_web_scrapper.global_pep_list_web_scrapper'].search([]),
#         })

#     @http.route('/global_pep_list_web_scrapper/global_pep_list_web_scrapper/objects/<model("global_pep_list_web_scrapper.global_pep_list_web_scrapper"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('global_pep_list_web_scrapper.object', {
#             'object': obj
#         })
