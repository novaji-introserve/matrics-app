# -*- coding: utf-8 -*-
from odoo import http
from odoo.http import request

class Mydashboard(http.Controller):
    @http.route('/dashboard/user', auth='public', type='json')
    def index(self, **kw):
        user = request.env.user
        
        domain = [
            '|',
            '|',
            '|',
            '|',
            ('specific_email_recipients', 'in', user.ids),
            ('alert_id.email_cc', 'in', user.ids),
            ('alert_id.email', 'in', user.ids),
            ('first_owner', '=', user.id),
            ('second_owner', '=', user.id),
        ]
        

   
        result = {
            "group": any(group.name.lower() == 'chief compliance officer' for group in user.groups_id),
            "branch": [branch.id for branch in user.branches_id],
            "alert_rules_domain": domain 
        }
        return result

#     @http.route('/mydashboard/mydashboard/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('mydashboard.listing', {
#             'root': '/mydashboard/mydashboard',
#             'objects': http.request.env['mydashboard.mydashboard'].search([]),
#         })

#     @http.route('/mydashboard/mydashboard/objects/<model("mydashboard.mydashboard"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('mydashboard.object', {
#             'object': obj
        # })
