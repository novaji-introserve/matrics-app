# -*- coding: utf-8 -*-
from odoo import http
from odoo.http import request
import json
from datetime import datetime, timedelta
from odoo.fields import Datetime

class CaseDashboardController(http.Controller):

    @http.route('/case_dashboard/data', type='json', auth='user')
    def get_dashboard_data(self, period='30d'):
        Case = request.env['case']

        domain = []
        now = Datetime.now()
        if period == '00d':
            domain.append(('create_date', '>=', now - timedelta(days=0)))
        elif period == '1d':
            domain.append(('create_date', '>=', now - timedelta(days=1)))
        elif period == '3d':
            domain.append(('create_date', '>=', now - timedelta(days=3)))
        elif period == '5d':
            domain.append(('create_date', '>=', now - timedelta(days=5)))
        elif period == '7d':
            domain.append(('create_date', '>=', now - timedelta(days=7)))
        elif period == '30d':
            domain.append(('create_date', '>=', now - timedelta(days=30)))
        elif period == '90d':
            domain.append(('create_date', '>=', now - timedelta(days=90)))
        # else: show all

        # KPI Counts (filtered)
        all_cases = Case.search(domain)
        open_cases = all_cases.filtered(lambda c: c.status_id.name == 'open')
        closed_cases = all_cases.filtered(lambda c: c.status_id.name == 'closed')
        overdue_cases = all_cases.filtered(lambda c: c.status_id.name == 'overdue')

        def count_by(severity):
            return len(all_cases.filtered(lambda c: c.title == severity))
        
        # Calculate the total number of cases
        total_cases = len(all_cases)

        

        dashboard_data = {
            'kpi_data': {
                'all_cases': {'value': total_cases, 'percentage': 100 if total_cases else 0},
                'open_cases': {
                    'value': len(open_cases),
                    'percentage': round((len(open_cases) / total_cases * 100)) if total_cases else 0
                },
                'closed_cases': {
                    'value': len(closed_cases),
                    'percentage': round((len(closed_cases) / total_cases * 100)) if total_cases else 0
                },
                'overdue_cases': {
                    'value': len(overdue_cases),
                    'percentage': round((len(overdue_cases) / total_cases * 100)) if total_cases else 0
                },
            },
        





        # dashboard_data = {
        #      'kpi_data': {
        #         'all_cases': {'value': len(all_cases), 'percentage': 0},
        #         'open_cases': {'value': len(open_cases), 'percentage': 0},
        #         'closed_cases': {'value': len(closed_cases), 'percentage': 0},
        #         'overdue_cases': {'value': len(overdue_cases), 'percentage': 0},
        #     },
                
            
            
            
            
            
        #                 'kpi_data': {
        #             'all_cases': {
        #                 'value': total_cases, 
        #                 'percentage': 100
        #             },
        #             'open_cases': {
        #                 'value': len(open_cases), 
        #                 'percentage': round((len(open_cases) / total_cases * 100)) if total_cases else 0
        #             },
        #             'closed_cases': {
        #                 'value': len(closed_cases), 
        #                 'percentage': round((len(closed_cases) / total_cases * 100)) if total_cases else 0
        #             },
        #             'overdue_cases': {
        #                 'value': len(overdue_cases), 
        #                 'percentage': round((len(overdue_cases) / total_cases * 100)) if total_cases else 0
        #             },
        #                 },
           
            'chart_data': {
                'cases_by_category': {
                    'process': [{'label': data.get('process_category_id') and data['process_category_id'][1] or 'N/A',
                                 'value': data['process_category_id_count']}
                                for data in Case.read_group(domain, ['process_category_id'], ['process_category_id'])],
                    'root': [{'label': data.get('root_category_id') and data['root_category_id'][1] or 'N/A',
                              'value': data['root_category_id_count']}
                             for data in Case.read_group(domain, ['root_category_id'], ['root_category_id'])],
                },
                'case_rate': {
                    'labels': ['Open', 'Close', 'Overdue'],
                    'datasets': [
                        {
                            'label': 'Cases',
                            'data': [len(open_cases), len(closed_cases), len(overdue_cases)],
                            'backgroundColor': ['yellow', 'blue', 'red']
                        }
                    ],
                },
                'cases_by_severity': {
                    'labels': ['Low', 'Medium', 'High'],
                    'datasets': [
                        {
                            'label': 'Severity',
                            'data': [count_by('1'), count_by('2'), count_by('3')],
                            'backgroundColor': ['red', 'yellow', 'blue']
                        }
                    ],
                },
                'cases_by_status': {
                    'labels': [status.name.capitalize() for status in request.env['case.status'].search([])],
                    'datasets': [
                        {
                            'label': 'Status',
                            'data': [Case.search_count(domain + [('status_id', '=', status.id)])
                                     for status in request.env['case.status'].search([])],
                            'backgroundColor': ['yellow', 'green', 'red']
                        }
                    ],
                },
            },
        }
        return dashboard_data

















# # -*- coding: utf-8 -*-
# from odoo import http
# from odoo.http import request
# import json

# class CaseDashboardController(http.Controller):
#     @http.route('/case_dashboard/data', type='json', auth='user')
#     def get_dashboard_data(self):
#         Case = request.env['case']

#         # KPI Counts
#         all_cases_count = Case.search_count([])
#         open_cases_count = Case.search_count([('status_id.name', '=', 'open')])
#         closed_cases_count = Case.search_count([('status_id.name', '=', 'closed')])
#         overdue_cases_count = Case.search_count([('status_id.name', '=', 'overdue')])

#         # Cases By Category Chart Data
#         cases_by_process = Case.read_group([], ['process_category_id'], ['process_category_id'])
#         cases_by_root = Case.read_group([], ['root_category_id'], ['root_category_id'])

#         # cases_by_process = Case.read_group([], ['process_category_id'], ['process_category_id:count'])
#         # cases_by_root = Case.read_group([], ['root_category_id'], ['root_category_id:count'])

#         # Case Rate Chart Data
#         dashboard_data = {
#             'kpi_data': {
#                 'all_cases': {'value': all_cases_count, 'percentage': 0},
#                 'open_cases': {'value': open_cases_count, 'percentage': 0},
#                 'closed_cases': {'value': closed_cases_count, 'percentage': 0},
#                 'overdue_cases': {'value': overdue_cases_count, 'percentage': 0},
#             },
#             'chart_data': {
#                 'cases_by_category': {
#                     'process': [{'label': data.get('process_category_id') and data['process_category_id'][1] or 'N/A', 'value': data['process_category_id_count']} for data in cases_by_process],
#                     'root': [{'label': data.get('root_category_id') and data['root_category_id'][1] or 'N/A', 'value': data['root_category_id_count']} for data in cases_by_root],
#                 },
#                 'case_rate': {
#                     'labels': ['Open', 'Close', 'Overdue'],
#                     'datasets': [
#                         {'label': 'Cases',
#                          'data': [open_cases_count, closed_cases_count, overdue_cases_count],
#                          'backgroundColor': ['yellow', 'blue', 'red']},
#                     ],
#                 },
#                 'cases_by_severity': {
#                     'labels': ['Low', 'Medium', 'High'],
#                     'datasets': [
#                         {'label': 'Severity',
#                          'data': [
#                              Case.search_count([('title', '=', '1')]),
#                              Case.search_count([('title', '=', '2')]),
#                              Case.search_count([('title', '=', '3')]),
#                          ],
#                          'backgroundColor': ['red', 'yellow', 'blue']},
#                     ],
#                 },
#                 'cases_by_status': {
#                     'labels': [status['name'].capitalize() for status in request.env['case.status'].search([])],
#                     'datasets': [
#                         {'label': 'Status',
#                          'data': [Case.search_count([('status_id', '=', status.id)]) for status in request.env['case.status'].search([])],
#                          'backgroundColor': ['yellow', 'green', 'red']}, # Using colors from your template
#                     ],
#                 },
#             },
#         }
#         return dashboard_data




















# # # -*- coding: utf-8 -*-
# # from odoo import http
# # from odoo.http import request
# # import json

# # class CaseDashboardController(http.Controller):
# #     @http.route('/case_dashboard/data', type='json', auth='user')
# #     def get_dashboard_data(self):
# #         Case = request.env['case']

# #         # KPI Counts
# #         all_cases_count = Case.search_count([])
# #         open_cases_count = Case.search_count([('status_id.name', '=', 'open')])
# #         closed_cases_count = Case.search_count([('status_id.name', '=', 'closed')])
# #         overdue_cases_count = Case.search_count([('status_id.name', '=', 'overdue')])

# #         # Cases By Category Chart Data
# #         cases_by_process = Case.read_group([], ['process_category_id'], ['process_category_id:count'])
# #         cases_by_root = Case.read_group([], ['root_category_id'], ['root_category_id:count'])

# #         # Case Rate Chart Data
# #         dashboard_data = {
# #             'kpi_data': {
# #                 'all_cases': {'value': all_cases_count, 'percentage': 0},
# #                 'open_cases': {'value': open_cases_count, 'percentage': 0},
# #                 'closed_cases': {'value': closed_cases_count, 'percentage': 0},
# #                 'overdue_cases': {'value': overdue_cases_count, 'percentage': 0},
# #             },
# #             'chart_data': {
# #                 'cases_by_category': {
# #                     'process': [{'label': data['process_category_id'][1] if data['process_category_id'] else 'N/A', 'value': data['process_category_id_count']} for data in cases_by_process],
# #                     'root': [{'label': data['root_category_id'][1] if data['root_category_id'] else 'N/A', 'value': data['root_category_id_count']} for data in cases_by_root],
# #                 },
# #                 'case_rate': {
# #                     'labels': ['Open', 'Close', 'Overdue'],
# #                     'datasets': [
# #                         {'label': 'Cases',
# #                          'data': [open_cases_count, closed_cases_count, overdue_cases_count],
# #                          'backgroundColor': ['yellow', 'blue', 'red']},
# #                     ],
# #                 },
# #                 'cases_by_severity': {
# #                     'labels': ['Low', 'Medium', 'High'],
# #                     'datasets': [
# #                         {'label': 'Severity',
# #                          'data': [
# #                              Case.search_count([('title', '=', '1')]),
# #                              Case.search_count([('title', '=', '2')]),
# #                              Case.search_count([('title', '=', '3')]),
# #                          ],
# #                          'backgroundColor': ['red', 'yellow', 'blue']},
# #                     ],
# #                 },
# #                 'cases_by_status': {
# #                     'labels': [status['name'].capitalize() for status in request.env['case.status'].search([])],
# #                     'datasets': [
# #                         {'label': 'Status',
# #                          'data': [Case.search_count([('status_id', '=', status.id)]) for status in request.env['case.status'].search([])],
# #                          'backgroundColor': ['yellow', 'green', 'red']}, # Using colors from your template
# #                     ],
# #                 },
# #             },
# #         }
# #         return dashboard_data