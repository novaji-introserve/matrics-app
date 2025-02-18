# -*- coding: utf-8 -*-
from odoo import http
from odoo.http import request
from datetime import datetime, timedelta
from odoo import fields
# class CustomerManagement(http.Controller):
#     @http.route('/customer_management/customer_management', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/customer_management/customer_management/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('customer_management.listing', {
#             'root': '/customer_management/customer_management',
#             'objects': http.request.env['customer_management.customer_management'].search([]),
#         })

#     @http.route('/customer_management/customer_management/objects/<model("customer_management.customer_management"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('customer_management.object', {
#             'object': obj
#         })

class Compliance(http.Controller):
    @http.route('/dashboard/user', auth='public', type='json')
    def index(self, **kw):

        user = request.env.user

        group = any(group.name.lower() == 'chief compliance officer' for group in user.groups_id)
        branch = [branch.id for branch in user.branches_id]

        result = {
            "group": group,
            "branch": branch,
        }
        return result

    @http.route('/dashboard/stats', auth='public', type='json')
    def getAllstats(self, cco, branches_id, datepicked, **kw):


        today = datetime.now().date()  # Get today's date
        prevDate = today - timedelta(days=datepicked)  # Get previous date

        # Convert to datetime for start and end of the day
        start_of_prev_day = fields.Datetime.to_string(datetime.combine(prevDate, datetime.min.time()))

        end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))

    
        if cco == True:
                # fetch all data for chief compliance officer
                results = request.env["res.compliance.stat"].search([('create_date', '>=', start_of_prev_day), ('create_date', '<', end_of_today)])

                computed_results = []

                for result in results:
                   computed_results.append({"name": result["name"],"scope": result["scope"], "val": result["val"], "id": result["id"], "scope_color": result["scope_color"]})

                return {
                    "data": computed_results,
                    "total": len(results)
                }
        else:
            # fetch data for branches

            results = request.env['res.compliance.stat'].search([('create_uid.branches_id', 'in', branches_id),('create_date', '>=', start_of_prev_day), ('create_date', '<', end_of_today)])
            computed_results = []

            for result in results:
                   computed_results.append({"name": result["name"],"scope": result["scope"], "val": result["val"]})

            return {
                    "data": computed_results,
                    "total": len(results)
            }

    @http.route('/dashboard/statsbycategory', auth='public', type='json')
    def getAllstatsByCategory(self, cco, branches_id, category, datepicked, **kw):

        today = datetime.now().date()  # Get today's date
        prevDate = today - timedelta(days=datepicked)  # Get previous date

        # Convert to datetime for start and end of the day
        start_of_prev_day = fields.Datetime.to_string(datetime.combine(prevDate, datetime.min.time()))

        end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))


    
        if cco == True:
                # fetch all data for chief compliance officer
                results = request.env["res.compliance.stat"].search([('scope', '=', category),('create_date', '>=', start_of_prev_day), ('create_date', '<', end_of_today)])

                computed_results = []

                for result in results:
                   computed_results.append({"name": result["name"],"scope": result["scope"], "val": result["val"], "id": result["id"], "scope_color": result["scope_color"]})

                return {
                    "data": computed_results,
                    "total": len(results)
                }
        else:
            # fetch data for branches

            results = request.env['res.compliance.stat'].search([('create_uid.branches_id', 'in', branches_id), ('scope', '=', category),('create_date', '>=', start_of_prev_day), ('create_date', '<', end_of_today)])
            computed_results = []
            for result in results:
                   computed_results.append({"name": result["name"],"scope": result["scope"], "val": result["val"]})

            return {
                    "data": computed_results,
                    "total": len(results)
            }

    @http.route('/dashboard/get_scope_data', auth='public', type='json')
    def get_scope_data(self, cco, branches_id, datepicked, **kw):

        today = datetime.now().date()  # Get today's date
        prevDate = today - timedelta(days=datepicked)  # Get previous date

        # Convert to datetime for start and end of the day
        start_of_prev_day = fields.Datetime.to_string(datetime.combine(prevDate, datetime.min.time()))

        end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))


    
        if cco == True:
            # fetch all data for chief compliance officer
            stats = request.env["res.compliance.stat"].search([('create_date', '>=', start_of_prev_day), ('create_date', '<', end_of_today)])

    
        
            # Initialize a dictionary to hold the grouped data
            grouped_data = {}
            
            # Loop through the records and group by scope and name
            for stat in stats:
                if stat.scope not in grouped_data:
                    grouped_data[stat.scope] = {
                        'total_value': 1,  # Start with 1 for the first occurrence
                        'records': [{
                            'name': stat.name,  # Assuming 'name' is an attribute on your model
                            'value': stat.val  # Assuming 'val' is an attribute on your model
                        }]
                    }
                else:
                    grouped_data[stat.scope]['total_value'] += 1  # Increment total_value
                    grouped_data[stat.scope]['records'].append({
                        'name': stat.name,  # Append the record for this scope
                        'value': stat.val
                    })

            # Prepare the data for the bar chart
            chart_data = []
            for scope, data in grouped_data.items():
                chart_data.append({
                    'scope': scope,
                    'total_value': data['total_value'],
                    'records': data['records']
                })
            



            return chart_data
        else:
            # fetch data for branches

            stats = request.env['res.compliance.stat'].search([('create_uid.branches_id', 'in', branches_id),('create_date', '>=', start_of_prev_day), ('create_date', '<', end_of_today)])
            
            grouped_data = {}
            
           # Loop through the records and group by scope and name
            for stat in stats:
                if stat.scope not in grouped_data:
                    grouped_data[stat.scope] = {
                        'total_value': 1,  # Start with 1 for the first occurrence
                        'records': [{
                            'name': stat.name,  # Assuming 'name' is an attribute on your model
                            'value': stat.val  # Assuming 'val' is an attribute on your model
                        }]
                    }
                else:
                    grouped_data[stat.scope]['total_value'] += 1  # Increment total_value
                    grouped_data[stat.scope]['records'].append({
                        'name': stat.name,  # Append the record for this scope
                        'value': stat.val
                    })

            # Prepare the data for the bar chart
            chart_data = []
            for scope, data in grouped_data.items():
                chart_data.append({
                    'scope': scope,
                    'total_value': data['total_value'],
                    'records': data['records']
                })

            return chart_data
    
    @http.route('/dashboard/get_transaction', auth='public', type='json')
    def get_transaction_data(self, cco, branches_id, datepicked, **kw):

        today = datetime.now().date()  # Get today's date
        prevDate = today - timedelta(days=datepicked)  # Get previous date

        # Convert to datetime for start and end of the day
        start_of_prev_day = fields.Datetime.to_string(datetime.combine(prevDate, datetime.min.time()))

        end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))


    
        if cco == True:
            # fetch all data for chief compliance officer
            transactions = request.env["res.customer.transaction"].search([('create_date', '>=', start_of_prev_day), ('create_date', '<', end_of_today)])

            # Initialize a dictionary to hold the grouped data
            grouped_data = {}
            
            # Loop through the records and group by scope and name
            for transaction in transactions:
                if transaction.risk_level not in grouped_data:
                    grouped_data[transaction.risk_level] = 1  # Start with 1 for the first occurrence
                    
                else:
                    grouped_data[transaction.risk_level] += 1  # Increment total_value
            
            labels = []
            values = []

            for key, value in grouped_data.items():

                labels.append(key)
                values.append(value)

            return {
                "labels": labels,
                "values": values
            }
        
        else:
            transactions = request.env["res.customer.transaction"].search([('create_date', '>=', start_of_prev_day), ('create_date', '<', end_of_today), ('create_uid.branches_id', 'in', branches_id)])

            # Initialize a dictionary to hold the grouped data
            grouped_data = {}

            for transaction in transactions:
                if transaction.risk_level not in grouped_data:
                    grouped_data[transaction.risk_level] = 1  # Start with 1 for the first occurrence
                    
                else:
                    grouped_data[transaction.risk_level] += 1  # Increment total_value
            
            labels = []
            values = []

            for key, value in grouped_data.items():

                labels.append(key)
                values.append(value)

            return {
                "labels": labels,
                "values": values
            }
                   





        

     
   



       


     

