# -*- coding: utf-8 -*-
from odoo import http
from odoo.http import request
from datetime import datetime, timedelta
from odoo import fields


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
    
    @http.route('/dashboard/get_top_screened_rules', auth='public', type='json')
    def get_transaction_data(self, cco, branches_id, datepicked, **kw):

        today = datetime.now().date()  # Get today's date
        prevDate = today - timedelta(days=datepicked)  # Get previous date

    
        if cco == True:
            # fetch all data for chief compliance officer
            sql = """
                SELECT rtsr.id, rtsr.name, COUNT(rct.id) AS hit_count
                FROM res_transaction_screening_rule rtsr
                JOIN res_customer_transaction rct ON rtsr.id = rct.rule_id
                WHERE rct.date_created BETWEEN %s AND %s
                GROUP BY rtsr.id, rtsr.name
                ORDER BY hit_count DESC
                LIMIT 10;
            """
            request.env.cr.execute(sql,(prevDate, today))

            results = request.env.cr.fetchall()

            customer_counts = []

            for row in results:
                customer_counts.append({
                    "id": row[0],
                    'name': row[1],
                    'count': row[2]
                })

            return customer_counts

        else:
            sql = """
            SELECT rtsr.id, rtsr.name, COUNT(rct.id) AS hit_count
            FROM res_transaction_screening_rule rtsr
            JOIN res_customer_transaction rct ON rtsr.id = rct.rule_id
            WHERE rct.date_created BETWEEN %s AND %s AND rct.branch_id IN %s
            GROUP BY rtsr.id, rtsr.name
            ORDER BY hit_count DESC
            LIMIT 10;
            """
            request.env.cr.execute(sql,(prevDate, today, tuple(branches_id)))

            results = request.env.cr.fetchall()

            customer_counts = []

            for row in results:
                customer_counts.append({
                    "id": row[0],
                    'name': row[1],
                    'count': row[2]
                })

            return customer_counts



    # @http.route('/dashboard/get_branch_by_customer', auth='public', type='json')
    # def get_branch_data(self, cco, branches_id, datepicked, **kw):

    #     today = datetime.now().date()  # Get today's date
    #     prevDate = today - timedelta(days=datepicked)  # Get previous date

    #     # Convert to datetime for start and end of the day
    #     start_of_prev_day = fields.Datetime.to_string(datetime.combine(prevDate, datetime.min.time()))

    #     end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))

        

    #     if cco ==True:
    #         sql = """
    #         SELECT rb.id, rb.name, COUNT(rp.id) AS customer_count
    #         FROM res_branch rb
    #         JOIN res_partner rp ON rb.id = rp.branch_id
    #         WHERE rp.create_date BETWEEN %s AND %s
    #         GROUP BY rb.id, rb.name
    #         ORDER BY customer_count DESC
    #         LIMIT 10;
    #     """
    #         request.env.cr.execute(sql,(start_of_prev_day, end_of_today))

    #         results = request.env.cr.fetchall()

    #         customer_counts = []

    #         for row in results:
    #             customer_counts.append({
    #                 "id": row[0],
    #                 'branch_name': row[1],
    #                 'customer_count': row[2]
    #             })

    #         return customer_counts

    #     else:
    #         sql = """
    #         SELECT rb.id, rb.name, COUNT(rp.id) AS customer_count
    #         FROM res_branch rb
    #         JOIN res_partner rp ON rb.id = rp.branch_id
    #         WHERE rb.id IN %s AND rp.create_date BETWEEN %s AND %s
    #         GROUP BY rb.id, rb.name
    #         ORDER BY customer_count DESC;

    #     """
    #         request.env.cr.execute(sql, (tuple(branches_id),start_of_prev_day, end_of_today))
    #         results = request.env.cr.fetchall()

    #         customer_counts = []

    #         for row in results:
    #             customer_counts.append({
    #                 "id": row[0],
    #                 'branch_name': row[1],
    #                 'customer_count': row[2]
    #             })

    #         return customer_counts

    @http.route('/dashboard/get_branch_by_customer', auth='public', type='json')
    def get_branch_data(self, cco, branches_id, datepicked, **kw):
        today = datetime.now().date()
        prevDate = today - timedelta(days=datepicked)
        
        start_of_prev_day = fields.Datetime.to_string(datetime.combine(prevDate, datetime.min.time()))
        end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))
        
        if cco == True:
            # This part of the code works fine
            sql = """
            SELECT rb.id, rb.name, COUNT(rp.id) AS customer_count
            FROM res_branch rb
            JOIN res_partner rp ON rb.id = rp.branch_id
            WHERE rp.create_date BETWEEN %s AND %s
            GROUP BY rb.id, rb.name
            ORDER BY customer_count DESC
            LIMIT 10;
            """
            request.env.cr.execute(sql, (start_of_prev_day, end_of_today))
            
            results = request.env.cr.fetchall()
            
            customer_counts = []
            
            for row in results:
                customer_counts.append({
                    "id": row[0],
                    'branch_name': row[1],
                    'customer_count': row[2]
                })
            
            return customer_counts
        
        else:
            # Check if branches_id is empty
            if not branches_id:
                return []  # Return empty result if no branches provided
            
            sql = """
            SELECT rb.id, rb.name, COUNT(rp.id) AS customer_count
            FROM res_branch rb
            JOIN res_partner rp ON rb.id = rp.branch_id
            WHERE rb.id IN %s AND rp.create_date BETWEEN %s AND %s
            GROUP BY rb.id, rb.name
            ORDER BY customer_count DESC;
            """
            request.env.cr.execute(sql, (tuple(branches_id), start_of_prev_day, end_of_today))
            results = request.env.cr.fetchall()
            
            customer_counts = []
            
            for row in results:
                customer_counts.append({
                    "id": row[0],
                    'branch_name': row[1],
                    'customer_count': row[2]
                })
            
            return customer_counts







        

     
   



       


     

