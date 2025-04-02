# # -*- coding: utf-8 -*-
from odoo import http
from odoo.http import request
from datetime import datetime, timedelta
from odoo import fields


class Compliance(http.Controller):
    @http.route('/dashboard/user', auth='public', type='json')
    def index(self, **kw):

        user = request.env.user

        is_superuser = user.has_group('base.group_system')

        group = any(group.name.lower() == 'chief compliance officer' for group in user.groups_id)
        branch = [branch.id for branch in user.branches_id] 

        result = {
            "group": group,
            "branch": branch,
        }
        return result
    
    def check_branches_id(self, branches_id):
        # Ensure branches_id is a list
        if not isinstance(branches_id, list):
            branches_id = [branches_id]  # Convert to list if it's a single integer
            return branches_id
        else:
            return branches_id

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

            query = """
            SELECT rcs.*
            FROM res_compliance_stat rcs
            WHERE rcs.create_date >= %s
              AND rcs.create_date < %s
              AND rcs.create_uid IN (
                  SELECT rbur.user_id
                  FROM res_branch_users_rel rbur
                  WHERE rbur.branch_id = ANY(%s::integer[])
              );
            """
            branches_id = self.check_branches_id(branches_id)

            request.env.cr.execute(query, (start_of_prev_day, end_of_today, branches_id))

            # Fetch results
            # Get column names
            columns = [desc[0] for desc in request.env.cr.description]
            result_tuple = request.env.cr.fetchall()

            results = [dict(zip(columns, row)) for row in result_tuple]

            computed_results = []

            for result in results:
                computed_results.append({"name": result["name"],"scope": result["scope"], "val": result["val"], "id": result["id"], "scope_color": result["scope_color"]})

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

            query = """
            SELECT rcs.*
            FROM res_compliance_stat rcs
            WHERE rcs.create_date >= %s
              AND rcs.create_date < %s
              AND rcs.create_uid IN (
                  SELECT rbur.user_id
                  FROM res_branch_users_rel rbur
                  WHERE rbur.branch_id = ANY(%s::integer[])
              );
            """
            branches_id = self.check_branches_id(branches_id)

            request.env.cr.execute(query, (start_of_prev_day, end_of_today, branches_id))

            # Fetch results
            # Get column names
            columns = [desc[0] for desc in request.env.cr.description]
            result_tuple = request.env.cr.fetchall()

            results = [dict(zip(columns, row)) for row in result_tuple]

            computed_results = []

            for result in results:
                computed_results.append({"name": result["name"],"scope": result["scope"], "val": result["val"], "id": result["id"], "scope_color": result["scope_color"]})

            return {
                    "data": computed_results,
                    "total": len(results)
            }

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

            if not branches_id:
                return []
                
            sql = """
            SELECT rtsr.id, rtsr.name, COUNT(rct.id) AS hit_count
            FROM res_transaction_screening_rule rtsr
            JOIN res_customer_transaction rct ON rtsr.id = rct.rule_id
            WHERE rct.date_created BETWEEN %s AND %s AND rct.branch_id IN %s
            GROUP BY rtsr.id, rtsr.name
            ORDER BY hit_count DESC
            LIMIT 10;
            # """
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

    @http.route('/dashboard/get_high_risk_customer_by_branch', auth='public', type='json')
    def get_high_risk(self, cco, branches_id, datepicked, **kw):

        today = datetime.now().date()  # Get today's date
        prev_date = today - timedelta(days=datepicked)  # Get previous date


        print(today)
        print(prev_date)

        

        def _execute_query(sql, params=None):
            request.env.cr.execute(sql, params) if params else request.env.cr.execute(sql)
            results = request.env.cr.fetchall()
            return [{
                "id": row[0],
                'name': row[1],
                'count': row[2]
            } for row in results]

        if cco:
            sql = """
                    SELECT rb.id, rb.name, COUNT(rp.id) AS high_risk_customers
                    FROM res_branch rb
                    JOIN res_partner rp ON rb.id = rp.branch_id
                    WHERE rp.risk_level = 'high' AND rp.create_date BETWEEN %s AND %s 
                    GROUP BY rb.id, rb.name
                    ORDER BY high_risk_customers DESC
                    LIMIT 10
                """
            return _execute_query(sql, (prev_date, today))

        else:

            if not branches_id:
                return []

            sql = """
                    SELECT rb.id, rb.name, COUNT(rp.id) AS high_risk_customers
                    FROM res_branch rb
                    JOIN res_partner rp ON rb.id = rp.branch_id
                    WHERE rp.risk_level = 'high' AND rp.create_date BETWEEN %s AND %s AND rb.id IN %s
                    GROUP BY rb.id, rb.name
                    ORDER BY high_risk_customers DESC
                    LIMIT 10

                    """
            return _execute_query(sql, (prev_date, today,tuple(branches_id)))

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
