# # -*- coding: utf-8 -*-
from odoo import http
from odoo.http import request
from datetime import datetime, timedelta
from odoo import fields

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
            "branch": [],  # Initialize branch as an empty list
            "alert_rules_domain": domain,
        }

        if hasattr(user, 'branches_id') and user.branches_id:  # Check if branches_id exists and is not empty
            result["branch"] = [branch.id for branch in user.branches_id]

        return result

    def check_branches_id(self, branches_id):
        # Ensure branches_id is a list
        if not isinstance(branches_id, list):
            branches_id = [branches_id]  # Convert to list if it's a single integer
            return branches_id
        else:
            return branches_id

    @http.route('/dashboard/get_top_screening_rules', auth='public', type='json')
    def get_top_screening(self, cco, branches_id, datepicked, **kw):
        
        today = datetime.now().date()  # Get today's date
        prev_date = today - timedelta(days=datepicked)  # Get previous date

        def _execute_query(sql, params=None):
            request.env.cr.execute(sql, params) if params else request.env.cr.execute(sql)
            results = request.env.cr.fetchall()
            return [{
                "id": row[0],
                'name': row[1],
                'count': row[2]
            } for row in results]

        if cco:
            if datepicked > 0:
                sql = """
                    SELECT rtsr.id, rtsr.name, COUNT(rct.id) AS hit_count
                    FROM res_transaction_screening_rule rtsr
                    JOIN res_customer_transaction rct ON rtsr.id = rct.rule_id
                    WHERE rct.date_created BETWEEN %s AND %s
                    GROUP BY rtsr.id, rtsr.name
                    ORDER BY hit_count DESC
                    LIMIT 10;
                """
                return _execute_query(sql, (prev_date, today))
            else:  # datepicked == 0

                sql = """
                    SELECT rtsr.id, rtsr.name, COUNT(rct.id) AS hit_count
                    FROM res_transaction_screening_rule rtsr
                    JOIN res_customer_transaction rct ON rtsr.id = rct.rule_id
                    GROUP BY rtsr.id, rtsr.name
                    ORDER BY hit_count DESC
                    LIMIT 10;
                """
                return _execute_query(sql)

        else: # cco == False
            if not branches_id:
                return []

            if datepicked > 0:
                sql = """
                SELECT rtsr.id, rtsr.name, COUNT(rct.id) AS hit_count
                FROM res_transaction_screening_rule rtsr
                JOIN res_customer_transaction rct ON rtsr.id = rct.rule_id
                WHERE rct.date_created BETWEEN %s AND %s AND rct.branch_id = ANY(%s)
                GROUP BY rtsr.id, rtsr.name
                ORDER BY hit_count DESC
                LIMIT 10;
                """
    
                try:
                    return _execute_query(sql, (prev_date, today, (branches_id, )))
                except Exception as e:
                    print(f"error occured {str(e)}")
            else:
                sql = """
                SELECT rtsr.id, rtsr.name, COUNT(rct.id) AS hit_count
                FROM res_transaction_screening_rule rtsr
                JOIN res_customer_transaction rct ON rtsr.id = rct.rule_id
                WHERE rct.branch_id = ANY(%s)
                GROUP BY rtsr.id, rtsr.name
                ORDER BY hit_count DESC
                LIMIT 10;

                """
    
                try:
                    return _execute_query(sql, (branches_id,))
                except Exception as e:
                    print(f"error occured {str(e)}")

    @http.route('/dashboard/get_high_risk_customer_by_branch', auth='public', type='json')
    def get_high_risk(self, cco, branches_id, datepicked, **kw):

        print("**********")
        print(tuple(branches_id))

        today = datetime.now().date()  # Get today's date
        prev_date = today - timedelta(days=datepicked)  # Get previous date

        def _execute_query(sql, params=None):
            request.env.cr.execute(sql, params) if params else request.env.cr.execute(sql)
            results = request.env.cr.fetchall()
            return [{
                "id": row[0],
                'name': row[1],
                'count': row[2]
            } for row in results]

        if cco:
            if datepicked > 0:
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
                sql = """
                    SELECT rb.id, rb.name, COUNT(rp.id) AS high_risk_customers
                    FROM res_branch rb
                    JOIN res_partner rp ON rb.id = rp.branch_id
                    WHERE rp.risk_level = 'high'
                    GROUP BY rb.id, rb.name
                    ORDER BY high_risk_customers DESC
                    LIMIT 10
                """
                return _execute_query(sql)

        else:
            if not branches_id:
                return []

            if datepicked == 0:
                sql = """
                    SELECT rb.id, rb.name, COUNT(rp.id) AS high_risk_customers
                    FROM res_branch rb
                    JOIN res_partner rp ON rb.id = rp.branch_id
                    WHERE rp.risk_level = 'high' AND rb.id = ANY(%s)
                    GROUP BY rb.id, rb.name
                    ORDER BY high_risk_customers DESC
                    LIMIT 10

                    """
        
                try:
                    return _execute_query(sql, (branches_id,))
                except Exception as e:
                    print(f"error occured {str(e)}")

            else:
                sql = """
                    SELECT rb.id, rb.name, COUNT(rp.id) AS high_risk_customers
                    FROM res_branch rb
                    JOIN res_partner rp ON rb.id = rp.branch_id
                    WHERE rp.risk_level = 'high' AND rp.create_date BETWEEN %s AND %s AND rb.id = ANY(%s)
                    GROUP BY rb.id, rb.name
                    ORDER BY high_risk_customers DESC
                    LIMIT 10

                    """
                try:
                    return _execute_query(sql, (prev_date, today, (branches_id,)))
                except Exception as e:
                    print(f"error occured {str(e)}")

    @http.route('/dashboard/branch_by_customer', auth='public', type='json')
    def get_branch_by_customer(self, cco, branches_id, datepicked, **kw):


        today = datetime.now().date()  # Get today's date
        prev_date = today - timedelta(days=datepicked)  # Get previous date

        start_of_prev_day = fields.Datetime.to_string(datetime.combine(prev_date, datetime.min.time()))
        end_of_today = fields.Datetime.to_string(datetime.combine(today, datetime.max.time()))

        def _execute_query(sql, params=None):
            try:
                request.env.cr.execute(sql, params) if params else request.env.cr.execute(sql)
                results = request.env.cr.fetchall()
                return [{
                    "id": row[0],
                    'branch_name': row[1],
                    'customer_count': row[2]
                } for row in results]

            except Exception as e:
                print(e)

        if cco:
            if datepicked == 0:
                sql = """
                    SELECT rb.id, rb.name, COUNT(rp.id) AS customer_count
                    FROM res_branch rb
                    JOIN res_partner rp ON rb.id = rp.branch_id
                    GROUP BY rb.id, rb.name
                    ORDER BY customer_count DESC
                    LIMIT 10;
                """
                return _execute_query(sql)
            else:  # datepicked > 0
                sql = """
                    SELECT rb.id, rb.name, COUNT(rp.id) AS customer_count
                    FROM res_branch rb
                    JOIN res_partner rp ON rb.id = rp.branch_id
                    WHERE rp.create_date BETWEEN %s AND %s
                    GROUP BY rb.id, rb.name
                    ORDER BY customer_count DESC
                    LIMIT 10;
                """
                return _execute_query(sql, (start_of_prev_day, end_of_today))
        else:  # cco == False

            if not branches_id:
                return []  # Return empty list if no branches provided

            if datepicked == 0:
                sql = """
                    SELECT rb.id, rb.name, COUNT(rp.id) AS customer_count
                    FROM res_branch rb
                    JOIN res_partner rp ON rb.id = rp.branch_id
                    WHERE rb.id = ANY(%s)
                    GROUP BY rb.id, rb.name
                    ORDER BY customer_count DESC;
                """
                
                try:
                    return _execute_query(sql, (branches_id,)) 
                except Exception as e:
                    print(f"error occured {str(e)}")
            else:  # datepicked > 0
                sql = """
                    SELECT rb.id, rb.name, COUNT(rp.id) AS customer_count
                    FROM res_branch rb
                    JOIN res_partner rp ON rb.id = rp.branch_id
                    WHERE rb.id = ANY(%s) AND rp.create_date BETWEEN %s AND %s
                    GROUP BY rb.id, rb.name
                    ORDER BY customer_count DESC;
                """
                
                try:
                    return _execute_query(sql, ((branches_id, ), start_of_prev_day, end_of_today))
                except Exception as e:
                    print(f"error occured {str(e)}")
