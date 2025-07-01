
# -*- coding: utf-8 -*-

from odoo import models, api
from odoo import models, fields, api, _, tools
from psycopg2 import ProgrammingError
import logging
from dotenv import load_dotenv
import psycopg2
import os
from datetime import timedelta, datetime
import pytz
from dateutil.relativedelta import relativedelta
from odoo.exceptions import UserError, AccessError
from psycopg2.extras import execute_values
import time
from datetime import datetime, timedelta




load_dotenv()
_logger = logging.getLogger(__name__)



class Shareholders(models.Model):
    _name = 'res.partner.shareholders'
    _description = 'Shareholders and Directors'

    name = fields.Char(string='Name', required=True, tracking=True)
    role = fields.Selection(string='Role', selection=[(
        'director', 'Director'), ('shareholder', 'shareholder')])
    pct_equity = fields.Float(
        string='Equity (%)', digits=(10, 2), tracking=True)
    bvn = fields.Char(string='BVN', tracking=True)
    customer_id = fields.Many2one(
        comodel_name='res.partner', string='Partner', ondelete="cascade")


class PartnerRiskPlanLines(models.Model):
    _name = "res.partner.risk.plan.line"
    _description = "Partner Risk Analysis Lines"
    partner_id = fields.Many2one(
        'res.partner', string='Partner', ondelete="cascade", index=True)
    plan_line_id = fields.Many2one(
        'res.compliance.risk.assessment.plan', string='Plan Line', index=True)
    risk_score = fields.Float(string='Risk Score', digits=(10, 2))


class Customer(models.Model):
    _inherit = 'res.partner'
    _sql_constraints = [
        ('uniq_customer_id', 'unique(customer_id)',
         "Customer ID already exists. Value must be unique!"),
    ]

    customer_id = fields.Char(string="Customer ID",
                              index=True, tracking=True, readonly=True)
    bvn = fields.Char(string='BVN', tracking=True, readonly=True, index=True)
    branch_id = fields.Many2one(
        comodel_name='res.branch', string='Branch', index=True,
        tracking=True, readonly=True, store=True)
    # branch_id = fields.Many2one(
    #     comodel_name='res.branch', string='Branch', index=True,
    #     tracking=True, readonly=True,compute='_compute_branch',store=True,)

    education_level_id = fields.Many2one(
        comodel_name='res.education.level', string='Education Level', index=True, tracking=True, readonly=True)
    kyc_limit_id = fields.Many2one(
        comodel_name='res.partner.kyc.limit', string='KYC Limit')
    tier_id = fields.Many2one(
        comodel_name='res.partner.tier', string='Customer Tier', index=True)
    identification_type_id = fields.Many2one(
        comodel_name='res.identification.type', string='Identification Type', index=True, tracking=True, readonly=True)
    identification_number = fields.Char(
        string='Identification Number', tracking=True, readonly=True)
    identification_expiry_date = fields.Date(
        string='Identification Expiry Date', index=True, tracking=True, readonly=True)
    dob = fields.Date(
        string='Date of Birth', tracking=True, readonly=True)
    vat = fields.Char(string='Tax ID/TIN', index=True,
                      help="The Tax Identification Number. Values here will be validated based on the country format. You can use '/' to indicate that the partner is not subject to tax.", readonly=True)
    region_id = fields.Many2one(
        comodel_name='res.partner.region', string='Region', tracking=True, readonly=True)
    sector_id = fields.Many2one(

        comodel_name='res.partner.sector', string='Sector', index=True, tracking=True, readonly=True)

    customer_industry_id = fields.Many2one(
        comodel_name='customer.industry', string='Industry', index=True, tracking=True)

    sex_id = fields.Many2one(
        comodel_name='res.partner.gender', string='Sex', readonly=True)
    firstname = fields.Char(string='Firstname', readonly=True)
    # fullname = fields.Char(string='Fullname')
    short_name = fields.Char(string='Short name', readonly=True)
    lastname = fields.Char(string='Lastname', index=True, readonly=True)
    middlename = fields.Char(string='Middle Name', readonly=True)
    othername = fields.Char(string='Other Name', readonly=True)
    town = fields.Char(string='Town', readonly=True)
    registration_date = fields.Date(
        string='Registration Date', tracking=True, required=False, readonly=True)
    company_reg_date = fields.Date(
        string='Company Registration Date', tracking=True)
    risk_score = fields.Float(
        string='Risk Score', digits=(10, 2), index=True, tracking=True)
    risk_level = fields.Char(
        string='Risk Level', index=True, default='low', tracking=True)
    account_officer_id = fields.Many2one(
        comodel_name='account.officers', string='Account Officer', index=True, tracking=True, readonly=True)
    risk_level_id = fields.Many2one(
        comodel_name='res.risk.level', string='Risk Level')
    account_ids = fields.One2many(
        comodel_name='res.partner.account', index=True, inverse_name='customer_id', string='Accounts', readonly=True)

    res_partner_account_ids = fields.One2many(
        'res.partner.account', 'customer_id', string='Accounts', readonly=True)

    edd_ids = fields.One2many(
        comodel_name='res.partner.edd', index=True, inverse_name='customer_id', string='EDD Lines', tracking=True)
    shareholder_ids = fields.One2many(
        comodel_name='res.partner.shareholders', inverse_name='customer_id', string='Shareholder', tracking=True)
    risk_plan_line_ids = fields.One2many(
        comodel_name='res.partner.risk.plan.line', inverse_name='partner_id', string='Risk Analysis Lines', tracking=True)
    risk_assessment_ids = fields.One2many(
        comodel_name='res.risk.assessment', inverse_name='partner_id', string='Risk Assessments')
    is_pep = fields.Boolean(
        string="Is PEP", default=False, tracking=True, index=True)
    is_watchlist = fields.Boolean(
        string="Is Watchlist", default=False, tracking=True)
    is_fep = fields.Boolean(string="Is FEP", default=False, tracking=True)
    is_blacklist = fields.Boolean(
        string="Is Blacklist", default=False, tracking=True)
    global_pep = fields.Boolean(
        string="Global PEP",  index=True, default=False)
    current_branch_id = fields.Integer(
        string='Current Branch', compute='_get_current_branch')
    internal_category = fields.Selection(string='Internal Category', selection=[('customer', 'Customer'), (
        'vendor', 'Vendor'), ('partner', 'Partner'), ('correspondent', 'Correspondent'), ('respondent', 'Respondent')], default='customer', index=True, readonly=True)
    anti_bribery = fields.Binary(string='Anti-Bribery & Corruption Docs')
    anti_bribery_file_name = fields.Char(
        string='Anti-Bribery & Corruption Docs')
    data_protection = fields.Binary(string='Data Protection Docs')
    data_protection_file_name = fields.Char(string='Data Protection Docs')
    whistle_blowing = fields.Binary(string='Whistle Blowing and Ethics Docs')
    whistle_blowing_file_name = fields.Char(
        string='Whistle Blowing and Ethics Docs')
    anti_money_laundering = fields.Binary(
        string='Anti-Money Laundering & Terrorism Financing Doc')
    anti_money_laundering_file_name = fields.Char(
        string='Anti-Money Laundering & Terrorism Financing Doc')
    total_accounts = fields.Integer(
        string='Accounts', compute='customer_total_accounts', index=True, store=False)
    global_pep_id = fields.Many2one(
        'res.pep', string='Related Global PEP', tracking=True)

    address = fields.Char(string="Address", required=False, readonly=True)
    customer_title = fields.Char(string="Title", required=False, readonly=True)
    gender = fields.Char(string="Gender", required=False, readonly=True)
    marital_status = fields.Char(
        string="Marital Status", required=False, readonly=True)
    employment_status = fields.Char(
        string="Employment Status", required=False, readonly=True)
    state_residence = fields.Char(
        string="State Residence", required=False, readonly=True)
    nin = fields.Char(
        string="National Identification Number (NIN)", index=True, required=False, readonly=True)
    customer_rating = fields.Char(
        string="Customer Rating", required=False, index=True, readonly=True)
    active = fields.Boolean(default=True, readonly=True)

    is_greylist = fields.Boolean(
        string="Is Greylist", default=False, tracking=True)

    origin = fields.Selection(string='Data Origin', selection=[('demo', 'Demo Data'), (
        'test', 'Test Data'), ('prod', 'Production Data')], index=True)

    first_risk_rating = fields.Char(string='Bank Risk Rating', index=True)
    pep = fields.Char(string='Bank Pep Customer', index=True)
    customer_phone = fields.Char(string='Phone Number(s)', index=True)

    # phone = fields.Char(string='Phone Number(s)', index=True)
    formatted_phone = fields.Char(
        string='Phone Number(s)', index=True, compute='_compute_formatted_phone')

    likely_sanction = fields.Boolean(string='Is Sanctioned',tracking=True)
    likely_pep = fields.Boolean()
    branch_code = fields.Char(string="Branch Code", index=True)
   
    formatted_gender=fields.Char(string='Gender', compute='_compute_gender')
    
    digital_product_view_ids = fields.One2many(
        'res.partner.digital.product.view', 'partner_id', 
        string='Digital Products', readonly=True, auto_join=True)
    
    channel_subscription_ids = fields.One2many(
        'customer.channel.subscription', 'partner_id',
        string='Channel Subscriptions', readonly=True)
    
    composite_risk_score = fields.Float(
        string='Composite Risk Score', digits=(10, 2))
    
    last_risk_calculation = fields.Datetime(string='Last Risk Calculation', readonly=True,
                                            help="When the risk score was last calculated")
    
    # universe_weight_ids = fields.One2many('res.partner.risk.universe.weight', 'partner_id',
    #                                       string='Universe Weights')
    

    show_create_case_button = fields.Boolean(
        string="Case Management Installed",
        compute='_compute_is_case_management_installed',
        store=False,
    )
    
    screening_ids = fields.One2many(
        'res.partner.screening.result', 'partner_id',
        string='Screening Results', domain=[('active', '=', True)])
    last_screening_date = fields.Datetime(
        string='Last Screening Date', readonly=True)
    likely_pep_match_id = fields.Many2one(
        'pep.list', string='Likely PEP Match')
    likely_watchlist_match_id = fields.Many2one(
        'res.partner.watchlist', string='Likely Watchlist Match')
    likely_sanction_match_id = fields.Many2one(
        'sanction.list', string='Likely Sanction Match')
    likely_global_pep_match_id = fields.Many2one(
        'res.pep', string='Likely Global PEP Match')
    screening_needed = fields.Boolean(
        string='Screening Needed',
        help="Flag to indicate if customer needs screening")
    composite_plan_line_ids = fields.One2many(
        'res.partner.composite.plan.line', 'partner_id',
        string='Composite Risk Plan Lines')
    


    def action_view_screening_results(self):
        """View screening results for this customer"""
        self.ensure_one()
        return {
            'name': _('Screening Results'),
            'view_mode': 'tree,form',
            'res_model': 'res.partner.screening.result',
            'domain': [('partner_id', '=', self.id), ('active', '=', True)],
            'type': 'ir.actions.act_window',
            'context': {'default_partner_id': self.id}
        }
    
            
    def action_screen_customer(self):
        """Screen customer against all lists"""
        self.ensure_one()
        screening = self.env['res.partner.screening.result']
        result = screening.screen_customer(self.id)

        # Show appropriate message based on screening result and email status
        if result:
            # result is now a dict with 'matches_found' and 'email_sent' keys
            if result.get('email_sent', False):
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': _('Screening Complete'),
                        'message': _('Potential matches found. Compliance officers have been notified via email.'),
                        'type': 'warning',
                        'sticky': True,
                    }
                }
            else:
                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': _('Screening Complete'),
                        'message': _('Potential matches found, but email notification failed. Please check system logs'),
                        'type': 'danger',
                        'sticky': True,  # Make it sticky so user notices the email failure
                    }
                }
        else:
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Screening Complete'),
                    'message': _('No matches found.'),
                    'type': 'success',
                    'sticky': True,
                }
            }

    @api.depends('registration_date')  
    def _compute_is_case_management_installed(self):
        case_management_installed = bool(self.env['ir.module.module'].search([
            ('name', '=', 'case_management'),
            ('state', '=', 'installed')
        ], limit=1))
        
        for record in self:
            record.show_create_case_button = case_management_installed


    def _get_risk_score_from_plan(self):
        """
        Modified method to calculate both normal and composite risk scores
        """
        setting = self.env['res.compliance.settings'].search(
            [('code', '=', 'risk_plan_computation')], limit=1)

        # Default value if no settings found
        plan_setting = 'max'  # Default to 'max'
        for e in setting:
            plan_setting = e.val

        record_id = self.id

        # Clear previous risk plan lines
        self.env["res.partner.risk.plan.line"].search(
            [('partner_id', '=', record_id)]).unlink()

        # Clear previous composite plan lines (added this line)
        self.env['res.partner.composite.plan.line'].sudo().search(
            [('partner_id', '=', record_id)]).unlink()

        scores = []

        # First, find all plans that should contribute to composite calculation
        composite_plans = self.env['res.compliance.risk.assessment.plan'].search([
            ('state', '=', 'active'),
            ('use_composite_calculation', '=', True),
            ('compute_score_from', '=', 'risk_assessment')
        ])

        # Get IDs of composite plans to exclude from regular calculation
        composite_plan_ids = composite_plans.ids

        if composite_plans:
            # Calculate composite score
            composite_score = self._calculate_composite_score(composite_plans)
            # Store composite score directly
            self.composite_risk_score = composite_score

        # Now process regular risk plans, excluding those used for composite calculation
        plans = self.env['res.compliance.risk.assessment.plan'].search([
            ('state', '=', 'active'),
            ('id', 'not in', composite_plan_ids),  # Exclude composite plans
        ], order='priority')

        if plans:
            for pl in plans:
                score = 0
                try:
                    self.env.cr.execute(pl.sql_query, (record_id,))
                    rec = self.env.cr.fetchone()
                    if rec is not None:
                        # we have a hit
                        if pl.compute_score_from == 'dynamic':
                            score = float(
                                rec[0]) if rec is not None else pl.risk_score
                        if pl.compute_score_from == 'static':
                            score = pl.risk_score
                        if pl.compute_score_from == 'risk_assessment':
                            score = pl.risk_assessment.risk_rating if pl.risk_assessment is not None else pl.risk_score
                    scores.append(score)
                    line_id = self.env['res.partner.risk.plan.line'].create({
                        'partner_id': record_id,
                        'plan_line_id': pl.id,
                        'risk_score': score,
                    })
                except Exception as e:
                    _logger.error(
                        f"Error executing risk plan {pl.name}: {str(e)}")
                    pass

        records = None

        if len(scores) > 0:
            if plan_setting == 'avg':
                self.env.cr.execute(
                    f"select avg(risk_score) from res_partner_risk_plan_line where partner_id={record_id} and risk_score > 0")
            elif plan_setting == 'max':
                self.env.cr.execute(
                    f"select max(risk_score) from res_partner_risk_plan_line where partner_id={record_id}")
            elif plan_setting == 'sum':
                self.env.cr.execute(
                    f"select sum(risk_score) from res_partner_risk_plan_line where partner_id={record_id} and risk_score > 0")
            else:
                # Default to max if the setting isn't recognized
                self.env.cr.execute(
                    f"select max(risk_score) from res_partner_risk_plan_line where partner_id={record_id} ")

            records = self.env.cr.fetchone()

        # Ensure records is not None before returning
        """
        - Priority is Risk Assessment > EDD > Risk Plan
        """
        risk_assessments = self.env['res.risk.assessment'].search(
            [('partner_id', '=', record_id)], order='create_date desc', limit=1)
        if risk_assessments:
            for r in risk_assessments:
                if r.risk_rating:
                    # Store the risk score directly
                    self.risk_score = r.risk_rating
                    return r.risk_rating
        approved_edd = self.env['res.partner.edd'].search(
            [('status', '=', 'approved'), ('customer_id', '=', record_id)], order='date_approved desc', limit=1)
        for edd in approved_edd:
            if edd.risk_score:
                # Store the risk score directly
                self.risk_score = edd.risk_score
                return edd.risk_score
        # Use risk analysis
        risk_score = records[0] if records is not None else 0.00
        # Store the risk score directly
        self.risk_score = risk_score
        return risk_score

    
    def calculate_risk_batch(self, batch_size=1000):
        """
        Optimized method to calculate risk for the current recordset in batches
        Returns the number of customers processed
        """
        start_time = time.time()
        total_processed = 0

        # Process in batches to avoid memory issues
        for i in range(0, len(self), batch_size):
            batch = self[i:i+batch_size]
            self._calculate_risk_batch_internal(batch)
            total_processed += len(batch)
            _logger.info(
                f"Processed batch {i//batch_size + 1}, total {total_processed} customers")
            # Commit transaction to release memory
            self.env.cr.commit()

        end_time = time.time()
        _logger.info(
            f"Total processing time for {total_processed} customers: {end_time - start_time:.2f} seconds")
        return total_processed

    def _calculate_risk_batch_internal(self, batch):
        """Internal method to process a batch of customers"""
        if not batch:
            return

        # Get all the configuration we need up front
        setting = self.env['res.compliance.settings'].search(
            [('code', '=', 'risk_plan_computation')], limit=1)
        plan_setting = setting.val if setting else 'max'

        record_ids = batch.ids
        timestamp = fields.Datetime.now()

        # 1. Clear existing risk plan lines in bulk
        if record_ids:
            self.env.cr.execute(
                "DELETE FROM res_partner_risk_plan_line WHERE partner_id IN %s",
                (tuple(record_ids),)
            )

        # 2. Get all active risk plans in one query
        plans = self.env['res.compliance.risk.assessment.plan'].search(
            [('state', '=', 'active')], order='priority')

        # 3. Process composite plans - get them all at once
        composite_plans = plans.filtered(
            lambda p: p.use_composite_calculation and p.compute_score_from == 'risk_assessment')

        # 4. Prepare data for bulk insert of risk plan lines
        risk_line_values = []
        score_data = {rec_id: [] for rec_id in record_ids}

        # 5. Execute all plans for all customers in a more efficient way
        for plan in plans:
            try:
                # Create a modified query that works for multiple customer IDs
                base_query = plan.sql_query
                # If the original query has a WHERE clause, we need to adapt it
                if "WHERE" in base_query.upper():
                    # Replace the parameter placeholder with a value for IN clause
                    batch_query = base_query.replace(
                        "(%s)", f"IN {tuple(record_ids)}")
                else:
                    # Add a WHERE clause for the parameter
                    batch_query = base_query + \
                        f" WHERE partner_id IN {tuple(record_ids)}"

                self.env.cr.execute(batch_query)
                results = self.env.cr.fetchall()

                # Map results to customer IDs - this depends on your query structure
                # Assuming query returns (customer_id, score) or just (score) for one customer
                if results:
                    if len(results[0]) > 1:  # If query returns customer_id and score
                        for res in results:
                            cust_id = res[0]
                            score_val = res[1] if res[1] is not None else plan.risk_score
                            if cust_id in score_data:
                                score_data[cust_id].append(score_val)
                                risk_line_values.append({
                                    'partner_id': cust_id,
                                    'plan_line_id': plan.id,
                                    'risk_score': score_val,
                                })
                    else:  # If query returns just a score for the single customer in WHERE clause
                        # For single customer query case
                        if len(record_ids) == 1:
                            cust_id = record_ids[0]
                            score_val = results[0][0] if results[0][0] is not None else plan.risk_score
                            score_data[cust_id].append(score_val)
                            risk_line_values.append({
                                'partner_id': cust_id,
                                'plan_line_id': plan.id,
                                'risk_score': score_val,
                            })
            except Exception as e:
                _logger.error(
                    f"Error executing batch risk plan {plan.name}: {str(e)}")
                continue

        # 6. Bulk insert risk plan lines
        if risk_line_values:
            self._bulk_create_risk_plan_lines(risk_line_values)

        # 7. Calculate final scores for all customers in batch
        final_scores = {}
        composite_scores = {}

        # Calculate risk scores based on plan setting
        for cust_id, scores in score_data.items():
            if not scores:
                continue

            if plan_setting == 'avg':
                # Filter out zeros to match original behavior
                non_zero_scores = [s for s in scores if s > 0]
                if non_zero_scores:
                    final_scores[cust_id] = sum(
                        non_zero_scores) / len(non_zero_scores)
            elif plan_setting == 'max':
                final_scores[cust_id] = max(scores) if scores else 0
            elif plan_setting == 'sum':
                # Filter out zeros to match original behavior
                non_zero_scores = [s for s in scores if s > 0]
                final_scores[cust_id] = sum(non_zero_scores)
            else:
                # Default to avg
                non_zero_scores = [s for s in scores if s > 0]
                if non_zero_scores:
                    final_scores[cust_id] = sum(
                        non_zero_scores) / len(non_zero_scores)

        # 8. Calculate composite scores for customers in batch
        if composite_plans:
            composite_scores = self._calculate_composite_scores_batch(
                batch, composite_plans)

        # 9. Apply priority logic (Risk Assessment > EDD > Risk Plan)
        # Get all relevant risk assessments in one query
        risk_assessments = self.env['res.risk.assessment'].search([
            ('partner_id', 'in', record_ids),
        ], order='create_date desc')

        # Group by partner_id to get the most recent for each
        grouped_assessments = {}
        for ra in risk_assessments:
            if ra.partner_id.id not in grouped_assessments and ra.risk_rating:
                grouped_assessments[ra.partner_id.id] = ra.risk_rating

        # Get all relevant EDDs in one query
        edds = self.env['res.partner.edd'].search([
            ('status', '=', 'approved'),
            ('customer_id', 'in', record_ids),
        ], order='date_approved desc')

        # Group by customer_id to get the most recent for each
        grouped_edds = {}
        for edd in edds:
            if edd.customer_id not in grouped_edds and edd.risk_score:
                grouped_edds[edd.customer_id] = edd.risk_score

        # 10. Prepare values for bulk update
        update_values = []
        for cust_id in record_ids:
            # Priority: Risk Assessment > EDD > Risk Plan
            if cust_id in grouped_assessments:
                risk_score = grouped_assessments[cust_id]
            elif cust_id in grouped_edds:
                risk_score = grouped_edds[cust_id]
            elif cust_id in final_scores:
                risk_score = final_scores[cust_id]
            else:
                risk_score = 0.0

            composite_score = composite_scores.get(cust_id, 0.0)

            update_values.append({
                'id': cust_id,
                'risk_score': risk_score,
                'composite_risk_score': composite_score,
                'last_risk_calculation': timestamp,
            })

        # 11. Bulk update customer risk scores
        if update_values:
            self._bulk_update_risk_scores(update_values)

    def _bulk_create_risk_plan_lines(self, values_list):
        """Efficiently create risk plan lines in bulk"""
        if not values_list:
            return

        # Prepare columns and values for execute_values
        columns = ['partner_id', 'plan_line_id', 'risk_score']
        vals = [(v['partner_id'], v['plan_line_id'], v['risk_score'])
                for v in values_list]

        query = """
            INSERT INTO res_partner_risk_plan_line
            (partner_id, plan_line_id, risk_score, create_uid, create_date, write_uid, write_date)
            VALUES %s
        """

        # Add create/write user and timestamps
        uid = self.env.user.id
        timestamp = datetime.now()
        vals = [(v[0], v[1], v[2], uid, timestamp, uid, timestamp)
                for v in vals]

        # Execute the bulk insert
        execute_values(self.env.cr, query, vals, template=None, page_size=1000)

    def _bulk_update_risk_scores(self, values_list):
        """Efficiently update customer risk scores in bulk"""
        if not values_list:
            return

        # Group updates in batches of 1000 to avoid query size limits
        batch_size = 1000
        for i in range(0, len(values_list), batch_size):
            batch = values_list[i:i+batch_size]

            # Build CASE statements for each field
            case_risk_score = " ".join([
                f"WHEN id = {v['id']} THEN {v['risk_score']}" for v in batch
            ])

            case_composite_score = " ".join([
                f"WHEN id = {v['id']} THEN {v['composite_risk_score']}" for v in batch
            ])

            # Get all IDs for this batch
            ids = [str(v['id']) for v in batch]

            # Build and execute the update query
            query = f"""
                UPDATE res_partner
                SET 
                    risk_score = CASE {case_risk_score} ELSE risk_score END,
                    composite_risk_score = CASE {case_composite_score} ELSE composite_risk_score END,
                    last_risk_calculation = %s,
                    write_uid = %s,
                    write_date = %s
                WHERE id IN ({','.join(ids)})
            """

            self.env.cr.execute(
                query, (values_list[0]['last_risk_calculation'], self.env.user.id, fields.Datetime.now()))

    def _calculate_composite_score(self, composite_plans):
        """
        Calculate composite risk score based on weighted risk universes
        - Only include universes with violations (universe_score > 0) in the CCR calculation
        - Show all risk plans regardless of match status
        - Apply dynamic calculation (avg/max/sum) at universe level based on settings
        """
        record_id = self.id
        composite_score = 0.0

        # Get the risk assessment computation setting
        setting = self.env['res.compliance.settings'].search([
            ('code', '=', 'risk_composite_computation')
        ], limit=1)

        # Default to 'avg' if no setting found
        plan_setting = 'avg'
        if setting:
            plan_setting = setting.val.strip().lower()

        # Validate setting, default to 'avg' if invalid
        if plan_setting not in ['avg', 'max', 'sum']:
            plan_setting = 'avg'

        _logger.info(f"Using risk calculation method: {plan_setting.upper()}")

        # Clear previous composite plan lines
        self.env['res.partner.composite.plan.line'].sudo().search(
            [('partner_id', '=', record_id)]).unlink()

        # Get all universes that are included in composite calculation and have weight > 0
        universes = self.env['res.risk.universe'].search([
            ('is_included_in_composite', '=', True),
            ('weight_percentage', '>', 0)
        ])

        # Initialize dictionary to track all universes and their subjects
        universe_scores = {}
        for universe in universes:
            universe_scores[universe.id] = {
                'universe': universe,
                # Total universe score (calculated using dynamic method)
                'total_score': 0.0,
                'weight': universe.weight_percentage / 100.0,
                'name': universe.name,
                'subjects': {}  # Dict to track each subject's score
            }

        # Process all plans to find matches and create plan lines
        for plan in composite_plans:
            # Skip if universe doesn't exist, isn't included, or has no weight
            if not plan.universe_id or not plan.universe_id.is_included_in_composite or plan.universe_id.weight_percentage <= 0:
                continue

            # Skip if risk assessment has empty/zero score
            if not plan.risk_assessment or not plan.risk_assessment.risk_rating or plan.risk_assessment.risk_rating <= 0:
                continue

            universe_id = plan.universe_id.id
            subject_id = plan.risk_assessment.subject_id.id if plan.risk_assessment.subject_id else False

            # Ensure the subject is tracked in this universe
            if subject_id and subject_id not in universe_scores[universe_id]['subjects']:
                universe_scores[universe_id]['subjects'][subject_id] = {
                    'subject': plan.risk_assessment.subject_id,
                    'score': 0.0,
                    'matched_plans': [],
                    'assessment': None
                }

            # Check for SQL hit if compute_score_from is risk_assessment
            matched = False
            score = 0.0

            try:
                self.env.cr.execute(plan.sql_query, (record_id,))
                rec = self.env.cr.fetchone()

                if rec is not None:  # SQL hit (violation)
                    matched = True
                    score = plan.risk_assessment.risk_rating

                    # Update subject score if this subject exists
                    if subject_id:
                        # Add to matched plans list
                        universe_scores[universe_id]['subjects'][subject_id]['matched_plans'].append(
                            plan)

                        # Set the assessment if not already set
                        if not universe_scores[universe_id]['subjects'][subject_id]['assessment']:
                            universe_scores[universe_id]['subjects'][subject_id]['assessment'] = plan.risk_assessment

                        # Update the subject score by adding this score (sum all matches)
                        universe_scores[universe_id]['subjects'][subject_id]['score'] += score

                # Create plan line records for ALL plans, whether they match or not
                if matched :
                    self.env['res.partner.composite.plan.line'].sudo().create({
                        'partner_id': record_id,
                        'plan_id': plan.id,
                        'universe_id': universe_id,
                        'subject_id': subject_id,
                        'matched': matched,
                        'risk_score': score,
                        'assessment_id': plan.risk_assessment.id if plan.risk_assessment else False,
                    })

            except Exception as e:
                _logger.error(
                    f"Error checking for violations in plan {plan.name}: {str(e)}")
                

        # Calculate the total score for each universe using dynamic method (avg/max/sum)
        for universe_id, universe_data in universe_scores.items():
            # Collect all subject scores that are > 0 for this universe
            subject_scores = []
            for subject_id, subject_data in universe_data['subjects'].items():
                if subject_data['score'] > 0:
                    subject_scores.append(subject_data['score'])

            # Apply dynamic calculation method to get universe total
            if subject_scores:  # Only calculate if there are scores
                if plan_setting == 'max':
                    universe_data['total_score'] = max(subject_scores)
                elif plan_setting == 'sum':
                    universe_data['total_score'] = sum(subject_scores)
                else:  # 'avg' (default)
                    universe_data['total_score'] = sum(
                        subject_scores) / len(subject_scores)
            else:
                universe_data['total_score'] = 0.0

            # Log the calculation details for debugging
            if subject_scores:
                _logger.info(
                    f"Universe {universe_data['name']}: "
                    f"Subject scores={subject_scores}, "
                    f"Method={plan_setting.upper()}, "
                    f"Universe total={universe_data['total_score']:.2f}"
                )

        # Create records for ALL universes and ALL subjects and calculate composite score
        for universe_id, universe_data in universe_scores.items():
            # Calculate weighted score for the universe
            weighted_score = universe_data['total_score'] * universe_data['weight']

            # Only add to CCR if there's a violation (score > 0)
            if universe_data['total_score'] > 0:
                composite_score += weighted_score
                _logger.info(
                    f"Universe {universe_data['name']} : Score={universe_data['total_score']:.2f}, "
                    f"Weight={universe_data['weight']:.2f}, Weighted Score={weighted_score:.2f}")

        _logger.info(f"Final CCR for customer {record_id}: {composite_score:.2f}")
        return round(composite_score, 2)

    def action_sync_channels(self):
        """Fast channel sync for individual customer"""
        if not self.customer_id:
            from odoo.exceptions import UserError
            raise UserError("Customer ID is required for syncing channels")

        self._cr.execute("""
            INSERT INTO customer_channel_subscription (customer_id, partner_id, channel_id, value, last_updated)
            SELECT %s, %s, dc.id, 'NO', NOW()
            FROM digital_delivery_channel dc
            WHERE dc.status = 'active'
            AND NOT EXISTS (
                SELECT 1 FROM customer_channel_subscription ccs
                WHERE ccs.customer_id = %s AND ccs.channel_id = dc.id
            )
        """, (self.customer_id, self.id, self.customer_id))

        created_count = self._cr.rowcount
        self._cr.commit()

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Sync Complete',
                'message': f'Customer synced - {created_count} new channels added',
                'type': 'success',
            }
        }

    def action_view_channels(self):
        """Open customer channels view"""
        return {
            'name': f'Digital Channels - {self.name}',
            'type': 'ir.actions.act_window',
            'res_model': 'customer.channel.subscription',
            'view_mode': 'tree,form',
            'domain': [('partner_id', '=', self.id)],
            'context': {
                'default_customer_id': self.customer_id,
                'default_partner_id': self.id,
            }
        }
        
    
    @api.depends('gender')
    def _compute_gender(self):
        for record in self:
            if not record.gender:
                record.formatted_gender = False
                continue

            # trim whitespace and convert to lowercase
            cleaned_gender = record.gender.strip().lower()

            if cleaned_gender.startswith('f'):
                record.formatted_gender = 'Female'
            elif cleaned_gender.startswith('m'):
                record.formatted_gender = 'Male'

    def action_create_case(self):
        """
        Opens the case management form with the customer pre-filled
        """
        # Create the context with required values
        context = {
            'default_status_id': self.env.ref('case_management.case_status_open').id,
            'case_created': True,
            'show_creation_notification': True,
        }

        # Since customer_id in the case model is a Many2one field referencing res.partner,
        # and this model (Customer) inherits from res.partner,
        # we need to pass the ID of the current record
        context['default_customer_id'] = self.id

        return {
            'type': 'ir.actions.act_window',
            'name': 'New Case',
            'res_model': 'case',
            'view_mode': 'form',
            'view_id': self.env.ref('case_management.case_form_view').id,
            'target': 'current',
            'context': context
        }

    @api.depends('customer_phone')
    def _compute_formatted_phone(self):
        for record in self:
            if not record.customer_phone:
                record.formatted_phone = False
                continue

            # Get the original phone number
            phone = record.customer_phone

            # Step 1: Strip any trailing or leading commas
            phone = phone.strip(',')

            # Step 2: Replace ^ with comma
            phone = phone.replace('^', ',')

            # Step 3: Split by comma, clean each part, and rejoin with proper formatting
            parts = [part.strip() for part in phone.split(',')]

            # Step 4: Filter out empty parts
            parts = [part for part in parts if part]

            # Step 5: Join with comma+space
            formatted = ', '.join(parts)

            record.formatted_phone = formatted

    @api.model
    def cron_run_risk_assessment(self):
        customer_counts = self.search_count([])

        if customer_counts <= 200:
            """
            Main entry point for risk assessment cron job with protection against
            concurrent execution.
            """
            # Get a timestamp to use as a unique identifier
            cron_name = "risk_assessment_cron"
            cron_record = self.env.ref(
                'compliance_management.ir_cron_run_risk_assessment')

            # Check if the cron is already running
            if cron_record.nextcall and fields.Datetime.from_string(cron_record.nextcall) > fields.Datetime.now():
                _logger.info(
                    "Risk assessment job already running or scheduled, skipping this run")
                return False

            try:
                # Set the nextcall far in the future to prevent new runs starting

                # cron_record.write({
                #     'nextcall': fields.Datetime.now() + timedelta(hours=24)
                # })
                # self.env.cr.commit()

                _logger.info("Starting scheduled risk assessment process")
                results = {'sanction_status': {}, 'risk_scores': 0}

                try:
                    results['sanction_status'] = self.update_sanction_status()
                    _logger.info(
                        "Sanction status update completed successfully")
                except Exception as e:
                    _logger.error(
                        f"Error in update_sanction_status: {str(e)}", exc_info=True)

                try:
                    results['risk_scores'] = self.compute_risk_score_for_all_users()
                    _logger.info(
                        "Risk score computation completed successfully")
                except Exception as e:
                    _logger.error(
                        f"Error in compute_risk_score_for_all_users: {str(e)}", exc_info=True)

                _logger.info(
                    f"Completed full risk assessment process: {results}")
                return results

            finally:
                # Reset the nextcall to 5 minutes from now
                next_run = fields.Datetime.now() + timedelta(minutes=5)
                cron_record.write({
                    'nextcall': next_run
                })
                self.env.cr.commit()

    def update_sanction_status(self):
        _logger.info("Starting PEP status check using Odoo ORM for tracking.")

        # Process in batches to avoid memory issues
        batch_size = 1000
        processed = 0

        # Step 1: Find matches using SQL but let ORM handle the writes

        # Update global_pep from res_pep
        self.env.cr.execute("""
            SELECT rp.id 
            FROM res_partner rp
            JOIN res_pep pep ON LOWER(TRIM(rp.firstname)) || ' ' || LOWER(TRIM(rp.lastname)) = LOWER(TRIM(pep.name))
            WHERE rp.firstname IS NOT NULL AND rp.lastname IS NOT NULL
        """)
        global_pep_ids = [r[0] for r in self.env.cr.fetchall()]

        # Process global_pep matches in batches
        total_pep = len(global_pep_ids)
        _logger.info(f"Found {total_pep} partners matching PEP records")

        for i in range(0, total_pep, batch_size):
            batch = global_pep_ids[i:i+batch_size]
            partners = self.env['res.partner'].browse(batch)
            partners.write({'global_pep': True})
            self.env.cr.commit()  # Commit each batch
            processed += len(batch)
            _logger.info(
                f"Updated global_pep for {processed}/{total_pep} partners")

        # Update is_blacklist from res_partner_blacklist
        self.env.cr.execute("""
            SELECT rp.id 
            FROM res_partner rp
            JOIN res_partner_blacklist bl ON LOWER(TRIM(rp.firstname)) || ' ' || LOWER(TRIM(rp.lastname)) = 
                                            LOWER(TRIM(bl.first_name)) || ' ' || LOWER(TRIM(bl.surname))
            WHERE rp.firstname IS NOT NULL AND rp.lastname IS NOT NULL
        """)
        blacklist_ids = [r[0] for r in self.env.cr.fetchall()]

        # Process blacklist matches in batches
        total_blacklist = len(blacklist_ids)
        processed = 0
        _logger.info(
            f"Found {total_blacklist} partners matching blacklist records")

        for i in range(0, total_blacklist, batch_size):
            batch = blacklist_ids[i:i+batch_size]
            partners = self.env['res.partner'].browse(batch)
            partners.write({'is_blacklist': True})
            self.env.cr.commit()  # Commit each batch
            processed += len(batch)
            _logger.info(
                f"Updated is_blacklist for {processed}/{total_blacklist} partners")

        # Update is_watchlist from res_partner_watchlist
        self.env.cr.execute("""
            SELECT rp.id 
            FROM res_partner rp
            JOIN res_partner_watchlist wl ON LOWER(TRIM(rp.firstname)) || ' ' || LOWER(TRIM(rp.lastname)) = 
                                            LOWER(TRIM(wl.first_name)) || ' ' || LOWER(TRIM(wl.surname))
            WHERE rp.firstname IS NOT NULL AND rp.lastname IS NOT NULL
        """)
        watchlist_ids = [r[0] for r in self.env.cr.fetchall()]

        # Process watchlist matches in batches
        total_watchlist = len(watchlist_ids)
        processed = 0
        _logger.info(
            f"Found {total_watchlist} partners matching watchlist records")

        for i in range(0, total_watchlist, batch_size):
            batch = watchlist_ids[i:i+batch_size]
            partners = self.env['res.partner'].browse(batch)
            partners.write({'is_watchlist': True})
            self.env.cr.commit()  # Commit each batch
            processed += len(batch)
            _logger.info(
                f"Updated is_watchlist for {processed}/{total_watchlist} partners")

        _logger.info(
            "Sanction status update completed for global_pep, blacklist, and watchlist.")
        return {
            'global_pep_updated': len(global_pep_ids),
            'blacklist_updated': len(blacklist_ids),
            'watchlist_updated': len(watchlist_ids)
        }

    def init(self):
        
        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS res_partner_id_idx ON res_partner (id)")



    @api.model_create_multi
    def create(self, vals_list):
        # Create records
        records = super(Customer, self).create(vals_list)

        # Trigger notification for UI refresh
        self.env['bus.bus']._sendmany([
            ('dashboard_refresh_channel', 'refresh', {
                'type': 'refresh',
                'channelName': 'dashboard_refresh_channel',
                'model': self._name
            })
        ])

        # Create a context to prevent recursion
        new_ctx = dict(self.env.context, computing_risk=True)
        self = self.with_context(new_ctx)

        # Update risk score and level for all created records
        for record in records:
            score = record._get_risk_score_from_plan()
            risk_level = record.compute_risk_level()

            # Use direct SQL update to avoid triggering write()
            if score > float(self.env['res.compliance.settings'].get_setting('maximum_risk_threshold')):
                score = float(self.env['res.compliance.settings'].get_setting('maximum_risk_threshold'))
            self.env.cr.execute(
                """UPDATE %s SET risk_score = %%s, risk_level = %%s 
                WHERE id = %%s""" % self._table,
                (score, risk_level, record.id)
            )

            # Invalidate cache for these fields
            record.invalidate_recordset(['risk_score', 'risk_level'])

        return records

    def write(self, vals):
        # Apply updates from vals
        result = super(Customer, self).write(vals)

        # Trigger notification for UI refresh
        self.env['bus.bus']._sendmany([
            ('dashboard_refresh_channel', 'refresh', {
                'type': 'refresh',
                'channelName': 'dashboard_refresh_channel',
                'model': self._name
            })
        ])

        # Only update risk scores if we're not already in a risk score update
        # This prevents recursion
        if not self.env.context.get('computing_risk', False):
            # Create a new context to mark that we're computing risk
            new_ctx = dict(self.env.context, computing_risk=True)
            self = self.with_context(new_ctx)

            # # Update risk score and level for all records
            # for record in self:
            #     score = record._get_risk_score_from_plan()
            #     risk_level = record.compute_risk_level()

            #     # Use direct SQL update to avoid triggering write() again
            #     self.env.cr.execute(
            #         """UPDATE %s SET risk_score = %%s, risk_level = %%s
            #         WHERE id = %%s""" % self._table,
            #         (score, risk_level, record.id)
            #     )

            #     # Invalidate cache for these fields
            #     record.invalidate_recordset(['risk_score', 'risk_level'])
            # record.invalidate_cache(['risk_score', 'risk_level'])

        return result

    def scan_news_articles(self):
        """Trigger news scanning via adverse.media"""
        self.ensure_one()  # Ensure we're working with a single record
        # adverse_media = self.adverse_media_id
        # if not adverse_media:
        # Create or find an adverse.media record if no direct link exists
        adverse_media = self.env['adverse.media'].search(
            [('partner_id', '=', self.id)], limit=1)
        if not adverse_media:
            adverse_media = self.env['adverse.media'].create({
                'partner_id': self.id,
                # Add other required fields for adverse.media if any
                'monitoring_status': 'active',  # Example default
            })

        # Call the original method from adverse.media
        return adverse_media.scan_news_articles()

    @api.depends('res_partner_account_ids')
    def customer_total_accounts(self):
        for e in self:
            e.total_accounts = len(e.res_partner_account_ids)

    def action_total_accounts(self):
        print(self.id)
        print(self.id)
        return {
            'name': _('Accounts'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner.account',
            'view_mode': 'tree,form',
            'domain': [('customer_id.id', 'in', [self.id])],
            'context': {'search_default_group_branch': 1}
        }

    def action_risk_level(self):
        return {
            'type': 'ir.actions.client',
            'tag': 'reload',
        }

    def compute_risk_level(self):
        for record in self:
            try:
                if record.risk_score is None:
                    return 'low'
                if record.risk_score <=  float(self.env['res.compliance.settings'].get_setting('low_risk_threshold')):
                    return 'low'
                if record.risk_score <= float(self.env['res.compliance.settings'].get_setting('medium_risk_threshold')):
                    return 'medium'
                else :
                    return 'high'
            except:
                return 'low'

    def compute_customer_rating(self, score):
        try:
            if score is None:
                return 'low'
            if score <= float(self.env['res.compliance.settings'].get_setting('low_risk_threshold')):
                return 'low'
            if score <= float(self.env['res.compliance.settings'].get_setting('medium_risk_threshold')):
                return 'medium'
            else :
                return 'high'
        except:
            return 'low'

    @api.model
    def _get_risk_level_from_score(self, risk_score):
        try:
            if risk_score is None:
                return 'low'
            if risk_score <= float(self.env['res.compliance.settings'].get_setting('low_risk_threshold')):
                return 'low'
            if risk_score <= float(self.env['res.compliance.settings'].get_setting('medium_risk_threshold')):
                return 'medium'
            else:
                return 'high'
        except:
            return 'low'

    @api.model
    def update_partner_risk_levels(self):
        """
        Cron job to update all partners' risk levels based on their risk scores.
        This method should be called by a scheduled action.
        """
        partners = self.search([])
        for partner in partners:
            risk_score = partner.risk_score  # Assuming risk_score is a field on res.partner
            risk_level = self._get_risk_level_from_score(risk_score)
            # Assuming risk_level is a field on res.partner
            partner.write({'risk_level': risk_level})

        return True

    def _get_current_branch(self):
        for record in self:
            self.current_branch_id = self.env.user.default_branch_id.id

    def action_initiate_edd(self):
        return {
            'name': _('Enhanced Due Diligence'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner.edd',
            'view_mode': 'form',
            'context': {"default_customer_id": self.id},
        }

    def action_unmark_pep(self):
        for e in self:
            e.write({'is_pep': False, 'global_pep': False, 'global_pep_id': None})
            e.action_compute_risk_score_with_plan()

    def action_add_pep(self):
        for e in self:
            e.write({'is_pep': True})
            e.action_compute_risk_score_with_plan()

    def action_remove_pep(self):
        for e in self:
            e.write({'is_pep': False})
            e.action_compute_risk_score_with_plan()

    def action_add_fep(self):
        for e in self:
            e.write({'is_fep': True})
            e.action_compute_risk_score_with_plan()

    def action_remove_fep(self):
        for e in self:
            e.write({'is_fep': False})
            e.action_compute_risk_score_with_plan()

    def action_blacklist(self):
        for e in self:
            e.write({'is_blacklist': True})
            e.action_compute_risk_score_with_plan()

    def action_remove_blacklist(self):
        for e in self:
            e.write({'is_blacklist': False})
            e.action_compute_risk_score_with_plan()

    def action_watchlist(self):
        for e in self:
            e.write({'is_watchlist': True})
            e.action_compute_risk_score_with_plan()

    def action_remove_watchlist(self):
        for e in self:
            e.write({'is_watchlist': False})
            e.action_compute_risk_score_with_plan()

    def action_sanction_list(self):
        for e in self:
            e.write({'likely_sanction': True})
            e.action_compute_risk_score_with_plan()

    def action_remove_sanction_list(self):
        for e in self:
            e.write({'likely_sanction': False})
            e.action_compute_risk_score_with_plan()

    def action_conduct_risk_assessment(self):
        return {
            'name': _('Risk Assessment'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.risk.assessment',
            'view_mode': 'form',
            'context': {"default_partner_id": self.id},
        }

    def action_open_customers(self):
        return {
            'name': _('Customers'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': [('branch_id.id', 'in', [e.id for e in self.env.user.branches_id]), ('internal_category', '=', 'customer')],
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_customers(self):
        # Check if the current user belongs to the Chief Compliance Officer group
        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')

        is_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_compliance_officer')

        # Set domain based on user group
        if is_chief_compliance_officer or is_compliance_officer:
            # Chief Compliance Officers see all customers
            domain = [('internal_category', '=', 'customer'),
                      ('origin', 'in', ['demo', 'test', 'prod'])]
        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id]),
                ('internal_category', '=', 'customer'),
                ('origin', 'in', ['demo', 'test', 'prod'])


            ]

        return {
            'name': _('Customers'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_vendors(self):
        # Check if the current user belongs to the Chief Compliance Officer group
        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')

        is_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_compliance_officer')

        # Set domain based on user group
        if is_chief_compliance_officer or is_compliance_officer:
            # Chief Compliance Officers see all customers
            domain = [('internal_category', '=', 'vendor'),
                      ('origin', 'in', ['demo', 'test', 'prod'])]
        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id]),
                ('internal_category', '=', 'vendor'), ('origin',
                                                       'in', ['demo', 'test', 'prod'])

            ]

        return {
            'name': _('Vendors'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_partners(self):
        # Check if the current user belongs to the Chief Compliance Officer group
        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')

        is_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_compliance_officer')

        # Set domain based on user group
        if is_chief_compliance_officer or is_compliance_officer:
            # Chief Compliance Officers see all customers
            domain = [('internal_category', '=', 'partner')]
        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id]),
                ('internal_category', '=', 'partner'), ('origin',
                                                        'in', ['demo', 'test', 'prod'])
            ]

        return {
            'name': _('Partners'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_correspondents(self):
        # Check if the current user belongs to the Chief Compliance Officer group
        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')

        is_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_compliance_officer')

        # Set domain based on user group
        if is_chief_compliance_officer or is_compliance_officer:
            # Chief Compliance Officers see all customers
            domain = [('internal_category', '=', 'correspondent'),
                      ('origin', 'in', ['demo', 'test', 'prod'])]
        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id]),
                ('internal_category', '=', 'correspondent'), ('origin', 'in', ['demo', 'test', 'prod'])]

        return {
            'name': _('Correspondents'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_respondents(self):
        # Check if the current user belongs to the Chief Compliance Officer group
        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')

        is_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_compliance_officer')

        # Set domain based on user group
        if is_chief_compliance_officer or is_compliance_officer:
            # Chief Compliance Officers see all customers
            domain = [('internal_category', '=', 'respondent'),
                      ('origin', 'in', ['demo', 'test', 'prod'])]
        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id]),
                ('internal_category', '=', 'respondent'),
                ('origin', 'in', ['demo', 'test', 'prod'])

            ]

        return {
            'name': _('Respondents'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def action_view_likely_sanction_customer(self):

        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')

        is_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_compliance_officer')

        # Set domain based on user group
        if is_chief_compliance_officer or is_compliance_officer:
            # Chief Compliance Officers see all customers
            domain = [('origin', 'in', ['demo', 'test', 'prod']),
                      ('likely_sanction', '=', True)]

        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id]),
                ('likely_sanction', '=', True),
                ('origin', 'in', ['demo', 'test', 'prod'])

            ]

        return {
            'name': _('Customers on the Sanction List'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def action_view_global_pep_customer(self):
        # Check if the current user belongs to the Chief Compliance Officer group
        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')

        domain = [('origin', 'in', ['demo', 'test', 'prod']),
                  ('likely_pep', '=', True)]

        # Set domain based on user group
        if not is_chief_compliance_officer:
            domain.append(('branch_id.id', 'in', [
                e.id for e in self.env.user.branches_id]))

        return {
            'name': _('Customers on the Global PEP List'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }

    def get_risk_score(self):
        return self.risk_score

    def get_risk_level(self):
        return self.risk_level

    @api.depends('risk_score')
    def _compute_risk_level(self):
        for record in self:
            if record.risk_score <= float(self.env['res.compliance.settings'].get_setting('low_risk_threshold')):
                record.risk_level = "low"
            elif record.risk_score <= float(self.env['res.compliance.settings'].get_setting('medium_risk_threshold')):
                record.risk_level = "medium"
            else:
                record.risk_level = "high"

    @api.onchange('risk_score')
    def _onchange_risk_score(self):
        if self.risk_score <= float(self.env['res.compliance.settings'].get_setting('low_risk_threshold')):
            self.risk_level = "low"
        elif self.risk_score <=  float(self.env['res.compliance.settings'].get_setting('medium_risk_threshold')):
            self.risk_level = "medium"
        else:
            self.risk_level = "high"

    def get_risk_level_name(self):
        return '%s risk' % (self.risk_level)

    @api.model
    def compute_risk_score_for_all_users(self):
        _logger.info(
            "Starting risk score computation for all users with ORM tracking")

        # Configuration
        batch_size = 500  # Smaller batch size since ORM operations are heavier
        total_processed = 0
        total_records = self.search_count([])

        # Process in batches to reduce memory usage
        offset = 0
        while offset < total_records:
            # Get batch of records
            batch = self.search([], limit=batch_size, offset=offset)

            # Group records by risk score and level for bulk processing
            groups = {}
            for record in batch:
                score = record._get_risk_score_from_plan()
                risk_level = self.compute_customer_rating(score)
                key = (score, risk_level)
                if key not in groups:
                    groups[key] = self.env[self._name]
                groups[key] |= record
                
                if score > float(self.env['res.compliance.settings'].get_setting('maximum_risk_threshold')):
                    score = float(self.env['res.compliance.settings'].get_setting('maximum_risk_threshold'))

            # Use ORM write for each group of records with same values
            for (score, risk_level), records in groups.items():
                records.write({
                    'risk_score': score,
                    'risk_level': risk_level
                })

            # Commit transaction to free memory
            self.env.cr.commit()

            # Update progress
            total_processed += len(batch)
            _logger.info(
                f"Processed risk scores: {total_processed}/{total_records}")

            # Move to next batch
            offset += batch_size

        _logger.info(
            f"Completed risk score computation for {total_processed} users")
        return True

    def action_compute_risk_score_with_plan(self):
        """Manual action to compute risk score using ORM for proper tracking"""
        for record in self:
            # Calculate the risk score and level
            score = record._get_risk_score_from_plan()
            risk_level = self.compute_customer_rating(score)
            
            if record.composite_risk_score and record.composite_risk_score >0:
                composite_risk_score = record.composite_risk_score
                score = composite_risk_score + score
            if score > float(self.env['res.compliance.settings'].get_setting('maximum_risk_threshold')):
                score = float(self.env['res.compliance.settings'].get_setting('maximum_risk_threshold'))

            # Use ORM write method to update and track changes
            record.sudo().write({
                'risk_score': score,
                'risk_level': risk_level
            })

        return True


    def action_greylist(self):
        for e in self:
            e.write({'is_greylist': True})
            e.action_compute_risk_score_with_plan()

    def action_remove_greylist(self):
        for e in self:
            e.write({'is_greylist': False})
            e.action_compute_risk_score_with_plan()
            
    
    
    # Smart button method to show customer's channels
    def action_view_channel_subscriptions(self):
        self.ensure_one()
        return {
            'name': 'Digital Channels',
            'view_mode': 'tree,form',
            'res_model': 'customer.channel.subscription',
            'domain': [('customer_id', '=', self.customer_id)],
            'type': 'ir.actions.act_window',
            'context': {'default_customer_id': self.customer_id}
        }


class Partner(models.Model):
    _inherit = 'res.partner'

    @api.model
    def remove_unwanted_partner_actions(self):
        """Remove unwanted actions from partner view"""
        _logger.info("Starting removal of unwanted partner actions")

        # Try various possible XML IDs for merge actions
        merge_action_refs = [
            'base.partner_merge_automatic_wizard_action',
            'contacts.action_partner_merge',
            'base_partner_merge.action_partner_merge_automatic',
            'base_partner_merge.partner_merge_automatic_wizard_action'
        ]

        for xml_id in merge_action_refs:
            try:
                action = self.env.ref(xml_id, raise_if_not_found=False)
                if action:
                    _logger.info("Found merge action with XML ID: %s", xml_id)
                    action.binding_model_id = False
            except Exception as e:
                _logger.warning(
                    "Error when trying to disable %s: %s", xml_id, e)

        # Try various possible XML IDs for portal actions
        portal_action_refs = [
            'portal.partner_portal_action',
            'portal.portal_share_action',
            'portal.portal_share_wizard_action'
        ]

        for xml_id in portal_action_refs:
            try:
                action = self.env.ref(xml_id, raise_if_not_found=False)
                if action:
                    _logger.info("Found portal action with XML ID: %s", xml_id)
                    action.binding_model_id = False
            except Exception as e:
                _logger.warning(
                    "Error when trying to disable %s: %s", xml_id, e)

        # Try various possible XML IDs for email actions
        email_action_refs = [
            'mail.action_partner_mass_mail',
            'mail.email_compose_message_wizard_action',
            'mail.action_email_compose_message_wizard'
        ]

        for xml_id in email_action_refs:
            try:
                action = self.env.ref(xml_id, raise_if_not_found=False)
                if action:
                    _logger.info("Found email action with XML ID: %s", xml_id)
                    action.binding_model_id = False
            except Exception as e:
                _logger.warning(
                    "Error when trying to disable %s: %s", xml_id, e)

        # Fallback to searching for actions by model and name pattern
        try:
            # Find merge actions
            merge_actions = self.env['ir.actions.act_window'].search([
                ('res_model', '=', 'base.partner.merge.automatic.wizard'),
                ('binding_model_id.model', '=', 'res.partner')
            ])
            if merge_actions:
                merge_actions.write({'binding_model_id': False})

            # Find actions with "Mail" or "Email" in name
            email_actions = self.env['ir.actions.act_window'].search([
                ('binding_model_id.model', '=', 'res.partner')
            ])
            for action in email_actions:
                if 'mail' in action.name.lower() or 'email' in action.name.lower():
                    action.binding_model_id = False

            # Find actions with "Portal" in name
            portal_actions = self.env['ir.actions.server'].search([
                ('binding_model_id.model', '=', 'res.partner')
            ])
            for action in portal_actions:
                if 'portal' in action.name.lower():
                    action.binding_model_id = False
        except Exception as e:
            _logger.error("Error in fallback action search: %s", e)

        _logger.info("Completed removal of unwanted partner actions")
        return True
