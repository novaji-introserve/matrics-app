# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
from psycopg2 import ProgrammingError
import logging
from dotenv import load_dotenv
import psycopg2
import os
from datetime import timedelta, datetime
import pytz
from dateutil.relativedelta import relativedelta


load_dotenv()
_logger = logging.getLogger(__name__)


LOW_RISK_THRESHOLD = 10
MEDIUM_RISK_THRESHOLD = 15
HIGH_RISK_THRESHOLD = 25


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
    _description = "Partner Risk Plan Lines"
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
        comodel_name='res.branch', string='Branch', index=True, tracking=True, readonly=True)
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
        comodel_name='res.partner.gender', string='Sex', index=True, readonly=True)
    firstname = fields.Char(string='Firstname', readonly=True)
    # fullname = fields.Char(string='Fullname')
    short_name = fields.Char(string='Short name', readonly=True)
    lastname = fields.Char(string='Lastname', readonly=True)
    middlename = fields.Char(string='Middle Name', readonly=True)
    othername = fields.Char(string='Other Name', readonly=True)
    town = fields.Char(string='Town', readonly=True)
    registration_date = fields.Date(
        string='Registration Date', tracking=True, required=False, readonly=True)
    company_reg_date = fields.Date(
        string='Company Registration Date', tracking=True)
    risk_score = fields.Float(
        string='Risk Score', digits=(10, 2), tracking=True, group_operator='avg')
    risk_level = fields.Char(
        string='Risk Level', index=True, default='low', tracking=True)
    account_officer_id = fields.Many2one(
        comodel_name='account.officers', string='Account Officer', index=True, tracking=True, readonly=True)
    risk_level_id = fields.Many2one(
        comodel_name='res.risk.level', string='Risk Level', index=True)
    account_ids = fields.One2many(
        comodel_name='res.partner.account', inverse_name='customer_id', string='Accounts', readonly=True)
    
    res_partner_account_ids = fields.One2many(
        'res.partner.account', 'customer_id', string='Accounts', readonly=True)
    
    edd_ids = fields.One2many(
        comodel_name='res.partner.edd', inverse_name='customer_id', string='EDD Lines', tracking=True)
    shareholder_ids = fields.One2many(
        comodel_name='res.partner.shareholders', inverse_name='customer_id', string='Shareholder', tracking=True)
    risk_plan_line_ids = fields.One2many(
        comodel_name='res.partner.risk.plan.line', inverse_name='partner_id', string='Risk Assessment Plan')
    risk_assessment_ids = fields.One2many(
        comodel_name='res.risk.assessment', inverse_name='partner_id', string='Risk Assessments')
    is_pep = fields.Boolean(string="Is PEP", default=False, tracking=True, index=True)
    is_watchlist = fields.Boolean(
        string="Is Watchlist", default=False, tracking=True)
    is_fep = fields.Boolean(string="Is FEP", default=False, tracking=True)
    is_blacklist = fields.Boolean(
        string="Is Blacklist", default=False, tracking=True)
    global_pep = fields.Boolean(string="Global PEP", default=False)
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
        string='Accounts', compute='customer_total_accounts', store=False)
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
        string="Region", required=False, readonly=True)
    nin = fields.Char(
        string="National Identification Number (NIN)", required=False, readonly=True)
    customer_rating = fields.Char(
        string="Customer Rating", required=False, readonly=True)
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
        string='Phone Number(s)', compute='_compute_formatted_phone')

    likely_sanction = fields.Boolean()
    likely_pep = fields.Boolean()
    branch_code = fields.Char(string="Branch Code")
    
    @api.depends('customer_phone')
    def _compute_formatted_phone(self):
        for record in self:
            if record.customer_phone and '^' in record.customer_phone:
                record.formatted_phone = record.customer_phone.replace(
                    '^', ', ')
            else:
                record.formatted_phone = record.customer_phone
    
    
    # is_branch_compliance = fields.Boolean(
    #     string="Is Branch Compliance Officer",
    #     compute="_compute_is_branch_compliance"
    # )

    # def cron_run_risk_assessment(self):
    #     self.update_sanction_status()
    #     self.compute_risk_score_for_all_users()

    # def update_sanction_status(self):

    #     query_fetch = """
    #         SELECT id, firstname, lastname 
    #         FROM res_partner
    #         WHERE firstname IS NOT NULL AND lastname IS NOT NULL
    #     """
    #     self.env.cr.execute(query_fetch)
    #     partners = self.env.cr.fetchall()

    #     if not partners:
    #         _logger.info("No customers found in res_partner.")
    #         return

    #     # Prepare a set of unique full names
    #     full_names = list(
    #         set(f"{first} {last}" for _, first, last in partners))
    #     _logger.info(f"Unique customer full names: {full_names}")

    #     #  DB Update

    # #   Update global_pep from res_pep
    #     query_update_pep = """
    #         UPDATE res_partner
    #         SET global_pep = True
    #         FROM res_pep
    #         WHERE LOWER(TRIM(res_partner.firstname)) || ' ' || LOWER(TRIM(res_partner.lastname)) = LOWER(TRIM(res_pep.name))
    #     """
    #     self.env.cr.execute(query_update_pep)

    #     #  Update is_blacklist from res_partner_blacklist
    #     query_update_blacklist = """
    #         UPDATE res_partner
    #         SET is_blacklist = TRUE
    #         FROM res_partner_blacklist
    #         WHERE LOWER(TRIM(res_partner.firstname)) || ' ' || LOWER(TRIM(res_partner.lastname)) = 
    #         LOWER(TRIM(res_partner_blacklist.first_name)) || ' ' || LOWER(TRIM(res_partner_blacklist.surname))
    #     """
    #     self.env.cr.execute(query_update_blacklist)

    #     # Update is_watchlist from res_partner_watchlist
    #     query_update_watchlist = """
    #         UPDATE res_partner
    #         SET is_watchlist = TRUE
    #         FROM res_partner_watchlist
    #         WHERE LOWER(TRIM(res_partner.firstname)) || ' ' || LOWER(TRIM(res_partner.lastname)) =
    #         LOWER(TRIM(res_partner_watchlist.first_name)) || ' ' || LOWER(TRIM(res_partner_watchlist.surname))
    #     """
    #     self.env.cr.execute(query_update_watchlist)

    #     self.env.cr.commit()
    #     _logger.info(
    #         "Sanction status update completed for global_pep, blacklist, and watchlist.")

    # industry =

    @api.model
    def cron_run_risk_assessment(self):
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
            
            cron_record.write({
                'nextcall': fields.Datetime.now() + timedelta(hours=24)
            })
            self.env.cr.commit()

            _logger.info("Starting scheduled risk assessment process")
            results = {'sanction_status': {}, 'risk_scores': 0}

            try:
                results['sanction_status'] = self.update_sanction_status()
                _logger.info("Sanction status update completed successfully")
            except Exception as e:
                _logger.error(
                    f"Error in update_sanction_status: {str(e)}", exc_info=True)

            try:
                results['risk_scores'] = self.compute_risk_score_for_all_users()
                _logger.info("Risk score computation completed successfully")
            except Exception as e:
                _logger.error(
                    f"Error in compute_risk_score_for_all_users: {str(e)}", exc_info=True)

            _logger.info(f"Completed full risk assessment process: {results}")
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
        # Drop the trigger if it exists
        self.env.cr.execute(
            "DROP TRIGGER IF EXISTS set_partner_defaults ON res_partner;")
        self.env.cr.execute(
            "DROP TRIGGER IF EXISTS set_partner_defaults_after ON res_partner;")

        # Create the trigger
        self.env.cr.execute("""
            CREATE OR REPLACE FUNCTION set_partner_defaults_func()
            RETURNS TRIGGER AS $$
            BEGIN

                -- Check if this is demo data (origin = 'demo')
                IF NEW.origin = 'demo' THEN
                    -- For demo data: Set defaults but preserve certain fields like risk_level
                    -- Save the original risk_level value if it exists
                    DECLARE original_risk_level VARCHAR;
                    BEGIN
                        original_risk_level := NEW.risk_level;
                        
                        -- Set basic defaults
                        NEW.create_uid = 1;
                        NEW.write_uid = 1;
                        NEW.type = 'contact';
                        NEW.lang = 'en_US';
                        NEW.color = 0;
                        NEW.tz = 'Africa/Lagos';
                        
                        
                        
                        -- Restore the original risk_level if it was set
                        IF original_risk_level IS NOT NULL THEN
                            NEW.risk_level := original_risk_level;
                        END IF;
                        
                        RETURN NEW;
                    END;
                END IF;

                IF NEW.active IS NULL THEN
                    NEW.active = TRUE;
                END IF;
                
                IF NEW.type IS NULL THEN
                    NEW.type = 'contact';
                END IF;
                
                IF NEW.lang IS NULL THEN
                    NEW.lang = 'en_US';
                END IF;
                
                IF NEW.create_uid IS NULL THEN
                    NEW.create_uid = 1;
                END IF;
                
                IF NEW.write_uid IS NULL THEN
                    NEW.write_uid = 1;
                END IF;
                    
                IF NEW.color IS NULL THEN
                    NEW.color = 0;
                END IF;
                
                IF NEW.create_date IS NULL THEN
                    NEW.create_date = NOW();
                END IF;
                
                IF NEW.tz IS NULL THEN
                    NEW.tz = 'Africa/Lagos';
                END IF;
                
                IF NEW.write_date IS NULL THEN
                    NEW.write_date = NOW();
                END IF;
                
                IF NEW.internal_category IS NULL THEN
                    NEW.internal_category = 'customer';
                END IF;
                
                IF NEW.commercial_partner_id IS NULL THEN
                    NEW.commercial_partner_id = NEW.id;
                END IF;

                IF (NEW.display_name IS NULL OR TRIM(NEW.display_name) = '') AND NEW.name IS NOT NULL THEN
                    NEW.display_name = NEW.name;
                END IF;
                
                -- Set commercial_partner_id to the record's ID after insert
                -- This requires a BEFORE INSERT trigger to work properly
                IF NEW.commercial_partner_id IS NULL THEN
                    -- Using NEW.id directly in a BEFORE INSERT trigger
                    -- This will work since the record already has an ID before the trigger
                    NEW.commercial_partner_id = NEW.id;
                END IF;
                
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            CREATE TRIGGER set_partner_defaults
            BEFORE INSERT ON res_partner
            FOR EACH ROW
            EXECUTE FUNCTION set_partner_defaults_func();
        """)

        # Create AFTER INSERT trigger for commercial_partner_id
        self.env.cr.execute("""
            CREATE OR REPLACE FUNCTION set_partner_defaults_after_func()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.commercial_partner_id IS NULL THEN
                    UPDATE res_partner SET commercial_partner_id = NEW.id WHERE id = NEW.id;
                END IF;
                
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            CREATE TRIGGER set_partner_defaults_after
            AFTER INSERT ON res_partner
            FOR EACH ROW
            EXECUTE FUNCTION set_partner_defaults_after_func();
        """)

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

    # @api.depends('account_ids')
    # def _total_accounts(self):
    #     for e in self:
    #         e.total_accounts = len(e.account_ids)
            
    @api.depends('res_partner_account_ids')
    def customer_total_accounts(self):
        for e in self:
            e.total_accounts = len(e.res_partner_account_ids)

    def action_total_accounts(self):
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
                if record.risk_score <= LOW_RISK_THRESHOLD:
                    return 'low'
                if record.risk_score <= MEDIUM_RISK_THRESHOLD:
                    return 'medium'
                if record.risk_score <= HIGH_RISK_THRESHOLD:
                    return 'high'
            except:
                return 'low'
    
    def compute_customer_rating(self,score):
        try:
            if score is None:
                return 'low'
            if score <= LOW_RISK_THRESHOLD:
                return 'low'
            if score <= MEDIUM_RISK_THRESHOLD:
                return 'medium'
            if score <= HIGH_RISK_THRESHOLD:
                return 'high'
        except:
            return 'low'

    @api.model
    def _get_risk_level_from_score(self, risk_score):
        try:
            if risk_score is None:
                return 'low'
            if risk_score <= LOW_RISK_THRESHOLD:
                return 'low'
            if risk_score <= MEDIUM_RISK_THRESHOLD:
                return 'medium'
            if risk_score <= HIGH_RISK_THRESHOLD:
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

        # Set domain based on user group
        if is_chief_compliance_officer:
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

        # Set domain based on user group
        if is_chief_compliance_officer:
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

        # Set domain based on user group
        if is_chief_compliance_officer:
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

        # Set domain based on user group
        if is_chief_compliance_officer:
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

        # Set domain based on user group
        if is_chief_compliance_officer:
            # Chief Compliance Officers see all customers
            domain = [('internal_category', '=', 'respondent'),
                      ('origin', 'in', ['demo', 'test', 'prod'])]
        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id]),
                ('internal_category', '=', 'respondent'), 
                ('origin','in', ['demo', 'test', 'prod'])

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
        # Check if the current user belongs to the Chief Compliance Officer group
        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')

        domain = [('origin', 'in', ['demo', 'test', 'prod']), ('likely_sanction', '=', True)]

        # Set domain based on user group
        if not is_chief_compliance_officer:
            domain.append(('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id]))

        return {
            'name': _('Likely Santions List'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def action_view_likely_pep(self):
        # Check if the current user belongs to the Chief Compliance Officer group
        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')

        domain = [('origin', 'in', ['demo', 'test', 'prod']), ('likely_pep', '=', True)]

        # Set domain based on user group
        if not is_chief_compliance_officer:
            domain.append(('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id]))

        return {
            'name': _('Likely Santions List'),
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
            if record.risk_score <= LOW_RISK_THRESHOLD:
                record.risk_level = "low"
            elif record.risk_score <= MEDIUM_RISK_THRESHOLD:
                record.risk_level = "medium"
            else:
                record.risk_level = "high"

    @api.onchange('risk_score')
    def _onchange_risk_score(self):
        if self.risk_score <= LOW_RISK_THRESHOLD:
            self.risk_level = "low"
        elif self.risk_score <= MEDIUM_RISK_THRESHOLD:
            self.risk_level = "medium"
        else:
            self.risk_level = "high"

    def get_risk_level_name(self):
        return '%s risk' % (self.risk_level)

    # logic to commpute total risk sore of all users

    # @api.model
    # def compute_risk_score_for_all_users(self):
    #     records = self.search([])
    #     for record in records:
    #         score = record._get_risk_score_from_plan()
    #         risk_level = record.compute_risk_level()

    #         # Use direct SQL update to avoid triggering write()
    #         self.env.cr.execute(
    #             """UPDATE %s SET risk_score = %%s, risk_level = %%s 
    #             WHERE id = %%s""" % self._table,
    #             (score, risk_level, record.id)
    #         )

    #         # Invalidate cache for these fields
    #         # record.invalidate_cache(['risk_score', 'risk_level'])
    #         record.invalidate_recordset(['risk_score', 'risk_level'])

    #     return True

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
    
    
    # def action_compute_risk_score_with_plan(self):
    #     """Manual action to compute risk score"""
    #     for record in self:
    #         score = record._get_risk_score_from_plan()
    #         risk_level = record.compute_risk_level()

    #         # Use direct SQL update to avoid triggering write()
    #         self.env.cr.execute(
    #             """UPDATE %s SET risk_score = %%s, risk_level = %%s 
    #             WHERE id = %%s""" % self._table,
    #             (score, risk_level, record.id)
    #         )

    #         # Invalidate cache for these fields
    #         record.invalidate_recordset(['risk_score', 'risk_level'])

    #     return True
    def action_compute_risk_score_with_plan(self):
        """Manual action to compute risk score using ORM for proper tracking"""
        for record in self:
            # Calculate the risk score and level
            score = record._get_risk_score_from_plan()
            risk_level = self.compute_customer_rating(score)
            
            # Use ORM write method to update and track changes
            record.sudo().write({
                'risk_score': score,
                'risk_level': risk_level
            })
        
        return True

    def _get_risk_score_from_plan(self):
        setting = self.env['res.compliance.settings'].search(
            [('code', '=', 'risk_plan_computation')], limit=1)

        # Default value if no settings found
        plan_setting = 'avg'  # Default to 'avg'
        for e in setting:
            plan_setting = e.val

        record_id = self.id
        self.env["res.partner.risk.plan.line"].search(
            [('partner_id', '=', record_id)]).unlink()
        scores = []
        plans = self.env['res.compliance.risk.assessment.plan'].search(
            [('state', '=', 'active')], order='priority')

        if plans:
            for pl in plans:
                score = 0
                try:
                    self.env.cr.execute(pl.sql_query, (record_id,))
                    rec = self.env.cr.fetchone()
                    if rec is not None:
                        # we have a hit
                        if pl.compute_score_from == 'dynamic':
                            score = float(rec[0]) if rec is not None else score
                        if pl.compute_score_from == 'static':
                            score = pl.risk_score
                    scores.append(score)
                    line_id = self.env['res.partner.risk.plan.line'].create({
                        'partner_id': record_id,
                        'plan_line_id': pl.id,
                        'risk_score': score,
                    })
                except:
                    pass

        # Default value for records to avoid unbound variable error
        records = None

        if len(scores) > 0:
            if plan_setting == 'avg':
                self.env.cr.execute(
                    f"select avg(risk_score) from res_partner_risk_plan_line where partner_id={record_id} and risk_score > 0")
            if plan_setting == 'max':
                self.env.cr.execute(
                    f"select max(risk_score) from res_partner_risk_plan_line where partner_id={record_id}")
            records = self.env.cr.fetchone()

        # Ensure records is not None before returning
        return records[0] if records is not None else 0.00

    def action_greylist(self):
        for e in self:
            e.write({'is_greylist': True})
            e.action_compute_risk_score_with_plan()

    def action_remove_greylist(self):
        for e in self:
            e.write({'is_greylist': False})
            e.action_compute_risk_score_with_plan()

    # @api.model
    # def _compute_is_branch_compliance(self):
    #     # Check if the current user belongs to the Chief Compliance Officer group
    #     # coo_group = self.env.ref(
    #     #     'compliance_management.group_compliance_chief_compliance_officer')

    #     is_branch_compliance_officer = self.env.ref(
    #         'compliance_management.group_compliance_branch_compliance_officer')
    #     # Set domain based on user group
    #     for record in self:
    #         record.is_branch_compliance = is_branch_compliance_officer
