
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


load_dotenv()
_logger = logging.getLogger(__name__)


LOW_RISK_THRESHOLD = 10
MEDIUM_RISK_THRESHOLD =  15
HIGH_RISK_THRESHOLD =  16


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
        string="Region", required=False, readonly=True)
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
        # Drop the trigger if it exists
        self.env.cr.execute(
            "DROP TRIGGER IF EXISTS set_partner_defaults ON res_partner;")
        self.env.cr.execute(
            "DROP TRIGGER IF EXISTS set_partner_defaults_after ON res_partner;")

        # Create index on res_partner which we know exists
        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS res_partner_id_idx ON res_partner (id)")

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

        # self.cron_run_risk_assessment()

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
                if record.risk_score >= float(self.env['res.compliance.settings'].get_setting('high_risk_threshold')):
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
            if score >= float(self.env['res.compliance.settings'].get_setting('high_risk_threshold')):
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
            if risk_score >= float(self.env['res.compliance.settings'].get_setting('high_risk_threshold')):
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
        """
        - First check for approved EDD then use the Plan if no EDD
        """
        approved_edd = self.env['res.partner.edd'].search(
            [('status', '=', 'approved'),('customer_id','=',record_id)],order='date_approved desc', limit=1)
        for edd in approved_edd:
            if edd.risk_score:
                return edd.risk_score
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

    # @api.depends('customer_phone')
    # def _compute_formatted_phone(self):
    #     for record in self:
    #         if record.customer_phone and '^' in record.customer_phone:
    #             record.formatted_phone = record.customer_phone.replace(
    #                 '^', ', ')
    #         else:
    #             record.formatted_phone = record.customer_phone

    # def _compute_risk_scores(self):
    #     """Cron job to precompute and store weighted average risk scores."""
    #     # Clear existing records
    #     self.env['customer.agg.risk.score'].search([]).unlink()

    #     # Group customers by branch_id
    #     customers = self.search([['internal_category', '=', 'customer'], ['origin', 'in', ['demo', 'test', 'prod']]])

    #     grouped_data = {}
    #     for record in customers:
    #         group_key = record.branch_id
    #         group_key_value = group_key.display_name if group_key else 'No Branch'
    #         if group_key_value not in grouped_data:
    #             grouped_data[group_key_value] = []
    #         grouped_data[group_key_value].append(record)

    #     # Compute and store weighted averages
    #     for key, group_records in grouped_data.items():
    #         total_customers = len(group_records)
    #         formatted_key = f"{key}({total_customers})" if total_customers > 0 else key

    #         if total_customers == 0:
    #             weighted_avg = 0.0

    #         else:
    #             risk_counts = {'low': 0, 'medium': 0, 'high': 0}
    #             risk_scores = {'low': 0, 'medium': 0, 'high': 0}
    #             for rec in group_records:
    #                 risk_level = rec.risk_level.lower() if rec.risk_level else 'low'
    #                 risk_counts[risk_level] = risk_counts.get(risk_level, 0) + 1
    #                 risk_scores[risk_level] += rec.risk_score or 0.0
    #             _logger.info("start of each branch calculation")
    #             _logger.info(f"the risk_count is {risk_counts}")
    #             _logger.info(f"the risk_scores is {risk_scores}")

    #             # Compute mean average per risk level
    #             mean_avg_low = risk_scores['low'] / risk_counts['low'] if risk_counts['low'] > 0 else 0.0
    #             mean_avg_medium = risk_scores['medium'] / risk_counts['medium'] if risk_counts['medium'] > 0 else 0.0
    #             mean_avg_high = risk_scores['high'] / risk_counts['high'] if risk_counts['high'] > 0 else 0.0

    #             _logger.info(f"the mean avg is {mean_avg_low} | {mean_avg_medium} | {mean_avg_high}")
    #             _logger.info(f"total customer is {total_customers}")

    #             weighted_avg = ((risk_counts['low'] * mean_avg_low) +
    #                         (risk_counts['medium'] * mean_avg_medium) +
    #                         (risk_counts['high'] * mean_avg_high)) / total_customers if total_customers > 0 else 0.0

    #             _logger.info(f"for low risk level customer is {risk_counts['low']} and avg = {mean_avg_low} sum up to  {(risk_counts['low'] * mean_avg_low)}")
    #             _logger.info(f"for medium risk level customer is {risk_counts['medium']} and avg = {mean_avg_medium} sum up to {(risk_counts['medium'] * mean_avg_medium)}")
    #             _logger.info(f"for high risk level customer is {risk_counts['high']} and avg = {mean_avg_high} sum up to {(risk_counts['high'] * mean_avg_high)}")

    #             _logger.info(f"the weighted avg is {weighted_avg}")
    #             _logger.info("end of each branch calculation")

    #         # Store in customer.risk.score
    #         branch = self.env['res.branch'].search([('name', '=', key)], limit=1)
    #         self.env['customer.agg.risk.score'].create({
    #             'branch_id': branch.id if branch else False,
    #             'weighted_avg_risk_score': weighted_avg,
    #             'total_customers': total_customers,
    #             'formatted_name': formatted_key
    #         })

    # def read_group(self, domain, fields, groupby, offset=0, limit=None, orderby=False, lazy=True):
    #     if not any(f in fields for f in ['risk_score']):
    #         return super().read_group(domain, fields, groupby, offset, limit, orderby, lazy)

    #     result = []
    #     groupby_field = groupby[0] if groupby else None

    #     if groupby_field == 'branch_id':
    #         # Parse the orderby parameter to determine sorting
    #         order_field = 'branch_id'  # Default order field
    #         order_direction = 'ASC'    # Default direction

    #         if orderby:
    #             # Handle multiple orderby fields separated by comma
    #             orderby_parts = orderby.split(',')
    #             for part in orderby_parts:
    #                 part = part.strip()
    #                 if 'risk_score' in part:
    #                     order_field = 'weighted_avg_risk_score'
    #                     order_direction = 'DESC' if 'DESC' in part.upper() else 'ASC'
    #                     break
    #                 elif 'branch_id' in part:
    #                     order_field = 'branch_id'
    #                     order_direction = 'DESC' if 'DESC' in part.upper() else 'ASC'
    #                     break

    #         # Build the order string for the search
    #         order_str = f"{order_field} {order_direction}"

    #         # Fetch precomputed data with pagination
    #         risk_scores = self.env['customer.agg.risk.score'].search(
    #             [], order=order_str, offset=offset, limit=limit
    #         )

    #         # Get total count for pagination info
    #         total_count = self.env['customer.agg.risk.score'].search_count([])

    #         for risk_score in risk_scores:
    #             group_result = {
    #                 'branch_id': risk_score.branch_id.display_name if risk_score.branch_id else False,
    #                 'branch_id_count': risk_score.total_customers,
    #                 'branch_id:formatted': risk_score.formatted_name,
    #                 'risk_score': risk_score.weighted_avg_risk_score,
    #                 '__count': risk_score.total_customers,
    #                 '__domain': [('branch_id', '=', risk_score.branch_id.id if risk_score.branch_id else False)] + domain
    #             }
    #             # Only include requested fields
    #             result.append(group_result)

    #         # Add pagination metadata if needed
    #         if hasattr(result, '__dict__'):
    #             result.__dict__['total_count'] = total_count

    #         return result
    #     else:
    #         # Fallback to super if grouping by a different field
    #         return super().read_group(domain, fields, groupby, offset, limit, orderby, lazy)


class CustomerDigitalProduct(models.Model):
    _name = 'customer.digital.product'
    _sql_constraints = [
        ('uniq_customer_id', 'unique(customer_id)',
         "Customer already exists. Customer must be unique!"),
    ]

    customer_id = fields.Text(string='Customer ID',
                              index=True, readonly=True)  # customer,
    customer_name = fields.Char(string='Name', tracking=True, readonly=True)
    customer_segment = fields.Char(
        string='Customer Segment', tracking=True, readonly=True)
    ussd = fields.Char(string='Uses USSD', index=True, readonly=True)
    onebank = fields.Char(string='Uses One Bank', index=True, readonly=True)
    carded_customer = fields.Char(
        string='Has A Card', index=True, readonly=True)
    alt_bank = fields.Char(string='Is On Alt Bank', readonly=True)
    sterling_pro = fields.Char(string='Has Sterling Pro', readonly=True)
    banca = fields.Char(string='Has Banca', readonly=True)
    doubble = fields.Char(string='Has Doubble', readonly=True)
    specta = fields.Char(string='Has Specta', readonly=True)
    switch = fields.Char(string='Has Switch', readonly=True)

    def init(self):
        # Drop the trigger if it exists
        self.env.cr.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'customer_digital_product'
        )
    """)
        table_exists = self.env.cr.fetchone()[0]

        if table_exists:
            # Create the index if it doesn't exist
            self.env.cr.execute("""
                CREATE INDEX IF NOT EXISTS customer_digital_product_id_idx
                ON customer_digital_product (id)
            """)

# view model to display Customer digital products
class PartnerDigitalProductView(models.Model):
    _name = 'res.partner.digital.product.view'
    _description = 'Partner Digital Products View'
    _auto = False  # This is a database view

    partner_id = fields.Many2one(
        'res.partner', string='Partner', readonly=True)
    customer_id = fields.Char(string='Customer ID', readonly=True)
    ussd = fields.Char(string='Uses USSD', readonly=True)
    onebank = fields.Char(string='Uses One Bank', readonly=True)
    carded_customer = fields.Char(string='Has A Card', readonly=True)
    alt_bank = fields.Char(string='Is On Alt Bank', readonly=True)
    sterling_pro = fields.Char(string='Has Sterling Pro', readonly=True)
    banca = fields.Char(string='Has Banca', readonly=True)
    doubble = fields.Char(string='Has Doubble', readonly=True)
    specta = fields.Char(string='Has Specta', readonly=True)
    switch = fields.Char(string='Has Switch', readonly=True)
    customer_segment = fields.Char(string='Customer Segment', readonly=True)

    def init(self):
        """Create database view that joins partner with digital products"""
        tools.drop_view_if_exists(self.env.cr, self._table)
        self.env.cr.execute("""
            CREATE OR REPLACE VIEW %s AS (
                SELECT 
                    cdp.id,  
                    rp.id AS partner_id,
                    cdp.customer_id,
                    cdp.ussd,
                    cdp.onebank,
                    cdp.carded_customer,
                    cdp.alt_bank,
                    cdp.sterling_pro,
                    cdp.banca,
                    cdp.doubble,
                    cdp.specta,
                    cdp.switch,
                    cdp.customer_segment
                FROM customer_digital_product cdp
                JOIN res_partner rp ON cdp.customer_id = rp.customer_id
                WHERE rp.customer_id IS NOT NULL
            )
        """ % self._table)


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
