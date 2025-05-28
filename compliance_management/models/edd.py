import os
import logging
from datetime import datetime
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError

# from odoo.exceptions import ValidationError, UserError
_logger = logging.getLogger(__name__)


class CustomerEDD(models.Model):
    _name = 'res.partner.edd'
    _description = 'Enhanced Due Diligence'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string="Name", tracking=True)
    status = fields.Selection(
        string='Status',
        selection=[
            ('draft', 'Draft'),
            ('completed', 'Completed'),
            ('approved', 'Approved'),
            ('cancelled', 'Cancelled'),
            ('deleted', 'Deleted'),
            ('archived', 'Archived')],
        default='draft',
        tracking=True
    )
    description = fields.Text(string='Description', tracking=True)
    user_id = fields.Many2one(comodel_name='res.users', string='User',
                              index=True, default=lambda self: self.env.user.id)
    current_user_id = fields.Many2one(comodel_name='res.users', string='Current User',
                                      index=True, default=lambda self: self.env.user.id)
    approved_by = fields.Many2one(
        comodel_name='res.users', string='Approver', readonly=True)
    customer_id = fields.Many2one(comodel_name='res.partner', string='Customer',
                                  index=True, domain="[('origin', 'in', ['demo', 'test', 'prod'])]")
    responsible_id = fields.Many2one(
        comodel_name='res.users', string='Responsible User', index=True)
    risk_score = fields.Float(
        # Add a default value
        string='Risk Score', tracking=True, inverse='_inverse_risk_score', store=True, default=1.0)
    date_approved = fields.Date(
        string="Date Approved", readonly=True, tracking=True)
    date_reviewed = fields.Date(
        string="Date Reviewed", readonly=True, tracking=True)
    approving_officer_id = fields.Many2one(
        comodel_name='res.users', string='Approving Officer', tracking=True)
    account_status = fields.Selection(
        string='Account Status',
        selection=[
            ('active', 'Active'),
            ('dormant', 'Dormant')],
        tracking=True
    )
    documentation_status = fields.Selection(
        string='Documentation Status',
        selection=[
            ('incomplete', 'Incomplete'),
            ('complete', 'Complete')],
        tracking=True
    )
    supporting_document = fields.Many2many(
        'ir.attachment',
        'res_partner_supporting_document_rel',
        'partner_id',
        'attachment_id',
        string="Add Supporting Document(s)",
        help="Add supporting document(s) for customer EDD"
    )
    last_kyc_date = fields.Date(string="Last Kyc Date", tracking=True)
    was_kyc_comprehensive = fields.Boolean(
        string="Was Kyc Comprehensive", tracking=True)
    has_initiated_new_kyc = fields.Boolean(
        string="Has Initiated New Kyc", tracking=True)
    visitation_observation = fields.Text(
        string="Visitation Observation", tracking=True)
    overall_kyc_outcome = fields.Text(
        string="Overall Kyc Outcome", tracking=True)
    is_foreigner = fields.Boolean(string="Is Foreigner", tracking=True)
    is_pep = fields.Boolean(string="Is PEP", tracking=True)
    id_expired = fields.Boolean(string="Id Expired", tracking=True)
    activity_level_matches_business = fields.Boolean(
        string="Activity Level Matches Business", tracking=True)
    main_cash_purpose = fields.Text(string="Main Cash Purpose", tracking=True)
    main_inflow_purpose = fields.Text(
        string="Main Inflow Purpose", tracking=True)
    main_fund_remitters = fields.Text(
        string="Main Fund Remitters", tracking=True)
    inflow_sources = fields.Text(string="Inflow Sources", tracking=True)
    other_related_accounts = fields.Text(
        string="Other Related Accounts", tracking=True)
    occupation = fields.Text(string="Occupation", tracking=True)
    is_current_from_normal = fields.Boolean(
        string="Is Current From Normal", tracking=True)
    does_business_support_volume = fields.Boolean(
        string="Does Business Support Volume", tracking=True)
    
    customer_type = fields.Selection(selection=[
                                    ('individual', 'Individual'),
                                    ('corporate', 'Corporate') ], string="EDD Type", required=True, default='individual', tracking=True)

    brief_customer_profile = fields.Text(string="Please give a brief profile of the subject customer", tracking=True)
    nature_of_business = fields.Text(string="What is the specific nature of the customers business/occupation/employment?", tracking=True)
    employment_position = fields.Text(string="Employment Position", tracking=True)
    employer_name = fields.Text(string= "Name of employer/company", tracking=True)
    expected_source_of_funds = fields.Text(string="What is the expected source of funds into the customers account?", tracking=True)
    source_of_funds_document = fields.Many2many(
        'ir.attachment',
        'res_partner_source_of_funds_document_rel',
        'partner_id',
        'attachment_id',
        string="Kindly provide documentary evidence if available",
        help="Add source of wealth document(s) for customer EDD"
    )
    source_of_wealth = fields.Text(string="What is the source of wealth of the customer (e.g., inheritance, business revenue, investment income)?")
    source_of_wealth_document = fields.Many2many(
        'ir.attachment',
        'res_partner_source_of_wealth_document_rel',
        'partner_id',
        'attachment_id',
        string="Kindly provide documentary evidence if available",
        help="Add source of wealth document(s) for customer EDD"
    )
    residency_status = fields.Selection([
        ('resident_nigerian', 'Resident Nigerian'),
        ('non_resident_nigerian', 'Non-resident Nigerian'),
        ('resident_non_nigerian', 'Resident Non-Nigerian'),
    ],  string="Residency Status", help="Is the customer a resident Non-Nigerian, a Non-resident Nigerian or a Resident Nigerian?", tracking=True)
    applicable_country_ids = fields.Many2many(
        'res.country',
        'res_partner_country_rel',
        'partner_id',
        'country_id',
        string="Residential Countries",
        help = "Kindly list the countries as applicable"

    )
    is_customer_pep = fields.Boolean(string="Is the customer a Politically Exposed Person (PEP) or associated to a PEP?", tracking=True)
    relationship_with_pep = fields.Text(string="If “Yes”, state Designation/Position/relationship with PEP)", tracking=True)
    cross_border_transaction = fields.Boolean(string="Is the customer expected to engage in transactions involving cross-border fund transfers or transactions?", tracking=True)
    cross_border_jurisdictions = fields.Many2many(
        'res.country',
        'res_partner_crossborder_country_rel',
        'partner_id',
        'country_id',
        string="If “Yes,” specify the jurisdictions involved."
    )
    expected_monthly_income = fields.Float(string="What is the expected monthly income/inflow of the customer? (State in Naira - NGN)",tracking=True)
    estimated_net_worth = fields.Text(string="What is the estimated total net worth of the customer? (State in Naira or USD)", tracking=True)
    inflow_purpose = fields.Text(string="What would be the purpose of the inflows routed into the customer’s account?", tracking=True)
    inflow_document = fields.Many2many(
        'ir.attachment',
        'res_partner_inflow_document_rel',
        'partner_id',
        'attachment_id',
        string="(Provide supporting documents if applicable)",
        help="Add inflow document(s) for customer EDD"
    )
    outflow_purpose = fields.Text(string="What would be the expected purpose of the outflows from the account?", tracking=True)
    outflow_document = fields.Many2many(
        'ir.attachment',
        'res_partner_outflow_document_rel',
        'partner_id',
        'attachment_id',
        string="Provide supporting documents if applicable",
        help="Add outflow document(s) for customer EDD"
    )
    has_negative_media = fields.Boolean(
        string="Is there any negative news (Including on the internet) about the customer or the customers activities?", tracking=True)
    negative_media_details = fields.Text(string="provide details", tracking=True)
    negative_media_document = fields.Many2many(
        'ir.attachment',
        string="Attach evidence",
        help="Add negative media document(s) for customer EDD"
    )
    other_comments = fields.Text(string="Any Other Comments", tracking=True)
    attestation_checked = fields.Boolean(string="I hereby attest that the information provided above and in the enclosed documents is accurate to the best of my knowledge. I understand that providing false or incomplete information may result in disciplinary action.", tracking=True)
    responsible_officer_signature = fields.Binary(
        string="Officer In Charge Signature", tracking=True)
    responsible_signature_filename = fields.Char(tracking=True)
    approving_officer_signature = fields.Binary(
        string="Approving Officer Signature", tracking=True)
    approving_signature_filename = fields.Char(tracking=True)
    dual_citizenship_info = fields.Text(string="Do any signatories/directors/shareholders hold dual citizenship or residence in other jurisdictions?", tracking=True)
    citizenship_country_ids = fields.Many2many(
        'res.country',
        'res_partner_country_rel',
        'partner_id',
        'country_id',
        string="Citizenship Countries",
        help = "Kindly list the countries as applicable"

    )
    customer_address = fields.Char(string='Address',related='customer_id.street',store=True,readonly=False, tracking=True)
    company_registration_number = fields.Char(string='Company Registration Number (RC/BN)',help='Company Registration Number or Business Number', tracking = True)
    signatories_details = fields.Text(string='Signatories Details (Names)',help='List the names of authorized signatories', tracking=True)
    beneficial_owner_details = fields.Text(string='Beneficial Owner Details', help='List the names of the Directors/Shareholders/Person of Significant Control')
    high_risk_industries = fields.Text(string="Does the customer have any direct or indirect link to high-risk industries (e.g. Crypto, Financial Services, DNFBPs)?", tracking=True)
    pep_association = fields.Boolean(string="Is any Director/Shareholder/Signatory a Politically Exposed Person (PEP) or associated with a PEP?", tracking=True)
    pep_relationship_details = fields.Text(string="If Yes, state Designation/Position/relationship with PEP", tracking=True)
    third_party_involvement = fields.Boolean(string="Are there any third-party, proxies, nominees or legal reps managing the entity/account?",tracking=True)
    third_party_details = fields.Text(string="If Yes, provide details & documentation", tracking=True)
    third_party_document = fields.Many2many(
        'ir.attachment',
        'res_partner_third_party_document_rel',
        'partner_id',
        'attachment_id',
        string="Documentation",
        help="Add third party document(s) for customer EDD"
    )
    kycc_info = fields.Text(string="What types/nature of clientele/customers does this business provide services/goods to?")


    

    is_current_user_responsible = fields.Boolean(
        compute='_compute_is_current_user_responsible')
    is_current_user_approver = fields.Boolean(
        compute='_compute_is_current_user_approver', store=False)
    is_current_user_approving_officer = fields.Boolean(
        compute='_compute_is_current_user_approving_officer')
    is_cco = fields.Boolean(compute='_compute_is_cco', store=False,
                            default=lambda self: self._default_is_cco())

    @api.model
    def _default_is_cco(self):
        """Default method to set is_cco based on the user group."""
        return self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')

    @api.depends_context('uid')
    def _compute_is_cco(self):
        """Compute method to update is_cco based on user group when editing records."""
        for record in self:
            record.is_cco = self.env.user.has_group(
                'compliance_management.group_compliance_chief_compliance_officer')

    @api.depends('responsible_id')
    def _compute_is_current_user_responsible(self):
        for record in self:
            # Check if the responsible_id matches the current user ID
            record.is_current_user_responsible = (
                record.responsible_id.id == self.env.user.id)

    @api.depends('approved_by')
    def _compute_is_current_user_approver(self):
        for record in self:
            # Check if the approved_by matches the current user ID
            record.is_current_user_approver = (
                record.approved_by.id == self.env.user.id)

    @api.depends('approving_officer_id')
    def _compute_is_current_user_approving_officer(self):
        for record in self:
            # Check if the approved_by matches the current user ID
            record.is_current_user_approving_officer = (
                record.approving_officer_id.id == self.env.user.id)

    # logic to send email      
    def _send_email_to_officers(self, template_ref, to_cco_only, officer=None):
        try:
            template = self.env.ref(template_ref, raise_if_not_found=False)
            if not template:
                _logger.error(f"Email template not found: {template_ref}")
                raise ValidationError("Email template not found")

            # Fetch CCO user group
            cco_group = self.env.ref(
                'compliance_management.group_compliance_chief_compliance_officer')
            cco_users = cco_group.users
            
            # Get the initiating CCO details (user who created the record)
            initiating_cco = self.create_uid
            cco_email = initiating_cco.email if initiating_cco in cco_users and initiating_cco.email else None
            cco_name = initiating_cco.name if cco_email else None

            # Get current user (the officer sending the notification)
            current_user = self.env.user
            sending_officer_email = current_user.email if current_user.email else None

            if officer:
                _logger.info(f"Using provided officer: {officer.name}")
                target_officer = officer
            else:
                target_officer = self.approving_officer_id or officer.responsible_id
            
            officer = target_officer 
           
            officer_email = officer.email

            if officer is None or not officer_email:
                _logger.warning(
                    "No valid officer found or officer has no email.")
                raise ValidationError("No valid officer to send email to.")

            base_url = self.env['ir.config_parameter'].sudo(
            ).get_param('web.base.url')

            edd_url = f"{base_url}/web#id={self.id}&model=res.partner.edd&view_type=tree"
            _logger.info(f"EDD URL: {edd_url}")

            ctx = {
                'officer_name': officer.name,
                'cco': cco_name,
                'edd_url': edd_url,
            }
            _logger.info(f"your officer {officer.name}")                
            
            if to_cco_only:
                primary_email = cco_email
                email_values = {
                    'email_to': cco_email
                }
                # If sending officer is different from CCO, add them to CC
                if sending_officer_email and sending_officer_email != cco_email:
                    email_values['email_cc'] = sending_officer_email
                    
                _logger.info(
                    f"{template.name} notification sent to {cco_email}")
            else:
                primary_email = officer_email
                # Build CC list
                cc_emails = []
                if cco_email:
                    cc_emails.append(cco_email)
                if sending_officer_email and sending_officer_email != officer_email and sending_officer_email != cco_email:
                    cc_emails.append(sending_officer_email)
                
                email_values = {
                    'email_to': officer_email,
                }
                
                # Only add CC if there are emails to CC
                if cc_emails:
                    email_values['email_cc'] = ','.join(cc_emails)
                
                _logger.info(
                    f"{template.name} notification sent to {officer.email} with CC to {email_values.get('email_cc', 'none')}")
                
            
            try:
                 # Render the template with context
                template_id = template.with_context(**ctx)

                 # Get the rendered HTML content
                rendered_html = template_id._render_template(
                    template_id.body_html,
                    template_id.model,
                    [self.id],
                    engine='qweb',
                    add_context=ctx
                )[self.id]
    

                email_result = template_id.send_mail(
                    self.id,
                    force_send=True,
                    email_values=email_values
                )
                _logger.info(f"Email sent with ID: {email_result}")
                _logger.info(f"Email values: {email_values}")
            
                if email_result:    
                        _logger.info(f"Email sent successfully to {primary_email}")
                        try:
                        # create alert history record 
                            alert_history = self.env['alert.history'].sudo(flag=True).create({
                                "ref_id": f"{self._name},{self.id}",
                                'html_body': rendered_html,
                                'attachment_data':  None,
                                'attachment_link':  None,
                                'last_checked': fields.Datetime.now(),
                                'risk_rating': 'low',
                                'process_id': None,
                                'source': self._description,
                                'date_created': fields.Datetime.now(),
                                'email': primary_email,
                                'email_cc': email_values.get('email_cc', ''),
                                'narration': f"EDD notification sent via {template.name}",
                                'name': f"EDD-{self.id} Email Notification"
                                
                            })
                            _logger.info(f"Alert history created with ID: {alert_history.id}")
                            _logger.info(f"EDD notification sent to officers ({primary_email})")   
                        except Exception as history_error:
                                _logger.error(f"Failed to create alert history: {str(history_error)}")
                else:
                    error_msg = "Email sending returned no mail ID - send may have failed"
                    _logger.error(error_msg)
                    raise ValidationError(f"Failed to send email: {error_msg}")
               
            except Exception as send_error:
                _logger.error(f"Failed to send email or create history: {str(send_error)}")
                raise ValidationError(f"Failed to send notification: {str(send_error)}")
            
        except Exception as e:
            _logger.error(f"{template.name} Failed to send notification: {str(e)}")
            raise ValidationError(f"{template.name} Failed to send notification: {str(e)}")

    @api.model
    def create(self, vals):
        _logger.info(f"Creating EDD record with values: {vals}")
        record = super(CustomerEDD, self).create(vals)
        _logger.info(f"Created EDD record ID: {record.id}")

        if record.status == 'draft' and record.create_date:
            _logger.info(
                f"EDD record is in draft and has create_date — sending email.")
            record._send_email_to_officers(
                'compliance_management.enhanced_due_diligence_assessment_template',
                to_cco_only=False,
                officer=record.responsible_id
            )
        else:
            _logger.info(
                "EDD record is not in draft or has no create_date, skipping email.")
        return record

    # @api.model
    # def cron_send_for_assessment(self):
    #     records = self.search([
    #             ('status', '=', 'draft'),
    #             ('responsible_id', '!=', False),
    #             ('create_date', '!=', False)
    #         ])
    #     for record in records:
    #             self._send_email_to_officers(
    #                 'compliance_management.enhanced_due_diligence_assessment_template',
    #                 to_cco_only=False,
    #                 officer= record
    #             )

    def action_submit_for_review(self):
        self.ensure_one()
        self.write({
            'status': 'completed',
            'date_reviewed': fields.Date.today()
        })

        self._send_email_to_officers(
            'compliance_management.enhanced_due_diligence_review_template', to_cco_only=False)

        # try:
        #     template = self.env.ref('compliance_management.enhanced_due_diligence_review_template', raise_if_not_found=False)
        #     if not template:
        #         _logger.error("Email template not found: compliance_management.enhanced_due_diligence_review_template")
        #         raise ValidationError("Email template not found")
        #     approving_officer = self.approving_officer_id
        #     if not approving_officer or not approving_officer.email:
        #         _logger.warning("No approving officer configured or missing email")
        #         return

        #     ctx = {
        #         # "" add a context to the email template
        #     }
        #     template.with_context(**ctx).send_mail(
        #         self.id, force_send=True, email_values={
        #             'email_to': approving_officer.email,
        #         }
        #     )
        #     _logger.info(f"Review notification sent to {approving_officer.email}")
        # except Exception as e:
        #     _logger.error(f"Failed to send review notification: {str(e)}")
        #     raise ValidationError(f"Failed to send review notification: {str(e)}")
        #     # end of email logic

        return {
            'type': 'ir.actions.act_window',
            'name': _('Enhanced Due Diligence'),
            'res_model': 'res.partner.edd',
            'view_mode': 'list,form',
            'view_id': False,
            'views': [
                (self.env.ref('compliance_management.edd_tree_view').id, 'list'),
                (False, 'form')
            ],
            'target': 'main',
            'context': {
                'message': _('Submitted for review successfully.'),
                'type': 'success',
                'sticky': False,
            },
        }

    def action_approve(self):
        self.ensure_one()
        self.write({
            'status': 'approved',
            'approved_by': self.env.user.id,
            'date_approved': fields.Date.today(),
        })

        self._send_email_to_officers(
            "compliance_management.enhanced_due_diligence_approved_template", to_cco_only=False)

        return {
            'type': 'ir.actions.act_window',
            'name': _('Enhanced Due Diligence'),
            'res_model': 'res.partner.edd',
            'view_mode': 'list,form',
            'view_id': False,
            'views': [
                (self.env.ref('compliance_management.edd_tree_view').id, 'list'),
                (False, 'form')
            ],
            'target': 'main',
            'context': {
                'message': _('Approved successfully.'),
                'type': 'success',
                'sticky': False,
            },
        }

    def action_cancel(self):
        self.ensure_one()
        self.write({
            'status': 'cancelled',
            'approved_by': "",
            'date_approved': False,
        })

        self._send_email_to_officers(
            'compliance_management.enhanced_due_diligence_cancellation_template', to_cco_only=True)

        return {
            'type': 'ir.actions.act_window',
            'name': _('Enhanced Due Diligence'),
            'res_model': 'res.partner.edd',
            'view_mode': 'list,form',
            'view_id': False,
            'views': [
                (self.env.ref('compliance_management.edd_tree_view').id, 'list'),
                (False, 'form')
            ],
            'target': 'main',
            'context': {
                'message': _('Canceled successfully.'),
                'type': 'success',
                'sticky': False,
            },
        }

    def action_send_back(self):
        self.ensure_one()
        self.write({
            'status': 'draft',
            'approved_by': "",
            'date_approved': False,
        })
        self._send_email_to_officers(
            'compliance_management.enhanced_due_diligence_sent_back_template', to_cco_only=True)

        return {
            'type': 'ir.actions.act_window',
            'name': _('Enhanced Due Diligence'),
            'res_model': 'res.partner.edd',
            'view_mode': 'list,form',
            'view_id': False,
            'views': [
                (self.env.ref('compliance_management.edd_tree_view').id, 'list'),
                (False, 'form')
            ],
            'target': 'main',
            'context': {
                'message': _('Sent to draft successfully.'),
                'type': 'success',
                'sticky': False,
            },
        }

    def action_archive(self):
        self.ensure_one()
        self.write({
            'status': 'archived',
        })

        self._send_email_to_officers(
            'compliance_management.enhanced_due_diligence_archived_template', to_cco_only=True)

        return {
            'type': 'ir.actions.act_window',
            'name': _('Enhanced Due Diligence'),
            'res_model': 'res.partner.edd',
            'view_mode': 'list,form',
            'view_id': False,
            'views': [
                (self.env.ref('compliance_management.edd_tree_view').id, 'list'),
                (False, 'form')
            ],
            'target': 'main',
            'context': {
                'message': _('Archived successfully.'),
                'type': 'success',
                'sticky': False,
            },
        }

    def _inverse_risk_score(self):
        for record in self:
            # Convert to float first to ensure it's a number
            try:
                value = float(record.risk_score)
                # Cap the value at 25
                if value > 9:
                    record.risk_score = 9.0
                # Ensure it's not below minimum
                elif value < 0.5:
                    record.risk_score = 0.5
            except (ValueError, TypeError):
                # If conversion fails, set a default value
                record.risk_score = 1.0
    
    def action_download_pdf(self):
        self.ensure_one()
        base_url = self.env['ir.config_parameter'].sudo().get_param('web.base.url')
        return {
            'type': 'ir.actions.act_url',
            'url': f'{base_url}/compliance/pdf_report/{self.id}',
            'target': 'new',
        }

       