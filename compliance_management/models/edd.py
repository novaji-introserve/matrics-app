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
            ('submitted', 'Submitted'),
            ('completed', 'Completed'),
            ('approved', 'Approved'),
            ('rejected', 'Rejected'),
            # ('deleted', 'Deleted'),
            ('archived', 'Archived')],
        default='draft',
        tracking=True
    )
    active = fields.Boolean(default=True, tracking=True)
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
    is_co = fields.Boolean(compute='_compute_is_co', store=False, default=lambda self: self._default_is_co())
    is_editable_user= fields.Boolean(compute='_compute_is_editable_user',default=lambda self: self._default_is_editable_user(), store=False)
    is_officer_notified = fields.Boolean(string="Officer Notified", default=False)
    is_relationship_manager = fields.Boolean(compute='_compute_is_relationship_manager',default=lambda self: self._default_is_relationship_manager(), store=False)
    is_diligence_officer = fields.Boolean(default=lambda self: (self.env.user.has_group('compliance_management.group_compliance_compliance_officer') or
        self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')
    ), store=False)
    created_by_rm = fields.Boolean(compute='_compute_created_by_rm', store=False)
    company_id = fields.Many2one('res.company', string='Company', default=lambda self: self.env.company)
    reject_reason = fields.Text(string='Reject Reason', tracking =True)
    

    _sql_constraints = [
    ('unique_name', 'UNIQUE(name)', 'The EDD name must be unique.')]

    @api.constrains('name')
    def _check_unique_name(self):
        for record in self:
            if self.search_count([('name', '=', record.name), ('id', '!=', record.id)]):
                raise ValidationError("An EDD record with this name already exists.")

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
            
    @api.model
    def _default_is_co(self):
        """Default method to set is_cco based on the user group."""
        return self.env.user.has_group('compliance_management.group_compliance_compliance_officer')
  
    @api.depends_context('uid')
    def _compute_is_co(self):
        """Compute method to update is_cco based on user group when editing records."""
        for record in self:
            record.is_co = self.env.user.has_group(
                'compliance_management.group_compliance_compliance_officer')
            
    @api.model
    def _default_is_editable_user(self):
        """Allow CO/CCO to create new records"""
        user = self.env.user
        is_cco = user.has_group('compliance_management.group_compliance_chief_compliance_officer')
        is_co = user.has_group('compliance_management.group_compliance_compliance_officer')
        is_relationship_manager= user.has_group('compliance_management.group_compliance_relationship_manager')
        return is_cco or is_co or is_relationship_manager 
            
    @api.depends_context('uid')
    def _compute_is_editable_user(self):
        current_user = self.env.user
        for record in self:
            # Check groups once
            is_cco = current_user.has_group('compliance_management.group_compliance_chief_compliance_officer')
            is_co = current_user.has_group('compliance_management.group_compliance_compliance_officer')
            is_relationship_manager= current_user.has_group('compliance_management.group_compliance_relationship_manager')
            
            # Handle new vs existing records
            if not record.id:  # New record (not saved yet)
                is_creator = True
                _logger.info(f"[EDD] NEW RECORD - User: {current_user.name} (ID: {current_user.id})")
            else:  # Existing record
                is_creator = record.user_id.id == current_user.id if record.user_id else False
                _logger.info(f"[EDD] EXISTING RECORD - User: {current_user.name} (ID: {current_user.id})")
                _logger.info(f"[EDD] Record ID: {record.id}, User ID: {record.user_id}")
            
            # Calculate editability
            record.is_editable_user = (is_cco and is_creator) or (is_co and is_creator) or (is_relationship_manager and is_creator)
            
            # Final logging
            _logger.info(f"[EDD] Is CCO: {is_cco}, Is CO: {is_co}, Is Creator: {is_creator}")
            _logger.info(f"[EDD] Editable: {record.is_editable_user}")


    @api.model
    def _default_is_relationship_manager(self):
        return self.env.user.has_group('compliance_management.group_compliance_relationship_manager')
  
    @api.depends_context('uid')
    def _compute_is_relationship_manager(self):
        for record in self:
            record.is_relationship_manager = self.env.user.has_group(
                'compliance_management.group_compliance_relationship_manager')
            
    def _compute_created_by_rm(self):
        rm_group = self.env.ref("compliance_management.group_compliance_relationship_manager")
        for record in self:
            record.created_by_rm = (record.create_uid in rm_group.users)
            


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
            
    
    def _create_alert_history_record(self, template_with_context, recipient, template, notification_type, email_values=None):
        """Create alert history record for sent email"""
        try:
            # Get the rendered HTML for history
            rendered_html = template_with_context._render_template(
                template_with_context.body_html,
                template_with_context.model,
                [self.id],
                engine='qweb'
            )[self.id]
            
            # Create alert history record
            alert_history = self.env['alert.history'].sudo().create({
                "ref_id": f"{self._name},{self.id}",
                'html_body': rendered_html,
                'attachment_data': None,
                'attachment_link': None,
                'last_checked': fields.Datetime.now(),
                'risk_rating': 'low',
                'process_id': None,
                'source': self._description,
                'date_created': fields.Datetime.now(),
                'email': recipient,
                'email_cc': email_values.get('email_cc', '') if email_values else '',
                'narration': f"EDD {notification_type} sent via {template.name}",
                'name': f"EDD-{self.id} {notification_type.title()} Notification to {recipient}"
            })
            
            _logger.info(f"Alert history created with ID: {alert_history.id} for recipient: {recipient}")
            
        except Exception as history_error:
            _logger.error(f"Failed to create alert history for {recipient}: {str(history_error)}")
            # Don't raise error here - email was sent successfully, history is secondary
        

    # logic to send email      
    def _send_email_to_officers(self, template_ref, to_creator_only, officer=None):
        """
        Send email notifications for EDD workflow.
        
        Args:
            template_ref: Reference to email template
            to_creator_only: Boolean - if True, send only to creator
            officer: Target officer to receive the email (if not to_creator_only)
        """
        try:
            # Get email template
            template = self.env.ref(template_ref, raise_if_not_found=False)
            if not template:
                _logger.error(f"Email template not found: {template_ref}")
                raise ValidationError("Email template not found")

            # Get compliance officer groups
            compliance_groups = [
                self.env.ref('compliance_management.group_compliance_chief_compliance_officer'),
                self.env.ref('compliance_management.group_compliance_compliance_officer'),
                self.env.ref('compliance_management.group_compliance_relationship_manager'),
            ]
            
            # Get all users from compliance groups
            compliance_users = self.env['res.users']
            for group in compliance_groups:
                compliance_users |= group.users

            # Get creator details (user who created the EDD record)
            creator = self.user_id or self.create_uid
            creator_email = None
            creator_name = None
            
            if creator and creator in compliance_users and creator.email:
                creator_email = creator.email
                creator_name = creator.name

            # Get current user (officer performing the action)
            current_user = self.env.user
            current_user_email = current_user.email if current_user.email else None

            # Determine target officer
            if officer:
                _logger.info(f"Using provided officer: {officer.name}")
                target_officer = officer
            else:
                target_officer = self.approving_officer_id or self.responsible_id
                _logger.info(f"Using provided officer: {target_officer.name}")
            
            officer = target_officer 
            officer_email = officer.email

            if officer is None or not officer_email:
                _logger.warning("No valid officer found or officer has no email.")
                raise ValidationError("No valid officer to send email to.")
            
            # Build EDD URL
            base_url = self.env['ir.config_parameter'].sudo().get_param('web.base.url')
            edd_url = f"{base_url}/web#id={self.id}&model=res.partner.edd&view_type=tree"
            
            # Prepare email context
            email_context = {
                'officer_name': officer.name,
                'cco': creator_name,
                'edd_url': edd_url,
                'record_name': self.name or f"EDD-{self.id}",
            }

            # Handle email routing based on to_creator_only flag
            primary_email = None  # Initialize primary_email variable
            
            if to_creator_only:
                # Send to creator only, CC current user if different
                if not creator_email:
                    raise ValidationError("Creator email not found or creator is not a compliance officer.")
                
                email_values = {'email_to': creator_email}
                primary_email = creator_email
                
                # Add current user to CC if different from creator
                if current_user_email and current_user_email != creator_email:
                    email_values['email_cc'] = current_user_email
                
                recipient_info = f"creator ({creator_email})"
                cc_info = email_values.get('email_cc', 'none')
                
            else:
                # Send to target officer, CC creator and current user
                if not target_officer or not target_officer.email:
                    raise ValidationError("No valid target officer with email found.")
                
                email_values = {'email_to': target_officer.email}
                primary_email = target_officer.email
                
                # Build CC list
                cc_emails = []
                
                # Add creator to CC if exists and different from target
                if creator_email and creator_email != target_officer.email:
                    cc_emails.append(creator_email)
                
                # Add current user to CC if different from target and creator
                if (current_user_email and 
                    current_user_email != target_officer.email and 
                    current_user_email != creator_email):
                    cc_emails.append(current_user_email)
                
                # Set CC if we have any
                if cc_emails:
                    email_values['email_cc'] = ','.join(cc_emails)
                
                recipient_info = f"target officer ({target_officer.email})"
                cc_info = email_values.get('email_cc', 'none')

            # Log email details
            _logger.info(f"Sending {template.name} notification to {recipient_info}, CC: {cc_info}")
            
            try:
                template_with_context = template.with_context(**email_context)
                
                # Send the email with the context
                email_result = template_with_context.send_mail(
                    self.id,
                    force_send=True,
                    email_values=email_values
                )
                
                _logger.info(f"Email sent with ID: {email_result}")
                _logger.info(f"Email values: {email_values}")
                
                if email_result:    
                    _logger.info(f"Email sent successfully to {email_values}")
                    self._create_alert_history_record(template_with_context, primary_email, template, template.name, email_values)
                else:
                    error_msg = "Email sending returned no mail ID - send may have failed"
                    _logger.error(error_msg)
                    raise ValidationError(f"Failed to send email: {error_msg}")
            
            except Exception as send_error:
                _logger.error(f"Failed to send email: {str(send_error)}")
                raise ValidationError(f"Failed to send notification: {str(send_error)}")
            
        except ValidationError:
            # Re-raise validation errors
            raise
        except Exception as e:
            _logger.error(f"{template.name} Failed to send notification: {str(e)}")
            raise ValidationError(f"{template.name} Failed to send notification: {str(e)}")
        
    def _send_approval_request_notification(self):
        # Get email template for approval request
        template = self.env.ref("compliance_management.enhanced_due_diligence_approval_reqired_template", raise_if_not_found=False)
        if not template:
            raise ValidationError("Missing email template: enhanced_due_diligence_approval_reqired_template")

        # Get compliance officer groups
        cco_group = self.env.ref("compliance_management.group_compliance_chief_compliance_officer")
        co_group = self.env.ref("compliance_management.group_compliance_compliance_officer")

        # Get all officers with valid email addresses
        officer_users = (cco_group.users | co_group.users).filtered(lambda u: u.email and u.email.strip())
        
        if not officer_users:
            raise ValidationError("No CO/CCO users with valid email addresses found.")

        # Send approval request emails
        self._send_bulk_emails(template, officer_users, "approval request")
    
    def _send_bulk_emails(self, template, recipients, notification_type):
        """Send emails to multiple recipients with proper error handling"""
        base_url = self.env['ir.config_parameter'].sudo().get_param('web.base.url')
        edd_url = f"{base_url}/web#id={self.id}&model=res.partner.edd&view_type=form"

        # Track email sending results
        successful_sends = []
        failed_sends = []

        # Send email to each recipient
        for recipient in recipients:
            try:
                # Store email address for consistent reference
                recipient_email = recipient.email
                
                # Prepare email context
                email_context = {
                    'edd_url': edd_url,
                    'record_name': self.name,
                    'officer_name': recipient.name,
                }

                # Prepare email values (can include CC if needed)
                email_values = {'email_to': recipient_email}
                # If you need CC, you can add it here:
                # email_values['email_cc'] = 'cc@example.com'

                # Send email with context
                template_with_context = template.with_context(**email_context)
                email_result = template_with_context.send_mail(
                    self.id,
                    force_send=True,
                    email_values=email_values
                )

                if email_result:
                    _logger.info(f"Email sent successfully to {recipient_email} with ID: {email_result}")
                    successful_sends.append(recipient_email)
                    # Pass email_values to get CC information
                    self._create_alert_history_record(template_with_context, recipient_email, template, template.name, email_values)
                    
                else:
                    error_msg = f"Email sending to {recipient_email} returned no mail ID"
                    _logger.error(error_msg)
                    failed_sends.append(recipient_email)

            except Exception as send_error:
                # Use recipient_email if available, otherwise fallback to recipient
                recipient_ref = recipient_email if 'recipient_email' in locals() else str(recipient)
                error_msg = f"Failed to send email to {recipient_ref}: {str(send_error)}"
                _logger.error(error_msg)
                failed_sends.append(recipient_ref)

        # Handle results
        if successful_sends:
            _logger.info(f"EDD {notification_type} notifications sent successfully to: {', '.join(successful_sends)}")
            
        if failed_sends:
            error_msg = f"Failed to send EDD {notification_type} notifications to: {', '.join(failed_sends)}"
            _logger.error(error_msg)
            
            if not successful_sends:  # All emails failed
                raise ValidationError(f"Failed to send any {notification_type} notifications: {error_msg}")
            else:  # Some succeeded, some failed
                _logger.warning(f"Partial failure in {notification_type} email sending: {error_msg}")

    def action_notify_officer(self):
        self.write({
            'is_officer_notified':True
        })
        _logger.info(f"Creating EDD record with values: {self}")
        self._send_email_to_officers(
            'compliance_management.enhanced_due_diligence_assessment_template', to_creator_only=False, officer= self.responsible_id)
        
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
                'message': _('Submitted to officer successfully.'),
                'type': 'success',
                'sticky': False,
            },
        }

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
        # Perform attestation check before allowing submission
        if (self.is_current_user_responsible and
            self.status == 'draft' and 
            not self.attestation_checked):
            raise ValidationError("Attestation must be checked before submission.")

        self.write({
            'status': 'submitted',
            'date_reviewed': fields.Date.today()
        })

        self._send_email_to_officers(
            'compliance_management.enhanced_due_diligence_review_template', to_creator_only=False)

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
    
    def action_mark_review_completed(self):
        self.ensure_one()

        if (self.is_current_user_approving_officer and
            self.status == 'submitted' and
            not self.approving_officer_signature):
            raise ValidationError("Signature must be uploaded before completing review.")

        # Update status
        self.write({'status': 'completed'})

        # Identify the creator of the record
        creator = self.create_uid

        is_creator_relationship_manager = creator.has_group('compliance_management.group_compliance_relationship_manager')
        is_creator_compliance_officer = (
            creator.has_group('compliance_management.group_compliance_chief_compliance_officer') or
            creator.has_group('compliance_management.group_compliance_compliance_officer')
        )

        # Creator is Risk Manager
        if is_creator_relationship_manager:
            self._send_approval_request_notification()
            self._send_email_to_officers("compliance_management.enhanced_due_diligence_completed_template", to_creator_only=False)

        # Creator is Compliance Officer or CCO
        elif is_creator_compliance_officer:
            self._send_email_to_officers("compliance_management.enhanced_due_diligence_completed_template", to_creator_only=False)


        
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
                'message': _('Review Completed.'),
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
            "compliance_management.enhanced_due_diligence_approved_template", to_creator_only=True)
            

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
            'status': 'draft',
            'approved_by': "",
            'date_approved': False,
        })

        self._send_email_to_officers(
            'compliance_management.enhanced_due_diligence_cancellation_template', to_creator_only=True)

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
            'compliance_management.enhanced_due_diligence_sent_back_template', to_creator_only=True)

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

        self._send_email_to_officers(
            'compliance_management.enhanced_due_diligence_archived_template', to_creator_only=True)
        
        result = super().action_archive()

        self.write({
            'status': 'archived',
        })

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
    
    def action_reject(self, reject_reason=None):
        self.ensure_one()
        self.write({
            'status': 'rejected',
            'approved_by': "",
            'date_approved': False,
            'approving_officer_signature': False,
            'reject_reason': reject_reason or False,
        })
        self._send_email_to_officers(
            'compliance_management.enhanced_due_diligence_rejected_template', to_creator_only=True)

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
                'message': _('EDD rejected'),
                'type': 'success',
                'sticky': False,
            },
        }
    def open_reject_wizard(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': _('Reject EDD'),
            'res_model': 'res.partner.edd.reject.wizard',
            'view_mode': 'form',
            'view_id': self.env.ref('compliance_management.view_res_partner_edd_reject_wizard_form').id,
            'target': 'new',
            'context': {'default_edd_id': self.id},
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


class EDDRrejectWizard(models.TransientModel):
    _name = 'res.partner.edd.reject.wizard'
    _description = 'EDD Reject Reason Wizard'

    edd_id = fields.Many2one('res.partner.edd', string='EDD', required=True)
    reject_reason = fields.Text(string='Reject Reason', required=True)

    def action_confirm_reject(self):
        self.edd_id.action_reject(self.reject_reason)
        return {'type': 'ir.actions.act_window_close'}
    
    