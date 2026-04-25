import os
import logging
import base64
import binascii
from datetime import datetime
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
from odoo.exceptions import ValidationError, UserError, AccessError
from odoo.http import request

_logger = logging.getLogger(__name__)


class CustomerEDD(models.Model):
    _name = 'res.partner.edd'
    _description = 'Enhanced Due Diligence'
    _inherit = ['mail.thread', 'mail.activity.mixin','conditional.method.mixin']
    _sql_constraints = [
        ('unique_name', 'UNIQUE(name)', 'The EDD name must be unique.')]

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
                                    ('corporate', 'Corporate')], string="EDD Type", required=True, default='individual', tracking=True)

    brief_customer_profile = fields.Text(
        string="Please give a brief profile of the subject customer", tracking=True)
    nature_of_business = fields.Text(
        string="What is the specific nature of the customers business/occupation/employment?", tracking=True)
    employment_position = fields.Text(
        string="Employment Position", tracking=True)
    employer_name = fields.Text(
        string="Name of employer/company", tracking=True)
    expected_source_of_funds = fields.Text(
        string="What is the expected source of funds into the customers account?", tracking=True)
    source_of_funds_document = fields.Many2many(
        'ir.attachment',
        'res_partner_source_of_funds_document_rel',
        'partner_id',
        'attachment_id',
        string="Kindly provide documentary evidence if available",
        help="Add source of wealth document(s) for customer EDD"
    )
    source_of_wealth = fields.Text(
        string="What is the source of wealth of the customer (e.g., inheritance, business revenue, investment income)?")
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
        help="Kindly list the countries as applicable"

    )
    is_customer_pep = fields.Boolean(
        string="Is the customer a Politically Exposed Person (PEP) or associated to a PEP?", tracking=True)
    relationship_with_pep = fields.Text(
        string="If “Yes”, state Designation/Position/relationship with PEP)", tracking=True)
    cross_border_transaction = fields.Boolean(
        string="Is the customer expected to engage in transactions involving cross-border fund transfers or transactions?", tracking=True)
    cross_border_jurisdictions = fields.Many2many(
        'res.country',
        'res_partner_crossborder_country_rel',
        'partner_id',
        'country_id',
        string="If “Yes,” specify the jurisdictions involved."
    )
    expected_monthly_income = fields.Float(
        string="What is the expected monthly income/inflow of the customer? (State in Naira - NGN)", tracking=True)
    estimated_net_worth = fields.Text(
        string="What is the estimated total net worth of the customer? (State in Naira or USD)", tracking=True)
    inflow_purpose = fields.Text(
        string="What would be the purpose of the inflows routed into the customer’s account?", tracking=True)
    inflow_document = fields.Many2many(
        'ir.attachment',
        'res_partner_inflow_document_rel',
        'partner_id',
        'attachment_id',
        string="(Provide supporting documents if applicable)",
        help="Add inflow document(s) for customer EDD"
    )
    outflow_purpose = fields.Text(
        string="What would be the expected purpose of the outflows from the account?", tracking=True)
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
    negative_media_details = fields.Text(
        string="provide details", tracking=True)
    negative_media_document = fields.Many2many(
        'ir.attachment',
        string="Attach evidence",
        help="Add negative media document(s) for customer EDD"
    )
    other_comments = fields.Text(string="Any Other Comments", tracking=True)
    attestation_checked = fields.Boolean(
        string="I hereby attest that the information provided above and in the enclosed documents is accurate to the best of my knowledge. I understand that providing false or incomplete information may result in disciplinary action.", tracking=True)
    responsible_officer_signature = fields.Binary(
        string="Officer In Charge Signature", tracking=True)
    responsible_signature_filename = fields.Char(tracking=True)
    approving_officer_signature = fields.Binary(
        string="Approving Officer Signature", tracking=True)
    approving_signature_filename = fields.Char(tracking=True)
    dual_citizenship_info = fields.Text(
        string="Do any signatories/directors/shareholders hold dual citizenship or residence in other jurisdictions?", tracking=True)
    citizenship_country_ids = fields.Many2many(
        'res.country',
        'res_partner_country_rel',
        'partner_id',
        'country_id',
        string="Citizenship Countries",
        help="Kindly list the countries as applicable"

    )
    customer_address = fields.Char(
        string='Address', related='customer_id.street', store=True, readonly=False, tracking=True)
    company_registration_number = fields.Char(
        string='Company Registration Number (RC/BN)', help='Company Registration Number or Business Number', tracking=True)
    signatories_details = fields.Text(
        string='Signatories Details (Names)', help='List the names of authorized signatories', tracking=True)
    beneficial_owner_details = fields.Text(
        string='Beneficial Owner Details', help='List the names of the Directors/Shareholders/Person of Significant Control')
    structured_ubo_summary = fields.Text(
        string='Structured UBO Summary',
        related='customer_id.ubo_summary',
        readonly=True,
    )
    structured_ubo_count = fields.Integer(
        string='Structured UBO Count',
        related='customer_id.ubo_count',
        readonly=True,
    )
    high_risk_industries = fields.Text(
        string="Does the customer have any direct or indirect link to high-risk industries (e.g. Crypto, Financial Services, DNFBPs)?", tracking=True)
    pep_association = fields.Boolean(
        string="Is any Director/Shareholder/Signatory a Politically Exposed Person (PEP) or associated with a PEP?", tracking=True)
    pep_relationship_details = fields.Text(
        string="If Yes, state Designation/Position/relationship with PEP", tracking=True)
    third_party_involvement = fields.Boolean(
        string="Are there any third-party, proxies, nominees or legal reps managing the entity/account?", tracking=True)
    third_party_details = fields.Text(
        string="If Yes, provide details & documentation", tracking=True)
    third_party_document = fields.Many2many(
        'ir.attachment',
        'res_partner_third_party_document_rel',
        'partner_id',
        'attachment_id',
        string="Documentation",
        help="Add third party document(s) for customer EDD"
    )
    kycc_info = fields.Text(
        string="What types/nature of clientele/customers does this business provide services/goods to?")

    is_current_user_responsible = fields.Boolean(
        compute='_compute_is_current_user_responsible')
    is_current_user_approver = fields.Boolean(
        compute='_compute_is_current_user_approver', store=False)
    is_current_user_approving_officer = fields.Boolean(
        compute='_compute_is_current_user_approving_officer')
    is_cco = fields.Boolean(compute='_compute_is_cco', store=False,
                            default=lambda self: self._default_is_cco())
    is_co = fields.Boolean(compute='_compute_is_co', store=False,
                           default=lambda self: self._default_is_co())
    is_editable_user = fields.Boolean(compute='_compute_is_editable_user',
                                      default=lambda self: self._default_is_editable_user(), store=False)
    is_officer_notified = fields.Boolean(
        string="Officer Notified", default=False)

    def action_load_customer_ubo_details(self):
        for rec in self:
            if not rec.customer_id:
                raise UserError(_('A customer is required before loading UBO details.'))
            rec.beneficial_owner_details = rec.customer_id.ubo_summary
    is_relationship_manager = fields.Boolean(
        compute='_compute_is_relationship_manager', default=lambda self: self._default_is_relationship_manager(), store=False)
    is_diligence_officer = fields.Boolean(default=lambda self: (self.env.user.has_group('compliance_management.group_compliance_compliance_officer') or
                                                                self.env.user.has_group(
                                                                    'compliance_management.group_compliance_chief_compliance_officer')
                                                                ), store=False)
    created_by_rm = fields.Boolean(
        compute='_compute_created_by_rm', store=False)
    company_id = fields.Many2one(
        'res.company', string='Company', default=lambda self: self.env.company)
    reject_reason = fields.Text(string='Reject Reason', tracking=True)
    approving_officer_name = fields.Char(
        string="Approving Officer Name", compute='_compute_officer_names', store=True, tracking=True)
    responsible_officer_name = fields.Char(
        string="Responsible Officer Name", compute='_compute_officer_names', store=True, tracking=True)
    is_current_user_creator = fields.Boolean(
        compute='_compute_is_current_user_creator')

    show_approval_button = fields.Boolean(
        string='Show Approval Button',
        compute='_compute_button_visibility',
        store=False
    )

    show_completion_button = fields.Boolean(
        string='Show Completion Button',
        compute='_compute_button_visibility',
        store=False
    )

    show_reject_button = fields.Boolean(
        string='Show Reject Button',
        compute='_compute_button_visibility',
        store=False
    )

    show_cancel_button = fields.Boolean(
        string='Show Cancel Button',
        compute='_compute_button_visibility',
        store=False
    )

    show_send_back_button = fields.Boolean(
        string='Show Send Back Button',
        compute='_compute_button_visibility',
        store=False
    )
    show_archive_button = fields.Boolean(
        string='Show Archive Button',
        compute='_compute_button_visibility',
        store=False
    )

    show_notify_button = fields.Boolean(
        string='Show Notify Button',
        compute='_compute_button_visibility',
        store=False
    )
    show_submit_for_review_button = fields.Boolean(
        string='Show Submit For Review Button',
        compute='_compute_button_visibility',
        store=False
    )

    @api.depends('status', 'create_uid', 'approving_officer_id', 'responsible_id', 'is_officer_notified')
    def _compute_button_visibility(self):
        for record in self:
            current_user = self.env.user

            # Get user groups
            rm_group = self.env.ref(
                'compliance_management.group_compliance_relationship_manager', raise_if_not_found=False)
            dd_group = self.env.ref(
                'compliance_management.group_compliance_compliance_officer', raise_if_not_found=False)

            user_in_rm = rm_group and rm_group in current_user.groups_id
            user_in_dd = dd_group and dd_group in current_user.groups_id
            creator_in_rm = rm_group and rm_group in record.create_uid.groups_id if record.create_uid else False
            user_is_authorized = (
                current_user.id == record.create_uid.id or  # Creator
                current_user.id == record.approving_officer_id.id or  # Approving Officer
                current_user.id == record.responsible_id.id  # Responsible Officer
            )
            # APPROVAL BUTTON CONDITIONS
            # Condition 1: status is complete AND logged in user is approving officer (has id)
            condition1 = (record.status == 'completed' and
                          record.approving_officer_id and
                          current_user.id == record.create_uid.id)

            # Condition 2: status is completed AND creator is RM group AND login belongs to DD group
            condition2 = (record.status == 'completed' and
                          creator_in_rm and
                          user_in_dd)

            record.show_approval_button = condition1 or condition2

            # COMPLETION BUTTON CONDITIONS
            # current_user = approving_officer id AND status is submitted
            record.show_completion_button = (record.create_uid and
                                             record.approving_officer_id and
                                             current_user.id == record.approving_officer_id.id and
                                             record.status == 'submitted')

            # REJECT BUTTON CONDITIONS
            # Condition 1: status is submitted AND current_user = approving_officer
            reject_condition1 = (record.status == 'submitted' and
                                 record.create_uid and
                                 record.approving_officer_id and
                                 current_user.id == record.approving_officer_id.id)

            # Condition 2: status is completed AND create_uid belongs to RM group AND logged in user is in DD group
            reject_condition2 = (record.status == 'completed' and
                                 creator_in_rm and
                                 user_in_dd)

            reject_condition3 = (record.status == 'completed'
                                 and current_user.id == record.create_uid.id)

            record.show_reject_button = reject_condition1 or reject_condition2 or reject_condition3

            # CANCEL BUTTON CONDITIONS
            # Show to responsible officer AND record does not have submitted status
            record.show_cancel_button = (record.responsible_id and
                                         current_user.id == record.responsible_id.id and
                                         record.status == 'submitted')

            # Helper function to check if user is authorized (creator, approving officer, or responsible officer)

            # ARCHIVE BUTTON CONDITION
            # Show if status is approved AND logged-in user is either creator, approving officer, or responsible officer
            record.show_archive_button = (
                record.status == 'approved' and
                user_is_authorized
            )

            # SEND BACK BUTTON CONDITION
            record.show_send_back_button = (
                record.status == 'rejected' and
                user_is_authorized
            )

            # SHOW NOTIFY BUTTON CONDITION
            record.show_notify_button = (
                not record.is_officer_notified and
                user_is_authorized
            )

            # SUBMIT FOR REVIEW BUTTON CONDITION
            record.show_submit_for_review_button = (
                record.is_officer_notified and record.status == 'draft' and
                current_user.id == record.responsible_id.id
            )

    @api.depends('create_uid')
    def _compute_is_current_user_creator(self):
        user_id = self.env.user.id
        for record in self:
            record.is_current_user_creator = record.create_uid.id == user_id if record.create_uid else False

    @api.depends('approving_officer_id', 'responsible_id')
    def _compute_officer_names(self):
        for record in self:
            record.approving_officer_name = record.approving_officer_id.name if record.approving_officer_id else False
            record.responsible_officer_name = record.responsible_id.name if record.responsible_id else False

    @api.constrains('name')
    def _check_unique_name(self):
        for record in self:
            if self.search_count([('name', '=', record.name), ('id', '!=', record.id)]):
                raise ValidationError(
                    "An EDD record with this name already exists.")

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
        is_cco = user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')
        is_co = user.has_group(
            'compliance_management.group_compliance_compliance_officer')
        is_relationship_manager = user.has_group(
            'compliance_management.group_compliance_relationship_manager')
        return is_cco or is_co or is_relationship_manager

    @api.depends_context('uid')
    def _compute_is_editable_user(self):
        current_user = self.env.user
        for record in self:
            # Check groups once
            is_cco = current_user.has_group(
                'compliance_management.group_compliance_chief_compliance_officer')
            is_co = current_user.has_group(
                'compliance_management.group_compliance_compliance_officer')
            is_relationship_manager = current_user.has_group(
                'compliance_management.group_compliance_relationship_manager')

            # Handle new vs existing records
            if not record.id:  # New record (not saved yet)
                is_creator = True
                _logger.info(
                    f"[EDD] NEW RECORD - User: {current_user.name} (ID: {current_user.id})")
            else:  # Existing record
                is_creator = record.user_id.id == current_user.id if record.user_id else False
                _logger.info(
                    f"[EDD] EXISTING RECORD - User: {current_user.name} (ID: {current_user.id})")
                _logger.info(
                    f"[EDD] Record ID: {record.id}, User ID: {record.user_id}")

            # Calculate editability
            record.is_editable_user = (is_cco and is_creator) or (
                is_co and is_creator) or (is_relationship_manager and is_creator)

            # Final logging
            _logger.info(
                f"[EDD] Is CCO: {is_cco}, Is CO: {is_co}, Is Creator: {is_creator}")
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
        rm_group = self.env.ref(
            "compliance_management.group_compliance_relationship_manager")
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
                record.approving_officer_id.id == self.env.user.id if record.approving_officer_id else False
            )

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
                "ref_id": self._name,
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

            _logger.info(
                f"Alert history created with ID: {alert_history.id} for recipient: {recipient}")

        except Exception as history_error:
            _logger.error(
                f"Failed to create alert history for {recipient}: {str(history_error)}")
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
                self.env.ref(
                    'compliance_management.group_compliance_chief_compliance_officer'),
                self.env.ref(
                    'compliance_management.group_compliance_compliance_officer'),
                self.env.ref(
                    'compliance_management.group_compliance_relationship_manager'),
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
                _logger.warning(
                    "No valid officer found or officer has no email.")
                raise ValidationError("No valid officer to send email to.")

            # Build EDD URL
            base_url = self.env['ir.config_parameter'].sudo(
            ).get_param('web.base.url')
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
                    raise ValidationError(
                        "Creator email not found or creator is not a compliance officer.")

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
                    raise ValidationError(
                        "No valid target officer with email found.")

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
            _logger.info(
                f"Sending {template.name} notification to {recipient_info}, CC: {cc_info}")

            try:
                
                template_with_context = template.sudo().with_context(**email_context)

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
                    self._create_alert_history_record(
                        template_with_context, primary_email, template, template.name, email_values)
                else:
                    error_msg = "Email sending returned no mail ID - send may have failed"
                    _logger.error(error_msg)
                    raise ValidationError(f"Failed to send email: {error_msg}")

            except Exception as send_error:
                _logger.error(f"Failed to send email: {str(send_error)}")
                raise ValidationError(
                    f"Failed to send notification: {str(send_error)}")

        except ValidationError:
            # Re-raise validation errors
            raise
        except Exception as e:
            _logger.error(
                f"{template.name} Failed to send notification: {str(e)}")
            raise ValidationError(
                f"{template.name} Failed to send notification: {str(e)}")

    def _send_approval_request_notification(self):
        # Get email template for approval request
        template = self.env.ref(
            "compliance_management.enhanced_due_diligence_approval_reqired_template", raise_if_not_found=False)
        if not template:
            raise ValidationError(
                "Missing email template: enhanced_due_diligence_approval_reqired_template")

        # Get compliance officer groups
        cco_group = self.env.ref(
            "compliance_management.group_compliance_chief_compliance_officer")
        co_group = self.env.ref(
            "compliance_management.group_compliance_compliance_officer")

        # Get all officers with valid email addresses
        officer_users = (cco_group.users | co_group.users).filtered(
            lambda u: u.email and u.email.strip())

        if not officer_users:
            raise ValidationError(
                "No CO/CCO users with valid email addresses found.")

        # Send approval request emails
        self._send_bulk_emails(template, officer_users, "approval request")

    def _send_bulk_emails(self, template, recipients, notification_type):
        """Send emails to multiple recipients with proper error handling"""
        base_url = self.env['ir.config_parameter'].sudo(
        ).get_param('web.base.url')
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
                    _logger.info(
                        f"Email sent successfully to {recipient_email} with ID: {email_result}")
                    successful_sends.append(recipient_email)
                    # Pass email_values to get CC information
                    self._create_alert_history_record(
                        template_with_context, recipient_email, template, template.name, email_values)

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
            _logger.info(
                f"EDD {notification_type} notifications sent successfully to: {', '.join(successful_sends)}")

        if failed_sends:
            error_msg = f"Failed to send EDD {notification_type} notifications to: {', '.join(failed_sends)}"
            _logger.error(error_msg)

            if not successful_sends:  # All emails failed
                raise ValidationError(
                    f"Failed to send any {notification_type} notifications: {error_msg}")
            else:  # Some succeeded, some failed
                _logger.warning(
                    f"Partial failure in {notification_type} email sending: {error_msg}")

    def action_notify_officer(self):
        self.write({
            'is_officer_notified': True
        })
        _logger.info(f"Creating EDD record with values: {self}")
        self._send_email_to_officers(
            'compliance_management.enhanced_due_diligence_assessment_template', to_creator_only=False, officer=self.responsible_id)

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

   

    def action_submit_for_review(self):

        self.ensure_one()

        # Perform attestation check before allowing submission
        if (self.is_current_user_responsible and
            self.status == 'draft' and
                not self.attestation_checked):
            raise ValidationError(
                "Attestation must be checked before submission.")

        self.call_session_method('validate_responsible_user')

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
            raise ValidationError(
                "Signature must be uploaded before completing review.")

        self.call_session_method('validate_officer_can_complete')

        # Update status
        self.write({'status': 'completed'})

        # Identify the creator of the record
        creator = self.create_uid
        rm_group = 'compliance_management.group_compliance_relationship_manager'

        is_creator_relationship_manager = creator.has_group(rm_group)
        is_creator_compliance_officer = (
            creator.has_group('compliance_management.group_compliance_chief_compliance_officer') or
            creator.has_group(
                'compliance_management.group_compliance_compliance_officer')
        )

        # Creator is Risk Manager
        if is_creator_relationship_manager:
            self._send_approval_request_notification()
            self._send_email_to_officers(
                "compliance_management.enhanced_due_diligence_completed_template", to_creator_only=False)

        # Creator is Compliance Officer or CCO
        elif is_creator_compliance_officer:
            self._send_email_to_officers(
                "compliance_management.enhanced_due_diligence_completed_template", to_creator_only=False)

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

        self.call_session_method('validate_user_can_do_final_approval')

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

        self.call_session_method('validate_approving_officer_or_creator')

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

        self.call_session_method('validate_approving_officer_or_creator')

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

        self.call_session_method('validate_user_can_reject')

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
        base_url = self.env['ir.config_parameter'].sudo(
        ).get_param('web.base.url')
        return {
            'type': 'ir.actions.act_url',
            'url': f'{base_url}/compliance/pdf_report/{self.id}',
            'target': 'new',
        }

    def validate_user_can_do_final_approval(self):
        """
        Validates that the session user is NOT in a specified group AND IS the creator of the record.
        """
        try:
            # Get current session user
            user_session = self.env['user.session']
            validation_result = user_session.validate_current_session_secure()
            session_user_id = validation_result['user_id']

            current_user = self.env.user
            user_is_authorized = (
                session_user_id == self.create_uid.id or  # Creator
                session_user_id == self.approving_officer_id.id or  # Approving Officer
                session_user_id == self.responsible_id.id  # Responsible Officer
            )

            dd_group = 'compliance_management.group_compliance_branch_compliance_officer'

            if not validation_result['valid']:
                _logger.info(
                    f"You do not have permission to perform this action.{validation_result}")
                raise UserError(
                    "You do not have permission to perform this action."

                )

            # Check if user is in the specified group
            user = self.env['res.users'].browse(session_user_id)
            if not user.has_group(dd_group):
                raise UserError(
                    f"Unauthorized Group User. you cannot perform this action."
                )

            # Check if user is the creator of the record
            if not user_is_authorized:
                raise UserError(
                    "Unauthorized. you cannot perform this action."
                )

            return True

        except UserError:
            raise
        except Exception as e:
            _logger.error(
                "Error in validate_user_can_do_final_approval: %s", e)
            raise UserError(
                "An error occurred while validating permissions."
            )

    def validate_user_can_reject(self):
        """
        Validates that the session user is NOT in a specified group AND IS the creator of the record.
        """
        try:
            # Get current session user
            user_session = self.env['user.session']
            validation_result = user_session.validate_current_session_secure()
            session_user_id = validation_result['user_id']
            current_user = self.env.user
            user_is_authorized = (
                session_user_id == self.create_uid.id or  # Creator
                session_user_id == self.approving_officer_id.id  # Approving Officer
            )
            rm_group = 'compliance_management.group_compliance_relationship_manager'
            dd_group = 'compliance_management.group_compliance_branch_compliance_officer'

            # validation_result = user_session.validate_current_session_in_table()
            if not validation_result['valid']:
                _logger.info(
                    f"You do not have permission to perform this action.{validation_result}")
                raise UserError(
                    "You do not have permission to perform this action."

                )

            # Check if user is in the specified group
            user = self.env['res.users'].browse(session_user_id)
            if not user.has_group(rm_group) or not user.has_group(dd_group):
                raise UserError(
                    f"Unauthorized Group User. you cannot perform this action."
                )

            # Check if user is the creator of the record
            if not user_is_authorized:
                raise UserError(
                    "Unauthorized. you cannot perform this action."
                )

            return True

        except UserError:
            raise
        except Exception as e:
            _logger.error(
                "Error in validate_user_can_do_final_approval: %s", e)
            raise UserError(
                "An error occurred while validating permissions."
            )

    def validate_responsible_user(self):
        """
        Validates that the session user is equal to the responsible_user of the record.
        
        """
        try:
            # Get current session user
            user_session = self.env['user.session']
            validation_result = user_session.validate_current_session_secure()
            # validation_result = user_session.validate_current_session_in_table()
            if not validation_result['valid']:
                _logger.info(
                    f"You do not have permission to perform this action.{validation_result}")
                raise UserError(
                    "You do not have permission to perform this action."

                )

            session_user_id = validation_result['user_id']

            # Check if user is the responsible user
            if not hasattr(self, 'responsible_id') or not self.responsible_id:
                raise UserError(
                    "No responsible user is assigned to this record."
                )

            if self.responsible_id.id != session_user_id:
                raise UserError(
                    "Only the responsible user can perform this action."
                )

            return True

        except UserError:
            raise
        except Exception as e:
            _logger.error("Error in validate_responsible_user: %s", e)
            raise UserError(
                "An error occurred while validating responsible user permissions."
            )

    def validate_officer_can_complete(self):
        """
        Validates that the session user is equal to the approving officer of the record.
        """
        try:
            # Get current session user
            user_session = self.env['user.session']
            validation_result = user_session.validate_current_session_secure()
            # validation_result = user_session.validate_current_session_in_table()
            if not validation_result['valid']:
                _logger.info(
                    f"You do not have permission to perform this action.{validation_result}")
                raise UserError(
                    "You do not have permission to perform this action."

                )

            session_user_id = validation_result['user_id']

            # Check if user is the approving officer
            if not hasattr(self, 'approving_officer_id') or not self.approving_officer_id:
                raise UserError(
                    "No approving officer is assigned to this record."
                )

            if self.approving_officer_id.id != session_user_id:
                raise UserError(
                    "Only the approving officer can perform this action."
                )

            return True

        except UserError:
            raise
        except Exception as e:
            _logger.error("Error in validate_officer_can_complete: %s", e)
            raise UserError(
                "An error occurred while validating approving officer permissions."
            )

    def validate_approving_officer_or_creator(self):
        """
        Validates that the session user is either the approving officer OR the creator of the record.
        
        """
        try:
            # Get current session user
            user_session = self.env['user.session']
            validation_result = user_session.validate_current_session_secure()
            # validation_result = user_session.validate_current_session_in_table()
            if not validation_result['valid']:
                _logger.info(
                    f"You do not have permission to perform this action.{validation_result}")
                raise UserError(
                    "You do not have permission to perform this action."

                )
            session_user_id = validation_result['user_id']

            # Check if user is the creator
            is_creator = self.create_uid.id == session_user_id

            # Check if user is the approving officer
            is_approving_officer = False
            if hasattr(self, 'approving_officer_id') and self.approving_officer_id:
                is_approving_officer = self.approving_officer_id.id == session_user_id

            # User must be either creator or approving officer
            if not (is_creator or is_approving_officer):
                raise UserError(
                    "Only the creator or the approving officer can perform this action."
                )

            return True

        except UserError:
            raise
        except Exception as e:
            _logger.error(
                "Error in validate_approving_officer_or_creator: %s", e)
            raise UserError(
                "An error occurred while validating permissions."
            )

    def _validate_reject_permissions(self):
        """
        Comprehensive server-side validation for reject button access
        """
        current_user = self.env.user

        # Check if user has any of the required groups
        required_groups = [
            'compliance_management.group_compliance_branch_compliance_officer',
            'compliance_management.group_compliance_relationship_manager',
            'compliance_management.group_compliance_compliance_officer'
        ]

        has_required_group = any(
            current_user.has_group(group) for group in required_groups
        )

        if not has_required_group:
            raise AccessError(
                _("You don't have permission to reject this record."))

        # Validation for Branch Compliance Officer and Relationship Manager
        if (current_user.has_group('compliance_management.group_compliance_branch_compliance_officer') or
                current_user.has_group('compliance_management.group_compliance_relationship_manager')):

            if self.status != 'submitted':
                raise ValidationError(
                    _("Record can only be rejected when status is 'Submitted'."))

            if not self.is_current_user_approving_officer:
                raise ValidationError(
                    _("Only the assigned approving officer can reject this record."))

        # Validation for Compliance Officer (first button - general completed status)
        elif (current_user.has_group('compliance_management.group_compliance_compliance_officer') and
              not self.created_by_rm):  # This distinguishes from the third button

            if self.status != 'completed':
                raise ValidationError(
                    _("Record can only be rejected when status is 'Completed'."))

            if not self.is_editable_user:
                raise ValidationError(
                    _("You don't have edit permissions for this record."))

        # Validation for Compliance Officer (third button - RM created records)
        elif (current_user.has_group('compliance_management.group_compliance_compliance_officer') and
              self.created_by_rm):

            if self.status != 'completed':
                raise ValidationError(
                    _("Record can only be rejected when status is 'Completed'."))

            if not self.created_by_rm:
                raise ValidationError(
                    _("This action is only available for records created by Relationship Managers."))

            if not self.is_diligence_officer:
                raise ValidationError(
                    _("Only diligence officers can reject RM-created records."))

        else:
            raise AccessError(
                _("You don't have the necessary permissions to perform this action."))

    def _get_allowed_file_types(self):
        """Define allowed MIME types and extensions with mapping"""
        return {
            'mimetypes': [
                # Images
                'image/jpeg',
                'image/png',
                'image/webp',
                # PDF
                'application/pdf',
                # Word Documents
                'application/msword',
                'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                # Excel Files
                'application/vnd.ms-excel',
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            ],
            'extensions': [
                # Images
                '.jpg', '.jpeg', '.png', '.webp',
                # PDF
                '.pdf',
                # Word
                '.doc', '.docx',
                # Excel
                '.xls', '.xlsx'
            ],
            # Mapping to ensure MIME type and extension consistency
            'mime_extension_map': {
                'image/jpeg': ['.jpg', '.jpeg'],
                'image/png': ['.png'],
                'image/webp': ['.webp'],
                'application/pdf': ['.pdf'],
                'application/msword': ['.doc'],
                'application/vnd.openxmlformats-officedocument.wordprocessingml.document': ['.docx'],
                'application/vnd.ms-excel': ['.xls'],
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
            }
        }


    def _validate_document_field(self, field_name, attachments):
        """Validate a single document field"""
        if not attachments:
            return

        allowed_types = self._get_allowed_file_types()
        invalid_files = []

        for attachment in attachments:
            is_valid = False

            # Extract file extension from filename
            file_extension = None
            if attachment.name and '.' in attachment.name:
                file_extension = '.' + attachment.name.lower().split('.')[-1]

            # Check if both MIME type and extension are allowed and match
            if (attachment.mimetype in allowed_types['mimetypes'] and
                    file_extension in allowed_types['extensions']):

                # Verify MIME type and extension consistency
                mime_to_ext = allowed_types['mime_extension_map']
                expected_extensions = mime_to_ext.get(attachment.mimetype, [])

                if file_extension in expected_extensions:
                    is_valid = True

            if not is_valid:
                invalid_files.append(attachment.name or 'Unknown file')

        if invalid_files:
            field_label = self._fields[field_name].string
            raise ValidationError(_(
                'Invalid file format in "%s". '
                'The following files are not allowed: %s\n\n'
                'Allowed formats: Images (JPG, PNG, WEBP), '
                'PDF, Word (DOC, DOCX), Excel (XLS, XLSX). '
                'Both file extension and content type must match.'
            ) % (field_label, ', '.join(invalid_files)))

    @api.constrains(
        'source_of_wealth_document',
        'inflow_document',
        'outflow_document',
        'third_party_document',
        'negative_media_document',
        'supporting_document',
        'source_of_funds_document'
    )
    def _check_document_file_types(self):
        """Validate all document fields for allowed file types"""
        document_fields = [
            'source_of_wealth_document',
            'inflow_document',
            'outflow_document',
            'third_party_document',
            'negative_media_document',
            'supporting_document',
            'source_of_funds_document'
        ]

        for record in self:
            for field_name in document_fields:
                attachments = getattr(record, field_name)
                self._validate_document_field(field_name, attachments)

    @api.onchange('source_of_wealth_document')
    def _onchange_source_of_wealth_document(self):
        self._onchange_document_validation('source_of_wealth_document')

    @api.onchange('inflow_document')
    def _onchange_inflow_document(self):
        self._onchange_document_validation('inflow_document')

    @api.onchange('outflow_document')
    def _onchange_outflow_document(self):
        self._onchange_document_validation('outflow_document')

    @api.onchange('third_party_document')
    def _onchange_third_party_document(self):
        self._onchange_document_validation('third_party_document')

    @api.onchange('source_of_funds_document')
    def _onchange_source_of_funds_document(self):
        self._onchange_document_validation('source_of_funds_document')

    @api.onchange('negative_media_document')
    def _onchange_negative_media_document(self):
        self._onchange_document_validation('negative_media_document')

    @api.onchange('supporting_document')
    def _onchange_supporting_document(self):
        self._onchange_document_validation('supporting_document')

    def _onchange_document_validation(self, field_name):
        """Show warning for invalid files during onchange"""
        attachments = getattr(self, field_name)
        if not attachments:
            return

        allowed_types = self._get_allowed_file_types()
        invalid_files = []

        for attachment in attachments:
            is_valid = False

            if attachment.mimetype in allowed_types['mimetypes']:
                is_valid = True
            elif attachment.name:
                file_extension = '.' + \
                    attachment.name.lower().split(
                        '.')[-1] if '.' in attachment.name else ''
                if file_extension in allowed_types['extensions']:
                    is_valid = True

            if not is_valid:
                invalid_files.append(attachment.name or 'Unknown file')

        if invalid_files:
            field_label = self._fields[field_name].string
            return {
                'warning': {
                    'title': _('Invalid File Format'),
                    'message': _(
                        'Invalid files detected in "%s": %s\n\n'
                        'Allowed formats: Images (JPG, PNG,), '
                        'PDF, Word (DOC, DOCX), Excel (XLS, XLSX)'
                    ) % (field_label, ', '.join(invalid_files))
                }
            }

    def _get_file_type_from_binary(self, binary_data):
        """
        Detect file type from binary data using magic bytes/file signatures
        Returns the detected file type or None if not recognized
        """
        if not binary_data:
            return None

        try:
            # Decode base64 data
            file_data = base64.b64decode(binary_data)

            # Check file signatures (magic bytes)
            file_signatures = {
                # Images
                b'\xFF\xD8\xFF': 'jpeg',  # JPEG
                b'\x89PNG\r\n\x1a\n': 'png',  # PNG
                b'RIFF': 'webp',  # WEBP
                # PDF
                b'%PDF': 'pdf',  # PDF
            }

            # Check against known signatures
            for signature, file_type in file_signatures.items():
                if file_data.startswith(signature):
                    # Special case for WEBP (need to check WEBP signature)
                    if signature == b'WEBP' in file_data[:12]:
                        return 'webp'
                    elif signature != b'RIFF':
                        return file_type

            return None

        except (binascii.Error, Exception):
            return None

    def _validate_signature_file_type(self, binary_data, filename=None, field_name=None):
        """Validate that the binary data is a PDF or image file"""
        if not binary_data:
            return  # Empty field is allowed

        allowed_types = ['jpeg', 'png', 'webp', 'pdf']
        allowed_extensions = ['.jpg', '.jpeg', '.png', '.webp', '.pdf']

        # Try to detect file type from binary data
        detected_type = self._get_file_type_from_binary(binary_data)

        is_valid = False

        # First check: file type detection
        if detected_type in allowed_types:
            is_valid = True

        # Second check: filename extension (fallback)
        if not is_valid and filename:
            file_extension = '.' + \
                filename.lower().split('.')[-1] if '.' in filename else ''
            if file_extension in allowed_extensions:
                is_valid = True

        if not is_valid:
            field_label = field_name or "Signature field"
            raise ValidationError(_(
                'Invalid file format for "%s". '
                'Only images (JPG, PNG, WEBP) and PDF files are allowed.'
            ) % field_label)

    @api.constrains('responsible_officer_signature', 'approving_officer_signature')
    def _check_signature_file_types(self):
        """Validate signature file types on save"""
        for record in self:
            if record.responsible_officer_signature:
                record._validate_signature_file_type(
                    record.responsible_officer_signature,
                    getattr(record, 'responsible_signature_filename', None),
                    "Officer In Charge Signature"
                )

            if record.approving_officer_signature:
                record._validate_signature_file_type(
                    record.approving_officer_signature,
                    getattr(record, 'approving_signature_filename', None),
                    "Approving Officer Signature"
                )

    @api.onchange('responsible_officer_signature')
    def _onchange_responsible_officer_signature(self):
        """Validate responsible officer signature on change"""
        if self.responsible_officer_signature:
            try:
                self._validate_signature_file_type(
                    self.responsible_officer_signature,
                    getattr(self, 'responsible_signature_filename', None),
                    "Officer In Charge Signature"
                )
            except UserError as e:
                return {
                    'warning': {
                        'title': _('Invalid File Format'),
                        'message': str(e)
                    }
                }

    @api.onchange('approving_officer_signature')
    def _onchange_approving_officer_signature(self):
        """Validate approving officer signature on change"""
        if self.approving_officer_signature:
            try:
                self._validate_signature_file_type(
                    self.approving_officer_signature,
                    getattr(self, 'approving_officer_signature_filename', None),
                    "Approving Officer Signature"
                )
            except UserError as e:
                return {
                    'warning': {
                        'title': _('Invalid File Format'),
                        'message': str(e)
                    }
                }


class EDDRrejectWizard(models.TransientModel):
    _name = 'res.partner.edd.reject.wizard'
    _description = 'EDD Reject Reason Wizard'

    edd_id = fields.Many2one('res.partner.edd', string='EDD', required=True)
    reject_reason = fields.Text(string='Reject Reason', required=True)

    def action_confirm_reject(self):
        self.edd_id.action_reject(self.reject_reason)
        return {'type': 'ir.actions.act_window_close'}
