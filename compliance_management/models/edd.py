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

            # Select the officer, ensuring one of the officers exists
            officer = self.approving_officer_id or officer.responsible_id
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

                email_values = {
                    'email_to': cco_email
                }
                _logger.info(
                    f"{template.name} notification sent to {cco_email}")

            else:
                email_values = {
                    'email_to': officer_email,
                    'email_cc': cco_email
                }
                _logger.info(
                    f"{template.name} notification sent to {officer.email} with CC to {cco_email}")

            template.with_context(**ctx).send_mail(
                self.id,
                force_send=True,
                email_values=email_values
            )
            _logger.info(f"here are your {email_values}")
        except Exception as e:
            _logger.error(
                f"{template.name}Failed to send notification: {str(e)}")
            raise ValidationError(
                f"{template.name}Failed to send notification: {str(e)}")

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
                officer=record
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
            'status': 'completed'
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
                if value > 25:
                    record.risk_score = 25.0
                # Ensure it's not below minimum
                elif value < 0.5:
                    record.risk_score = 0.5
            except (ValueError, TypeError):
                # If conversion fails, set a default value
                record.risk_score = 1.0
