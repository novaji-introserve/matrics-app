from odoo import _, api, fields, models
from odoo.exceptions import ValidationError, UserError


class CustomerEDD(models.Model):
    _name = 'res.partner.edd'
    _description = 'Enhanced Due Diligence'
    _inherit = ['mail.thread']
    _inherit = ['mail.thread', 'mail.activity.mixin']


    name = fields.Char(string="Name", required=True)
    status = fields.Selection(string='Status', selection=[(
        'draft', 'Draft'), ('completed', 'Completed'), ('approved', 'Approved'), ('cancelled', 'Cancelled'), ('deleted', 'Deleted'), ('archived', 'Archived')], default='draft', required=True)
    description = fields.Text(string='Description', required=True)
    user_id = fields.Many2one(comodel_name='res.users', string='User',
                            required=True, index=True, default=lambda self: self.env.user.id)
    current_user_id = fields.Many2one(comodel_name='res.users', string='Current User',
                                    required=True, index=True, default=lambda self: self.env.user.id)
    approved_by = fields.Many2one(comodel_name='res.users', string='Approver', readonly=True)
    customer_id = fields.Many2one(
        comodel_name='res.partner', string='Customer', required=True, index=True)
    responsible_id = fields.Many2one(
        comodel_name='res.users', string='Responsible User', required=True, index=True)
    risk_score = fields.Float(
        string='Risk Score', required=True, tracking=True)
    date_approved = fields.Date(string="Date Approved", readonly=True)
    approving_officer_id = fields.Many2one(
        comodel_name='res.users', string='Approving Officer', required=True)
    account_status = fields.Selection(string='Account Status', selection=[
                                    ('active', 'Active'), ('dormant', 'Dormant')], required=True)
    documentation_status = fields.Char(string="Documentation Status", required=True)
    last_kyc_date = fields.Date(string="Last Kyc Date", required=True)
    was_kyc_comprehensive = fields.Boolean(string="Was Kyc Comprehensive", required=True)
    has_initiated_new_kyc = fields.Boolean(string="Has Initiated New Kyc", required=True)
    visitation_observation = fields.Text(string="Visitation Observation", required=True)
    overall_kyc_outcome = fields.Text(string="Overall Kyc Outcome", required=True)
    is_foreigner = fields.Boolean(string="Is Foreigner", required=True)
    is_pep = fields.Boolean(string="Is PEP", required=True)
    id_expired = fields.Boolean(string="Id Expired", required=True)
    activity_level_matches_business = fields.Boolean(
        string="Activity Level Matches Business", required=True)
    main_cash_purpose = fields.Text(string="Main Cash Purpose", required=True)
    main_inflow_purpose = fields.Text(string="Main Inflow Purpose", required=True)
    main_fund_remitters = fields.Text(string="Main Fund Remitters", required=True)
    inflow_sources = fields.Text(string="Inflow Sources", required=True)
    other_related_accounts = fields.Text(string="Other Related Accounts", required=True)
    occupation = fields.Text(string="Occupation", required=True)
    is_current_from_normal = fields.Boolean(string="Is Current From Normal", required=True)
    does_business_support_volume = fields.Boolean(
        string="Does Business Support Volume", required=True)
    is_current_user_responsible = fields.Boolean(compute='_compute_is_current_user_responsible')
    is_current_user_approver = fields.Boolean(compute='_compute_is_current_user_approver')


    @api.depends('responsible_id')
    def _compute_is_current_user_responsible(self):
        for record in self:
            # Check if the responsible_id matches the current user ID
            record.is_current_user_responsible = (record.responsible_id.id == self.env.user.id)

    @api.depends('approved_by')
    def _compute_is_current_user_approver(self):
        for record in self:
            # Check if the approved_by matches the current user ID
            record.is_current_user_approver = (record.approved_by.id == self.env.user.id)

    def action_submit_for_review(self):
        self.ensure_one()
        if not self.name or not self.description or not self.risk_score:
            raise ValidationError(_('Please fill in all required fields before submitting for review.'))
        
        self.write({
            'status': 'completed'           
        })   

        # Get the email template and send an email to the Officer in Charge (responsible_id)
        template_id = self.env.ref('compliance_management.email_template_notify_officer_in_charge')
        if template_id:
            template_id.sudo().send_mail(self.id, force_send=True)

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
