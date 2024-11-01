from odoo import _, api, fields, models
# from odoo.exceptions import ValidationError, UserError

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
    approved_by = fields.Many2one(comodel_name='res.users', string='Approver', readonly=True)
    customer_id = fields.Many2one(comodel_name='res.partner', string='Customer', index=True)
    responsible_id = fields.Many2one(comodel_name='res.users', string='Responsible User', index=True)
    risk_score = fields.Float(string='Risk Score', tracking=True)
    date_approved = fields.Date(string="Date Approved", readonly=True, tracking=True)
    approving_officer_id = fields.Many2one(comodel_name='res.users', string='Approving Officer', tracking=True)
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
    was_kyc_comprehensive = fields.Boolean(string="Was Kyc Comprehensive", tracking=True)
    has_initiated_new_kyc = fields.Boolean(string="Has Initiated New Kyc", tracking=True)
    visitation_observation = fields.Text(string="Visitation Observation", tracking=True)
    overall_kyc_outcome = fields.Text(string="Overall Kyc Outcome", tracking=True)
    is_foreigner = fields.Boolean(string="Is Foreigner", tracking=True)
    is_pep = fields.Boolean(string="Is PEP", tracking=True)
    id_expired = fields.Boolean(string="Id Expired", tracking=True)
    activity_level_matches_business = fields.Boolean(string="Activity Level Matches Business", tracking=True)
    main_cash_purpose = fields.Text(string="Main Cash Purpose", tracking=True)
    main_inflow_purpose = fields.Text(string="Main Inflow Purpose", tracking=True)
    main_fund_remitters = fields.Text(string="Main Fund Remitters", tracking=True)
    inflow_sources = fields.Text(string="Inflow Sources", tracking=True)
    other_related_accounts = fields.Text(string="Other Related Accounts", tracking=True)
    occupation = fields.Text(string="Occupation", tracking=True)
    is_current_from_normal = fields.Boolean(string="Is Current From Normal", tracking=True)
    does_business_support_volume = fields.Boolean(string="Does Business Support Volume", tracking=True)
    is_current_user_responsible = fields.Boolean(compute='_compute_is_current_user_responsible')
    is_current_user_approver = fields.Boolean(compute='_compute_is_current_user_approver', store=False)
    is_current_user_approving_officer = fields.Boolean(compute='_compute_is_current_user_approving_officer')
    is_cco = fields.Boolean(compute='_compute_is_cco', store=False, default=lambda self: self._default_is_cco())

    @api.model
    def _default_is_cco(self):
        """Default method to set is_cco based on the user group."""
        return self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')

    @api.depends_context('uid')
    def _compute_is_cco(self):
        """Compute method to update is_cco based on user group when editing records."""
        for record in self:
            record.is_cco = self.env.user.has_group('compliance_management.group_compliance_chief_compliance_officer')


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
            
    @api.depends('approving_officer_id')
    def _compute_is_current_user_approving_officer(self):
        for record in self:
            # Check if the approved_by matches the current user ID
            record.is_current_user_approving_officer = (record.approving_officer_id.id == self.env.user.id)

    def action_submit_for_review(self):
        self.ensure_one()
        self.write({
            'status': 'completed'           
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
