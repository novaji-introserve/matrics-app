from odoo import _, api, fields, models


class CustomerEDD(models.Model):
    _name = 'res.partner.edd'
    _description = 'Enhanced Due Diligence'

    name = fields.Char(string="Name")
    status = fields.Selection(string='Status', selection=[(
        'draft', 'Draft'), ('completed', 'Completed'), ('approved', 'Approved'), ('cancelled', 'Cancelled'), ('deleted', 'Deleted'), ('archived', 'Archived')], default='draft')
    description = fields.Text(string='Description')
    user_id = fields.Many2one(comodel_name='res.users', string='User',
                              required=True, index=True, default=lambda self: self.env.user.id)
    approved_by = fields.Many2one(comodel_name='res.users', string='Approver', readonly=True)
    customer_id = fields.Many2one(
        comodel_name='res.partner', string='Customer',required=True,index=True)
    responsible_id = fields.Many2one(
        comodel_name='res.users', string='Responsible User',required=True,index=True)
    # risk_score = fields.Float(string='Risk Score', digits=(10, 2))
    risk_score = fields.Float(
        string='Risk Score', required=True, tracking=True)
    date_approved = fields.Date(string="Date Approved",required=True, readonly=True)
    approving_officer_id = fields.Many2one(
        comodel_name='res.users', string='Approving Officer')
    account_status = fields.Selection(string='Account Status', selection=[
                                      ('active', 'Active'), ('dormant', 'Dormant')])
    documentation_status = fields.Char(string="Documentation Status")
    last_kyc_date = fields.Date(string="Last Kyc Date")
    was_kyc_comprehensive = fields.Boolean(string="Was Kyc Comprehensive")
    has_initiated_new_kyc = fields.Boolean(string="Has Initiated New Kyc")
    visitation_observation = fields.Text(string="Visitation Observation")
    overall_kyc_outcome = fields.Text(string="Overall Kyc Outcome")
    is_foreigner = fields.Boolean(string="Is Foreigner")
    is_pep = fields.Boolean(string="Is PEP")
    id_expired = fields.Boolean(string="Id Expired")
    activity_level_matches_business = fields.Boolean(
        string="Activity Level Matches Business")
    main_cash_purpose = fields.Text(string="Main Cash Purpose")
    main_inflow_purpose = fields.Text(string="Main Inflow Purpose")
    main_fund_remitters = fields.Text(string="Main Fund Remitters")
    inflow_sources = fields.Text(string="Inflow Sources")
    other_related_accounts = fields.Text(string="Other Related Accounts")
    occupation = fields.Text(string="Occupation")
    is_current_from_normal = fields.Boolean(string="Is Current From Normal")
    does_business_support_volume = fields.Boolean(
        string="Does Business Support Volume")
