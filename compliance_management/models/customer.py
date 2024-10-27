# -*- coding: utf-8 -*-

from odoo import models, fields, api, _

LOW_RISK_THRESHOLD = 10
MEDIUM_RISK_THRESHOLD = 15
HIGH_RISK_THRESHOLD = 25

class Customer(models.Model):
    _inherit = 'res.partner'
    _sql_constraints = [
        ('uniq_customer_id', 'unique(customer_id)',
         "Customer ID already exists. Value must be unique!"),
    ]

    customer_id = fields.Char(string="Customer ID", index=True,tracking=True)
    bvn = fields.Char(string='BVN',tracking=True)
    branch_id = fields.Many2one(
        comodel_name='res.branch', string='Branch', index=True,tracking=True)
    education_level_id = fields.Many2one(
        comodel_name='res.education.level', string='Education Level', index=True,tracking=True)
    kyc_limit_id = fields.Many2one(
        comodel_name='res.partner.kyc.limit', string='KYC Limit')
    tier_id = fields.Many2one(
        comodel_name='res.partner.tier', string='Customer Tier', index=True)
    identification_type_id = fields.Many2one(
        comodel_name='res.identification.type', string='Identification Type', index=True,tracking=True)
    identification_number = fields.Char(string='Identification Number',tracking=True)
    identification_expiry_date = fields.Date(
        string='Identification Expiry Date', index=True,tracking=True)
    dob = fields.Date(
        string='Date of Birth',tracking=True)
    vat = fields.Char(string='Tax ID/TIN', index=True,
                      help="The Tax Identification Number. Values here will be validated based on the country format. You can use '/' to indicate that the partner is not subject to tax.")
    region_id = fields.Many2one(
        comodel_name='res.partner.region', string='Region',tracking=True)
    sector_id = fields.Many2one(
        comodel_name='res.partner.sector', string='Sector', index=True,tracking=True)
    sex_id = fields.Many2one(
        comodel_name='res.partner.gender', string='Sex', index=True)
    firstname = fields.Char(string='Firstname')
    lastname = fields.Char(string='Lastname')
    middlename = fields.Char(string='Middle Name')
    othername = fields.Char(string='Other Name')
    town = fields.Char(string='Town')
    registration_date = fields.Date(string='Registration Date',tracking=True)
    company_reg_date = fields.Date(string='Company Registration Date',tracking=True)
    risk_score = fields.Float(string='Risk Score', digits=(10, 2),tracking=True)
    risk_level = fields.Char(string='Risk Level',index=True,default='low',tracking=True)
    account_officer_id = fields.Many2one(
        comodel_name='res.users', string='Account Officer', index=True,tracking=True)
    risk_level_id = fields.Many2one(
        comodel_name='res.risk.level', string='Risk Level', index=True)
    account_ids = fields.One2many(
        comodel_name='res.partner.account', inverse_name='customer_id', string='Accounts')
    edd_ids = fields.One2many(
        comodel_name='res.partner.edd', inverse_name='customer_id', string='EDD Lines',tracking=True)
    risk_assessment_ids = fields.One2many(
        comodel_name='res.risk.assessment', inverse_name='partner_id', string='Risk Assessments')
    is_pep = fields.Boolean(string="Is PEP", default=False,tracking=True)
    is_watchlist = fields.Boolean(string="Is Watchlist", default=False,tracking=True)
    is_fep = fields.Boolean(string="Is FEP", default=False,tracking=True)
    is_blacklist = fields.Boolean(string="Is Blacklist", default=False,tracking=True)
    global_pep = fields.Boolean(string="Global PEP", default=False)
    current_branch_id = fields.Integer(
        string='Current Branch', compute='_get_current_branch')
    
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

    def action_add_pep(self):
        self.write({'is_pep': True})
        self.action_compute_risk_score_with_plan()

    def action_remove_pep(self):
        self.write({'is_pep': False})
        self.action_compute_risk_score_with_plan()

    def action_add_fep(self):
        self.write({'is_fep': True})
        self.action_compute_risk_score_with_plan()

    def action_remove_fep(self):
        self.write({'is_fep': False})
        self.action_compute_risk_score_with_plan()

    def action_blacklist(self):
        self.write({'is_blacklist': True})
        self.action_compute_risk_score_with_plan()

    def action_remove_blacklist(self):
        self.write({'is_blacklist': False})
        self.action_compute_risk_score_with_plan()

    def action_watchlist(self):
        self.write({'is_watchlist': True})
        self.action_compute_risk_score_with_plan()

    def action_remove_watchlist(self):
        self.write({'is_watchlist': False})
        self.action_compute_risk_score_with_plan()

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
            'domain': [('branch_id.id', 'in', [e.id for e in self.env.user.branches_id])],
            'context':{'search_default_group_branch': 1}
        }
        
    @api.model
    def open_customers(self):
        return {
            'name': _('Customers'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner',
            'view_mode': 'tree,form',
            'domain': [('branch_id.id', 'in', [e.id for e in self.env.user.branches_id])],
            'context':{'search_default_group_branch': 1}
        }

    def get_risk_score(self):
        return self.risk_score
    
    def get_risk_level(self):
        return self.risk_level
    
    def get_risk_level_name(self):
        return '%s risk'%(self.risk_level)

    def action_compute_risk_score_with_plan(self):
        self.env.cr.execute(
            'select risk_assessment_plan from res_config_settings order by id desc limit 1')
        rec = self.env.cr.fetchone()
        plan_setting = rec[0]
        for r in self:
            record_id = self.id
            scores = []
            plans = self.env['res.compliance.risk.assessment.plan'].search(
                [('state', '=', 'active')], order='priority')
            if plans:
                for pl in plans:
                    try:
                        self.env.cr.execute(pl.sql_query, (record_id,))
                        rec = self.env.cr.fetchone()
                        if rec is not None:
                            # we have a hit
                            if pl.compute_score_from == 'dynamic':
                                scores.append(
                                    float(rec[0])) if rec is not None else None
                            else:
                                # static
                                scores.append(float(pl.risk_score))
                    except:
                        pass
            if len(scores) > 0:
                if plan_setting == 'avg':
                    r.write({'risk_score': (sum(scores) / len(scores))})
                if plan_setting == 'max':
                    r.write({'risk_score': max(scores)})
            # Compute risk level
            partners = self.env['res.partner'].search([('id','=',r.id)],limit=1)
            for e in partners:
                risk_level = e.compute_risk_level()
                e.write({'risk_level':risk_level})
