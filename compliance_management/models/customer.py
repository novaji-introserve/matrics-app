# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class Customer(models.Model):
    _inherit = 'res.partner'
    _sql_constraints = [
        ('uniq_customer_id', 'unique(customer_id)',
         "Customer ID already exists. Value must be unique!"),
    ]

    customer_id = fields.Char(string="Customer ID", index=True)
    bvn = fields.Char(string='BVN')
    branch_id = fields.Many2one(
        comodel_name='res.branch', string='Branch', index=True)
    education_level_id = fields.Many2one(
        comodel_name='res.education.level', string='Education Level', index=True)
    kyc_limit_id = fields.Many2one(
        comodel_name='res.partner.kyc.limit', string='KYC Limit')
    tier_id = fields.Many2one(
        comodel_name='res.partner.tier', string='Customer Tier', index=True)
    identification_type_id = fields.Many2one(
        comodel_name='res.identification_type', string='Identification Type', index=True)
    identification_number = fields.Char(string='Identification Number')
    identification_expiry_date = fields.Date(
        string='Identification Expiry Date', index=True)
    vat = fields.Char(string='Tax ID/TIN', index=True,
                      help="The Tax Identification Number. Values here will be validated based on the country format. You can use '/' to indicate that the partner is not subject to tax.")
    region_id = fields.Many2one(
        comodel_name='res.partner.region', string='Region')
    sector_id = fields.Many2one(
        comodel_name='res.partner.sector', string='Sector', index=True)
    sex_id = fields.Many2one(
        comodel_name='res.partner.gender', string='Sex', index=True)
    firstname = fields.Char(string='Firstname')
    lastname = fields.Char(string='Lastname')
    middlename = fields.Char(string='Middle Name')
    othername = fields.Char(string='Other Name')
    town = fields.Char(string='Town')
    registration_date = fields.Date(string='Registration Date')
    company_reg_date = fields.Date(string='Company Registration Date')
    risk_score = fields.Float(string='Risk Score', digits=(10, 5))
    account_officer_id = fields.Many2one(
        comodel_name='res.user', string='Account Officer', index=True)
    risk_level_id = fields.Many2one(
        comodel_name='res.risk.level', string='Risk Level', index=True)
    account_ids = fields.One2many(comodel_name='res.partner.account', inverse_name='customer_id', string='Accounts')
    edd_ids = fields.One2many(comodel_name='res.partner.edd', inverse_name='customer_id', string='EDD Lines')
    risk_assessment_ids = fields.One2many(comodel_name='res.risk.assessment', inverse_name='partner_id', string='Risk Assessments')
    is_pep = fields.Boolean(string="Is PEP",default=False)
    is_watchlist = fields.Boolean(string="Is Watchlist",default=False)
    is_fep = fields.Boolean(string="Is FEP",default=False)
    is_blacklist = fields.Boolean(string="Is Blacklist",default=False)
    global_pep = fields.Boolean(string="Global PEP",default=False)

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

    def action_remove_pep(self):
        self.write({'is_pep': False})

    def action_add_fep(self):
        self.write({'is_fep': True})

    def action_remove_fep(self):
        self.write({'is_fep': False})

    def action_blacklist(self):
        self.write({'is_blacklist': True})

    def action_remove_blacklist(self):
        self.write({'is_blacklist': False})

    def action_watchlist(self):
        self.write({'is_watchlist': True})

    def action_remove_watchlist(self):
        self.write({'is_watchlist': False})

    def action_conduct_risk_assessment(self):
        return {
            'name': _('Risk Assessment'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.risk.assessment',
            'view_mode': 'form',
            'context': {"default_partner_id": self.id},
        }
    
    def get_risk_score(self):
         return self.risk_score
