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
    #lang_id = fields.Many2one(comodel_name='res.lang', string='Language')
    kyc_limit_id = fields.Many2one(
        comodel_name='res.partner.kyc.limit', string='KYC Limit')
    tier_id = fields.Many2one(
        comodel_name='res.partner.tier', string='Customer Tier', index=True)
    identification_type_id = fields.Many2one(
        comodel_name='res.identification_type', string='Identification Type', index=True)
    identification_number = fields.Char(string='Identification Number')
    identification_expiry_date = fields.Date(
        string='Identification Expiry Date', index=True)
    vat = fields.Char(string='Tax ID/TIN', index=True, help="The Tax Identification Number. Values here will be validated based on the country format. You can use '/' to indicate that the partner is not subject to tax.")
    #marital_status_id = fields.Many2one(
    #    comodel_name='res.marital.status', string='Marital Status', index=True)
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
    #dob = fields.Date(string='Date of Birth', index=True)
    registration_date = fields.Date(string='Registration Date')
    company_reg_date = fields.Date(string='Company Registration Date')
    risk_score = fields.Float(string='Risk Score', digits=(10, 5))
    account_officer_id = fields.Many2one(
        comodel_name='res.user', string='Account Officer', index=True)
    risk_level_id = fields.Many2one(
        comodel_name='res.risk.level', string='Risk Level', index=True)
    account_ids = fields.One2many(comodel_name='res.partner.account', inverse_name='customer_id', string='Accounts')
    edd_ids = fields.One2many(comodel_name='res.partner.edd', inverse_name='customer_id', string='EDD Lines')
    is_pep = fields.Boolean(string="Is PEP")
    is_watchlist = fields.Boolean(string="Is Watchlist")
    is_fep = fields.Boolean(string="Is FEP")
    is_blacklist = fields.Boolean(string="Is Blacklist")
    global_pep = fields.Boolean(string="Global PEP")
    
    def action_initiate_edd(self):
        print('Initiate edd')
    
    def action_add_pep(self):
        print('Add to PEP list')
    
    def action_add_fep(self):
        print('Add to FEP list')
    
    def action_blacklist(self):
        print('blacklist')
    
    def action_watchlist(self):
        print('Add to watchlist')
    
    def action_conduct_risk_assessment(self):
        print('Risk assessment')
