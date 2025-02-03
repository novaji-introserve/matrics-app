# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class CustomerAccount(models.Model):
    _name = 'res.partner.account'
    _description = 'Account'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _sql_constraints = [
        ('uniq_account_name', 'unique(name)',
         "Account Name already exists. Value must be unique!"),
    ]
    _order = "name" 
    name = fields.Char(string="Account Number")
    account_name = fields.Char(string='Account Name', index=True)
    currency_id = fields.Many2one(
        comodel_name='res.currency', string='Currency', index=True)
    product_id = fields.Many2one(
        comodel_name='res.partner.account.product', string='Product',index=True)
    date_created = fields.Date(string='Date Created', index=True)
    ledger_id = fields.Many2one(comodel_name='res.partner.account.ledger', string='Ledger',index=True)
    closure_status = fields.Selection(string='Closure Status', selection=[('N', 'No'), ('Y', 'Yes')])
    customer_id = fields.Many2one(comodel_name='res.partner', string='Customer',index=True)
    branch_id = fields.Many2one(comodel_name='res.branch', string='Branch',index=True)
    balance = fields.Float(string='Balance', digits=(15,4))
    account_type_id = fields.Many2one(comodel_name='res.partner.account.type', string='Account Type',index=True)
    risk_assessment = fields.Many2one(comodel_name='res.risk.assessment', string='Risk Assessment',index=True)
    num_tran_last6m = fields.Integer(string='Transactions - Last 6m')
    avg_tran_last6m = fields.Float(string='Avg. Transaction Amount - Last 6m', digits=(10,2))
    max_tran_last6m = fields.Float(string='Max. Transaction - Last 6m', digits=(10,2))
    tot_tran_last6m = fields.Float(string='Total Transaction Amount - Last 6m', digits=(15,2))
    num_tran_last1y = fields.Integer(string='Transactions - Last 1Y')
    avg_tran_last1y = fields.Float(string='Avg. Transaction Amount - Last 1Y', digits=(10,2))
    max_tran_last1y = fields.Float(string='Max. Transaction - Last 1Y', digits=(10,2))
    tot_tran_last1y = fields.Float(string='Total Transaction Amount - Last 1Y', digits=(15,2))
    risk_score = fields.Float(string='Risk Score', digits=(10,2),related="customer_id.risk_score")
    risk_level = fields.Char(string='Risk Rating',related="customer_id.risk_level")
    state = fields.Selection(string='Status', selection=[('active', 'Active'), ('dormant', 'Dormant'),('locked','Locked')],tracking=True,default='active')
    
     
    @api.model
    def open_accounts(self):
        return {
            'name': _('Accounts'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.partner.account',
            'view_mode': 'tree,form',
            'domain': [('branch_id.id', 'in', [e.id for e in self.env.user.branches_id])],
            'context': {'search_default_group_branch': 1}
        } 
        
    def get_balance(self):
        return  '{0:.2f}'.format(self.balance)
    
    def get_risk_score(self):
        return self.risk_score

    def get_risk_level(self):
        return self.risk_level
