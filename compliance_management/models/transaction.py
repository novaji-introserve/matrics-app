# -*- coding: utf-8 -*-

from odoo import models, fields, api, _


class Transaction(models.Model):
    _name = 'res.customer.transaction'
    _description = 'Transaction'
    _sql_constraints = [
        ('uniq_account_name', 'unique(name)',
         "Account Name already exists. Value must be unique!"),
    ]
    _order = 'date_created desc'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    name = fields.Char(string="Reference Number", required=True,index=True)
    account_id = fields.Many2one(comodel_name='res.partner.account', string='Account',index=True)
    currency_id = fields.Many2one(
        comodel_name='res.currency', string='Currency', index=True)
    date_created = fields.Date(string='Tran. Date', index=True)
    customer_id = fields.Many2one(comodel_name='res.partner', string='Customer',index=True)
    branch_id = fields.Many2one(comodel_name='res.branch', string='Branch',index=True)
    amount = fields.Float(string='Transaction Amount', digits=(10,2))
    tran_type = fields.Selection(string='Tran. Type', selection=[('dr', 'Debit'), ('cr', 'Credit')],index=True)
    narration = fields.Text(string='Narration')
    batch_code = fields.Char(string='Batch Code',index=True)
    rule_id = fields.Many2one(comodel_name='res.transaction.screening.rule', string='Exception Rule',tracking=True,index=True)
    risk_level = fields.Selection(string='Risk Level', selection=[('low', 'Low'), ('medium', 'Medium'),('high','High')],default='low',tracking=True)
    state = fields.Selection(string='Status', selection=[('new', 'To Review'), ('done', 'Done')],tracking=True,index=True,default='new')
    
    def get_risk_level(self):
        return self.risk_level
    
    def done(self):
        for e in self:
            e.write({'state':'done'})
    
    def action_screen(self):
        rules = self.env['res.transaction.screening.rule'].search(
            [('state', '=', 'active')], order='priority')
   
        if rules:
            for rule in rules:
                #try:
                query = rule.sql_query
                char_to_replace = {'#AMOUNT#': f"{self.amount}",
                                    '#ACCOUNT_ID#': f"{self.account_id.id}"}
                    # Iterate over all key-value pairs in dictionary
                for key, value in char_to_replace.items():
                    # Replace key character with value character in string
                    query = query.replace(key, value)
                self.env.cr.execute(query)
                rec = self.env.cr.fetchone()
                if rec is not None:
                    self.rule_id = rule
                    self.risk_level = rule.risk_level
                    return

    @api.model
    def open_transactions(self):
        return {
            'name': _('Transactions'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'limit': 50,
            'view_mode': 'tree,form',
            'domain': [('branch_id.id', 'in', [e.id for e in self.env.user.branches_id])],
            'context': {'search_default_group_branch': 1,'default_state':'new'}
        }
    
