# -*- coding: utf-8 -*-

from odoo import models, fields, api, _

class Transaction(models.Model):
    _name = 'res.customer.transaction'
    _description = 'Transaction'
    _sql_constraints = [
        ('uniq_trans_name', 'unique(name)',
         "Account Reference already exists. Value must be unique!"),
    ]
    _order = 'date_created desc'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    name = fields.Char(string="Reference Number", index=True)
    account_id = fields.Many2one(
        comodel_name='res.partner.account', string='Account', index=True)
    currency_id = fields.Many2one(
        comodel_name='res.currency', string='Currency', index=True)
    date_created = fields.Date(string='Tran. Date', index=True)
    customer_id = fields.Many2one(
        comodel_name='res.partner', string='Customer', index=True) 
    branch_id = fields.Many2one(
        comodel_name='res.branch', string='Branch', index=True)
    amount = fields.Float(string='Transaction Amount', digits=(15, 2))
    # tran_type = fields.Selection(string='Tran. Type', selection=[
    #                              ('dr', 'Debit'), ('cr', 'Credit')], index=True)
    tran_type = fields.Many2one(comodel_name='res.transaction.type',
                              string='Tran. Type', index=True)
    narration = fields.Text(string='Narration')
    batch_code = fields.Char(string='Batch Code', index=True)
    rule_id = fields.Many2one(comodel_name='res.transaction.screening.rule',
                              string='Exception Rule', tracking=True, index=True)
    risk_level = fields.Selection(string='Risk Level', selection=[(
        'low', 'Low'), ('medium', 'Medium'), ('high', 'High')], default='low', tracking=True)
    state = fields.Selection(string='Status', selection=[(
        'new', 'To Review'), ('done', 'Done')], tracking=True, index=True, default='new')
    likely_fraud = fields.Boolean(string='Likely Fraud',tracking=True,related='rule_id.likely_fraud')
    

    def action_create_case(self):
        """
        Opens the case management form with the transaction reference pre-filled
        """
        context = {
            'default_status_id': self.env.ref('case_management.case_status_open').id,
            'case_created': True,
            'show_creation_notification': True,
            #'default_transaction_reference': self.name,
            'default_transaction_reference': self.id,
            # Don't set default_transaction_id here since it will be computed
        }
        
        return {
            'type': 'ir.actions.act_window',
            'name': 'New Case',
            'res_model': 'case',
            'view_mode': 'form',
            'view_id': self.env.ref('case_management.case_form_view').id,
            'target': 'current',
            'context': context
        }
        
        
    
    
    def action_create_case(self):
        """
        Opens the case management form with the transaction reference pre-filled
        """
        # Create the context with required values
        context = {
            'default_status_id': self.env.ref('case_management.case_status_open').id,
            'case_created': True,
            'show_creation_notification': True,
        }
        
        # Pre-fill the transaction reference
        context['default_transaction_reference'] = self.name
        
        # Pre-fill the transaction_id field if it exists in the case model
        context['default_transaction_id'] = self.id
        
        return {
            'type': 'ir.actions.act_window',
            'name': 'New Case',
            'res_model': 'case',
            'view_mode': 'form',
            'view_id': self.env.ref('case_management.case_form_view').id,
            'target': 'current',
            'context': context
        }

    account_officer_id = fields.Many2one(
        comodel_name='account.officers', string='Account Officer', index=True, tracking=True, readonly=True)
    trans_code = fields.Char(string='Transaction Code')
    currency = fields.Char(string='Currency')
    inputter = fields.Char(string='Inputter')
    authorizer = fields.Char(string='Authorizer')
    transaction_type = fields.Selection(selection=[(
        'C', 'Credit'), ('D', 'Debit')],  index=True, string='Transaction Type')
    active = fields.Boolean(default=True, readonly=True)
    branch_code = fields.Char(string="Branch Code")


    def init(self):
        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS res_customer_transaction_id_idx ON res_customer_transaction (id)")

    def get_risk_level(self):
        return self.risk_level

    def done(self):
        for e in self:
            e.write({'state': 'done'})

    def action_screen(self):
        rules = self.env['res.transaction.screening.rule'].search(
            [('state', '=', 'active')], order='priority')

        if rules:
            for rule in rules:
                # try:
                    query = rule.sql_query
                    char_to_replace = {'#AMOUNT#': f"{self.amount}",
                                    '#ACCOUNT_ID#': f"{self.account_id.id}",
                                    "#CUSTOMER_ID#": f"{self.customer_id.id}",
                                    "#TRAN_DATE#": f"{self.date_created}",
                                    "#BRANCH_ID#": f"{self.branch_id.id}",
                                    "#CURRENCY_ID#": f"{self.currency_id.id}"}
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
        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')
        
        is_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_compliance_officer')

        # Set domain based on user group
        if is_chief_compliance_officer or is_compliance_officer:
            # Chief Compliance Officers see all customers
            domain = [('state', '=', 'new')]
        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id]), ('state', '=', 'new')]
            
        return {
            'name': _('Transactions To Review'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'limit': 50,
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1, 'default_state': 'new'}
        }

    @api.model
    def open_transactions_done(self):
        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')
        
        is_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_compliance_officer')

        # Set domain based on user group
        if is_chief_compliance_officer or is_compliance_officer:
            # Chief Compliance Officers see all customers
            domain = [('state', '=', 'done')]
        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id]), ('state', '=', 'done')]
        return {
            'name': _('Transactions Reviewed'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'limit': 50,
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1, 'default_state': 'done'}
        }

    @api.model
    def open_transactions_all(self):
        is_chief_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_chief_compliance_officer')
        
        is_compliance_officer = self.env.user.has_group(
            'compliance_management.group_compliance_compliance_officer')

        # Set domain based on user group
        if is_chief_compliance_officer or is_compliance_officer:
            # Chief Compliance Officers see all customers
            domain = []
        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id])  ]

        return {
            'name': _('All Transactions'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }
