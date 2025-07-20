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
    # risk_level = fields.Selection(string='Risk Level', selection=[(
    #     'low', 'Low'), ('medium', 'Medium'), ('high', 'High')], default='low', tracking=True)
    
    risk_score = fields.Float(string='Risk Score', digits=(
        10, 2), related="customer_id.risk_score")
    risk_level = fields.Char(string='Risk Rating',
                             related="customer_id.risk_level")
    
    state = fields.Selection(string='Status', selection=[(
        'new', 'To Review'), ('done', 'Done')], tracking=True, index=True, default='new')
    likely_fraud = fields.Boolean(string='Likely Fraud',tracking=True,related='rule_id.likely_fraud')
    
    
    show_create_case_button = fields.Boolean(
    string="Case Management Installed",
    compute='_compute_is_case_management_installed',
    store=False,)
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
    
    show_create_case= fields.Boolean(
        string="Case Management_v2 Installed",
        compute='_compute_is_case_manager_installed',
        store=False,)

    def action_create_transaction_case(self):
        self.ensure_one()
        # Prepare action to open case form
        action = self.env.ref(
            'case_management_v2.action_case_manager').read()[0]

        form_view = self.env.ref('case_management_v2.view_case_manager_form')
        action['views'] = [(form_view.id, 'form')]
        action['target'] = 'current'
        action['flags'] = {'initial_mode': 'edit'}

        # Prepare default values from customer
        context = {
            'default_customer_id': self.id,
            'default_case_status': 'draft',
        }

        # Add customer if available
        if self.customer_id:
            context['default_customer_id'] = self.customer_id.id

        # Set risk level
        if self.risk_level:
            context['default_case_rating'] = self.risk_level
            
        if self.risk_score:
            context['default_case_score'] = self.risk_score

        # Add transaction details to narration
        narration = f"Transaction Reference: {self.name or ''}\n"
        narration += f"Amount: {self.amount or 0.0} {self.currency or ''}\n"
        narration += f"Date: {self.date_created or ''}\n"

        if self.narration:
            narration += f"\nTransaction Narration: {self.narration}"

        context['default_narration'] = narration

        # Add branch if available
        if hasattr(self, 'branch_id') and self.branch_id:
            context['default_department_id'] = self.branch_id.id

        action['context'] = context
        return action
    
    @api.depends('customer_id')
    def _compute_is_case_manager_installed(self):
        for record in self:
            case_model = bool(self.env['ir.module.module'].search([
                ('name', '=', 'case_management_v2'),
                ('state', '=', 'installed')
            ], limit=1))
            record.show_create_case = bool(case_model)



    @api.depends('date_created')  
    def _compute_is_case_management_installed(self):
        case_management_installed = bool(self.env['ir.module.module'].search([
            ('name', '=', 'case_management'),
            ('state', '=', 'installed')
        ], limit=1))
        
        for record in self:
            record.show_create_case_button = case_management_installed
        
    
    
    @api.model
    def _is_case_management_available(self):
        """Cache the module availability check"""
        if not hasattr(self, '_case_management_available'):
            self._case_management_available = bool(self.env['ir.module.module'].search([
                ('name', '=', 'case_management'),
                ('state', '=', 'installed')
            ], limit=1))
        return self._case_management_available    
    
    
    
            
    

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
        
        user = self.env.user
        compliance_groups = [
            'compliance_management.group_compliance_chief_compliance_officer',
            'compliance_management.group_compliance_compliance_officer',
            'compliance_management.group_compliance_transaction_monitoring_team'
        ]
        has_compliance_access = any(user.has_group(group)
                                    for group in compliance_groups)

        # Set domain based on user group
        if has_compliance_access:
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
        
        user = self.env.user
        compliance_groups = [
            'compliance_management.group_compliance_chief_compliance_officer',
            'compliance_management.group_compliance_compliance_officer',
            'compliance_management.group_compliance_transaction_monitoring_team'
        ]
        has_compliance_access = any(user.has_group(group)
                                    for group in compliance_groups)

        # Set domain based on user group
        if has_compliance_access :
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
        
        user = self.env.user
        compliance_groups = [
            'compliance_management.group_compliance_chief_compliance_officer',
            'compliance_management.group_compliance_compliance_officer',
            'compliance_management.group_compliance_transaction_monitoring_team'
        ]
        has_compliance_access = any(user.has_group(group)
                                    for group in compliance_groups)

        # Set domain based on user group
        if has_compliance_access:
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

    def get_risk_score(self):
        return self.risk_score
