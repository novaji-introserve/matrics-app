# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
import logging
from dotenv import load_dotenv
import requests

load_dotenv()
_logger = logging.getLogger(__name__)


class Transaction(models.Model):
    _name = 'res.customer.transaction'
    _description = 'Transaction'
    _sql_constraints = [
        ('uniq_trans_name', 'unique(name)',
         "Account Reference already exists. Value must be unique!"),
    ]
    _order = 'id desc'
    
    _inherit = ['mail.thread', 'mail.activity.mixin']
    name = fields.Char(string="Reference Number", index=True)
    account_id = fields.Many2one(
        comodel_name='res.partner.account', string='Account', index=True)
    currency_id = fields.Many2one(
        comodel_name='res.currency', string='Currency', index=True)
    date_created = fields.Datetime(string='Tran. Date', index=True)
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
    
    branch_code = fields.Char(string="Branch Code")
    
    show_create_case= fields.Boolean(
        string="Case Management Installed",
        compute='_compute_is_case_manager_installed',
        store=False,)

    rule_ids = fields.One2many(
        'res.transaction.screening.history', 'transaction_id',
        string='Screening Rules', tracking=True)
    
    total_rules = fields.Integer(
        string='Rules', compute='transaction_total_rules', index=True, store=False)
    
    rule_line_ids = fields.One2many(
        comodel_name='res.transaction.screening.rule.line', inverse_name='transaction_id', string='Exception Analysis Lines', tracking=True)
    
    @api.model
    def create(self,vals_list):
        records = super(Transaction, self).create(vals_list)
        for rec in records:
            rec.action_screen()
        return records

    @api.depends('rule_ids')
    def transaction_total_rules(self):
        for e in self:
            e.total_rules = len(e.rule_ids)

    def action_view_transaction_screening_rules(self):
        return {
            'name': _('Transaction Screening Rules'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.transaction.screening.history',
            'view_mode': 'tree,form',
            'domain': [('transaction_id.id', 'in', [self.id])],
            'context': {'search_default_group_rule_id': 1}
        }

    def action_create_transaction_case(self):
        self.ensure_one()
        # Prepare action to open case form
        action = self.env.ref(
            'case_management.action_case_manager').read()[0]

        form_view = self.env.ref('case_management.view_case_manager_form')
        action['views'] = [(form_view.id, 'form')]
        action['target'] = 'current'
        action['flags'] = {'initial_mode': 'edit'}

        # Prepare default values from customer
        context = {
            'default_customer_id': self.customer_id.id if self.customer_id else False,
            'default_case_status': 'open',
            'default_transaction_id': self.id,
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
                ('name', '=', 'case_management'),
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
        self.ensure_one()

        # Create the context with required values
        context = {
            'default_case_status': 'open',
            'case_created': True,
            'show_creation_notification': True,
            'default_transaction_id': self.id,
            'default_transaction_reference': self.name,
            'default_customer_id': self.customer_id.id if self.customer_id else False,
        }
        
        return {
            'type': 'ir.actions.act_window',
            'name': 'New Case',
            'res_model': 'case.manager',
            'view_mode': 'form',
            'view_id': self.env.ref('case_management.view_case_manager_form').id,
            'target': 'current',
            'context': context
        }
        
        


    def init(self):
        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS res_customer_transaction_id_idx ON res_customer_transaction (id)")
        self.env.cr.execute("""
        ALTER TABLE res_customer_transaction
        ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
    """)

    def get_risk_level(self):
        return self.risk_level

    def predict(self):
        self.ensure_one()
        risk_rating_map = {
            'low': 1,
            'medium': 2,
            'high': 3,
        }

        payload = {
            'amount': self.amount or 0.0,
            'transaction_date': fields.Date.to_string(
                fields.Datetime.to_datetime(self.date_created).date()
            ) if self.date_created else False,
            'currency': self.currency or self.currency_id.name or 'NGN',
            'account_id': self.account_id.id if self.account_id else False,
            'terminal_id': self.branch_id.id if self.branch_id else 317,
            'cust_risk_rating': risk_rating_map.get((self.risk_level or '').lower(), 3),
            'cust_risk_score': self.risk_score or 9,
        }

        try:
            response = requests.post(
                'http://fastapi:8000/fraud/predict',
                json=payload,
                timeout=10,
            )
            response.raise_for_status()
            result = response.json()
            return bool(result.get('fraud_prediction'))
        except Exception as exc:
            _logger.error(
                "Fraud prediction failed for transaction %s: %s",
                self.id,
                exc,
            )
            return False

    def done(self):
        for e in self:
            e.write({'state': 'done'})
            
    def multi_screen(self):
        for e in self:
            try:
                e.action_screen()
            except Exception as ex:
                print(f"Error screening transaction {e.name}: {ex}")

    def action_screen(self):
        self.ensure_one()
        rules = self.env['res.transaction.screening.rule'].search(
            [('state', '=', 'active')], order='priority')
        
        if rules:
            risk_levels = []
            for rule in rules:
                # try:
                if rule.condition_select == 'python':
                    localdict = {
                        'result': None,
                        'transaction': self,
                        'customer': self.customer_id,
                        'branch': self.branch_id,
                        'account': self.account_id,
                        'currency': self.currency_id,
                        'env': self.env
                    }
                    if rule._satisfy_condition(localdict) == True:
                        history_id = self.env['res.transaction.screening.history'].create({
                            'transaction_id': self.id,
                            'rule_id': rule.id,
                            'risk_level': rule.risk_level
                        })
                        self.rule_id = rule
                        risk_levels.append(rule.risk_level)
                        if rule.transaction_flag =='suspicious':
                            self.action_mark_as_suspicious()
                            
                if rule.condition_select == 'sql':
                    # self.action_compute_rule_lines(rule)
                    query = rule.sql_query
                    char_to_replace = {
                                    '#TRANSACTION_ID#': f"{self.id}",
                                    '#AMOUNT#': f"{self.amount}",
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
                    _logger.info(f'this is the query to execute {query}')
                    rec = self.env.cr.fetchone()
                    _logger.info(f'this is the result of the query {rec}, this is the trans amount {self.amount}')
                    if rec is not None:
                        history_id = self.env['res.transaction.screening.history'].create({
                            'transaction_id': self.id,
                            'rule_id': rule.id,
                            'risk_level': rule.risk_level
                        })
                        self.rule_id = rule
                        risk_levels.append(rule.risk_level)
                        if rule.transaction_flag =='suspicious':
                            self.action_mark_as_suspicious()
            
            if 'high' in risk_levels:
                self.risk_level = 'high'
                return
            if 'medium' in risk_levels:
                self.risk_level = 'medium'
                return
            self.risk_level = 'low'
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
            domain = [('state', '!=', 'done')]
        else:
            # Regular users only see customers in their assigned branches
            domain = [
                ('branch_id.id', 'in', [
                 e.id for e in self.env.user.branches_id]), ('state', '!=', 'done')]
            
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
                    e.id for e in self.env.user.branches_id])
            ]

        action = {
            'name': _('All Transactions'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': domain,
            'limit' : 3000000,
            'context': {'search_default_group_branch': 1},
        }
            

        return action
    
    def get_risk_score(self):
        return self.risk_score
    
    def action_compute_rule_lines(self, rule):
        
        self.env["res.transaction.screening.rule.line"].search(
            [('transaction_id', '=', self.id)]).unlink()
                        
        result = self.env.cr.execute(rule.sql_query, (self.id,))                
        if result is not None:        
            self.env['res.transaction.screening.rule.line'].create({
                'transaction_id': self.id,
                'rule_line_id': rule.id,
            })
