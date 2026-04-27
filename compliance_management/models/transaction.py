# -*- coding: utf-8 -*-

from odoo import models, fields, api, _
import logging
from dotenv import load_dotenv

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
    date_created = fields.Datetime(string='Tran. Date',  help="Tran. Date", index=True)
    
    customer_id = fields.Many2one(
        comodel_name='res.partner', string='Customer', index=True) 
    branch_id = fields.Many2one(
        comodel_name='res.branch', string='Branch', index=True)
    amount = fields.Float(string='Transaction Amount', digits=(15, 2))
    tran_type = fields.Selection(string='Tran. Type', selection=[
                                 ('dr', 'Debit'), ('cr', 'Credit')], index=True)
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
    transaction_risk_level = fields.Selection(
        string='Transaction Risk Level',
        selection=[('low', 'Low'), ('medium', 'Medium'), ('high', 'High')],
        tracking=True, index=True,
        help="Worst risk level matched by screening rules on this transaction.",
    )

    state = fields.Selection(string='Status', selection=[(
        'new', 'To Review'), ('done', 'Done')], tracking=True, index=True, default='new')
    likely_fraud = fields.Boolean(string='Likely Fraud',tracking=True,related='rule_id.likely_fraud')
    
    
    
    account_officer_id = fields.Many2one(
        comodel_name='account.officers', string='Account Officer', index=True, tracking=True, readonly=True)
    trans_code = fields.Char(string='Transaction Code')
    currency = fields.Char(string='Currency')
    inputter = fields.Char(string='Inputter')
    authorizer = fields.Char(string='Authorizer')
    transaction_type = fields.Selection(selection=[(
        'C', 'Credit'), ('D', 'Debit')],  index=True, string='Transaction Type')
    branch_code = fields.Char(string="Branch Code")
    
    show_create_case = fields.Boolean(
        string="Case Management_v2 Installed",
        compute='_compute_is_case_manager_installed',
        store=False,)

    show_create_case_button = fields.Boolean(
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
    def create(self, vals_list):
        return super(Transaction, self).create(vals_list)

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

    def action_create_case(self):
        """Alias kept for backward-compatibility with stale database views."""
        return self.action_create_transaction_case()

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
            'default_customer_id': self.customer_id.id if self.customer_id else False,
            'default_case_status': 'draft',
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
        narration += f"Date: {self.date_created or (self.created_date if hasattr(self, 'created_date') else None)}\n"

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
        case_installed = bool(self.env['ir.module.module'].search([
            ('name', '=', 'case_management_v2'),
            ('state', '=', 'installed')
        ], limit=1))
        for record in self:
            record.show_create_case = case_installed
            record.show_create_case_button = case_installed



        
    
    
    @api.model
    def _is_case_management_available(self):
        """Cache the module availability check"""
        if not hasattr(self, '_case_management_available'):
            self._case_management_available = bool(self.env['ir.module.module'].search([
                ('name', '=', 'case_management'),
                ('state', '=', 'installed')
            ], limit=1))
        return self._case_management_available    
   


    def init(self):
        self.env.cr.execute(
            "CREATE INDEX IF NOT EXISTS res_customer_transaction_id_idx ON res_customer_transaction (id)")
        self.env.cr.execute("""
        ALTER TABLE res_customer_transaction
        ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
    """)

    def get_risk_level(self):
        return self.risk_level

    def done(self):
        for e in self:
            e.write({'state': 'done'})
            
    def multi_screen(self):
        rules = self.env['res.transaction.screening.rule'].search(
            [('state', '=', 'active')], order='priority'
        )
        if not rules:
            return True

        for tx in self:
            try:
                tx.action_screen(rules=rules)  
            except Exception as ex:
                _logger.error("Error screening transaction %s: %s", tx.name, ex)
        return True

    def action_screen(self, rules=None):
        """
        Screen a single transaction against all active rules.

        Args:
            rules: pre-fetched recordset from multi_screen().
                If None (called from a UI button) rules are fetched here.
        """
        self.ensure_one()

        # Fallback: called standalone from UI button
        if rules is None:
            rules = self.env['res.transaction.screening.rule'].search(
                [('state', '=', 'active')], order='priority asc'
            )

        if not rules:
            _logger.info("[action_screen] No active rules — marking done  ref=%s", self.name)
            self.write({'state': 'done'})
            return

        _logger.debug(
            "[action_screen] START  ref=%s | amount=%s | customer=%s | rules=%d",
            self.name, self.amount, self.customer_id.name or 'N/A', len(rules)
        )

        # Clear stale history so re-screening doesn't produce duplicate rows
        self.env['res.transaction.screening.history'].search(
            [('transaction_id', '=', self.id)]
        ).unlink()

        risk_levels  = []
        matched_rule = None

        for rule in rules:
            matched = False

            try:
                with self.env.cr.savepoint():
                    if rule.condition_select == 'python':
                        localdict = {
                            'result':      False,
                            'transaction': self,
                            'customer':    self.customer_id,
                            'branch':      self.branch_id,
                            'account':     self.account_id,
                            'currency':    self.currency_id,
                            'env':         self.env,
                        }
                        matched = rule._satisfy_condition(localdict)
                        _logger.debug(
                            "[action_screen] rule='%s' (python) → %s",
                            rule.name, 'MATCHED' if matched else 'no match'
                        )

                    elif rule.condition_select == 'sql':
                        placeholders = {
                            '#TRANSACTION_ID#': str(self.id),
                            '#AMOUNT#':         str(self.amount),
                            '#ACCOUNT_ID#':     str(self.account_id.id),
                            '#CUSTOMER_ID#':    str(self.customer_id.id),
                            '#TRAN_DATE#':      str(self.date_created or ''),
                            '#BRANCH_ID#':      str(self.branch_id.id),
                            '#CURRENCY_ID#':    str(self.currency_id.id),
                        }
                        query = rule.sql_query
                        for placeholder, value in placeholders.items():
                            query = query.replace(placeholder, value)

                        self.env.cr.execute(query)
                        rec     = self.env.cr.fetchone()
                        matched = rec is not None
                        _logger.debug(
                            "[action_screen] rule='%s' (sql) → %s | result=%s",
                            rule.name, 'MATCHED' if matched else 'no match', rec
                        )

                    else:
                        _logger.warning(
                            "[action_screen] rule='%s' has unknown condition_select='%s' — skipped",
                            rule.name, rule.condition_select
                        )

            except Exception as ex:
                # One broken rule must NOT abort screening of the remaining rules
                _logger.error(
                    "[action_screen] rule='%s' raised an error on tx=%s: %s",
                    rule.name, self.name, ex, exc_info=True
                )
                continue

            if matched:
                self.env['res.transaction.screening.history'].create({
                    'transaction_id': self.id,
                    'rule_id':        rule.id,
                    'risk_level':     rule.risk_level,
                })
                risk_levels.append(rule.risk_level)
                matched_rule = rule  

                if rule.transaction_flag == 'suspicious':
                    self.action_mark_as_suspicious(rule=rule)
                    _logger.info(
                        "[action_screen] tx=%s marked SUSPICIOUS by rule='%s'",
                        self.name, rule.name
                    )
                elif rule.transaction_flag == 'unusual':
                    self.action_mark_unusual()
                    _logger.info(
                        "[action_screen] tx=%s marked UNUSUAL by rule='%s'",
                        self.name, rule.name
                    )

        # Worst risk level wins: high > medium > low
        RISK_ORDER = ['high', 'medium', 'low']
        final_risk = next((r for r in RISK_ORDER if r in risk_levels), 'low')

        _logger.info(
            "[action_screen] DONE  ref=%s | matched=%d rule(s) | final_risk=%s",
            self.name, len(risk_levels), final_risk
        )

        # Single write — one UPDATE to the DB instead of scattered assignments
        vals = {
            'state':                  'done',
            'transaction_risk_level': final_risk,   # dedicated field, NOT the related= risk_level
        }
        if matched_rule:
            vals['rule_id'] = matched_rule.id

        self.write(vals)                

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
