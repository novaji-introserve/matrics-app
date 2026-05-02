from odoo import _, api, fields, models
from odoo.exceptions import ValidationError
import xml.etree.ElementTree as ET
from lxml import etree
import base64
from datetime import datetime, timedelta


class SuspiciousTransactionHistory(models.Model):
    _name = 'nfiu.suspicious.transaction.hist'
    _description = 'Suspicious Transaction History'

    transaction_id = fields.Many2one(
        'res.customer.transaction', string='Transaction', required=True, ondelete='cascade',index=True)
    name = fields.Char(string='Name',related='transaction_id.name')
    account_id = fields.Many2one(comodel_name='res.partner.account',related='transaction_id.account_id', string='Account',ondelete='cascade',index=True)
    customer_id = fields.Many2one(
        comodel_name='res.partner', string='Customer', related='transaction_id.customer_id',
        ondelete='cascade', index=True)
    date_reported = fields.Datetime(
        string='Date Reported', default=fields.Datetime.now)
    reported_by = fields.Many2one(
        'res.users', string='Reported By', default=lambda self: self.env.user)
    comments = fields.Text(string='Comments')
    


    def action_view_transaction(self):
        return {
            'type': 'ir.actions.act_window',
            'name': _('View Transaction'),
            'res_model': 'res.customer.transaction',
            'view_mode': 'form',
            'res_id': self.transaction_id.id,
            'target': 'current',
        }


class Case(models.Model):
    _inherit = 'case.manager'
    
    @api.model_create_multi
    def create(self, vals_list):
        result = super(Case, self).create(vals_list)
        for vals in vals_list:
            if 'transaction_id' in vals and vals['transaction_id']:
                transaction = self.env['res.customer.transaction'].browse(vals['transaction_id'])
                if transaction:
                    transaction.write({
                        'state': 'awaiting_approval',  })
        return result


class TransactionScreeningRule(models.Model):
    _inherit= 'res.transaction.screening.rule'
    transaction_flag = fields.Selection(string='Flag Transactions As', selection=[('unusual', 'Unusual'), ('suspicious', 'Suspicious')],tracking=True,help='Flag transactions as Unusual or Suspicious based on the rule. This will be used to set the state of the transaction when the rule is applied.')

class NFIUTransaction(models.Model):
    _description = 'Reporting Transaction'
    _inherit = 'res.customer.transaction'

    report_id = fields.Many2one(
        'nfiu.report', string='Report', required=True, ondelete='cascade')
    transaction_number = fields.Char(
        string='Transaction Number', required=True, size=50)
    internal_ref_number = fields.Char(
        string='Internal Reference Number', size=50)
    transaction_location = fields.Char(string='Transaction Location', size=255)

    teller = fields.Char(string='Teller', size=50)
    authorized = fields.Char(string='Authorized By', size=50)

    report_nfiu = fields.Boolean(
        string='NFIU Reported', default=False, index=True)
    suspicious_transaction = fields.Boolean(
        string='Suspicious Transaction', default=False, index=True, tracking=True)
    transmode_code = fields.Selection([
        ('A', 'Cash'),
        ('B', 'Check'),
        ('C', 'Credit Card'),
        ('E', 'Electronic Transfer'),
        ('k', 'Other'),
        ('T', 'Wire Transfer'),
    ], string='Transaction Mode Code', required=True, default='E')

    transmode_comment = fields.Char(string='Transaction Mode Comment', size=50)
    amount_local = fields.Float(
        string='Amount (Local Currency)', digits=(10, 2), required=True)
    from_person_id = fields.Many2one('res.partner', string='From Person')
    '''
    # From party details
    from_account_id = fields.Many2one('nfiu.account', string='From Account')
    '''
    from_entity_id = fields.Many2one('nfiu.entity', string='From Entity')
    from_funds_code = fields.Selection([
        ('A', 'Account'),
        ('B', 'Bearer Bonds'),
        ('C', 'Cash'),
        ('DW', 'Digital Wallet'),
        ('E', 'Electronic Transfer'),
        ('F', 'Funds Transfer'),
        ('G', 'Government Securities'),
        ('H', 'High Value Goods'),
        ('J', 'Jewelry'),
        ('K', 'Known Source'),
        ('L', 'Life Insurance'),
        ('MM', 'Money Market'),
        ('P', 'Precious Metals'),
    ], string='From Funds Code', default='F')
    from_funds_comment = fields.Char(string='From Funds Comment', size=255)
    from_country = fields.Char(string='From Country', size=2, default='NG')
    to_person_id = fields.Many2one('res.partner', string='To Person')
    '''
    # To party details   
    to_account_id = fields.Many2one('nfiu.account', string='To Account')
    '''
    to_entity_id = fields.Many2one('nfiu.entity', string='To Entity')
    to_funds_code = fields.Selection([
        ('A', 'Account'),
        ('B', 'Bearer Bonds'),
        ('C', 'Cash'),
        ('DW', 'Digital Wallet'),
        ('E', 'Electronic Transfer'),
        ('F', 'Funds Transfer'),
        ('G', 'Government Securities'),
        ('H', 'High Value Goods'),
        ('J', 'Jewelry'),
        ('K', 'Known Source'),
        ('L', 'Life Insurance'),
        ('MM', 'Money Market'),
        ('P', 'Precious Metals'),
    ], string='To Funds Code', default='F')
    to_funds_comment = fields.Char(string='To Funds Comment', size=255)
    to_country = fields.Char(string='To Country', size=2, default='NG')
    comments = fields.Text(string='Comments')
    suspicious_transaction_history_ids = fields.One2many(
        'nfiu.suspicious.transaction.hist', 'transaction_id', string='Suspicious Transaction History')
    
    state = fields.Selection(string='Status', selection=[(
        'new', 'To Review'),('unusual','Unusual'),('awaiting_approval','Under Investigation'),('suspicious','Suspicious'), ('done', 'Done')], tracking=True, index=True, default='new')
    case_ids = fields.One2many(
        comodel_name='case.manager', index=True, inverse_name='transaction_id', string='Cases', readonly=True)
    total_cases = fields.Integer(
        string='Cases', compute='transaction_total_cases', index=True, store=False)

    @api.depends('date_created')
    def _compute_date(self):
        for rec in self:
            rec.date_transaction = rec.date_created
            rec.value_date = rec.date_created

    value_date = fields.Datetime(string='Value Date', compute=_compute_date)
    date_transaction = fields.Datetime(
        string='Transaction Date', required=True, compute=_compute_date)

    def action_mark_as_suspicious(self):
        for record in self:
            self.env['nfiu.suspicious.transaction.hist'].create({
                'transaction_id': record.id,
                'name': record.name,
                'comments': record.comments,
                'reported_by': self.env.user.id,
            })
            record.write({
                'suspicious_transaction': True,
                'state': 'suspicious',
                'report_nfiu': True,
            })

    def action_unmark_as_suspicious(self):
        for record in self:
            record.write({
                'suspicious_transaction': False,
                'state': 'new',
            })
            
    def action_mark_unusual(self):
        for record in self:
            record.write({
                'suspicious_transaction': False,
                'state': 'unusual',
            })

    def report_fiu(self):
        self.write({
            'report_nfiu': True,
            'transaction_number': self.name,
            'internal_ref_number': self.name,
            'transaction_location': self.branch_id.name,
            'teller': 'SYSTEM',
            'authorized': 'SYSTEM',
            'transmode_code': 'E',
            'transmode_comment': self.narration,
            'amount_local': self.amount,
            'from_funds_comment': self.narration,
            'to_funds_comment': self.narration,
            'comments': self.narration

        })

    @api.model
    def open_local_transactions(self):
        # show only nfiu transactions
        setting = self.env['res.compliance.settings'].search(
            [('code', '=', 'nfiu_default_ctr_currency')], limit=1)
        # Default value if no settings found
        currency = 'NGN'
        for e in setting:
            currency = e.val.strip()
        currency_rec = self.env['res.currency'].search(
            [('name', '=', currency.upper())], limit=1)
        currency_id = currency_rec.id if currency_rec else False
        domain = [('report_nfiu', '=', True),
                  ('currency_id', '=', currency_id)]
        return {
            'name': _('Local Currency Transactions'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_foreign_transactions(self):
        # show only nfiu transactions
        setting = self.env['res.compliance.settings'].search(
            [('code', '=', 'nfiu_default_ctr_currency')], limit=1)
        # Default value if no settings found
        currency = 'NGN'
        for e in setting:
            currency = e.val.strip()
        currency_rec = self.env['res.currency'].search(
            [('name', '=', currency.upper())], limit=1)
        currency_id = currency_rec.id if currency_rec else False
        domain = [('report_nfiu', '=', True),
                  ('currency_id', '!=', currency_id)]
        return {
            'name': _('Foreign Currency Transactions'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }

    @api.model
    def open_suspicious_transactions(self):
        # show only nfiu transactions
        setting = self.env['res.compliance.settings'].search(
            [('code', '=', 'nfiu_default_ctr_currency')], limit=1)
        domain = [('suspicious_transaction', '=', True)]
        return {
            'name': _('Suspicious Transactions'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1, 'search_default_group_currency': 1}
        }
    
    @api.depends('case_ids')
    def transaction_total_cases(self):
        for e in self:
            e.total_cases = len(e.case_ids)

    def action_transaction_cases(self):
        return {
            'name': _('Cases'),
            'type': 'ir.actions.act_window',
            'res_model': 'case.manager',
            'view_mode': 'tree,form',
            'domain': [('transaction_id.id', 'in', [self.id])],
            'context': {'search_default_group_by_status': 1}
        }
        
