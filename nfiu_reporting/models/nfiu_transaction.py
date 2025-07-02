from odoo import _, api, fields, models
from odoo.exceptions import ValidationError
import xml.etree.ElementTree as ET
from lxml import etree
import base64
from datetime import datetime, timedelta


class NFIUTransaction(models.Model):
    #_name = 'nfiu.transaction'
    _description = 'NFIU Transaction'
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
    
    report_nfiu = fields.Boolean(string='NFIU Reported',default=False, index=True)
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
        string='Amount (Local Currency)',digits=(10,2),required=True)
    '''
    # From party details
    from_person_id = fields.Many2one('nfiu.person', string='From Person')
    from_entity_id = fields.Many2one('nfiu.entity', string='From Entity')
    from_account_id = fields.Many2one('nfiu.account', string='From Account')
    '''
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
    '''
    # To party details
    to_person_id = fields.Many2one('nfiu.person', string='To Person')
    to_entity_id = fields.Many2one('nfiu.entity', string='To Entity')
    to_account_id = fields.Many2one('nfiu.account', string='To Account')
    '''
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
    comments = fields.Text(string='Comments', size=4000)
    
    @api.depends('date_created')
    def _compute_date(self):
        for rec in self:
            rec.date_transaction = rec.date_created
            rec.value_date = rec.date_created
            
    value_date = fields.Datetime(string='Value Date',compute=_compute_date)
    date_transaction = fields.Datetime(
       string='Transaction Date', required=True,compute=_compute_date)
    
    def report_fiu(self):
        self.write({
            'report_nfiu': True,
            'transaction_number': self.name,
            'internal_ref_number':self.name,
            'transaction_location':self.branch_id.name,
            'teller': 'SYSTEM',
            'authorized': 'SYSTEM',
            'transmode_code':'E',
            'transmode_comment': self.narration,
            'amount_local': self.amount,
            'from_funds_comment': self.narration,
            'to_funds_comment': self.narration,
            'comments': self.narration
            
        })
    
    @api.model
    def open_transactions(self):
        # show only nfiu transactions
        setting = self.env['res.compliance.settings'].search([('code', '=', 'nfiu_default_ctr_currency')], limit=1)
        # Default value if no settings found
        currency = 'NGN'
        for e in setting:
            currency = e.val.strip()
        currency_ids = self.env['res.currency'].search([('name','=',currency.upper())])
        for i in currency_ids:
            currency_id = i.id
        domain = [('report_nfiu', '=', True),('currency_id','=',currency_id)]
        return {
            'name': _('NFIU CTR Transactions'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }
    
    @api.model
    def other_transactions(self):
        # show only nfiu transactions
        setting = self.env['res.compliance.settings'].search([('code', '=', 'nfiu_default_ctr_currency')], limit=1)
        # Default value if no settings found
        currency = 'NGN'
        for e in setting:
            currency = e.val.strip()
        currency_ids = self.env['res.currency'].search([('name','=',currency.upper())])
        for i in currency_ids:
            currency_id = i.id
        domain = [('report_nfiu', '=', True),('currency_id','!=',currency_id)]
        return {
            'name': _('NFIU FTR Transactions'),
            'type': 'ir.actions.act_window',
            'res_model': 'res.customer.transaction',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'search_default_group_branch': 1}
        }

        
