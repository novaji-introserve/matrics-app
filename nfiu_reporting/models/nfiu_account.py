from odoo import _, api, fields, models
from odoo.exceptions import ValidationError
import xml.etree.ElementTree as ET
from lxml import etree
import base64
from datetime import datetime, timedelta


class NFIUAccount(models.Model):
    _name = 'nfiu.account'
    _description = 'NFIU Account'
    name = fields.Char(string='Account Name',
                      compute='_compute_name', store=True)
    institution_name = fields.Char(
        string='Institution Name', required=True, size=255)
    institution_code = fields.Char(string='Institution Code', size=50)
    swift = fields.Char(string='SWIFT Code', size=11)
    non_bank_institution = fields.Boolean(
        string='Non-Bank Institution', default=False)
    branch = fields.Char(string='Branch', size=255)
    account = fields.Char(string='Account Number', required=True, size=50)
    currency_code = fields.Selection([
        ('NGN', 'Nigerian Naira'),
        ('USD', 'US Dollar'),
        ('EUR', 'Euro'),
        ('GBP', 'British Pound'),
    ], string='Currency Code', default='NGN')
    account_name = fields.Char(string='Account Name', required=True, size=255)
    iban = fields.Char(string='IBAN', size=34)
    client_number = fields.Char(string='Client Number', size=30)

    personal_account_type = fields.Selection([
        ('A', 'Current Account'),
        ('B', 'Savings Account'),
        ('C', 'Credit Account'),
        ('D', 'Deposit Account'),
        ('E', 'Electronic Money Account'),
        ('I', 'Investment Account'),
        ('L', 'Loan Account'),
        ('O', 'Other'),
        ('P', 'Pension Account'),
        ('T', 'Trust Account'),
        ('U', 'Utility Account'),
        ('Y', 'Money Market Account'),
    ], string='Account Type')

    opened = fields.Date(string='Date Opened')
    closed = fields.Date(string='Date Closed')
    balance = fields.Float(string='Balance')
    date_balance = fields.Date(string='Balance Date')

    status_code = fields.Selection([
        ('A', 'Active'),
        ('B', 'Blocked'),
        ('C', 'Closed'),
        ('CL', 'Cancelled'),
        ('h', 'Held'),
    ], string='Account Status')

    beneficiary = fields.Char(string='Beneficiary', size=50)
    beneficiary_comment = fields.Char(string='Beneficiary Comment', size=255)
    comments = fields.Text(string='Comments', size=4000)

    @api.depends('institution_name', 'account')
    def _compute_name(self):
        for record in self:
            record.name = f"{record.institution_name} - {record.account}"
    
