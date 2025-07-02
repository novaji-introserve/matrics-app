
from odoo import _, api, fields, models
from odoo.exceptions import ValidationError
import xml.etree.ElementTree as ET
from lxml import etree
import base64
from datetime import datetime, timedelta

class NFIUIndicator(models.Model):
    _name = 'nfiu.indicator'
    _description = 'NFIU Report Indicator'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    name = fields.Char(string='Indicator Name', required=True,tracking=True)
    code = fields.Char(string='Indicator Code', required=True,tracking=True)
    description = fields.Text(string='Description')
    category = fields.Selection([
        ('THRESHOLDREPORT', 'Threshold Report'),
        ('CILPA', 'Customer Identity and Location'),
        ('CMIA', 'Customer Monitoring and Investigation'),
        ('FGAD', 'Fraud and Government Anti-Corruption'),
        ('FXCD', 'Foreign Exchange and Currency'),
        ('ILA', 'Investment and Loan Activities'),
        ('LGAAR', 'Legal and Government Anti-Avoidance'),
        ('MA', 'Money Laundering Activities'),
        ('MOMCA', 'Money Order and Money Change'),
        ('NEWTC', 'New Technology and Currency'),
        ('P_Offence', 'Predicate Offence'),
        ('RECSO', 'Real Estate and Commercial'),
        ('SGAD', 'Suspicious Goods and Activities'),
        ('TRML', 'Terrorism and Related Money Laundering'),
    ], string='Category',tracking=True)
