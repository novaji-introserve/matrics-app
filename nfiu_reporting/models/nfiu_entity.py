from odoo import _, api, fields, models
from odoo.exceptions import ValidationError
import xml.etree.ElementTree as ET
from lxml import etree
import base64
from datetime import datetime, timedelta


class NFIUEntity(models.Model):
    _name = 'nfiu.entity'
    _description = 'Reporting Entity'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    name = fields.Char(string='Entity Name', required=True, size=255,tracking=True,related='company_id.display_name', store=True)
    commercial_name = fields.Char(string='Commercial Name', size=255,tracking=True)
    incorporation_legal_form = fields.Selection([
        ('-', 'Not Specified'),
        ('A', 'Association'),
        ('B', 'Business'),
        ('G', 'Government'),
        ('R', 'Religious'),
    ], string='Legal Form', default='-',tracking=True)
    local_currency = fields.Many2one(comodel_name='nfiu.currency.threshold', string='Local Currency',tracking=True)
    branch_id = fields.Many2one(comodel_name='res.branch', string='Branch', tracking=True)
    identification_id = fields.Integer(string='Reporting ID', tracking=True)
    entity_reference = fields.Char(string='Entity Reference', size=50, tracking=True)
    fiu_reference = fields.Char(string='FIU Reference Number', size=50, tracking=True)
    incorporation_number = fields.Char(string='Incorporation Number',related='company_id.company_registry', size=50,tracking=True)
    business = fields.Char(string='Business Description', size=255,tracking=True)
    incorporation_country_code = fields.Char(
        string='Incorporation Country', size=2,related='local_currency.shortname',default='NG',tracking=True)
    incorporation_date = fields.Date(string='Incorporation Date',tracking=True)

    email = fields.Char(string='Email', size=255,related='company_id.email',tracking=True)
    url = fields.Char(string='Website URL', size=255,related='company_id.website',tracking=True)
    phone = fields.Char(string='Phone', size=50,related='company_id.phone',tracking=True)

    tax_number = fields.Char(string='Tax Number', size=100,tracking=True)
    tax_reg_number = fields.Char(string='Tax Registration Number', size=100,tracking=True)

    address_ids = fields.One2many(
        'nfiu.address', 'entity_id', string='Addresses',tracking=True)
    director_ids = fields.One2many(
        'nfiu.entity.director', 'entity_id', string='Directors',tracking=True)
    comments = fields.Text(string='Comments', size=4000,tracking=True)
    company_id = fields.Many2one(
        'res.company', string='Company', required=True, default=lambda self: self.env.company, tracking=True)
    
