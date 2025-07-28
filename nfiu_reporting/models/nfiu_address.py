from odoo import _, api, fields, models
from odoo.exceptions import ValidationError
import xml.etree.ElementTree as ET
from lxml import etree
import base64
from datetime import datetime, timedelta


class NFIUAddress(models.Model):
    _name = 'nfiu.address'
<<<<<<< HEAD
<<<<<<< HEAD
    _description = 'Address'
=======
    _description = 'NFIU Address'
>>>>>>> 816be76 (XML Schema Validator)
=======
    _description = 'Address'
>>>>>>> b75258c (Suspicious Transaction history)
    
    name = fields.Char(string='Name',required=True)
    person_id = fields.Many2one('nfiu.person', string='Person')
    entity_id = fields.Many2one('nfiu.entity', string='Entity')

    address_type = fields.Selection([
        ('B', 'Business'),
        ('P', 'Personal'),
    ], string='Address Type', required=True, default='P')

    address = fields.Char(string='Address', required=True, size=100,compute='_compute_address', store=True)
    town = fields.Char(string='Town', size=255)
    city = fields.Char(string='City', required=True, size=255)
    zip = fields.Char(string='ZIP Code', size=10)
    country_code = fields.Char(
        string='Country Code', required=True, size=2, default='NG')
    state = fields.Char(string='State', size=255)

    comments = fields.Text(string='Comments', size=4000)
    
    @api.depends('name')
    def _compute_address(self):
        for address in self:
            address.address = address.name
