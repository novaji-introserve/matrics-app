# -*- coding: utf-8 -*-

from odoo import models, fields, api

class PEPSource(models.Model):
    _name = 'pep.source'
    _description = 'PEP Source'
    
    name = fields.Char('Source Name', required=True)
    domain = fields.Char('Domain/URL', required=True)
    source_type = fields.Selection([
        ('government', 'Government List'),
        ('regulatory', 'Regulatory Body'),
        ('commercial', 'Commercial Database'),
        ('other', 'Other')
    ], string='Source Type', required=True)
    active = fields.Boolean('Active', default=True)
    description = fields.Text('Description')
    last_update = fields.Datetime('Last Updated')
    country_id = fields.Many2one('res.country', string='Country')
    api_key = fields.Char('API Key')
    is_free = fields.Boolean('Is Free', default=True)
    frequency = fields.Selection([
        ('daily', 'Daily'),
        ('weekly', 'Weekly'),
        ('monthly', 'Monthly'),
        ('quarterly', 'Quarterly')
    ], string='Update Frequency', default='monthly')
    