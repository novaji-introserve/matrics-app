# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class Country(models.Model):
    _inherit = 'res.country'
    _rec_name = 'countryname'
    _sql_constraints = [
        ('uniq_countrycode', 'unique(countrycode)',
         "country already exists. Value must be unique!"),
    ]
    
    countryname = fields.Char(string="Name", index=True)
    countrycode = fields.Char(string="code", index=True)
