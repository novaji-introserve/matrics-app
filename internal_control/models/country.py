# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class Country(models.Model):
    _name = 'res.partner.country'
    _description = 'Country'

    
    name = fields.Char(string="Name", index=True)
    code = fields.Char(string="Code", index=True, unique=True)
    shortname = fields.Char(string="Short Name", index=True)
