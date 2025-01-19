# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class State(models.Model):
    _name = 'res.partner.state'
    _description = 'State'

    
    name = fields.Char(string="State", index=True)
    code = fields.Char(string="Code", index=True, unique=True)
    shortname = fields.Char(string="Short Name", index=True)
    capital = fields.Char(string="Capital", index=True)
    country_id = fields.Many2one(comodel_name='res.partner.country',
                              string='Country', index=True)
    region_id = fields.Many2one(comodel_name='res.partner.region',
                              string='Region', index=True)
