# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class State(models.Model):
    _inherit = 'res.country.state'
    
    shortname = fields.Char(string="Short Name", index=True, unqiue=True)
    capital = fields.Char(string="Capital", index=True)
    region_id = fields.Many2one(comodel_name='res.partner.region',
                              string='Region', index=True)
