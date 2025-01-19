# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class Town(models.Model):
    _name = 'res.partner.town'
    _description = 'Town'
    _sql_constraints = [
        ('uniq_town_code', 'unique(code)',
         "Town already exists. Value must be unique!"),
    ]

    
    name = fields.Char(string="town", index=True)
    code = fields.Char(string="Code", index=True)
    shortname = fields.Char(string="Short Name", index=True)
    state_id = fields.Many2one(comodel_name='res.country.state',
                              string='State', index=True)
