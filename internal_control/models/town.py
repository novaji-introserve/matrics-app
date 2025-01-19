# -*- coding: utf-8 -*-

from odoo import models, fields, api,_


class State(models.Model):
    _name = 'res.partner.town'
    _description = 'Town'

    
    name = fields.Char(string="town", index=True)
    code = fields.Char(string="Code", index=True, unique=True)
    shortname = fields.Char(string="Short Name", index=True)
    state_id = fields.Many2one(comodel_name='res.partner.state',
                              string='State', index=True)
