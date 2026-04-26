# -*- coding: utf-8 -*-

from odoo import api
from odoo import fields
from odoo import models

class ResUsers(models.Model):
    _inherit = 'res.users'
    
    sidebar_type = fields.Selection(
                                    selection=[
                                    ('invisible', 'Invisible'),
                                    ('small', 'Small'),
                                    ('large', 'Large')
                                    ], 
                                    string="Sidebar Type",
                                    default='small',
        required=True)
    chatter_position = fields.Selection(
        selection=[
            ('normal', 'Normal'),
            ('sided', 'Sided'),
        ], 
        string="Chatter Position", 
        default='normal',
        required=True)