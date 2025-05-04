# -*- coding: utf-8 -*-
from odoo import api, fields, models

class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'
    
    default_sidebar_type = fields.Selection(
        related='company_id.default_sidebar_type',
        readonly=False,
        default_model='res.users', 
        selection=[
            ('invisible', 'Invisible'),
            ('small', 'Small'),
            ('large', 'Large')
        ],
        string="Default Sidebar Type")
        
    default_chatter_position = fields.Selection(
        related='company_id.default_chatter_position',
        readonly=False,
        default_model='res.users',
        selection=[
            ('normal', 'Normal'),
            ('sided', 'Sided'),
        ],
        string="Default Chatter Position")
    
    def set_values(self):
        super(ResConfigSettings, self).set_values()
        
        # 1. Set defaults for NEW users
        IrDefault = self.env['ir.default'].sudo()
        IrDefault.set('res.users', 'sidebar_type', self.default_sidebar_type)
        IrDefault.set('res.users', 'chatter_position', self.default_chatter_position)
        
        # 2. Always apply to ALL existing users
        self.env['res.users'].sudo().search([]).write({
            'sidebar_type': self.default_sidebar_type,
            'chatter_position': self.default_chatter_position,
        })