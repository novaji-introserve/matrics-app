# -*- coding: utf-8 -*-
from odoo import api, fields, models
import logging

_logger = logging.getLogger(__name__)

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
    
    @api.model
    def get_session_info(self):
        """Add chatter position to session info"""
        result = super(ResUsers, self).get_session_info()
        if self.env.user:
            # Add debug log
            _logger.info("Setting session info for user %s with chatter_position=%s", 
                        self.env.user.name, self.env.user.chatter_position)
            
            result['chatter_position'] = self.env.user.chatter_position
            result['sidebar_type'] = self.env.user.sidebar_type
        return result
    
    def write(self, vals):
        """Update session context when user preferences change"""
        res = super(ResUsers, self).write(vals)
        if 'chatter_position' in vals or 'sidebar_type' in vals:
            for user in self:
                if user == self.env.user:
                    self.env.user.context_get.clear_cache(self.env.user)
        return res