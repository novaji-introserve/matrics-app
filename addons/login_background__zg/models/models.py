# -*- coding: utf-8 -*-
from odoo import api, fields, models, modules


class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    login_background_image = fields.Binary(string="Image", help='Select Background Image For Login Page')

    

    @api.model
    def get_values(self):
        res = super(ResConfigSettings, self).get_values()
        # image_id = int(self.env['ir.config_parameter'].sudo().get_param('login_background__zg.login_background_image'))
        res.update(
            login_background_image=self.env['ir.config_parameter'].sudo().get_param('login_background__zg.login_background_image'),
        )
        return res

    def set_values(self):
        super(ResConfigSettings, self).set_values()
        param = self.env['ir.config_parameter'].sudo()
        set_login_background_image = self.login_background_image or False
        param.set_param('login_background__zg.login_background_image', set_login_background_image)
