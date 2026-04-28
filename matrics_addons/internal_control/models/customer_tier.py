from odoo import models, fields, api,_


class CustomerTier(models.Model):
    _inherit = 'res.partner.tier'

    depositmax = fields.Float(string='Maximum Single Deposit', digits=(20,2), readonly=True, index=True)
    cummulativemax = fields.Float(string='Maximum Balance', digits=(20,2), readonly=True, index=True)