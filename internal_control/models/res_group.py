from odoo import api, fields, models

class res_group_inherited(models.Model):
    _inherit = 'res.groups'
    
    active = fields.Boolean(default=True)