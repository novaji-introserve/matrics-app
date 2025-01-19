from odoo import fields, api, models


class Users(models.Model):
    _inherit = 'res.users'
    
    branches_id = fields.Many2many(
        'res.branch', 'res_branch_users_rel', 'user_id', 'subbranchcode', string='Branches')