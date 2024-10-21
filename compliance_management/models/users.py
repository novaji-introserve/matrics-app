from odoo import _, api, fields, models


class Users(models.Model):
    _inherit = 'res.users'

    branches_id = fields.Many2many('res.branch', 'res_branch_users_rel', 'user_id', 'branch_id', string='Branches')
    
    def get_branch_ids(self):
        return [1,2]
