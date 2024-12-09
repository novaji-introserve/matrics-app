from odoo import fields, api, models


class Users(models.Model):
    _inherit = 'res.users'
    
    branches_id = fields.Many2many(
        'tbl.branch', 'tbl_branch_users_rel', 'user_id', 'branch_id', string='Branches')
    default_branch_id = fields.Many2one(
        comodel_name='tbl.branch', string='Default Branch')