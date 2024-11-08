from odoo import models, fields, api




class Users(models.Model):
    _inherit = 'res.users'

    branches_id = fields.Many2many(
        'rulebook.branch', 'res_branch_users_rel', 'user_id', 'branch_id', string='Branches')
    default_branch_id = fields.Many2one(
        comodel_name='rulebook.branch', string='Default Branch')
