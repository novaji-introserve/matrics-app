from odoo import _, api, fields, models


class Users(models.Model):
    _inherit = 'res.users'

    branches_id = fields.Many2many(
        'res.branch', 'res_branch_users_rel', 'user_id', 'branch_id', string='Branches')
    default_branch_id = fields.Many2one(
        comodel_name='res.branch', string='Default Branch')
    
    """
    @api.model
    def create(self, vals):
        #Override create to handle branch assignments
        user = super(Users, self).create(vals)
        return user
    
    def write(self, vals):
        #Override write to handle branch updates
        result = super(Users, self).write(vals)
        return result
    """