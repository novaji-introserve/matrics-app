from odoo import models, fields, api
import logging


_logger = logging.getLogger(__name__)

class Users(models.Model):
    _inherit = 'res.users'

    branches_id = fields.Many2many(
        'rulebook.branch', 'res_branch_users_rel', 'user_id', 'branch_id', string='Branches')
    default_branch_id = fields.Many2one(
        comodel_name='rulebook.branch', string='Default Branch')
    # _logger.error(
    #     f'Current user ID: {env.user.id}, Employee ID: {user.employee_id.id if user.employee_id else None}')
