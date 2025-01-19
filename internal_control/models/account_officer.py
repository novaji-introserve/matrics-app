from odoo import fields, models, api, _


class AccountOfficer(models.Model):
    _name = 'res.account.officer'
    _description = _('Account Officer')
    _rec_name = 'staff_id'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    staff_id = fields.Char(string='Staff ID',  readonly=True, index=True, unique=True)
    officername = fields.Char(string='Staff Name',  readonly=True, index=True)
    status_id = fields.Many2one(comodel_name='res.user.status',
                              string='Staff Status', index=True)
    deptid = fields.Many2one(comodel_name='hr.department',
                              string='Staff Department', index=True)
    branch_id = fields.Many2one(comodel_name='res_branch',
                              string='Staff Branch', index=True)
