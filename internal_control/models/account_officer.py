from odoo import fields, models, api, _


class AccountOfficer(models.Model):
    _name = 'res.account.officer'
    _description = _('Account Officer')
    _rec_name = "officername"
    _sql_constraints = [
        ('uniq_staff_id', 'unique(staff_id)',
         "Account Officer already exists. Value must be unique!"),
    ]
    staff_id = fields.Char(string='Staff ID',  readonly=True, index=True)
    officername = fields.Char(string='Staff Name',  readonly=True, index=True)
    status_id = fields.Many2one(comodel_name='res.user.status',
                              string='Staff Status', index=True)
    deptid = fields.Many2one(comodel_name='hr.department',
                              string='Staff Department', index=True)
    branch_id = fields.Many2one(comodel_name='res.branch',
                              string='Staff Branch', index=True)
