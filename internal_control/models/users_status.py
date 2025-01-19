from odoo import fields, models, api, _


class UsersStatus(models.Model):
    _name = 'res.user.status'
    _description = _('Users Status')
    _rec_name = 'description'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    status_code = fields.Char(string='Status Code',  readonly=True, index=True, unique=True)
    description = fields.Char(string='Description',  readonly=True, index=True)
    
