from odoo import models, fields, api

class EmailBranch(models.Model):
    _name = 'email.branch'
    _description = 'branch table'
    _order = "id desc"

    branch_id = fields.Many2one('tbl.branch', string="Branch", required=True)
    users_ids = fields.Many2many('res.users', 'alert_email_branch_rel', 'alert_email_branch_id', 'user_id', string="Users", required=True)
    state = fields.Boolean(string="State",default=True, required=True)
    date_created = fields.Datetime(string="created_at", default=fields.Datetime.now())
    email_list = fields.Text(string="email_list", compute="split_email")

    @api.onchange('users_ids')
    def split_email(self):
        for record in self:
            
             record.email_list = ', '.join(user.email for user in record.users_ids)