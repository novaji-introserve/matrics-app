from odoo import models, fields, api
from odoo.exceptions import ValidationError
class Alert_group(models.Model):
    _name = 'alert.group'
    _description = 'alert Group for exception management'
    

    name = fields.Char(string="Name", required=True)
    users_ids = fields.Many2many('res.users', 'alert_group_user_rel', 'alert_group_id', 'user_id', string="Users", required=True)
    email_cc = fields.Char(string="Copy_Email")
    state = fields.Boolean(string="State",default=True, required=True)
    tag = fields.Char(string="Tag")
    date_created = fields.Datetime(string="created_at", default=fields.Datetime.now())
    email_list = fields.Text(string="email_list", compute="split_email")

    @api.onchange('users_ids')
    def split_email(self):
        for record in self:
            
             record.email_list = ', '.join(user.email for user in record.users_ids)
           
